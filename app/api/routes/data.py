"""Data catalog API routes."""
import asyncio
import logging
import math
import time
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from google.api_core import exceptions as google_exceptions
from google.cloud import firestore
from typing import List, Optional, Dict, Any, Sequence
from pydantic import BaseModel, ConfigDict, Field

from app.core.auth import verify_api_key_or_admin, get_current_user, AuthenticatedUser
from app.db.firestore import get_db
from app.db.repositories.catalogue import catalogue_repo, CATALOGUE_META_ID
from app.db.repositories.usage import EXPORT_BYTES, QUERY_BYTES_BILLED, usage_repo
from app.core.config import get_settings
from app.core.rate_limiter import limiter
from app.core.certification_manager import get_all_certifications, get_certification_status
from app.utils import bigquery_ops
from app.utils.query_tracker import get_query_counts, increment_query_count

logger = logging.getLogger(__name__)

MAX_QUERY_ROWS = 10_000
MAX_PREVIEW_ROWS = 1_000


def _make_json_serializable(obj: Any) -> Any:
    """Recursively convert bytes, decimals and non-finite floats to JSON-serializable values."""
    if isinstance(obj, dict):
        return {k: _make_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_json_serializable(v) for v in obj]
    if isinstance(obj, bytes):
        return "<binary>"
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, Decimal):
        return float(obj) if obj.is_finite() else None
    return obj


async def _ensure_within_budget(db: firestore.AsyncClient, field: str, budget: int, what: str) -> None:
    if (await usage_repo.today(db))[field] >= budget:
        raise HTTPException(
            status_code=429,
            detail=f"The daily {what} budget of the API is used up; try again tomorrow (UTC).",
        )


async def _run_billed_query(
    db: firestore.AsyncClient,
    sql: str,
    *,
    endpoint: str,
    limit: int,
    offset: int = 0,
    parameters: Sequence[Any] = (),
) -> bigquery_ops.Page:
    """Run a BigQuery query within the daily bytes budget, mapping its errors to HTTP errors."""
    await _ensure_within_budget(db, QUERY_BYTES_BILLED, get_settings().query_daily_bytes_budget, "query")
    try:
        page = await asyncio.to_thread(
            bigquery_ops.run_query, sql, endpoint=endpoint, limit=limit, offset=offset, parameters=parameters
        )
    except bigquery_ops.QueryTimeout as exc:
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except bigquery_ops.QueryRejected as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    except (google_exceptions.BadRequest, google_exceptions.NotFound) as exc:
        raise HTTPException(status_code=400, detail=f"Query failed: {exc.message}") from exc
    except google_exceptions.GoogleAPICallError as exc:
        logger.error("BigQuery could not run the %s query: %s", endpoint, exc)
        raise HTTPException(status_code=503, detail="The query service is unavailable; try again later.") from exc
    if page.bytes_billed:
        await usage_repo.add(db, QUERY_BYTES_BILLED, page.bytes_billed)
    return page


def _as_dict_or_none(value: Any) -> Optional[Dict[str, Any]]:
    """Return dict payloads as-is; coerce other JSON types to None."""
    return value if isinstance(value, dict) else None


def _as_list(value: Any) -> List[Any]:
    """Return list payloads as-is; coerce other JSON types to an empty list."""
    return value if isinstance(value, list) else []


def _as_str_or_none(value: Any) -> Optional[str]:
    """Coerce scalar values to string for strict response models."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool, Decimal)):
        return str(value)
    return None


async def verify_table_access(
    layer: str,
    table_name: str,
    db: firestore.AsyncClient,
    current_user: AuthenticatedUser,
) -> None:
    """
    Verify that the user has access to the specified table.
    
    - Admins (current_user.is_admin) can access any table
    - Regular users can access all silver tables (certification temporarily disabled)
    - Bronze and gold tables require admin access
    
    Raises HTTPException if access is denied.
    """
    if current_user.is_admin:
        return
    
    # Only silver tables can be accessed by non-admins.
    if layer != "silver":
        raise HTTPException(
            status_code=403,
            detail=f"Access to {layer} tables requires admin privileges"
        )

    # Certification restriction temporarily disabled.
    # Re-enable this block when certified-only access is required again:
    # certified = await is_table_certified(layer, table_name, db)
    # if not certified:
    #     raise HTTPException(
    #         status_code=403,
    #         detail=f"Table {table_name} is not certified for public use. Please contact an administrator."
    #     )

async def load_catalogue_from_db(db: firestore.AsyncClient) -> Dict[str, Any]:
    """Build a compatibility in-memory view from V2 per-table catalogue documents."""
    try:
        rows = await catalogue_repo.list_table_rows(db)
        meta = await catalogue_repo.get_meta_row(db) or {}
        tables = {table_id: document for table_id, document in rows}
        logger.info("Loaded V2 catalogue documents from Firestore with %d tables", len(tables))
        return {"tables": tables, "_catalogue_meta": meta}
    except Exception as e:
        logger.error("Error loading catalogue from Firestore: %s", e)
        return {"tables": {}}


async def load_silver_catalog_rows(db: firestore.AsyncClient) -> List[tuple[str, Dict[str, Any]]]:
    """Load all table documents from the V2 data_catalogue collection."""
    return await catalogue_repo.list_table_rows(db)


async def load_catalogue_meta(db: firestore.AsyncClient) -> Dict[str, Any]:
    """Load the _catalogue_meta document from the V2 data_catalogue collection."""
    return await catalogue_repo.get_meta_row(db) or {}

router = APIRouter(prefix="/api/data", tags=["data"])


class TableInfo(BaseModel):
    """Table information."""
    name: str
    path: str
    version: int


class SchemaField(BaseModel):
    """Schema field information."""
    name: str
    type: str
    nullable: bool
    description: Optional[str] = None
    example: Optional[str] = None


class TableSchema(BaseModel):
    """Table schema information."""
    fields: List[SchemaField]
    version: int
    row_count: Optional[int]
    num_fields: int


class PreviewFilter(BaseModel):
    """Filter specification for table preview."""
    column: str
    operator: str = "="  # =, !=, contains, >, <, >=, <=
    value: str


class PreviewRequest(BaseModel):
    """Request for table preview."""
    limit: int = 100
    filters: Optional[List[PreviewFilter]] = None
    sort_by: Optional[str] = None
    sort_order: str = "asc"  # asc or desc


class PreviewResponse(BaseModel):
    """Table preview response."""
    columns: List[str]
    data: List[Dict[str, Any]]
    total_rows: int
    filtered_rows: int
    preview_rows: int


class CatalogResponse(BaseModel):
    """Catalog response with all schemas and tables."""
    schemas: Dict[str, List[TableInfo]]


class QueryRequest(BaseModel):
    """Request for SQL query execution."""
    sql: str
    limit: int = 1000
    offset: int = 0


class QueryResponse(BaseModel):
    """SQL query execution response."""
    columns: List[str]
    data: List[Dict[str, Any]]
    row_count: int
    offset: int
    has_more: bool
    execution_time_ms: float


class SilverTableInfo(BaseModel):
    """Silver table information with French description."""
    name: str
    actual_table_name: str  # The table name in the silver dataset (e.g., dim_commune)
    description_fr: str
    dependencies: List[str]
    version: int
    row_count: Optional[int]
    category: Optional[str] = None
    annee_reference: Optional[int] = None
    certified: bool = False
    certified_at: Optional[str] = None
    certified_by: Optional[str] = None
    query_count: Optional[int] = 0
    schema_drift: Optional[bool] = None
    drift_details: Optional[Dict[str, Any]] = None


class SilverCatalogResponse(BaseModel):
    """Catalog response for silver tables only."""
    tables: List[SilverTableInfo]
    last_synced: Optional[str] = None
    drift_report: Optional[Dict[str, Any]] = None


class CatalogueRefreshResponse(BaseModel):
    """Response from catalogue refresh operation."""
    status: str
    tables_synced: int
    last_synced: str
    version: str


class SourceInfo(BaseModel):
    """A raw data source that feeds a silver table."""
    name: str
    description: Optional[str] = None
    download_url: Optional[str] = None
    doc_url: Optional[str] = None


class SilverTableDetail(BaseModel):
    """Detailed information about a silver table."""
    model_config = ConfigDict(populate_by_name=True)
    name: str
    description_fr: str
    dependencies: List[str]
    tags: List[str] = []
    upstream_models: List[str] = []
    category: Optional[str] = None
    annee_reference: Optional[int] = None
    sources: List[SourceInfo] = []
    table_schema: TableSchema = Field(alias="schema")  # API key "schema"; avoids shadowing BaseModel.schema
    preview: List[Dict[str, Any]]
    certified: bool = False
    certified_at: Optional[str] = None
    certified_by: Optional[str] = None
    schema_drift: Optional[bool] = None
    drift_details: Optional[Dict[str, Any]] = None


@router.get("/catalog", response_model=CatalogResponse)
@limiter.limit("30/minute")
async def get_catalog(
    request: Request,
    user_id: str = Depends(verify_api_key_or_admin),
):
    """Get the complete data catalog with all schemas and tables.

    Each layer lists the tables of its BigQuery dataset (cached listing); a layer whose listing
    fails is returned empty.
    """
    schemas: Dict[str, List[TableInfo]] = {}
    for layer in bigquery_ops.LAYERS:
        try:
            names = await asyncio.to_thread(bigquery_ops.list_table_names, layer)
        except Exception as e:
            logger.warning("Could not list %s tables in BigQuery: %s", layer, e)
            names = []
        schemas[layer] = [TableInfo(name=name, path=f"{layer}/{name}", version=0) for name in names]
    return CatalogResponse(schemas=schemas)


@router.get("/catalog/silver", response_model=SilverCatalogResponse)
@limiter.limit("30/minute")
async def get_silver_catalog(
    request: Request,
    user_id: str = Depends(verify_api_key_or_admin),
    db: firestore.AsyncClient = Depends(get_db)
):
    """
    Get catalog of silver tables with French descriptions.
    
    Returns all silver layer tables with their French descriptions, dependencies,
    and basic metadata. Uses direct table names (dim_*, fact_*).
    
    Reads entirely from the Firestore catalogue (cached per instance) for maximum speed.
    """
    logger.info("Fetching silver catalog with French descriptions")

    try:
        # Load catalogue from Firestore — single source of truth (written by dbt)
        rows = await load_silver_catalog_rows(db)
        meta = await load_catalogue_meta(db)
        certifications = {
            cert["table_name"]: cert
            for cert in await get_all_certifications(db)
            if cert.get("layer") == "silver"
        }
        query_counts = await get_query_counts(db)

        tables = []
        for table_name, catalogue_info in sorted(rows, key=lambda row: row[0]):
            business_metadata = _as_dict_or_none(catalogue_info.get("business_metadata")) or {}
            schema_cache = _as_dict_or_none(catalogue_info.get("schema_cache")) or {}
            row_count = schema_cache.get("row_count")
            version = schema_cache.get("version", 0)
            schema_drift_raw = schema_cache.get("schema_drift")
            schema_drift = bool(schema_drift_raw) if schema_drift_raw is not None else None
            drift_details = _as_dict_or_none(schema_cache.get("drift_details"))
            if schema_cache.get("drift_details") is not None and drift_details is None:
                logger.warning("Ignoring non-dict drift_details for table '%s'", table_name)

            cert_status = certifications.get(table_name)
            query_count = query_counts.get(f"silver_{table_name}", 0)

            tables.append(SilverTableInfo(
                name=table_name,
                actual_table_name=table_name,
                description_fr=(
                    business_metadata.get("description")
                    or "Description non disponible"
                ),
                dependencies=_as_list(business_metadata.get("upstream_models")),
                version=version,
                row_count=row_count,
                category=business_metadata.get("category"),
                annee_reference=business_metadata.get("annee_reference"),
                certified=cert_status is not None and cert_status.get("certified", False),
                certified_at=cert_status.get("certified_at") if cert_status else None,
                certified_by=cert_status.get("certified_by") if cert_status else None,
                query_count=query_count,
                schema_drift=schema_drift,
                drift_details=drift_details,
            ))

        last_synced = (
            meta.get("catalog_generated_at")
            or meta.get("dbt_manifest_generated_at")
            or (rows[0][1].get("catalog_generated_at") if rows else None)
        )
        drift_report = _as_dict_or_none(meta.get("drift_report"))
        if meta.get("drift_report") is not None and drift_report is None:
            logger.warning("Ignoring non-dict drift_report from _catalogue_meta")
        return SilverCatalogResponse(
            tables=tables,
            last_synced=last_synced,
            drift_report=drift_report,
        )
    
    except Exception as e:
        logger.error(f"Error fetching silver catalog: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch silver catalog: {str(e)}")


@router.get("/catalog/silver/{table_name}", response_model=SilverTableDetail)
@limiter.limit("30/minute")
async def get_silver_table_detail(
    request: Request,
    table_name: str,
    user_id: str = Depends(verify_api_key_or_admin),
    db: firestore.AsyncClient = Depends(get_db)
):
    """
    Get detailed information about a specific silver table.
    
    Returns table metadata, schema, French description, dependencies,
    and first 10 rows of data. All data is cached in Firestore for fast access.
    
    Args:
        table_name: Name of the silver table
    """
    logger.info(f"Fetching details for silver.{table_name}")

    try:
        table_catalogue = await catalogue_repo.get_table_row(db, table_name)

        if not table_catalogue:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found in catalogue")

        field_docs = _as_dict_or_none(table_catalogue.get("field_docs")) or {}
        schema_cache = _as_dict_or_none(table_catalogue.get("schema_cache")) or {}
        cached_fields = _as_list(schema_cache.get("fields"))

        schema_fields: List[SchemaField] = []
        for field in cached_fields:
            if not isinstance(field, dict):
                continue
            field_name = field.get("name")
            if not field_name:
                continue
            doc_for_field = _as_dict_or_none(field_docs.get(field_name)) or {}
            schema_fields.append(
                SchemaField(
                    name=field_name,
                    type=str(field.get("type") or "unknown"),
                    nullable=bool(field.get("nullable", True)),
                    description=_as_str_or_none(doc_for_field.get("description")),
                    example=_as_str_or_none(doc_for_field.get("example")),
                )
            )

        # Fallback when cached schema fields are missing: show documented fields only.
        if not schema_fields:
            for field_name, doc in field_docs.items():
                doc_for_field = _as_dict_or_none(doc) or {}
                schema_fields.append(
                    SchemaField(
                        name=field_name,
                        type="unknown",
                        nullable=True,
                        description=_as_str_or_none(doc_for_field.get("description")),
                        example=_as_str_or_none(doc_for_field.get("example")),
                    )
                )

        table_schema = TableSchema(
            fields=schema_fields,
            version=int(schema_cache.get("version") or 0),
            row_count=schema_cache.get("row_count"),
            num_fields=int(schema_cache.get("num_fields") or len(schema_fields)),
        )

        # Get preview from cached data (ensure JSON-serializable for response)
        runtime_hints = table_catalogue.get("runtime_hints", {})
        preview_data = _make_json_serializable(
            runtime_hints.get("preview")
            or runtime_hints.get("preview_rows")
            or []
        )
        
        # Get certification status
        cert_status = await get_certification_status("silver", table_name, db)
        
        business_metadata = _as_dict_or_none(table_catalogue.get("business_metadata")) or {}
        raw_sources = _as_list(business_metadata.get("sources"))
        drift_details = _as_dict_or_none(schema_cache.get("drift_details"))
        if schema_cache.get("drift_details") is not None and drift_details is None:
            logger.warning("Ignoring non-dict drift_details for table detail '%s'", table_name)
        sources = [
            SourceInfo(
                # Accept both YAML convention (name) and pipeline catalogue convention (source_key)
                name=s.get("name") or s.get("source_key", ""),
                description=s.get("description"),
                # Accept both YAML convention (download_url) and pipeline catalogue convention (url)
                download_url=s.get("download_url") or s.get("url"),
                doc_url=s.get("doc_url"),
            )
            for s in raw_sources
            if isinstance(s, dict)
        ]

        return SilverTableDetail(
            name=table_name,
            description_fr=(
                business_metadata.get("description")
                or "Description non disponible"
            ),
            dependencies=_as_list(business_metadata.get("upstream_models")),
            tags=_as_list(business_metadata.get("tags")),
            upstream_models=_as_list(business_metadata.get("upstream_models")),
            category=business_metadata.get("category"),
            annee_reference=business_metadata.get("annee_reference"),
            sources=sources,
            table_schema=table_schema,
            preview=preview_data,
            certified=cert_status is not None and cert_status.get("certified", False),
            certified_at=cert_status.get("certified_at") if cert_status else None,
            certified_by=cert_status.get("certified_by") if cert_status else None,
            schema_drift=bool(schema_cache.get("schema_drift")) if schema_cache.get("schema_drift") is not None else None,
            drift_details=drift_details,
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching table detail: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch table detail: {str(e)}")


@router.get("/table/{layer}/{table}", response_model=TableSchema)
@limiter.limit("60/minute")
async def get_table_metadata(
    request: Request,
    layer: str,
    table: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
    db: firestore.AsyncClient = Depends(get_db),
):
    """
    Get metadata for a specific table including schema and row count.
    
    Args:
        layer: Layer name (bronze, silver, gold)
        table: Table name
    """
    if layer not in bigquery_ops.LAYERS:
        raise HTTPException(status_code=400, detail="Layer must be bronze, silver, or gold")
    await verify_table_access(layer, table, db, current_user)

    try:
        bq_table = await asyncio.to_thread(bigquery_ops.get_table, layer, table)
    except Exception as e:
        logger.error("Error fetching table metadata for %s.%s: %s", layer, table, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch table metadata: {str(e)}")
    if bq_table is None:
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")

    fields = [SchemaField(**field) for field in bigquery_ops.schema_fields(bq_table)]
    return TableSchema(fields=fields, version=0, row_count=bq_table.num_rows, num_fields=len(fields))


@router.post("/preview/{layer}/{table}", response_model=PreviewResponse)
@limiter.limit("60/minute")
async def preview_table(
    request: Request,
    layer: str,
    table: str,
    preview_req: PreviewRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
    db: firestore.AsyncClient = Depends(get_db)
):
    """
    Get a preview of table data with optional filtering and sorting.
    
    Args:
        layer: Layer name (bronze, silver, gold)
        table: Table name
        preview_req: Preview request with filters and sort options
    """
    logger.info(f"Previewing {layer}.{table} with filters={preview_req.filters}, sort={preview_req.sort_by}")
    if layer not in bigquery_ops.LAYERS:
        raise HTTPException(status_code=400, detail="Layer must be bronze, silver, or gold")
    await verify_table_access(layer, table, db, current_user)

    # Silver: serve the 10-row cached preview from the Firestore catalogue, without reading BigQuery.
    # Filters and sort are ignored for the cached version (same 10 rows used in the catalogue modal).
    if layer == "silver":
        table_doc = await catalogue_repo.get_table_row(db, table) or {}
        schema_cache = table_doc.get("schema_cache", {})
        runtime_hints = table_doc.get("runtime_hints", {})
        cached = _make_json_serializable(
            runtime_hints.get("preview")
            or runtime_hints.get("preview_rows")
            or []
        )
        if cached:
            sliced = cached[: preview_req.limit]
            columns = list(sliced[0].keys()) if sliced else []
            return PreviewResponse(
                columns=columns,
                data=sliced,
                total_rows=schema_cache.get("row_count") or len(cached),
                filtered_rows=len(sliced),
                preview_rows=len(sliced),
            )

    try:
        bq_table = await asyncio.to_thread(bigquery_ops.get_table, layer, table)
    except Exception as e:
        logger.error("Error previewing %s.%s: %s", layer, table, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to preview table: {str(e)}")
    if bq_table is None:
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")

    limit = max(min(preview_req.limit, MAX_PREVIEW_ROWS), 0)
    total_rows = bq_table.num_rows or 0
    if not preview_req.filters and not preview_req.sort_by:
        page = await asyncio.to_thread(bigquery_ops.head, bq_table, limit)
        return PreviewResponse(
            columns=page.columns,
            data=_make_json_serializable(page.rows),
            total_rows=total_rows,
            filtered_rows=total_rows,
            preview_rows=len(page.rows),
        )

    try:
        sql, parameters = bigquery_ops.preview_query(
            bq_table, preview_req.filters or [], preview_req.sort_by, preview_req.sort_order, limit
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    page = await _run_billed_query(db, sql, endpoint="preview", limit=limit, parameters=parameters)
    filtered_rows = int(page.rows[0][bigquery_ops.FILTERED_ROWS_COLUMN]) if page.rows else 0
    for row in page.rows:
        row.pop(bigquery_ops.FILTERED_ROWS_COLUMN, None)
    return PreviewResponse(
        columns=[column for column in page.columns if column != bigquery_ops.FILTERED_ROWS_COLUMN],
        data=_make_json_serializable(page.rows),
        total_rows=total_rows,
        filtered_rows=filtered_rows,
        preview_rows=len(page.rows),
    )


@router.post("/query", response_model=QueryResponse)
@limiter.limit("60/minute")
async def execute_sql_query(
    request: Request,
    query_req: QueryRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
    db: firestore.AsyncClient = Depends(get_db)
):
    """Run a BigQuery SQL (GoogleSQL) SELECT query with cursor-style pagination.

    Only a single SELECT statement is accepted. Silver tables are referenced by name (`dim_commune`)
    or as `odas_silver.dim_commune`; admins may also read `odas_bronze.<table>` and `odas_gold.<table>`.
    Results are capped at min(requested limit, 10 000) rows per page. Use `offset` to paginate:
    BigQuery caches the result, so the next pages of the same SQL are not billed again.
    `has_more=true` in the response indicates another page exists.
    """
    allowed_layers = bigquery_ops.LAYERS if current_user.is_admin else ("silver",)
    try:
        prepared = await asyncio.to_thread(bigquery_ops.prepare_user_query, query_req.sql, allowed_layers)
    except bigquery_ops.QueryRejected as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    limit = max(min(query_req.limit, MAX_QUERY_ROWS), 0)
    offset = max(query_req.offset, 0)
    start = time.monotonic()
    page = await _run_billed_query(db, prepared.sql, endpoint="query", limit=limit, offset=offset)
    elapsed_ms = (time.monotonic() - start) * 1000

    for layer, table_name in prepared.tables:
        await increment_query_count(db=db, table_name=f"{layer}_{table_name}", user_id=current_user.user_id)
    return QueryResponse(
        columns=page.columns,
        data=_make_json_serializable(page.rows),
        row_count=len(page.rows),
        offset=offset,
        has_more=offset + len(page.rows) < (page.total_rows or 0),
        execution_time_ms=elapsed_ms,
    )


@router.get("/export/{table}", tags=["data"])
@limiter.limit("10/minute")
async def export_table(
    request: Request,
    table: str,
    format: str = "csv",
    current_user: AuthenticatedUser = Depends(get_current_user),
    db: firestore.AsyncClient = Depends(get_db),
):
    """Stream a full silver table as CSV or Parquet, read from BigQuery without a query.

    Any user can export any silver table with no row limit, within the API's daily export budget.
    Pass `?format=parquet` to receive a Parquet file instead of CSV.
    """
    if format not in ("csv", "parquet"):
        raise HTTPException(status_code=400, detail="format must be 'csv' or 'parquet'.")
    await verify_table_access("silver", table, db, current_user)

    try:
        bq_table = await asyncio.to_thread(bigquery_ops.get_table, "silver", table)
    except Exception as e:
        logger.error("Export failed for table %s: %s", table, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to export table: {str(e)}")
    if bq_table is None:
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")

    await _ensure_within_budget(db, EXPORT_BYTES, get_settings().export_daily_bytes_budget, "export")
    try:
        schema, batches = await asyncio.to_thread(bigquery_ops.table_batches, bq_table)
    except Exception as e:
        logger.error("Export failed for table %s: %s", table, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to export table: {str(e)}")
    await usage_repo.add(db, EXPORT_BYTES, bq_table.num_bytes or 0)

    if format == "parquet":
        return StreamingResponse(
            bigquery_ops.parquet_chunks(schema, batches),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={table}.parquet"},
        )
    return StreamingResponse(
        bigquery_ops.csv_chunks(schema, batches),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={table}.csv"},
    )


@router.post("/catalog/refresh", response_model=CatalogueRefreshResponse)
@limiter.limit("10/minute")
async def refresh_catalogue(
    request: Request,
    user_id: str = Depends(verify_api_key_or_admin),
    db: firestore.AsyncClient = Depends(get_db)
):
    """Reload the silver catalogue from the Firestore data_catalogue collection.

    The pipeline project (dbt + manifest loader) is the single source of truth —
    it writes descriptions, tags, schema, preview and row_count after each run.
    This endpoint drops this instance's catalogue cache, stamps last_synced on the
    meta document, and returns the number of tables now visible in the catalogue.
    No BigQuery reads, no YAML reads. Completes in < 1 s.
    """
    try:
        from datetime import datetime, timezone

        catalogue_repo.invalidate_cache()
        existing_doc = await catalogue_repo.get_meta_row(db)
        if not existing_doc:
            raise HTTPException(
                status_code=404,
                detail=f"No {CATALOGUE_META_ID} document found in Firestore. Run the pipeline first.",
            )

        sync_time = datetime.now(timezone.utc)
        existing_doc["catalog_generated_at"] = sync_time.isoformat()
        await catalogue_repo.upsert_meta_row(db, existing_doc)

        num_tables = len(await load_silver_catalog_rows(db))
        logger.info("Catalogue refreshed: %d tables, last_synced=%s", num_tables, sync_time.isoformat())

        return CatalogueRefreshResponse(
            status="success",
            tables_synced=num_tables,
            last_synced=sync_time.isoformat(),
            version=existing_doc.get("version", "unknown"),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to refresh catalogue: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to refresh catalogue: {str(e)}",
        )

