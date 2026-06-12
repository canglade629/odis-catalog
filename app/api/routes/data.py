"""Data catalog API routes."""
import io
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from decimal import Decimal
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import AsyncIterator, List, Optional, Dict, Any
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession
import numpy as np

from app.core.auth import verify_api_key, verify_admin_secret, verify_api_key_or_admin, get_current_user, AuthenticatedUser
from app.db.session import get_db
from app.db.repositories.catalogue import catalogue_repo
from app.core.config import get_settings
from app.utils.delta_ops import DeltaOperations, find_latest_metadata
from app.utils.sql_executor import SQLExecutor
from app.core.rate_limiter import limiter
from app.core.certification_manager import get_certification_status

logger = logging.getLogger(__name__)

security = HTTPBearer(auto_error=False)

MAX_TABLES_PER_QUERY = 10
QUERY_TIMEOUT_SECONDS = 60


def _make_json_serializable(obj: Any) -> Any:
    """Recursively convert bytes, numpy types, etc. to JSON-serializable values."""
    if isinstance(obj, dict):
        return {k: _make_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_json_serializable(v) for v in obj]
    if isinstance(obj, bytes):
        return "<binary>"
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj) if np.isfinite(obj) else None
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return _make_json_serializable(obj.tolist())
    if isinstance(obj, Decimal):
        return float(obj) if obj.is_finite() else None
    return obj


def _is_select_query(sql: str) -> bool:
    """Return True only for SELECT or WITH (CTE) statements; block DDL/DML."""
    first_word = sql.strip().split()[0].upper() if sql.strip() else ""
    return first_word in ("SELECT", "WITH")


def _extract_table_names(sql: str, known_tables: List[str]) -> List[str]:
    """Extract referenced table names by intersecting SQL tokens with known table names."""
    tokens = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", sql.lower()))
    lower_to_actual = {name.lower(): name for name in known_tables}
    matched = [lower_to_actual[token] for token in tokens if token in lower_to_actual]
    return sorted(set(matched))


def _load_certified_silver_tables(executor: SQLExecutor, certified_names: list, settings: Any) -> None:
    """Register certified silver Delta tables into a DuckDB executor instance."""
    for name in certified_names:
        try:
            executor.register_delta_table(name, settings.get_silver_path(name))
        except Exception as e:
            logger.warning("Could not register silver table %s for query: %s", name, e)


def _load_all_tables(executor: SQLExecutor, catalogue: Dict[str, Any], settings: Any) -> None:
    """Register all catalogue tables (all layers) into a DuckDB executor instance."""
    for name in catalogue.get("tables", {}):
        for get_path in (settings.get_silver_path, settings.get_bronze_path, settings.get_gold_path):
            try:
                executor.register_delta_table(name, get_path(name))
                break
            except Exception:
                continue


async def verify_table_access(
    layer: str,
    table_name: str,
    session: AsyncSession,
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
    # certified = await is_table_certified(layer, table_name, session)
    # if not certified:
    #     raise HTTPException(
    #         status_code=403,
    #         detail=f"Table {table_name} is not certified for public use. Please contact an administrator."
    #     )

async def load_catalogue_from_db(session: AsyncSession) -> Dict[str, Any]:
    """Load the data catalogue from PostgreSQL. Falls back to empty if not found."""
    try:
        data = await catalogue_repo.get(session)
        if data:
            logger.info("Loaded catalogue from DB with %d tables", len(data.get("tables", {})))
            return data
        logger.warning("Catalogue document not found in DB")
        return {"tables": {}}
    except Exception as e:
        logger.error("Error loading catalogue from DB: %s", e)
        return {"tables": {}}

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
    actual_table_name: str  # The actual Delta table name (e.g., dim_commune)
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


class SilverCatalogResponse(BaseModel):
    """Catalog response for silver tables only."""
    tables: List[SilverTableInfo]
    last_synced: Optional[str] = None


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


@router.get("/catalog", response_model=CatalogResponse)
@limiter.limit("30/minute")
async def get_catalog(
    request: Request,
    user_id: str = Depends(verify_api_key_or_admin),
    session: AsyncSession = Depends(get_db),
):
    """Get the complete data catalog with all schemas and tables.

    Silver tables are read from the data_catalogue PostgreSQL document.
    Bronze tables are read from the iceberg_tables catalog table (PyIceberg SqlCatalog).
    Gold is not yet populated and returns an empty list.
    No S3 reads — all data comes from PostgreSQL.
    """
    logger.info("Fetching data catalog from PostgreSQL")
    from sqlalchemy import text

    try:
        # Silver: from pipeline-written data_catalogue document
        catalogue = await load_catalogue_from_db(session)
        silver_tables = [
            TableInfo(name=name, path=f"silver/{name}", version=0)
            for name in sorted(catalogue.get("tables", {}).keys())
        ]

        # Bronze: from PyIceberg's SqlCatalog (iceberg_tables)
        bronze_tables = []
        try:
            result = await session.execute(
                text(
                    "SELECT table_name FROM iceberg_tables "
                    "WHERE table_namespace = 'bronze' ORDER BY table_name"
                )
            )
            bronze_tables = [
                TableInfo(name=row[0], path=f"bronze/{row[0]}", version=0)
                for row in result
            ]
        except Exception as e:
            logger.warning("Could not read bronze tables from iceberg_tables: %s", e)

        return CatalogResponse(schemas={"bronze": bronze_tables, "silver": silver_tables, "gold": []})

    except Exception as e:
        logger.error("Error fetching catalog: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch catalog: {str(e)}")


@router.get("/catalog/silver", response_model=SilverCatalogResponse)
@limiter.limit("30/minute")
async def get_silver_catalog(
    request: Request,
    user_id: str = Depends(verify_api_key_or_admin),
    session: AsyncSession = Depends(get_db)
):
    """
    Get catalog of silver tables with French descriptions.
    
    Returns all silver layer tables with their French descriptions, dependencies,
    and basic metadata. Uses direct table names (dim_*, fact_*).
    
    Reads entirely from PostgreSQL catalogue cache for maximum speed.
    """
    logger.info("Fetching silver catalog with French descriptions")

    try:
        # Import query tracker
        from app.utils.query_tracker import get_table_query_count

        # Load catalogue from PostgreSQL — single source of truth (written by dbt)
        catalogue = await load_catalogue_from_db(session)
        catalogue_tables = catalogue.get('tables', {})

        tables = []
        for table_name, catalogue_info in sorted(catalogue_tables.items()):
            row_count = catalogue_info.get("row_count")
            version = catalogue_info.get("schema", {}).get("version", 0)

            # Get certification status from PostgreSQL (fast)
            cert_status = await get_certification_status("silver", table_name, session)

            # Get query count from PostgreSQL
            query_count = await get_table_query_count(session, f"silver_{table_name}")

            tables.append(SilverTableInfo(
                name=table_name,
                actual_table_name=table_name,
                description_fr=(
                    catalogue_info.get("description")
                    or "Description non disponible"
                ),
                dependencies=catalogue_info.get("upstream_models") or [],
                version=version,
                row_count=row_count,
                category=catalogue_info.get("category"),
                annee_reference=catalogue_info.get("annee_reference"),
                certified=cert_status is not None and cert_status.get("certified", False),
                certified_at=cert_status.get("certified_at") if cert_status else None,
                certified_by=cert_status.get("certified_by") if cert_status else None,
                query_count=query_count
            ))

        last_synced = catalogue.get("last_synced") or catalogue.get("generated_at")
        return SilverCatalogResponse(tables=tables, last_synced=last_synced)
    
    except Exception as e:
        logger.error(f"Error fetching silver catalog: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch silver catalog: {str(e)}")


@router.get("/catalog/silver/{table_name}", response_model=SilverTableDetail)
@limiter.limit("30/minute")
async def get_silver_table_detail(
    request: Request,
    table_name: str,
    user_id: str = Depends(verify_api_key_or_admin),
    session: AsyncSession = Depends(get_db)
):
    """
    Get detailed information about a specific silver table.
    
    Returns table metadata, schema, French description, dependencies,
    and first 10 rows of data. All data is cached in PostgreSQL for fast access.
    
    Args:
        table_name: Name of the silver table
    """
    logger.info(f"Fetching details for silver.{table_name}")

    try:
        # Load enriched catalogue from PostgreSQL (includes schema + preview)
        catalogue = await load_catalogue_from_db(session)
        logger.info(f"Loaded enriched catalogue from PostgreSQL with {len(catalogue.get('tables', {}))} tables")

        table_catalogue = catalogue.get("tables", {}).get(table_name)

        if not table_catalogue:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found in catalogue")
        
        # Extract schema from cached data
        cached_schema = table_catalogue.get('schema', {})
        field_descriptions = table_catalogue.get("fields", {})
        
        # Build schema with descriptions merged in
        fields = []
        for field in cached_schema.get('fields', []):
            field_name = field['name']
            field_desc_info = field_descriptions.get(field_name, {})
            
            fields.append(SchemaField(
                name=field_name,
                type=field['type'],
                nullable=field['nullable'],
                description=field_desc_info.get("description"),
                example=str(field_desc_info.get("example", "")) if field_desc_info.get("example") else None
            ))
        
        table_schema = TableSchema(
            fields=fields,
            version=cached_schema.get('version', 0),
            row_count=table_catalogue.get('row_count'),
            num_fields=cached_schema.get('num_fields', len(fields))
        )
        
        # Get preview from cached data (ensure JSON-serializable for response)
        preview_data = _make_json_serializable(table_catalogue.get('preview', []))
        
        # Get certification status
        cert_status = await get_certification_status("silver", table_name, session)
        
        raw_sources = table_catalogue.get("sources") or []
        sources = [
            SourceInfo(
                # Accept both YAML convention (name) and Postgres DBT convention (source_key)
                name=s.get("name") or s.get("source_key", ""),
                description=s.get("description"),
                # Accept both YAML convention (download_url) and Postgres DBT convention (url)
                download_url=s.get("download_url") or s.get("url"),
                doc_url=s.get("doc_url"),
            )
            for s in raw_sources
            if isinstance(s, dict)
        ]

        return SilverTableDetail(
            name=table_name,
            description_fr=(
                table_catalogue.get("description")
                or "Description non disponible"
            ),
            dependencies=table_catalogue.get("upstream_models") or [],
            tags=table_catalogue.get("tags") or [],
            upstream_models=table_catalogue.get("upstream_models") or [],
            category=table_catalogue.get("category"),
            annee_reference=table_catalogue.get("annee_reference"),
            sources=sources,
            table_schema=table_schema,
            preview=preview_data,
            certified=cert_status is not None and cert_status.get("certified", False),
            certified_at=cert_status.get("certified_at") if cert_status else None,
            certified_by=cert_status.get("certified_by") if cert_status else None
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
    user_id: str = Depends(verify_api_key_or_admin)
):
    """
    Get metadata for a specific table including schema and row count.
    
    Args:
        layer: Layer name (bronze, silver, gold)
        table: Table name
    """
    logger.info(f"Fetching metadata for {layer}.{table}")
    settings = get_settings()
    
    # Validate layer
    if layer not in ["bronze", "silver", "gold"]:
        raise HTTPException(status_code=400, detail="Layer must be bronze, silver, or gold")
    
    # Construct table path
    if layer == "silver":
        table_path = settings.get_silver_path(table)
    elif layer == "bronze":
        table_path = settings.get_bronze_path(table)
    else:
        table_path = settings.get_gold_path(table)
    
    try:
        schema_info = DeltaOperations.get_table_schema(table_path)
        
        return TableSchema(
            fields=[
                SchemaField(
                    name=field["name"],
                    type=field["type"],
                    nullable=field["nullable"]
                )
                for field in schema_info["fields"]
            ],
            version=schema_info["version"],
            row_count=schema_info["row_count"],
            num_fields=schema_info["num_fields"]
        )
    
    except Exception as e:
        logger.error(f"Error fetching table metadata: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch table metadata: {str(e)}")


@router.post("/preview/{layer}/{table}", response_model=PreviewResponse)
@limiter.limit("60/minute")
async def preview_table(
    request: Request,
    layer: str,
    table: str,
    preview_req: PreviewRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_db)
):
    """
    Get a preview of table data with optional filtering and sorting.
    
    Args:
        layer: Layer name (bronze, silver, gold)
        table: Table name
        preview_req: Preview request with filters and sort options
    """
    logger.info(f"Previewing {layer}.{table} with filters={preview_req.filters}, sort={preview_req.sort_by}")
    settings = get_settings()
    
    # Validate layer
    if layer not in ["bronze", "silver", "gold"]:
        raise HTTPException(status_code=400, detail="Layer must be bronze, silver, or gold")
    
    # Check access permissions
    await verify_table_access(layer, table, session, current_user)

    # Silver: serve the 10-row cached preview from PostgreSQL — fast and no S3 reads.
    # Filters and sort are ignored for the cached version (same 10 rows used in the catalogue modal).
    if layer == "silver":
        catalogue = await load_catalogue_from_db(session)
        table_doc = catalogue.get("tables", {}).get(table, {})
        cached = _make_json_serializable(table_doc.get("preview", []))
        if cached:
            sliced = cached[: preview_req.limit]
            columns = list(sliced[0].keys()) if sliced else []
            return PreviewResponse(
                columns=columns,
                data=sliced,
                total_rows=table_doc.get("row_count") or len(cached),
                filtered_rows=len(sliced),
                preview_rows=len(sliced),
            )

    # Bronze / Gold (admin only): fall back to live Iceberg scan
    # Construct table path
    if layer == "silver":
        table_path = settings.get_silver_path(table)
    elif layer == "bronze":
        table_path = settings.get_bronze_path(table)
    else:
        table_path = settings.get_gold_path(table)
    
    try:
        # Convert Pydantic models to dicts
        filters_dict = None
        if preview_req.filters:
            filters_dict = [f.dict() for f in preview_req.filters]
        
        preview_data = DeltaOperations.preview_table(
            table_path=table_path,
            limit=preview_req.limit,
            filters=filters_dict,
            sort_by=preview_req.sort_by,
            sort_order=preview_req.sort_order
        )
        
        return PreviewResponse(
            columns=preview_data["columns"],
            data=preview_data["data"],
            total_rows=preview_data["total_rows"],
            filtered_rows=preview_data["filtered_rows"],
            preview_rows=preview_data["preview_rows"]
        )
    
    except Exception as e:
        logger.error(f"Error previewing table: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to preview table: {str(e)}")


@router.post("/query", response_model=QueryResponse)
@limiter.limit("60/minute")
async def execute_sql_query(
    request: Request,
    query_req: QueryRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_db)
):
    """Execute a SQL SELECT query against the data lake with cursor-style pagination.

    Regular users may only run SELECT statements against silver tables.
    Admin users may run any SQL against all available tables.
    Results are capped at min(requested limit, 10 000) rows per page.
    Use `offset` to paginate through large result sets.
    `has_more=true` in the response indicates another page exists.
    """
    if not current_user.is_admin and not _is_select_query(query_req.sql):
        raise HTTPException(
            status_code=403,
            detail="Only SELECT queries are allowed for non-admin users.",
        )

    settings = get_settings()
    executor = SQLExecutor()
    try:
        catalogue = await load_catalogue_from_db(session)
        all_silver_names = list(catalogue.get("tables", {}).keys())
        referenced_tables = _extract_table_names(query_req.sql, all_silver_names)

        if len(referenced_tables) > MAX_TABLES_PER_QUERY:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Query references {len(referenced_tables)} tables "
                    f"(max allowed is {MAX_TABLES_PER_QUERY})."
                ),
            )

        if not current_user.is_admin:
            disallowed = [name for name in referenced_tables if name not in all_silver_names]
            if disallowed:
                raise HTTPException(
                    status_code=403,
                    detail=f"Only silver tables are allowed for non-admin users: {', '.join(disallowed)}",
                )

        s3_config = {
            "endpoint": settings.scw_object_storage_endpoint,
            "access_key_id": settings.scw_access_key,
            "secret_access_key": settings.scw_secret_key,
            "region": settings.scw_region,
        }

        for table_name in referenced_tables:
            table_path = settings.get_silver_path(table_name)
            metadata_path = find_latest_metadata(table_path)
            if not metadata_path:
                raise HTTPException(
                    status_code=404,
                    detail=f"Could not find Iceberg metadata for table: {table_name}",
                )
            executor.register_iceberg_view(table_name, metadata_path, s3_config)

        limit = min(query_req.limit, 10_000)
        offset = max(query_req.offset, 0)
        # Fetch limit+1 rows to detect whether more pages exist without a COUNT(*)
        wrapped_sql = (
            f"SELECT * FROM ({query_req.sql}) __q"
            f" LIMIT {limit + 1} OFFSET {offset}"
        )

        start = time.time()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(executor.execute_query, wrapped_sql)
            try:
                result_df = future.result(timeout=QUERY_TIMEOUT_SECONDS)
            except FuturesTimeout:
                raise HTTPException(
                    status_code=504,
                    detail=f"Query timed out after {QUERY_TIMEOUT_SECONDS}s.",
                )
        elapsed_ms = (time.time() - start) * 1000

        has_more = len(result_df) > limit
        result_df = result_df.head(limit)

        records = _make_json_serializable(result_df.to_dict(orient="records"))
        return QueryResponse(
            columns=list(result_df.columns),
            data=records,
            row_count=len(records),
            offset=offset,
            has_more=has_more,
            execution_time_ms=elapsed_ms,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("SQL query execution failed: %s", e, exc_info=True)
        raise HTTPException(status_code=400, detail=f"Query failed: {str(e)}")
    finally:
        executor.close()


@router.get("/export/{table}", tags=["data"])
@limiter.limit("10/minute")
async def export_table(
    request: Request,
    table: str,
    format: str = "csv",
    current_user: AuthenticatedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_db),
):
    """Stream a full certified silver table as CSV or Parquet.

    Regular users can export any silver table with no row limit.
    Admin users can export any table.
    Pass `?format=parquet` to receive a Parquet file instead of CSV.
    """
    if format not in ("csv", "parquet"):
        raise HTTPException(status_code=400, detail="format must be 'csv' or 'parquet'.")

    # Certification restriction temporarily disabled — all silver tables are accessible.
    # Admins keep unrestricted access across all layers; regular users are limited to silver.
    if not current_user.is_admin:
        await verify_table_access("silver", table, session, current_user)

    settings = get_settings()
    table_path = settings.get_silver_path(table)

    try:
        df = DeltaOperations.read_delta(table_path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")
    except Exception as e:
        logger.error("Export failed for table %s: %s", table, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to read table: {str(e)}")

    if format == "parquet":
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={table}.parquet"},
        )

    # CSV — stream in 10 000-row chunks to keep memory flat
    def _csv_chunks():
        header_written = False
        chunk_size = 10_000
        for start in range(0, max(len(df), 1), chunk_size):
            chunk = df.iloc[start : start + chunk_size]
            buf = io.StringIO()
            chunk.to_csv(buf, index=False, header=not header_written)
            header_written = True
            yield buf.getvalue()

    return StreamingResponse(
        _csv_chunks(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={table}.csv"},
    )


@router.post("/catalog/refresh", response_model=CatalogueRefreshResponse)
@limiter.limit("10/minute")
async def refresh_catalogue(
    request: Request,
    user_id: str = Depends(verify_api_key_or_admin),
    session: AsyncSession = Depends(get_db)
):
    """Reload the silver catalogue from the PostgreSQL data_catalogue document.

    The pipeline project (dbt + manifest loader) is the single source of truth —
    it writes descriptions, tags, schema, preview and row_count after each run.
    This endpoint reads the current document, stamps last_synced, and returns the
    number of tables now visible in the catalogue. The updated count is immediately
    reflected in GET /catalog/silver since that endpoint reads directly from Postgres.
    No S3 reads, no YAML reads. Completes in < 1 s.
    """
    try:
        from datetime import datetime, timezone

        existing_doc = await catalogue_repo.get(session)
        if not existing_doc:
            raise HTTPException(
                status_code=404,
                detail="No catalogue document found in PostgreSQL. Run the pipeline first.",
            )

        sync_time = datetime.now(timezone.utc)
        existing_doc["last_synced"] = sync_time.isoformat()
        await catalogue_repo.set(session, existing_doc)

        num_tables = len(existing_doc.get("tables", {}))
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

