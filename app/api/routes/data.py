"""Data catalog API routes."""
import io
import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import AsyncIterator, List, Optional, Dict, Any
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession
import numpy as np
import pandas as pd
import sqlglot

from app.core.auth import verify_api_key, verify_admin_secret, verify_api_key_or_admin, get_current_user, AuthenticatedUser
from app.db.session import get_db
from app.db.repositories.catalogue import catalogue_repo, CATALOGUE_META_ID
from app.core.config import get_settings
from app.utils.iceberg_ops import find_latest_metadata
from app.utils.sql_executor import get_sql_executor
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
    """Extract referenced table names by parsing SQL with sqlglot."""
    lower_to_actual = {name.lower(): name for name in known_tables}
    parsed = sqlglot.parse_one(sql, read="duckdb")
    referenced = set()
    for table in parsed.find_all(sqlglot.exp.Table):
        table_name = table.name.lower()
        if table_name in lower_to_actual:
            referenced.add(lower_to_actual[table_name])
    return sorted(referenced)


def _build_s3_config(settings: Any) -> Dict[str, str]:
    return {
        "endpoint": settings.scw_object_storage_endpoint,
        "access_key_id": settings.scw_access_key,
        "secret_access_key": settings.scw_secret_key,
        "region": settings.scw_region,
    }


def _sanitize_preview_df(df: pd.DataFrame) -> List[Dict[str, Any]]:
    return _make_json_serializable(df.to_dict(orient="records"))


def _as_dict_or_none(value: Any) -> Optional[Dict[str, Any]]:
    """Return dict payloads as-is; coerce other JSON types to None."""
    return value if isinstance(value, dict) else None


def _as_list(value: Any) -> List[Any]:
    """Return list payloads as-is; coerce other JSON types to an empty list."""
    return value if isinstance(value, list) else []


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
    """Build a compatibility in-memory view from V2 per-table catalogue rows."""
    try:
        rows = await catalogue_repo.list_table_rows(session)
        meta = await catalogue_repo.get_meta_row(session) or {}
        tables = {table_id: document for table_id, document in rows}
        logger.info("Loaded V2 catalogue rows from DB with %d tables", len(tables))
        return {"tables": tables, "_catalogue_meta": meta}
    except Exception as e:
        logger.error("Error loading catalogue from DB: %s", e)
        return {"tables": {}}


async def load_silver_catalog_rows(session: AsyncSession) -> List[tuple[str, Dict[str, Any]]]:
    """Load all table rows from V2 data_catalogue model."""
    return await catalogue_repo.list_table_rows(session)


async def load_catalogue_meta(session: AsyncSession) -> Dict[str, Any]:
    """Load _catalogue_meta row from V2 data_catalogue model."""
    return await catalogue_repo.get_meta_row(session) or {}

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
        rows = await load_silver_catalog_rows(session)
        silver_tables = [
            TableInfo(name=name, path=f"silver/{name}", version=0)
            for name, _ in sorted(rows, key=lambda row: row[0])
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
        rows = await load_silver_catalog_rows(session)
        meta = await load_catalogue_meta(session)

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

            # Get certification status from PostgreSQL (fast)
            cert_status = await get_certification_status("silver", table_name, session)

            # Get query count from PostgreSQL
            query_count = await get_table_query_count(session, f"silver_{table_name}")

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
        table_catalogue = await catalogue_repo.get_table_row(session, table_name)

        if not table_catalogue:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found in catalogue")

        # Runtime schema is authoritative and comes from live DuckDB/Iceberg.
        live_schema = await get_table_metadata(request, "silver", table_name, user_id)
        field_docs = _as_dict_or_none(table_catalogue.get("field_docs")) or {}
        table_schema = TableSchema(
            fields=[
                SchemaField(
                    name=field.name,
                    type=field.type,
                    nullable=field.nullable,
                    description=(field_docs.get(field.name) or {}).get("description"),
                    example=(field_docs.get(field.name) or {}).get("example"),
                )
                for field in live_schema.fields
            ],
            version=live_schema.version,
            row_count=live_schema.row_count,
            num_fields=live_schema.num_fields,
        )

        # Get preview from cached data (ensure JSON-serializable for response)
        runtime_hints = table_catalogue.get("runtime_hints", {})
        preview_data = _make_json_serializable(
            runtime_hints.get("preview")
            or runtime_hints.get("preview_rows")
            or []
        )
        
        # Get certification status
        cert_status = await get_certification_status("silver", table_name, session)
        
        business_metadata = _as_dict_or_none(table_catalogue.get("business_metadata")) or {}
        raw_sources = _as_list(business_metadata.get("sources"))
        schema_cache = _as_dict_or_none(table_catalogue.get("schema_cache")) or {}
        drift_details = _as_dict_or_none(schema_cache.get("drift_details"))
        if schema_cache.get("drift_details") is not None and drift_details is None:
            logger.warning("Ignoring non-dict drift_details for table detail '%s'", table_name)
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
    executor = get_sql_executor()
    
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
        metadata_path = find_latest_metadata(table_path)
        if not metadata_path:
            raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")

        executor.register_iceberg_view(
            table_name=table,
            metadata_path=metadata_path,
            s3_config=_build_s3_config(settings),
        )
        schema_info = executor.get_table_schema(table)
        return TableSchema(
            fields=[SchemaField(name=field["name"], type=field["type"], nullable=field["nullable"]) for field in schema_info["fields"]],
            version=int(schema_info["version"]),
            row_count=schema_info["row_count"],
            num_fields=int(schema_info["num_fields"]),
        )
    except HTTPException:
        raise
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
    executor = get_sql_executor()
    
    # Validate layer
    if layer not in ["bronze", "silver", "gold"]:
        raise HTTPException(status_code=400, detail="Layer must be bronze, silver, or gold")
    
    # Check access permissions
    await verify_table_access(layer, table, session, current_user)

    # Silver: serve the 10-row cached preview from PostgreSQL — fast and no S3 reads.
    # Filters and sort are ignored for the cached version (same 10 rows used in the catalogue modal).
    if layer == "silver":
        table_doc = await catalogue_repo.get_table_row(session, table) or {}
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

    # Bronze / Gold (admin only): fall back to live Iceberg scan
    # Construct table path
    if layer == "silver":
        table_path = settings.get_silver_path(table)
    elif layer == "bronze":
        table_path = settings.get_bronze_path(table)
    else:
        table_path = settings.get_gold_path(table)
    
    try:
        metadata_path = find_latest_metadata(table_path)
        if not metadata_path:
            raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")

        executor.register_iceberg_view(
            table_name=table,
            metadata_path=metadata_path,
            s3_config=_build_s3_config(settings),
        )

        total_rows = int(executor.execute_query(f"SELECT COUNT(*) AS c FROM {table}").iloc[0]["c"])
        scan_limit = max(preview_req.limit * 10, preview_req.limit)
        df = executor.execute_query(f"SELECT * FROM {table} LIMIT {scan_limit}")

        if preview_req.filters:
            for spec in preview_req.filters:
                col = spec.column
                op = spec.operator
                val = spec.value
                if col in df.columns:
                    try:
                        if op == "=":
                            df = df[df[col] == val]
                        elif op == "!=":
                            df = df[df[col] != val]
                        elif op == "contains":
                            df = df[df[col].astype(str).str.contains(str(val), case=False, na=False)]
                        elif op == ">":
                            df = df[df[col] > float(val)]
                        elif op == "<":
                            df = df[df[col] < float(val)]
                        elif op == ">=":
                            df = df[df[col] >= float(val)]
                        elif op == "<=":
                            df = df[df[col] <= float(val)]
                    except Exception as exc:
                        logger.warning("Filter failed for %s %s %s: %s", col, op, val, exc)

        filtered_rows = len(df)
        if preview_req.sort_by and preview_req.sort_by in df.columns:
            df = df.sort_values(by=preview_req.sort_by, ascending=(preview_req.sort_order.lower() == "asc"))
        preview_df = df.head(preview_req.limit)

        return PreviewResponse(
            columns=list(preview_df.columns),
            data=_sanitize_preview_df(preview_df),
            total_rows=total_rows,
            filtered_rows=filtered_rows,
            preview_rows=len(preview_df),
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
    executor = get_sql_executor()
    try:
        rows = await load_silver_catalog_rows(session)
        all_silver_names = [table_name for table_name, _ in rows]
        try:
            referenced_tables = _extract_table_names(query_req.sql, all_silver_names)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid SQL query: {exc}") from exc

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

        s3_config = _build_s3_config(settings)

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
        from app.utils.query_tracker import increment_query_count
        for table_name in referenced_tables:
            await increment_query_count(
                session=session,
                table_name=f"silver_{table_name}",
                user_id=current_user.user_id,
            )
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


@router.get("/export/{table}", tags=["data"])
@limiter.limit("10/minute")
async def export_table(
    request: Request,
    table: str,
    format: str = "csv",
    current_user: AuthenticatedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_db),
):
    """Stream a full silver table as CSV or Parquet via DuckDB Iceberg scan.

    Regular users can export any silver table with no row limit.
    Admin users can export any table.
    Pass `?format=parquet` to receive a Parquet file instead of CSV.
    """
    if format not in ("csv", "parquet"):
        raise HTTPException(status_code=400, detail="format must be 'csv' or 'parquet'.")

    if not current_user.is_admin:
        await verify_table_access("silver", table, session, current_user)

    settings = get_settings()
    table_path = settings.get_silver_path(table)
    metadata_path = find_latest_metadata(table_path)
    if not metadata_path:
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")

    s3_config = _build_s3_config(settings)
    executor = get_sql_executor()
    try:
        executor.register_iceberg_view(table, metadata_path, s3_config)

        if format == "parquet":
            data = executor.export_to_parquet(table)
            return StreamingResponse(
                io.BytesIO(data),
                media_type="application/octet-stream",
                headers={"Content-Disposition": f"attachment; filename={table}.parquet"},
            )

        return StreamingResponse(
            executor.export_to_csv_chunks(table),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={table}.csv"},
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Export failed for table %s: %s", table, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to export table: {str(e)}")


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

        existing_doc = await catalogue_repo.get_meta_row(session)
        if not existing_doc:
            raise HTTPException(
                status_code=404,
                detail=f"No {CATALOGUE_META_ID} document found in PostgreSQL. Run the pipeline first.",
            )

        sync_time = datetime.now(timezone.utc)
        existing_doc["catalog_generated_at"] = sync_time.isoformat()
        await catalogue_repo.upsert_meta_row(session, existing_doc)

        num_tables = len(await load_silver_catalog_rows(session))
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

