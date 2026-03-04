"""Data catalog API routes."""
import logging
import time
from decimal import Decimal
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
import numpy as np

from app.core.auth import verify_api_key, verify_admin_secret, verify_api_key_or_admin
from app.db.session import get_db
from app.db.repositories.catalogue import catalogue_repo
from app.core.config import get_settings
from app.utils.delta_ops import DeltaOperations
from app.utils.sql_executor import SQLExecutor
from app.core.rate_limiter import limiter
from app.core.pipeline_registry import get_registry
from app.core.config_loader import get_config_loader
from app.core.models import PipelineLayer
from app.core.certification_manager import is_table_certified, get_certification_status

logger = logging.getLogger(__name__)

security = HTTPBearer(auto_error=False)


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


async def check_admin_or_raise(credentials: Optional[HTTPAuthorizationCredentials]) -> bool:
    """
    Check if request has valid admin credentials.
    Returns True if admin, False otherwise (doesn't raise).
    """
    if not credentials:
        return False
    
    try:
        from app.core.config import get_settings
        settings = get_settings()
        return credentials.credentials == settings.admin_secret
    except:
        return False


async def verify_table_access(
    layer: str,
    table_name: str,
    session: AsyncSession,
    credentials: Optional[HTTPAuthorizationCredentials] = None,
) -> None:
    """
    Verify that the user has access to the specified table.
    
    - Admins can access any table
    - Regular users can only access certified silver tables
    - Bronze and gold tables require admin access
    
    Raises HTTPException if access is denied.
    """
    # Check if user is admin
    is_admin = await check_admin_or_raise(credentials)
    
    # Admins can access everything
    if is_admin:
        return
    
    # Only silver tables can be accessed by non-admins (if certified)
    if layer != "silver":
        raise HTTPException(
            status_code=403,
            detail=f"Access to {layer} tables requires admin privileges"
        )
    
    # Check if the silver table is certified
    certified = await is_table_certified(layer, table_name, session)
    
    if not certified:
        raise HTTPException(
            status_code=403,
            detail=f"Table {table_name} is not certified for public use. Please contact an administrator."
        )

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


class QueryResponse(BaseModel):
    """SQL query execution response."""
    columns: List[str]
    data: List[Dict[str, Any]]
    row_count: int
    execution_time_ms: float


class SilverTableInfo(BaseModel):
    """Silver table information with French description."""
    name: str
    actual_table_name: str  # The actual Delta table name (e.g., dim_commune)
    description_fr: str
    dependencies: List[str]
    version: int
    row_count: Optional[int]
    certified: bool = False
    certified_at: Optional[str] = None
    certified_by: Optional[str] = None
    query_count: Optional[int] = 0


class SilverCatalogResponse(BaseModel):
    """Catalog response for silver tables only."""
    tables: List[SilverTableInfo]


class CatalogueRefreshResponse(BaseModel):
    """Response from catalogue refresh operation."""
    status: str
    tables_synced: int
    last_synced: str
    version: str


class SilverTableDetail(BaseModel):
    """Detailed information about a silver table."""
    name: str
    description_fr: str
    dependencies: List[str]
    table_schema: TableSchema = Field(alias="schema")  # API key "schema"; avoids shadowing BaseModel.schema
    preview: List[Dict[str, Any]]
    certified: bool = False
    certified_at: Optional[str] = None
    certified_by: Optional[str] = None


@router.get("/catalog", response_model=CatalogResponse)
@limiter.limit("30/minute")
async def get_catalog(request: Request, user_id: str = Depends(verify_api_key_or_admin)):
    """
    Get the complete data catalog with all schemas and tables.
    
    Scans S3 for Delta/Parquet tables in bronze, silver, and gold layers.
    """
    logger.info("Fetching data catalog")
    settings = get_settings()
    
    try:
        schemas = {}
        
        # Scan each layer (S3 paths)
        for layer in ["bronze", "silver", "gold"]:
            layer_path = (
                settings.bronze_path
                if layer == "bronze"
                else (settings.silver_path if layer == "silver" else settings.gold_path)
            )
            try:
                tables = DeltaOperations.list_delta_tables(layer_path)
                schemas[layer] = [
                    TableInfo(
                        name=table["name"],
                        path=table["path"],
                        version=table["version"]
                    )
                    for table in tables
                ]
            except Exception as e:
                logger.warning(f"Could not list tables in {layer}: {e}")
                schemas[layer] = []
        
        return CatalogResponse(schemas=schemas)
    
    except Exception as e:
        logger.error(f"Error fetching catalog: {e}", exc_info=True)
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
    config_loader = get_config_loader()

    try:
        # Import query tracker
        from app.utils.query_tracker import get_table_query_count

        # Load catalogue from PostgreSQL (fast!)
        catalogue = await load_catalogue_from_db(session)
        catalogue_tables = catalogue.get('tables', {})

        # Silver table list from config (DBT-managed; not in app registry)
        silver_configs = config_loader.load_layer_config("silver")

        tables = []
        for pipeline in silver_configs:
            # Pipeline name is now the same as the table name (dim_*, fact_*)
            table_name = pipeline.name

            # Get all metadata from cached catalogue (no S3 calls)
            catalogue_info = catalogue_tables.get(table_name, {})
            row_count = catalogue_info.get("row_count")
            version = catalogue_info.get("schema", {}).get("version", 0)
            
            # Get certification status from PostgreSQL (fast)
            cert_status = await get_certification_status("silver", table_name, session)
            
            # Get query count from PostgreSQL
            query_count = await get_table_query_count(session, f"silver_{table_name}")
            
            tables.append(SilverTableInfo(
                name=table_name,
                actual_table_name=table_name,
                description_fr=pipeline.description_fr or "Description non disponible",
                dependencies=pipeline.dependencies or [],
                version=version,
                row_count=row_count,
                certified=cert_status is not None and cert_status.get("certified", False),
                certified_at=cert_status.get("certified_at") if cert_status else None,
                certified_by=cert_status.get("certified_by") if cert_status else None,
                query_count=query_count
            ))
        
        return SilverCatalogResponse(tables=tables)
    
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
    config_loader = get_config_loader()

    try:
        silver_configs = config_loader.load_layer_config("silver")
        pipeline_info = next((p for p in silver_configs if p.name == table_name), None)

        if not pipeline_info:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")
        
        # Load enriched catalogue from PostgreSQL (includes schema + preview)
        catalogue = await load_catalogue_from_db(session)
        logger.info(f"Loaded enriched catalogue from PostgreSQL with {len(catalogue.get('tables', {}))} tables")
        
        table_catalogue = catalogue.get("tables", {}).get(table_name, {})
        
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
        
        return SilverTableDetail(
            name=table_name,
            description_fr=pipeline_info.description_fr or "Description non disponible",
            dependencies=pipeline_info.dependencies or [],
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
    user_id: str = Depends(verify_api_key_or_admin),
    credentials: HTTPAuthorizationCredentials = Security(security),
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
    await verify_table_access(layer, table, session, credentials)
    
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
    user_id: str = Depends(verify_api_key_or_admin),
    credentials: HTTPAuthorizationCredentials = Security(security),
    session: AsyncSession = Depends(get_db)
):
    """
    Execute a SQL query against Delta tables.
    
    Tables should be referenced as layer_table_name (e.g., bronze_accueillants, silver_geo).
    All available Delta tables are automatically registered.
    
    Args:
        query_req: SQL query request with query string and optional limit
    """
    logger.info(f"Executing SQL query (limit={query_req.limit})")
    logger.debug(f"SQL: {query_req.sql}")
    
    settings = get_settings()
    sql_executor = SQLExecutor()
    
    # Check if user is admin
    is_admin = await check_admin_or_raise(credentials)
    
    try:
        start_time = time.time()
        
        # Register all Delta tables from all layers (S3 paths)
        registered_tables = []
        registration_errors = []
        
        for layer in ["bronze", "silver", "gold"]:
            layer_path = (
                settings.bronze_path
                if layer == "bronze"
                else (settings.silver_path if layer == "silver" else settings.gold_path)
            )
            try:
                tables = DeltaOperations.list_delta_tables(layer_path)
                logger.info(f"Found {len(tables)} tables in {layer}")
                for table in tables:
                    # Use underscore instead of dot for SQL compatibility
                    table_name = f"{layer}_{table['name']}"
                    table_path = table['path']
                    
                    # Check access for non-admin users
                    if not is_admin:
                        # Non-admins can only query certified silver tables
                        if layer != "silver":
                            logger.info(f"Skipping {table_name} - non-silver table, user not admin")
                            continue
                        
                        # Check if silver table is certified
                        certified = await is_table_certified(layer, table['name'], session)
                        if not certified:
                            logger.info(f"Skipping {table_name} - not certified")
                            continue
                    
                    try:
                        sql_executor.register_delta_table(table_name, table_path)
                        registered_tables.append(table_name)
                        logger.info(f"Successfully registered table {table_name}")
                    except Exception as e:
                        error_msg = f"Failed to register {table_name}: {str(e)}"
                        logger.error(error_msg)
                        registration_errors.append(error_msg)
            except Exception as e:
                error_msg = f"Failed to list tables in {layer}: {str(e)}"
                logger.error(error_msg)
                registration_errors.append(error_msg)
        
        logger.info(f"Registered {len(registered_tables)} tables: {registered_tables}")
        
        if not registered_tables:
            error_details = "; ".join(registration_errors) if registration_errors else "No tables found"
            raise HTTPException(
                status_code=500, 
                detail=f"No tables could be registered. {error_details}"
            )
        
        # Execute the query
        result_df = sql_executor.execute_query(query_req.sql)
        
        # Track usage for queried tables
        from app.utils.query_tracker import increment_query_count
        
        # Parse SQL to find which tables were queried
        sql_upper = query_req.sql.upper()
        queried_tables = []
        for table_name in registered_tables:
            # Check if table is referenced in query (case insensitive)
            if table_name.upper() in sql_upper:
                queried_tables.append(table_name)
        
        # Track usage for each queried table
        for table_name in queried_tables:
            await increment_query_count(session, table_name, user_id)
        
        # Apply limit
        limited_df = result_df.head(query_req.limit)
        
        # Convert to response format
        columns = limited_df.columns.tolist()
        data = limited_df.to_dict('records')
        
        # Convert any NaN or None to null properly
        for row in data:
            for key, value in row.items():
                if value is None or (isinstance(value, float) and str(value) == 'nan'):
                    row[key] = None
        
        execution_time = (time.time() - start_time) * 1000  # ms
        
        logger.info(f"Query executed successfully, returned {len(data)} rows in {execution_time:.2f}ms")
        
        return QueryResponse(
            columns=columns,
            data=data,
            row_count=len(result_df),
            execution_time_ms=round(execution_time, 2)
        )
    
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Error executing SQL query: {e}", exc_info=True)
        error_msg = str(e)
        # Make error messages more user-friendly
        if "Catalog Error" in error_msg or "not found" in error_msg.lower():
            available_tables = ", ".join(registered_tables) if registered_tables else "none"
            raise HTTPException(
                status_code=400, 
                detail=f"Table not found. Available tables: {available_tables}. Use format: layer_table_name (e.g., bronze_accueillants, silver_geo)"
            )
        elif "Parser Error" in error_msg or "syntax" in error_msg.lower():
            raise HTTPException(status_code=400, detail=f"SQL syntax error: {error_msg}")
        else:
            raise HTTPException(status_code=500, detail=f"Query execution failed: {error_msg}")
    finally:
        sql_executor.close()


@router.post("/catalog/refresh", response_model=CatalogueRefreshResponse)
@limiter.limit("10/minute")
async def refresh_catalogue(
    request: Request,
    user_id: str = Depends(verify_api_key_or_admin),
    session: AsyncSession = Depends(get_db)
):
    """
    Refresh the data catalogue in PostgreSQL from the YAML file.
    
    This endpoint reads config/data_catalogue.yaml, enriches it with
    schema and preview data from S3 (Delta/Parquet), and syncs to PostgreSQL.
    
    Available to all authenticated users (API key or admin secret).
    
    Args:
        request: FastAPI request object for rate limiting
        user_id: Authenticated user ID or "admin"
        session: AsyncSession (DB)
        
    Returns:
        Sync status with number of tables and timestamp
    """
    try:
        import yaml
        from datetime import datetime, timezone
        
        settings = get_settings()
        
        # Find catalogue file
        possible_paths = [
            Path(__file__).parent.parent.parent / "config" / "data_catalogue.yaml",
            Path("/app/config/data_catalogue.yaml"),
            Path("config/data_catalogue.yaml"),
        ]
        
        catalogue_path = None
        for path in possible_paths:
            if path.exists():
                catalogue_path = path
                break
        
        if not catalogue_path:
            raise HTTPException(
                status_code=500,
                detail=f"Catalogue file not found in any of: {[str(p) for p in possible_paths]}"
            )
        
        # Load YAML
        logger.info(f"Loading catalogue from {catalogue_path}")
        with open(catalogue_path, 'r', encoding='utf-8') as f:
            catalogue_data = yaml.safe_load(f)
        
        # Enrich with Delta table metadata (schema + preview)
        tables_data = catalogue_data.get('tables', {})
        enriched_tables = {}
        
        for table_name, table_info in tables_data.items():
            enriched_table = dict(table_info)
            
            try:
                # Get schema from Delta table
                table_path = settings.get_silver_path(table_name)
                schema_info = DeltaOperations.get_table_schema(table_path)
                
                enriched_table['schema'] = {
                    'fields': schema_info['fields'],
                    'version': schema_info['version'],
                    'num_fields': schema_info['num_fields']
                }
                
                # Get preview data (first 10 rows)
                preview_data = DeltaOperations.preview_table(
                    table_path=table_path,
                    limit=10,
                    filters=None,
                    sort_by=None,
                    sort_order="asc"
                )
                enriched_table['preview'] = preview_data['data']
                
                logger.info(f"Enriched {table_name} with schema and preview")
            except Exception as e:
                logger.warning(f"Could not enrich {table_name}: {e}")
                # Keep original table info without enrichment
            
            enriched_tables[table_name] = enriched_table
        
        sync_time = datetime.now(timezone.utc)
        document = {
            "tables": enriched_tables,
            "version": catalogue_data.get("version", "unknown"),
            "generated_at": catalogue_data.get("generated_at", ""),
            "last_synced": sync_time.isoformat(),
            "source_file": "data_catalogue.yaml",
            "enriched": True,
        }
        document = _make_json_serializable(document)
        await catalogue_repo.set(session, document)
        num_tables = len(enriched_tables)
        logger.info("Synced %d enriched tables to DB at %s", num_tables, sync_time.isoformat())
        
        result = {
            'status': 'success',
            'tables_synced': num_tables,
            'last_synced': sync_time.isoformat(),
            'version': catalogue_data.get('version', 'unknown')
        }
        
        return CatalogueRefreshResponse(**result)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to refresh catalogue: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to refresh catalogue: {str(e)}"
        )

