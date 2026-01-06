"""Admin API routes for managing API keys."""
from fastapi import APIRouter, Depends, HTTPException, status, Request
from google.cloud import firestore
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import logging
from pathlib import Path

from app.core.auth import verify_admin_secret, verify_api_key_or_admin, get_firestore_client
from app.core.rate_limiter import limiter
from app.core.api_key_manager import (
    create_api_key,
    revoke_api_key,
    delete_api_key,
    list_api_keys
)
from app.core.certification_manager import (
    certify_table,
    uncertify_table,
    get_certification_status,
    get_all_certifications
)

logger = logging.getLogger(__name__)


router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(verify_admin_secret)]
)


class CreateAPIKeyRequest(BaseModel):
    """Request model for creating a new API key."""
    user_id: str


class CreateAPIKeyResponse(BaseModel):
    """Response model for API key creation."""
    api_key: str
    user_id: str
    created_at: str
    message: str = "API key created successfully. Save this key - it will not be shown again."


class RevokeAPIKeyRequest(BaseModel):
    """Request model for revoking an API key."""
    api_key: str


class DeleteAPIKeyRequest(BaseModel):
    """Request model for deleting an API key."""
    api_key: str


class APIKeyInfo(BaseModel):
    """API key metadata (without plaintext key)."""
    hash: str
    user_id: str
    created_at: Optional[datetime]
    last_used_at: Optional[datetime]
    active: bool


class CertifyTableRequest(BaseModel):
    """Request model for certifying a table."""
    table_name: str
    layer: str = "silver"


class UncertifyTableRequest(BaseModel):
    """Request model for uncertifying a table."""
    table_name: str
    layer: str = "silver"


class CertificationResponse(BaseModel):
    """Response model for certification operations."""
    layer: str
    table_name: str
    certified: bool
    certified_at: Optional[str] = None
    certified_by: Optional[str] = None


class CatalogueRefreshResponse(BaseModel):
    """Response model for catalogue refresh operations."""
    status: str
    tables_synced: int
    last_synced: str
    version: str


@router.post("/api-keys", response_model=CreateAPIKeyResponse, status_code=status.HTTP_201_CREATED)
@limiter.limit("100/hour")
async def create_new_api_key(
    request: Request,
    api_key_request: CreateAPIKeyRequest,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Create a new API key for a user.
    
    Requires admin authentication via ADMIN_SECRET.
    
    Args:
        request: Contains user_id for the new API key
        db: Firestore async client
        
    Returns:
        The newly created API key (shown only once)
    """
    result = await create_api_key(api_key_request.user_id, db)
    
    return CreateAPIKeyResponse(
        api_key=result["api_key"],
        user_id=result["user_id"],
        created_at=result["created_at"]
    )


@router.delete("/api-keys/revoke")
@limiter.limit("100/hour")
async def revoke_existing_api_key(
    request: Request,
    revoke_request: RevokeAPIKeyRequest,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Revoke an API key (soft delete - sets active=False).
    
    Requires admin authentication via ADMIN_SECRET.
    
    Args:
        request: Contains the API key to revoke
        db: Firestore async client
        
    Returns:
        Success message
    """
    success = await revoke_api_key(revoke_request.api_key, db)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    return {"message": "API key revoked successfully"}


@router.delete("/api-keys/delete")
@limiter.limit("100/hour")
async def delete_existing_api_key(
    request: Request,
    delete_request: DeleteAPIKeyRequest,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Permanently delete an API key from Firestore.
    
    Requires admin authentication via ADMIN_SECRET.
    
    Args:
        request: Contains the API key to delete
        db: Firestore async client
        
    Returns:
        Success message
    """
    success = await delete_api_key(delete_request.api_key, db)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found"
        )
    
    return {"message": "API key deleted successfully"}


@router.get("/api-keys", response_model=List[APIKeyInfo])
@limiter.limit("100/hour")
async def list_all_api_keys(
    request: Request,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    List all API keys (without plaintext keys).
    
    Requires admin authentication via ADMIN_SECRET.
    
    Args:
        db: Firestore async client
        
    Returns:
        List of API key metadata
    """
    keys = await list_api_keys(db)
    return keys


@router.post("/tables/certify", response_model=CertificationResponse)
@limiter.limit("100/hour")
async def certify_table_endpoint(
    request: Request,
    certify_request: CertifyTableRequest,
    db: firestore.AsyncClient = Depends(get_firestore_client),
    admin_verified: bool = Depends(verify_admin_secret)
):
    """
    Certify a table for public use.
    
    Requires admin authentication via ADMIN_SECRET.
    
    Args:
        request: Contains table_name and layer
        db: Firestore async client
        admin_verified: Admin authentication status
        
    Returns:
        Certification details
    """
    try:
        result = await certify_table(
            layer=certify_request.layer,
            table_name=certify_request.table_name,
            admin_id="admin",  # In the future, could track specific admin user
            db=db
        )
        
        return CertificationResponse(**result)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to certify table: {str(e)}"
        )


@router.post("/tables/uncertify")
@limiter.limit("100/hour")
async def uncertify_table_endpoint(
    request: Request,
    uncertify_request: UncertifyTableRequest,
    db: firestore.AsyncClient = Depends(get_firestore_client),
    admin_verified: bool = Depends(verify_admin_secret)
):
    """
    Remove certification from a table.
    
    Requires admin authentication via ADMIN_SECRET.
    
    Args:
        request: Contains table_name and layer
        db: Firestore async client
        admin_verified: Admin authentication status
        
    Returns:
        Success message
    """
    try:
        success = await uncertify_table(
            layer=uncertify_request.layer,
            table_name=uncertify_request.table_name,
            db=db
        )
        
        if success:
            return {
                "message": f"Table {uncertify_request.layer}.{uncertify_request.table_name} uncertified successfully"
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to uncertify table"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to uncertify table: {str(e)}"
        )


@router.get("/tables/certifications")
@limiter.limit("100/hour")
async def list_table_certifications(
    request: Request,
    db: firestore.AsyncClient = Depends(get_firestore_client),
    admin_verified: bool = Depends(verify_admin_secret)
):
    """
    List all table certifications.
    
    Requires admin authentication via ADMIN_SECRET.
    
    Args:
        db: Firestore async client
        admin_verified: Admin authentication status
        
    Returns:
        List of all certification statuses
    """
    try:
        certifications = await get_all_certifications(db)
        return {"certifications": certifications}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve certifications: {str(e)}"
        )


@router.post("/catalogue/refresh", response_model=CatalogueRefreshResponse)
@limiter.limit("10/hour")
async def refresh_catalogue(
    request: Request,
    user_id: str = Depends(verify_api_key_or_admin),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Refresh the data catalogue in Firestore from the YAML file.
    
    This endpoint reads config/data_catalogue.yaml, enriches it with
    schema and preview data from Delta tables, and syncs to Firestore
    for fast access.
    
    Available to all authenticated users (API key or admin secret).
    
    Args:
        db: Firestore async client
        admin_verified: Admin authentication status
        
    Returns:
        Sync status with number of tables and timestamp
    """
    try:
        import yaml
        from datetime import datetime, timezone
        from app.core.config import get_settings
        from app.utils.delta_ops import DeltaOperations
        
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
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
                table_path = f"{settings.delta_path}/silver/{table_name}"
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
        
        # Convert async Firestore client to sync for the sync operation
        sync_db = firestore.Client()
        
        # Prepare document with metadata
        sync_time = datetime.now(timezone.utc)
        
        firestore_doc = {
            'tables': enriched_tables,
            'version': catalogue_data.get('version', 'unknown'),
            'generated_at': catalogue_data.get('generated_at', ''),
            'last_synced': sync_time,
            'source_file': 'data_catalogue.yaml',
            'enriched': True
        }
        
        # Write to Firestore
        collection_ref = sync_db.collection('data_catalogue')
        doc_ref = collection_ref.document('silver_tables')
        doc_ref.set(firestore_doc)
        
        num_tables = len(enriched_tables)
        
        logger.info(f"✅ Synced {num_tables} enriched tables to Firestore at {sync_time.isoformat()}")
        
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
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to refresh catalogue: {str(e)}"
        )


