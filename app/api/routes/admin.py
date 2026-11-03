"""Admin API routes for managing API keys (Firestore-backed)."""
from fastapi import APIRouter, Depends, HTTPException, status, Request
from google.cloud import firestore
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import logging
from pathlib import Path

from app.core.auth import verify_admin_secret_or_admin_key, verify_api_key_or_admin
from app.db.firestore import get_db
from app.db.repositories.catalogue import catalogue_repo, CATALOGUE_META_ID
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
from app.utils.bigquery_ops import LAYERS, dataset_id, invalidate_table_names

logger = logging.getLogger(__name__)


router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(verify_admin_secret_or_admin_key)]
)


class CreateAPIKeyRequest(BaseModel):
    """Request model for creating a new API key."""
    user_id: str
    is_admin: bool = False


class CreateAPIKeyResponse(BaseModel):
    """Response model for API key creation."""
    api_key: str
    user_id: str
    is_admin: bool
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
    is_admin: bool
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


class RuntimeCacheInvalidateRequest(BaseModel):
    """Request model for runtime metadata cache invalidation."""
    all: bool = False
    layer: Optional[str] = "silver"
    table_name: Optional[str] = None


class RuntimeCacheInvalidateResponse(BaseModel):
    """Response model for runtime metadata cache invalidation."""
    status: str
    scope: str
    table_path: Optional[str] = None
    message: str


@router.post("/api-keys", response_model=CreateAPIKeyResponse, status_code=status.HTTP_201_CREATED)
@limiter.limit("100/hour")
async def create_new_api_key(
    request: Request,
    api_key_request: CreateAPIKeyRequest,
    db: firestore.AsyncClient = Depends(get_db),
):
    """Create a new API key for a user. By default read-only; set is_admin=True for admin access. Requires admin authentication."""
    result = await create_api_key(
        api_key_request.user_id, db, is_admin=api_key_request.is_admin
    )
    return CreateAPIKeyResponse(
        api_key=result["api_key"],
        user_id=result["user_id"],
        is_admin=result["is_admin"],
        created_at=result["created_at"],
    )


@router.delete("/api-keys/revoke")
@limiter.limit("100/hour")
async def revoke_existing_api_key(
    request: Request,
    revoke_request: RevokeAPIKeyRequest,
    db: firestore.AsyncClient = Depends(get_db),
):
    """Revoke an API key (soft delete - sets active=False). Requires admin."""
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
    db: firestore.AsyncClient = Depends(get_db),
):
    """Permanently delete an API key. Requires admin authentication."""
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
    db: firestore.AsyncClient = Depends(get_db),
):
    """
    List all API keys (without plaintext keys).
    
    Requires admin authentication via ADMIN_SECRET.
    
    Args:
        db: Firestore client
        
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
    db: firestore.AsyncClient = Depends(get_db),
    admin_verified: bool = Depends(verify_admin_secret_or_admin_key)
):
    """
    Certify a table for public use.
    
    Requires admin authentication via ADMIN_SECRET.
    
    Args:
        request: Contains table_name and layer
        db: Firestore client
        admin_verified: Admin authentication status
        
    Returns:
        Certification details
    """
    try:
        result = await certify_table(
            certify_request.layer,
            certify_request.table_name,
            "admin",
            db,
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
    db: firestore.AsyncClient = Depends(get_db),
    admin_verified: bool = Depends(verify_admin_secret_or_admin_key)
):
    """
    Remove certification from a table.
    
    Requires admin authentication via ADMIN_SECRET.
    
    Args:
        request: Contains table_name and layer
        db: Firestore client
        admin_verified: Admin authentication status
        
    Returns:
        Success message
    """
    try:
        success = await uncertify_table(
            uncertify_request.layer,
            uncertify_request.table_name,
            db,
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
    db: firestore.AsyncClient = Depends(get_db),
    admin_verified: bool = Depends(verify_admin_secret_or_admin_key)
):
    """
    List all table certifications.
    
    Requires admin authentication via ADMIN_SECRET.
    
    Args:
        db: Firestore client
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
    db: firestore.AsyncClient = Depends(get_db),
):
    """Refresh the catalogue view from the Firestore documents written by the pipeline.

    The pipeline project (dbt + manifest loader) is the single source of truth:
    it writes descriptions, examples, schema, preview and row_count into the
    data_catalogue collection.  This endpoint drops this instance's catalogue cache,
    re-reads the meta document, stamps a new last_synced timestamp, and writes it
    back so the UI picks up the latest data without any GCS or YAML reads.
    """
    try:
        from datetime import datetime, timezone

        catalogue_repo.invalidate_cache()
        existing_doc = await catalogue_repo.get_meta_row(db)
        if not existing_doc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No {CATALOGUE_META_ID} document found in Firestore. Run the pipeline first.",
            )

        sync_time = datetime.now(timezone.utc)
        existing_doc["catalog_generated_at"] = sync_time.isoformat()
        await catalogue_repo.upsert_meta_row(db, existing_doc)

        num_tables = len(await catalogue_repo.list_table_rows(db))
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
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to refresh catalogue: {str(e)}",
        )


@router.post("/runtime-cache/invalidate", response_model=RuntimeCacheInvalidateResponse)
@limiter.limit("30/hour")
async def invalidate_runtime_cache(
    request: Request,
    payload: RuntimeCacheInvalidateRequest,
):
    """Drop this instance's cached BigQuery table listings and Firestore catalogue.

    The caches are per layer, so the whole cache is dropped even when a single table is named.
    """
    if not payload.all and not payload.table_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="table_name is required when all=false",
        )
    layer = (payload.layer or "silver").lower()
    if not payload.all and layer not in LAYERS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="layer must be one of: bronze, silver, gold",
        )

    invalidate_table_names()
    catalogue_repo.invalidate_cache()
    if payload.all:
        return RuntimeCacheInvalidateResponse(
            status="success",
            scope="all",
            table_path=None,
            message="Runtime cache invalidated for all tables.",
        )
    table_path = f"{dataset_id(layer)}.{payload.table_name}"
    return RuntimeCacheInvalidateResponse(
        status="success",
        scope="table",
        table_path=table_path,
        message=f"Runtime cache invalidated for {table_path}.",
    )

