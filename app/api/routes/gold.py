"""Gold layer API endpoints (placeholder)."""
from fastapi import APIRouter, Depends, HTTPException, Request
from app.core.auth import verify_api_key, verify_admin_secret_or_admin_key
from app.core.models import PipelineRunResponse
from app.core.rate_limiter import limiter

router = APIRouter(prefix="/api/gold", tags=["gold"])


@router.post("/{pipeline_name}", response_model=PipelineRunResponse)
@limiter.limit("10/hour")
async def run_gold_pipeline(
    request: Request,
    pipeline_name: str,
    force: bool = False,
    admin_verified: bool = Depends(verify_admin_secret_or_admin_key)
):
    """
    Run a specific gold layer pipeline (placeholder).
    
    **Requires admin authentication.**
    
    Args:
        pipeline_name: Name of the gold pipeline to run
        force: Force reprocessing
        admin_verified: Admin authentication
        
    Returns:
        Pipeline run response
    """
    raise HTTPException(
        status_code=501,
        detail="Gold layer pipelines not yet implemented. Add gold pipelines as needed."
    )

