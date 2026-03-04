"""Pipeline orchestration endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Request
from app.core.auth import verify_api_key, verify_admin_secret, verify_api_key_or_admin
from app.core.models import (
    FullPipelineRunRequest,
    PipelineRunResponse,
    PipelineStatusResponse,
    PipelineListResponse,
    PipelineLayer,
    PipelineStatus,
    PipelineInfo,
)
from app.core.pipeline_executor import PipelineExecutor, get_pipeline_executor
from app.core.pipeline_registry import get_registry
from app.core.rate_limiter import limiter
from app.core.dbt_runner import run_dbt_async
from datetime import datetime
import uuid

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


@router.post("/run")
@limiter.limit("10/hour")
async def run_full_pipeline(
    request: Request,
    pipeline_request: FullPipelineRunRequest = FullPipelineRunRequest(),
    admin_verified: bool = Depends(verify_admin_secret),
    executor: PipelineExecutor = Depends(get_pipeline_executor),
):
    """
    Run the full data pipeline: bronze (in-process) then silver/gold via DBT.

    **Requires admin authentication.**
    """

    try:
        job_id, states = await executor.execute_full_pipeline(
            bronze_only=pipeline_request.bronze_only,
            silver_only=pipeline_request.silver_only,
            force=pipeline_request.force,
            user_id="admin"
        )

        total = len(states)
        succeeded = sum(1 for s in states if s.status == PipelineStatus.SUCCESS)
        failed = sum(1 for s in states if s.status == PipelineStatus.FAILED)

        dbt_run = None
        if not pipeline_request.bronze_only and failed == 0:
            # Run silver (and gold) via DBT
            dbt_ok, dbt_message, dbt_code = await run_dbt_async(
                silver_only=pipeline_request.silver_only,
                run_tests=False,
            )
            dbt_run = {
                "status": "success" if dbt_ok else "failed",
                "message": dbt_message[:500] if dbt_message else "",
                "exit_code": dbt_code,
            }
            if not dbt_ok:
                failed += 1

        return {
            "job_id": job_id,
            "status": "success" if failed == 0 else "partial",
            "total_pipelines": total,
            "succeeded": succeeded,
            "failed": failed,
            "pipelines": [s.to_dict() for s in states],
            "dbt_run": dbt_run,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{run_id}", response_model=PipelineStatusResponse)
@limiter.limit("60/minute")
async def get_pipeline_status(
    request: Request,
    run_id: str,
    user_id: str = Depends(verify_api_key_or_admin),
    executor: PipelineExecutor = Depends(get_pipeline_executor),
):
    """Get status of a pipeline run."""
    state = executor.get_execution_state(run_id)
    
    if state is None:
        raise HTTPException(status_code=404, detail=f"Run ID {run_id} not found")
    
    state_dict = state.to_dict()
    
    return PipelineStatusResponse(
        run_id=state_dict["run_id"],
        pipeline_name=state_dict["pipeline_name"],
        layer=PipelineLayer(state_dict["layer"]),
        status=PipelineStatus(state_dict["status"]),
        started_at=datetime.fromisoformat(state_dict["started_at"]) if state_dict["started_at"] else datetime.utcnow(),
        completed_at=datetime.fromisoformat(state_dict["completed_at"]) if state_dict["completed_at"] else None,
        duration_seconds=state_dict.get("duration_seconds"),
        message=state_dict.get("message", ""),
        error=state_dict.get("error"),
        stats=state_dict.get("stats")
    )


@router.get("/history")
@limiter.limit("60/minute")
async def get_pipeline_history(
    request: Request,
    limit: int = 50,
    user_id: str = Depends(verify_api_key_or_admin),
    executor: PipelineExecutor = Depends(get_pipeline_executor),
):
    """Get recent pipeline execution history."""
    history = executor.get_execution_history(limit=limit)
    
    return {
        "history": history,
        "count": len(history)
    }


# DBT-managed silver/gold model names (for list endpoint; actual run is via dbt)
DBT_SILVER_PIPELINES = [
    "dim_commune", "dim_accueillant", "dim_gare_segment", "dim_gare", "dim_ligne",
    "dim_siae_structure", "ref_logement_profil", "fact_loyer_annonce",
    "fact_zone_attraction", "fact_siae_poste",
]
DBT_GOLD_PIPELINES = []


@router.get("/list", response_model=PipelineListResponse)
@limiter.limit("60/minute")
async def list_pipelines(
    request: Request,
    layer: str = None,
    user_id: str = Depends(verify_api_key_or_admin)
):
    """
    List available pipelines. Bronze = app; silver/gold = DBT-managed (run via dbt).
    """
    registry = get_registry()
    layer_filter = None
    if layer:
        try:
            layer_filter = PipelineLayer(layer)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid layer: {layer}")

    pipelines = list(registry.list_pipelines(layer=layer_filter))
    if layer_filter is None or layer_filter == PipelineLayer.SILVER:
        for name in DBT_SILVER_PIPELINES:
            pipelines.append(PipelineInfo(
                name=name, layer=PipelineLayer.SILVER, description="DBT-managed",
                description_fr=None, dependencies=[],
            ))
    if layer_filter is None or layer_filter == PipelineLayer.GOLD:
        for name in DBT_GOLD_PIPELINES:
            pipelines.append(PipelineInfo(
                name=name, layer=PipelineLayer.GOLD, description="DBT-managed",
                description_fr=None, dependencies=[],
            ))
    return PipelineListResponse(pipelines=pipelines)

