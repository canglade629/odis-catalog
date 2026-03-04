"""Silver layer API endpoints. Silver is run via DBT (dbt/ project)."""
from fastapi import APIRouter, Depends, HTTPException, Request
from app.core.auth import verify_admin_secret
from app.core.job_manager import JobManager, get_job_manager, JobStatus
from app.core.rate_limiter import limiter
from app.core.dbt_runner import run_dbt_async
from datetime import datetime
import uuid

router = APIRouter(prefix="/api/silver", tags=["silver"])


@router.post("/{pipeline_name}")
@limiter.limit("10/hour")
async def run_silver_pipeline(
    request: Request,
    pipeline_name: str,
    force: bool = False,
    admin_verified: bool = Depends(verify_admin_secret),
    job_manager: JobManager = Depends(get_job_manager),
):
    """
    Run silver layer via DBT. pipeline_name can be a model name or "all" to run all silver models.

    **Requires admin authentication.**
    """
    job = await job_manager.create_job(f"silver.{pipeline_name} (dbt)", total_tasks=1)
    job_id = job.job_id
    await job_manager.update_job_progress(job_id, status=JobStatus.RUNNING)

    try:
        dbt_ok, dbt_message, dbt_code = await run_dbt_async(
            silver_only=True,
            run_tests=False,
        )
        final_status = JobStatus.SUCCESS if dbt_ok else JobStatus.FAILED
        await job_manager.update_job_progress(
            job_id,
            status=final_status,
            completed_tasks=1 if dbt_ok else 0,
            failed_tasks=0 if dbt_ok else 1,
            completed_at=datetime.utcnow(),
        )
        return {
            "job_id": job_id,
            "run_id": str(uuid.uuid4()),
            "pipeline_name": pipeline_name,
            "layer": "silver",
            "status": "success" if dbt_ok else "failed",
            "started_at": datetime.utcnow().isoformat(),
            "message": dbt_message[:500] if dbt_message else ("DBT run completed" if dbt_ok else "DBT run failed"),
        }
    except Exception as e:
        await job_manager.update_job_progress(
            job_id,
            status=JobStatus.FAILED,
            failed_tasks=1,
            completed_at=datetime.utcnow(),
        )
        raise HTTPException(status_code=500, detail=str(e))

