"""Job tracking API endpoints (PostgreSQL-backed)."""
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import verify_api_key_or_admin
from app.core.job_manager import JobManager, get_job_manager
from app.core.pipeline_executor import PipelineExecutor, get_pipeline_executor
from app.core.rate_limiter import limiter
from app.db.session import get_db
from app.db.repositories.job_logs import job_log_repo

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


@router.get("")
@limiter.limit("60/minute")
async def list_jobs(
    request: Request,
    limit: int = 50,
    user_id: str = Depends(verify_api_key_or_admin),
    job_manager: JobManager = Depends(get_job_manager),
) -> Dict[str, Any]:
    """List recent jobs with progress."""
    try:
        jobs = await job_manager.list_jobs(limit=limit)
        return {"jobs": jobs, "count": len(jobs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{job_id}")
@limiter.limit("60/minute")
async def get_job(
    request: Request,
    job_id: str,
    user_id: str = Depends(verify_api_key_or_admin),
    job_manager: JobManager = Depends(get_job_manager),
) -> Dict[str, Any]:
    """Get job details including all tasks."""
    try:
        job_data = await job_manager.get_job(job_id, include_tasks=True)
        if job_data is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        return job_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{job_id}/logs")
@limiter.limit("60/minute")
async def get_job_logs(
    request: Request,
    job_id: str,
    task_id: Optional[str] = None,
    limit: int = 1000,
    offset: int = 0,
    user_id: str = Depends(verify_api_key_or_admin),
    job_manager: JobManager = Depends(get_job_manager),
    session: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """Get logs for a specific job."""
    try:
        job_data = await job_manager.get_job(job_id)
        if job_data is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        logs = await job_log_repo.list_logs(
            session, job_id, task_id=task_id, limit=limit, offset=offset
        )
        return {
            "job_id": job_id,
            "logs": logs,
            "count": len(logs),
            "limit": limit,
            "offset": offset,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs/stream")
@limiter.limit("60/minute")
async def get_all_logs_stream(
    request: Request,
    limit: int = 500,
    user_id: str = Depends(verify_api_key_or_admin),
    job_manager: JobManager = Depends(get_job_manager),
    session: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """Get recent logs from all jobs in chronological order."""
    try:
        jobs = await job_manager.list_jobs(limit=3)
        all_logs = []
        for job in jobs:
            job_id = job.get("job_id")
            if not job_id:
                continue
            logs = await job_log_repo.list_logs(session, job_id, limit=500)
            all_logs.extend(logs)
        all_logs.sort(key=lambda x: x.get("timestamp") or "")
        all_logs = all_logs[-limit:]
        return {"logs": all_logs, "count": len(all_logs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{job_id}/cancel")
@limiter.limit("60/minute")
async def cancel_job(
    request: Request,
    job_id: str,
    user_id: str = Depends(verify_api_key_or_admin),
    executor: PipelineExecutor = Depends(get_pipeline_executor),
) -> Dict[str, Any]:
    """Cancel a running job."""
    try:
        success = await executor.cancel_job(job_id)
        if not success:
            raise HTTPException(
                status_code=400,
                detail=f"Could not cancel job {job_id}. Job may not exist or is not running.",
            )
        return {
            "message": f"Job {job_id} has been cancelled",
            "job_id": job_id,
            "status": "cancelled",
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
