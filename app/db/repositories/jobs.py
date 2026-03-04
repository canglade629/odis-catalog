"""Jobs and tasks repository (PostgreSQL)."""
from datetime import datetime
from typing import List, Dict, Any, Optional

from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job as JobModel, JobTask as JobTaskModel


def _job_to_dict(row: JobModel) -> Dict[str, Any]:
    return {
        "job_id": row.job_id,
        "job_name": row.job_name,
        "status": row.status,
        "started_at": row.started_at,
        "completed_at": row.completed_at,
        "total_tasks": row.total_tasks,
        "completed_tasks": row.completed_tasks,
        "failed_tasks": row.failed_tasks,
        "progress_percent": row.progress_percent,
        "user_id": row.user_id,
    }


def _task_to_dict(row: JobTaskModel) -> Dict[str, Any]:
    return {
        "task_id": row.task_id,
        "pipeline_name": row.pipeline_name,
        "layer": row.layer,
        "status": row.status,
        "started_at": row.started_at,
        "completed_at": row.completed_at,
        "duration_seconds": row.duration_seconds,
        "message": row.message,
        "error": row.error,
        "stats": row.stats,
    }


class JobRepository:
    async def create(
        self,
        session: AsyncSession,
        job_id: str,
        job_name: str,
        total_tasks: int = 0,
        user_id: Optional[str] = None,
    ) -> JobModel:
        job = JobModel(
            job_id=job_id,
            job_name=job_name,
            status="pending",
            started_at=datetime.utcnow(),
            completed_at=None,
            total_tasks=total_tasks,
            completed_tasks=0,
            failed_tasks=0,
            progress_percent=0.0,
            user_id=user_id,
        )
        session.add(job)
        await session.flush()
        return job

    async def update(
        self,
        session: AsyncSession,
        job_id: str,
        **kwargs: Any,
    ) -> bool:
        result = await session.execute(select(JobModel).where(JobModel.job_id == job_id))
        row = result.scalars().first()
        if not row:
            return False
        for k, v in kwargs.items():
            if hasattr(row, k):
                setattr(row, k, v)
        await session.flush()
        return True

    async def get(self, session: AsyncSession, job_id: str) -> Optional[Dict[str, Any]]:
        result = await session.execute(select(JobModel).where(JobModel.job_id == job_id))
        row = result.scalars().first()
        if not row:
            return None
        return _job_to_dict(row)

    async def get_with_tasks(self, session: AsyncSession, job_id: str) -> Optional[Dict[str, Any]]:
        result = await session.execute(select(JobModel).where(JobModel.job_id == job_id))
        job_row = result.scalars().first()
        if not job_row:
            return None
        data = _job_to_dict(job_row)
        tasks_result = await session.execute(
            select(JobTaskModel).where(JobTaskModel.job_id == job_id).order_by(JobTaskModel.started_at)
        )
        data["tasks"] = [_task_to_dict(t) for t in tasks_result.scalars().all()]
        return data

    async def list_jobs(self, session: AsyncSession, limit: int = 50) -> List[Dict[str, Any]]:
        result = await session.execute(
            select(JobModel).order_by(desc(JobModel.started_at)).limit(limit)
        )
        return [_job_to_dict(r) for r in result.scalars().all()]

    async def add_task(
        self,
        session: AsyncSession,
        job_id: str,
        task_id: str,
        pipeline_name: str,
        layer: str,
        status: str = "pending",
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        duration_seconds: Optional[float] = None,
        message: str = "",
        error: Optional[str] = None,
        stats: Optional[Dict] = None,
    ) -> None:
        task = JobTaskModel(
            job_id=job_id,
            task_id=task_id,
            pipeline_name=pipeline_name,
            layer=layer,
            status=status,
            started_at=started_at,
            completed_at=completed_at,
            duration_seconds=duration_seconds,
            message=message or None,
            error=error,
            stats=stats,
        )
        session.add(task)
        await session.flush()

    async def update_task(
        self,
        session: AsyncSession,
        job_id: str,
        task_id: str,
        **kwargs: Any,
    ) -> bool:
        result = await session.execute(
            select(JobTaskModel).where(
                JobTaskModel.job_id == job_id,
                JobTaskModel.task_id == task_id,
            )
        )
        row = result.scalars().first()
        if not row:
            return False
        for k, v in kwargs.items():
            if hasattr(row, k):
                setattr(row, k, v)
        await session.flush()
        return True

    async def get_tasks(self, session: AsyncSession, job_id: str) -> List[Dict[str, Any]]:
        result = await session.execute(
            select(JobTaskModel).where(JobTaskModel.job_id == job_id).order_by(JobTaskModel.started_at)
        )
        return [_task_to_dict(r) for r in result.scalars().all()]


job_repo = JobRepository()
