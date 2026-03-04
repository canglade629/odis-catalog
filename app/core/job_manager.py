"""Job and Task management (PostgreSQL-backed)."""
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum
import logging

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.repositories.jobs import job_repo
from app.db.repositories.job_logs import job_log_repo

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"
    CANCELLED = "cancelled"


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Task:
    """Single pipeline execution task."""

    def __init__(
        self,
        task_id: str,
        pipeline_name: str,
        layer: str,
        status: TaskStatus = TaskStatus.PENDING,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        duration_seconds: Optional[float] = None,
        message: str = "",
        error: Optional[str] = None,
        stats: Optional[Dict[str, Any]] = None,
    ):
        self.task_id = task_id
        self.pipeline_name = pipeline_name
        self.layer = layer
        self.status = status
        self.started_at = started_at
        self.completed_at = completed_at
        self.duration_seconds = duration_seconds
        self.message = message
        self.error = error
        self.stats = stats


class Job:
    """Job containing multiple pipeline tasks."""

    def __init__(
        self,
        job_id: str,
        job_name: str,
        status: JobStatus = JobStatus.PENDING,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        total_tasks: int = 0,
        completed_tasks: int = 0,
        failed_tasks: int = 0,
        progress_percent: float = 0.0,
        user_id: Optional[str] = None,
    ):
        self.job_id = job_id
        self.job_name = job_name
        self.status = status
        self.started_at = started_at
        self.completed_at = completed_at
        self.total_tasks = total_tasks
        self.completed_tasks = completed_tasks
        self.failed_tasks = failed_tasks
        self.progress_percent = progress_percent
        self.user_id = user_id


class JobManager:
    """Manages jobs and tasks with PostgreSQL persistence."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def create_job(
        self, job_name: str, total_tasks: int = 0, user_id: Optional[str] = None
    ) -> Job:
        job_id = str(uuid.uuid4())
        await job_repo.create(
            self._session, job_id, job_name, total_tasks=total_tasks, user_id=user_id
        )
        job = Job(
            job_id=job_id,
            job_name=job_name,
            status=JobStatus.PENDING,
            started_at=datetime.utcnow(),
            total_tasks=total_tasks,
            completed_tasks=0,
            failed_tasks=0,
            progress_percent=0.0,
            user_id=user_id,
        )
        logger.info("Created job %s: %s (user: %s)", job_id, job_name, user_id)
        return job

    async def update_job_progress(
        self,
        job_id: str,
        status: Optional[JobStatus] = None,
        total_tasks: Optional[int] = None,
        completed_tasks: Optional[int] = None,
        failed_tasks: Optional[int] = None,
        completed_at: Optional[datetime] = None,
    ) -> None:
        updates: Dict[str, Any] = {}
        if status is not None:
            updates["status"] = status.value
        if total_tasks is not None:
            updates["total_tasks"] = total_tasks
        if completed_tasks is not None:
            updates["completed_tasks"] = completed_tasks
            if total_tasks and total_tasks > 0:
                updates["progress_percent"] = (completed_tasks / total_tasks) * 100
        if failed_tasks is not None:
            updates["failed_tasks"] = failed_tasks
        if completed_at is not None:
            updates["completed_at"] = completed_at
        if updates:
            await job_repo.update(self._session, job_id, **updates)

    async def add_task(self, job_id: str, task: Task) -> None:
        await job_repo.add_task(
            self._session,
            job_id,
            task.task_id,
            task.pipeline_name,
            task.layer,
            status=task.status.value if isinstance(task.status, TaskStatus) else task.status,
            started_at=task.started_at,
        )

    async def update_task(self, job_id: str, task: Task) -> None:
        await job_repo.update_task(
            self._session,
            job_id,
            task.task_id,
            started_at=task.started_at,
            completed_at=task.completed_at,
            duration_seconds=task.duration_seconds,
            message=task.message,
            error=task.error,
            stats=task.stats,
            status=task.status.value if isinstance(task.status, TaskStatus) else task.status,
        )

    async def get_job(
        self, job_id: str, include_tasks: bool = False
    ) -> Optional[Dict[str, Any]]:
        if include_tasks:
            return await job_repo.get_with_tasks(self._session, job_id)
        return await job_repo.get(self._session, job_id)

    async def list_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        return await job_repo.list_jobs(self._session, limit=limit)

    async def get_tasks_for_job(self, job_id: str) -> List[Dict[str, Any]]:
        return await job_repo.get_tasks(self._session, job_id)


def get_job_manager(session: AsyncSession = Depends(get_db)) -> JobManager:
    """FastAPI dependency: return a JobManager bound to the request session."""
    return JobManager(session)
