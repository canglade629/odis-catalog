"""Job logs repository (PostgreSQL)."""
from datetime import datetime
from typing import List, Dict, Any, Optional

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import JobLog


class JobLogRepository:
    async def add(
        self,
        session: AsyncSession,
        job_id: str,
        level: str,
        message: str,
        logger_name: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> None:
        log = JobLog(
            job_id=job_id,
            timestamp=datetime.utcnow(),
            level=level,
            message=message,
            logger_name=logger_name,
            task_id=task_id,
        )
        session.add(log)
        await session.flush()

    async def add_batch(
        self,
        session: AsyncSession,
        job_id: str,
        entries: List[Dict[str, Any]],
    ) -> None:
        for e in entries:
            log = JobLog(
                job_id=job_id,
                timestamp=e.get("timestamp", datetime.utcnow()),
                level=e.get("level"),
                message=e.get("message"),
                logger_name=e.get("logger_name"),
                task_id=e.get("task_id"),
            )
            session.add(log)
        await session.flush()

    async def list_logs(
        self,
        session: AsyncSession,
        job_id: str,
        task_id: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        q = select(JobLog).where(JobLog.job_id == job_id)
        if task_id is not None:
            q = q.where(JobLog.task_id == task_id)
        q = q.order_by(JobLog.timestamp).offset(offset).limit(limit)
        result = await session.execute(q)
        rows = result.scalars().all()
        return [
            {
                "timestamp": r.timestamp.isoformat() if r.timestamp else None,
                "level": r.level,
                "message": r.message,
                "logger_name": r.logger_name,
                "task_id": r.task_id,
            }
            for r in rows
        ]


job_log_repo = JobLogRepository()
