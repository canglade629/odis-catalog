"""Query tracker repository (PostgreSQL)."""
from datetime import datetime
from typing import Optional

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import QueryTracker


class QueryTrackerRepository:
    async def increment(self, session: AsyncSession, table_name: str, user_id: str) -> None:
        result = await session.execute(
            select(QueryTracker).where(
                QueryTracker.table_name == table_name,
                QueryTracker.user_id == user_id,
            )
        )
        row = result.scalars().first()
        now = datetime.utcnow()
        if row:
            row.query_count += 1
            row.last_query_at = now
        else:
            session.add(
                QueryTracker(
                    table_name=table_name,
                    user_id=user_id,
                    query_count=1,
                    last_query_at=now,
                )
            )
        await session.flush()

    async def get_table_total(self, session: AsyncSession, table_name: str) -> int:
        result = await session.execute(
            select(func.coalesce(func.sum(QueryTracker.query_count), 0)).where(
                QueryTracker.table_name == table_name
            )
        )
        return int(result.scalar() or 0)


query_tracker_repo = QueryTrackerRepository()
