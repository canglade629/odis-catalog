"""Query usage tracking (PostgreSQL-backed)."""
import logging
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.repositories.query_tracker import query_tracker_repo

logger = logging.getLogger(__name__)


async def increment_query_count(
    session: AsyncSession,
    table_name: str,
    user_id: str,
) -> None:
    """Increment query count for a table and user in PostgreSQL."""
    try:
        await query_tracker_repo.increment(session, table_name, user_id)
        logger.info("Incremented query count for table %s, user %s", table_name, user_id)
    except Exception as e:
        logger.error("Failed to increment query count: %s", e)


async def get_table_query_count(session: AsyncSession, table_name: str) -> int:
    """Get total query count for a table (sum across all users)."""
    try:
        return await query_tracker_repo.get_table_total(session, table_name)
    except Exception as e:
        logger.error("Failed to get query count for %s: %s", table_name, e)
        return 0
