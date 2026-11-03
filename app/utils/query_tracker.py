"""Query usage tracking (Firestore-backed)."""
import logging
from typing import Dict

from google.cloud import firestore

from app.db.repositories.query_tracker import query_tracker_repo

logger = logging.getLogger(__name__)


async def increment_query_count(
    db: firestore.AsyncClient,
    table_name: str,
    user_id: str,
) -> None:
    """Increment query count for a table and user in Firestore."""
    try:
        await query_tracker_repo.increment(db, table_name, user_id)
        logger.info("Incremented query count for table %s, user %s", table_name, user_id)
    except Exception as e:
        logger.error("Failed to increment query count: %s", e)


async def get_table_query_count(db: firestore.AsyncClient, table_name: str) -> int:
    """Get total query count for a table (all users)."""
    try:
        return await query_tracker_repo.get_table_total(db, table_name)
    except Exception as e:
        logger.error("Failed to get query count for %s: %s", table_name, e)
        return 0


async def get_query_counts(db: firestore.AsyncClient) -> Dict[str, int]:
    """Get total query counts for every tracked table, keyed by tracker table name."""
    try:
        return await query_tracker_repo.get_all_totals(db)
    except Exception as e:
        logger.error("Failed to get query counts: %s", e)
        return {}
