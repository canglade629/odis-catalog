"""Query usage tracking utilities."""
import logging
from google.cloud import firestore
from typing import Optional

logger = logging.getLogger(__name__)

async def increment_query_count(
    db: firestore.AsyncClient,
    table_name: str,
    user_id: str
) -> None:
    """
    Increment query count for a table and user in Firestore.
    
    Structure: tables/{table_name}/users/{user_id}
    Fields: query_count (int), last_query_at (timestamp)
    """
    try:
        doc_ref = db.collection('tables').document(table_name).collection('users').document(user_id)
        await doc_ref.set({
            'query_count': firestore.Increment(1),
            'last_query_at': firestore.SERVER_TIMESTAMP
        }, merge=True)
        logger.info(f"Incremented query count for table {table_name}, user {user_id}")
    except Exception as e:
        logger.error(f"Failed to increment query count: {e}")
        # Don't raise - tracking should not block queries

async def get_table_query_count(
    db: firestore.AsyncClient,
    table_name: str
) -> int:
    """
    Get total query count for a table (sum across all users).
    """
    try:
        users_ref = db.collection('tables').document(table_name).collection('users')
        users_docs = users_ref.stream()
        
        total = 0
        async for doc in users_docs:
            data = doc.to_dict()
            total += data.get('query_count', 0)
        
        return total
    except Exception as e:
        logger.error(f"Failed to get query count for {table_name}: {e}")
        return 0


