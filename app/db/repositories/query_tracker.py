"""Query tracker repository (Firestore collection `query_tracker`, one document per table).

Each document holds the table total plus a `users` map of per-user counters, so listing
counts for the whole catalogue is a single collection read.
"""
from typing import Dict

from google.cloud import firestore

COLLECTION = "query_tracker"


def _count(data: Dict) -> int:
    return int(data.get("query_count") or 0)


class QueryTrackerRepository:
    async def increment(self, db: firestore.AsyncClient, table_name: str, user_id: str) -> None:
        await db.collection(COLLECTION).document(table_name).set(
            {
                "table_name": table_name,
                "query_count": firestore.Increment(1),
                "last_query_at": firestore.SERVER_TIMESTAMP,
                "users": {
                    user_id: {
                        "query_count": firestore.Increment(1),
                        "last_query_at": firestore.SERVER_TIMESTAMP,
                    }
                },
            },
            merge=True,
        )

    async def get_table_total(self, db: firestore.AsyncClient, table_name: str) -> int:
        snapshot = await db.collection(COLLECTION).document(table_name).get()
        if not snapshot.exists:
            return 0
        return _count(snapshot.to_dict() or {})

    async def get_all_totals(self, db: firestore.AsyncClient) -> Dict[str, int]:
        return {
            snapshot.id: _count(snapshot.to_dict() or {})
            async for snapshot in db.collection(COLLECTION).stream()
        }


query_tracker_repo = QueryTrackerRepository()
