"""Daily usage repository (Firestore collection `daily_usage`, one document per UTC day).

Counts the bytes BigQuery bills for the API's queries and the bytes of the exported tables, to enforce
the QUERY_DAILY_BYTES_BUDGET and EXPORT_DAILY_BYTES_BUDGET settings.
"""
from datetime import datetime, timezone
from typing import Dict

from google.cloud import firestore

COLLECTION = "daily_usage"
QUERY_BYTES_BILLED = "query_bytes_billed"
EXPORT_BYTES = "export_bytes"


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class UsageRepository:
    async def today(self, db: firestore.AsyncClient) -> Dict[str, int]:
        snapshot = await db.collection(COLLECTION).document(_today()).get()
        data = (snapshot.to_dict() or {}) if snapshot.exists else {}
        return {field: int(data.get(field) or 0) for field in (QUERY_BYTES_BILLED, EXPORT_BYTES)}

    async def add(self, db: firestore.AsyncClient, field: str, amount: int) -> None:
        await db.collection(COLLECTION).document(_today()).set(
            {field: firestore.Increment(amount), "updated_at": firestore.SERVER_TIMESTAMP},
            merge=True,
        )


usage_repo = UsageRepository()
