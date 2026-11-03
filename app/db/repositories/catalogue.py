"""Data catalogue repository (Firestore collection `data_catalogue`).

One document per silver table (id = table name) plus the `_catalogue_meta` document, all
written by the pipeline. The whole collection is cached per instance for
METADATA_CACHE_TTL_SECONDS to keep Firestore reads within the free quota.
"""
import copy
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import firestore

from app.core.config import get_settings

COLLECTION = "data_catalogue"
CATALOGUE_META_ID = "_catalogue_meta"
LEGACY_CATALOGUE_ID = "silver_tables"
UPDATED_AT_FIELD = "updated_at"


def _plain(value: Any) -> Any:
    """Convert Firestore timestamps to ISO strings so documents keep their JSON shape."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def _document(snapshot: Any) -> Dict[str, Any]:
    data = snapshot.to_dict() or {}
    data.pop(UPDATED_AT_FIELD, None)
    return _plain(data)


class CatalogueRepository:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cache: Optional[Tuple[float, Dict[str, Dict[str, Any]]]] = None

    def invalidate_cache(self) -> None:
        with self._lock:
            self._cache = None

    def _cached_documents(self) -> Optional[Dict[str, Dict[str, Any]]]:
        ttl = max(get_settings().metadata_cache_ttl_seconds, 0)
        with self._lock:
            if self._cache is None:
                return None
            loaded_at, documents = self._cache
            if time.monotonic() - loaded_at >= ttl:
                return None
            return documents

    async def _all_documents(self, db: firestore.AsyncClient) -> Dict[str, Dict[str, Any]]:
        documents = self._cached_documents()
        if documents is None:
            documents = {
                snapshot.id: _document(snapshot)
                async for snapshot in db.collection(COLLECTION).stream()
            }
            with self._lock:
                self._cache = (time.monotonic(), documents)
        return documents

    async def get(self, db: firestore.AsyncClient, doc_id: str) -> Optional[Dict[str, Any]]:
        documents = self._cached_documents()
        if documents is not None:
            document = documents.get(doc_id)
            return copy.deepcopy(document) if document is not None else None
        snapshot = await db.collection(COLLECTION).document(doc_id).get()
        return _document(snapshot) if snapshot.exists else None

    async def list_table_rows(self, db: firestore.AsyncClient) -> List[Tuple[str, Dict[str, Any]]]:
        """List all per-table catalogue documents (excluding the meta and legacy documents)."""
        documents = await self._all_documents(db)
        return [
            (doc_id, copy.deepcopy(document))
            for doc_id, document in documents.items()
            if doc_id not in (CATALOGUE_META_ID, LEGACY_CATALOGUE_ID)
        ]

    async def get_table_row(self, db: firestore.AsyncClient, table_name: str) -> Optional[Dict[str, Any]]:
        """Get one table catalogue document by table id."""
        return await self.get(db, table_name)

    async def get_meta_row(self, db: firestore.AsyncClient) -> Optional[Dict[str, Any]]:
        """Get the global catalogue meta document."""
        return await self.get(db, CATALOGUE_META_ID)

    async def set(
        self,
        db: firestore.AsyncClient,
        document: Dict[str, Any],
        doc_id: str,
    ) -> None:
        await db.collection(COLLECTION).document(doc_id).set(
            {**document, UPDATED_AT_FIELD: firestore.SERVER_TIMESTAMP}
        )
        self.invalidate_cache()

    async def upsert_meta_row(self, db: firestore.AsyncClient, document: Dict[str, Any]) -> None:
        """Upsert the global catalogue meta document."""
        await self.set(db, document, doc_id=CATALOGUE_META_ID)


catalogue_repo = CatalogueRepository()
