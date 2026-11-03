"""API keys repository (Firestore collection `api_keys`, document id = SHA-256 of the key)."""
from typing import Optional, List, Dict, Any

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

COLLECTION = "api_keys"


def _to_record(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "user_id": data.get("user_id"),
        "is_admin": bool(data.get("is_admin", False)),
        "created_at": data.get("created_at"),
        "last_used_at": data.get("last_used_at"),
        "active": bool(data.get("active", False)),
    }


class ApiKeyRepository:
    async def replace_for_user(
        self,
        db: firestore.AsyncClient,
        key_hash: str,
        user_id: str,
        is_admin: bool = False,
    ) -> int:
        """Store a new key for the user and delete their previous keys in one batch.

        Returns the number of keys that were replaced.
        """
        collection = db.collection(COLLECTION)
        query = collection.where(filter=FieldFilter("user_id", "==", user_id))
        existing = [snapshot async for snapshot in query.stream()]
        batch = db.batch()
        for snapshot in existing:
            if snapshot.id != key_hash:
                batch.delete(snapshot.reference)
        batch.set(
            collection.document(key_hash),
            {
                "user_id": user_id,
                "is_admin": is_admin,
                "created_at": firestore.SERVER_TIMESTAMP,
                "last_used_at": None,
                "active": True,
            },
        )
        await batch.commit()
        return len(existing)

    async def get_by_hash(self, db: firestore.AsyncClient, key_hash: str) -> Optional[Dict[str, Any]]:
        snapshot = await db.collection(COLLECTION).document(key_hash).get()
        if not snapshot.exists:
            return None
        return _to_record(snapshot.to_dict() or {})

    async def update_last_used(self, db: firestore.AsyncClient, key_hash: str) -> None:
        await db.collection(COLLECTION).document(key_hash).update(
            {"last_used_at": firestore.SERVER_TIMESTAMP}
        )

    async def set_active(self, db: firestore.AsyncClient, key_hash: str, active: bool) -> bool:
        ref = db.collection(COLLECTION).document(key_hash)
        snapshot = await ref.get()
        if not snapshot.exists:
            return False
        await ref.update({"active": active})
        return True

    async def delete(self, db: firestore.AsyncClient, key_hash: str) -> bool:
        ref = db.collection(COLLECTION).document(key_hash)
        snapshot = await ref.get()
        if not snapshot.exists:
            return False
        await ref.delete()
        return True

    async def list_all(self, db: firestore.AsyncClient) -> List[Dict[str, Any]]:
        return [
            {"hash": snapshot.id, **_to_record(snapshot.to_dict() or {})}
            async for snapshot in db.collection(COLLECTION).stream()
        ]


api_key_repo = ApiKeyRepository()
