"""Table certifications repository (Firestore collection `table_certifications`, id = `<layer>_<table>`)."""
from datetime import datetime
from typing import Optional, Dict, List, Any

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

COLLECTION = "table_certifications"


def _doc_id(layer: str, table_name: str) -> str:
    return f"{layer}_{table_name}"


def _to_status(data: Dict[str, Any]) -> Dict:
    certified_at = data.get("certified_at")
    return {
        "layer": data.get("layer"),
        "table_name": data.get("table_name"),
        "certified": bool(data.get("certified", False)),
        "certified_at": certified_at.isoformat() if isinstance(certified_at, datetime) else certified_at,
        "certified_by": data.get("certified_by"),
    }


class CertificationRepository:
    async def set(
        self,
        db: firestore.AsyncClient,
        layer: str,
        table_name: str,
        certified_by: str,
    ) -> None:
        await db.collection(COLLECTION).document(_doc_id(layer, table_name)).set(
            {
                "layer": layer,
                "table_name": table_name,
                "certified": True,
                "certified_at": firestore.SERVER_TIMESTAMP,
                "certified_by": certified_by,
            }
        )

    async def delete(self, db: firestore.AsyncClient, layer: str, table_name: str) -> bool:
        ref = db.collection(COLLECTION).document(_doc_id(layer, table_name))
        snapshot = await ref.get()
        if not snapshot.exists:
            return False
        await ref.delete()
        return True

    async def get(
        self,
        db: firestore.AsyncClient,
        layer: str,
        table_name: str,
    ) -> Optional[Dict]:
        snapshot = await db.collection(COLLECTION).document(_doc_id(layer, table_name)).get()
        if not snapshot.exists:
            return None
        data = snapshot.to_dict() or {}
        if not data.get("certified"):
            return None
        return _to_status(data)

    async def is_certified(self, db: firestore.AsyncClient, layer: str, table_name: str) -> bool:
        return await self.get(db, layer, table_name) is not None

    async def list_all(self, db: firestore.AsyncClient) -> List[Dict]:
        query = db.collection(COLLECTION).where(filter=FieldFilter("certified", "==", True))
        return [_to_status(snapshot.to_dict() or {}) async for snapshot in query.stream()]


certification_repo = CertificationRepository()
