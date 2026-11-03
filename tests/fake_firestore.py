"""In-memory stand-in for google.cloud.firestore.AsyncClient, limited to what the app uses."""
import copy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import NotFound
from google.cloud.firestore_v1 import transforms


def _resolve(value: Any, current: Any) -> Any:
    """Apply SERVER_TIMESTAMP / Increment sentinels the way Firestore does."""
    if value is transforms.SERVER_TIMESTAMP:
        return datetime.now(timezone.utc)
    if isinstance(value, transforms.Increment):
        return (current if isinstance(current, (int, float)) else 0) + value.value
    if isinstance(value, dict):
        return {key: _resolve(item, None) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve(item, None) for item in value]
    return copy.deepcopy(value)


def _merge(existing: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    result = copy.deepcopy(existing)
    for key, value in data.items():
        current = existing.get(key)
        if isinstance(value, dict):
            result[key] = _merge(current if isinstance(current, dict) else {}, value)
        else:
            result[key] = _resolve(value, current)
    return result


async def _iterate(items: List[Any]):
    for item in items:
        yield item


class FakeSnapshot:
    def __init__(self, reference: "FakeDocumentReference", data: Optional[Dict[str, Any]]):
        self.reference = reference
        self.id = reference.id
        self._data = data

    @property
    def exists(self) -> bool:
        return self._data is not None

    def to_dict(self) -> Optional[Dict[str, Any]]:
        return copy.deepcopy(self._data) if self._data is not None else None


class FakeDocumentReference:
    def __init__(self, client: "FakeFirestore", collection: str, doc_id: str):
        self._client = client
        self._collection = collection
        self.id = doc_id

    @property
    def _documents(self) -> Dict[str, Dict[str, Any]]:
        return self._client.data.setdefault(self._collection, {})

    async def get(self) -> FakeSnapshot:
        self._client.reads += 1
        return FakeSnapshot(self, self._documents.get(self.id))

    async def set(self, document_data: Dict[str, Any], merge: bool = False) -> None:
        self._client.writes += 1
        if merge:
            self._documents[self.id] = _merge(self._documents.get(self.id, {}), document_data)
        else:
            self._documents[self.id] = _resolve(document_data, None)

    async def update(self, field_updates: Dict[str, Any]) -> None:
        if self.id not in self._documents:
            raise NotFound(f"{self._collection}/{self.id}")
        self._client.writes += 1
        current = self._documents[self.id]
        for key, value in field_updates.items():
            current[key] = _resolve(value, current.get(key))

    async def delete(self) -> None:
        self._client.writes += 1
        self._documents.pop(self.id, None)


class FakeQuery:
    def __init__(self, client: "FakeFirestore", collection: str, filters: Optional[list] = None):
        self._client = client
        self._collection = collection
        self._filters = filters or []

    def where(self, *, filter) -> "FakeQuery":
        if filter.op_string != "==":
            raise NotImplementedError(filter.op_string)
        return FakeQuery(self._client, self._collection, self._filters + [filter])

    def _snapshots(self) -> List[FakeSnapshot]:
        documents = self._client.data.get(self._collection, {})
        snapshots = []
        for doc_id, data in list(documents.items()):
            if all(data.get(f.field_path) == f.value for f in self._filters):
                self._client.reads += 1
                snapshots.append(FakeSnapshot(FakeDocumentReference(self._client, self._collection, doc_id), data))
        return snapshots

    def stream(self):
        return _iterate(self._snapshots())

    async def get(self) -> List[FakeSnapshot]:
        return self._snapshots()


class FakeCollection(FakeQuery):
    def document(self, document_id: str) -> FakeDocumentReference:
        return FakeDocumentReference(self._client, self._collection, document_id)


class FakeWriteBatch:
    def __init__(self):
        self._operations = []

    def set(self, reference: FakeDocumentReference, document_data: Dict[str, Any], merge: bool = False) -> None:
        self._operations.append(lambda: reference.set(document_data, merge=merge))

    def update(self, reference: FakeDocumentReference, field_updates: Dict[str, Any]) -> None:
        self._operations.append(lambda: reference.update(field_updates))

    def delete(self, reference: FakeDocumentReference) -> None:
        self._operations.append(reference.delete)

    async def commit(self) -> None:
        for operation in self._operations:
            await operation()


class FakeFirestore:
    """Collections are stored as data[collection][doc_id] = dict; reads/writes are counted."""

    project = "test-project"

    def __init__(self):
        self.data: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.reads = 0
        self.writes = 0

    def collection(self, name: str) -> FakeCollection:
        return FakeCollection(self, name)

    def batch(self) -> FakeWriteBatch:
        return FakeWriteBatch()

    def seed(self, collection: str, doc_id: str, data: Dict[str, Any]) -> None:
        self.data.setdefault(collection, {})[doc_id] = copy.deepcopy(data)
