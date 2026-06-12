"""Iceberg helper utilities for metadata discovery on Scaleway S3."""
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from app.core.config import get_settings
from app.utils.s3_ops import get_s3_operations

logger = logging.getLogger(__name__)

_cache_lock = threading.Lock()
_metadata_cache: Dict[str, Tuple[float, Optional[str]]] = {}


def _metadata_cache_ttl_seconds() -> int:
    """Return metadata cache TTL in seconds."""
    settings = get_settings()
    raw_ttl = getattr(settings, "metadata_cache_ttl_seconds", 300)
    try:
        ttl = int(raw_ttl)
    except (TypeError, ValueError):
        ttl = 300
    return max(ttl, 0)


def _find_latest_metadata_uncached(table_path: str) -> Optional[str]:
    """Lookup latest Iceberg metadata file under {table_path}/metadata/."""
    s3 = get_s3_operations()
    metadata_prefix = table_path.rstrip("/") + "/metadata/"
    try:
        files = s3.list_files(metadata_prefix)
    except Exception as exc:
        logger.debug("No metadata dir at %s: %s", metadata_prefix, exc)
        return None

    candidates = [f for f in files if f.endswith(".metadata.json")]
    if not candidates:
        return None

    def _sort_key(path: str) -> tuple:
        name = path.rsplit("/", 1)[-1]
        if name.startswith("v") and name[1:].split(".")[0].isdigit():
            return (1, int(name[1:].split(".")[0]))
        return (0, name)

    candidates.sort(key=_sort_key)
    return candidates[-1]


def find_latest_metadata(table_path: str) -> Optional[str]:
    """Return latest Iceberg metadata path with a small TTL cache."""
    ttl_seconds = _metadata_cache_ttl_seconds()
    now = time.time()

    if ttl_seconds > 0:
        with _cache_lock:
            cached = _metadata_cache.get(table_path)
            if cached and (now - cached[0]) < ttl_seconds:
                return cached[1]

    latest = _find_latest_metadata_uncached(table_path)
    with _cache_lock:
        _metadata_cache[table_path] = (now, latest)
    return latest


def invalidate_metadata_cache(table_path: Optional[str] = None) -> None:
    """Invalidate one cached table path or the entire metadata cache."""
    with _cache_lock:
        if table_path is None:
            _metadata_cache.clear()
            return
        _metadata_cache.pop(table_path, None)


class IcebergOperations:
    """Minimal read-only helpers retained for compatibility."""

    @staticmethod
    def table_exists(table_path: str) -> bool:
        return find_latest_metadata(table_path) is not None

    @staticmethod
    def list_iceberg_tables(base_path: str) -> List[Dict[str, Any]]:
        """List Iceberg table roots under the given base prefix."""
        s3 = get_s3_operations()
        settings = get_settings()
        bucket = settings.scw_bucket_name
        try:
            all_files = s3.list_files(base_path)
        except Exception as exc:
            logger.warning("Could not list files at %s: %s", base_path, exc)
            return []

        table_roots = set()
        for full_path in all_files:
            if "/metadata/" in full_path and full_path.endswith(".metadata.json"):
                table_roots.add(full_path.split("/metadata/")[0])

        tables: List[Dict[str, Any]] = []
        for root in sorted(table_roots):
            normalized_root = root if root.startswith("s3://") else f"s3://{bucket}/{root}"
            table_name = normalized_root.rstrip("/").rsplit("/", 1)[-1]
            tables.append({"name": table_name, "path": normalized_root})
        return tables


def get_iceberg_operations() -> IcebergOperations:
    """Return Iceberg operations helper."""
    return IcebergOperations()

