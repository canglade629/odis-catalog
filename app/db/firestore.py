"""Firestore client shared by the repositories (API keys, catalogue, certifications, query counters)."""
import logging
from typing import Optional

from google.cloud import firestore

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_client: Optional[firestore.AsyncClient] = None


def get_firestore_client() -> firestore.AsyncClient:
    """Return the process-wide async Firestore client, creating it on first use."""
    global _client
    if _client is None:
        settings = get_settings()
        _client = firestore.AsyncClient(
            project=settings.gcp_project_id or None,
            database=settings.firestore_database,
        )
        logger.info(
            "Firestore client initialized (project=%s, database=%s)",
            _client.project,
            settings.firestore_database,
        )
    return _client


async def get_db() -> firestore.AsyncClient:
    """FastAPI dependency returning the Firestore client."""
    return get_firestore_client()
