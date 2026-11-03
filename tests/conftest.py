"""Shared fixtures: hermetic settings, in-memory Firestore and BigQuery."""
import os

# Settings are read once at import time, so the test environment must be in place before app imports.
os.environ.update(
    {
        "ADMIN_SECRET": "test-admin-secret-7c1e9b",
        "GCP_PROJECT_ID": "test-project",
        "FIRESTORE_DATABASE": "(default)",
        "ENVIRONMENT": "development",
        "CORS_ORIGINS": "*",
        "METADATA_CACHE_TTL_SECONDS": "300",
    }
)

from typing import Optional

import pyarrow as pa
import pytest

from tests.fake_bigquery import FakeBigQuery
from tests.fake_firestore import FakeFirestore

ADMIN_SECRET = os.environ["ADMIN_SECRET"]
ADMIN_HEADERS = {"Authorization": f"Bearer {ADMIN_SECRET}"}

TOWNS = pa.table(
    {
        "code_insee": ["75056", "69123", "13055"],
        "nom": ["Paris", "Lyon", "Marseille"],
        "population": [2_100_000, 520_000, 870_000],
    }
)


@pytest.fixture
def fake_db(monkeypatch):
    from app.db import firestore as firestore_module
    from app.db.repositories.catalogue import catalogue_repo

    db = FakeFirestore()
    monkeypatch.setattr(firestore_module, "_client", db)
    catalogue_repo.invalidate_cache()
    yield db
    catalogue_repo.invalidate_cache()


@pytest.fixture
def fake_bq(monkeypatch):
    from app.utils import bigquery_ops

    bq = FakeBigQuery()
    monkeypatch.setattr(bigquery_ops, "_client", bq)
    monkeypatch.setattr(bigquery_ops, "_read_client", object())
    bigquery_ops.invalidate_table_names()
    yield bq
    bigquery_ops.invalidate_table_names()


@pytest.fixture
def client(fake_db, fake_bq):
    from fastapi.testclient import TestClient

    from app.core.rate_limiter import limiter
    from app.main import app

    limiter.reset()
    with TestClient(app) as test_client:
        yield test_client


def catalogue_document(table_name: str, preview: Optional[list] = None) -> dict:
    """A V2 catalogue document as written by the pipeline's refresh_catalogue step."""
    return {
        "business_metadata": {
            "description": f"Table {table_name}",
            "category": "geo",
            "upstream_models": ["bronze_geo"],
            "tags": ["silver"],
            "sources": [{"source_key": "geo", "url": "https://example.org/geo.csv"}],
        },
        "field_docs": {"code_insee": {"description": "Code INSEE", "example": "75056"}},
        "schema_cache": {
            "fields": [
                {"name": "code_insee", "type": "VARCHAR", "nullable": False},
                {"name": "nom", "type": "VARCHAR", "nullable": True},
            ],
            "row_count": 2,
            "num_fields": 2,
            "version": 1,
        },
        "runtime_hints": {
            "preview": [{"code_insee": "75056", "nom": "Paris"}] if preview is None else preview,
        },
        "catalog_generated_at": "2026-10-01T00:00:00+00:00",
    }
