"""Firestore repositories against the in-memory client."""
import asyncio
from datetime import datetime, timedelta, timezone

from app.core.api_key_manager import create_api_key, hash_api_key, validate_api_key
from app.db.repositories.api_keys import api_key_repo
from app.db.repositories.catalogue import CATALOGUE_META_ID, LEGACY_CATALOGUE_ID, catalogue_repo
from app.db.repositories.certifications import certification_repo
from app.db.repositories.query_tracker import query_tracker_repo
from tests.conftest import catalogue_document


def run(coroutine):
    return asyncio.run(coroutine)


def test_create_api_key_replaces_previous_key(fake_db):
    first = run(create_api_key("alice", fake_db))
    second = run(create_api_key("alice", fake_db, is_admin=True))

    assert first["replaced"] is False
    assert second["replaced"] is True
    keys = fake_db.data["api_keys"]
    assert list(keys) == [hash_api_key(second["api_key"])]
    assert keys[hash_api_key(second["api_key"])]["is_admin"] is True
    assert isinstance(keys[hash_api_key(second["api_key"])]["created_at"], datetime)


def test_validate_api_key_throttles_last_used_writes(fake_db):
    created = run(create_api_key("bob", fake_db))
    key_hash = hash_api_key(created["api_key"])

    assert run(validate_api_key(created["api_key"], fake_db))["user_id"] == "bob"
    first_stamp = fake_db.data["api_keys"][key_hash]["last_used_at"]
    assert isinstance(first_stamp, datetime)

    writes_before = fake_db.writes
    run(validate_api_key(created["api_key"], fake_db))
    assert fake_db.writes == writes_before

    fake_db.data["api_keys"][key_hash]["last_used_at"] = datetime.now(timezone.utc) - timedelta(minutes=6)
    run(validate_api_key(created["api_key"], fake_db))
    assert fake_db.writes == writes_before + 1


def test_revoked_and_unknown_keys_are_rejected(fake_db):
    created = run(create_api_key("carol", fake_db))
    key_hash = hash_api_key(created["api_key"])

    assert run(api_key_repo.set_active(fake_db, key_hash, False)) is True
    assert run(validate_api_key(created["api_key"], fake_db)) is None
    assert run(validate_api_key("sk_live_unknown", fake_db)) is None
    assert run(api_key_repo.set_active(fake_db, "missing", False)) is False
    assert run(api_key_repo.delete(fake_db, key_hash)) is True
    assert run(api_key_repo.list_all(fake_db)) == []


def test_certifications_round_trip(fake_db):
    run(certification_repo.set(fake_db, "silver", "dim_commune", "admin"))

    status = run(certification_repo.get(fake_db, "silver", "dim_commune"))
    assert status["certified"] is True
    assert status["certified_by"] == "admin"
    assert isinstance(status["certified_at"], str)
    assert "silver_dim_commune" in fake_db.data["table_certifications"]
    assert [c["table_name"] for c in run(certification_repo.list_all(fake_db))] == ["dim_commune"]

    assert run(certification_repo.delete(fake_db, "silver", "dim_commune")) is True
    assert run(certification_repo.is_certified(fake_db, "silver", "dim_commune")) is False
    assert run(certification_repo.delete(fake_db, "silver", "dim_commune")) is False


def test_query_tracker_counts_per_table_and_user(fake_db):
    for user_id in ("alice", "alice", "bob.martin@example.org"):
        run(query_tracker_repo.increment(fake_db, "silver_dim_commune", user_id))
    run(query_tracker_repo.increment(fake_db, "silver_fact_loyer", "alice"))

    document = fake_db.data["query_tracker"]["silver_dim_commune"]
    assert document["query_count"] == 3
    assert document["users"]["alice"]["query_count"] == 2
    assert document["users"]["bob.martin@example.org"]["query_count"] == 1
    assert run(query_tracker_repo.get_table_total(fake_db, "silver_dim_commune")) == 3
    assert run(query_tracker_repo.get_table_total(fake_db, "silver_unknown")) == 0
    assert run(query_tracker_repo.get_all_totals(fake_db)) == {"silver_dim_commune": 3, "silver_fact_loyer": 1}


def test_catalogue_lists_tables_and_caches_reads(fake_db):
    fake_db.seed("data_catalogue", "dim_commune", catalogue_document("dim_commune"))
    fake_db.seed("data_catalogue", CATALOGUE_META_ID, {"version": "2", "updated_at": datetime.now(timezone.utc)})
    fake_db.seed("data_catalogue", LEGACY_CATALOGUE_ID, {"tables": {}})

    rows = run(catalogue_repo.list_table_rows(fake_db))
    assert [table_id for table_id, _ in rows] == ["dim_commune"]
    assert run(catalogue_repo.get_meta_row(fake_db)) == {"version": "2"}

    reads_after_first_load = fake_db.reads
    rows[0][1]["business_metadata"]["description"] = "mutated by caller"
    assert run(catalogue_repo.get_table_row(fake_db, "dim_commune"))["business_metadata"]["description"] == "Table dim_commune"
    assert fake_db.reads == reads_after_first_load

    run(catalogue_repo.set(fake_db, catalogue_document("dim_epci"), doc_id="dim_epci"))
    assert isinstance(fake_db.data["data_catalogue"]["dim_epci"]["updated_at"], datetime)
    assert sorted(table_id for table_id, _ in run(catalogue_repo.list_table_rows(fake_db))) == ["dim_commune", "dim_epci"]
