"""API endpoints with the in-memory Firestore and BigQuery."""
import io
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core import exceptions as google_exceptions

from app.core.config import get_settings
from app.db.repositories.catalogue import CATALOGUE_META_ID
from tests.conftest import ADMIN_HEADERS, TOWNS, catalogue_document


def bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def create_key(client, user_id: str, is_admin: bool = False) -> str:
    response = client.post("/admin/api-keys", json={"user_id": user_id, "is_admin": is_admin}, headers=ADMIN_HEADERS)
    assert response.status_code == 201, response.text
    return response.json()["api_key"]


def seed_catalogue(fake_db, *table_names):
    for table_name in table_names:
        fake_db.seed("data_catalogue", table_name, catalogue_document(table_name))
    fake_db.seed(
        "data_catalogue",
        CATALOGUE_META_ID,
        {"version": "2.0", "catalog_generated_at": "2026-10-02T00:00:00+00:00", "drift_report": {"tables": []}},
    )


def usage_today(fake_db) -> dict:
    return fake_db.data.get("daily_usage", {}).get(datetime.now(timezone.utc).strftime("%Y-%m-%d"), {})


def test_health_and_ui(client):
    assert client.get("/health").json()["status"] == "healthy"
    assert client.get("/").status_code == 200


def test_api_key_lifecycle(client, fake_db):
    assert client.post("/admin/api-keys", json={"user_id": "x"}).status_code == 401
    assert client.post("/admin/api-keys", json={"user_id": "x"}, headers=bearer("wrong")).status_code == 403

    old_key = create_key(client, "alice")
    assert client.get("/api/me", headers=bearer(old_key)).json() == {"user_id": "alice", "is_admin": False}

    new_key = create_key(client, "alice")
    assert client.get("/api/me", headers=bearer(old_key)).status_code == 403
    assert client.get("/api/me", headers=bearer(new_key)).status_code == 200

    listed = client.get("/admin/api-keys", headers=ADMIN_HEADERS).json()
    assert [(k["user_id"], k["active"]) for k in listed] == [("alice", True)]

    assert client.request("DELETE", "/admin/api-keys/revoke", json={"api_key": new_key}, headers=ADMIN_HEADERS).status_code == 200
    assert client.get("/api/me", headers=bearer(new_key)).status_code == 403
    assert client.request("DELETE", "/admin/api-keys/delete", json={"api_key": new_key}, headers=ADMIN_HEADERS).status_code == 200
    assert client.request("DELETE", "/admin/api-keys/delete", json={"api_key": new_key}, headers=ADMIN_HEADERS).status_code == 404


def test_admin_api_key_grants_admin_routes(client):
    admin_key = create_key(client, "ops", is_admin=True)
    user_key = create_key(client, "reader")

    assert client.get("/admin/api-keys", headers=bearer(admin_key)).status_code == 200
    assert client.get("/admin/api-keys", headers=bearer(user_key)).status_code == 403


def test_silver_catalog_combines_catalogue_certifications_and_counts(client, fake_db):
    seed_catalogue(fake_db, "dim_commune", "fact_loyer")
    fake_db.seed("query_tracker", "silver_dim_commune", {"query_count": 4, "users": {"alice": {"query_count": 4}}})

    certify = client.post("/admin/tables/certify", json={"table_name": "dim_commune"}, headers=ADMIN_HEADERS)
    assert certify.status_code == 200

    body = client.get("/api/data/catalog/silver", headers=ADMIN_HEADERS).json()
    tables = {table["name"]: table for table in body["tables"]}
    assert list(tables) == ["dim_commune", "fact_loyer"]
    assert tables["dim_commune"]["certified"] is True
    assert tables["dim_commune"]["query_count"] == 4
    assert tables["fact_loyer"]["certified"] is False
    assert tables["fact_loyer"]["query_count"] == 0
    assert tables["dim_commune"]["description_fr"] == "Table dim_commune"
    assert body["last_synced"] == "2026-10-02T00:00:00+00:00"

    certifications = client.get("/admin/tables/certifications", headers=ADMIN_HEADERS).json()["certifications"]
    assert [c["table_name"] for c in certifications] == ["dim_commune"]
    assert client.post("/admin/tables/uncertify", json={"table_name": "dim_commune"}, headers=ADMIN_HEADERS).status_code == 200


def test_silver_table_detail(client, fake_db):
    seed_catalogue(fake_db, "dim_commune")

    body = client.get("/api/data/catalog/silver/dim_commune", headers=ADMIN_HEADERS).json()
    assert [field["name"] for field in body["schema"]["fields"]] == ["code_insee", "nom"]
    assert body["schema"]["fields"][0]["description"] == "Code INSEE"
    assert body["preview"] == [{"code_insee": "75056", "nom": "Paris"}]
    assert body["sources"][0]["download_url"] == "https://example.org/geo.csv"
    assert client.get("/api/data/catalog/silver/unknown", headers=ADMIN_HEADERS).status_code == 404


def test_catalog_lists_the_bigquery_datasets(client, fake_bq):
    fake_bq.add_table("odas_bronze", "logement", TOWNS)
    fake_bq.add_table("odas_bronze", "geo", TOWNS)
    fake_bq.add_table("odas_silver", "dim_commune", TOWNS)

    schemas = client.get("/api/data/catalog", headers=ADMIN_HEADERS).json()["schemas"]
    assert [table["name"] for table in schemas["bronze"]] == ["geo", "logement"]
    assert schemas["silver"] == [{"name": "dim_commune", "path": "silver/dim_commune", "version": 0}]
    assert schemas["gold"] == []

    client.get("/api/data/catalog", headers=ADMIN_HEADERS)
    assert fake_bq.list_calls == 3

    response = client.post("/admin/runtime-cache/invalidate", json={"all": True}, headers=ADMIN_HEADERS)
    assert response.status_code == 200, response.text
    client.get("/api/data/catalog", headers=ADMIN_HEADERS)
    assert fake_bq.list_calls == 6


def test_catalog_refresh_stamps_meta_and_rereads_firestore(client, fake_db):
    seed_catalogue(fake_db, "dim_commune")
    assert len(client.get("/api/data/catalog/silver", headers=ADMIN_HEADERS).json()["tables"]) == 1

    fake_db.seed("data_catalogue", "dim_epci", catalogue_document("dim_epci"))
    response = client.post("/api/data/catalog/refresh", headers=ADMIN_HEADERS)
    assert response.status_code == 200, response.text
    assert response.json()["tables_synced"] == 2
    assert fake_db.data["data_catalogue"][CATALOGUE_META_ID]["catalog_generated_at"] == response.json()["last_synced"]
    assert len(client.get("/api/data/catalog/silver", headers=ADMIN_HEADERS).json()["tables"]) == 2

    fake_db.data["data_catalogue"].pop(CATALOGUE_META_ID)
    assert client.post("/admin/catalogue/refresh", headers=ADMIN_HEADERS).status_code == 404


def test_non_admin_cannot_read_bronze(client, fake_bq):
    fake_bq.add_table("odas_bronze", "geo", TOWNS)
    user_key = create_key(client, "reader")

    assert client.get("/api/data/table/bronze/geo", headers=bearer(user_key)).status_code == 403
    assert client.post("/api/data/preview/bronze/geo", json={}, headers=bearer(user_key)).status_code == 403
    assert client.post("/api/data/query", json={"sql": "DROP TABLE x"}, headers=bearer(user_key)).status_code == 403
    response = client.post("/api/data/query", json={"sql": "SELECT * FROM odas_bronze.geo"}, headers=bearer(user_key))
    assert response.status_code == 403
    assert client.post("/api/data/query", json={"sql": "SELECT * FROM odas_bronze.geo"}, headers=ADMIN_HEADERS).status_code == 200
    assert len(fake_bq.queries) == 1


def test_query_runs_qualified_sql_pages_and_tracks_usage(client, fake_db, fake_bq):
    fake_bq.add_table("odas_silver", "dim_test", TOWNS)
    fake_bq.query_result = TOWNS.select(["nom", "population"]).sort_by([("population", "descending")])
    fake_bq.query_bytes_billed = 10 * 1024**2
    user_key = create_key(client, "bob.martin@example.org")
    sql = "SELECT nom, population FROM dim_test ORDER BY population DESC"

    response = client.post("/api/data/query", json={"sql": sql, "limit": 2}, headers=bearer(user_key))
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["columns"] == ["nom", "population"]
    assert body["data"] == [{"nom": "Paris", "population": 2100000}, {"nom": "Marseille", "population": 870000}]
    assert body["has_more"] is True

    sent_sql, job_config = fake_bq.queries[-1]
    assert sent_sql == (
        "SELECT nom, population FROM `test-project`.`odas_silver`.`dim_test` AS dim_test ORDER BY population DESC"
    )
    assert job_config.maximum_bytes_billed == get_settings().query_max_bytes_billed
    assert job_config.labels == {"app": "odas", "endpoint": "query"}

    second = client.post("/api/data/query", json={"sql": sql, "limit": 2, "offset": 2}, headers=bearer(user_key)).json()
    assert second["data"] == [{"nom": "Lyon", "population": 520000}]
    assert second["has_more"] is False
    assert fake_bq.queries[-1][0] == sent_sql

    counter = fake_db.data["query_tracker"]["silver_dim_test"]
    assert counter["query_count"] == 2
    assert counter["users"]["bob.martin@example.org"]["query_count"] == 2
    assert usage_today(fake_db)["query_bytes_billed"] == 2 * 10 * 1024**2
    seed_catalogue(fake_db, "dim_test")
    assert client.get("/api/data/catalog/silver", headers=ADMIN_HEADERS).json()["tables"][0]["query_count"] == 2


def test_query_daily_budget(client, fake_db, fake_bq):
    fake_bq.add_table("odas_silver", "dim_test", TOWNS)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fake_db.seed("daily_usage", today, {"query_bytes_billed": get_settings().query_daily_bytes_budget})

    response = client.post("/api/data/query", json={"sql": "SELECT * FROM dim_test"}, headers=ADMIN_HEADERS)
    assert response.status_code == 429
    assert fake_bq.queries == []


def test_query_errors(client, fake_bq):
    fake_bq.add_table("odas_silver", "dim_test", TOWNS)

    unknown = client.post("/api/data/query", json={"sql": "SELECT * FROM dim_unknown"}, headers=ADMIN_HEADERS)
    assert unknown.status_code == 400
    assert "not found" in unknown.json()["detail"]

    fake_bq.query_error = google_exceptions.BadRequest("Unrecognized name: foo at [1:8]")
    failed = client.post("/api/data/query", json={"sql": "SELECT foo FROM dim_test"}, headers=ADMIN_HEADERS)
    assert failed.status_code == 400
    assert failed.json()["detail"] == "Query failed: Unrecognized name: foo at [1:8]"

    fake_bq.query_error = google_exceptions.BadRequest(
        "Query exceeded limit for bytes billed", errors=[{"reason": "bytesBilledLimitExceeded"}]
    )
    too_big = client.post("/api/data/query", json={"sql": "SELECT * FROM dim_test"}, headers=ADMIN_HEADERS)
    assert too_big.status_code == 400
    assert "more than 5 GiB" in too_big.json()["detail"]


def test_table_metadata_endpoint(client, fake_bq):
    fake_bq.add_table("odas_silver", "dim_test", TOWNS)

    response = client.get("/api/data/table/silver/dim_test", headers=ADMIN_HEADERS)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["row_count"] == 3
    assert [(field["name"], field["type"]) for field in body["fields"]] == [
        ("code_insee", "STRING"),
        ("nom", "STRING"),
        ("population", "INT64"),
    ]
    assert client.get("/api/data/table/silver/unknown", headers=ADMIN_HEADERS).status_code == 404
    assert client.get("/api/data/table/silver/a.b", headers=ADMIN_HEADERS).status_code == 404


def test_preview_reads_rows_without_query_unless_filtered(client, fake_db, fake_bq):
    fake_bq.add_table("odas_bronze", "geo", TOWNS)

    plain = client.post("/api/data/preview/bronze/geo", json={"limit": 2}, headers=ADMIN_HEADERS).json()
    assert plain["data"] == TOWNS.slice(0, 2).to_pylist()
    assert (plain["total_rows"], plain["filtered_rows"], plain["preview_rows"]) == (3, 3, 2)
    assert fake_bq.queries == []

    fake_bq.query_result = TOWNS.slice(0, 1).append_column("odas_filtered_rows", pa.array([1]))
    filtered = client.post(
        "/api/data/preview/bronze/geo",
        json={"filters": [{"column": "population", "operator": ">", "value": "1000000"}], "sort_by": "nom"},
        headers=ADMIN_HEADERS,
    ).json()
    assert filtered["columns"] == ["code_insee", "nom", "population"]
    assert filtered["data"] == [{"code_insee": "75056", "nom": "Paris", "population": 2100000}]
    assert (filtered["total_rows"], filtered["filtered_rows"]) == (3, 1)
    sql, job_config = fake_bq.queries[-1]
    assert "`population` > @filter_0" in sql and "ORDER BY `nom` ASC" in sql
    assert job_config.labels["endpoint"] == "preview"


def test_silver_preview_uses_the_catalogue_copy(client, fake_db, fake_bq):
    seed_catalogue(fake_db, "dim_test")
    fake_bq.add_table("odas_silver", "dim_test", TOWNS)

    body = client.post("/api/data/preview/silver/dim_test", json={}, headers=ADMIN_HEADERS).json()
    assert body["data"] == [{"code_insee": "75056", "nom": "Paris"}]


def test_export_csv_and_parquet(client, fake_db, fake_bq):
    fake_bq.add_table("odas_silver", "dim_test", TOWNS)
    user_key = create_key(client, "reader")

    csv_response = client.get("/api/data/export/dim_test", headers=bearer(user_key))
    assert csv_response.status_code == 200, csv_response.text
    assert csv_response.text.splitlines() == [
        '"code_insee","nom","population"',
        '"75056","Paris",2100000',
        '"69123","Lyon",520000',
        '"13055","Marseille",870000',
    ]

    parquet_response = client.get("/api/data/export/dim_test?format=parquet", headers=bearer(user_key))
    assert parquet_response.status_code == 200
    assert pq.read_table(io.BytesIO(parquet_response.content)).equals(TOWNS)
    assert usage_today(fake_db)["export_bytes"] == 2 * TOWNS.nbytes

    assert client.get("/api/data/export/unknown", headers=bearer(user_key)).status_code == 404
    fake_db.data["daily_usage"][datetime.now(timezone.utc).strftime("%Y-%m-%d")]["export_bytes"] = (
        get_settings().export_daily_bytes_budget
    )
    assert client.get("/api/data/export/dim_test", headers=bearer(user_key)).status_code == 429
