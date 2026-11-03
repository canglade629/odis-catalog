"""User SQL validation and preview SQL generation."""
import pytest

from app.utils import bigquery_ops
from app.utils.bigquery_ops import QueryRejected, prepare_user_query, preview_query
from tests.conftest import TOWNS

TABLES = {"silver": ["dim_commune", "fact_loyer"], "bronze": ["geo"], "gold": []}
ALL_LAYERS = bigquery_ops.LAYERS


def prepare(sql, layers=("silver",)):
    return prepare_user_query(sql, layers, table_names=lambda layer: TABLES[layer])


@pytest.fixture(autouse=True)
def bigquery(fake_bq):
    return fake_bq


def test_tables_are_qualified_with_the_project_and_datasets():
    prepared = prepare(
        "WITH recent AS (SELECT * FROM Fact_Loyer) "
        "SELECT c.nom FROM dim_commune c JOIN recent USING (code_insee) "
        "JOIN `odas_bronze.geo` g ON TRUE JOIN bronze.geo AS g2 ON TRUE",
        ALL_LAYERS,
    )
    assert sorted(prepared.tables) == [("bronze", "geo"), ("silver", "dim_commune"), ("silver", "fact_loyer")]
    assert "FROM `test-project`.`odas_silver`.`fact_loyer` AS Fact_Loyer" in prepared.sql
    assert "FROM `test-project`.`odas_silver`.`dim_commune` AS c" in prepared.sql
    assert "JOIN recent USING (code_insee)" in prepared.sql
    assert "JOIN `test-project.odas_bronze.geo` AS g ON TRUE" in prepared.sql
    assert "JOIN `test-project`.`odas_bronze`.`geo` AS g2 ON TRUE" in prepared.sql


def test_fully_qualified_names_of_the_project_are_accepted():
    prepared = prepare("SELECT * FROM `test-project.odas_silver.dim_commune`")
    assert prepared.tables == [("silver", "dim_commune")]


@pytest.mark.parametrize(
    ("sql", "status_code", "message"),
    [
        ("DROP TABLE dim_commune", 403, "single SELECT"),
        ("SELECT 1; SELECT 2", 403, "single SELECT"),
        ("INSERT INTO dim_commune SELECT * FROM dim_commune", 403, "single SELECT"),
        ("SELECT * FROM odas_bronze.geo", 403, "requires admin"),
        ("SELECT * FROM `other-project.odas_silver.dim_commune`", 403, "Only the ODAS datasets"),
        ("SELECT * FROM odis_logs.requests", 403, "Unknown dataset"),
        ("SELECT * FROM `region-eu`.INFORMATION_SCHEMA.JOBS", 403, "Unknown dataset"),
        ("SELECT * FROM odas_silver.INFORMATION_SCHEMA.TABLES", 400, "Table not found"),
        ("SELECT * FROM EXTERNAL_QUERY('connection', 'SELECT 1')", 403, "Table functions"),
        ("SELECT * FROM odas_silver.dim_*", 400, "Table not found"),
        ("SELECT * FROM dim_unknown", 400, "Table not found"),
        ("SELECT * FROM", 400, "Invalid SQL"),
    ],
)
def test_rejected_queries(sql, status_code, message):
    with pytest.raises(QueryRejected) as rejected:
        prepare(sql)
    assert rejected.value.status_code == status_code
    assert message in str(rejected.value)


def test_too_many_tables(monkeypatch):
    monkeypatch.setattr(bigquery_ops, "MAX_TABLES_PER_QUERY", 1)
    with pytest.raises(QueryRejected, match="max allowed is 1"):
        prepare("SELECT * FROM dim_commune, fact_loyer")


def test_preview_query_filters_and_sorts(fake_bq):
    fake_bq.add_table("odas_bronze", "geo", TOWNS)
    table = bigquery_ops.get_table("bronze", "geo")
    filters = [
        type("Filter", (), {"column": "nom", "operator": "contains", "value": "par"}),
        type("Filter", (), {"column": "population", "operator": ">=", "value": "500000"}),
        type("Filter", (), {"column": "code_insee", "operator": "<", "value": "7"}),
        type("Filter", (), {"column": "missing", "operator": "=", "value": "x"}),
    ]
    sql, parameters = preview_query(table, filters, "population", "desc", 10)

    assert sql == (
        "WITH filtered AS (SELECT * FROM `test-project.odas_bronze.geo` WHERE "
        "STRPOS(LOWER(CAST(`nom` AS STRING)), LOWER(@filter_0)) > 0 AND `population` >= @filter_1 "
        "AND SAFE_CAST(`code_insee` AS FLOAT64) < @filter_2) "
        "SELECT *, (SELECT COUNT(*) FROM filtered) AS odas_filtered_rows FROM filtered "
        "ORDER BY `population` DESC LIMIT @limit"
    )
    assert [(p.name, p.type_, p.value) for p in parameters] == [
        ("limit", "INT64", 10),
        ("filter_0", "STRING", "par"),
        ("filter_1", "FLOAT64", 500000.0),
        ("filter_2", "FLOAT64", 7.0),
    ]


def test_preview_query_rejects_bad_filters(fake_bq):
    fake_bq.add_table("odas_bronze", "geo", TOWNS)
    table = bigquery_ops.get_table("bronze", "geo")
    with pytest.raises(ValueError, match="Unsupported filter operator"):
        preview_query(table, [type("Filter", (), {"column": "nom", "operator": "LIKE", "value": "x"})], None, "asc", 10)
    with pytest.raises(ValueError, match="must be a number"):
        preview_query(table, [type("Filter", (), {"column": "population", "operator": ">", "value": "many"})], None, "asc", 10)
