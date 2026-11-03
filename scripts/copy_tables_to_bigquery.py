#!/usr/bin/env python3
"""Copy the current Iceberg tables of the legacy Scaleway pipeline into the BigQuery datasets.

The current snapshot of every table registered in the legacy PyIceberg SQL catalog (`iceberg_tables`
in PostgreSQL) is read from Scaleway S3 and loaded into <prefix>_<layer>.<table> (odas_bronze,
odas_silver), replacing the previous copy; load jobs are free. The silver data_catalogue documents in
Firestore then lose their Iceberg-only runtime hints.

Run it after scripts/migrate_postgres_to_firestore.py, and again to resync while the pipeline still
writes to Scaleway. Do not run it once the pipeline writes to BigQuery: it would replace the
pipeline's tables with the Scaleway ones.

Reads the legacy PG_DB_* (or DATABASE_URL) and SCW_* variables from the environment / .env, and writes
with the application default credentials.

Usage:
    python scripts/copy_tables_to_bigquery.py --project odis-stream2-app --dry-run
    python scripts/copy_tables_to_bigquery.py --project odis-stream2-app [--layer silver] [--table dim_commune]
"""
import argparse
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, NamedTuple

import psycopg2
import pyarrow.parquet as pq
from dotenv import load_dotenv
from google.cloud import bigquery, firestore
from google.cloud.bigquery.format_options import ParquetOptions
from pyiceberg.table import StaticTable

from migrate_postgres_to_firestore import postgres_dsn

LEGACY_CATALOG = "odis"
LAYERS = ("bronze", "silver")


class SourceTable(NamedTuple):
    layer: str
    name: str
    metadata_location: str


class CopiedTable(NamedTuple):
    source: SourceTable
    destination: str
    rows: int
    bigquery_bytes: int
    seconds: float


def scaleway_properties() -> Dict[str, str]:
    names = ("SCW_OBJECT_STORAGE_ENDPOINT", "SCW_REGION", "SCW_ACCESS_KEY", "SCW_SECRET_KEY")
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        sys.exit(f"Set {', '.join(missing)}")
    return {
        "s3.endpoint": os.environ["SCW_OBJECT_STORAGE_ENDPOINT"],
        "s3.region": os.environ["SCW_REGION"],
        "s3.access-key-id": os.environ["SCW_ACCESS_KEY"],
        "s3.secret-access-key": os.environ["SCW_SECRET_KEY"],
        "s3.connect-timeout": "30",
        "s3.request-timeout": "120",
    }


def source_tables(layers: List[str], names: List[str]) -> List[SourceTable]:
    conn = psycopg2.connect(postgres_dsn())
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT table_namespace, table_name, metadata_location FROM iceberg_tables "
                "WHERE catalog_name = %s AND table_namespace = ANY(%s) ORDER BY 1, 2",
                (LEGACY_CATALOG, layers),
            )
            rows = cursor.fetchall()
    finally:
        conn.close()
    return [SourceTable(*row) for row in rows if not names or row[1] in names]


def load_job_config() -> bigquery.LoadJobConfig:
    parquet_options = ParquetOptions()
    parquet_options.enable_list_inference = True
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        parquet_options=parquet_options,
        decimal_target_types=["NUMERIC", "BIGNUMERIC", "STRING"],
        # Bronze keeps the source column names; characters BigQuery rejects (".", "(", ...) become "_"
        column_name_character_map="V2",
        labels={"app": "odas", "job": "copy-legacy-tables"},
    )


def copy_table(
    client: bigquery.Client, source: SourceTable, scaleway: Dict[str, str], dataset_prefix: str, workdir: str
) -> CopiedTable:
    started = time.monotonic()
    data = StaticTable.from_metadata(source.metadata_location, properties=scaleway).scan().to_arrow()
    # BigQuery cannot read Parquet columns whose name contains a dot, whatever the character map
    names = [name.replace(".", "_") for name in data.column_names]
    if len(set(names)) != len(names):
        raise ValueError(f"Replacing dots with underscores makes column names collide: {data.column_names}")
    data = data.rename_columns(names)
    path = os.path.join(workdir, f"{source.layer}.{source.name}.parquet")
    pq.write_table(data, path, compression="zstd")
    destination = f"{client.project}.{dataset_prefix}_{source.layer}.{source.name}"
    try:
        with open(path, "rb") as parquet_file:
            client.load_table_from_file(parquet_file, destination, job_config=load_job_config()).result()
    finally:
        os.remove(path)
    return CopiedTable(
        source=source,
        destination=destination,
        rows=data.num_rows,
        bigquery_bytes=client.get_table(destination).num_bytes or 0,
        seconds=time.monotonic() - started,
    )


def drop_iceberg_hints(db: firestore.Client, table_name: str) -> None:
    document = db.collection("data_catalogue").document(table_name)
    if document.get().exists:
        document.update({
            "runtime_hints.metadata_location": firestore.DELETE_FIELD,
            "runtime_hints.snapshot_id": firestore.DELETE_FIELD,
        })


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Copy the current legacy Iceberg tables into BigQuery.")
    parser.add_argument("--project", required=True, help="Target GCP project (no default on purpose)")
    parser.add_argument("--dataset-prefix", default="odas", help="Datasets are <prefix>_bronze and <prefix>_silver")
    parser.add_argument("--database", default="(default)", help="Firestore database of the catalogue")
    parser.add_argument("--layer", action="append", choices=LAYERS, help="Layer to copy (repeatable, default: all)")
    parser.add_argument("--table", action="append", default=[], help="Table to copy (repeatable, default: all)")
    parser.add_argument("--workers", type=int, default=4, help="Tables copied in parallel")
    parser.add_argument("--dry-run", action="store_true", help="List the tables to copy, write nothing")
    args = parser.parse_args()

    scaleway = scaleway_properties()
    tables = source_tables(args.layer or list(LAYERS), args.table)
    if not tables:
        sys.exit("No matching table in the legacy catalog")

    if args.dry_run:
        total_bytes = 0
        for source in tables:
            snapshot = StaticTable.from_metadata(source.metadata_location, properties=scaleway).current_snapshot()
            summary = snapshot.summary if snapshot else {}
            total_bytes += int(summary.get("total-files-size", 0))
            print(f"{source.layer}.{source.name}: {int(summary.get('total-records', 0)):,} rows")
        print(f"Dry run: {len(tables)} tables, {total_bytes / 1e6:.0f} MB of Parquet on Scaleway, nothing written.")
        return

    client = bigquery.Client(project=args.project)
    db = firestore.Client(project=args.project, database=args.database)
    failed = []
    total_bytes = 0
    with tempfile.TemporaryDirectory() as workdir, ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(copy_table, client, source, scaleway, args.dataset_prefix, workdir): source
            for source in tables
        }
        for future in as_completed(futures):
            source = futures[future]
            label = f"{source.layer}.{source.name}"
            try:
                copied = future.result()
                if source.layer == "silver":
                    drop_iceberg_hints(db, source.name)
            except Exception as exc:
                failed.append(label)
                print(f"{label}: FAILED {type(exc).__name__}: {exc}")
                continue
            total_bytes += copied.bigquery_bytes
            print(f"{label}: {copied.rows:,} rows, {copied.bigquery_bytes / 1e6:.0f} MB in BigQuery, {copied.seconds:.0f}s")

    print(f"Copied {len(tables) - len(failed)} of {len(tables)} tables ({total_bytes / 1e9:.2f} GB in BigQuery).")
    if failed:
        sys.exit(f"Failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
