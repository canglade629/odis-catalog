#!/usr/bin/env python3
"""One-off copy of the legacy PostgreSQL app data into Firestore.

Copies api_keys, table_certifications, query_tracker (merged into one document per table, with a
`users` map) and the V2 data_catalogue documents. Document ids are the ones the API reads, so the
script can be re-run safely. The legacy `silver_tables` catalogue row is skipped.

Reads the legacy PG_DB_* variables (or DATABASE_URL) from the environment / .env.

Usage:
    python scripts/migrate_postgres_to_firestore.py --project odis-stream2-app --dry-run
    python scripts/migrate_postgres_to_firestore.py --project odis-stream2-app [--skip-catalogue]
"""
import argparse
import json
import os
import sys
from collections import Counter
from typing import Any, Dict, List, Tuple

import psycopg2
from dotenv import load_dotenv
from google.cloud import firestore

LEGACY_CATALOGUE_ID = "silver_tables"
BATCH_SIZE = 100
FIRESTORE_MAX_DOCUMENT_BYTES = 1_048_576

Document = Tuple[str, str, Dict[str, Any]]


def postgres_dsn() -> str:
    url = os.environ.get("DATABASE_URL")
    if url:
        return url.replace("postgresql+asyncpg://", "postgresql://", 1)
    missing = [name for name in ("PG_DB_HOST", "PG_DB_NAME", "PG_DB_USER", "PG_DB_PWD") if not os.environ.get(name)]
    if missing:
        sys.exit(f"Set DATABASE_URL or {', '.join(missing)}")
    return psycopg2.extensions.make_dsn(
        host=os.environ["PG_DB_HOST"],
        port=os.environ.get("PG_DB_PORT", "5432"),
        dbname=os.environ["PG_DB_NAME"],
        user=os.environ["PG_DB_USER"],
        password=os.environ["PG_DB_PWD"],
    )


def firestore_safe(value: Any, inside_array: bool = False) -> Any:
    """Firestore rejects arrays directly nested in arrays; store those as JSON strings."""
    if isinstance(value, dict):
        return {str(key): firestore_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        if inside_array:
            return json.dumps(value, ensure_ascii=False, default=str)
        return [firestore_safe(item, inside_array=True) for item in value]
    return value


def fetch(conn, sql: str) -> List[Dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(sql)
        columns = [column.name for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def build_documents(conn, include_catalogue: bool) -> List[Document]:
    documents: List[Document] = []

    for row in fetch(conn, "SELECT key_hash, user_id, is_admin, created_at, last_used_at, active FROM api_keys"):
        documents.append(("api_keys", row.pop("key_hash"), row))

    for row in fetch(
        conn,
        "SELECT id, layer, table_name, certified, certified_at, certified_by FROM table_certifications",
    ):
        documents.append(("table_certifications", row.pop("id"), row))

    trackers: Dict[str, Dict[str, Any]] = {}
    for row in fetch(conn, "SELECT table_name, user_id, query_count, last_query_at FROM query_tracker"):
        tracker = trackers.setdefault(
            row["table_name"],
            {"table_name": row["table_name"], "query_count": 0, "last_query_at": None, "users": {}},
        )
        tracker["query_count"] += row["query_count"]
        tracker["users"][row["user_id"]] = {
            "query_count": row["query_count"],
            "last_query_at": row["last_query_at"],
        }
        if tracker["last_query_at"] is None or row["last_query_at"] > tracker["last_query_at"]:
            tracker["last_query_at"] = row["last_query_at"]
    documents.extend(("query_tracker", table_name, tracker) for table_name, tracker in trackers.items())

    if include_catalogue:
        for row in fetch(conn, "SELECT id, document, updated_at FROM data_catalogue"):
            if row["id"] == LEGACY_CATALOGUE_ID:
                continue
            payload = firestore_safe(row["document"] or {})
            documents.append(("data_catalogue", row["id"], {**payload, "updated_at": row["updated_at"]}))

    return documents


def approximate_size(data: Dict[str, Any]) -> int:
    return len(json.dumps(data, default=str).encode("utf-8"))


def write(client: firestore.Client, documents: List[Document]) -> None:
    for start in range(0, len(documents), BATCH_SIZE):
        batch = client.batch()
        for collection, doc_id, data in documents[start:start + BATCH_SIZE]:
            batch.set(client.collection(collection).document(doc_id), data)
        batch.commit()


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Copy the legacy PostgreSQL app data into Firestore.")
    parser.add_argument("--project", required=True, help="Target GCP project (no default on purpose)")
    parser.add_argument("--database", default="(default)", help="Target Firestore database")
    parser.add_argument("--skip-catalogue", action="store_true", help="Leave data_catalogue to the pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Read PostgreSQL and report, write nothing")
    args = parser.parse_args()

    conn = psycopg2.connect(postgres_dsn())
    try:
        documents = build_documents(conn, include_catalogue=not args.skip_catalogue)
    finally:
        conn.close()

    for collection, count in sorted(Counter(collection for collection, _, _ in documents).items()):
        print(f"{collection}: {count} documents")
    oversized = [(c, d) for c, d, data in documents if approximate_size(data) >= FIRESTORE_MAX_DOCUMENT_BYTES]
    if oversized:
        sys.exit(f"Documents over the 1 MiB Firestore limit: {oversized}")
    if documents:
        largest = max(documents, key=lambda document: approximate_size(document[2]))
        print(f"Largest document: {largest[0]}/{largest[1]} (~{approximate_size(largest[2]) // 1024} KiB)")

    if args.dry_run:
        print("Dry run: nothing written.")
        return
    write(firestore.Client(project=args.project, database=args.database), documents)
    print(f"Wrote {len(documents)} documents to project {args.project}, database {args.database}.")


if __name__ == "__main__":
    main()
