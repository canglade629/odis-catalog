"""Iceberg and Parquet read operations for S3/Scaleway (catalogue web app — read-only)."""
import decimal
import io
import json
import logging
from typing import Optional, List, Dict, Any

import pandas as pd
import pyarrow.parquet as pq

from app.core.config import get_settings
from app.utils.s3_ops import get_s3_operations

logger = logging.getLogger(__name__)


def _s3_props() -> Dict[str, str]:
    """Build PyIceberg S3 FileIO properties for Scaleway."""
    s = get_settings()
    return {
        "s3.endpoint": s.scw_object_storage_endpoint,
        "s3.access-key-id": s.scw_access_key,
        "s3.secret-access-key": s.scw_secret_key,
        "s3.region": s.scw_region,
    }


def _is_s3_path(path: str) -> bool:
    return path.startswith("s3://")


def _is_parquet_path(path: str) -> bool:
    return path.rstrip("/").endswith(".parquet")


def _strip_s3_prefix(path: str, bucket: str) -> str:
    """Return the key portion of an s3://bucket/key path."""
    prefix = f"s3://{bucket}/"
    return path[len(prefix):] if path.startswith(prefix) else path


def _find_latest_metadata(table_path: str) -> Optional[str]:
    """
    Scan {table_path}/metadata/ on S3 and return the path of the most recent
    *.metadata.json file.  Iceberg writes files as:
      - v1.metadata.json, v2.metadata.json, … (sequential)
      - or UUID-based names (fall back to last-modified / lexicographic sort)
    """
    s3 = get_s3_operations()
    metadata_prefix = table_path.rstrip("/") + "/metadata/"
    try:
        files = s3.list_files(metadata_prefix)
    except Exception as e:
        logger.debug("No metadata dir at %s: %s", metadata_prefix, e)
        return None

    candidates = [f for f in files if f.endswith(".metadata.json")]
    if not candidates:
        return None

    # Prefer vN.metadata.json; fall back to lexicographic (last = newest for UUIDs)
    def _sort_key(path: str) -> tuple:
        name = path.rsplit("/", 1)[-1]
        if name.startswith("v") and name[1:].split(".")[0].isdigit():
            return (1, int(name[1:].split(".")[0]))
        return (0, name)

    candidates.sort(key=_sort_key)
    return candidates[-1]


def _load_iceberg_table(table_path: str):
    """Load a PyIceberg StaticTable from the latest metadata file."""
    from pyiceberg.table import StaticTable

    metadata_path = _find_latest_metadata(table_path)
    if not metadata_path:
        raise FileNotFoundError(f"No Iceberg metadata found at {table_path}/metadata/")

    props = _s3_props()
    return StaticTable.from_metadata(metadata_path, properties=props)


def _sanitize_records(df: pd.DataFrame) -> List[Dict]:
    """Convert a DataFrame to JSON-safe records (handle Decimal, datetime, dict)."""
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
        elif df[col].dtype == "object":
            sample = df[col].iloc[0] if len(df) > 0 else None
            if isinstance(sample, dict):
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
            elif isinstance(sample, decimal.Decimal):
                df[col] = df[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df.to_dict(orient="records")


class DeltaOperations:
    """Read-only data operations for the catalogue web app (Iceberg + Parquet)."""

    @staticmethod
    def read_delta(table_path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """Read an Iceberg table into a pandas DataFrame."""
        logger.info("Reading Iceberg table from %s", table_path)
        if _is_parquet_path(table_path):
            return DeltaOperations.read_parquet(table_path)
        table = _load_iceberg_table(table_path)
        scan = table.scan(selected_fields=tuple(columns) if columns else None)
        df = scan.to_pandas()
        logger.info("Read %d rows from Iceberg table", len(df))
        return df

    @staticmethod
    def table_exists(table_path: str) -> bool:
        """Return True if an Iceberg table (metadata dir) exists at the given path."""
        if _is_parquet_path(table_path):
            try:
                s3 = get_s3_operations()
                s3.download_file(table_path)
                return True
            except Exception:
                return False
        return _find_latest_metadata(table_path) is not None

    @staticmethod
    def list_delta_tables(base_path: str) -> List[Dict[str, Any]]:
        """
        List all Iceberg tables under base_path by scanning for metadata/ directories.
        Returns [{name, path, snapshot_id}].
        """
        logger.info("Scanning for Iceberg tables in %s", base_path)
        s3 = get_s3_operations()
        settings = get_settings()
        bucket = settings.scw_bucket_name

        try:
            all_files = s3.list_files(base_path)
        except Exception as e:
            logger.warning("Could not list files at %s: %s", base_path, e)
            return []

        # Detect table roots: any path containing /metadata/*.metadata.json
        table_roots: set = set()
        for full_path in all_files:
            if "/metadata/" in full_path and full_path.endswith(".metadata.json"):
                # e.g. s3://bucket/bronze/my_table/metadata/v1.metadata.json
                root = full_path.split("/metadata/")[0]
                table_roots.add(root)

        tables = []
        for root in sorted(table_roots):
            table_name = root.rstrip("/").rsplit("/", 1)[-1]
            # Normalise to s3://bucket/... path
            if not root.startswith("s3://"):
                root = f"s3://{bucket}/{root}"
            try:
                table = _load_iceberg_table(root)
                snapshot_id = table.current_snapshot().snapshot_id if table.current_snapshot() else None
                tables.append({"name": table_name, "path": root, "snapshot_id": snapshot_id})
            except Exception as e:
                logger.warning("Could not load Iceberg table at %s: %s", root, e)

        logger.info("Found %d Iceberg tables", len(tables))
        return tables

    @staticmethod
    def get_table_schema(table_path: str) -> Dict[str, Any]:
        """
        Return schema info for an Iceberg table or a single Parquet file.
        Schema format: {fields: [{name, type, nullable}], version, row_count, num_fields}
        """
        logger.info("Getting schema for %s", table_path)
        if _is_parquet_path(table_path):
            return DeltaOperations._get_parquet_schema(table_path)

        table = _load_iceberg_table(table_path)
        schema = table.schema()
        fields = [
            {
                "name": field.name,
                "type": str(field.field_type),
                "nullable": field.optional,
            }
            for field in schema.fields
        ]
        snapshot = table.current_snapshot()
        snapshot_id = snapshot.snapshot_id if snapshot else 0

        try:
            row_count = table.scan().to_arrow().num_rows
        except Exception as e:
            logger.warning("Could not get row count for %s: %s", table_path, e)
            row_count = None

        return {
            "fields": fields,
            "version": snapshot_id,
            "row_count": row_count,
            "num_fields": len(fields),
        }

    @staticmethod
    def preview_table(
        table_path: str,
        limit: int = 100,
        filters: Optional[List[Dict[str, str]]] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
    ) -> Dict[str, Any]:
        """Get a preview of an Iceberg table with optional filtering and sorting."""
        logger.info("Previewing table %s (limit=%s)", table_path, limit)
        if _is_parquet_path(table_path):
            return DeltaOperations._preview_parquet(table_path, limit, filters, sort_by, sort_order)

        table = _load_iceberg_table(table_path)
        df = table.scan().to_pandas()
        total_rows = len(df)

        if filters:
            for spec in filters:
                col = spec.get("column")
                op = spec.get("operator", "=")
                val = spec.get("value")
                if col and col in df.columns:
                    try:
                        if op == "=":
                            df = df[df[col] == val]
                        elif op == "!=":
                            df = df[df[col] != val]
                        elif op == "contains":
                            df = df[df[col].astype(str).str.contains(str(val), case=False, na=False)]
                        elif op == ">":
                            df = df[df[col] > float(val)]
                        elif op == "<":
                            df = df[df[col] < float(val)]
                        elif op == ">=":
                            df = df[df[col] >= float(val)]
                        elif op == "<=":
                            df = df[df[col] <= float(val)]
                    except Exception as e:
                        logger.warning("Filter failed for %s %s %s: %s", col, op, val, e)

        filtered_rows = len(df)

        if sort_by and sort_by in df.columns:
            df = df.sort_values(by=sort_by, ascending=(sort_order.lower() == "asc"))

        df_preview = df.head(limit)
        records = _sanitize_records(df_preview)

        return {
            "columns": list(df_preview.columns),
            "data": records,
            "total_rows": total_rows,
            "filtered_rows": filtered_rows,
            "preview_rows": len(records),
        }

    # ------------------------------------------------------------------
    # Parquet helpers (legacy / single-file fallback)
    # ------------------------------------------------------------------

    @staticmethod
    def read_parquet(table_path: str) -> pd.DataFrame:
        """Read a single Parquet file from S3 into a pandas DataFrame."""
        s3 = get_s3_operations()
        content = s3.download_file(table_path)
        table = pq.read_table(io.BytesIO(content))
        return table.to_pandas()

    @staticmethod
    def _get_parquet_schema(table_path: str) -> Dict[str, Any]:
        """Read schema from a single Parquet file on S3."""
        s3 = get_s3_operations()
        content = s3.download_file(table_path)
        table = pq.read_table(io.BytesIO(content))
        schema = table.schema
        fields = [
            {"name": f.name, "type": str(f.type), "nullable": f.nullable}
            for f in schema
        ]
        return {
            "fields": fields,
            "version": 0,
            "row_count": table.num_rows,
            "num_fields": len(fields),
        }

    @staticmethod
    def _preview_parquet(
        table_path: str,
        limit: int,
        filters: Optional[List[Dict[str, str]]],
        sort_by: Optional[str],
        sort_order: str,
    ) -> Dict[str, Any]:
        """Preview a single Parquet file on S3."""
        s3 = get_s3_operations()
        content = s3.download_file(table_path)
        tbl = pq.read_table(io.BytesIO(content))
        df = tbl.to_pandas()
        total_rows = len(df)

        if filters:
            for spec in filters:
                col = spec.get("column")
                op = spec.get("operator", "=")
                val = spec.get("value")
                if col and col in df.columns:
                    try:
                        if op == "=":
                            df = df[df[col] == val]
                        elif op == "!=":
                            df = df[df[col] != val]
                        elif op == "contains":
                            df = df[df[col].astype(str).str.contains(str(val), case=False, na=False)]
                        elif op == ">":
                            df = df[df[col] > float(val)]
                        elif op == "<":
                            df = df[df[col] < float(val)]
                        elif op == ">=":
                            df = df[df[col] >= float(val)]
                        elif op == "<=":
                            df = df[df[col] <= float(val)]
                    except Exception as e:
                        logger.warning("Filter failed for %s %s %s: %s", col, op, val, e)

        filtered_rows = len(df)
        if sort_by and sort_by in df.columns:
            df = df.sort_values(by=sort_by, ascending=(sort_order.lower() == "asc"))

        df_preview = df.head(limit)
        records = _sanitize_records(df_preview)

        return {
            "columns": list(df_preview.columns),
            "data": records,
            "total_rows": total_rows,
            "filtered_rows": filtered_rows,
            "preview_rows": len(records),
        }


def get_delta_operations() -> DeltaOperations:
    """Return the DeltaOperations helper (kept for backward compat)."""
    return DeltaOperations()
