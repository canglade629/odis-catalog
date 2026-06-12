"""SQL execution utilities backed by DuckDB + Iceberg."""
import io
import logging
import threading
from typing import Dict

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class SQLExecutor:
    """Execute SQL queries using a shared DuckDB in-memory connection."""

    def __init__(self):
        """Initialize DuckDB connection and required extensions."""
        self.conn = duckdb.connect(":memory:")
        self._lock = threading.RLock()
        self._iceberg_ready = False
        self._s3_initialized = False

    @staticmethod
    def _quote_literal(value: str) -> str:
        """Escape a SQL string literal for DuckDB SET statements."""
        return value.replace("'", "''")

    def _ensure_iceberg_extensions(self) -> None:
        """Install/load Iceberg + HTTPFS only once per executor."""
        if self._iceberg_ready:
            return
        self.conn.execute("INSTALL httpfs")
        self.conn.execute("LOAD httpfs")
        self.conn.execute("INSTALL iceberg")
        self.conn.execute("LOAD iceberg")
        self._iceberg_ready = True

    def _configure_s3(self, s3_config: Dict[str, str]) -> None:
        """Configure DuckDB S3 access once for the process."""
        if self._s3_initialized:
            return
        endpoint = s3_config["endpoint"]
        for scheme in ("https://", "http://"):
            if endpoint.startswith(scheme):
                endpoint = endpoint[len(scheme):]
                break
        self.conn.execute("SET s3_endpoint = ?", [endpoint])
        self.conn.execute("SET s3_access_key_id = ?", [s3_config["access_key_id"]])
        self.conn.execute("SET s3_secret_access_key = ?", [s3_config["secret_access_key"]])
        self.conn.execute("SET s3_region = ?", [s3_config["region"]])
        self.conn.execute("SET s3_url_style='path'")
        self._s3_initialized = True

    def register_iceberg_view(self, table_name: str, metadata_path: str, s3_config: Dict[str, str]) -> None:
        """Register an Iceberg table as a DuckDB view."""
        with self._lock:
            self._ensure_iceberg_extensions()
            self._configure_s3(s3_config)
            self.conn.execute(f"DROP VIEW IF EXISTS {table_name}")
            safe_metadata_path = self._quote_literal(metadata_path)
            self.conn.execute(
                f"CREATE VIEW {table_name} AS SELECT * FROM iceberg_scan('{safe_metadata_path}')",
            )
            logger.info("Registered Iceberg view %s from %s", table_name, metadata_path)

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        with self._lock:
            logger.debug("Executing query: %s", query)
            result = self.conn.execute(query).fetchdf()
            logger.info("Query returned %d rows", len(result))
            return result

    def get_table_schema(self, table_name: str) -> Dict[str, object]:
        """Return table schema metadata from a registered view."""
        with self._lock:
            describe_df = self.conn.execute(f"DESCRIBE SELECT * FROM {table_name}").fetchdf()
            fields = [
                {
                    "name": row["column_name"],
                    "type": str(row["column_type"]),
                    "nullable": str(row.get("null", "YES")).upper() != "NO",
                }
                for _, row in describe_df.iterrows()
            ]
            row_count = int(self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
            return {
                "fields": fields,
                "version": 0,
                "row_count": row_count,
                "num_fields": len(fields),
            }

    def export_to_parquet(self, table_name: str) -> bytes:
        """Export a registered table/view as Parquet bytes."""
        with self._lock:
            df = self.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        return buf.getvalue()

    def export_to_csv_chunks(self, table_name: str, chunk_size: int = 10_000):
        """Yield CSV string chunks for a registered table/view."""
        with self._lock:
            df = self.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        header_written = False
        for start in range(0, max(len(df), 1), chunk_size):
            chunk = df.iloc[start:start + chunk_size]
            sbuf = io.StringIO()
            chunk.to_csv(sbuf, index=False, header=not header_written)
            header_written = True
            yield sbuf.getvalue()

_executor_instance: SQLExecutor | None = None
_executor_lock = threading.Lock()


def initialize_sql_executor() -> SQLExecutor:
    """Initialize the singleton SQL executor during application startup."""
    return get_sql_executor()


def get_sql_executor() -> SQLExecutor:
    """Get the shared SQL executor singleton."""
    global _executor_instance
    if _executor_instance is None:
        with _executor_lock:
            if _executor_instance is None:
                _executor_instance = SQLExecutor()
    return _executor_instance

