"""SQL execution using DuckDB with Delta and Iceberg support."""
import duckdb
import pandas as pd
from typing import Optional, Dict, Any
import logging
from app.utils.delta_ops import DeltaOperations

logger = logging.getLogger(__name__)


class SQLExecutor:
    """Execute SQL queries using DuckDB with Delta and Iceberg support."""
    
    def __init__(self):
        """Initialize DuckDB connection."""
        self.conn = duckdb.connect(":memory:")
        self._iceberg_ready = False
        # Install and load delta extension
        try:
            self.conn.execute("INSTALL delta")
            self.conn.execute("LOAD delta")
            logger.info("DuckDB Delta extension loaded")
        except Exception as e:
            logger.warning(f"Could not load Delta extension: {e}")

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

    def register_iceberg_view(self, table_name: str, metadata_path: str, s3_config: Dict[str, str]) -> None:
        """Register an Iceberg table lazily as a DuckDB view."""
        try:
            self._ensure_iceberg_extensions()
            self.conn.execute(
                "SET s3_endpoint = ?",
                [s3_config["endpoint"]],
            )
            self.conn.execute(
                "SET s3_access_key_id = ?",
                [s3_config["access_key_id"]],
            )
            self.conn.execute(
                "SET s3_secret_access_key = ?",
                [s3_config["secret_access_key"]],
            )
            self.conn.execute(
                "SET s3_region = ?",
                [s3_config["region"]],
            )
            self.conn.execute("SET s3_url_style='path'")

            # Avoid collision if view already exists in this executor.
            self.conn.execute(f"DROP VIEW IF EXISTS {table_name}")
            self.conn.execute(
                f"CREATE VIEW {table_name} AS SELECT * FROM iceberg_scan(?)",
                [metadata_path],
            )
            logger.info("Registered Iceberg view %s from %s", table_name, metadata_path)
        except Exception as e:
            logger.error("Failed to register Iceberg view %s: %s", table_name, e)
            raise
    
    def register_delta_table(self, table_name: str, delta_path: str) -> None:
        """
        Register a Delta table for SQL queries.
        
        Args:
            table_name: Name to use in SQL queries
            delta_path: Path to table (s3://...)
        """
        try:
            # Read Delta table as pandas DataFrame
            df = DeltaOperations.read_delta(delta_path)
            # Register as DuckDB table
            self.conn.register(table_name, df)
            logger.info(f"Registered Delta table {table_name} from {delta_path}")
        except Exception as e:
            logger.error(f"Failed to register table {table_name}: {e}")
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query results as pandas DataFrame
        """
        logger.info(f"Executing SQL query")
        logger.debug(f"Query: {query}")
        
        result = self.conn.execute(query).fetchdf()
        logger.info(f"Query returned {len(result)} rows")
        return result
    
    def execute_merge(
        self,
        target_table: str,
        source_table: str,
        merge_condition: str,
        update_set: Dict[str, str],
        insert_columns: list,
        insert_values: list
    ) -> pd.DataFrame:
        """
        Execute a MERGE statement (simulated in DuckDB).
        
        Since DuckDB doesn't support MERGE directly, we simulate it with INSERT/UPDATE logic.
        
        Args:
            target_table: Target table name
            source_table: Source table name
            merge_condition: Join condition
            update_set: Dictionary of column -> expression for updates
            insert_columns: List of columns for insert
            insert_values: List of values/expressions for insert
            
        Returns:
            Merged DataFrame
        """
        # This is a simplified approach - for complex merges, 
        # we'll handle the logic in Python/pandas
        
        # Get source and target
        source_df = self.conn.execute(f"SELECT * FROM {source_table}").fetchdf()
        
        try:
            target_df = self.conn.execute(f"SELECT * FROM {target_table}").fetchdf()
        except:
            # Target doesn't exist, just return source
            return source_df
        
        # For now, return source - the actual merge logic will be in pipeline classes
        return source_df
    
    def close(self):
        """Close the DuckDB connection."""
        self.conn.close()


def get_sql_executor() -> SQLExecutor:
    """Get a new SQL executor instance."""
    return SQLExecutor()

