"""In-memory stand-in for the BigQuery client used by app.utils.bigquery_ops.

Tables are Arrow tables keyed by (dataset, table). Queries are recorded with their job config and
return `query_result` (or raise `query_error`); paging and total_rows follow the BigQuery client.
"""
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple

import pyarrow as pa
from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery
from google.cloud.bigquery.table import Row

_FIELD_TYPES = {
    pa.string(): "STRING",
    pa.int64(): "INTEGER",
    pa.float64(): "FLOAT",
    pa.bool_(): "BOOLEAN",
    pa.date32(): "DATE",
}


def bigquery_schema(schema: pa.Schema) -> List[bigquery.SchemaField]:
    return [
        bigquery.SchemaField(field.name, _FIELD_TYPES[field.type], mode="NULLABLE" if field.nullable else "REQUIRED")
        for field in schema
    ]


class FakeRows:
    def __init__(self, data: pa.Table, total_rows: Optional[int] = None):
        self.data = data
        self.schema = bigquery_schema(data.schema)
        self.total_rows = data.num_rows if total_rows is None else total_rows
        index = {name: position for position, name in enumerate(data.column_names)}
        self._rows = [Row(tuple(record.values()), index) for record in data.to_pylist()]

    def __iter__(self):
        return iter(self._rows)

    def to_arrow(self) -> pa.Table:
        return self.data

    def to_arrow_iterable(self, bqstorage_client=None):
        return iter(self.data.to_batches(max_chunksize=2))


class FakeJob:
    def __init__(self, data: pa.Table, bytes_billed: int, error: Optional[Exception]):
        self.data = data
        self.total_bytes_billed = bytes_billed
        self.error = error
        self.cancelled = False

    def result(self, start_index=None, max_results=None, timeout=None) -> FakeRows:
        if self.error is not None:
            raise self.error
        page = self.data.slice(start_index or 0, max_results)
        return FakeRows(page, total_rows=self.data.num_rows)

    def cancel(self) -> None:
        self.cancelled = True


class FakeBigQuery:
    project = "test-project"

    def __init__(self):
        self.tables: Dict[Tuple[str, str], pa.Table] = {}
        self.queries: List[Tuple[str, bigquery.QueryJobConfig]] = []
        self.query_result = pa.table({})
        self.query_bytes_billed = 0
        self.query_error: Optional[Exception] = None
        self.list_calls = 0

    def add_table(self, dataset: str, name: str, data: pa.Table) -> None:
        self.tables[(dataset, name)] = data

    def list_tables(self, dataset: bigquery.DatasetReference):
        self.list_calls += 1
        names = sorted(name for dataset_id, name in self.tables if dataset_id == dataset.dataset_id)
        return [SimpleNamespace(table_id=name) for name in names]

    def get_table(self, reference: bigquery.TableReference) -> bigquery.Table:
        data = self.tables.get((reference.dataset_id, reference.table_id))
        if data is None:
            raise google_exceptions.NotFound(f"Not found: Table {reference}")
        table = bigquery.Table(reference, schema=bigquery_schema(data.schema))
        table._properties.update({"numRows": str(data.num_rows), "numBytes": str(data.nbytes)})
        return table

    def list_rows(self, table: bigquery.Table, max_results: Optional[int] = None) -> FakeRows:
        data = self.tables[(table.dataset_id, table.table_id)]
        return FakeRows(data.slice(0, max_results), total_rows=data.num_rows)

    def query(self, sql: str, job_config: Optional[bigquery.QueryJobConfig] = None) -> FakeJob:
        self.queries.append((sql, job_config))
        return FakeJob(self.query_result, self.query_bytes_billed, self.query_error)
