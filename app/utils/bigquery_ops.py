"""BigQuery access to the data layers: one dataset per layer, <prefix>_bronze, <prefix>_silver and <prefix>_gold."""
import io
import itertools
import logging
import re
import threading
import time
from concurrent.futures import TimeoutError as FuturesTimeout
from typing import Any, Callable, Dict, Iterable, Iterator, List, NamedTuple, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import sqlglot
from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery
from sqlglot import exp

from app.core.config import get_settings

logger = logging.getLogger(__name__)

LAYERS = ("bronze", "silver", "gold")
MAX_TABLES_PER_QUERY = 10
MAX_SQL_LENGTH = 100_000
QUERY_TIMEOUT_SECONDS = 60
PREVIEW_OPERATORS = ("=", "!=", "contains", ">", "<", ">=", "<=")
FILTERED_ROWS_COLUMN = "odas_filtered_rows"

_TABLE_NAME = re.compile(r"[\w-]+")
_NUMERIC_TYPES = {"INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"}
_UNORDERABLE_TYPES = {"RECORD", "STRUCT", "GEOGRAPHY", "JSON"}
_STANDARD_TYPE_NAMES = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL", "RECORD": "STRUCT"}

_client_lock = threading.Lock()
_client: Optional[bigquery.Client] = None
_read_client: Any = None
_cache_lock = threading.Lock()
_table_names: Dict[str, Tuple[float, List[str]]] = {}


class QueryRejected(Exception):
    """SQL the API refuses to run; status_code is the HTTP status to answer with."""

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


class QueryTimeout(Exception):
    """The query did not finish within QUERY_TIMEOUT_SECONDS (BigQuery cancels it)."""


class Page(NamedTuple):
    columns: List[str]
    rows: List[Dict[str, Any]]
    total_rows: Optional[int]  # Rows of the whole result, not only of this page
    bytes_billed: int


class PreparedQuery(NamedTuple):
    sql: str
    tables: List[Tuple[str, str]]  # (layer, table name) of the tables the query reads


def get_client() -> bigquery.Client:
    global _client
    with _client_lock:
        if _client is None:
            settings = get_settings()
            _client = bigquery.Client(project=settings.gcp_project_id or None, location=settings.bigquery_location)
        return _client


def _get_read_client() -> Any:
    """Storage Read API client, used for exports."""
    global _read_client
    with _client_lock:
        if _read_client is None:
            from google.cloud import bigquery_storage

            _read_client = bigquery_storage.BigQueryReadClient()
        return _read_client


def dataset_id(layer: str) -> str:
    return f"{get_settings().bigquery_dataset_prefix}_{layer}"


def table_reference(layer: str, table_name: str) -> bigquery.TableReference:
    return bigquery.DatasetReference(get_client().project, dataset_id(layer)).table(table_name)


def list_table_names(layer: str) -> List[str]:
    """Names of the tables of a layer, cached for METADATA_CACHE_TTL_SECONDS (listing is free)."""
    ttl = max(get_settings().metadata_cache_ttl_seconds, 0)
    with _cache_lock:
        cached = _table_names.get(layer)
        if cached and time.monotonic() - cached[0] < ttl:
            return list(cached[1])
    client = get_client()
    try:
        names = sorted(table.table_id for table in client.list_tables(bigquery.DatasetReference(client.project, dataset_id(layer))))
    except google_exceptions.NotFound:
        names = []
    with _cache_lock:
        _table_names[layer] = (time.monotonic(), names)
    return list(names)


def invalidate_table_names() -> None:
    with _cache_lock:
        _table_names.clear()


def get_table(layer: str, table_name: str) -> Optional[bigquery.Table]:
    """Table metadata (schema, num_rows, num_bytes), or None when the table does not exist."""
    if not _TABLE_NAME.fullmatch(table_name):
        return None
    try:
        return get_client().get_table(table_reference(layer, table_name))
    except google_exceptions.NotFound:
        return None


def _type_name(field: bigquery.SchemaField) -> str:
    name = _STANDARD_TYPE_NAMES.get(field.field_type, field.field_type)
    if name == "STRUCT":
        name = f"STRUCT<{', '.join(f'{sub.name} {_type_name(sub)}' for sub in field.fields)}>"
    return f"ARRAY<{name}>" if field.mode == "REPEATED" else name


def schema_fields(table: bigquery.Table) -> List[Dict[str, Any]]:
    return [
        {"name": field.name, "type": _type_name(field), "nullable": field.mode != "REQUIRED"}
        for field in table.schema
    ]


def _records(rows: Iterable[bigquery.Row]) -> List[Dict[str, Any]]:
    return [dict(row.items()) for row in rows]


def head(table: bigquery.Table, limit: int) -> Page:
    """First rows of a table, read without a query (free)."""
    rows = get_client().list_rows(table, max_results=limit)
    records = _records(rows)
    return Page([field.name for field in rows.schema], records, table.num_rows, 0)


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("\\", "\\\\").replace("`", "\\`") + "`"


def _is_scalar(field: bigquery.SchemaField) -> bool:
    return field.mode != "REPEATED" and field.field_type not in ("RECORD", "STRUCT")


def preview_query(
    table: bigquery.Table,
    filters: Sequence[Any],
    sort_by: Optional[str],
    sort_order: str,
    limit: int,
) -> Tuple[str, List[bigquery.ScalarQueryParameter]]:
    """SQL and parameters of a filtered or sorted preview, with the filtered row count in FILTERED_ROWS_COLUMN.

    Filters have column, operator (PREVIEW_OPERATORS) and value attributes. Filters on unknown columns
    and sorts on unknown or unorderable columns are ignored. Raises ValueError for an unsupported
    operator or a non-numeric value compared to a number.
    """
    fields = {field.name: field for field in table.schema}
    conditions: List[str] = []
    parameters = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    for index, spec in enumerate(filters):
        field = fields.get(spec.column)
        if field is None:
            continue
        if spec.operator not in PREVIEW_OPERATORS:
            raise ValueError(f"Unsupported filter operator {spec.operator!r}; use one of {', '.join(PREVIEW_OPERATORS)}.")
        column, name = _quote(field.name), f"filter_{index}"
        as_text = f"CAST({column} AS STRING)" if _is_scalar(field) else f"TO_JSON_STRING({column})"
        compares_numbers = (
            spec.operator in (">", "<", ">=", "<=")
            and _is_scalar(field)
            and (field.field_type in _NUMERIC_TYPES or field.field_type == "STRING")
        )
        if spec.operator == "contains":
            condition, value_type, value = f"STRPOS(LOWER({as_text}), LOWER(@{name})) > 0", "STRING", spec.value
        elif compares_numbers:
            try:
                value = float(spec.value)
            except ValueError:
                raise ValueError(f"Filter value for {spec.operator} on {field.name} must be a number.") from None
            target = column if field.field_type in _NUMERIC_TYPES else f"SAFE_CAST({column} AS FLOAT64)"
            condition, value_type = f"{target} {spec.operator} @{name}", "FLOAT64"
        else:
            # Equality, and comparisons of dates, timestamps, ... on their text form (ISO, so ordered)
            condition, value_type, value = f"{as_text} {spec.operator} @{name}", "STRING", spec.value
        conditions.append(condition)
        parameters.append(bigquery.ScalarQueryParameter(name, value_type, value))

    order = ""
    sort_field = fields.get(sort_by) if sort_by else None
    if sort_field is not None and _is_scalar(sort_field) and sort_field.field_type not in _UNORDERABLE_TYPES:
        order = f" ORDER BY {_quote(sort_field.name)} {'DESC' if sort_order.lower() == 'desc' else 'ASC'}"
    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = (
        f"WITH filtered AS (SELECT * FROM {_quote(f'{table.project}.{table.dataset_id}.{table.table_id}')}{where}) "
        f"SELECT *, (SELECT COUNT(*) FROM filtered) AS {FILTERED_ROWS_COLUMN} FROM filtered{order} LIMIT @limit"
    )
    return sql, parameters


def prepare_user_query(
    sql: str,
    allowed_layers: Sequence[str],
    table_names: Callable[[str], Sequence[str]] = list_table_names,
) -> PreparedQuery:
    """Validate user SQL (GoogleSQL) and point its tables at the ODAS datasets.

    Only a single SELECT statement over existing tables of the allowed layers is accepted: unqualified
    names are silver tables, other layers are written <layer>.<table> or <prefix>_<layer>.<table>
    (case-insensitive). The SQL is regenerated from the parsed tree, so BigQuery reads exactly the
    tables validated here.
    """
    if len(sql) > MAX_SQL_LENGTH:
        raise QueryRejected(f"The query is longer than {MAX_SQL_LENGTH} characters.")
    try:
        statements = [statement for statement in sqlglot.parse(sql, read="bigquery") if statement is not None]
    except sqlglot.errors.SqlglotError as exc:
        raise QueryRejected(f"Invalid SQL query: {exc}") from exc
    if len(statements) != 1 or not isinstance(statements[0], exp.Query):
        raise QueryRejected("Only a single SELECT statement is allowed.", status_code=403)

    tree = statements[0]
    project = get_client().project
    layer_of_dataset = {name.lower(): layer for layer in LAYERS for name in (layer, dataset_id(layer))}
    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    referenced: Dict[Tuple[str, str], None] = {}
    for table in list(tree.find_all(exp.Table)):
        if not isinstance(table.this, exp.Identifier):
            raise QueryRejected("Table functions are not allowed.", status_code=403)
        if not table.db and not table.catalog and table.name.lower() in cte_names:
            continue
        if table.catalog and table.catalog != project:
            raise QueryRejected(f"Only the ODAS datasets can be queried, not {table.sql(dialect='bigquery')}.", status_code=403)
        layer = layer_of_dataset.get(table.db.lower()) if table.db else "silver"
        if layer is None:
            datasets = ", ".join(dataset_id(name) for name in LAYERS)
            raise QueryRejected(f"Unknown dataset {table.db}; the datasets are {datasets}.", status_code=403)
        if layer not in allowed_layers:
            raise QueryRejected(f"Access to {layer} tables requires admin privileges.", status_code=403)
        name = {known.lower(): known for known in table_names(layer)}.get(table.name.lower())
        if name is None:
            raise QueryRejected(f"Table not found: {dataset_id(layer)}.{table.name}")
        referenced[(layer, name)] = None
        if not table.alias:
            table.set("alias", exp.TableAlias(this=exp.to_identifier(table.name)))
        table.set("this", exp.to_identifier(name, quoted=True))
        table.set("db", exp.to_identifier(dataset_id(layer), quoted=True))
        table.set("catalog", exp.to_identifier(project, quoted=True))

    if len(referenced) > MAX_TABLES_PER_QUERY:
        raise QueryRejected(f"Query references {len(referenced)} tables (max allowed is {MAX_TABLES_PER_QUERY}).")
    return PreparedQuery(tree.sql(dialect="bigquery"), list(referenced))


def _error_reasons(exc: google_exceptions.GoogleAPICallError) -> List[str]:
    return [error.get("reason", "") for error in (exc.errors or []) if isinstance(error, dict)]


def run_query(
    sql: str,
    *,
    endpoint: str,
    limit: int,
    offset: int = 0,
    parameters: Sequence[bigquery.ScalarQueryParameter] = (),
) -> Page:
    """Run a query and read one page of its result.

    The bytes billed are capped by QUERY_MAX_BYTES_BILLED. Results are cached by BigQuery for 24 hours,
    so reading the next pages of the same SQL is free.
    """
    settings = get_settings()
    job_config = bigquery.QueryJobConfig(
        query_parameters=list(parameters),
        maximum_bytes_billed=settings.query_max_bytes_billed,
        use_query_cache=True,
        job_timeout_ms=QUERY_TIMEOUT_SECONDS * 1000,
        labels={"app": "odas", "endpoint": endpoint},
    )
    job = get_client().query(sql, job_config=job_config)
    try:
        rows = job.result(start_index=offset or None, max_results=limit, timeout=QUERY_TIMEOUT_SECONDS)
    except FuturesTimeout:
        job.cancel()
        raise QueryTimeout(f"Query timed out after {QUERY_TIMEOUT_SECONDS}s.") from None
    except google_exceptions.GoogleAPICallError as exc:
        reasons = _error_reasons(exc)
        if "bytesBilledLimitExceeded" in reasons:
            limit_gib = settings.query_max_bytes_billed / 1024**3
            raise QueryRejected(
                f"The query would process more than {limit_gib:g} GiB; select fewer columns or filter on partitions."
            ) from exc
        if "timeout" in reasons or "jobTimeout" in reasons:
            raise QueryTimeout(f"Query timed out after {QUERY_TIMEOUT_SECONDS}s.") from exc
        raise
    records = _records(rows)
    return Page([field.name for field in rows.schema], records, rows.total_rows, job.total_bytes_billed or 0)


def table_batches(table: bigquery.Table) -> Tuple[pa.Schema, Iterator[pa.RecordBatch]]:
    """Arrow schema and record batches of a whole table, without a query.

    Uses the Storage Read API, and the slower REST API when the runtime may not create read sessions.
    """
    client = get_client()
    try:
        batches = client.list_rows(table).to_arrow_iterable(bqstorage_client=_get_read_client())
        first = next(batches, None)
    except google_exceptions.PermissionDenied as exc:
        logger.warning("Storage Read API unavailable (%s); exporting %s over REST", exc.message, table.table_id)
        batches = client.list_rows(table).to_arrow_iterable()
        first = next(batches, None)
    if first is None:
        return client.list_rows(table).to_arrow().schema, iter(())
    return first.schema, itertools.chain([first], batches)


def csv_chunks(schema: pa.Schema, batches: Iterable[pa.RecordBatch]) -> Iterator[bytes]:
    buffer = io.BytesIO()
    pa_csv.write_csv(schema.empty_table(), buffer)
    yield buffer.getvalue()
    options = pa_csv.WriteOptions(include_header=False)
    for batch in batches:
        buffer = io.BytesIO()
        pa_csv.write_csv(batch, buffer, write_options=options)
        yield buffer.getvalue()


class _Chunks(io.RawIOBase):
    """Write-only file whose content is handed out between Parquet row groups."""

    def __init__(self) -> None:
        super().__init__()
        self._parts: List[bytes] = []
        self._size = 0

    def writable(self) -> bool:
        return True

    def write(self, data: Any) -> int:
        self._parts.append(bytes(data))
        self._size += len(data)
        return len(data)

    def tell(self) -> int:
        return self._size

    def drain(self) -> bytes:
        data = b"".join(self._parts)
        self._parts.clear()
        return data


def parquet_chunks(schema: pa.Schema, batches: Iterable[pa.RecordBatch], rows_per_group: int = 100_000) -> Iterator[bytes]:
    sink = _Chunks()
    with pq.ParquetWriter(sink, schema) as writer:
        pending: List[pa.RecordBatch] = []
        pending_rows = 0
        for batch in batches:
            pending.append(batch)
            pending_rows += batch.num_rows
            if pending_rows >= rows_per_group:
                writer.write_table(pa.Table.from_batches(pending, schema))
                pending, pending_rows = [], 0
                yield sink.drain()
        if pending:
            writer.write_table(pa.Table.from_batches(pending, schema))
    yield sink.drain()
