# Odace Data API

Odace backend is the API layer for the marketplace experience:
- PostgreSQL stores catalogue metadata, certifications, API keys, and query counters.
- Scaleway S3 stores Iceberg tables (`bronze/`, `silver/`, `gold/`).
- DuckDB executes SQL and export endpoints directly on Iceberg metadata.

The pipeline project is the source of truth for schema content and writes the `data_catalogue` document in PostgreSQL after each run.

## Data Catalogue V2 Contract

`data_catalogue` now uses a per-table model:
- One row per silver table (`id = dim_commune`, `id = fact_logement_social_rpls`, ...).
- One global meta row (`id = _catalogue_meta`).

Per-table `document` keys:
- `business_metadata`: business-level description/category/tags/sources/upstream info.
- `field_docs`: enrichment docs only (`description`, `example`, `pii`, `unit`).
- `runtime_hints`: runtime pointers (snapshot id, metadata location, estimates, optional preview hints).
- `quality`: quality metadata from pipeline.
- `schema_cache`: non-authoritative cached schema and drift info.
- `catalog_generated_at`: generation timestamp for that row.

Important semantics:
- `field_docs` is documentation, not runtime schema truth.
- `schema_cache.fields` is cache only and may drift.
- Runtime columns/types/nullability must come from live DuckDB/Iceberg reads (`GET /api/data/table/{layer}/{table}`).

Meta row (`_catalogue_meta`) includes:
- `catalog_generated_at`
- `dbt_manifest_generated_at`
- `version`
- `table_names`
- `drift_report`
- `source_file`

## Core Product Goal

- Expose real schema information from PostgreSQL catalogue data.
- Provide SQL query endpoints over silver data.
- Provide data export endpoints (CSV, Parquet).

## API Surface (Current)

### Data endpoints
- `GET /api/data/catalog`
- `GET /api/data/catalog/silver`
- `GET /api/data/catalog/silver/{table_name}`
- `GET /api/data/table/{layer}/{table}`
- `POST /api/data/preview/{layer}/{table}`
- `POST /api/data/query`
- `GET /api/data/export/{table}`
- `POST /api/data/catalog/refresh`

### Admin endpoints
- `POST /admin/api-keys`
- `DELETE /admin/api-keys/revoke`
- `DELETE /admin/api-keys/delete`
- `GET /admin/api-keys`
- `POST /admin/tables/certify`
- `POST /admin/tables/uncertify`
- `GET /admin/tables/certifications`
- `POST /admin/catalogue/refresh`
- `GET /admin/debug/s3-ls`

### Utility endpoints
- `GET /`
- `GET /api/me`
- `GET /health`
- `GET /api/docs/data-model`

## Query Layer Architecture

The query stack is unified around DuckDB + Iceberg:
- A shared DuckDB executor is initialized at startup.
- Iceberg metadata paths are resolved through `app/utils/iceberg_ops.py`.
- Metadata lookup is cached in-process (`METADATA_CACHE_TTL_SECONDS`, default 300s).
- SQL table extraction uses `sqlglot` (instead of token matching).

This keeps PostgreSQL focused on metadata and S3 focused on analytical data.

## Local Run

1. Configure environment variables (`.env`) with:
   - `SCW_OBJECT_STORAGE_ENDPOINT`, `SCW_REGION`, `SCW_BUCKET_NAME`, `SCW_ACCESS_KEY`, `SCW_SECRET_KEY`
   - `DATABASE_URL` or `PG_DB_*`
   - `ADMIN_SECRET`, `CORS_ORIGINS`, `ENVIRONMENT`

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Apply database schema:
   ```bash
   psql -h $PG_DB_HOST -p $PG_DB_PORT -d $PG_DB_NAME -U $PG_DB_USER -f app/db/schema.sql
   ```

4. Start API:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8080
   ```

5. Open:
   - API docs: `http://localhost:8080/docs`
   - Health: `http://localhost:8080/health`

## Deployment

Deploy frontend and backend together via the existing Coolify workflow in this repository.  
Do not use Cloud Run/Cloud Build/Terraform here.
