# Scaleway S3 Storage Layout

Odace uses Scaleway Object Storage (S3-compatible) for Iceberg tables.

## Required Environment Variables

- `SCW_OBJECT_STORAGE_ENDPOINT` (example: `https://s3.fr-par.scw.cloud`)
- `SCW_REGION` (example: `fr-par`)
- `SCW_BUCKET_NAME`
- `SCW_ACCESS_KEY`
- `SCW_SECRET_KEY`

## Bucket Prefixes

- `raw/`: source files
- `bronze/`: bronze Iceberg tables (`{table}.iceberg/metadata/*.metadata.json`)
- `silver/`: silver Iceberg tables (`{table}.iceberg/metadata/*.metadata.json`)
- `gold/`: gold datasets

## How the API uses S3

- `app/utils/s3_ops.py`: S3 listing/download/upload operations.
- `app/utils/iceberg_ops.py`: resolves latest Iceberg metadata file with TTL cache.
- `app/utils/sql_executor.py`: DuckDB `iceberg_scan(...)` execution and exports.

## Notes

- Query and export endpoints do not copy data to PostgreSQL.
- PostgreSQL stores only metadata and access-control information.
- Silver catalogue descriptions and previews are served from `data_catalogue` in PostgreSQL.
