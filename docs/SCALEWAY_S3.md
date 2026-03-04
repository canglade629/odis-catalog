# Scaleway S3 (Object Storage)

The Odace app uses **Scaleway Object Storage** (S3-compatible API) as the primary storage for raw data, bronze (Delta), silver (Parquet), and gold layers.

## Configuration

Set these environment variables (or use `env.template`):

| Variable | Description | Example |
|----------|-------------|---------|
| `SCW_OBJECT_STORAGE_ENDPOINT` | S3 API endpoint | `https://s3.fr-par.scw.cloud` |
| `SCW_REGION` | Region | `fr-par` |
| `SCW_BUCKET_NAME` | Bucket name | `odis-s3` |
| `SCW_ACCESS_KEY` | Access key (Scaleway console) | your key |
| `SCW_SECRET_KEY` | Secret key | your secret |

## Bucket layout

The app expects the following prefix layout inside the bucket:

- **`raw/`** – Raw ingested files (CSV, JSON, XLSX, etc.) per domain, e.g. `raw/geo/`, `raw/accueillants/`.
- **`bronze/`** – Delta Lake tables produced by bronze pipelines, e.g. `bronze/geo/`, `bronze/accueillants/`.
- **`silver/`** – Silver layer tables as single Parquet files, e.g. `silver/dim_commune.parquet`.
- **`gold/`** – Gold layer (aggregations).

Bronze checkpoint data is stored under `bronze/checkpoints/`.

## How the app uses S3

- **`app/utils/s3_ops.py`** – `S3Operations`: list objects, download/upload files, path-style addressing for Scaleway.
- **`app/utils/delta_ops.py`** – `DeltaOperations`: read/write Delta tables and Parquet files on S3 using the same credentials; uses `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_REGION` for the deltalake library.
- **Pipelines** – Bronze pipelines read from `raw/` and write Delta to `bronze/`; silver/gold may be produced by DBT or other jobs and written to `silver/` and `gold/`.
- **Data API** – Catalog and preview endpoints list and read from these S3 paths (via `delta_ops` and `s3_ops`).

## References

- [Scaleway Object Storage](https://www.scaleway.com/en/docs/storage/object/)
- [S3-compatible API](https://www.scaleway.com/en/docs/storage/object/api-cli/object-storage-api/)
