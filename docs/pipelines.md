# Pipelines

The project is split into **App** (UI, API, data access, bronze ingestion) and **DBT** (silver/gold transformations). See [MIGRATION_DBT.md](MIGRATION_DBT.md) for the DBT project layout.

## Overview

- **Bronze**: Ingestion from files (S3) or APIs. Implemented in Python (`app/pipelines/bronze/`). Registered and run by the app.
- **Silver / Gold**: Transformations can be run by **DBT** (in-repo `dbt/` or external project). The app can trigger `dbt run` via `app/core/dbt_runner.py` when you run the full pipeline or silver from the API (if a dbt project is present).

## Source of truth

- **Bronze**: `config/pipelines/bronze.yaml` and `app/core/config_loader.py` (app loads only bronze and gold; silver is not loaded for execution).
- **Silver/Gold**: DBT may live in another repo; in-repo `dbt/` was removed. `config/pipelines/silver.yaml` remains for reference/documentation; the app uses it for catalog only.

## How to run pipelines

### Via API

- **Bronze (single)**: `POST /api/bronze/{name}` with optional `?force=true`. Requires admin.
- **Silver**: `POST /api/silver/{pipeline_name}` — runs DBT (all silver models). Requires admin.
- **Full pipeline**: `POST /api/pipeline/run` with body `{"bronze_only": false, "silver_only": false, "force": false}`. Runs bronze in-process, then triggers `dbt run` for silver/gold. Response includes `dbt_run`: `{status, message, exit_code}`. Requires admin.
- **List**: `GET /api/pipeline/list?layer=bronze|silver|gold`. Bronze from registry; silver/gold list is fixed (DBT-managed).

### Via scripts

- **DBT (silver/gold)**: Run from your DBT project; set `DBT_S3_PATH` to the app's bronze path (e.g. `s3://bucket/bronze`) if DBT reads from the same S3.
- **Validate silver** (after pipelines): `python scripts/validate_silver.py` (reads from S3 via app config).

Run scripts from the project root.

## Data catalogue

Table and field descriptions for the silver layer are in `config/data_catalogue.yaml`. The app uses this for catalog/preview; keep it in sync with DBT model output.
