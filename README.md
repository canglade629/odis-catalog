# ODAS API

> **Archived.** ODAS is now developed in the private repository `canglade629/odas`, together with its data pipeline. This repository is read-only and kept for its history.

ODAS (Open Data Accueil Solidaire) exposes the open-data catalogue and query API behind the marketplace experience. The API and the static UI (`app/static/index.html`) run as a single Cloud Run service:
- Firestore stores catalogue metadata, certifications, API keys, query counters and daily usage.
- BigQuery stores the tables, one dataset per layer (`odas_bronze`, `odas_silver`, `odas_gold`), and runs the SQL of the query endpoint.

The pipeline project is the source of truth for schema content and writes the `data_catalogue` documents in Firestore after each run.

## Data Catalogue V2 Contract

The `data_catalogue` Firestore collection uses a per-table model:
- One document per silver table (`dim_commune`, `fact_logement_social_rpls`, ...).
- One global meta document (`_catalogue_meta`).

Per-table document keys:
- `business_metadata`: business-level description/category/tags/sources/upstream info.
- `field_docs`: enrichment docs only (`description`, `example`, `pii`, `unit`).
- `runtime_hints`: runtime hints (estimates, `preview` rows served by the silver preview).
- `quality`: quality metadata from pipeline.
- `schema_cache`: non-authoritative cached schema and drift info.
- `catalog_generated_at`: generation timestamp for that document.

Important semantics:
- `field_docs` is documentation, not runtime schema truth.
- `schema_cache.fields` is cache only and may drift.
- Runtime columns/types/nullability come from the BigQuery table metadata (`GET /api/data/table/{layer}/{table}`).
- Firestore limits apply: documents must stay under 1 MiB and arrays cannot directly contain arrays.

Meta document (`_catalogue_meta`) includes:
- `catalog_generated_at`
- `dbt_manifest_generated_at`
- `version`
- `table_names`
- `drift_report`
- `source_file`

## Firestore Data Model

| Collection | Document id | Fields |
|---|---|---|
| `api_keys` | SHA-256 of the key | `user_id`, `is_admin`, `active`, `created_at`, `last_used_at` |
| `data_catalogue` | table name, `_catalogue_meta` | see the V2 contract above, plus `updated_at` |
| `table_certifications` | `<layer>_<table>` | `layer`, `table_name`, `certified`, `certified_at`, `certified_by` |
| `query_tracker` | `<layer>_<table>` | `query_count`, `last_query_at`, `users.<user_id>.{query_count,last_query_at}` |
| `daily_usage` | UTC day, `YYYY-MM-DD` | `query_bytes_billed`, `export_bytes`, `updated_at` |

Only the `(default)` database gets the Firestore free quota (1 GiB, 50K reads, 20K writes and 20K deletes per day). The API stays well inside it:
- the `data_catalogue` collection is cached per instance for `METADATA_CACHE_TTL_SECONDS` (default 300s) and refreshed on writes, on `POST /api/data/catalog/refresh` and on `POST /admin/runtime-cache/invalidate`;
- certifications and query counters are loaded with one query each per catalogue page;
- `last_used_at` is refreshed at most every 5 minutes per key;
- a query, filtered preview or export reads and writes the day's `daily_usage` document once.

## BigQuery Layout

```
odis-stream2-app.odas_bronze.<table>    source data as loaded by the pipeline (admin only)
odis-stream2-app.odas_silver.<table>    cleaned, documented tables (catalogue, query, export)
odis-stream2-app.odas_gold.<table>      not populated yet (admin only)
```

The datasets are in `europe-west1`, next to the service. Bronze column names keep the source names, except that characters BigQuery does not accept (`.`, `(`, `)`, ...) become `_`.

## Core Product Goal

- Expose real schema information from the Firestore catalogue.
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
- `POST /admin/runtime-cache/invalidate`

### Utility endpoints
- `GET /`
- `GET /api/me`
- `GET /health`
- `GET /api/docs/data-model`

## Query Layer Architecture

All table access goes through `app/utils/bigquery_ops.py`, with the runtime service account:
- Catalog, table metadata and unfiltered previews use the BigQuery API without running a query, so they are free. Table listings are cached per instance (`METADATA_CACHE_TTL_SECONDS`, default 300s).
- `POST /api/data/query` takes one GoogleSQL `SELECT` statement. `sqlglot` parses it, rejects anything else (DDL, DML, scripts, table functions, other projects and datasets, `INFORMATION_SCHEMA`) and rewrites every table to its fully qualified name; BigQuery runs the regenerated SQL. Unqualified names are silver tables (`dim_commune`); `odas_silver.dim_commune` and `silver.dim_commune` work too. Non-admins may only read silver; admins also read `odas_bronze.<table>` and `odas_gold.<table>`.
- Pages of a result are read with `offset`; BigQuery caches results for 24 hours, so the next pages of the same SQL are not billed again.
- Filtered or sorted previews run a parameterized query.
- Exports stream the table without a query, through the BigQuery Storage Read API (REST when the runtime may not create read sessions), as CSV or Parquet.

Cost caps, shared by all users (bytes; see `app/core/config.py`):

| Setting | Default | Effect |
|---|---|---|
| `QUERY_MAX_BYTES_BILLED` | 5 GiB | A query that would bill more fails (400); the largest silver table is 1.7 GiB |
| `QUERY_DAILY_BYTES_BUDGET` | 20 GiB | Queries and filtered previews answer 429 once the day's billed bytes reach it |
| `EXPORT_DAILY_BYTES_BUDGET` | 5 GiB | Exports answer 429 once the day's exported table sizes reach it |

## Local Run

1. Authenticate with Application Default Credentials (the same identity is used for Firestore and BigQuery; it needs to read the datasets and create BigQuery jobs):
   ```bash
   gcloud auth application-default login
   ```

2. Copy `env.template` to `.env` and set `GCP_PROJECT_ID`, `ADMIN_SECRET`, `CORS_ORIGINS`, `ENVIRONMENT` (and `BIGQUERY_DATASET_PREFIX`, `BIGQUERY_LOCATION`, `FIRESTORE_DATABASE` when they differ from the defaults).

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   For tests and the scripts that copy the legacy stack, install the dev dependencies instead:
   ```bash
   pip install -r requirements-dev.txt
   ```

4. Start API:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8080
   ```

5. Open:
   - API docs: `http://localhost:8080/docs`
   - Health: `http://localhost:8080/health`

## Tests

```bash
pip install -r requirements-test.txt
python -m pytest
```

Tests use in-memory stand-ins for Firestore (`tests/fake_firestore.py`) and BigQuery (`tests/fake_bigquery.py`), so they need no GCP access. The fake BigQuery does not run SQL: query tests check the SQL sent to BigQuery and serve canned results.

## Deployment

ODAS runs as the Cloud Run service `odas-app` in the shared GCP project `odis-stream2-app` (region `europe-west1`), next to the unrelated `odis-app`. `cloudbuild.yaml` runs the tests, builds the Docker image, pushes it to the project's shared Artifact Registry repository and deploys the public service. Builds run as the project's CI account `odis-ci`; the service runs as its own account `odas-run`. Every push to `main` triggers a build through the Cloud Build GitHub trigger; `gcloud builds submit` runs the same build by hand. Do not use Terraform.

| Resource | Name |
|---|---|
| Cloud Run service | `odas-app`, public; the deploy step prints its `run.app` URL |
| Image | `europe-west1-docker.pkg.dev/odis-stream2-app/odis-stream2-repo/odas-app:<commit>` |
| BigQuery datasets | `odas_bronze`, `odas_silver`, `odas_gold` (`europe-west1`, label `app=odas`) |
| Pipeline bucket | `odis-stream2-app-odas-lake-euw1` (7-day soft delete); for the pipeline's raw files, not used by the API |
| Firestore | `(default)` database, Native mode, `europe-west1` (the only database with the free quota) |
| Admin secret | `ODAS_ADMIN_SECRET` |
| Runtime service account | `odas-run@odis-stream2-app.iam.gserviceaccount.com` |
| Build service account | `odis-ci@odis-stream2-app.iam.gserviceaccount.com` (existing) |
| GitHub connection, trigger | `odas-github`, `odas-app-deploy` (both in `europe-west1`) |

These names are the defaults of the `cloudbuild.yaml` substitutions; override them on the trigger or with `--substitutions`:

| Substitution | Meaning |
|---|---|
| `_REGION` | Region for Artifact Registry and Cloud Run (`europe-west1`) |
| `_AR_REPOSITORY` | Artifact Registry Docker repository (`odis-stream2-repo`) |
| `_SERVICE` | Cloud Run service name (`odas-app`) |
| `_RUNTIME_SERVICE_ACCOUNT` | Service account the service runs as (`odas-run@...`) |
| `_BIGQUERY_DATASET_PREFIX` | Datasets `<prefix>_bronze`, `<prefix>_silver`, `<prefix>_gold`, in `_REGION` (`odas`) |
| `_FIRESTORE_DATABASE` | Firestore database (`(default)`) |
| `_ADMIN_SECRET_NAME` | Secret Manager secret holding `ADMIN_SECRET` (`ODAS_ADMIN_SECRET`) |
| `_CORS_ORIGINS` | Extra allowed origins, comma-separated (empty: same-origin only) |
| `_CPU`, `_MEMORY`, `_MAX_INSTANCES` | Service sizing (`1`, `2Gi`, `2`) |
| `_IMAGE_TAG` | Image tag; the commit SHA for trigger builds, required for `gcloud builds submit` |

The service scales to zero (`--min-instances=0`), so idle time is free.

### Access

The project Owner provisioned the resources above, except the BigQuery datasets, and these roles; the deployer cannot enable APIs, create service accounts or grant project roles.

- `odas-run` (runtime): `roles/datastore.user` on the project; `roles/bigquery.jobUser` and `roles/bigquery.readSessionUser` on the project (to run queries and stream exports); READER on the three datasets; `roles/storage.objectUser` and `roles/storage.legacyBucketReader` on the pipeline bucket; `roles/secretmanager.secretAccessor` on `ODAS_ADMIN_SECRET`; `roles/run.invoker`, with Cloud Scheduler allowed to use it to start the pipeline.
- Datasets: the deployer created them and owns them, so it grants dataset access itself (`bq update --source` with the edited `bq show --format=prettyjson` output, or the console's "Share" panel).
- `odis-ci` and the deployer: `roles/iam.serviceAccountUser` on `odas-run`. `odis-ci` already pushes to `odis-stream2-repo` and deploys public services.
- Deployer: `roles/datastore.user`, `roles/datastore.indexAdmin`, `roles/cloudbuild.connectionAdmin` and `run.services.setIamPolicy` (for `--allow-unauthenticated`).
- Cloud Build service agent: `roles/secretmanager.admin`, to store the GitHub token of the connection.

The commands below use these variables:

```bash
PROJECT_ID=odis-stream2-app
REGION=europe-west1
CI_SA=odis-ci@$PROJECT_ID.iam.gserviceaccount.com
```

### Admin secret

`ADMIN_SECRET` authenticates the admin endpoints (API key management) as a bearer token. Read it with `gcloud secrets versions access latest --secret=ODAS_ADMIN_SECRET --project=$PROJECT_ID`. To rotate it, add a version, then redeploy: instances read `latest` when they start.

```bash
openssl rand -base64 32 | tr -d '\n' | gcloud secrets versions add ODAS_ADMIN_SECRET --project=$PROJECT_ID --data-file=-
```

### Data from the legacy stack

Until the pipeline is migrated, it keeps writing to Scaleway S3 and PostgreSQL, and these two scripts copy its output to GCP. Both read the legacy `PG_DB_*` (or `DATABASE_URL`) and `SCW_*` variables from `.env`, and write with the application default credentials (`gcloud auth application-default login` as the deployer). Run them in this order, with `--dry-run` first:

```bash
# API keys, certifications, query counters and catalogue documents; document ids are kept, so API keys keep working
python scripts/migrate_postgres_to_firestore.py --project $PROJECT_ID
# Current snapshot of every bronze and silver Iceberg table, loaded into odas_bronze / odas_silver (load jobs are free)
python scripts/copy_tables_to_bigquery.py --project $PROJECT_ID
```

The second script replaces each BigQuery table with the current Scaleway snapshot and drops the Iceberg-only `runtime_hints` that the first one copies into the catalogue, hence the order. Stop running them once the pipeline writes to BigQuery and Firestore.

### Manual deployment

`gcloud builds submit` runs the same build as the trigger. It uploads the working tree without the `.gitignore`d files, so run it from a clean checkout of the commit to deploy:

```bash
gcloud builds submit --project=$PROJECT_ID --region=$REGION --config=cloudbuild.yaml \
  --service-account=projects/$PROJECT_ID/serviceAccounts/$CI_SA \
  --gcs-source-staging-dir=gs://odis-stream2-app-cloudbuild-euw1/source \
  --substitutions=_IMAGE_TAG=$(git rev-parse --short HEAD)

curl -fsS "$(gcloud run services describe odas-app --project=$PROJECT_ID --region=$REGION --format='value(status.url)')/health"
```

### Git integration

Cloud Build reaches GitHub through the regional connection `odas-github`, authorized by the owner of the private repository `canglade629/odas`. To recreate it:

```bash
gcloud builds connections create github odas-github --project=$PROJECT_ID --region=$REGION
# Open the printed link while signed in to GitHub as the repository owner, authorize Cloud Build and
# install its GitHub App with access to canglade629/odas. `connections describe` then shows installationState COMPLETE.
gcloud builds connections describe odas-github --project=$PROJECT_ID --region=$REGION

gcloud builds repositories create odas --remote-uri=https://github.com/canglade629/odas.git \
  --connection=odas-github --project=$PROJECT_ID --region=$REGION
gcloud builds triggers create github --name=odas-app-deploy --project=$PROJECT_ID --region=$REGION \
  --repository=projects/$PROJECT_ID/locations/$REGION/connections/odas-github/repositories/odas \
  --branch-pattern='^main$' --build-config=cloudbuild.yaml \
  --service-account=projects/$PROJECT_ID/serviceAccounts/$CI_SA
```

From then on, every push to `main` deploys. To change a default on the trigger, add `--substitutions`; `_CORS_ORIGINS` contains commas, so use gcloud's alternate delimiter: `--substitutions='^;^_CORS_ORIGINS=https://a.example,https://b.example'`.

### Domain and rollback

- The service is reached on its default `run.app` URL until a custom domain is added (by the project Owner, like the one of `odis-app`). The `odace.services.d4g.fr` URLs in the docs, the OpenAPI `servers` and the email templates then need updating.
- Rollback: `gcloud run services update-traffic odas-app --region=$REGION --to-revisions=<revision>=100`. While traffic is pinned to a revision, new deployments receive none; restore with `--to-latest`.

### Free-tier budget

Free tiers apply per billing account, so `odis-app` and any other project on the same billing account draw on the same quotas.

- Cloud Run: 2M requests, 180K vCPU-seconds and 360K GiB-seconds per month are free; with 1 vCPU / 2 GiB that is about 50 hours of request processing per month. Idle instances scale to zero.
- Cloud Build: 2,500 build-minutes per month are free; a build takes a few minutes.
- Artifact Registry: 0.5 GB free. The shared repository keeps the 5 most recent versions of each image and deletes those older than 7 days.
- BigQuery: 1 TiB of queries and 10 GiB of storage per month are free; loads, table listings, metadata, unfiltered previews and cached results are free, and the Storage Read API used by exports is free up to 300 TiB per month. The datasets hold about 11 GiB (logical storage, about $0.02 per GiB-month above the free tier, half that for tables unchanged for 90 days). The daily query budget above keeps the API under about 600 GiB of queries per month.
- Outbound data transfer: exports and API responses leaving Google Cloud are billed (about $0.12 per GiB from `europe-west1`), hence the daily export budget.
- Cloud Storage: the 5 GB free tier only applies to US regions; Standard storage in `europe-west1` costs about $0.02 per GB-month.
- Firestore (`(default)` database only) and Secret Manager usage of this app fits in their free quotas.
