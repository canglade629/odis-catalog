# Odace Data Pipeline

A cloud-based data pipeline platform built with FastAPI and Delta Lake. The Odace Data Pipeline provides a REST API for managing data ingestion, transformation, and processing across bronze, silver, and gold data layers.

## Running locally (Docker + S3 + PostgreSQL)

The app runs in a container using **Scaleway S3** for storage and **PostgreSQL** for API keys, catalogue, jobs, and certifications.

1. **Configure environment**: Copy `env.template` to `.env` and set:
   - `SCW_*` (Scaleway S3)
   - `PG_DB_*` or `DATABASE_URL` (PostgreSQL)
   - `ADMIN_SECRET`, `ENVIRONMENT`, `CORS_ORIGINS`

2. **Apply DB schema once** (if not already done):
   ```bash
   psql -h $PG_DB_HOST -p $PG_DB_PORT -d $PG_DB_NAME -U $PG_DB_USER -f app/db/schema.sql
   ```

3. **Build and run**:
   ```bash
   docker compose up --build
   # Or: docker build -t odace-pipeline . && docker run -p 8080:8080 --env-file .env odace-pipeline
   ```
   API: http://localhost:8080. Health: http://localhost:8080/health. **Production:** [https://odace.services.d4g.fr](https://odace.services.d4g.fr) (Coolify).

4. **Optional – sync catalogue from YAML to DB**:
   ```bash
   python scripts/sync_catalogue_to_db.py
   ```

## Deploy with Coolify

You can deploy this app to [Coolify](https://coolify.io) (self-hosted PaaS) in one click.

1. **Connect the repo**: In Coolify, create a new resource and connect this Git repository (public, or via GitHub App / Deploy Key).
2. **Build**: Coolify will detect `coolify.json` and use the **Dockerfile** build pack with port **8080**.
3. **Environment variables**: In Coolify’s “Environment Variables” for the application, set the same variables as in `env.template`:
   - `SCW_OBJECT_STORAGE_ENDPOINT`, `SCW_REGION`, `SCW_BUCKET_NAME`, `SCW_ACCESS_KEY`, `SCW_SECRET_KEY` (Scaleway S3)
   - `PG_DB_HOST`, `PG_DB_PORT`, `PG_DB_NAME`, `PG_DB_USER`, `PG_DB_PWD` (or `DATABASE_URL`)
   - `ADMIN_SECRET`, `ENVIRONMENT`, `CORS_ORIGINS`
4. **Post-deploy**: Apply the database schema once (e.g. from your machine: `psql -h $PG_DB_HOST ... -f app/db/schema.sql`). Optionally run `python scripts/sync_catalogue_to_db.py` to sync the catalogue from YAML to the DB.

**Coolify troubleshooting (404):**
- **Ports**: Do not bind host ports; the repo uses `ports: ["8080"]` and Traefik labels for **https://odace.services.d4g.fr**.
- **Custom Docker Options**: leave empty (no `--device=/dev/fuse`, etc.).
- **Runtime logs (critical)**: In Coolify go to the application → **Logs** (runtime/container logs, not build). If the container crashes you will see the error here.
  - **Success**: you should see `Application startup complete` and `Uvicorn running on http://0.0.0.0:8080`. Then `/`, `/health`, `/docs` should work.
  - **Crash**: if you see `ValueError: ADMIN_SECRET is set to an insecure value` or `Set DATABASE_URL or PG_DB_...`, add the required env vars in Coolify (Environment Variables) and redeploy. The app **will not start** without a valid `ADMIN_SECRET` (and in production, CORS_ORIGINS must not be `*`).
- **Env vars**: At minimum set `ADMIN_SECRET` (strong random string), and for full functionality `SCW_*`, `PG_DB_*` or `DATABASE_URL`. Use `ENVIRONMENT=development` and `CORS_ORIGINS=*` only for testing.

## What is Odace?

Odace is a data pipeline platform that:
- Ingests raw data files into structured Delta Lake tables (Bronze layer)
- Transforms and cleans data (Silver layer)
- Provides aggregated business metrics (Gold layer)
- Offers a REST API for pipeline orchestration
- Includes a web UI for monitoring and management (Tailwind CSS)

## For API Users

If you've received an API key, you can use the Odace API to trigger data pipelines, upload files, and monitor processing status.

### Prerequisites

- An API key (format: `sk_live_...`)
- HTTP client (curl, Postman, Python requests, etc.)
- API endpoint URL: **https://odace.services.d4g.fr** (or provided by your administrator)

### Authentication

All API requests require your API key in the `Authorization` header using Bearer token format:

```bash
Authorization: Bearer sk_live_YOUR_API_KEY
```

### Quick Start

1. **Test your API key**:
   ```bash
   curl -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
        https://odace.services.d4g.fr/api/pipeline/list
   ```

2. **View available pipelines**:
   ```bash
   curl -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
        https://odace.services.d4g.fr/api/pipeline/list?layer=bronze
   ```

3. **Run a pipeline**:
   ```bash
   curl -X POST \
        -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
        https://odace.services.d4g.fr/api/bronze/geo
   ```

## Core API Endpoints

### Pipeline Management

**List available pipelines**:
```bash
GET /api/pipeline/list?layer=bronze|silver|gold
```

**Run a bronze pipeline** (ingestion):
```bash
POST /api/bronze/{pipeline_name}?force=false
```

**Run a silver pipeline** (transformation):
```bash
POST /api/silver/{pipeline_name}?force=false
```

**Run full pipeline** (bronze + silver):
```bash
POST /api/pipeline/run
Content-Type: application/json

{
  "bronze_only": false,
  "silver_only": false,
  "force": false
}
```

### Running the Full Pipeline - Detailed Guide

The full pipeline endpoint (`/api/pipeline/run`) orchestrates the complete data processing workflow from raw data to cleaned, transformed tables.

#### Basic Full Pipeline Run

**Run the complete pipeline** (Bronze → Silver):
```bash
curl -X POST \
  -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"force": true}' \
  https://odace.services.d4g.fr/api/pipeline/run
```

**Expected Response**:
```json
{
  "job_id": "3c8f78d2-671a-4565-806a-b77e614f5868",
  "status": "success",
  "total_pipelines": 12,
  "succeeded": 12,
  "failed": 0,
  "pipelines": [
    {
      "run_id": "uuid",
      "pipeline_name": "geo",
      "layer": "bronze",
      "status": "success",
      "started_at": "2025-11-29T12:18:42.731198",
      "completed_at": "2025-11-29T12:18:49.615840",
      "duration_seconds": 6.88,
      "message": "Successfully processed 1 file(s), 34935 rows"
    },
    ...
  ]
}
```

#### Pipeline Run Options

**1. Full Pipeline (Recommended)**
Runs all bronze pipelines first, then all silver pipelines:
```bash
curl -X POST \
  -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "bronze_only": false,
    "silver_only": false,
    "force": true
  }' \
  https://odace.services.d4g.fr/api/pipeline/run
```
- **Executes**: 6 bronze + 6 silver = 12 total pipelines
- **Duration**: ~2-3 minutes
- **Use when**: Running a complete data refresh

**2. Bronze Layer Only**
Only ingests raw data into bronze tables:
```bash
curl -X POST \
  -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "bronze_only": true,
    "force": true
  }' \
  https://odace.services.d4g.fr/api/pipeline/run
```
- **Executes**: 6 bronze pipelines
- **Duration**: ~1-2 minutes
- **Use when**: Only new raw files need to be ingested

**3. Silver Layer Only**
Only transforms bronze data into silver tables:
```bash
curl -X POST \
  -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "silver_only": true,
    "force": true
  }' \
  https://odace.services.d4g.fr/api/pipeline/run
```
- **Executes**: 6 silver pipelines
- **Duration**: ~1 minute
- **Use when**: Bronze data exists and only transformations need updating

**4. Incremental Run**
Only processes new files (uses checkpoints):
```bash
curl -X POST \
  -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "force": false
  }' \
  https://odace.services.d4g.fr/api/pipeline/run
```
- **Executes**: Only pipelines with new data
- **Duration**: Varies (only processes changes)
- **Use when**: Running scheduled incremental updates

#### Understanding `force` Parameter

| `force` | Behavior | Checkpoints | Use Case |
|---------|----------|-------------|----------|
| `true` | Reprocess ALL files | Cleared | Complete refresh, ensure idempotency |
| `false` | Process NEW files only | Preserved | Incremental updates, daily runs |

**Idempotency**: With `force=true`, running the pipeline multiple times produces identical results (no data duplication).

#### Monitoring Pipeline Execution

**1. Check job status**:
```bash
curl -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
  https://odace.services.d4g.fr/api/jobs/{job_id}
```

**2. List recent jobs**:
```bash
curl -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
  https://odace.services.d4g.fr/api/jobs?limit=10
```

**3. View task details**:
```json
{
  "job_id": "uuid",
  "status": "success",
  "job_name": "Full Pipeline - Bronze → Silver",
  "total_tasks": 12,
  "completed_tasks": 12,
  "failed_tasks": 0,
  "started_at": "2025-11-29T12:18:42Z",
  "completed_at": "2025-11-29T12:21:03Z",
  "tasks": [
    {
      "task_id": "uuid",
      "pipeline_name": "geo",
      "layer": "bronze",
      "status": "success",
      "duration_seconds": 6.88,
      "message": "Successfully processed 1 file(s), 34935 rows"
    },
    ...
  ]
}
```

#### Expected Results

After a successful full pipeline run, you should have:

**Bronze Tables** (raw ingested data):
- `bronze.accueillants`: ~1,634 rows
- `bronze.geo`: ~34,935 rows
- `bronze.gares`: ~3,884 rows
- `bronze.lignes`: ~1,069 rows
- `bronze.logement`: ~279,760 rows
- `bronze.zones_attraction`: ~34,875 rows

**Silver Tables** (cleaned & transformed):
- `silver.accueillants`: ~1,634 rows
- `silver.geo`: ~34,935 rows
- `silver.gares`: ~2,974 rows (deduplicated)
- `silver.lignes`: ~933 rows (deduplicated)
- `silver.logement`: ~34,928 rows (deduplicated)
- `silver.zones_attraction`: ~28,377 rows

#### Error Handling

**If a pipeline fails**:
```json
{
  "job_id": "uuid",
  "status": "failed",
  "total_pipelines": 12,
  "succeeded": 5,
  "failed": 1,
  "pipelines": [
    ...
    {
      "pipeline_name": "logement",
      "layer": "bronze",
      "status": "failed",
      "error": "Error message here"
    }
  ]
}
```

**Recovery**: Simply re-run with `force=true` to retry:
```bash
curl -X POST \
  -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"force": true}' \
  https://odace.services.d4g.fr/api/pipeline/run
```

The pipeline is **idempotent**, so re-running will clear the error and produce correct results.

#### Best Practices

1. **Use `force=true` for manual runs** - Ensures clean, consistent results
2. **Use `force=false` for scheduled runs** - Efficient incremental processing
3. **Monitor job status** - Check `/api/jobs/{job_id}` for detailed progress
4. **Verify results** - Query data tables after pipeline completion
5. **Re-run on failures** - Safe to retry with `force=true`

**Check pipeline status**:
```bash
GET /api/pipeline/status/{run_id}
```

**View execution history**:
```bash
GET /api/pipeline/history
```

### File Upload

**Upload a file for processing**:
```bash
POST /api/files/upload?domain=logement
Content-Type: multipart/form-data
```

Example with curl:
```bash
curl -X POST \
     -H "Authorization: Bearer sk_live_YOUR_API_KEY" \
     -F "file=@myfile.csv" \
     "https://odace.services.d4g.fr/api/files/upload?domain=logement"
```

### API Documentation

Visit the interactive API documentation at:
```
https://odace.services.d4g.fr/docs
```

## Available Pipelines

### Bronze Layer (Data Ingestion)
- `accueillants` - Host locations
- `geo` - French commune geographic data
- `logement` - Housing prices and statistics
- `gares` - Train station data
- `lignes` - Train line data
- `zones_attraction` - Urban attraction zones
- `siae_structures` - Social inclusion employment structures (API source)
- `siae_postes` - Job positions in social inclusion structures (API source)

### Silver Layer (Data Transformation)
- `accueillants` - Cleaned host data
- `geo` - Normalized commune data
- `gares` - Deduplicated station data
- `lignes` - Deduplicated line data
- `logement` - Standardized housing data
- `zones_attraction` - Processed attraction zones
- `siae_structures` - Cleaned SIAE structures with INSEE codes
- `siae_postes` - Standardized job positions with geographic context

## Response Formats

### Success Response
```json
{
  "status": "success",
  "run_id": "uuid-here",
  "message": "Pipeline started successfully"
}
```

### Error Response
```json
{
  "detail": "Error message here"
}
```

## Rate Limits

- Standard users: Consult your API key documentation
- Contact your administrator for rate limit increases

## Support

For questions or issues:
- Check the [API Key Usage Guide](docs/API_KEY_USAGE.md)
- Review the interactive API docs at `/docs`
- Contact your data team administrator

## SIAE Data Integration

The Odace pipeline now includes data from the **emplois.inclusion.beta.gouv.fr** API, providing social inclusion employment structure data.

### What is SIAE Data?

SIAE (Structures d'Insertion par l'Activité Économique) are French organizations that provide employment opportunities to people facing social and professional difficulties. The data includes:

- **Structures**: Companies and organizations offering inclusive employment
  - SIRET numbers, legal names, addresses
  - Contact information (phone, email, website)
  - Geographic location (city, postal code, department)
  - Structure types (EI, AI, ETTI, etc.)

- **Job Positions (Postes)**: Active job openings within these structures
  - Job classifications (ROME codes)
  - Contract types and number of positions
  - Recruitment status
  - Geographic context linked from structures

### Data Sources

- **API**: `https://emplois.inclusion.beta.gouv.fr/api/v1/siaes/`
- **Update Frequency**: Daily via API (rate limited: 12 requests/minute)
- **Coverage**: All French departments (metropolitan and overseas)
- **Estimated Volume**: 5,000-10,000 structures, 10,000-30,000 job positions

### Integration with Existing Data

SIAE data can be joined with other datasets:

1. **With `geo` data**: Via city names and postal codes
   - Silver layer enriches SIAE structures with INSEE codes
   - Enables commune-level analysis

2. **With `accueillants` data**: Similar location-based structures
   - Compare host locations with employment opportunities
   - Spatial proximity analysis

3. **With `logement` data**: Housing accessibility analysis
   - Link housing availability to SIAE job locations
   - Affordability vs employment opportunity mapping

4. **With `gares`/`lignes` data**: Public transport access
   - Assess SIAE accessibility by public transport
   - Commute time analysis

### Example Queries

**Find SIAE structures with their commune data:**
```sql
SELECT 
    s.legal_name,
    s.structure_type,
    s.city,
    s.postal_code,
    s.insee_code,
    s.standardized_city_name
FROM silver_siae_structures s
WHERE s.accepting_applications = true
```

**Find active job positions by department:**
```sql
SELECT 
    p.department,
    p.structure_type,
    COUNT(*) as active_positions,
    SUM(p.positions_available) as total_openings
FROM silver_siae_postes p
WHERE p.is_recruiting = 'True'
GROUP BY p.department, p.structure_type
ORDER BY active_positions DESC
```

**Join SIAE with housing data:**
```sql
SELECT 
    s.city,
    s.insee_code,
    COUNT(DISTINCT s.id) as siae_count,
    AVG(l.some_housing_metric) as avg_housing_metric
FROM silver_siae_structures s
LEFT JOIN silver_geo g ON s.insee_code = g.CODGEO
LEFT JOIN silver_logement l ON g.CODGEO = l.CODGEO
GROUP BY s.city, s.insee_code
```

## Additional Documentation

For developers and administrators:
- **[Pipelines](docs/pipelines.md)** - How pipelines are registered (YAML), run (API and scripts), and data catalogue
- **[DBT migration preparation](docs/MIGRATION_DBT.md)** - Pipeline inventory, dependency graph, and config for a future DBT migration
- [API Key Management](docs/API_KEY_USAGE.md) - Complete guide to API key creation and management
- [Data Model Reference](DATA_MODEL.md) - Silver layer schema and table relationships
- [Implementation Details](docs/archive/IMPLEMENTATION_SUMMARY.md) - Technical architecture and design (archive)

## API Key Management for Administrators

### Managing API Keys

Use the **Admin API** (with `ADMIN_SECRET` in the `Authorization: Bearer` header):

- **Create a key**: `POST /api/admin/api-keys` with body `{"user_id": "user@example.com"}`
- **List keys**: `GET /api/admin/api-keys`
- **Revoke**: `DELETE /api/admin/api-keys/revoke` with body `{"api_key": "sk_live_..."}`
- **Delete**: `DELETE /api/admin/api-keys/delete` with body `{"api_key": "sk_live_..."}`

See [API Key Usage Guide](docs/API_KEY_USAGE.md) for full documentation.

## Local Development

For developers setting up the project locally:

1. **Clone and setup**:
   ```bash
   git clone <repository>
   cd odace_backend
   cp env.template .env
   # Edit .env with Scaleway S3 and PostgreSQL settings (see env.template)
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run locally**:
   ```bash
   ./scripts/run_local.sh
   ```

4. **Access locally**:
   - UI: http://localhost:8080 (local) or https://odace.services.d4g.fr (production)
   - API Docs: http://localhost:8080/docs or https://odace.services.d4g.fr/docs

Storage: [Scaleway S3](docs/SCALEWAY_S3.md). Database: [PostgreSQL](docs/POSTGRESQL.md). Pipelines: [docs/pipelines.md](docs/pipelines.md).

## License

Internal use only - Odace project.
