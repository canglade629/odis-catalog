# PostgreSQL

The Odace app uses **PostgreSQL** for all persistent application data: API keys, data catalogue cache, jobs, job logs, table certifications, and query tracking.

## Configuration

Use either individual variables or a single URL:

**Option A – individual variables**

| Variable | Description |
|----------|-------------|
| `PG_DB_HOST` | Database host |
| `PG_DB_PORT` | Port (default 5432) |
| `PG_DB_NAME` | Database name |
| `PG_DB_USER` | User |
| `PG_DB_PWD` | Password |

**Option B – connection URL**

- `DATABASE_URL=postgresql://user:password@host:port/dbname`  
  The app will use the async driver (`postgresql+asyncpg`) for the main API and a sync URL for background tasks (e.g. log writing) when needed.

## Schema

Apply the schema once after creating the database:

```bash
psql -h $PG_DB_HOST -p $PG_DB_PORT -d $PG_DB_NAME -U $PG_DB_USER -f app/db/schema.sql
```

Or with a URL:

```bash
psql "$DATABASE_URL" -f app/db/schema.sql
```

Schema file: [app/db/schema.sql](../app/db/schema.sql).

## Tables

| Table | Purpose |
|-------|---------|
| `api_keys` | API key hashes, user_id, active flag, last_used_at |
| `data_catalogue` | Cached catalogue (from YAML sync + optional enrichment) |
| `table_certifications` | Certification status per table |
| `jobs` | Pipeline job metadata and progress |
| `job_tasks` | Per-task status and stats |
| `job_logs` | Pipeline run logs (batched writes) |
| `query_tracker` | Per-table, per-user query counts |

## Sync catalogue from YAML

To populate or refresh the catalogue cache from the YAML config:

```bash
python scripts/sync_catalogue_to_db.py
```

Requires `PG_*` or `DATABASE_URL` and reads `config/data_catalogue.yaml`.

## Backup and connections

- Use your provider’s backup tools (e.g. Scaleway Managed Database backups) for point-in-time recovery.
- Ensure the app can reach PostgreSQL (security groups / allowed IPs, SSL if required). The app uses asyncpg by default for the API and a sync driver only where needed (e.g. log handler in a background thread).
