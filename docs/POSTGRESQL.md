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

Schema file: [app/db/schema.sql](../app/db/schema.sql). **Full schema reference:** [POSTGRESQL_SCHEMA.md](POSTGRESQL_SCHEMA.md).

**Existing databases:** If you already have `api_keys` without `is_admin`, run:
`psql "$DATABASE_URL" -f app/db/migrations/001_add_is_admin_to_api_keys.sql`

## Tables

| Table | Purpose |
|-------|---------|
| `api_keys` | API key hashes, user_id, is_admin, active flag, last_used_at |
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

## Persistence (API keys and data)

**The application never drops, truncates, or resets the database.** Schema is applied with `CREATE TABLE IF NOT EXISTS` only. If API keys (or other data) disappear, the cause is the **PostgreSQL instance** being recreated or replaced:

- **Ephemeral Postgres**: e.g. a Postgres container without a named volume — data is lost on container/VM restart or redeploy.
- **Different database**: `DATABASE_URL` or `PG_*` changed so the app now points at another (e.g. new, empty) database.

For production, use a **persistent** PostgreSQL instance (e.g. Scaleway Managed Database, or a Postgres container with a **named volume**) and keep the same connection settings across deploys so API keys, certifications, and jobs are retained.

## Backup and connections

- Use your provider’s backup tools (e.g. Scaleway Managed Database backups) for point-in-time recovery.
- Ensure the app can reach PostgreSQL (security groups / allowed IPs, SSL if required). The app uses asyncpg by default for the API and a sync driver only where needed (e.g. log handler in a background thread).
