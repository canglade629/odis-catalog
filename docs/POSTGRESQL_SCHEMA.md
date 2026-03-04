# PostgreSQL schema reference

This document describes the PostgreSQL schema used by the Odace backend. The source of truth is [app/db/schema.sql](../app/db/schema.sql). To apply the schema: `psql ... -f app/db/schema.sql`. For configuration and backup, see [POSTGRESQL.md](POSTGRESQL.md).

---

## api_keys

Stores API key hashes and metadata (replaces Firestore collection `api_keys`; document id was the key hash). `user_id` is the identity (e.g. "salma", "ronan"); `is_admin` controls admin vs read-only access.

| Column       | Type         | Constraints                    |
|-------------|--------------|--------------------------------|
| key_hash    | VARCHAR(64)  | PRIMARY KEY                    |
| user_id     | VARCHAR(512) | NOT NULL                       |
| is_admin    | BOOLEAN      | NOT NULL, DEFAULT FALSE       |
| created_at  | TIMESTAMPTZ  | NOT NULL, DEFAULT NOW()        |
| last_used_at| TIMESTAMPTZ  | —                              |
| active      | BOOLEAN      | NOT NULL, DEFAULT TRUE         |

---

## data_catalogue

Cached data catalogue entries (replaces Firestore `data_catalogue` / `silver_tables`). Populated from YAML via `scripts/sync_catalogue_to_db.py` and optionally enriched with schema/preview from S3.

| Column    | Type        | Constraints             |
|-----------|-------------|-------------------------|
| id        | VARCHAR(128)| PRIMARY KEY             |
| document  | JSONB       | NOT NULL                |
| updated_at| TIMESTAMPTZ | NOT NULL, DEFAULT NOW() |

---

## table_certifications

Certification status per table (replaces Firestore `table_certifications`).

| Column     | Type        | Constraints             |
|------------|-------------|-------------------------|
| id         | VARCHAR(256)| PRIMARY KEY             |
| layer      | VARCHAR(64) | NOT NULL                |
| table_name | VARCHAR(256)| NOT NULL                |
| certified  | BOOLEAN     | NOT NULL, DEFAULT TRUE  |
| certified_at | TIMESTAMPTZ| —                       |
| certified_by | VARCHAR(256)| —                      |

---

## jobs

Pipeline job metadata and progress (replaces Firestore collection `jobs`).

| Column         | Type        | Constraints             |
|----------------|-------------|-------------------------|
| job_id         | VARCHAR(64) | PRIMARY KEY             |
| job_name       | VARCHAR(512)| NOT NULL                |
| status         | VARCHAR(32) | NOT NULL                |
| started_at     | TIMESTAMPTZ | —                       |
| completed_at   | TIMESTAMPTZ | —                       |
| total_tasks    | INTEGER     | NOT NULL, DEFAULT 0     |
| completed_tasks| INTEGER     | NOT NULL, DEFAULT 0     |
| failed_tasks   | INTEGER     | NOT NULL, DEFAULT 0     |
| progress_percent | REAL     | NOT NULL, DEFAULT 0.0   |
| user_id        | VARCHAR(256)| —                       |

---

## job_tasks

Per-task status and stats for each job (replaces Firestore subcollection `jobs/{id}/tasks`). Rows are deleted when the parent job is deleted (CASCADE).

| Column          | Type        | Constraints                          |
|-----------------|-------------|--------------------------------------|
| job_id          | VARCHAR(64) | NOT NULL, REFERENCES jobs(job_id) ON DELETE CASCADE |
| task_id         | VARCHAR(64) | NOT NULL                             |
| pipeline_name   | VARCHAR(256)| NOT NULL                             |
| layer           | VARCHAR(64) | NOT NULL                             |
| status          | VARCHAR(32) | NOT NULL                             |
| started_at      | TIMESTAMPTZ | —                                    |
| completed_at    | TIMESTAMPTZ | —                                    |
| duration_seconds| REAL        | —                                    |
| message         | TEXT        | —                                    |
| error           | TEXT        | —                                    |
| stats           | JSONB       | —                                    |

**Primary key:** `(job_id, task_id)`

---

## job_logs

Pipeline run logs, written in batches (replaces Firestore `jobs/{id}/logs`).

| Column     | Type        | Constraints             |
|------------|-------------|-------------------------|
| id         | SERIAL      | PRIMARY KEY             |
| job_id     | VARCHAR(64) | NOT NULL                |
| timestamp  | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() |
| level      | VARCHAR(32) | —                       |
| message    | TEXT        | —                       |
| logger_name| VARCHAR(256)| —                       |
| task_id    | VARCHAR(64) | —                       |

**Index:** `idx_job_logs_job_id` on `job_id`

---

## query_tracker

Per-table, per-user query counts (replaces Firestore `tables/{table_name}/users/{user_id}`).

| Column       | Type        | Constraints             |
|--------------|-------------|-------------------------|
| table_name   | VARCHAR(256)| NOT NULL                |
| user_id      | VARCHAR(256)| NOT NULL                |
| query_count  | INTEGER     | NOT NULL, DEFAULT 0     |
| last_query_at| TIMESTAMPTZ | NOT NULL, DEFAULT NOW()|

**Primary key:** `(table_name, user_id)`
