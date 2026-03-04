-- PostgreSQL schema for Odace backend (replaces Firestore)
-- Run once to create tables (e.g. psql or migration tool).

-- API keys (was Firestore collection api_keys, doc id = key hash)
CREATE TABLE IF NOT EXISTS api_keys (
    key_hash VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(512) NOT NULL,
    is_admin BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

-- Data catalogue (was Firestore data_catalogue/silver_tables)
CREATE TABLE IF NOT EXISTS data_catalogue (
    id VARCHAR(128) PRIMARY KEY,
    document JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table certifications (was Firestore table_certifications)
CREATE TABLE IF NOT EXISTS table_certifications (
    id VARCHAR(256) PRIMARY KEY,
    layer VARCHAR(64) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    certified BOOLEAN NOT NULL DEFAULT TRUE,
    certified_at TIMESTAMPTZ,
    certified_by VARCHAR(256)
);

-- Jobs (was Firestore collection jobs)
CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(64) PRIMARY KEY,
    job_name VARCHAR(512) NOT NULL,
    status VARCHAR(32) NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    total_tasks INTEGER NOT NULL DEFAULT 0,
    completed_tasks INTEGER NOT NULL DEFAULT 0,
    failed_tasks INTEGER NOT NULL DEFAULT 0,
    progress_percent REAL NOT NULL DEFAULT 0.0,
    user_id VARCHAR(256)
);

-- Job tasks (was Firestore jobs/{id}/tasks)
CREATE TABLE IF NOT EXISTS job_tasks (
    job_id VARCHAR(64) NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    task_id VARCHAR(64) NOT NULL,
    pipeline_name VARCHAR(256) NOT NULL,
    layer VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    duration_seconds REAL,
    message TEXT,
    error TEXT,
    stats JSONB,
    PRIMARY KEY (job_id, task_id)
);

-- Job logs (was Firestore jobs/{id}/logs)
CREATE TABLE IF NOT EXISTS job_logs (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(64) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    level VARCHAR(32),
    message TEXT,
    logger_name VARCHAR(256),
    task_id VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);

-- Query tracker (was Firestore tables/{table_name}/users/{user_id})
CREATE TABLE IF NOT EXISTS query_tracker (
    table_name VARCHAR(256) NOT NULL,
    user_id VARCHAR(256) NOT NULL,
    query_count INTEGER NOT NULL DEFAULT 0,
    last_query_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_name, user_id)
);
