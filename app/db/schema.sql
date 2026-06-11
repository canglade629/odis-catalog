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

-- Query tracker (was Firestore tables/{table_name}/users/{user_id})
CREATE TABLE IF NOT EXISTS query_tracker (
    table_name VARCHAR(256) NOT NULL,
    user_id VARCHAR(256) NOT NULL,
    query_count INTEGER NOT NULL DEFAULT 0,
    last_query_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_name, user_id)
);
