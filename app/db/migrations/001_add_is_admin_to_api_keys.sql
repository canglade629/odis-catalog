-- Migration: add is_admin to api_keys (run on existing databases only)
-- New installs get this from schema.sql.
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS is_admin BOOLEAN NOT NULL DEFAULT FALSE;

-- Optional: grant admin to keys that were created with user_id = 'admin' before this column existed
-- UPDATE api_keys SET is_admin = TRUE WHERE user_id = 'admin';
