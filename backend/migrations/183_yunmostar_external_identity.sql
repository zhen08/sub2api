-- YunMoProject-managed users and API keys.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS source varchar(50) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS source_id varchar(255) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS source varchar(50) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS source_id varchar(255) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS tags jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS permissions jsonb NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_users_source_source_id
    ON users(source, source_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_active_source_source_id_unique
    ON users(source, source_id)
    WHERE deleted_at IS NULL AND source <> '' AND source_id <> '';

CREATE INDEX IF NOT EXISTS idx_api_keys_source_source_id
    ON api_keys(source, source_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_active_source_source_id_unique
    ON api_keys(source, source_id)
    WHERE deleted_at IS NULL AND source <> '' AND source_id <> '';

CREATE INDEX IF NOT EXISTS idx_api_keys_tags_gin ON api_keys USING gin(tags);
CREATE INDEX IF NOT EXISTS idx_api_keys_permissions_gin ON api_keys USING gin(permissions);
