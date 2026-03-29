-- +goose Up
-- Existing tenants will have an empty api_key_hash and cannot log in via the
-- /login endpoint until they re-register. This is intentional for the migration
-- period; a future data migration can populate hashes if needed.
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS api_key_hash TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE tenants DROP COLUMN IF EXISTS api_key_hash;
