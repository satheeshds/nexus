-- +goose Up
-- Add minio_secret_key column to service_accounts table to persist tenant-specific
-- MinIO credentials. This allows each session to use the correct scoped credentials
-- instead of relying on global config defaults.
ALTER TABLE service_accounts ADD COLUMN IF NOT EXISTS minio_secret_key TEXT;

-- +goose Down
ALTER TABLE service_accounts DROP COLUMN IF EXISTS minio_secret_key;
