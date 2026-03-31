-- +goose Up
-- Enable pgcrypto for symmetric at-rest encryption of sensitive credentials.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Convert minio_secret_key from plain TEXT to BYTEA for pgcrypto encrypted storage.
-- Existing rows (which hold empty or unencrypted values from migration 008) are
-- set to NULL. Tenants will receive a clear error on next session creation and
-- must be re-registered (or credentials manually rotated) to restore access.
ALTER TABLE service_accounts ALTER COLUMN minio_secret_key DROP DEFAULT;
ALTER TABLE service_accounts ALTER COLUMN minio_secret_key DROP NOT NULL;
ALTER TABLE service_accounts ALTER COLUMN minio_secret_key TYPE BYTEA USING NULL;

-- +goose Down
-- WARNING: Rolling back this migration after encrypted data has been written is
-- destructive. Encrypted BYTEA values cannot be recovered as plain TEXT without
-- the encryption key. Only roll back on a fresh installation with no live tenant data.
ALTER TABLE service_accounts ALTER COLUMN minio_secret_key TYPE TEXT USING '';
ALTER TABLE service_accounts ALTER COLUMN minio_secret_key SET NOT NULL;
ALTER TABLE service_accounts ALTER COLUMN minio_secret_key SET DEFAULT '';
