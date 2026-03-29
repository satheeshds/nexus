-- +goose Up
-- Create a dedicated table for service accounts, linked to their parent customer tenant.
-- This replaces storing service accounts as rows in the tenants table.
CREATE TABLE IF NOT EXISTS service_accounts (
    id               TEXT PRIMARY KEY,
    tenant_id        TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    s3_prefix        TEXT NOT NULL,
    pg_schema        TEXT NOT NULL,
    minio_access_key TEXT NOT NULL DEFAULT '',
    api_key_hash     TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS service_accounts_tenant_id_idx ON service_accounts (tenant_id);

-- +goose Down
DROP INDEX IF EXISTS service_accounts_tenant_id_idx;
DROP TABLE IF EXISTS service_accounts;
