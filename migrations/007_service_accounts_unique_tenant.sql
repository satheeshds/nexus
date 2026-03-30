-- +goose Up
-- Enforce one service account per tenant. Replace the non-unique index with a
-- unique index so that UpdateServiceAccountKeyHash always targets exactly one row.

-- Remove duplicate tenant rows, keeping the most recently created one.
DELETE FROM service_accounts
WHERE id NOT IN (
    SELECT DISTINCT ON (tenant_id) id
    FROM service_accounts
    ORDER BY tenant_id, created_at DESC
);

DROP INDEX IF EXISTS service_accounts_tenant_id_idx;
CREATE UNIQUE INDEX service_accounts_tenant_id_idx ON service_accounts (tenant_id);

-- +goose Down
DROP INDEX IF EXISTS service_accounts_tenant_id_idx;
CREATE INDEX service_accounts_tenant_id_idx ON service_accounts (tenant_id);
