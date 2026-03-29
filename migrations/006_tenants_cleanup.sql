-- +goose Up
-- Rename api_key_hash to password_hash: the tenants table now stores only customer
-- accounts, so the credential stored is always a login password hash.
-- Service account API key hashes are stored in the service_accounts table.
ALTER TABLE tenants RENAME COLUMN api_key_hash TO password_hash;

-- Drop account_type: service accounts now live in the service_accounts table,
-- so every row in tenants is a customer account. The column is redundant.
ALTER TABLE tenants DROP COLUMN IF EXISTS account_type;

-- +goose Down
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS account_type TEXT NOT NULL DEFAULT 'customer';
ALTER TABLE tenants RENAME COLUMN password_hash TO api_key_hash;
