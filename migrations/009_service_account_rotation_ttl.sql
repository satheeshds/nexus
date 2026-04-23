-- +goose Up
-- Persist the latest encrypted service account API key and its rotation timestamp
-- so the rotate endpoint can return the same credentials until TTL expiry.
ALTER TABLE service_accounts
    ADD COLUMN IF NOT EXISTS api_key_ciphertext TEXT;

ALTER TABLE service_accounts
    ADD COLUMN IF NOT EXISTS api_key_rotated_at TIMESTAMPTZ;

-- +goose Down
ALTER TABLE service_accounts DROP COLUMN IF EXISTS api_key_rotated_at;
ALTER TABLE service_accounts DROP COLUMN IF EXISTS api_key_ciphertext;
