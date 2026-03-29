-- +goose Up
-- Add account_type column to distinguish between customer and service accounts.
-- Customer accounts are visible to users; service accounts are internal only (e.g., data ingestion).
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS account_type TEXT NOT NULL DEFAULT 'customer' CHECK (account_type IN ('customer', 'service'));

-- +goose Down
ALTER TABLE tenants DROP COLUMN IF EXISTS account_type;
