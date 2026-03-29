-- +goose Up
CREATE TABLE IF NOT EXISTS tenants (
    id         TEXT PRIMARY KEY,
    org_name   TEXT NOT NULL,
    email      TEXT NOT NULL UNIQUE,
    s3_prefix  TEXT NOT NULL,
    pg_schema  TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF EXISTS tenants;
