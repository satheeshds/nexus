package catalog

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/satheeshds/nexus/internal/config"
)

// Tenant is the canonical representation of a provisioned tenant.
type Tenant struct {
	ID          string    `json:"id"`
	OrgName     string    `json:"org_name"`
	Email       string    `json:"email"`
	S3Prefix    string    `json:"s3_prefix"`
	PGSchema    string    `json:"pg_schema"`
	AccountType string    `json:"account_type"` // "customer" or "service"
	APIKeyHash  string    `json:"-"`            // bcrypt hash – never serialised in API responses
	CreatedAt   time.Time `json:"created_at"`
}

// DB wraps a pgxpool and exposes catalog operations.
type DB struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, cfg config.PostgresConfig) (*DB, error) {
	pool, err := pgxpool.New(ctx, cfg.URL())
	if err != nil {
		return nil, fmt.Errorf("create pg pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &DB{pool: pool}, nil
}

func (db *DB) Close() {
	db.pool.Close()
}

// Pool returns the underlying connection pool (used by goose migrations).
func (db *DB) Pool() *pgxpool.Pool {
	return db.pool
}

// CreateTenantSchema creates the PG schema that DuckLake will populate.
func (db *DB) CreateTenantSchema(ctx context.Context, pgSchema string) error {
	_, err := db.pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, pgSchema))
	if err != nil {
		return fmt.Errorf("create schema %q: %w", pgSchema, err)
	}
	return nil
}

// DropTenantSchema removes the schema and all DuckLake metadata within it.
func (db *DB) DropTenantSchema(ctx context.Context, pgSchema string) error {
	_, err := db.pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, pgSchema))
	if err != nil {
		return fmt.Errorf("drop schema %q: %w", pgSchema, err)
	}
	return nil
}

// InsertTenant stores the tenant record after provisioning is complete.
func (db *DB) InsertTenant(ctx context.Context, t Tenant) error {
	_, err := db.pool.Exec(ctx, `
		INSERT INTO tenants (id, org_name, email, s3_prefix, pg_schema, account_type, api_key_hash, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, t.ID, t.OrgName, t.Email, t.S3Prefix, t.PGSchema, t.AccountType, t.APIKeyHash, t.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert tenant: %w", err)
	}
	return nil
}

// GetTenant retrieves a tenant by ID.
func (db *DB) GetTenant(ctx context.Context, id string) (*Tenant, error) {
	row := db.pool.QueryRow(ctx, `
		SELECT id, org_name, email, s3_prefix, pg_schema, account_type, api_key_hash, created_at
		FROM tenants WHERE id = $1
	`, id)
	var t Tenant
	if err := row.Scan(&t.ID, &t.OrgName, &t.Email, &t.S3Prefix, &t.PGSchema, &t.AccountType, &t.APIKeyHash, &t.CreatedAt); err != nil {
		return nil, fmt.Errorf("get tenant %q: %w", id, err)
	}
	return &t, nil
}

// DeleteTenant removes the tenant record.
func (db *DB) DeleteTenant(ctx context.Context, id string) error {
	_, err := db.pool.Exec(ctx, `DELETE FROM tenants WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete tenant %q: %w", id, err)
	}
	return nil
}

// ListTenants returns all tenants.
func (db *DB) ListTenants(ctx context.Context) ([]Tenant, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT id, org_name, email, s3_prefix, pg_schema, account_type, api_key_hash, created_at
		FROM tenants ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()

	var tenants []Tenant
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.ID, &t.OrgName, &t.Email, &t.S3Prefix, &t.PGSchema, &t.AccountType, &t.APIKeyHash, &t.CreatedAt); err != nil {
			return nil, err
		}
		tenants = append(tenants, t)
	}
	return tenants, nil
}

// ListCustomerTenants returns only customer-type tenants (excludes service accounts).
func (db *DB) ListCustomerTenants(ctx context.Context) ([]Tenant, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT id, org_name, email, s3_prefix, pg_schema, account_type, api_key_hash, created_at
		FROM tenants WHERE account_type = 'customer' ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list customer tenants: %w", err)
	}
	defer rows.Close()

	var tenants []Tenant
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.ID, &t.OrgName, &t.Email, &t.S3Prefix, &t.PGSchema, &t.AccountType, &t.APIKeyHash, &t.CreatedAt); err != nil {
			return nil, err
		}
		tenants = append(tenants, t)
	}
	return tenants, nil
}
