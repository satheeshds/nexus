package catalog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/satheeshds/nexus/internal/config"
)

// ErrNotFound is returned when a requested record does not exist.
var ErrNotFound = errors.New("not found")

// Tenant is the canonical representation of a provisioned customer tenant.
type Tenant struct {
	ID           string    `json:"id"`
	OrgName      string    `json:"org_name"`
	Email        string    `json:"email"`
	S3Prefix     string    `json:"s3_prefix"`
	PGSchema     string    `json:"pg_schema"`
	PasswordHash string    `json:"-"` // bcrypt hash of customer login password – never serialised in API responses
	CreatedAt    time.Time `json:"created_at"`
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
		INSERT INTO tenants (id, org_name, email, s3_prefix, pg_schema, password_hash, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, t.ID, t.OrgName, t.Email, t.S3Prefix, t.PGSchema, t.PasswordHash, t.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert tenant: %w", err)
	}
	return nil
}

// GetTenant retrieves a tenant by ID.
func (db *DB) GetTenant(ctx context.Context, id string) (*Tenant, error) {
	row := db.pool.QueryRow(ctx, `
		SELECT id, org_name, email, s3_prefix, pg_schema, password_hash, created_at
		FROM tenants WHERE id = $1
	`, id)
	var t Tenant
	if err := row.Scan(&t.ID, &t.OrgName, &t.Email, &t.S3Prefix, &t.PGSchema, &t.PasswordHash, &t.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("get tenant %q: %w", id, ErrNotFound)
		}
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

// ServiceAccount represents a service account linked to a customer tenant.
// Service accounts are used by internal services (e.g., data ingestion pipelines)
// to access the tenant's data namespace.
type ServiceAccount struct {
	ID               string    `json:"id"`
	TenantID         string    `json:"tenant_id"`
	S3Prefix         string    `json:"s3_prefix"`
	PGSchema         string    `json:"pg_schema"`
	MinioAccessKey   string    `json:"-"` // stored for deprovisioning; never exposed in API responses
	MinioSecretKey   string    `json:"-"` // stored for session initialization; never exposed in API responses
	APIKeyHash       string    `json:"-"` // bcrypt hash; never exposed
	APIKeyCiphertext string    `json:"-"` // encrypted key used for TTL-based key reuse; never exposed
	APIKeyRotatedAt  time.Time `json:"-"`
	CreatedAt        time.Time `json:"created_at"`
}

// InsertServiceAccount stores a new service account record.
func (db *DB) InsertServiceAccount(ctx context.Context, sa ServiceAccount) error {
	rotatedAt := sa.APIKeyRotatedAt
	switch {
	case rotatedAt.IsZero() && !sa.CreatedAt.IsZero():
		rotatedAt = sa.CreatedAt
	case rotatedAt.IsZero():
		rotatedAt = time.Now().UTC()
	}
	_, err := db.pool.Exec(ctx, `
		INSERT INTO service_accounts (id, tenant_id, s3_prefix, pg_schema, minio_access_key, minio_secret_key, api_key_hash, api_key_ciphertext, api_key_rotated_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, sa.ID, sa.TenantID, sa.S3Prefix, sa.PGSchema, sa.MinioAccessKey, sa.MinioSecretKey, sa.APIKeyHash, sa.APIKeyCiphertext, rotatedAt, sa.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert service account: %w", err)
	}
	return nil
}

// GetServiceAccountByTenantID retrieves the service account for a given customer tenant.
func (db *DB) GetServiceAccountByTenantID(ctx context.Context, tenantID string) (*ServiceAccount, error) {
	sa, err := db.querySingleServiceAccount(ctx, `
		SELECT id, tenant_id, s3_prefix, pg_schema, minio_access_key, minio_secret_key, api_key_hash, COALESCE(api_key_ciphertext, ''), COALESCE(api_key_rotated_at, created_at), created_at
		FROM service_accounts WHERE tenant_id = $1
	`, tenantID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("get service account for tenant %q: %w", tenantID, ErrNotFound)
		}
		return nil, fmt.Errorf("get service account for tenant %q: %w", tenantID, err)
	}
	return sa, nil
}

// GetServiceAccount retrieves a service account by its ID.
func (db *DB) GetServiceAccount(ctx context.Context, id string) (*ServiceAccount, error) {
	sa, err := db.querySingleServiceAccount(ctx, `
		SELECT id, tenant_id, s3_prefix, pg_schema, minio_access_key, minio_secret_key, api_key_hash, COALESCE(api_key_ciphertext, ''), COALESCE(api_key_rotated_at, created_at), created_at
		FROM service_accounts WHERE id = $1
	`, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("get service account %q: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("get service account %q: %w", id, err)
	}
	return sa, nil
}

func (db *DB) querySingleServiceAccount(ctx context.Context, query string, arg string) (*ServiceAccount, error) {
	row := db.pool.QueryRow(ctx, query, arg)
	var sa ServiceAccount
	if err := row.Scan(&sa.ID, &sa.TenantID, &sa.S3Prefix, &sa.PGSchema, &sa.MinioAccessKey, &sa.MinioSecretKey, &sa.APIKeyHash, &sa.APIKeyCiphertext, &sa.APIKeyRotatedAt, &sa.CreatedAt); err != nil {
		return nil, err
	}
	return &sa, nil
}

// UpdateServiceAccountKey replaces the stored service account API key metadata.
// Call this after generating a new key during rotation.
func (db *DB) UpdateServiceAccountKey(ctx context.Context, tenantID, newHash, encryptedKey string, rotatedAt time.Time) error {
	tag, err := db.pool.Exec(ctx, `
		UPDATE service_accounts
		SET api_key_hash = $1, api_key_ciphertext = $2, api_key_rotated_at = $3
		WHERE tenant_id = $4
	`, newHash, encryptedKey, rotatedAt, tenantID)
	if err != nil {
		return fmt.Errorf("update api_key_hash for tenant %q: %w", tenantID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no service account found for tenant %q: %w", tenantID, ErrNotFound)
	}
	return nil
}

// DeleteServiceAccountByTenantID removes the service account for a given customer tenant.
func (db *DB) DeleteServiceAccountByTenantID(ctx context.Context, tenantID string) error {
	_, err := db.pool.Exec(ctx, `DELETE FROM service_accounts WHERE tenant_id = $1`, tenantID)
	if err != nil {
		return fmt.Errorf("delete service account for tenant %q: %w", tenantID, err)
	}
	return nil
}

// GetTenantByEmail retrieves a tenant by email address.
func (db *DB) GetTenantByEmail(ctx context.Context, email string) (*Tenant, error) {
	row := db.pool.QueryRow(ctx, `
		SELECT id, org_name, email, s3_prefix, pg_schema, password_hash, created_at
		FROM tenants WHERE email = $1
	`, email)
	var t Tenant
	if err := row.Scan(&t.ID, &t.OrgName, &t.Email, &t.S3Prefix, &t.PGSchema, &t.PasswordHash, &t.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("get tenant by email %q: %w", email, ErrNotFound)
		}
		return nil, fmt.Errorf("get tenant by email %q: %w", email, err)
	}
	return &t, nil
}

// ListTenants returns all tenants.
func (db *DB) ListTenants(ctx context.Context) ([]Tenant, error) {
	rows, err := db.pool.Query(ctx, `
		SELECT id, org_name, email, s3_prefix, pg_schema, password_hash, created_at
		FROM tenants ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()

	var tenants []Tenant
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.ID, &t.OrgName, &t.Email, &t.S3Prefix, &t.PGSchema, &t.PasswordHash, &t.CreatedAt); err != nil {
			return nil, err
		}
		tenants = append(tenants, t)
	}
	return tenants, nil
}
