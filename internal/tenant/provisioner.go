package tenant

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
	"github.com/satheeshds/nexus/internal/duckdb"
	"github.com/satheeshds/nexus/internal/storage"
	"golang.org/x/crypto/bcrypt"
)

// RegisterRequest is the input for provisioning a new tenant.
type RegisterRequest struct {
	OrgName  string
	Email    string
	Password string // Required: customer's login password (bcrypt-hashed before storage)
}

// RegisterResponse is returned after successful provisioning.
type RegisterResponse struct {
	TenantID string
}

// Provisioner orchestrates register/delete of tenants across all subsystems.
type Provisioner struct {
	db       *catalog.DB
	store    *storage.Client
	pgCfg    config.PostgresConfig
	minioCfg config.MinIOConfig
	dlCfg    config.DuckLakeConfig
}

func NewProvisioner(
	db *catalog.DB,
	store *storage.Client,
	pgCfg config.PostgresConfig,
	minioCfg config.MinIOConfig,
	dlCfg config.DuckLakeConfig,
) *Provisioner {
	return &Provisioner{
		db:       db,
		store:    store,
		pgCfg:    pgCfg,
		minioCfg: minioCfg,
		dlCfg:    dlCfg,
	}
}

// Register provisions a new customer tenant end-to-end.
// It also automatically creates a paired service account for internal operations
// (e.g. data ingestion). The service account API key is returned once and must be
// stored by the caller; it is never visible through customer-facing APIs.
func (p *Provisioner) Register(ctx context.Context, req RegisterRequest) (*RegisterResponse, error) {
	if req.Password == "" {
		return nil, fmt.Errorf("password is required")
	}

	tenantID := makeSlug(req.OrgName)
	s3Prefix := fmt.Sprintf("%s/%s", p.dlCfg.TenantBasePath, tenantID)
	pgSchema := fmt.Sprintf("ducklake_%s", tenantID)

	slog.Info("provisioning tenant", "tenant", tenantID)

	// Hash the customer's password.
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	// Step 1: Create Postgres schema for DuckLake metadata.
	if err := p.db.CreateTenantSchema(ctx, pgSchema); err != nil {
		return nil, fmt.Errorf("create schema: %w", err)
	}

	// Step 2: Initialize DuckLake catalog (writes initial snapshot tables to PG).
	if err := p.initDuckLake(ctx, tenantID, s3Prefix, pgSchema); err != nil {
		_ = p.db.DropTenantSchema(ctx, pgSchema)
		return nil, fmt.Errorf("init ducklake: %w", err)
	}

	// Step 3: Provision MinIO service account scoped to tenant prefix.
	minioCreds, err := p.store.ProvisionTenant(ctx, tenantID, s3Prefix)
	if err != nil {
		_ = p.db.DropTenantSchema(ctx, pgSchema)
		return nil, fmt.Errorf("provision minio: %w", err)
	}
	minioAccessKey := minioCreds.AccessKey
	minioSecretKey := minioCreds.SecretKey

	// Step 4: Persist customer tenant record.
	customer := catalog.Tenant{
		ID:           tenantID,
		OrgName:      req.OrgName,
		Email:        req.Email,
		S3Prefix:     s3Prefix,
		PGSchema:     pgSchema,
		PasswordHash: string(passwordHash),
		CreatedAt:    time.Now(),
	}
	if err := p.db.InsertTenant(ctx, customer); err != nil {
		// Rollback: deprovision MinIO and drop schema
		_ = p.store.DeprovisionTenant(ctx, minioAccessKey)
		_ = p.db.DropTenantSchema(ctx, pgSchema)
		return nil, fmt.Errorf("insert customer record: %w", err)
	}

	// Step 5: Auto-create a service account for internal operations (data ingestion, etc.).
	// The service account shares the same S3 prefix and PG schema as the customer so that
	// internal services operate in the same data namespace.
	serviceAPIKey, err := generateAPIKey()
	if err != nil {
		return nil, fmt.Errorf("generate service account key: %w", err)
	}
	serviceKeyHash, err := bcrypt.GenerateFromPassword([]byte(serviceAPIKey), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash service account key: %w", err)
	}
	serviceID := tenantID + "_svc"
	svcAccount := catalog.ServiceAccount{
		ID:             serviceID,
		TenantID:       tenantID,
		S3Prefix:       s3Prefix,
		PGSchema:       pgSchema,
		MinioAccessKey: minioAccessKey,
		MinioSecretKey: minioSecretKey,
		APIKeyHash:     string(serviceKeyHash),
		CreatedAt:      time.Now(),
	}
	if err := p.db.InsertServiceAccount(ctx, svcAccount); err != nil {
		// Rollback: delete customer record, deprovision MinIO, drop schema
		_ = p.db.DeleteTenant(ctx, tenantID)
		_ = p.store.DeprovisionTenant(ctx, minioAccessKey)
		_ = p.db.DropTenantSchema(ctx, pgSchema)
		return nil, fmt.Errorf("insert service account record: %w", err)
	}

	slog.Info("tenant provisioned", "tenant", tenantID, "service_account", serviceID)
	return &RegisterResponse{
		TenantID: tenantID,
	}, nil
}

// RotateServiceAccountKey generates a new API key for the tenant's service account,
// stores its bcrypt hash, and returns the new plain key along with the service account ID.
func (p *Provisioner) RotateServiceAccountKey(ctx context.Context, tenantID string) (string, string, error) {
	newKey, err := generateAPIKey()
	if err != nil {
		return "", "", fmt.Errorf("generate api key: %w", err)
	}
	newHash, err := bcrypt.GenerateFromPassword([]byte(newKey), bcrypt.DefaultCost)
	if err != nil {
		return "", "", fmt.Errorf("hash api key: %w", err)
	}

	// Lookup service ID to ensure consistency and return accurate metadata.
	sa, err := p.db.GetServiceAccountByTenantID(ctx, tenantID)
	if err != nil {
		return "", "", fmt.Errorf("get service account: %w", err)
	}

	if err := p.db.UpdateServiceAccountKeyHash(ctx, tenantID, string(newHash)); err != nil {
		return "", "", fmt.Errorf("update api key hash: %w", err)
	}

	slog.Info("service account key rotated", "tenant", tenantID, "service_id", sa.ID)
	return newKey, sa.ID, nil
}

// Delete tears down a tenant's catalog, MinIO account, and registry record.
func (p *Provisioner) Delete(ctx context.Context, tenantID string) error {
	t, err := p.db.GetTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant: %w", err)
	}

	// Look up the service account to get the MinIO access key for deprovisioning.
	svcAccount, err := p.db.GetServiceAccountByTenantID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get service account: %w", err)
	}

	// Tear down storage (MinIO) for this tenant.
	if err := p.store.DeprovisionTenant(ctx, svcAccount.MinioAccessKey); err != nil {
		return fmt.Errorf("deprovision storage: %w", err)
	}
	// Drop DuckLake schema (cascade removes all metadata)
	if err := p.db.DropTenantSchema(ctx, t.PGSchema); err != nil {
		return fmt.Errorf("drop schema: %w", err)
	}

	// Remove tenant record (service_accounts row is removed by ON DELETE CASCADE)
	if err := p.db.DeleteTenant(ctx, tenantID); err != nil {
		return fmt.Errorf("delete tenant record: %w", err)
	}

	slog.Info("tenant deprovisioned", "tenant", tenantID)
	return nil
}

// initDuckLake opens a short-lived DuckDB session to ATTACH the tenant's
// DuckLake catalog. This causes DuckLake to initialize its metadata tables
// inside the Postgres schema. The session is closed immediately after.
func (p *Provisioner) initDuckLake(ctx context.Context, tenantID, s3Prefix, pgSchema string) error {
	conn, err := duckdb.OpenForTenant(ctx, tenantID, p.pgCfg, p.minioCfg, s3Prefix, pgSchema)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Verify the attachment is healthy
	_, err = conn.ExecContext(ctx, "SELECT 1")
	return err
}

// makeSlug converts an org name to a safe tenant ID string.
// "Acme Corp!" → "acme_corp"
func makeSlug(orgName string) string {
	slug := strings.ToLower(orgName)
	slug = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			return r
		case r == ' ' || r == '-':
			return '_'
		default:
			return -1 // drop
		}
	}, slug)
	// Suffix with short UUID to ensure uniqueness
	short := uuid.New().String()[:8]
	return fmt.Sprintf("%s_%s", slug, short)
}

// generateAPIKey returns a cryptographically random 32-byte hex string.
func generateAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
