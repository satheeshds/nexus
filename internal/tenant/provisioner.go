package tenant

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/satheeshds/nexus/internal/auth"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
	"github.com/satheeshds/nexus/internal/duckdb"
	"github.com/satheeshds/nexus/internal/storage"
)

// RegisterRequest is the input for provisioning a new tenant.
type RegisterRequest struct {
	OrgName string
	Email   string
}

// RegisterResponse is returned after successful provisioning.
type RegisterResponse struct {
	TenantID string
	Token    string
}

// Provisioner orchestrates register/delete of tenants across all subsystems.
type Provisioner struct {
	db      *catalog.DB
	store   *storage.Client
	auth    *auth.Service
	pgCfg   config.PostgresConfig
	minioCfg config.MinIOConfig
	dlCfg   config.DuckLakeConfig
}

func NewProvisioner(
	db *catalog.DB,
	store *storage.Client,
	authSvc *auth.Service,
	pgCfg config.PostgresConfig,
	minioCfg config.MinIOConfig,
	dlCfg config.DuckLakeConfig,
) *Provisioner {
	return &Provisioner{
		db:      db,
		store:   store,
		auth:    authSvc,
		pgCfg:   pgCfg,
		minioCfg: minioCfg,
		dlCfg:   dlCfg,
	}
}

// Register provisions a new tenant end-to-end.
func (p *Provisioner) Register(ctx context.Context, req RegisterRequest) (*RegisterResponse, error) {
	tenantID := makeSlug(req.OrgName)
	s3Prefix  := fmt.Sprintf("%s/%s", p.dlCfg.TenantBasePath, tenantID)
	pgSchema   := fmt.Sprintf("ducklake_%s", tenantID)

	slog.Info("provisioning tenant", "tenant", tenantID)

	// Step 1: Create Postgres schema for DuckLake metadata
	if err := p.db.CreateTenantSchema(ctx, pgSchema); err != nil {
		return nil, fmt.Errorf("create schema: %w", err)
	}

	// Step 2: Initialize DuckLake catalog (writes initial snapshot tables to PG)
	if err := p.initDuckLake(ctx, tenantID, s3Prefix, pgSchema); err != nil {
		// Rollback schema on failure
		_ = p.db.DropTenantSchema(ctx, pgSchema)
		return nil, fmt.Errorf("init ducklake: %w", err)
	}

	// Step 3: Provision MinIO service account scoped to tenant prefix
	_, err := p.store.ProvisionTenant(ctx, tenantID, s3Prefix)
	if err != nil {
		_ = p.db.DropTenantSchema(ctx, pgSchema)
		return nil, fmt.Errorf("provision minio: %w", err)
	}

	// Step 4: Persist tenant record in catalog
	t := catalog.Tenant{
		ID:        tenantID,
		OrgName:   req.OrgName,
		Email:     req.Email,
		S3Prefix:  s3Prefix,
		PGSchema:  pgSchema,
		CreatedAt: time.Now(),
	}
	if err := p.db.InsertTenant(ctx, t); err != nil {
		return nil, fmt.Errorf("insert tenant record: %w", err)
	}

	// Step 5: Issue JWT
	token, err := p.auth.Issue(tenantID, req.OrgName, s3Prefix, pgSchema)
	if err != nil {
		return nil, fmt.Errorf("issue jwt: %w", err)
	}

	slog.Info("tenant provisioned", "tenant", tenantID)
	return &RegisterResponse{TenantID: tenantID, Token: token}, nil
}

// Delete tears down a tenant's catalog, MinIO account, and registry record.
func (p *Provisioner) Delete(ctx context.Context, tenantID string) error {
	t, err := p.db.GetTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant: %w", err)
	}

	// Tear down storage (MinIO) for this tenant.
	if err := p.store.DeprovisionTenant(ctx, tenantID); err != nil {
		return fmt.Errorf("deprovision storage: %w", err)
	}
	// Drop DuckLake schema (cascade removes all metadata)
	if err := p.db.DropTenantSchema(ctx, t.PGSchema); err != nil {
		return fmt.Errorf("drop schema: %w", err)
	}

	// Remove tenant record
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
