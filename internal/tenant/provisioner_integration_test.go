//go:build integration

package tenant_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
	"github.com/satheeshds/nexus/internal/storage"
	"github.com/satheeshds/nexus/internal/tenant"
	"github.com/satheeshds/nexus/internal/testutil"
)

func newProvisioner(t *testing.T, catalogDB *catalog.DB, pg *testutil.PostgresContainer, minio *testutil.MinIOContainer) *tenant.Provisioner {
	t.Helper()
	storageClient, err := storage.New(minio.Config)
	if err != nil {
		t.Fatalf("create storage client: %v", err)
	}
	ctx := context.Background()
	if err := storageClient.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}
	dlCfg := config.DuckLakeConfig{TenantBasePath: "tenants"}
	return tenant.NewProvisioner(catalogDB, storageClient, pg.Config, minio.Config, dlCfg, 10*time.Minute, "integration-test-secret")
}

// TestProvisioner_Register verifies that registering a new tenant provisions
// all required resources: Postgres schema, MinIO service account, DuckLake
// catalog, tenant record, and auto-created service account.
func TestProvisioner_Register(t *testing.T) {
	testutil.CheckDuckDBExtensionsAvailable(t)
	pg := testutil.StartPostgres(t)
	minio := testutil.StartMinIO(t)
	catalogDB := testutil.NewCatalogDB(t, pg)
	p := newProvisioner(t, catalogDB, pg, minio)
	ctx := context.Background()

	req := tenant.RegisterRequest{
		OrgName:  "Integration Test Co",
		Email:    "it@integration.test",
		Password: "supersecret",
	}

	resp, err := p.Register(ctx, req)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if resp.TenantID == "" {
		t.Error("expected non-empty TenantID in response")
	}

	// Verify tenant record was persisted.
	tnt, err := catalogDB.GetTenant(ctx, resp.TenantID)
	if err != nil {
		t.Fatalf("GetTenant after Register: %v", err)
	}
	if tnt.Email != req.Email {
		t.Errorf("tenant email: want %q got %q", req.Email, tnt.Email)
	}
	if tnt.S3Prefix == "" {
		t.Error("expected non-empty S3Prefix")
	}
	if tnt.PGSchema == "" {
		t.Error("expected non-empty PGSchema")
	}
	if tnt.PasswordHash == "" {
		t.Error("expected non-empty PasswordHash")
	}

	// Verify service account was created.
	sa, err := catalogDB.GetServiceAccountByTenantID(ctx, resp.TenantID)
	if err != nil {
		t.Fatalf("GetServiceAccountByTenantID after Register: %v", err)
	}
	if sa.TenantID != resp.TenantID {
		t.Errorf("service account TenantID: want %q got %q", resp.TenantID, sa.TenantID)
	}
	if sa.MinioAccessKey == "" {
		t.Error("expected non-empty MinioAccessKey on service account")
	}
	if sa.MinioSecretKey == "" {
		t.Error("expected non-empty MinioSecretKey on service account")
	}
}

// TestProvisioner_Register_RequiresPassword verifies that a missing password
// causes Register to return an error without creating any resources.
func TestProvisioner_Register_RequiresPassword(t *testing.T) {
	pg := testutil.StartPostgres(t)
	minio := testutil.StartMinIO(t)
	catalogDB := testutil.NewCatalogDB(t, pg)
	p := newProvisioner(t, catalogDB, pg, minio)
	ctx := context.Background()

	req := tenant.RegisterRequest{
		OrgName: "No Password Co",
		Email:   "nopass@integration.test",
		// Password intentionally omitted
	}
	if _, err := p.Register(ctx, req); err == nil {
		t.Error("expected error for missing password, got nil")
	}
}

// TestProvisioner_Delete verifies that deleting a tenant removes all associated
// resources from the catalog.
func TestProvisioner_Delete(t *testing.T) {
	testutil.CheckDuckDBExtensionsAvailable(t)
	pg := testutil.StartPostgres(t)
	minio := testutil.StartMinIO(t)
	catalogDB := testutil.NewCatalogDB(t, pg)
	p := newProvisioner(t, catalogDB, pg, minio)
	ctx := context.Background()

	resp, err := p.Register(ctx, tenant.RegisterRequest{
		OrgName:  "Delete Corp",
		Email:    "delete@integration.test",
		Password: "password",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := p.Delete(ctx, resp.TenantID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Tenant record should be gone.
	_, err = catalogDB.GetTenant(ctx, resp.TenantID)
	if !errors.Is(err, catalog.ErrNotFound) {
		t.Errorf("expected ErrNotFound after Delete, got %v", err)
	}

	// Service account should be cascade-deleted.
	_, err = catalogDB.GetServiceAccountByTenantID(ctx, resp.TenantID)
	if !errors.Is(err, catalog.ErrNotFound) {
		t.Errorf("expected service account to be gone after Delete, got %v", err)
	}
}

// TestProvisioner_RotateServiceAccountKey verifies that rotating the key
// updates the stored hash while keeping all other service account fields intact.
func TestProvisioner_RotateServiceAccountKey(t *testing.T) {
	testutil.CheckDuckDBExtensionsAvailable(t)
	pg := testutil.StartPostgres(t)
	minio := testutil.StartMinIO(t)
	catalogDB := testutil.NewCatalogDB(t, pg)
	p := newProvisioner(t, catalogDB, pg, minio)
	ctx := context.Background()

	resp, err := p.Register(ctx, tenant.RegisterRequest{
		OrgName:  "Rotate Corp",
		Email:    "rotate@integration.test",
		Password: "password",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Capture the original hash.
	saOriginal, err := catalogDB.GetServiceAccountByTenantID(ctx, resp.TenantID)
	if err != nil {
		t.Fatalf("GetServiceAccountByTenantID: %v", err)
	}
	originalHash := saOriginal.APIKeyHash

	// Rotate the key.
	newKey, serviceID, err := p.RotateServiceAccountKey(ctx, resp.TenantID, true)
	if err != nil {
		t.Fatalf("RotateServiceAccountKey: %v", err)
	}
	if newKey == "" {
		t.Error("expected non-empty new key")
	}
	if serviceID == "" {
		t.Error("expected non-empty service ID")
	}

	// Hash should have changed.
	saUpdated, err := catalogDB.GetServiceAccountByTenantID(ctx, resp.TenantID)
	if err != nil {
		t.Fatalf("GetServiceAccountByTenantID after rotate: %v", err)
	}
	if saUpdated.APIKeyHash == originalHash {
		t.Error("expected api_key_hash to change after rotation")
	}
	if saUpdated.APIKeyHash == "" {
		t.Error("expected non-empty api_key_hash after rotation")
	}
}

// TestProvisioner_RotateServiceAccountKey_TTLReuse verifies the same key is returned
// when rotation is requested within the TTL window, unless hard_reset is true.
func TestProvisioner_RotateServiceAccountKey_TTLReuse(t *testing.T) {
	testutil.CheckDuckDBExtensionsAvailable(t)
	pg := testutil.StartPostgres(t)
	minio := testutil.StartMinIO(t)
	catalogDB := testutil.NewCatalogDB(t, pg)
	p := newProvisioner(t, catalogDB, pg, minio)
	ctx := context.Background()

	resp, err := p.Register(ctx, tenant.RegisterRequest{
		OrgName:  "Rotate TTL Corp",
		Email:    "rotate-ttl@integration.test",
		Password: "password",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	key1, _, err := p.RotateServiceAccountKey(ctx, resp.TenantID, true)
	if err != nil {
		t.Fatalf("RotateServiceAccountKey hard reset 1: %v", err)
	}
	key2, _, err := p.RotateServiceAccountKey(ctx, resp.TenantID, false)
	if err != nil {
		t.Fatalf("RotateServiceAccountKey within ttl: %v", err)
	}
	if key1 != key2 {
		t.Errorf("expected same key within ttl; got %q and %q", key1, key2)
	}

	key3, _, err := p.RotateServiceAccountKey(ctx, resp.TenantID, true)
	if err != nil {
		t.Fatalf("RotateServiceAccountKey hard reset 2: %v", err)
	}
	if key3 == key2 {
		t.Error("expected hard reset to rotate key even within ttl")
	}
}

// TestProvisioner_DuplicateRegistration verifies that attempting to register
// the same email twice returns an error on the second attempt.
func TestProvisioner_DuplicateRegistration(t *testing.T) {
	testutil.CheckDuckDBExtensionsAvailable(t)
	pg := testutil.StartPostgres(t)
	minio := testutil.StartMinIO(t)
	catalogDB := testutil.NewCatalogDB(t, pg)
	p := newProvisioner(t, catalogDB, pg, minio)
	ctx := context.Background()

	req := tenant.RegisterRequest{
		OrgName:  "Dup Corp",
		Email:    "dup@integration.test",
		Password: "password",
	}
	if _, err := p.Register(ctx, req); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if _, err := p.Register(ctx, req); err == nil {
		t.Error("expected error on duplicate registration, got nil")
	}
}
