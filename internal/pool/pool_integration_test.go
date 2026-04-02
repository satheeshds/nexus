//go:build integration

package pool_test

import (
	"context"
	"testing"
	"time"

	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
	"github.com/satheeshds/nexus/internal/pool"
	"github.com/satheeshds/nexus/internal/storage"
	"github.com/satheeshds/nexus/internal/tenant"
	"github.com/satheeshds/nexus/internal/testutil"
)

func registerTestTenant(t *testing.T, catalogDB *catalog.DB, pg *testutil.PostgresContainer, minio *testutil.MinIOContainer, email string) string {
	t.Helper()
	storageClient, err := storage.New(minio.Config)
	if err != nil {
		t.Fatalf("create storage client: %v", err)
	}
	ctx := context.Background()
	if err := storageClient.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}
	p := tenant.NewProvisioner(catalogDB, storageClient, pg.Config, minio.Config, config.DuckLakeConfig{TenantBasePath: "tenants"})
	resp, err := p.Register(ctx, tenant.RegisterRequest{
		OrgName:  "Pool Test Co",
		Email:    email,
		Password: "testpassword",
	})
	if err != nil {
		t.Fatalf("register tenant: %v", err)
	}
	return resp.TenantID
}

// TestPool_GetSession verifies that the pool creates a DuckDB session for a
// provisioned tenant and returns the same session on subsequent calls.
func TestPool_GetSession(t *testing.T) {
	testutil.CheckDuckDBExtensionsAvailable(t)
	pg := testutil.StartPostgres(t)
	minio := testutil.StartMinIO(t)
	catalogDB := testutil.NewCatalogDB(t, pg)

	tenantID := registerTestTenant(t, catalogDB, pg, minio, "pooltest@integration.test")

	poolCfg := config.PoolConfig{
		MaxIdleSessions:  1,
		SessionTTL:       5 * time.Minute,
		EvictionInterval: 1 * time.Minute,
	}
	p := pool.New(catalogDB, pg.Config, minio.Config, poolCfg)
	t.Cleanup(p.Close)

	ctx := context.Background()

	// First call: creates a new session.
	session1, err := p.Get(ctx, tenantID)
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if session1 == nil {
		t.Fatal("expected non-nil session")
	}
	if session1.TenantID != tenantID {
		t.Errorf("session TenantID: want %q got %q", tenantID, session1.TenantID)
	}
	if session1.Conn == nil {
		t.Error("expected non-nil Conn in session")
	}

	// Second call: must return the cached session (same pointer).
	session2, err := p.Get(ctx, tenantID)
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if session2 != session1 {
		t.Error("expected second Get to return the same cached session")
	}
}

// TestPool_GetSession_Query verifies that a pooled session can execute queries.
func TestPool_GetSession_Query(t *testing.T) {
	testutil.CheckDuckDBExtensionsAvailable(t)
	pg := testutil.StartPostgres(t)
	minio := testutil.StartMinIO(t)
	catalogDB := testutil.NewCatalogDB(t, pg)

	tenantID := registerTestTenant(t, catalogDB, pg, minio, "poolquery@integration.test")

	poolCfg := config.PoolConfig{
		MaxIdleSessions:  1,
		SessionTTL:       5 * time.Minute,
		EvictionInterval: 1 * time.Minute,
	}
	p := pool.New(catalogDB, pg.Config, minio.Config, poolCfg)
	t.Cleanup(p.Close)

	ctx := context.Background()
	session, err := p.Get(ctx, tenantID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	rows, err := session.Conn.QueryContext(ctx, "SELECT 42 AS answer")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected one row")
	}
	var answer int
	if err := rows.Scan(&answer); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if answer != 42 {
		t.Errorf("expected 42, got %d", answer)
	}
}

// TestPool_Evict verifies that after eviction a new session can be created.
func TestPool_Evict(t *testing.T) {
	testutil.CheckDuckDBExtensionsAvailable(t)
	pg := testutil.StartPostgres(t)
	minio := testutil.StartMinIO(t)
	catalogDB := testutil.NewCatalogDB(t, pg)

	tenantID := registerTestTenant(t, catalogDB, pg, minio, "poolevict@integration.test")

	poolCfg := config.PoolConfig{
		MaxIdleSessions:  1,
		SessionTTL:       5 * time.Minute,
		EvictionInterval: 1 * time.Minute,
	}
	p := pool.New(catalogDB, pg.Config, minio.Config, poolCfg)
	t.Cleanup(p.Close)

	ctx := context.Background()

	session1, err := p.Get(ctx, tenantID)
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}

	p.Evict(tenantID)

	// After eviction, Get should create a new session (different pointer).
	session2, err := p.Get(ctx, tenantID)
	if err != nil {
		t.Fatalf("Get after Evict: %v", err)
	}
	if session2 == session1 {
		t.Error("expected a new session after eviction, got the same pointer")
	}
}

// TestPool_UnknownTenant verifies that requesting a session for a
// non-existent tenant returns an error.
func TestPool_UnknownTenant(t *testing.T) {
	pg := testutil.StartPostgres(t)
	minio := testutil.StartMinIO(t)
	catalogDB := testutil.NewCatalogDB(t, pg)

	poolCfg := config.PoolConfig{
		MaxIdleSessions:  1,
		SessionTTL:       5 * time.Minute,
		EvictionInterval: 1 * time.Minute,
	}
	p := pool.New(catalogDB, pg.Config, minio.Config, poolCfg)
	t.Cleanup(p.Close)

	ctx := context.Background()
	_, err := p.Get(ctx, "ghost_tenant_that_does_not_exist")
	if err == nil {
		t.Error("expected error for unknown tenant, got nil")
	}
}
