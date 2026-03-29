//go:build integration

package catalog_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
)

// newBenchDB opens a catalog.DB using environment variables (falls back to
// localhost defaults so the benchmarks work out-of-the-box against dev-infra).
// It calls b.Skip when Postgres is unreachable.
func newBenchDB(b *testing.B) (*catalog.DB, func()) {
	b.Helper()

	port := 5432
	if v := os.Getenv("NEXUS_POSTGRES_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			port = p
		}
	}

	cfg := config.PostgresConfig{
		Host:     getEnvOrDefault("NEXUS_POSTGRES_HOST", "localhost"),
		Port:     port,
		User:     getEnvOrDefault("NEXUS_POSTGRES_USER", "nexus"),
		Password: getEnvOrDefault("NEXUS_POSTGRES_PASSWORD", "changeme"),
		DBName:   getEnvOrDefault("NEXUS_POSTGRES_DBNAME", "lake_catalog"),
		SSLMode:  "disable",
	}

	ctx := context.Background()
	db, err := catalog.New(ctx, cfg)
	if err != nil {
		b.Skipf("postgres unavailable (%v) – set NEXUS_POSTGRES_* env vars or run make dev-infra", err)
	}
	return db, func() { db.Close() }
}

func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// benchTenant returns a Tenant with UUID-based unique fields.
func benchTenant() catalog.Tenant {
	id := uuid.New().String()
	return catalog.Tenant{
		ID:           id,
		OrgName:      "Bench Org",
		Email:        fmt.Sprintf("bench-%s@example.com", id),
		S3Prefix:     "tenants/" + id,
		PGSchema:     "bench_" + id[:8],
		PasswordHash: "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy",
		CreatedAt:    time.Now(),
	}
}

// BenchmarkCatalogInsertTenant measures the throughput of inserting a single
// tenant row. Test data is pre-allocated so only the INSERT is timed.
func BenchmarkCatalogInsertTenant(b *testing.B) {
	db, cleanup := newBenchDB(b)
	defer cleanup()
	ctx := context.Background()

	tenants := make([]catalog.Tenant, b.N)
	for i := range tenants {
		tenants[i] = benchTenant()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.InsertTenant(ctx, tenants[i]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	for _, t := range tenants {
		_ = db.DeleteTenant(ctx, t.ID)
	}
}

// BenchmarkCatalogGetTenant measures the throughput of retrieving a tenant by
// primary key. One tenant is inserted before the timed loop.
func BenchmarkCatalogGetTenant(b *testing.B) {
	db, cleanup := newBenchDB(b)
	defer cleanup()
	ctx := context.Background()

	t := benchTenant()
	if err := db.InsertTenant(ctx, t); err != nil {
		b.Fatalf("setup: insert tenant: %v", err)
	}
	defer func() { _ = db.DeleteTenant(ctx, t.ID) }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.GetTenant(ctx, t.ID); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCatalogGetTenantByEmail measures email-indexed lookups.
func BenchmarkCatalogGetTenantByEmail(b *testing.B) {
	db, cleanup := newBenchDB(b)
	defer cleanup()
	ctx := context.Background()

	t := benchTenant()
	if err := db.InsertTenant(ctx, t); err != nil {
		b.Fatalf("setup: insert tenant: %v", err)
	}
	defer func() { _ = db.DeleteTenant(ctx, t.ID) }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.GetTenantByEmail(ctx, t.Email); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCatalogListTenants measures the cost of a full tenant table scan.
// 50 tenants are seeded before the timed loop to give a representative result.
func BenchmarkCatalogListTenants(b *testing.B) {
	db, cleanup := newBenchDB(b)
	defer cleanup()
	ctx := context.Background()

	const seed = 50
	ids := make([]string, seed)
	for i := 0; i < seed; i++ {
		t := benchTenant()
		if err := db.InsertTenant(ctx, t); err != nil {
			b.Fatalf("setup: insert tenant %d: %v", i, err)
		}
		ids[i] = t.ID
	}
	defer func() {
		for _, id := range ids {
			_ = db.DeleteTenant(ctx, id)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.ListTenants(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCatalogDeleteTenant measures single-row DELETE performance.
// One tenant per iteration is inserted during setup, then deleted during the
// timed loop.
func BenchmarkCatalogDeleteTenant(b *testing.B) {
	db, cleanup := newBenchDB(b)
	defer cleanup()
	ctx := context.Background()

	tenants := make([]catalog.Tenant, b.N)
	for i := range tenants {
		tenants[i] = benchTenant()
		if err := db.InsertTenant(ctx, tenants[i]); err != nil {
			b.Fatalf("setup: insert tenant %d: %v", i, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.DeleteTenant(ctx, tenants[i].ID); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCatalogInsertServiceAccount measures service account insertion
// throughput. A parent tenant is pre-inserted.
func BenchmarkCatalogInsertServiceAccount(b *testing.B) {
	db, cleanup := newBenchDB(b)
	defer cleanup()
	ctx := context.Background()

	tenant := benchTenant()
	if err := db.InsertTenant(ctx, tenant); err != nil {
		b.Fatalf("setup: insert tenant: %v", err)
	}
	defer func() { _ = db.DeleteTenant(ctx, tenant.ID) }()

	sas := make([]catalog.ServiceAccount, b.N)
	for i := range sas {
		id := uuid.New().String()
		sas[i] = catalog.ServiceAccount{
			ID:             id,
			TenantID:       tenant.ID,
			S3Prefix:       "tenants/" + tenant.ID,
			PGSchema:       "bench_" + tenant.ID[:8],
			MinioAccessKey: "key-" + id,
			APIKeyHash:     "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy",
			CreatedAt:      time.Now(),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.InsertServiceAccount(ctx, sas[i]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	// Cascade delete removes service accounts via the FK.
}

// BenchmarkCatalogGetServiceAccount measures retrieval of a service account
// by tenant ID.
func BenchmarkCatalogGetServiceAccount(b *testing.B) {
	db, cleanup := newBenchDB(b)
	defer cleanup()
	ctx := context.Background()

	tenant := benchTenant()
	if err := db.InsertTenant(ctx, tenant); err != nil {
		b.Fatalf("setup: insert tenant: %v", err)
	}
	defer func() { _ = db.DeleteTenant(ctx, tenant.ID) }()

	saID := uuid.New().String()
	sa := catalog.ServiceAccount{
		ID:             saID,
		TenantID:       tenant.ID,
		S3Prefix:       "tenants/" + tenant.ID,
		PGSchema:       "bench_" + tenant.ID[:8],
		MinioAccessKey: "key-" + saID,
		APIKeyHash:     "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy",
		CreatedAt:      time.Now(),
	}
	if err := db.InsertServiceAccount(ctx, sa); err != nil {
		b.Fatalf("setup: insert service account: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.GetServiceAccountByTenantID(ctx, tenant.ID); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCatalogGetTenantParallel measures the throughput of concurrent
// reads against a single tenant record.
func BenchmarkCatalogGetTenantParallel(b *testing.B) {
	db, cleanup := newBenchDB(b)
	defer cleanup()
	ctx := context.Background()

	t := benchTenant()
	if err := db.InsertTenant(ctx, t); err != nil {
		b.Fatalf("setup: insert tenant: %v", err)
	}
	defer func() { _ = db.DeleteTenant(ctx, t.ID) }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := db.GetTenant(ctx, t.ID); err != nil {
				b.Error(err)
			}
		}
	})
}
