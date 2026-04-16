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

// newTestDB creates a catalog.DB connected to a real PostgreSQL instance.
// It skips the benchmark when NEXUS_POSTGRES_HOST is not set.
func newTestDB(tb testing.TB) *catalog.DB {
	tb.Helper()
	host := os.Getenv("NEXUS_POSTGRES_HOST")
	if host == "" {
		tb.Skip("NEXUS_POSTGRES_HOST not set; skipping integration benchmark")
	}
	port := 5432
	if p := os.Getenv("NEXUS_POSTGRES_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			port = v
		}
	}
	cfg := config.PostgresConfig{
		Host:     host,
		Port:     port,
		User:     envOrDefault("NEXUS_POSTGRES_USER", "nexus"),
		Password: envOrDefault("NEXUS_POSTGRES_PASSWORD", "changeme"),
		DBName:   envOrDefault("NEXUS_POSTGRES_DBNAME", "lake_catalog"),
		SSLMode:  "disable",
	}
	db, err := catalog.New(context.Background(), cfg)
	if err != nil {
		tb.Fatalf("connect to postgres: %v", err)
	}
	tb.Cleanup(db.Close)
	return db
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// BenchmarkInsertTenant measures the throughput of tenant inserts.
func BenchmarkInsertTenant(b *testing.B) {
	db := newTestDB(b)
	ctx := context.Background()

	// Pre-generate IDs so allocation does not skew the benchmark.
	ids := make([]string, b.N)
	for i := range ids {
		ids[i] = uuid.New().String()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := catalog.Tenant{
			ID:           ids[i],
			OrgName:      fmt.Sprintf("bench-org-%d", i),
			Email:        fmt.Sprintf("bench-%d@load.test", i),
			S3Prefix:     fmt.Sprintf("tenants/bench_%d", i),
			PGSchema:     fmt.Sprintf("ducklake_bench_%s", ids[i]),
			PasswordHash: "$2a$10$benchplaceholderpasswordhash",
			CreatedAt:    time.Now(),
		}
		if err := db.InsertTenant(ctx, t); err != nil {
			b.Fatalf("InsertTenant: %v", err)
		}
	}
	b.StopTimer()

	// Cleanup: remove all bench tenants inserted.
	for _, id := range ids {
		_ = db.DeleteTenant(ctx, id)
	}
}

// BenchmarkGetTenant measures the throughput of single-tenant lookups by ID.
func BenchmarkGetTenant(b *testing.B) {
	db := newTestDB(b)
	ctx := context.Background()

	// Setup: insert a tenant to read.
	id := uuid.New().String()
	seed := catalog.Tenant{
		ID:           id,
		OrgName:      "bench-org-read",
		Email:        "bench-read@load.test",
		S3Prefix:     "tenants/bench_read",
		PGSchema:     fmt.Sprintf("ducklake_bench_%s", id),
		PasswordHash: "$2a$10$benchplaceholderpasswordhash",
		CreatedAt:    time.Now(),
	}
	if err := db.InsertTenant(ctx, seed); err != nil {
		b.Fatalf("setup InsertTenant: %v", err)
	}
	b.Cleanup(func() { _ = db.DeleteTenant(ctx, id) })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.GetTenant(ctx, id); err != nil {
			b.Fatalf("GetTenant: %v", err)
		}
	}
}

// BenchmarkGetTenantByEmail measures tenant lookups by email (unique index scan).
func BenchmarkGetTenantByEmail(b *testing.B) {
	db := newTestDB(b)
	ctx := context.Background()

	id := uuid.New().String()
	email := fmt.Sprintf("bench-email-%s@load.test", id)
	seed := catalog.Tenant{
		ID:           id,
		OrgName:      "bench-org-email",
		Email:        email,
		S3Prefix:     "tenants/bench_email",
		PGSchema:     fmt.Sprintf("ducklake_bench_%s", id),
		PasswordHash: "$2a$10$benchplaceholderpasswordhash",
		CreatedAt:    time.Now(),
	}
	if err := db.InsertTenant(ctx, seed); err != nil {
		b.Fatalf("setup InsertTenant: %v", err)
	}
	b.Cleanup(func() { _ = db.DeleteTenant(ctx, id) })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.GetTenantByEmail(ctx, email); err != nil {
			b.Fatalf("GetTenantByEmail: %v", err)
		}
	}
}

// BenchmarkListTenants measures the cost of a full tenant table scan.
func BenchmarkListTenants(b *testing.B) {
	db := newTestDB(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.ListTenants(ctx); err != nil {
			b.Fatalf("ListTenants: %v", err)
		}
	}
}

// BenchmarkInsertServiceAccount measures service account insert throughput.
func BenchmarkInsertServiceAccount(b *testing.B) {
	db := newTestDB(b)
	ctx := context.Background()

	// Insert a parent tenant required by the FK constraint.
	tenantID := uuid.New().String()
	tenant := catalog.Tenant{
		ID:           tenantID,
		OrgName:      "bench-org-sa",
		Email:        fmt.Sprintf("bench-sa-%s@load.test", tenantID),
		S3Prefix:     fmt.Sprintf("tenants/bench_sa_%s", tenantID),
		PGSchema:     fmt.Sprintf("ducklake_bench_%s", tenantID),
		PasswordHash: "$2a$10$benchplaceholderpasswordhash",
		CreatedAt:    time.Now(),
	}
	if err := db.InsertTenant(ctx, tenant); err != nil {
		b.Fatalf("setup InsertTenant: %v", err)
	}
	b.Cleanup(func() { _ = db.DeleteTenant(ctx, tenantID) })

	// Pre-generate IDs to avoid allocation noise.
	ids := make([]string, b.N)
	for i := range ids {
		ids[i] = uuid.New().String()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sa := catalog.ServiceAccount{
			ID:             ids[i],
			TenantID:       tenantID,
			S3Prefix:       tenant.S3Prefix,
			PGSchema:       tenant.PGSchema,
			MinioAccessKey: fmt.Sprintf("access-%d", i),
			MinioSecretKey: fmt.Sprintf("secret-%d", i),
			APIKeyHash:     "$2a$10$benchplaceholderkeyhash",
			CreatedAt:      time.Now(),
		}
		if err := db.InsertServiceAccount(ctx, sa); err != nil {
			b.Fatalf("InsertServiceAccount: %v", err)
		}
		// Remove immediately so the UNIQUE index on tenant_id doesn't block the next iteration.
		_ = db.DeleteServiceAccountByTenantID(ctx, tenantID)
	}
}

// BenchmarkGetServiceAccountByTenantID measures service account lookup throughput.
func BenchmarkGetServiceAccountByTenantID(b *testing.B) {
	db := newTestDB(b)
	ctx := context.Background()

	// Setup parent tenant and service account.
	tenantID := uuid.New().String()
	tenant := catalog.Tenant{
		ID:           tenantID,
		OrgName:      "bench-org-sa-read",
		Email:        fmt.Sprintf("bench-sa-read-%s@load.test", tenantID),
		S3Prefix:     fmt.Sprintf("tenants/bench_sa_read_%s", tenantID),
		PGSchema:     fmt.Sprintf("ducklake_bench_%s", tenantID),
		PasswordHash: "$2a$10$benchplaceholderpasswordhash",
		CreatedAt:    time.Now(),
	}
	if err := db.InsertTenant(ctx, tenant); err != nil {
		b.Fatalf("setup InsertTenant: %v", err)
	}
	b.Cleanup(func() { _ = db.DeleteTenant(ctx, tenantID) })

	saID := uuid.New().String()
	sa := catalog.ServiceAccount{
		ID:             saID,
		TenantID:       tenantID,
		S3Prefix:       tenant.S3Prefix,
		PGSchema:       tenant.PGSchema,
		MinioAccessKey: "bench-access",
		MinioSecretKey: "bench-secret",
		APIKeyHash:     "$2a$10$benchplaceholderkeyhash",
		CreatedAt:      time.Now(),
	}
	if err := db.InsertServiceAccount(ctx, sa); err != nil {
		b.Fatalf("setup InsertServiceAccount: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.GetServiceAccountByTenantID(ctx, tenantID); err != nil {
			b.Fatalf("GetServiceAccountByTenantID: %v", err)
		}
	}
}
