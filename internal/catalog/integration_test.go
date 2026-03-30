//go:build integration

package catalog_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
	"github.com/satheeshds/nexus/migrations"
)

// testDB is the shared catalog.DB used across all tests in this package.
var testDB *catalog.DB

// TestMain sets up migrations and a shared DB connection before running all tests.
func TestMain(m *testing.M) {
	cfg := testPostgresConfig()

	// Apply all migrations so the schema is ready.
	if err := applyMigrations(cfg.URL()); err != nil {
		fmt.Fprintf(os.Stderr, "integration test setup: apply migrations: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	db, err := catalog.New(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration test setup: connect to postgres: %v\n", err)
		os.Exit(1)
	}
	testDB = db

	code := m.Run()

	db.Close()
	os.Exit(code)
}

// testPostgresConfig builds a PostgresConfig from environment variables,
// falling back to sensible defaults that match docker-compose.test.yml.
func testPostgresConfig() config.PostgresConfig {
	port := 5433
	if v := os.Getenv("TEST_POSTGRES_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			port = p
		}
	}
	return config.PostgresConfig{
		Host:     envOrDefault("TEST_POSTGRES_HOST", "localhost"),
		Port:     port,
		User:     envOrDefault("TEST_POSTGRES_USER", "nexus_test"),
		Password: envOrDefault("TEST_POSTGRES_PASSWORD", "testpassword"),
		DBName:   envOrDefault("TEST_POSTGRES_DBNAME", "lake_catalog_test"),
		SSLMode:  "disable",
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// applyMigrations runs all goose migrations against the given DSN.
func applyMigrations(dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set dialect: %w", err)
	}
	if err := goose.Up(db, "."); err != nil {
		return fmt.Errorf("goose up: %w", err)
	}
	return nil
}

// uniqueID generates a unique ID with the given prefix, safe for use as
// a tenant ID, schema name, or email address in tests.
func uniqueID(prefix string) string {
	return prefix + "_" + uuid.New().String()[:8]
}

// newTestTenant returns a Tenant with fields populated from the given unique ID.
func newTestTenant(id string) catalog.Tenant {
	return catalog.Tenant{
		ID:           id,
		OrgName:      "Test Org " + id,
		Email:        id + "@example.com",
		S3Prefix:     "tenants/" + id,
		PGSchema:     "ducklake_" + id,
		PasswordHash: "$2y$10$fakehashfortesting",
		CreatedAt:    time.Now().UTC().Truncate(time.Microsecond),
	}
}

// newTestServiceAccount returns a ServiceAccount linked to the given tenant.
func newTestServiceAccount(id, tenantID string) catalog.ServiceAccount {
	return catalog.ServiceAccount{
		ID:             id,
		TenantID:       tenantID,
		S3Prefix:       "tenants/" + tenantID,
		PGSchema:       "ducklake_" + tenantID,
		MinioAccessKey: "access_" + id,
		APIKeyHash:     "$2y$10$fakehashfortesting",
		CreatedAt:      time.Now().UTC().Truncate(time.Microsecond),
	}
}

// TestNew verifies that catalog.New successfully connects to PostgreSQL.
func TestNew(t *testing.T) {
	cfg := testPostgresConfig()
	ctx := context.Background()

	db, err := catalog.New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	db.Close()
}

// TestCreateDropTenantSchema verifies that schema creation and deletion are idempotent.
func TestCreateDropTenantSchema(t *testing.T) {
	ctx := context.Background()
	schema := uniqueID("test_schema")

	if err := testDB.CreateTenantSchema(ctx, schema); err != nil {
		t.Fatalf("CreateTenantSchema: %v", err)
	}
	// Creating the same schema a second time must not fail (IF NOT EXISTS).
	if err := testDB.CreateTenantSchema(ctx, schema); err != nil {
		t.Errorf("CreateTenantSchema (idempotent): %v", err)
	}

	if err := testDB.DropTenantSchema(ctx, schema); err != nil {
		t.Fatalf("DropTenantSchema: %v", err)
	}
	// Dropping a non-existent schema must not fail (IF EXISTS).
	if err := testDB.DropTenantSchema(ctx, schema); err != nil {
		t.Errorf("DropTenantSchema (idempotent): %v", err)
	}
}

// TestInsertGetTenant verifies that a tenant can be inserted and retrieved by ID.
func TestInsertGetTenant(t *testing.T) {
	ctx := context.Background()
	id := uniqueID("tenant")
	want := newTestTenant(id)

	if err := testDB.InsertTenant(ctx, want); err != nil {
		t.Fatalf("InsertTenant: %v", err)
	}
	t.Cleanup(func() { _ = testDB.DeleteTenant(ctx, id) })

	got, err := testDB.GetTenant(ctx, id)
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}

	if got.ID != want.ID {
		t.Errorf("ID: got %q, want %q", got.ID, want.ID)
	}
	if got.OrgName != want.OrgName {
		t.Errorf("OrgName: got %q, want %q", got.OrgName, want.OrgName)
	}
	if got.Email != want.Email {
		t.Errorf("Email: got %q, want %q", got.Email, want.Email)
	}
	if got.S3Prefix != want.S3Prefix {
		t.Errorf("S3Prefix: got %q, want %q", got.S3Prefix, want.S3Prefix)
	}
	if got.PGSchema != want.PGSchema {
		t.Errorf("PGSchema: got %q, want %q", got.PGSchema, want.PGSchema)
	}
	if got.PasswordHash != want.PasswordHash {
		t.Errorf("PasswordHash: got %q, want %q", got.PasswordHash, want.PasswordHash)
	}
}

// TestGetTenantByEmail verifies that a tenant can be retrieved by email address.
func TestGetTenantByEmail(t *testing.T) {
	ctx := context.Background()
	id := uniqueID("tenant")
	want := newTestTenant(id)

	if err := testDB.InsertTenant(ctx, want); err != nil {
		t.Fatalf("InsertTenant: %v", err)
	}
	t.Cleanup(func() { _ = testDB.DeleteTenant(ctx, id) })

	got, err := testDB.GetTenantByEmail(ctx, want.Email)
	if err != nil {
		t.Fatalf("GetTenantByEmail: %v", err)
	}
	if got.ID != want.ID {
		t.Errorf("ID: got %q, want %q", got.ID, want.ID)
	}
	if got.Email != want.Email {
		t.Errorf("Email: got %q, want %q", got.Email, want.Email)
	}
}

// TestDeleteTenant verifies that a deleted tenant can no longer be retrieved.
func TestDeleteTenant(t *testing.T) {
	ctx := context.Background()
	id := uniqueID("tenant")

	if err := testDB.InsertTenant(ctx, newTestTenant(id)); err != nil {
		t.Fatalf("InsertTenant: %v", err)
	}
	if err := testDB.DeleteTenant(ctx, id); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}

	_, err := testDB.GetTenant(ctx, id)
	if err == nil {
		t.Fatal("GetTenant after delete: expected error, got nil")
	}
}

// TestListTenants verifies that inserted tenants appear in the result of ListTenants.
func TestListTenants(t *testing.T) {
	ctx := context.Background()
	id1 := uniqueID("list_tenant")
	id2 := uniqueID("list_tenant")

	for _, id := range []string{id1, id2} {
		if err := testDB.InsertTenant(ctx, newTestTenant(id)); err != nil {
			t.Fatalf("InsertTenant(%s): %v", id, err)
		}
	}
	t.Cleanup(func() {
		_ = testDB.DeleteTenant(ctx, id1)
		_ = testDB.DeleteTenant(ctx, id2)
	})

	tenants, err := testDB.ListTenants(ctx)
	if err != nil {
		t.Fatalf("ListTenants: %v", err)
	}

	found := make(map[string]bool, len(tenants))
	for _, tenant := range tenants {
		found[tenant.ID] = true
	}
	for _, id := range []string{id1, id2} {
		if !found[id] {
			t.Errorf("ListTenants: tenant %q not found in results", id)
		}
	}
}

// TestInsertGetServiceAccount verifies round-trip insert and retrieval of a service account.
func TestInsertGetServiceAccount(t *testing.T) {
	ctx := context.Background()
	tenantID := uniqueID("tenant")

	if err := testDB.InsertTenant(ctx, newTestTenant(tenantID)); err != nil {
		t.Fatalf("InsertTenant: %v", err)
	}
	// Deleting the tenant cascades to the service account, so one cleanup is enough.
	t.Cleanup(func() { _ = testDB.DeleteTenant(ctx, tenantID) })

	saID := tenantID + "_svc"
	want := newTestServiceAccount(saID, tenantID)

	if err := testDB.InsertServiceAccount(ctx, want); err != nil {
		t.Fatalf("InsertServiceAccount: %v", err)
	}

	got, err := testDB.GetServiceAccountByTenantID(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetServiceAccountByTenantID: %v", err)
	}

	if got.ID != want.ID {
		t.Errorf("ID: got %q, want %q", got.ID, want.ID)
	}
	if got.TenantID != want.TenantID {
		t.Errorf("TenantID: got %q, want %q", got.TenantID, want.TenantID)
	}
	if got.S3Prefix != want.S3Prefix {
		t.Errorf("S3Prefix: got %q, want %q", got.S3Prefix, want.S3Prefix)
	}
	if got.PGSchema != want.PGSchema {
		t.Errorf("PGSchema: got %q, want %q", got.PGSchema, want.PGSchema)
	}
	if got.MinioAccessKey != want.MinioAccessKey {
		t.Errorf("MinioAccessKey: got %q, want %q", got.MinioAccessKey, want.MinioAccessKey)
	}
	if got.APIKeyHash != want.APIKeyHash {
		t.Errorf("APIKeyHash: got %q, want %q", got.APIKeyHash, want.APIKeyHash)
	}
}

// TestDeleteServiceAccountByTenantID verifies that explicit SA deletion removes it.
func TestDeleteServiceAccountByTenantID(t *testing.T) {
	ctx := context.Background()
	tenantID := uniqueID("tenant")

	if err := testDB.InsertTenant(ctx, newTestTenant(tenantID)); err != nil {
		t.Fatalf("InsertTenant: %v", err)
	}
	t.Cleanup(func() { _ = testDB.DeleteTenant(ctx, tenantID) })

	saID := tenantID + "_svc"
	if err := testDB.InsertServiceAccount(ctx, newTestServiceAccount(saID, tenantID)); err != nil {
		t.Fatalf("InsertServiceAccount: %v", err)
	}

	if err := testDB.DeleteServiceAccountByTenantID(ctx, tenantID); err != nil {
		t.Fatalf("DeleteServiceAccountByTenantID: %v", err)
	}

	_, err := testDB.GetServiceAccountByTenantID(ctx, tenantID)
	if err == nil {
		t.Fatal("GetServiceAccountByTenantID after delete: expected error, got nil")
	}
}

// TestServiceAccountCascadeDelete verifies that deleting a tenant also removes its service account.
func TestServiceAccountCascadeDelete(t *testing.T) {
	ctx := context.Background()
	tenantID := uniqueID("tenant")

	if err := testDB.InsertTenant(ctx, newTestTenant(tenantID)); err != nil {
		t.Fatalf("InsertTenant: %v", err)
	}

	saID := tenantID + "_svc"
	if err := testDB.InsertServiceAccount(ctx, newTestServiceAccount(saID, tenantID)); err != nil {
		_ = testDB.DeleteTenant(ctx, tenantID)
		t.Fatalf("InsertServiceAccount: %v", err)
	}

	// Deleting the tenant should cascade to the service account (ON DELETE CASCADE).
	if err := testDB.DeleteTenant(ctx, tenantID); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}

	_, err := testDB.GetServiceAccountByTenantID(ctx, tenantID)
	if err == nil {
		t.Fatal("GetServiceAccountByTenantID after tenant cascade delete: expected error, got nil")
	}
}

// TestGetTenantNotFound verifies that querying a non-existent tenant returns an error.
func TestGetTenantNotFound(t *testing.T) {
	ctx := context.Background()

	_, err := testDB.GetTenant(ctx, "nonexistent_tenant_id_xyz_integration")
	if err == nil {
		t.Fatal("GetTenant(nonexistent): expected error, got nil")
	}
}

// TestInsertDuplicateEmail verifies that the unique constraint on email is enforced.
func TestInsertDuplicateEmail(t *testing.T) {
	ctx := context.Background()
	id1 := uniqueID("tenant")
	id2 := uniqueID("tenant")

	t1 := newTestTenant(id1)
	t2 := newTestTenant(id2)
	t2.Email = t1.Email // force a duplicate email

	if err := testDB.InsertTenant(ctx, t1); err != nil {
		t.Fatalf("InsertTenant(t1): %v", err)
	}
	t.Cleanup(func() { _ = testDB.DeleteTenant(ctx, id1) })

	err := testDB.InsertTenant(ctx, t2)
	if err == nil {
		_ = testDB.DeleteTenant(ctx, id2)
		t.Fatal("InsertTenant(duplicate email): expected error, got nil")
	}
}
