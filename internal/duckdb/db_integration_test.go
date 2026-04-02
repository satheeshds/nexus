//go:build integration

package duckdb_test

import (
	"context"
	"testing"

	"github.com/satheeshds/nexus/internal/duckdb"
	"github.com/satheeshds/nexus/internal/storage"
	"github.com/satheeshds/nexus/internal/testutil"
)

// TestDuckDB_OpenForTenant verifies that OpenForTenant successfully creates a
// DuckDB session, attaches a DuckLake catalog, and can execute queries.
func TestDuckDB_OpenForTenant(t *testing.T) {
	testutil.CheckDuckDBExtensionsAvailable(t)

	pg := testutil.StartPostgres(t)
	minio := testutil.StartMinIO(t)
	ctx := context.Background()

	// Ensure the test bucket exists.
	storageClient, err := storage.New(minio.Config)
	if err != nil {
		t.Fatalf("create storage client: %v", err)
	}
	if err := storageClient.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}

	// Create the PG schema that DuckLake will use.
	pgCfg := pg.Config
	minioCfg := minio.Config
	tenantID := "duckdb_test_tenant"
	s3Prefix := "tenants/duckdb_test_tenant"
	pgSchema := "ducklake_duckdb_test"

	// Create the schema in Postgres before attaching.
	catalogDB := testutil.NewCatalogDB(t, pg)
	if err := catalogDB.CreateTenantSchema(ctx, pgSchema); err != nil {
		t.Fatalf("create tenant schema: %v", err)
	}

	conn, err := duckdb.OpenForTenant(ctx, tenantID, pgCfg, minioCfg, s3Prefix, pgSchema)
	if err != nil {
		t.Fatalf("OpenForTenant: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("close duckdb conn: %v", err)
		}
	})

	t.Run("QueryContext_Select1", func(t *testing.T) {
		rows, err := conn.QueryContext(ctx, "SELECT 1 AS n")
		if err != nil {
			t.Fatalf("QueryContext: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Fatal("expected one row")
		}
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if n != 1 {
			t.Errorf("expected 1, got %d", n)
		}
	})

	t.Run("ExecContext_CreateTable", func(t *testing.T) {
		_, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS integration_test_tbl (id INTEGER, name VARCHAR)`)
		if err != nil {
			t.Fatalf("CREATE TABLE: %v", err)
		}
	})

	t.Run("ExecContext_Insert", func(t *testing.T) {
		_, err := conn.ExecContext(ctx, `INSERT INTO integration_test_tbl VALUES (1, 'hello')`)
		if err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	})

	t.Run("QueryContext_Select_From_Table", func(t *testing.T) {
		rows, err := conn.QueryContext(ctx, `SELECT id, name FROM integration_test_tbl WHERE id = 1`)
		if err != nil {
			t.Fatalf("SELECT: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Fatal("expected one row")
		}
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if id != 1 || name != "hello" {
			t.Errorf("unexpected row: id=%d name=%q", id, name)
		}
	})

	t.Run("LakeName", func(t *testing.T) {
		if conn.LakeName() != "lake" {
			t.Errorf("LakeName: want %q got %q", "lake", conn.LakeName())
		}
	})
}

// TestDuckDB_MultipleTenantsIsolation verifies that two tenants opened
// separately maintain independent DuckDB sessions (no shared state).
func TestDuckDB_MultipleTenantsIsolation(t *testing.T) {
	testutil.CheckDuckDBExtensionsAvailable(t)

	pg := testutil.StartPostgres(t)
	minio := testutil.StartMinIO(t)
	ctx := context.Background()

	storageClient, err := storage.New(minio.Config)
	if err != nil {
		t.Fatalf("create storage client: %v", err)
	}
	if err := storageClient.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}

	catalogDB := testutil.NewCatalogDB(t, pg)

	openTenant := func(tenantID, pgSchema, s3Prefix string) *duckdb.Conn {
		t.Helper()
		if err := catalogDB.CreateTenantSchema(ctx, pgSchema); err != nil {
			t.Fatalf("create schema for %q: %v", tenantID, err)
		}
		conn, err := duckdb.OpenForTenant(ctx, tenantID, pg.Config, minio.Config, s3Prefix, pgSchema)
		if err != nil {
			t.Fatalf("OpenForTenant %q: %v", tenantID, err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}

	connA := openTenant("tenant_a", "ducklake_tenant_a", "tenants/tenant_a")
	connB := openTenant("tenant_b", "ducklake_tenant_b", "tenants/tenant_b")

	// Create a table in tenant A's session.
	if _, err := connA.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS only_in_a (x INTEGER)`); err != nil {
		t.Fatalf("create table in A: %v", err)
	}

	// Tenant B's session should not see tenant A's table.
	rows, err := connB.QueryContext(ctx, `SHOW TABLES`)
	if err != nil {
		t.Fatalf("SHOW TABLES in B: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if tableName == "only_in_a" {
			t.Errorf("tenant B should not see tenant A's table %q", tableName)
		}
	}
}
