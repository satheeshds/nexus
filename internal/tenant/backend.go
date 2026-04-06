package tenant

import (
	"context"

	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
	duckdbconst "github.com/satheeshds/nexus/internal/duckdb"
)

// backend abstracts the catalog-layer operations that differ between storage
// backends. Each concrete implementation lives in its own file:
//   - backend_ducklake.go – Postgres-backed DuckLake catalog
//   - backend_duckdb.go   – plain DuckDB, no Postgres catalog
type backend interface {
	// createSchema creates any catalog metadata store required before MinIO
	// provisioning (e.g. a Postgres schema for DuckLake). Returns nil for
	// backends that need no pre-provisioning step.
	createSchema(ctx context.Context, pgSchema string) error

	// initCatalog runs one-time catalog initialisation after the MinIO service
	// account is ready. For DuckLake this opens a short-lived DuckDB session
	// to seed the catalog tables; plain DuckDB is a no-op.
	initCatalog(ctx context.Context, tenantID string, minioCfg config.MinIOConfig, s3Prefix, pgSchema string) error

	// dropSchema tears down the catalog metadata store on tenant deletion.
	// Returns nil for backends that have no such store.
	dropSchema(ctx context.Context, pgSchema string) error
}

// newBackend returns the correct backend implementation for the configured
// storage backend. It is called once in NewProvisioner.
func newBackend(storageCfg config.StorageConfig, db *catalog.DB, pgCfg config.PostgresConfig) backend {
	if storageCfg.Backend == duckdbconst.BackendDuckDB {
		return &plainDuckDBBackend{}
	}
	return &duckLakeBackend{db: db, pgCfg: pgCfg}
}
