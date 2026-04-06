package tenant

import (
	"context"

	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
	"github.com/satheeshds/nexus/internal/duckdb"
)

// duckLakeBackend implements the backend interface for the DuckLake storage
// backend. It creates and drops a Postgres schema that stores the DuckLake
// catalog metadata, and opens a short-lived DuckDB session to initialise the
// catalog tables.
type duckLakeBackend struct {
	db    *catalog.DB
	pgCfg config.PostgresConfig
}

func (b *duckLakeBackend) createSchema(ctx context.Context, pgSchema string) error {
	return b.db.CreateTenantSchema(ctx, pgSchema)
}

func (b *duckLakeBackend) initCatalog(ctx context.Context, tenantID string, minioCfg config.MinIOConfig, s3Prefix, pgSchema string) error {
	conn, err := duckdb.OpenForTenant(ctx, tenantID, b.pgCfg, minioCfg, s3Prefix, pgSchema)
	if err != nil {
		return err
	}
	defer conn.Close()
	// Verify the attachment is healthy.
	_, err = conn.ExecContext(ctx, "SELECT 1")
	return err
}

func (b *duckLakeBackend) dropSchema(ctx context.Context, pgSchema string) error {
	return b.db.DropTenantSchema(ctx, pgSchema)
}
