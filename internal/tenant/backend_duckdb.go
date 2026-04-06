package tenant

import (
	"context"

	"github.com/satheeshds/nexus/internal/config"
)

// plainDuckDBBackend implements the backend interface for the plain DuckDB
// storage backend. DuckDB sessions talk directly to MinIO via httpfs and
// require no Postgres catalog schema, so all catalog lifecycle operations are
// no-ops.
type plainDuckDBBackend struct{}

func (b *plainDuckDBBackend) createSchema(_ context.Context, _ string) error {
	return nil
}

func (b *plainDuckDBBackend) initCatalog(_ context.Context, _ string, _ config.MinIOConfig, _, _ string) error {
	return nil
}

func (b *plainDuckDBBackend) dropSchema(_ context.Context, _ string) error {
	return nil
}
