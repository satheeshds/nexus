package duckdb

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/satheeshds/nexus/internal/config"
)

// Conn wraps a DuckDB *sql.DB with DuckLake attached for a specific tenant.
type Conn struct {
	db       *sql.DB
	tenantID string
	lakeName string // the ATTACH alias used in SQL, e.g. "lake"
}

// OpenForTenant creates a new in-memory DuckDB instance, installs extensions,
// configures S3 credentials, and ATTACHes the tenant's DuckLake catalog.
func OpenForTenant(
	ctx context.Context,
	tenantID string,
	pgCfg config.PostgresConfig,
	minioCfg config.MinIOConfig,
	s3Prefix string,
	pgSchema string,
) (*Conn, error) {
	// Open an in-memory DuckDB instance (one per tenant session)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	stmts := []string{
		// Install and load required extensions
		"INSTALL ducklake; LOAD ducklake;",
		"INSTALL httpfs;  LOAD httpfs;",

		// Configure S3/MinIO endpoint globally for this session
		fmt.Sprintf(`CREATE OR REPLACE SECRET minio_secret (
			TYPE        S3,
			KEY_ID      '%s',
			SECRET      '%s',
			ENDPOINT    '%s',
			URL_STYLE   'path',
			USE_SSL     false,
			REGION      'us-east-1'
		);`, minioCfg.AccessKey, minioCfg.SecretKey, minioCfg.Endpoint),

		// ATTACH the tenant's DuckLake catalog
		// This creates DuckLake metadata tables in pgSchema if they don't exist yet.
		fmt.Sprintf(`ATTACH 'ducklake:%s' AS lake (
			TYPE      DUCKLAKE,
			SCHEMA    '%s',
			DATA_PATH 's3://%s/%s/'
		);`,
			pgCfg.URL(),
			pgSchema,
			minioCfg.Bucket,
			s3Prefix,
		),
	}

	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("init duckdb [%s]: %w", stmt[:min(40, len(stmt))], err)
		}
	}

	return &Conn{db: db, tenantID: tenantID, lakeName: "lake"}, nil
}

// QueryContext executes a SQL query and returns rows.
func (c *Conn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query, args...)
}

// ExecContext executes a SQL statement.
func (c *Conn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}

// LakeName returns the ATTACH alias (always "lake") for use in SQL: lake.tablename
func (c *Conn) LakeName() string {
	return c.lakeName
}

// Close closes the DuckDB connection.
func (c *Conn) Close() error {
	return c.db.Close()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
