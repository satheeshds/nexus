package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

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
	slog.Debug("opening DuckDB session",
		"tenant", tenantID,
		"s3_prefix", s3Prefix,
		"pg_schema", pgSchema,
		"minio_endpoint", minioCfg.Endpoint,
		"minio_bucket", minioCfg.Bucket,
	)

	// Open an in-memory DuckDB instance (one per tenant session)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	stmts := []string{
		// Install and load required extensions
		"INSTALL ducklake; LOAD ducklake;",
		"INSTALL postgres; LOAD postgres;",
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
		fmt.Sprintf(`ATTACH 'postgres:%s' AS lake (
			TYPE            DUCKLAKE,
			METADATA_SCHEMA '%s',
			DATA_PATH       's3://%s/%s/'
		);`,
			pgCfg.DSN(),
			pgSchema,
			minioCfg.Bucket,
			s3Prefix,
		),

		// Set the default schema to 'lake' so tables are created in DuckLake by default
		// This ensures tables persist to S3 without requiring explicit lake. prefix
		"SET search_path = 'lake';",
	}

	for i, stmt := range stmts {
		slog.Debug("executing DuckDB init statement",
			"tenant", tenantID,
			"step", i+1,
		)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			slog.Error("failed to execute DuckDB init statement",
				"tenant", tenantID,
				"step", i+1,
				"err", err,
			)
			return nil, fmt.Errorf("init duckdb step %d: %w", i+1, err)
		}
	}

	slog.Info("DuckDB session created successfully",
		"tenant", tenantID,
		"s3_prefix", s3Prefix,
		"data_path", fmt.Sprintf("s3://%s/%s/", minioCfg.Bucket, s3Prefix),
	)

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
