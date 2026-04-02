//go:build integration

// Package testutil provides shared helpers for integration tests.
package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/pressly/goose/v3"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
	"github.com/satheeshds/nexus/migrations"
)

// PostgresContainer holds a running Postgres testcontainer and its connection config.
type PostgresContainer struct {
	Container *tcpostgres.PostgresContainer
	Config    config.PostgresConfig
}

// MinIOContainer holds a running MinIO testcontainer and its connection config.
type MinIOContainer struct {
	Container *tcminio.MinioContainer
	Config    config.MinIOConfig
}

// StartPostgres starts a Postgres container, runs migrations, and returns a
// PostgresContainer. The container is terminated when t.Cleanup runs.
func StartPostgres(t *testing.T) *PostgresContainer {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("testuser"),
		tcpostgres.WithPassword("testpass"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := pgContainer.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})

	host, err := pgContainer.Host(ctx)
	if err != nil {
		t.Fatalf("get postgres host: %v", err)
	}
	port, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get postgres port: %v", err)
	}

	pgCfg := config.PostgresConfig{
		Host:     host,
		Port:     port.Int(),
		User:     "testuser",
		Password: "testpass",
		DBName:   "testdb",
		SSLMode:  "disable",
	}

	// Run migrations
	db, err := sql.Open("pgx", pgCfg.URL())
	if err != nil {
		t.Fatalf("open db for migrations: %v", err)
	}
	defer db.Close()

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("set goose dialect: %v", err)
	}
	if err := goose.Up(db, "."); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	return &PostgresContainer{Container: pgContainer, Config: pgCfg}
}

// NewCatalogDB creates a catalog.DB connected to the given PostgresContainer.
func NewCatalogDB(t *testing.T, pg *PostgresContainer) *catalog.DB {
	t.Helper()
	ctx := context.Background()
	db, err := catalog.New(ctx, pg.Config)
	if err != nil {
		t.Fatalf("create catalog db: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

// StartMinIO starts a MinIO container and returns a MinIOContainer.
// The container is terminated when t.Cleanup runs.
func StartMinIO(t *testing.T) *MinIOContainer {
	t.Helper()
	ctx := context.Background()

	minioContainer, err := tcminio.Run(ctx,
		"minio/minio:latest",
		tcminio.WithUsername("minioadmin"),
		tcminio.WithPassword("minioadmin"),
	)
	if err != nil {
		t.Fatalf("start minio container: %v", err)
	}
	t.Cleanup(func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Logf("terminate minio container: %v", err)
		}
	})

	endpoint, err := minioContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("get minio endpoint: %v", err)
	}
	// Strip protocol prefix if present (testcontainers returns host:port)
	endpoint = stripScheme(endpoint)

	minioCfg := config.MinIOConfig{
		Endpoint:     endpoint,
		AccessKey:    "minioadmin",
		SecretKey:    "minioadmin",
		Bucket:       "testlakehouse",
		UseSSL:       false,
		UsePathStyle: true,
	}

	return &MinIOContainer{Container: minioContainer, Config: minioCfg}
}

// stripScheme removes a leading "http://" or "https://" from an endpoint string.
func stripScheme(s string) string {
	for _, prefix := range []string{"https://", "http://"} {
		if len(s) > len(prefix) && s[:len(prefix)] == prefix {
			return s[len(prefix):]
		}
	}
	return s
}

// UniqueID returns a unique string suffix useful for isolating test data.
func UniqueID(t *testing.T) string {
	return fmt.Sprintf("test_%s", sanitize(t.Name()))
}

// sanitize converts a test name into a safe identifier (lowercase, underscores).
func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for _, c := range []byte(s) {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			out = append(out, c)
		case c >= 'A' && c <= 'Z':
			out = append(out, c+32)
		default:
			out = append(out, '_')
		}
	}
	if len(out) > 40 {
		out = out[len(out)-40:]
	}
	return string(out)
}
