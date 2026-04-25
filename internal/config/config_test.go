package config_test

import (
	"os"
	"testing"

	"github.com/satheeshds/nexus/internal/config"
)

func TestPostgresConfig_DSN(t *testing.T) {
	cfg := config.PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "nexus",
		Password: "secret",
		DBName:   "lake_catalog",
		SSLMode:  "disable",
	}

	dsn := cfg.DSN()
	want := "host=localhost port=5432 user=nexus password=secret dbname=lake_catalog sslmode=disable"
	if dsn != want {
		t.Errorf("DSN() = %q, want %q", dsn, want)
	}
}

func TestPostgresConfig_URL(t *testing.T) {
	cfg := config.PostgresConfig{
		Host:     "db.example.com",
		Port:     5433,
		User:     "admin",
		Password: "pass",
		DBName:   "catalog",
		SSLMode:  "require",
	}

	url := cfg.URL()
	want := "postgres://admin:pass@db.example.com:5433/catalog?sslmode=require"
	if url != want {
		t.Errorf("URL() = %q, want %q", url, want)
	}
}

func TestLoad_Defaults(t *testing.T) {
	// Unset any environment overrides to test clean defaults
	unset := []string{
		"NEXUS_POSTGRES_HOST", "NEXUS_POSTGRES_PORT",
		"NEXUS_POSTGRES_USER", "NEXUS_POSTGRES_PASSWORD",
		"NEXUS_POSTGRES_DBNAME", "NEXUS_MINIO_ENDPOINT",
		"NEXUS_MINIO_ACCESS_KEY", "NEXUS_MINIO_SECRET_KEY",
		"NEXUS_MINIO_BUCKET", "NEXUS_AUTH_JWT_SECRET",
		"NEXUS_AUTH_ADMIN_API_KEY", "NEXUS_AUTH_SERVICE_ACCOUNT_ROTATION_TTL",
		"NEXUS_AUTH_SERVICE_ACCOUNT_KEY_ENCRYPTION_SECRET",
	}
	for _, k := range unset {
		prev, ok := os.LookupEnv(k)
		if err := os.Unsetenv(k); err != nil {
			t.Fatalf("Unsetenv(%q) unexpected error: %v", k, err)
		}
		t.Cleanup(func() {
			var err error
			if ok {
				err = os.Setenv(k, prev)
			} else {
				err = os.Unsetenv(k)
			}
			if err != nil {
				t.Errorf("restoring %q unexpected error: %v", k, err)
			}
		})
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	if cfg.Gateway.Port != 5433 {
		t.Errorf("Gateway.Port = %d, want 5433", cfg.Gateway.Port)
	}
	if cfg.Control.Port != 8080 {
		t.Errorf("Control.Port = %d, want 8080", cfg.Control.Port)
	}
	if cfg.Postgres.Host != "localhost" {
		t.Errorf("Postgres.Host = %q, want %q", cfg.Postgres.Host, "localhost")
	}
	if cfg.Postgres.Port != 5432 {
		t.Errorf("Postgres.Port = %d, want 5432", cfg.Postgres.Port)
	}
	if cfg.Postgres.User != "nexus" {
		t.Errorf("Postgres.User = %q, want %q", cfg.Postgres.User, "nexus")
	}
	if cfg.Postgres.DBName != "lake_catalog" {
		t.Errorf("Postgres.DBName = %q, want %q", cfg.Postgres.DBName, "lake_catalog")
	}
	if cfg.Postgres.SSLMode != "disable" {
		t.Errorf("Postgres.SSLMode = %q, want %q", cfg.Postgres.SSLMode, "disable")
	}
	if cfg.MinIO.Endpoint != "localhost:9000" {
		t.Errorf("MinIO.Endpoint = %q, want %q", cfg.MinIO.Endpoint, "localhost:9000")
	}
	if cfg.MinIO.Bucket != "lakehouse" {
		t.Errorf("MinIO.Bucket = %q, want %q", cfg.MinIO.Bucket, "lakehouse")
	}
	if cfg.Pool.MaxIdleSessions != 1 {
		t.Errorf("Pool.MaxIdleSessions = %d, want 1", cfg.Pool.MaxIdleSessions)
	}
	if cfg.Auth.ServiceAccountRotationTTL <= 0 {
		t.Errorf("Auth.ServiceAccountRotationTTL = %v, want > 0", cfg.Auth.ServiceAccountRotationTTL)
	}
	if cfg.Auth.ServiceAccountKeyEncryptionSecret != "" {
		t.Errorf("Auth.ServiceAccountKeyEncryptionSecret = %q, want empty by default", cfg.Auth.ServiceAccountKeyEncryptionSecret)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("NEXUS_POSTGRES_HOST", "pg.example.com")
	t.Setenv("NEXUS_POSTGRES_USER", "testuser")
	t.Setenv("NEXUS_MINIO_BUCKET", "my-bucket")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	if cfg.Postgres.Host != "pg.example.com" {
		t.Errorf("Postgres.Host = %q, want %q", cfg.Postgres.Host, "pg.example.com")
	}
	if cfg.Postgres.User != "testuser" {
		t.Errorf("Postgres.User = %q, want %q", cfg.Postgres.User, "testuser")
	}
	if cfg.MinIO.Bucket != "my-bucket" {
		t.Errorf("MinIO.Bucket = %q, want %q", cfg.MinIO.Bucket, "my-bucket")
	}
}
