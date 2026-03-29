package config_test

import (
	"testing"

	"github.com/satheeshds/nexus/internal/config"
)

func TestPostgresConfig_DSN(t *testing.T) {
	cfg := config.PostgresConfig{
		Host:     "db-host",
		Port:     5432,
		User:     "nexus",
		Password: "secret",
		DBName:   "catalog",
		SSLMode:  "disable",
	}

	got := cfg.DSN()
	want := "host=db-host port=5432 user=nexus password=secret dbname=catalog sslmode=disable"
	if got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}

func TestPostgresConfig_URL(t *testing.T) {
	cfg := config.PostgresConfig{
		Host:     "db-host",
		Port:     5432,
		User:     "nexus",
		Password: "secret",
		DBName:   "catalog",
		SSLMode:  "disable",
	}

	got := cfg.URL()
	want := "postgres://nexus:secret@db-host:5432/catalog?sslmode=disable"
	if got != want {
		t.Errorf("URL() = %q, want %q", got, want)
	}
}

func TestLoad_Defaults(t *testing.T) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
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
	if cfg.MinIO.Bucket != "lakehouse" {
		t.Errorf("MinIO.Bucket = %q, want %q", cfg.MinIO.Bucket, "lakehouse")
	}
	if cfg.DuckLake.TenantBasePath != "tenants" {
		t.Errorf("DuckLake.TenantBasePath = %q, want %q", cfg.DuckLake.TenantBasePath, "tenants")
	}
	if cfg.Pool.MaxIdleSessions != 1 {
		t.Errorf("Pool.MaxIdleSessions = %d, want 1", cfg.Pool.MaxIdleSessions)
	}
}
