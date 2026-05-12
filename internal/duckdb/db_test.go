package duckdb

import (
	"testing"

	"github.com/satheeshds/nexus/internal/config"
)

func TestInitStatements_DisablesKnownExtensionAutoloadAndAutoinstall(t *testing.T) {
	pgCfg := config.PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "user",
		Password: "pass",
		DBName:   "db",
		SSLMode:  "disable",
	}
	minioCfg := config.MinIOConfig{
		Endpoint:  "localhost:9000",
		AccessKey: "access",
		SecretKey: "secret",
		Bucket:    "lakehouse",
	}

	stmts := initStatements(pgCfg, minioCfg, "tenants/test", "ducklake_test")
	if len(stmts) < 4 {
		t.Fatalf("expected at least 4 init statements, got %d", len(stmts))
	}

	if got := stmts[0]; got != "SET autoload_known_extensions = false;" {
		t.Fatalf("unexpected first init statement: %q", got)
	}
	if got := stmts[1]; got != "SET autoinstall_known_extensions = false;" {
		t.Fatalf("unexpected second init statement: %q", got)
	}
	if got := stmts[2]; got != "INSTALL ducklake; LOAD ducklake;" {
		t.Fatalf("expected ducklake install/load statement to remain, got %q", got)
	}
	if got := stmts[3]; got != "INSTALL httpfs;  LOAD httpfs;" {
		t.Fatalf("expected httpfs install/load statement to remain, got %q", got)
	}
}
