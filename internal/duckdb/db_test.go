package duckdb

import (
	"strings"
	"testing"

	"github.com/satheeshds/nexus/internal/config"
)

func TestInitStatements_DisablesAutoExtensions(t *testing.T) {
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
	if len(stmts) < 5 {
		t.Fatalf("expected at least 5 init statements, got %d", len(stmts))
	}

	if got := stmts[0]; got != "SET autoload_known_extensions = false;" {
		t.Fatalf("unexpected first init statement: %q", got)
	}
	if got := stmts[1]; got != "SET autoinstall_known_extensions = false;" {
		t.Fatalf("unexpected second init statement: %q", got)
	}
	if !containsStatement(stmts, "INSTALL ducklake; LOAD ducklake;") {
		t.Fatalf("expected ducklake install/load statement to remain")
	}
	if !containsStatement(stmts, "INSTALL httpfs; LOAD httpfs;") {
		t.Fatalf("expected httpfs install/load statement to remain")
	}
	if !containsStatement(stmts, "INSTALL postgres_scanner; LOAD postgres_scanner;") {
		t.Fatalf("expected postgres_scanner install/load statement to remain")
	}
}

func TestEscapeDuckLiteral_EscapesSingleQuotes(t *testing.T) {
	in := "tenant'oops"
	out := escapeDuckLiteral(in)
	if out != "tenant''oops" {
		t.Fatalf("unexpected escaped value: %q", out)
	}
	if strings.Count(out, "'") != 2 {
		t.Fatalf("expected escaped output to contain exactly one quote pair, got %q", out)
	}
}

func containsStatement(stmts []string, expected string) bool {
	for _, stmt := range stmts {
		if normalizeSQL(stmt) == normalizeSQL(expected) {
			return true
		}
	}
	return false
}

func normalizeSQL(stmt string) string {
	return strings.Join(strings.Fields(stmt), " ")
}
