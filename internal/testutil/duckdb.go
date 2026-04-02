//go:build integration

package testutil

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// CheckDuckDBExtensionsAvailable skips the test if the DuckDB extension
// registry is unreachable. This is expected in sandboxed environments
// without outbound internet access.
func CheckDuckDBExtensionsAvailable(t *testing.T) {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Skipf("open duckdb for extension check: %v", err)
	}
	defer db.Close()

	_, err = db.ExecContext(context.Background(), "INSTALL ducklake;")
	if err != nil && isExtensionDownloadError(err) {
		t.Skipf("DuckDB extension registry unreachable (no internet access): %v", err)
	}
	if err != nil {
		// LOAD error is fine – it just means it was already installed.
		if !strings.Contains(err.Error(), "already loaded") {
			t.Skipf("DuckDB extension check failed unexpectedly: %v", err)
		}
	}
}

// isExtensionDownloadError returns true when the error indicates that the
// DuckDB extension HTTP registry could not be reached.
func isExtensionDownloadError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Failed to download extension") ||
		strings.Contains(msg, "Could not establish connection") ||
		strings.Contains(msg, "extensions.duckdb.org")
}
