package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/satheeshds/nexus/internal/duckdb"
)

// insertRE detects INSERT INTO statements with an explicit column list.
//
// Capture groups:
//
//	1 – table name (possibly schema-qualified or double-quoted, no spaces)
//	2 – column list (content inside the first set of parentheses)
//	3 – values string (everything after the VALUES keyword)
var insertRE = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+(\S+)\s*\(([^)]+)\)\s+VALUES\s+(.+)$`)

// integerDataTypes is the ordered list of DuckDB integer type names that
// qualify an 'id' column for automatic sequential ID generation. The slice
// is kept in deterministic order so that the generated IN(...) clause is
// stable across runs.
var integerDataTypes = []string{
	"BIGINT",
	"HUGEINT",
	"INT",
	"INT1",
	"INT2",
	"INT4",
	"INT8",
	"INTEGER",
	"SMALLINT",
	"TINYINT",
	"UBIGINT",
	"UINTEGER",
	"USMALLINT",
	"UTINYINT",
}

// validIdentRE allows only characters that appear in valid SQL identifiers
// (letters, digits, underscores, dots, and optional double-quote delimiters).
// Used to reject table names that could introduce SQL injection.
var validIdentRE = regexp.MustCompile(`^("?[A-Za-z_][A-Za-z0-9_]*"?\.)*"?[A-Za-z_][A-Za-z0-9_]*"?$`)

// rewriteInsertForSequentialID rewrites an INSERT statement to inject a
// sequential integer id when:
//   - the statement is an INSERT with an explicit column list,
//   - the target table has an integer 'id' column, and
//   - the column list does not already include 'id'.
//
// If 'id' is already present in the column list the statement is passed
// through unchanged. If the table does not have an integer 'id' column,
// or if any inspection step fails, the statement is also passed through
// unchanged so that the original error (if any) surfaces from DuckDB.
//
// tableIDCache is a caller-maintained map (tableName → hasIntID) that
// avoids repeated information_schema queries for the same table within a
// single connection's lifetime.
func rewriteInsertForSequentialID(
	ctx context.Context,
	conn *duckdb.Conn,
	query string,
	args []any,
	tableIDCache map[string]bool,
) (string, []any) {
	m := insertRE.FindStringSubmatch(query)
	if m == nil {
		return query, args
	}

	tableName := strings.TrimSpace(m[1])
	colListRaw := m[2]
	valuesRaw := strings.TrimSpace(m[3])

	// Pass through if 'id' is already in the column list.
	if columnListContainsID(colListRaw) {
		return query, args
	}

	// Check (with cache) whether the table has an integer 'id' column.
	hasID, seen := tableIDCache[tableName]
	if !seen {
		hasID = tableHasIntegerID(ctx, conn, tableName)
		tableIDCache[tableName] = hasID
	}
	if !hasID {
		return query, args
	}

	// Get the next sequential ID.
	nextID, err := getNextSequentialID(ctx, conn, tableName)
	if err != nil {
		slog.Warn("seqid: cannot get next id; passing query through",
			"table", tableName, "err", err)
		return query, args
	}

	// Split VALUES into individual row strings and inject the id.
	rows := splitValueRows(valuesRaw)
	if len(rows) == 0 {
		return query, args
	}

	rewritten := make([]string, len(rows))
	for i, row := range rows {
		rewritten[i] = injectIDIntoRow(row, nextID+int64(i))
	}

	newQuery := fmt.Sprintf("INSERT INTO %s (id, %s) VALUES %s",
		tableName, colListRaw, strings.Join(rewritten, ", "))

	slog.Debug("seqid: injected sequential id",
		"table", tableName, "start_id", nextID, "rows", len(rows))

	return newQuery, args
}

// columnListContainsID returns true when the comma-separated column list
// contains a column named 'id' (case-insensitive, tolerates double-quotes).
func columnListContainsID(colList string) bool {
	for _, col := range strings.Split(colList, ",") {
		col = strings.TrimSpace(col)
		col = strings.Trim(col, `"`)
		if strings.EqualFold(col, "id") {
			return true
		}
	}
	return false
}

// tableHasIntegerID returns true when the given table has a column named
// 'id' whose data_type is one of the standard SQL integer types supported
// by DuckDB.
func tableHasIntegerID(ctx context.Context, conn *duckdb.Conn, tableName string) bool {
	schema, table := splitTableName(tableName)

	// Build an IN list of integer type names for the WHERE clause.
	typeList := buildTypeList()

	query := fmt.Sprintf(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = '%s'
		  AND table_name   = '%s'
		  AND column_name  = 'id'
		  AND upper(data_type) IN (%s)`,
		escapeSQLString(schema),
		escapeSQLString(table),
		typeList,
	)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		slog.Debug("seqid: information_schema query failed", "err", err)
		return false
	}
	defer rows.Close()

	if rows.Next() {
		var count int64
		if rows.Scan(&count) == nil {
			return count > 0
		}
	}
	return false
}

// getNextSequentialID returns COALESCE(MAX(id), 0) + 1 from the given table.
//
// Note: computing MAX+1 is inherently subject to a time-of-check/time-of-use
// race when two concurrent connections insert into the same table at the same
// time. DuckLake does not support native sequences, so this best-effort
// approach is the available mechanism. Within a single tenant session the pool
// guarantees one DuckDB connection per tenant, so the risk is low in practice.
func getNextSequentialID(ctx context.Context, conn *duckdb.Conn, tableName string) (int64, error) {
	// Validate the table name to prevent SQL injection.
	if !validIdentRE.MatchString(tableName) {
		return 0, fmt.Errorf("invalid table name %q", tableName)
	}
	query := fmt.Sprintf("SELECT COALESCE(MAX(id), 0) + 1 FROM %s", tableName)
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("get max id from %s: %w", tableName, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return 1, nil
	}
	var nextID int64
	if err := rows.Scan(&nextID); err != nil {
		return 0, fmt.Errorf("scan max id from %s: %w", tableName, err)
	}
	return nextID, nil
}

// splitTableName splits a (possibly schema-qualified, possibly double-quoted)
// table name into its schema and table components. If no schema is present,
// "lake" is returned as the default (matching SET search_path='lake').
func splitTableName(tableName string) (schema, table string) {
	// Find a dot that is not inside double-quotes.
	inQuote := false
	dotIdx := -1
	for i, c := range tableName {
		switch c {
		case '"':
			inQuote = !inQuote
		case '.':
			if !inQuote {
				dotIdx = i
			}
		}
	}

	if dotIdx >= 0 {
		schema = strings.Trim(tableName[:dotIdx], `"`)
		table = strings.Trim(tableName[dotIdx+1:], `"`)
	} else {
		schema = "lake"
		table = strings.Trim(tableName, `"`)
	}
	return
}

// splitValueRows splits the VALUES portion of an INSERT statement into
// individual row strings (each with its surrounding parentheses). It
// correctly handles nested parentheses and single-quoted string literals
// (including doubled single-quote escape sequences '').
func splitValueRows(valuesStr string) []string {
	var rows []string
	depth := 0
	inString := false
	start := -1

	for i := 0; i < len(valuesStr); i++ {
		c := valuesStr[i]

		if inString {
			if c == '\'' {
				if i+1 < len(valuesStr) && valuesStr[i+1] == '\'' {
					i++ // skip escaped ''
				} else {
					inString = false
				}
			}
			continue
		}

		switch c {
		case '\'':
			inString = true
		case '(':
			depth++
			if depth == 1 {
				start = i
			}
		case ')':
			depth--
			if depth == 0 && start >= 0 {
				rows = append(rows, valuesStr[start:i+1])
				start = -1
			}
		}
	}
	return rows
}

// injectIDIntoRow prepends the given id literal to a single VALUES row.
// e.g. "(1, 'hello')" with id=5 → "(5, 1, 'hello')"
func injectIDIntoRow(row string, id int64) string {
	if len(row) < 2 || row[0] != '(' || row[len(row)-1] != ')' {
		return row
	}
	inner := row[1 : len(row)-1]
	if strings.TrimSpace(inner) == "" {
		return fmt.Sprintf("(%d)", id)
	}
	return fmt.Sprintf("(%d, %s)", id, inner)
}

// buildTypeList returns a SQL-ready comma-separated, single-quoted list of
// integer type names for use in an IN (...) clause. The output is
// deterministic because integerDataTypes is a sorted slice.
func buildTypeList() string {
	parts := make([]string, len(integerDataTypes))
	for i, t := range integerDataTypes {
		parts[i] = "'" + t + "'"
	}
	return strings.Join(parts, ", ")
}

// escapeSQLString escapes a string value for safe embedding inside a
// single-quoted SQL literal by doubling any contained single quotes.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
