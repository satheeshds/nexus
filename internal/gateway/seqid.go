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
//1 – table name (possibly schema-qualified or double-quoted, no spaces)
//2 – column list (content inside the first set of parentheses)
//3 – values string (everything after the VALUES keyword)
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

// timestampDataTypes is the ordered list of DuckDB timestamp type names that
// qualify 'created_at' / 'updated_at' columns for automatic NOW() injection.
var timestampDataTypes = []string{
"TIMESTAMP",
"TIMESTAMP WITH TIME ZONE",
"TIMESTAMPTZ",
}

// validIdentRE allows only characters that appear in valid SQL identifiers
// (letters, digits, underscores, dots, and optional double-quote delimiters).
// Used to reject table names that could introduce SQL injection.
var validIdentRE = regexp.MustCompile(`^("?[A-Za-z_][A-Za-z0-9_]*"?\.)*"?[A-Za-z_][A-Za-z0-9_]*"?$`)

// tableAutoColumns caches which auto-injectable columns a given table has.
type tableAutoColumns struct {
hasIntID     bool // integer 'id' column → COALESCE(MAX(id),0)+1
hasCreatedAt bool // timestamp 'created_at' column → NOW()
hasUpdatedAt bool // timestamp 'updated_at' column → NOW()
}

// rewriteInsertDefaults rewrites an INSERT statement to inject default values
// for columns that the table defines but the INSERT omits:
//
//   - integer 'id'           → COALESCE(MAX(id), 0) + 1
//   - timestamp 'created_at' → NOW()
//   - timestamp 'updated_at' → NOW()
//
// If none of the above apply, or if the INSERT already provides all relevant
// columns, the statement is passed through unchanged. Any failure in the
// inspection or ID calculation step also results in a pass-through so that
// the original error (if any) surfaces directly from DuckDB.
//
// tableAutoCache is a caller-maintained map that avoids repeated
// information_schema queries for the same table within a single connection.
func rewriteInsertDefaults(
ctx context.Context,
conn *duckdb.Conn,
query string,
args []any,
tableAutoCache map[string]*tableAutoColumns,
) (string, []any) {
m := insertRE.FindStringSubmatch(query)
if m == nil {
return query, args
}

tableName := strings.TrimSpace(m[1])
colListRaw := m[2]
valuesRaw := strings.TrimSpace(m[3])

// Get (or populate) the cached column info for this table.
auto, seen := tableAutoCache[tableName]
if !seen {
auto = queryTableAutoColumns(ctx, conn, tableName)
tableAutoCache[tableName] = auto
}

// Determine which columns need injection.
needID := auto.hasIntID && !columnListContains(colListRaw, "id")
needCreatedAt := auto.hasCreatedAt && !columnListContains(colListRaw, "created_at")
needUpdatedAt := auto.hasUpdatedAt && !columnListContains(colListRaw, "updated_at")

if !needID && !needCreatedAt && !needUpdatedAt {
return query, args
}

// Resolve the next sequential ID if required.
var nextID int64
if needID {
var err error
nextID, err = getNextSequentialID(ctx, conn, tableName)
if err != nil {
slog.Warn("seqid: cannot get next id; passing query through",
"table", tableName, "err", err)
needID = false
if !needCreatedAt && !needUpdatedAt {
return query, args
}
}
}

// Split VALUES into individual row strings.
rows := splitValueRows(valuesRaw)
if len(rows) == 0 {
return query, args
}

// Build the sets of columns and values to prepend (id) and append (timestamps).
var prependCols []string
var appendCols []string

if needID {
prependCols = append(prependCols, "id")
}
if needCreatedAt {
appendCols = append(appendCols, "created_at")
}
if needUpdatedAt {
appendCols = append(appendCols, "updated_at")
}

// Build the new column list: [id,] <original columns> [, created_at] [, updated_at]
newColList := strings.TrimSpace(colListRaw)
if len(prependCols) > 0 {
newColList = strings.Join(prependCols, ", ") + ", " + newColList
}
if len(appendCols) > 0 {
newColList = newColList + ", " + strings.Join(appendCols, ", ")
}

// Build the append values (same for every row).
var appendVals []string
if needCreatedAt {
appendVals = append(appendVals, "NOW()")
}
if needUpdatedAt {
appendVals = append(appendVals, "NOW()")
}

// Rewrite each row.
rewritten := make([]string, len(rows))
for i, row := range rows {
var prependVals []string
if needID {
prependVals = append(prependVals, fmt.Sprintf("%d", nextID+int64(i)))
}
rewritten[i] = injectValsIntoRow(row, prependVals, appendVals)
}

// Preserve any original suffix after the last VALUES row, such as
// RETURNING, ON CONFLICT, a trailing semicolon, or additional statements.
// If we cannot locate the final original row reliably, fall back to the
// original query rather than risk silently changing semantics.
lastRow := rows[len(rows)-1]
lastRowIdx := strings.LastIndex(query, lastRow)
if lastRowIdx < 0 {
slog.Debug("seqid: unable to preserve trailing SQL, skipping rewrite",
"table", tableName,
)
return query, args
}
suffix := query[lastRowIdx+len(lastRow):]

newQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s%s",
tableName, newColList, strings.Join(rewritten, ", "), suffix)
slog.Debug("seqid: injected defaults",
"table", tableName,
"id", needID,
"created_at", needCreatedAt,
"updated_at", needUpdatedAt,
)

return newQuery, args
}

// columnListContains returns true when the comma-separated column list
// contains the given column name (case-insensitive, tolerates double-quotes).
func columnListContains(colList, colName string) bool {
for _, col := range strings.Split(colList, ",") {
col = strings.TrimSpace(col)
col = strings.Trim(col, `"`)
if strings.EqualFold(col, colName) {
return true
}
}
return false
}

// queryTableAutoColumns queries information_schema.columns once for the given
// table and returns which auto-injectable columns it has.
func queryTableAutoColumns(ctx context.Context, conn *duckdb.Conn, tableName string) *tableAutoColumns {
schema, table := splitTableName(tableName)

intTypes := buildTypeList(integerDataTypes)
tsTypes := buildTypeList(timestampDataTypes)

query := fmt.Sprintf(`
SELECT column_name, upper(data_type)
FROM information_schema.columns
WHERE table_schema = '%s'
  AND table_name   = '%s'
  AND (
        (column_name = 'id'         AND upper(data_type) IN (%s))
     OR (column_name = 'created_at' AND upper(data_type) IN (%s))
     OR (column_name = 'updated_at' AND upper(data_type) IN (%s))
  )`,
escapeSQLString(schema),
escapeSQLString(table),
intTypes,
tsTypes,
tsTypes,
)

result := &tableAutoColumns{}
rows, err := conn.QueryContext(ctx, query)
if err != nil {
slog.Debug("seqid: information_schema query failed", "err", err)
return result
}
defer rows.Close()

for rows.Next() {
var colName, dataType string
if err := rows.Scan(&colName, &dataType); err != nil {
continue
}
switch strings.ToLower(colName) {
case "id":
result.hasIntID = true
case "created_at":
result.hasCreatedAt = true
case "updated_at":
result.hasUpdatedAt = true
}
}
return result
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

// injectValsIntoRow prepends and/or appends literal value strings to a single
// VALUES row. For example, with prepend=["5"] and append=["NOW()", "NOW()"]:
//
//"('hello', 3)" → "(5, 'hello', 3, NOW(), NOW())"
func injectValsIntoRow(row string, prepend, appendVals []string) string {
if len(row) < 2 || row[0] != '(' || row[len(row)-1] != ')' {
return row
}
inner := strings.TrimSpace(row[1 : len(row)-1])

var parts []string
parts = append(parts, prepend...)
if inner != "" {
parts = append(parts, inner)
}
parts = append(parts, appendVals...)

return "(" + strings.Join(parts, ", ") + ")"
}

// buildTypeList returns a SQL-ready comma-separated, single-quoted list of
// type names for use in an IN (...) clause. The output order follows the
// input slice, which callers keep deterministic.
func buildTypeList(types []string) string {
parts := make([]string, len(types))
for i, t := range types {
parts[i] = "'" + t + "'"
}
return strings.Join(parts, ", ")
}

// escapeSQLString escapes a string value for safe embedding inside a
// single-quoted SQL literal by doubling any contained single quotes.
func escapeSQLString(s string) string {
return strings.ReplaceAll(s, "'", "''")
}
