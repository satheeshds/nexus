package gateway

import (
	"testing"
)

// ── columnListContains ────────────────────────────────────────────────────────

func TestColumnListContains(t *testing.T) {
	cases := []struct {
		colList string
		colName string
		want    bool
	}{
		// id checks
		{"id, name, age", "id", true},
		{"name, id, age", "id", true},
		{"name, age, id", "id", true},
		{"ID, name", "id", true},          // case-insensitive
		{`"id", name`, "id", true},        // double-quoted
		{"name, age", "id", false},        // no id
		{"identifier, name", "id", false}, // 'identifier' is not 'id'
		{"tid, name", "id", false},        // 'tid' is not 'id'
		{"", "id", false},                 // empty

		// created_at checks
		{"name, created_at", "created_at", true},
		{"CREATED_AT, name", "created_at", true}, // case-insensitive
		{"name, age", "created_at", false},

		// updated_at checks
		{"name, updated_at", "updated_at", true},
		{"name, age", "updated_at", false},
	}

	for _, tc := range cases {
		got := columnListContains(tc.colList, tc.colName)
		if got != tc.want {
			t.Errorf("columnListContains(%q, %q) = %v; want %v", tc.colList, tc.colName, got, tc.want)
		}
	}
}

// ── splitTableName ────────────────────────────────────────────────────────────

func TestSplitTableName(t *testing.T) {
	cases := []struct {
		input      string
		wantSchema string
		wantTable  string
	}{
		{"tablename", "lake", "tablename"},
		{"lake.tablename", "lake", "tablename"},
		{"myschema.mytable", "myschema", "mytable"},
		{`"tablename"`, "lake", "tablename"},
		{`"lake"."tablename"`, "lake", "tablename"},
		{`"myschema"."mytable"`, "myschema", "mytable"},
	}

	for _, tc := range cases {
		gotSchema, gotTable := splitTableName(tc.input)
		if gotSchema != tc.wantSchema || gotTable != tc.wantTable {
			t.Errorf("splitTableName(%q) = (%q, %q); want (%q, %q)",
				tc.input, gotSchema, gotTable, tc.wantSchema, tc.wantTable)
		}
	}
}

// ── splitValueRows ────────────────────────────────────────────────────────────

func TestSplitValueRows(t *testing.T) {
	cases := []struct {
		input    string
		wantRows []string
	}{
		{
			"(1, 2)",
			[]string{"(1, 2)"},
		},
		{
			"(1, 2), (3, 4)",
			[]string{"(1, 2)", "(3, 4)"},
		},
		{
			"(1, 'hello'), (2, 'world')",
			[]string{"(1, 'hello')", "(2, 'world')"},
		},
		{
			// Single-quoted value with comma
			"(1, 'hello, world')",
			[]string{"(1, 'hello, world')"},
		},
		{
			// Escaped single-quote inside string
			"(1, 'it''s fine')",
			[]string{"(1, 'it''s fine')"},
		},
		{
			// Nested function call
			"(func(1, 2), 3)",
			[]string{"(func(1, 2), 3)"},
		},
		{
			// Three rows
			"(1, 'a'), (2, 'b'), (3, 'c')",
			[]string{"(1, 'a')", "(2, 'b')", "(3, 'c')"},
		},
		{
			// Trailing ON CONFLICT clause must not be counted as a row
			"(1) ON CONFLICT (id) DO NOTHING",
			[]string{"(1)"},
		},
		{
			// Multiple rows followed by ON CONFLICT
			"(1, 'a'), (2, 'b') ON CONFLICT (id) DO NOTHING",
			[]string{"(1, 'a')", "(2, 'b')"},
		},
		{
			// Trailing semicolon must not create an extra row
			"(1, 2);",
			[]string{"(1, 2)"},
		},
		{
			// Trailing RETURNING clause must not create an extra row
			"(1, 2) RETURNING id",
			[]string{"(1, 2)"},
		},
	}

	for _, tc := range cases {
		got := splitValueRows(tc.input)
		if len(got) != len(tc.wantRows) {
			t.Errorf("splitValueRows(%q): got %d rows, want %d: %v",
				tc.input, len(got), len(tc.wantRows), got)
			continue
		}
		for i, row := range got {
			if row != tc.wantRows[i] {
				t.Errorf("splitValueRows(%q)[%d] = %q; want %q",
					tc.input, i, row, tc.wantRows[i])
			}
		}
	}
}

// ── injectValsIntoRow ─────────────────────────────────────────────────────────

func TestInjectValsIntoRow(t *testing.T) {
	cases := []struct {
		row     string
		prepend []string
		append  []string
		want    string
	}{
		// id only
		{"(1, 2)", []string{"5"}, nil, "(5, 1, 2)"},
		// timestamps only
		{"('hello')", nil, []string{"NOW()", "NOW()"}, "('hello', NOW(), NOW())"},
		// id + timestamps
		{"('alice')", []string{"7"}, []string{"NOW()", "NOW()"}, "(7, 'alice', NOW(), NOW())"},
		// empty row
		{"()", []string{"3"}, nil, "(3)"},
		// empty row with timestamps
		{"()", nil, []string{"NOW()"}, "(NOW())"},
	}

	for _, tc := range cases {
		got := injectValsIntoRow(tc.row, tc.prepend, tc.append)
		if got != tc.want {
			t.Errorf("injectValsIntoRow(%q, %v, %v) = %q; want %q",
				tc.row, tc.prepend, tc.append, got, tc.want)
		}
	}
}

// ── insertRE (regex matching) ─────────────────────────────────────────────────

func TestInsertREMatch(t *testing.T) {
	matching := []string{
		"INSERT INTO t (name) VALUES ('hello')",
		"insert into t (name) VALUES ('hello')",
		"INSERT INTO lake.orders (product, qty) VALUES ('widget', 5)",
		"INSERT INTO t (a, b) VALUES (1, 2), (3, 4)",
		"  INSERT  INTO  t  (a)  VALUES  (1)  ",
	}

	for _, q := range matching {
		if !insertRE.MatchString(q) {
			t.Errorf("insertRE should match %q but did not", q)
		}
	}

	nonMatching := []string{
		"SELECT * FROM t",
		"UPDATE t SET a = 1",
		"DELETE FROM t",
		"INSERT INTO t VALUES (1, 2)", // no explicit column list
		"INSERT INTO t (a) SELECT 1",  // SELECT form
	}

	for _, q := range nonMatching {
		if insertRE.MatchString(q) {
			t.Errorf("insertRE should NOT match %q but did", q)
		}
	}
}

// ── escapeSQLString ───────────────────────────────────────────────────────────

func TestEscapeSQLString(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"normal", "normal"},
		{"it's", "it''s"},
		{"a'b'c", "a''b''c"},
		{"", ""},
	}

	for _, tc := range cases {
		got := escapeSQLString(tc.input)
		if got != tc.want {
			t.Errorf("escapeSQLString(%q) = %q; want %q", tc.input, got, tc.want)
		}
	}
}

// ── buildTypeList ─────────────────────────────────────────────────────────────

func TestBuildTypeList(t *testing.T) {
	got := buildTypeList([]string{"TIMESTAMP", "TIMESTAMPTZ"})
	want := "'TIMESTAMP', 'TIMESTAMPTZ'"
	if got != want {
		t.Errorf("buildTypeList = %q; want %q", got, want)
	}

	// Empty slice
	if buildTypeList(nil) != "" {
		t.Errorf("buildTypeList(nil) should return empty string")
	}
}

// ── integration test note ─────────────────────────────────────────────────────

func TestRewriteInsertDefaults_RequiresDB(t *testing.T) {
	// Full end-to-end rewrite requires a live DuckDB connection for both the
	// information_schema lookup and the COALESCE(MAX(id),0)+1 query.
	// This coverage belongs in integration-tagged tests.
	t.Skip("full rewrite requires a live DuckDB connection; see integration tests")
}

// ── stripReturningClause ──────────────────────────────────────────────────────

func TestStripReturningClause(t *testing.T) {
	cases := []struct {
		input    string
		wantBase string
		wantCols []string
	}{
		// No RETURNING clause
		{
			"INSERT INTO t (a) VALUES (1)",
			"INSERT INTO t (a) VALUES (1)",
			nil,
		},
		// Simple RETURNING id
		{
			"INSERT INTO t (a) VALUES (1) RETURNING id",
			"INSERT INTO t (a) VALUES (1)",
			[]string{"id"},
		},
		// Multiple returning cols
		{
			"INSERT INTO t (a) VALUES (1) RETURNING id, created_at",
			"INSERT INTO t (a) VALUES (1)",
			[]string{"id", "created_at"},
		},
		// Trailing semicolon
		{
			"INSERT INTO t (a) VALUES (1) RETURNING id;",
			"INSERT INTO t (a) VALUES (1)",
			[]string{"id"},
		},
		// Case-insensitive RETURNING keyword
		{
			"INSERT INTO t (a) VALUES (1) returning id",
			"INSERT INTO t (a) VALUES (1)",
			[]string{"id"},
		},
		// Double-quoted column name
		{
			`INSERT INTO t (a) VALUES (1) RETURNING "id"`,
			"INSERT INTO t (a) VALUES (1)",
			[]string{"id"},
		},
		// Multiple double-quoted column names
		{
			`INSERT INTO t (a) VALUES (1) RETURNING "id", "created_at"`,
			"INSERT INTO t (a) VALUES (1)",
			[]string{"id", "created_at"},
		},
	}

	for _, tc := range cases {
		gotBase, gotCols := stripReturningClause(tc.input)
		if gotBase != tc.wantBase {
			t.Errorf("stripReturningClause(%q) base = %q; want %q", tc.input, gotBase, tc.wantBase)
		}
		if len(gotCols) != len(tc.wantCols) {
			t.Errorf("stripReturningClause(%q) cols = %v; want %v", tc.input, gotCols, tc.wantCols)
			continue
		}
		for i, col := range gotCols {
			if col != tc.wantCols[i] {
				t.Errorf("stripReturningClause(%q) cols[%d] = %q; want %q", tc.input, i, col, tc.wantCols[i])
			}
		}
	}
}

// ── replaceIDSubqueries ───────────────────────────────────────────────────────

func TestReplaceIDSubqueries(t *testing.T) {
	cases := []struct {
		query  string
		table  string
		ids    []int64
		want   string
	}{
		{
			query: "INSERT INTO orders (id, name) VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM orders), 'x')",
			table: "orders",
			ids:   []int64{5},
			want:  "INSERT INTO orders (id, name) VALUES (5, 'x')",
		},
		{
			// Multi-row
			query: "INSERT INTO orders (id, name) VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM orders), 'a'), ((SELECT COALESCE(MAX(id), 0) + 2 FROM orders), 'b')",
			table: "orders",
			ids:   []int64{3, 4},
			want:  "INSERT INTO orders (id, name) VALUES (3, 'a'), (4, 'b')",
		},
		{
			// Schema-qualified table name
			query: "INSERT INTO lake.accounts (id, name) VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM lake.accounts), 'z')",
			table: "lake.accounts",
			ids:   []int64{10},
			want:  "INSERT INTO lake.accounts (id, name) VALUES (10, 'z')",
		},
	}

	for _, tc := range cases {
		got := replaceIDSubqueries(tc.query, tc.table, tc.ids)
		if got != tc.want {
			t.Errorf("replaceIDSubqueries(%q, %q, %v)\n  got  %q\n  want %q",
				tc.query, tc.table, tc.ids, got, tc.want)
		}
	}
}

