package gateway

import (
	"testing"
)

// ── columnListContainsID ──────────────────────────────────────────────────────

func TestColumnListContainsID(t *testing.T) {
	cases := []struct {
		colList string
		want    bool
	}{
		{"id, name, age", true},
		{"name, id, age", true},
		{"name, age, id", true},
		{"ID, name", true},        // case-insensitive
		{`"id", name`, true},      // double-quoted
		{"name, age", false},      // no id
		{"identifier, name", false}, // 'identifier' is not 'id'
		{"tid, name", false},      // 'tid' is not 'id'
		{"", false},               // empty
	}

	for _, tc := range cases {
		got := columnListContainsID(tc.colList)
		if got != tc.want {
			t.Errorf("columnListContainsID(%q) = %v; want %v", tc.colList, got, tc.want)
		}
	}
}

// ── splitTableName ────────────────────────────────────────────────────────────

func TestSplitTableName(t *testing.T) {
	cases := []struct {
		input        string
		wantSchema   string
		wantTable    string
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

// ── injectIDIntoRow ───────────────────────────────────────────────────────────

func TestInjectIDIntoRow(t *testing.T) {
	cases := []struct {
		row  string
		id   int64
		want string
	}{
		{"(1, 2)", 5, "(5, 1, 2)"},
		{"('hello', 'world')", 10, "(10, 'hello', 'world')"},
		{"()", 3, "(3)"},
	}

	for _, tc := range cases {
		got := injectIDIntoRow(tc.row, tc.id)
		if got != tc.want {
			t.Errorf("injectIDIntoRow(%q, %d) = %q; want %q", tc.row, tc.id, got, tc.want)
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
		"INSERT INTO t VALUES (1, 2)",   // no explicit column list
		"INSERT INTO t (a) SELECT 1",    // SELECT form
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

// ── end-to-end rewrite (without a real DB connection) ────────────────────────
// These tests verify the full rewrite path using a non-nil but nil-valued
// *duckdb.Conn. Since the table has no 'id' column check possible without a
// real DB, we use a pre-populated tableIDCache to simulate the cache hit.

func TestRewriteInsertForSequentialID_CacheHit(t *testing.T) {
	// We can't call rewriteInsertForSequentialID without a real DB connection
	// for the getNextSequentialID step. The tests below focus on the parts
	// that don't require a live connection (already covered above).
	//
	// Integration-level coverage of the full path is handled by
	// TestInsertSequentialID_Integration (seqid_integration_test.go) when the
	// -tags integration build tag is used.
	t.Skip("full rewrite requires a live DuckDB connection; see integration tests")
}
