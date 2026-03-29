package duckdb_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// openInMemory opens a fresh in-memory DuckDB instance.
// No extensions are loaded, so no network or external service access is needed.
func openInMemory(tb testing.TB) *sql.DB {
	tb.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		tb.Fatalf("open in-memory duckdb: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	return db
}

// BenchmarkDuckDBSimpleSelect benchmarks the overhead of running a trivial
// query (SELECT 42) through the DuckDB engine.
func BenchmarkDuckDBSimpleSelect(b *testing.B) {
	db := openInMemory(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := db.QueryRowContext(ctx, "SELECT 42")
		var v int
		if err := row.Scan(&v); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDuckDBInsert benchmarks inserting a single row into an in-memory
// DuckDB table.
func BenchmarkDuckDBInsert(b *testing.B) {
	db := openInMemory(b)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE bench_insert (
			id      INTEGER,
			name    VARCHAR,
			value   DOUBLE
		)
	`); err != nil {
		b.Fatalf("create table: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.ExecContext(ctx,
			"INSERT INTO bench_insert VALUES (?, ?, ?)",
			i, fmt.Sprintf("row-%d", i), float64(i)*1.5,
		); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDuckDBSelectByID benchmarks point-lookup queries against an
// in-memory table pre-populated with 10 000 rows.
func BenchmarkDuckDBSelectByID(b *testing.B) {
	db := openInMemory(b)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE bench_lookup (id INTEGER PRIMARY KEY, payload VARCHAR)
	`); err != nil {
		b.Fatalf("create table: %v", err)
	}

	const rows = 10_000
	for i := 0; i < rows; i++ {
		if _, err := db.ExecContext(ctx,
			"INSERT INTO bench_lookup VALUES (?, ?)",
			i, fmt.Sprintf("payload-%d", i),
		); err != nil {
			b.Fatalf("seed row %d: %v", i, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := db.QueryRowContext(ctx, "SELECT payload FROM bench_lookup WHERE id = ?", i%rows)
		var payload string
		if err := row.Scan(&payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDuckDBAggregate benchmarks an aggregation query (COUNT + SUM + AVG)
// over a table with 100 000 rows — representative of OLAP workloads.
func BenchmarkDuckDBAggregate(b *testing.B) {
	db := openInMemory(b)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE bench_agg AS
		SELECT
			range                            AS id,
			('cat' || (range % 10)::VARCHAR) AS category,
			(random() * 1000)::DOUBLE        AS amount
		FROM range(100000)
	`); err != nil {
		b.Fatalf("create aggregate table: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, `
			SELECT category, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg
			FROM bench_agg
			GROUP BY category
			ORDER BY category
		`)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var category string
			var cnt int64
			var total, avg float64
			if err := rows.Scan(&category, &cnt, &total, &avg); err != nil {
				b.Fatal(err)
			}
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

// BenchmarkDuckDBRangeScan benchmarks a full table scan with a filter and
// ORDER BY clause — typical of range queries over time-series lake data.
func BenchmarkDuckDBRangeScan(b *testing.B) {
	db := openInMemory(b)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE bench_range AS
		SELECT
			range                     AS ts,
			(random() * 100)::INTEGER AS value
		FROM range(50000)
	`); err != nil {
		b.Fatalf("create range table: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, `
			SELECT ts, value
			FROM bench_range
			WHERE ts BETWEEN 10000 AND 40000
			ORDER BY ts
		`)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var ts, value int64
			if err := rows.Scan(&ts, &value); err != nil {
				b.Fatal(err)
			}
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

// BenchmarkDuckDBParallelSelect benchmarks concurrent read queries. Each
// goroutine opens its own in-memory DuckDB connection (DuckDB's single-writer
// design means separate in-memory databases for parallel readers).
func BenchmarkDuckDBParallelSelect(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			b.Fatalf("open duckdb: %v", err)
		}
		defer db.Close()

		ctx := context.Background()

		if _, err := db.ExecContext(ctx, `
			CREATE TABLE t AS SELECT range AS id, (random()*1000)::INTEGER AS v FROM range(1000)
		`); err != nil {
			b.Fatalf("create table: %v", err)
		}

		for pb.Next() {
			row := db.QueryRowContext(ctx, "SELECT SUM(v) FROM t WHERE id < 500")
			var sum int64
			if err := row.Scan(&sum); err != nil {
				b.Error(err)
			}
		}
	})
}
