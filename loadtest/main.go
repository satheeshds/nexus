// loadtest is a standalone command that drives concurrent load against the
// Nexus catalog data layer and reports throughput and latency percentiles.
//
// Usage:
//
//	go run ./loadtest [flags]
//
// Flags:
//
//	-workers   int           Number of concurrent goroutines  (default 10)
//	-duration  duration      How long to run the test         (default 30s)
//	-pg-host   string        Postgres host                    (default localhost)
//	-pg-port   int           Postgres port                    (default 5432)
//	-pg-user   string        Postgres user                    (default nexus)
//	-pg-pass   string        Postgres password                (default changeme)
//	-pg-db     string        Postgres database                (default lake_catalog)
//	-ops       string        Comma-separated ops to run       (default all)
//	                         Supported: insert,get,list,delete
//
// The tool inserts, reads, and deletes tenant rows while measuring per-operation
// latency. At the end it prints a summary table with count, ops/sec, and p50,
// p95, p99, and max latencies.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
)

// --------------------------------------------------------------------------
// CLI flags
// --------------------------------------------------------------------------

var (
	flagWorkers  = flag.Int("workers", 10, "Number of concurrent goroutines")
	flagDuration = flag.Duration("duration", 30*time.Second, "Test duration")
	flagPGHost   = flag.String("pg-host", envOrDefault("NEXUS_POSTGRES_HOST", "localhost"), "Postgres host")
	flagPGPort   = flag.Int("pg-port", intEnvOrDefault("NEXUS_POSTGRES_PORT", 5432), "Postgres port")
	flagPGUser   = flag.String("pg-user", envOrDefault("NEXUS_POSTGRES_USER", "nexus"), "Postgres user")
	flagPGPass   = flag.String("pg-pass", envOrDefault("NEXUS_POSTGRES_PASSWORD", "changeme"), "Postgres password")
	flagPGDB     = flag.String("pg-db", envOrDefault("NEXUS_POSTGRES_DBNAME", "lake_catalog"), "Postgres database")
	flagOps      = flag.String("ops", "insert,get,list,delete", "Comma-separated operations to run")
)

// --------------------------------------------------------------------------
// Stats collection
// --------------------------------------------------------------------------

type opStats struct {
	mu        sync.Mutex
	latencies []time.Duration
	errors    int
}

func (s *opStats) record(d time.Duration) {
	s.mu.Lock()
	s.latencies = append(s.latencies, d)
	s.mu.Unlock()
}

func (s *opStats) recordErr() {
	s.mu.Lock()
	s.errors++
	s.mu.Unlock()
}

type collector struct {
	mu  sync.Mutex
	ops map[string]*opStats
}

func newCollector(ops []string) *collector {
	c := &collector{ops: make(map[string]*opStats, len(ops))}
	for _, op := range ops {
		c.ops[op] = &opStats{}
	}
	return c
}

func (c *collector) record(op string, d time.Duration, err error) {
	c.mu.Lock()
	s, ok := c.ops[op]
	if !ok {
		s = &opStats{}
		c.ops[op] = s
	}
	c.mu.Unlock()

	if err != nil {
		s.recordErr()
		return
	}
	s.record(d)
}

// --------------------------------------------------------------------------
// Reporting
// --------------------------------------------------------------------------

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func fmtDur(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.0fµs", float64(d)/float64(time.Microsecond))
	}
}

func printReport(c *collector, elapsed time.Duration) {
	// Gather and sort operation names for deterministic output.
	names := make([]string, 0, len(c.ops))
	for name := range c.ops {
		names = append(names, name)
	}
	sort.Strings(names)

	fmt.Println()
	fmt.Println("┌─────────────────────────────────────────────────────────────────────────────────────────┐")
	fmt.Printf( "│  Nexus Data Layer Load Test Results  (duration: %v)\n", elapsed.Round(time.Millisecond))
	fmt.Println("├──────────────────┬──────────┬────────────┬──────────┬──────────┬──────────┬──────────┬──────┤")
	fmt.Printf( "│ %-16s │ %8s │ %10s │ %8s │ %8s │ %8s │ %8s │ %4s │\n",
		"Operation", "Count", "Ops/sec", "P50", "P95", "P99", "Max", "Err")
	fmt.Println("├──────────────────┼──────────┼────────────┼──────────┼──────────┼──────────┼──────────┼──────┤")

	for _, name := range names {
		s := c.ops[name]
		s.mu.Lock()
		lats := make([]time.Duration, len(s.latencies))
		copy(lats, s.latencies)
		errs := s.errors
		s.mu.Unlock()

		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		count := len(lats)
		opsPerSec := float64(count) / elapsed.Seconds()

		var p50, p95, p99, maxD string
		if count > 0 {
			p50 = fmtDur(percentile(lats, 50))
			p95 = fmtDur(percentile(lats, 95))
			p99 = fmtDur(percentile(lats, 99))
			maxD = fmtDur(lats[count-1])
		} else {
			p50, p95, p99, maxD = "-", "-", "-", "-"
		}

		fmt.Printf("│ %-16s │ %8d │ %10.1f │ %8s │ %8s │ %8s │ %8s │ %4d │\n",
			name, count, opsPerSec, p50, p95, p99, maxD, errs)
	}

	fmt.Println("└──────────────────┴──────────┴────────────┴──────────┴──────────┴──────────┴──────────┴──────┘")
}

// --------------------------------------------------------------------------
// Worker logic
// --------------------------------------------------------------------------

// worker performs catalog operations in a loop until the deadline passes.
// It keeps a local slice of inserted tenant IDs so get/delete have valid targets.
func worker(ctx context.Context, db *catalog.DB, c *collector, ops []string, deadline time.Time) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // non-crypto use
	var insertedIDs []string

	for time.Now().Before(deadline) {
		op := ops[rng.Intn(len(ops))]

		switch op {
		case "insert":
			t := newTenant()
			start := time.Now()
			err := db.InsertTenant(ctx, t)
			c.record("insert", time.Since(start), err)
			if err == nil {
				insertedIDs = append(insertedIDs, t.ID)
			}

		case "get":
			if len(insertedIDs) == 0 {
				continue
			}
			id := insertedIDs[rng.Intn(len(insertedIDs))]
			start := time.Now()
			_, err := db.GetTenant(ctx, id)
			c.record("get", time.Since(start), err)

		case "list":
			start := time.Now()
			_, err := db.ListTenants(ctx)
			c.record("list", time.Since(start), err)

		case "delete":
			if len(insertedIDs) == 0 {
				continue
			}
			idx := rng.Intn(len(insertedIDs))
			id := insertedIDs[idx]
			// Remove from the local slice before issuing the delete so we do not
			// attempt to delete the same row twice even if the delete fails.
			insertedIDs = append(insertedIDs[:idx], insertedIDs[idx+1:]...)
			start := time.Now()
			err := db.DeleteTenant(ctx, id)
			c.record("delete", time.Since(start), err)
		}
	}

	// Clean up any remaining rows created by this worker.
	for _, id := range insertedIDs {
		_ = db.DeleteTenant(ctx, id)
	}
}

// newTenant creates a Tenant with UUID-derived unique fields.
func newTenant() catalog.Tenant {
	id := uuid.New().String()
	return catalog.Tenant{
		ID:           id,
		OrgName:      "LoadTest Org",
		Email:        fmt.Sprintf("lt-%s@example.com", id),
		S3Prefix:     "tenants/" + id,
		PGSchema:     "lt_" + id[:8],
		PasswordHash: "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy",
		CreatedAt:    time.Now(),
	}
}

// --------------------------------------------------------------------------
// Main
// --------------------------------------------------------------------------

func main() {
	flag.Parse()

	ops := parseOps(*flagOps)
	if len(ops) == 0 {
		log.Fatal("no valid operations specified; use -ops insert,get,list,delete")
	}

	cfg := config.PostgresConfig{
		Host:     *flagPGHost,
		Port:     *flagPGPort,
		User:     *flagPGUser,
		Password: *flagPGPass,
		DBName:   *flagPGDB,
		SSLMode:  "disable",
	}

	ctx := context.Background()
	db, err := catalog.New(ctx, cfg)
	if err != nil {
		log.Fatalf("connect to postgres: %v\n\nHint: start dev infrastructure with 'make dev-infra' and set NEXUS_POSTGRES_* environment variables.", err)
	}
	defer db.Close()

	fmt.Printf("Nexus Data Layer Load Test\n")
	fmt.Printf("  Workers:    %d\n", *flagWorkers)
	fmt.Printf("  Duration:   %v\n", *flagDuration)
	fmt.Printf("  Operations: %s\n", strings.Join(ops, ", "))
	fmt.Printf("  Postgres:   %s@%s:%d/%s\n", *flagPGUser, *flagPGHost, *flagPGPort, *flagPGDB)
	fmt.Println("Running…")

	c := newCollector(ops)
	deadline := time.Now().Add(*flagDuration)

	var wg sync.WaitGroup
	for i := 0; i < *flagWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx, db, c, ops, deadline)
		}()
	}

	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start)

	printReport(c, elapsed)
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

func parseOps(s string) []string {
	valid := map[string]bool{"insert": true, "get": true, "list": true, "delete": true}
	var ops []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(strings.ToLower(part))
		if valid[part] {
			ops = append(ops, part)
		} else if part != "" {
			log.Printf("warning: unknown operation %q (ignored)", part)
		}
	}
	return ops
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func intEnvOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}
