// loadtest is a standalone CLI tool that benchmarks the Nexus data layer by
// sending concurrent SQL queries through the pgwire gateway and reporting
// throughput and latency percentiles.
//
// Usage:
//
//	loadtest -dsn "postgres://user:pass@localhost:5433/lake" \
//	         -concurrency 10 \
//	         -duration 30s \
//	         -query "SELECT 42"
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// result holds the outcome of a single query execution.
type result struct {
	latency time.Duration
	err     error
}

// stats aggregates benchmark results.
type stats struct {
	total    int64
	errors   int64
	latencies []time.Duration
	mu        sync.Mutex
}

func (s *stats) record(r result) {
	atomic.AddInt64(&s.total, 1)
	if r.err != nil {
		atomic.AddInt64(&s.errors, 1)
		return
	}
	s.mu.Lock()
	s.latencies = append(s.latencies, r.latency)
	s.mu.Unlock()
}

// report prints a human-readable summary to stdout.
func (s *stats) report(elapsed time.Duration) {
	s.mu.Lock()
	latencies := make([]time.Duration, len(s.latencies))
	copy(latencies, s.latencies)
	s.mu.Unlock()

	total := atomic.LoadInt64(&s.total)
	errors := atomic.LoadInt64(&s.errors)
	successful := int64(len(latencies))

	fmt.Println("─────────────────────────────────────────────")
	fmt.Println("  Nexus Data Layer Load Test Results")
	fmt.Println("─────────────────────────────────────────────")
	fmt.Printf("  Duration:        %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Total requests:  %d\n", total)
	fmt.Printf("  Successful:      %d\n", successful)
	fmt.Printf("  Errors:          %d\n", errors)
	if elapsed.Seconds() > 0 {
		fmt.Printf("  Throughput:      %.2f req/s\n", float64(successful)/elapsed.Seconds())
	}

	if len(latencies) == 0 {
		fmt.Println("  No successful requests to report latency for.")
		return
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	mean := meanDuration(latencies)
	fmt.Printf("  Latency mean:    %v\n", mean.Round(time.Microsecond))
	fmt.Printf("  Latency p50:     %v\n", percentile(latencies, 50).Round(time.Microsecond))
	fmt.Printf("  Latency p90:     %v\n", percentile(latencies, 90).Round(time.Microsecond))
	fmt.Printf("  Latency p95:     %v\n", percentile(latencies, 95).Round(time.Microsecond))
	fmt.Printf("  Latency p99:     %v\n", percentile(latencies, 99).Round(time.Microsecond))
	fmt.Printf("  Latency min:     %v\n", latencies[0].Round(time.Microsecond))
	fmt.Printf("  Latency max:     %v\n", latencies[len(latencies)-1].Round(time.Microsecond))
	fmt.Println("─────────────────────────────────────────────")
}

func percentile(sorted []time.Duration, pct float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(pct/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func meanDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var sum time.Duration
	for _, v := range durations {
		sum += v
	}
	return sum / time.Duration(len(durations))
}

// worker executes the configured query in a loop until ctx is cancelled.
func worker(ctx context.Context, db *sql.DB, query string, s *stats, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		start := time.Now()
		rows, err := db.QueryContext(ctx, query)
		latency := time.Since(start)
		if err != nil {
			// Context cancellation is not an error we want to count.
			if ctx.Err() != nil {
				return
			}
			s.record(result{err: err})
			continue
		}
		// Drain rows so the connection is returned to the pool.
		for rows.Next() {
		}
		if err := rows.Err(); err != nil && ctx.Err() == nil {
			s.record(result{err: err})
		} else {
			s.record(result{latency: latency})
		}
		rows.Close()
	}
}

func main() {
	dsn := flag.String("dsn", "", "PostgreSQL DSN for the Nexus gateway (required)")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers")
	duration := flag.Duration("duration", 30*time.Second, "How long to run the load test")
	query := flag.String("query", "SELECT 1", "SQL query to benchmark")
	warmup := flag.Duration("warmup", 5*time.Second, "Warmup period before recording results")
	maxConns := flag.Int("max-conns", 0, "Maximum open connections (0 = 2× concurrency)")
	flag.Parse()

	if *dsn == "" {
		// Fall back to environment variable.
		*dsn = os.Getenv("NEXUS_LOADTEST_DSN")
	}
	if *dsn == "" {
		log.Fatal("--dsn or NEXUS_LOADTEST_DSN is required")
	}

	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	if *maxConns == 0 {
		*maxConns = *concurrency * 2
	}
	db.SetMaxOpenConns(*maxConns)
	db.SetMaxIdleConns(*concurrency)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify connectivity.
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := db.PingContext(pingCtx); err != nil {
		cancel()
		log.Fatalf("ping gateway: %v", err)
	}
	cancel()

	fmt.Printf("Connected to Nexus gateway.\n")
	fmt.Printf("  Query:       %s\n", *query)
	fmt.Printf("  Concurrency: %d\n", *concurrency)
	fmt.Printf("  Duration:    %v (+ %v warmup)\n", *duration, *warmup)
	fmt.Println()

	// Handle Ctrl-C gracefully.
	ctx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\nInterrupted – stopping workers …")
		cancelRun()
	}()

	// Warmup phase: run workers but discard results.
	if *warmup > 0 {
		fmt.Printf("Warming up for %v …\n", *warmup)
		warmupCtx, warmupCancel := context.WithTimeout(ctx, *warmup)
		warmupStats := &stats{}
		var wg sync.WaitGroup
		for i := 0; i < *concurrency; i++ {
			wg.Add(1)
			go worker(warmupCtx, db, *query, warmupStats, &wg)
		}
		wg.Wait()
		warmupCancel()
		if ctx.Err() != nil {
			return // interrupted during warmup
		}
		fmt.Println("Warmup complete. Starting benchmark …")
	}

	// Benchmark phase.
	s := &stats{
		latencies: make([]time.Duration, 0, 1024),
	}
	var wg sync.WaitGroup
	runCtx, runCancel := context.WithTimeout(ctx, *duration)
	defer runCancel()

	start := time.Now()
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(runCtx, db, *query, s, &wg)
	}
	wg.Wait()
	elapsed := time.Since(start)

	s.report(elapsed)
}
