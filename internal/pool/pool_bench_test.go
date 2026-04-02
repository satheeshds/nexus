package pool_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
	"github.com/satheeshds/nexus/internal/pool"
)

// stubCatalog is a minimal in-memory catalog.DB substitute.
// It satisfies the unexported catalogStore interface used by pool.Pool via
// the public *catalog.DB field in pool.New – but for pure lock-contention
// benchmarks we create a real catalog.DB only when PostgreSQL is available.

// skipIfNoInfra skips a benchmark when NEXUS_POSTGRES_HOST is not set.
func skipIfNoInfra(tb testing.TB) {
	tb.Helper()
	if os.Getenv("NEXUS_POSTGRES_HOST") == "" {
		tb.Skip("NEXUS_POSTGRES_HOST not set; skipping integration benchmark")
	}
}

// newTestPool builds a Pool backed by a real PostgreSQL catalog.
// The pool is closed when the test finishes.
func newTestPool(b *testing.B) *pool.Pool {
	b.Helper()
	skipIfNoInfra(b)

	host := os.Getenv("NEXUS_POSTGRES_HOST")
	pgCfg := config.PostgresConfig{
		Host:     host,
		Port:     5432,
		User:     envOrDefault("NEXUS_POSTGRES_USER", "nexus"),
		Password: envOrDefault("NEXUS_POSTGRES_PASSWORD", "changeme"),
		DBName:   envOrDefault("NEXUS_POSTGRES_DBNAME", "lake_catalog"),
		SSLMode:  "disable",
	}
	minioCfg := config.MinIOConfig{
		Endpoint:     envOrDefault("NEXUS_MINIO_ENDPOINT", "localhost:9000"),
		AccessKey:    envOrDefault("NEXUS_MINIO_ACCESS_KEY", "minioadmin"),
		SecretKey:    envOrDefault("NEXUS_MINIO_SECRET_KEY", "changeme"),
		Bucket:       envOrDefault("NEXUS_MINIO_BUCKET", "lakehouse"),
		UseSSL:       false,
		UsePathStyle: true,
	}
	poolCfg := config.PoolConfig{
		MaxIdleSessions:  1,
		SessionTTL:       30 * time.Minute,
		EvictionInterval: 5 * time.Minute,
	}

	cat, err := catalog.New(context.Background(), pgCfg)
	if err != nil {
		b.Fatalf("connect to postgres: %v", err)
	}

	p := pool.New(cat, pgCfg, minioCfg, poolCfg)
	b.Cleanup(func() {
		p.Close()
		cat.Close()
	})
	return p
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// BenchmarkPoolGet_CacheHit measures the hot-path cost of retrieving an already-
// cached session. The first call creates the session; subsequent iterations
// exercise only the lock and map lookup.
func BenchmarkPoolGet_CacheHit(b *testing.B) {
	p := newTestPool(b)
	ctx := context.Background()

	tenantID := os.Getenv("NEXUS_BENCH_TENANT_ID")
	if tenantID == "" {
		b.Skip("NEXUS_BENCH_TENANT_ID not set; skipping pool cache-hit benchmark")
	}

	// Warm up the cache.
	if _, err := p.Get(ctx, tenantID); err != nil {
		b.Fatalf("warm-up Get: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Get(ctx, tenantID); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

// BenchmarkPoolGet_ConcurrentCacheHit measures pool throughput when many
// goroutines concurrently request the same already-cached tenant session.
func BenchmarkPoolGet_ConcurrentCacheHit(b *testing.B) {
	p := newTestPool(b)
	ctx := context.Background()

	tenantID := os.Getenv("NEXUS_BENCH_TENANT_ID")
	if tenantID == "" {
		b.Skip("NEXUS_BENCH_TENANT_ID not set; skipping concurrent cache-hit benchmark")
	}

	// Warm up.
	if _, err := p.Get(ctx, tenantID); err != nil {
		b.Fatalf("warm-up Get: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := p.Get(ctx, tenantID); err != nil {
				b.Errorf("Get: %v", err)
			}
		}
	})
}

// BenchmarkPoolEvict measures the time to evict a session from the pool.
func BenchmarkPoolEvict(b *testing.B) {
	p := newTestPool(b)
	tenantID := os.Getenv("NEXUS_BENCH_TENANT_ID")
	if tenantID == "" {
		b.Skip("NEXUS_BENCH_TENANT_ID not set; skipping eviction benchmark")
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Re-create session so each iteration evicts a real session.
		if _, err := p.Get(ctx, tenantID); err != nil {
			b.Fatalf("Get: %v", err)
		}
		b.StartTimer()
		p.Evict(tenantID)
	}
}

// BenchmarkPoolGet_ConcurrentDifferentTenants measures pool throughput when
// many goroutines retrieve sessions for different (but already-cached) tenants.
func BenchmarkPoolGet_ConcurrentDifferentTenants(b *testing.B) {
	p := newTestPool(b)
	ctx := context.Background()

	// Read a comma-separated list of tenant IDs from env.
	tenantIDs := os.Getenv("NEXUS_BENCH_TENANT_IDS")
	if tenantIDs == "" {
		b.Skip("NEXUS_BENCH_TENANT_IDS not set; skipping multi-tenant benchmark")
	}

	// Parse comma-separated tenant IDs.
	ids := splitCSV(tenantIDs)
	if len(ids) == 0 {
		b.Skip("NEXUS_BENCH_TENANT_IDS is empty")
	}

	// Warm up all tenant sessions.
	for _, id := range ids {
		if _, err := p.Get(ctx, id); err != nil {
			b.Fatalf("warm-up Get(%s): %v", id, err)
		}
	}

	var counter uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			id := ids[i%len(ids)]
			i++
			if _, err := p.Get(ctx, id); err != nil {
				b.Errorf("Get(%s): %v", id, err)
			}
			atomic.AddUint64(&counter, 1)
			_ = fmt.Sprintf("%d", counter) // prevent optimisation
		}
	})
}

// splitCSV splits a comma-separated string into trimmed, non-empty parts.
func splitCSV(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			part := trim(s[start:i])
			if part != "" {
				out = append(out, part)
			}
			start = i + 1
		}
	}
	return out
}

func trim(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
