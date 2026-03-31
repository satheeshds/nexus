package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
	"github.com/satheeshds/nexus/internal/duckdb"
)

// Session holds an open DuckDB+DuckLake connection for a tenant.
type Session struct {
	Conn      *duckdb.Conn
	TenantID  string
	S3Prefix  string
	PGSchema  string
	CreatedAt time.Time
	LastUsed  time.Time
}

// Pool manages per-tenant DuckDB sessions.
type Pool struct {
	mu       sync.Mutex
	sessions map[string]*Session // key: tenantID

	catalog  *catalog.DB
	pgCfg    config.PostgresConfig
	minioCfg config.MinIOConfig
	poolCfg  config.PoolConfig
}

func New(catalog *catalog.DB, pgCfg config.PostgresConfig, minioCfg config.MinIOConfig, poolCfg config.PoolConfig) *Pool {
	p := &Pool{
		sessions: make(map[string]*Session),
		catalog:  catalog,
		pgCfg:    pgCfg,
		minioCfg: minioCfg,
		poolCfg:  poolCfg,
	}
	go p.evictLoop()
	return p
}

// Get returns an existing session for the tenant, or creates a new one.
// It uses a double-check pattern: the lock is released while creating the
// DuckDB session (which can involve network I/O), so other tenants are not
// blocked. A race where two goroutines create sessions for the same tenant
// simultaneously is resolved by keeping the first one stored and closing any
// duplicate.
// s3Prefix and pgSchema are derived from the catalog service account record to
// prevent mismatches from stale or untrusted caller-supplied values.
func (p *Pool) Get(ctx context.Context, tenantID string) (*Session, error) {
	// First check under lock.
	p.mu.Lock()
	if s, ok := p.sessions[tenantID]; ok {
		s.LastUsed = time.Now()
		p.mu.Unlock()
		slog.Debug("pool: reusing session", "tenant", tenantID)
		return s, nil
	}
	p.mu.Unlock()

	// Create the connection without holding the lock so other tenants are not blocked.
	slog.Info("pool: creating new session", "tenant", tenantID)

	// Fetch tenant-specific credentials and routing info from the catalog.
	// Using the canonical service account record prevents stale or untrusted
	// caller-supplied s3Prefix/pgSchema values from being used.
	sa, err := p.catalog.GetServiceAccountByTenantID(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("create session for tenant %q: %w", tenantID, err)
	}

	if sa.MinioSecretKey == "" {
		return nil, fmt.Errorf("create session for tenant %q: missing MinIO secret key; service account credentials must be updated", tenantID)
	}
	slog.Debug("fetched tenant MinIO credentials",
		"tenant", tenantID,
		"has_access_key", sa.MinioAccessKey != "",
		"has_secret_key", sa.MinioSecretKey != "",
	)

	if sa.MinioSecretKey == "" {
		return nil, fmt.Errorf("create session for tenant %q: MinIO secret key is not set; tenant credentials may need to be rotated (run migration 009 or re-register tenant)", tenantID)
	}

	// Build tenant-specific MinIO config using stored credentials.
	tenantMinioCfg := config.MinIOConfig{
		Endpoint:     p.minioCfg.Endpoint,
		AccessKey:    sa.MinioAccessKey,
		SecretKey:    sa.MinioSecretKey,
		Bucket:       p.minioCfg.Bucket,
		UseSSL:       p.minioCfg.UseSSL,
		UsePathStyle: p.minioCfg.UsePathStyle,
	}

	conn, err := duckdb.OpenForTenant(ctx, tenantID, p.pgCfg, tenantMinioCfg, sa.S3Prefix, sa.PGSchema)
	if err != nil {
		return nil, fmt.Errorf("open duckdb for tenant %q: %w", tenantID, err)
	}

	now := time.Now()
	newSession := &Session{
		Conn:      conn,
		TenantID:  tenantID,
		S3Prefix:  sa.S3Prefix,
		PGSchema:  sa.PGSchema,
		CreatedAt: now,
		LastUsed:  now,
	}

	// Re-acquire lock to publish. Another goroutine may have already inserted
	// a session for the same tenant while we were creating ours.
	p.mu.Lock()
	if existing, ok := p.sessions[tenantID]; ok {
		// Another goroutine beat us; close our duplicate and use theirs.
		p.mu.Unlock()
		if err := conn.Close(); err != nil {
			slog.Warn("pool: error closing duplicate session", "tenant", tenantID, "err", err)
		}
		existing.LastUsed = time.Now()
		return existing, nil
	}
	p.sessions[tenantID] = newSession
	p.mu.Unlock()

	return newSession, nil
}

// Evict forcibly closes and removes a tenant's session.
func (p *Pool) Evict(tenantID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.evict(tenantID)
}

func (p *Pool) evict(tenantID string) {
	if s, ok := p.sessions[tenantID]; ok {
		if err := s.Conn.Close(); err != nil {
			slog.Warn("pool: error closing session", "tenant", tenantID, "err", err)
		}
		delete(p.sessions, tenantID)
		slog.Info("pool: evicted session", "tenant", tenantID)
	}
}

// evictLoop runs in the background and removes stale sessions.
func (p *Pool) evictLoop() {
	ticker := time.NewTicker(p.poolCfg.EvictionInterval)
	defer ticker.Stop()
	for range ticker.C {
		// Collect tenants to evict under lock, then close connections outside the lock
		// to avoid blocking Get/Evict operations during potentially slow Close calls.
		var toEvict []*Session
		p.mu.Lock()
		for id, s := range p.sessions {
			if time.Since(s.LastUsed) > p.poolCfg.SessionTTL {
				slog.Info("pool: evicting idle session", "tenant", id)
				toEvict = append(toEvict, s)
				delete(p.sessions, id)
			}
		}
		p.mu.Unlock()

		for _, s := range toEvict {
			if err := s.Conn.Close(); err != nil {
				slog.Warn("pool: error closing evicted session", "tenant", s.TenantID, "err", err)
			}
		}
	}
}

// Close shuts down the pool and all open sessions.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id := range p.sessions {
		p.evict(id)
	}
}
