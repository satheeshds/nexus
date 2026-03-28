package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

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

	pgCfg    config.PostgresConfig
	minioCfg config.MinIOConfig
	poolCfg  config.PoolConfig
}

func New(pgCfg config.PostgresConfig, minioCfg config.MinIOConfig, poolCfg config.PoolConfig) *Pool {
	p := &Pool{
		sessions: make(map[string]*Session),
		pgCfg:    pgCfg,
		minioCfg: minioCfg,
		poolCfg:  poolCfg,
	}
	go p.evictLoop()
	return p
}

// Get returns an existing session for the tenant, or creates a new one.
func (p *Pool) Get(ctx context.Context, tenantID, s3Prefix, pgSchema string) (*Session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if s, ok := p.sessions[tenantID]; ok {
		s.LastUsed = time.Now()
		slog.Debug("pool: reusing session", "tenant", tenantID)
		return s, nil
	}

	slog.Info("pool: creating new session", "tenant", tenantID)
	conn, err := duckdb.OpenForTenant(ctx, tenantID, p.pgCfg, p.minioCfg, s3Prefix, pgSchema)
	if err != nil {
		return nil, fmt.Errorf("open duckdb for tenant %q: %w", tenantID, err)
	}

	now := time.Now()
	s := &Session{
		Conn:      conn,
		TenantID:  tenantID,
		S3Prefix:  s3Prefix,
		PGSchema:  pgSchema,
		CreatedAt: now,
		LastUsed:  now,
	}
	p.sessions[tenantID] = s
	return s, nil
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
		p.mu.Lock()
		for id, s := range p.sessions {
			if time.Since(s.LastUsed) > p.poolCfg.SessionTTL {
				slog.Info("pool: evicting idle session", "tenant", id)
				p.evict(id)
			}
		}
		p.mu.Unlock()
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
