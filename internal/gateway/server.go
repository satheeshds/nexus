package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/jackc/pgproto3/v2"
	"github.com/satheeshds/nexus/internal/auth"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/pool"
)

// Server listens for incoming Postgres wire connections and routes them
// to per-tenant DuckDB sessions.
type Server struct {
	addr    string
	pool    *pool.Pool
	auth    *auth.Service
	catalog *catalog.DB
}

func NewServer(addr string, p *pool.Pool, a *auth.Service, db *catalog.DB) *Server {
	return &Server{addr: addr, pool: p, auth: a, catalog: db}
}

// ListenAndServe starts the TCP listener.
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.addr, err)
	}
	defer ln.Close()

	slog.Info("gateway listening", "addr", s.addr)

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				slog.Warn("gateway: accept error", "err", err)
				continue
			}
		}
		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	// ── 1. Startup ───────────────────────────────────────────────────────────
	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil {
		slog.Warn("gateway: startup error", "err", err)
		return
	}

	sm, ok := startupMsg.(*pgproto3.StartupMessage)
	if !ok {
		// SSL or cancel requests — not supported yet
		slog.Warn("gateway: unexpected startup message type")
		return
	}

	tenantID := sm.Parameters["user"]
	slog.Info("gateway: new connection", "tenant", tenantID)

	// ── 2. Auth ───────────────────────────────────────────────────────────────
	// Ask for password (= JWT token)
	if err := backend.Send(&pgproto3.AuthenticationCleartextPassword{}); err != nil {
		slog.Warn("gateway: send auth request", "err", err)
		return
	}

	msg, err := backend.Receive()
	if err != nil {
		slog.Warn("gateway: receive password", "err", err)
		return
	}

	pwdMsg, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		_ = sendError(backend, "invalid auth message")
		return
	}

	claims, err := s.auth.Validate(pwdMsg.Password)
	if err != nil || claims.TenantID != tenantID {
		_ = sendError(backend, "authentication failed")
		return
	}

	// ── 3. Auth OK ────────────────────────────────────────────────────────────
	if err := backend.Send(&pgproto3.AuthenticationOk{}); err != nil {
		return
	}
	// Send server parameters expected by Postgres clients
	for _, kv := range [][2]string{
		{"server_version", "14.0"},
		{"client_encoding", "UTF8"},
		{"DateStyle", "ISO, MDY"},
	} {
		_ = backend.Send(&pgproto3.ParameterStatus{Name: kv[0], Value: kv[1]})
	}
	_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	// ── 4. Get/create tenant DuckDB session ──────────────────────────────────
	session, err := s.pool.Get(ctx, claims.TenantID, claims.S3Prefix, claims.PGSchema)
	if err != nil {
		_ = sendError(backend, fmt.Sprintf("could not open session: %v", err))
		return
	}

	// ── 5. Query loop ─────────────────────────────────────────────────────────
	h := &handler{backend: backend, session: session}
	h.run(ctx)
}

func sendError(backend *pgproto3.Backend, msg string) error {
	return backend.Send(&pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     "08006",
		Message:  msg,
	})
}
