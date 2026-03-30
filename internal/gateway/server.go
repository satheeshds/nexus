package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	_ "github.com/satheeshds/nexus/docs/gateway"
	httpSwagger "github.com/swaggo/http-swagger/v2"

	"github.com/jackc/pgproto3/v2"
	"github.com/satheeshds/nexus/internal/auth"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/pool"
)

// Server listens for incoming Postgres wire connections and routes them
// to per-tenant DuckDB sessions.
type Server struct {
	addr     string
	httpAddr string
	pool     *pool.Pool
	auth     *auth.Service
	catalog  *catalog.DB
}

func NewServer(addr string, httpAddr string, p *pool.Pool, a *auth.Service, db *catalog.DB) *Server {
	return &Server{addr: addr, httpAddr: httpAddr, pool: p, auth: a, catalog: db}
}

// ListenAndServe starts the TCP listener.
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.addr, err)
	}
	defer ln.Close()

	slog.Info("gateway listening", "pg_addr", s.addr, "http_addr", s.httpAddr)

	httpSrv := &http.Server{
		Addr:    s.httpAddr,
		Handler: s.buildHTTPRouter(),
	}

	go func() {
		slog.Debug("gateway http starting", "addr", s.httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("gateway http server error", "err", err)
		}
	}()

	go func() {
		<-ctx.Done()
		ln.Close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
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

	var sm *pgproto3.StartupMessage

	switch msg := startupMsg.(type) {
	case *pgproto3.StartupMessage:
		sm = msg
	case *pgproto3.SSLRequest:
		// SSL is not supported; respond with 'N' as per PostgreSQL protocol
		if _, err := conn.Write([]byte("N")); err != nil {
			slog.Warn("gateway: failed to write SSL denial", "err", err)
			return
		}

		// Read the real startup message after SSL negotiation attempt
		startupMsg, err = backend.ReceiveStartupMessage()
		if err != nil {
			slog.Warn("gateway: startup error after SSLRequest", "err", err)
			return
		}

		var ok bool
		sm, ok = startupMsg.(*pgproto3.StartupMessage)
		if !ok {
			slog.Warn("gateway: unexpected startup message type after SSLRequest")
			return
		}
	default:
		// Cancel or other unexpected requests — not supported yet
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

func (s *Server) buildHTTPRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/healthz", s.handleHealth)
	r.Get("/swagger/*", httpSwagger.Handler(httpSwagger.URL("/swagger/doc.json")))
	return r
}

// handleHealth godoc
// @Summary Gateway health check
// @Description returns status ok if gateway is running
// @Tags health
// @Produce json
// @Success 200 {object} map[string]string
// @Router /healthz [get]
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
