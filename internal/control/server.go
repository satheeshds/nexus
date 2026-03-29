package control

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/satheeshds/nexus/internal/auth"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/tenant"
	"golang.org/x/crypto/bcrypt"
)

type Server struct {
	router      *chi.Mux
	provisioner *tenant.Provisioner
	catalog     *catalog.DB
	auth        *auth.Service
	adminAPIKey string
}

func NewServer(p *tenant.Provisioner, db *catalog.DB, a *auth.Service, adminAPIKey string) *Server {
	s := &Server{provisioner: p, catalog: db, auth: a, adminAPIKey: adminAPIKey}
	s.router = s.buildRouter()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func (s *Server) buildRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", s.handleHealth)

	r.Route("/api/v1", func(r chi.Router) {
		// Public
		r.Post("/register", s.handleRegister)
		r.Post("/login", s.handleLogin)

		// Authenticated
		r.Group(func(r chi.Router) {
			r.Use(s.jwtMiddleware)
			r.Get("/tenants/{id}", s.handleGetTenant)
			r.Delete("/tenants/{id}", s.handleDeleteTenant)
			r.Get("/tenants", s.handleListTenants)
		})

		// Admin endpoints
		r.Group(func(r chi.Router) {
			r.Use(s.adminMiddleware)
			r.Get("/admin/tenants/{id}/service-account", s.handleGetServiceAccount)
		})
	})

	return r
}

// ── Handlers ─────────────────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

type registerRequest struct {
	OrgName  string `json:"org_name"`
	Email    string `json:"email"`
	Password string `json:"password"` // Required: customer login password
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.OrgName == "" || req.Email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "org_name, email, and password are required")
		return
	}

	resp, err := s.provisioner.Register(r.Context(), tenant.RegisterRequest{
		OrgName:  req.OrgName,
		Email:    req.Email,
		Password: req.Password,
	})
	if err != nil {
		slog.Error("register tenant", "err", err)
		writeError(w, http.StatusInternalServerError, "provisioning failed")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"tenant_id": resp.TenantID,
		"token":     resp.Token,
		"pg_host":   "localhost",
		"pg_port":   "5433",
		"pg_user":   resp.TenantID,
	})
}

type loginRequest struct {
	TenantID string `json:"tenant_id"`
	Password string `json:"password"` // API key returned at registration
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.TenantID == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "tenant_id and password are required")
		return
	}

	t, err := s.catalog.GetTenant(r.Context(), req.TenantID)
	if err != nil {
		// Return 401 to avoid leaking whether the tenant ID exists.
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(t.APIKeyHash), []byte(req.Password)); err != nil {
		if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			writeError(w, http.StatusUnauthorized, "invalid credentials")
			return
		}
		slog.Error("bcrypt compare", "err", err)
		writeError(w, http.StatusInternalServerError, "authentication error")
		return
	}

	token, err := s.auth.Issue(t.ID, t.OrgName, t.S3Prefix, t.PGSchema)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not issue token")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

func (s *Server) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	t, err := s.catalog.GetTenant(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	// Hide service accounts from customer-facing API
	if t.AccountType == "service" {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) handleListTenants(w http.ResponseWriter, r *http.Request) {
	// Only list customer accounts (exclude service accounts)
	tenants, err := s.catalog.ListCustomerTenants(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list tenants")
		return
	}
	writeJSON(w, http.StatusOK, tenants)
}

func (s *Server) handleDeleteTenant(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.provisioner.Delete(r.Context(), id); err != nil {
		slog.Error("delete tenant", "err", err)
		writeError(w, http.StatusInternalServerError, "deprovision failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ── Middleware ────────────────────────────────────────────────────────────────

func (s *Server) jwtMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if len(authHeader) < 8 || authHeader[:7] != "Bearer " {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		if _, err := s.auth.Validate(authHeader[7:]); err != nil {
			writeError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) adminMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-Admin-API-Key")
		if apiKey == "" {
			writeError(w, http.StatusUnauthorized, "missing admin API key")
			return
		}
		if apiKey != s.adminAPIKey {
			writeError(w, http.StatusUnauthorized, "invalid admin API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ── Admin Handlers ────────────────────────────────────────────────────────────

func (s *Server) handleGetServiceAccount(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "id")
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant ID is required")
		return
	}

	// Get the customer tenant
	customerTenant, err := s.catalog.GetTenant(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}

	// Ensure it's a customer account
	if customerTenant.AccountType != "customer" {
		writeError(w, http.StatusBadRequest, "service accounts cannot have service accounts")
		return
	}

	// Retrieve the associated service account
	serviceID := tenantID + "_svc"
	serviceAccount, err := s.catalog.GetTenant(r.Context(), serviceID)
	if err != nil {
		writeError(w, http.StatusNotFound, "service account not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"tenant_id":      tenantID,
		"service_id":     serviceAccount.ID,
		"s3_prefix":      serviceAccount.S3Prefix,
		"pg_schema":      serviceAccount.PGSchema,
		"note":           "API key hash is stored securely; plain key was shown once during registration",
	})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
