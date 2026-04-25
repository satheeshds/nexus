package control

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/satheeshds/nexus/internal/auth"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/tenant"
	"golang.org/x/crypto/bcrypt"

	_ "github.com/satheeshds/nexus/docs"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

// CatalogStore is the subset of catalog.DB operations used by the control server.
type CatalogStore interface {
	GetTenantByEmail(ctx context.Context, email string) (*catalog.Tenant, error)
	GetTenant(ctx context.Context, id string) (*catalog.Tenant, error)
	ListTenants(ctx context.Context) ([]catalog.Tenant, error)
	GetServiceAccountByTenantID(ctx context.Context, tenantID string) (*catalog.ServiceAccount, error)
}

// TenantProvisioner is the subset of tenant.Provisioner operations used by the control server.
type TenantProvisioner interface {
	Register(ctx context.Context, req tenant.RegisterRequest) (*tenant.RegisterResponse, error)
	Delete(ctx context.Context, tenantID string) error
	RotateServiceAccountKey(ctx context.Context, tenantID string) (string, string, error)
}
// TenantQueryRunner executes SQL statements against individual tenant DuckDB sessions.
// Implement this interface (e.g. with *pool.Pool) to enable the admin query endpoint.
type TenantQueryRunner interface {
	ExecForTenant(ctx context.Context, tenantID, query string) (int64, error)
}

type Server struct {
	router      *chi.Mux
	provisioner TenantProvisioner
	catalog     CatalogStore
	auth        *auth.Service
	adminAPIKey string
	queryRunner TenantQueryRunner

	// listTenantsFunc is used in tests to override the CatalogStore.ListTenants call.
	// If nil, the injected CatalogStore is used directly.
	listTenantsFunc func(ctx context.Context) ([]catalog.Tenant, error)
}

func NewServer(p TenantProvisioner, db CatalogStore, a *auth.Service, adminAPIKey string, qr TenantQueryRunner) *Server {
	if qr == nil {
		panic("control.NewServer: TenantQueryRunner must not be nil")
	}
	s := &Server{provisioner: p, catalog: db, auth: a, adminAPIKey: adminAPIKey, queryRunner: qr}
	s.router = s.buildRouter()
	return s
}

// listTenants returns all tenants, using listTenantsFunc when set (tests only).
func (s *Server) listTenants(ctx context.Context) ([]catalog.Tenant, error) {
	if s.listTenantsFunc != nil {
		return s.listTenantsFunc(ctx)
	}
	return s.catalog.ListTenants(ctx)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func (s *Server) buildRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	// Health check – no request/response logging to avoid noise.
	r.Get("/healthz", s.handleHealth)

	// All other routes are logged.
	r.Group(func(r chi.Router) {
		r.Use(middleware.Logger)

		r.Group(func(r chi.Router) {
			r.Use(s.adminMiddleware)
			r.Get("/swagger/*", httpSwagger.Handler(httpSwagger.URL("/swagger/doc.json")))
		})

		r.Route("/api/v1", func(r chi.Router) {
			// Public
			r.Post("/register", s.handleRegister)
			r.Post("/login", s.handleLogin)

			// Authenticated (JWT) - currently nothing here as tenant info moved to admin
			r.Group(func(r chi.Router) {
				r.Use(s.jwtMiddleware)
			})

			// Admin endpoints (X-Admin-API-Key)
			r.Group(func(r chi.Router) {
				r.Use(s.adminMiddleware)
				r.Get("/admin/tenants", s.handleListTenants)
				r.Get("/admin/tenants/{id}", s.handleGetTenant)
				r.Delete("/admin/tenants/{id}", s.handleDeleteTenant)
				r.Get("/admin/tenants/{id}/service-account", s.handleGetServiceAccount)
				r.Post("/admin/tenants/{id}/service-account/rotate", s.handleRotateServiceAccountKey)
				r.Post("/admin/query", s.handleAdminQuery)
			})
		})
	})

	return r
}

// ── Handlers ─────────────────────────────────────────────────────────────────

// handleHealth godoc
// @Summary Health check
// @Description returns status ok if server is running
// @Tags health
// @Produce json
// @Success 200 {object} map[string]string
// @Router /healthz [get]
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

type registerRequest struct {
	OrgName  string `json:"org_name"`
	Email    string `json:"email"`
	Password string `json:"password"` // Required: customer login password
}

// handleRegister godoc
// @Summary Register a new tenant
// @Description Provision a new customer tenant and its associated database schema and storage namespace.
// @Tags tenants
// @Accept json
// @Produce json
// @Param body body registerRequest true "Registration data"
// @Success 201 {object} map[string]string
// @Failure 400 {object} map[string]string
// @Failure 409 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /api/v1/register [post]
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

	// Check if email is already registered.
	if _, err := s.catalog.GetTenantByEmail(r.Context(), req.Email); err == nil {
		writeError(w, http.StatusConflict, "email already registered")
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
	})
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"` // Customer login password (same value used during registration)
}

// handleLogin godoc
// @Summary Login as a tenant
// @Description Authenticate with email and password to receive a JWT token for further requests.
// @Tags identity
// @Accept json
// @Produce json
// @Param body body loginRequest true "Login credentials"
// @Success 200 {object} map[string]string
// @Failure 400 {object} map[string]string
// @Failure 401 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /api/v1/login [post]
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "email and password are required")
		return
	}

	t, err := s.catalog.GetTenantByEmail(r.Context(), req.Email)
	if err != nil {
		// Return 401 to avoid leaking whether the email exists.
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(t.PasswordHash), []byte(req.Password)); err != nil {
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

// handleGetTenant godoc
// @Summary Get tenant details
// @Description Retrieves details for a specific tenant by ID. Requires admin API key authentication.
// @Tags tenants
// @Produce json
// @Param id path string true "Tenant ID"
// @Success 200 {object} catalog.Tenant
// @Failure 401 {object} map[string]string
// @Failure 404 {object} map[string]string
// @Security AdminAuth
// @Router /api/v1/admin/tenants/{id} [get]
func (s *Server) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	t, err := s.catalog.GetTenant(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// handleListTenants godoc
// @Summary List all tenants
// @Description Returns a list of all registered tenants. Requires admin API key authentication.
// @Tags tenants
// @Produce json
// @Success 200 {array} catalog.Tenant
// @Failure 401 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Security AdminAuth
// @Router /api/v1/admin/tenants [get]
func (s *Server) handleListTenants(w http.ResponseWriter, r *http.Request) {
	tenants, err := s.catalog.ListTenants(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list tenants")
		return
	}
	writeJSON(w, http.StatusOK, tenants)
}

// handleDeleteTenant godoc
// @Summary Delete a tenant
// @Description Deprovisions a tenant and removes its storage bucket and database schema. Requires admin authentication.
// @Tags tenants
// @Param id path string true "Tenant ID"
// @Success 204
// @Failure 401 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Security AdminAuth
// @Router /api/v1/admin/tenants/{id} [delete]
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

// handleGetServiceAccount godoc
// @Summary Get service account details (Admin only)
// @Description Retrieves the service account details for a tenant. Requires Admin API Key.
// @Tags admin
// @Produce json
// @Param id path string true "Tenant ID"
// @Success 200 {object} map[string]string
// @Failure 401 {object} map[string]string
// @Failure 404 {object} map[string]string
// @Security AdminAuth
// @Router /api/v1/admin/tenants/{id}/service-account [get]
func (s *Server) handleGetServiceAccount(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "id")
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant ID is required")
		return
	}

	// Ensure the customer tenant exists
	if _, err := s.catalog.GetTenant(r.Context(), tenantID); err != nil {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}

	// Retrieve the associated service account from the dedicated table
	svcAccount, err := s.catalog.GetServiceAccountByTenantID(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusNotFound, "service account not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"tenant_id":  tenantID,
		"service_id": svcAccount.ID,
		"s3_prefix":  svcAccount.S3Prefix,
		"pg_schema":  svcAccount.PGSchema,
		"note":       "Use the service-account key rotation endpoint to obtain or refresh the plain API key.",
	})
}

func (s *Server) handleRotateServiceAccountKey(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "id")
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenant ID is required")
		return
	}

	// Ensure the customer tenant exists, for consistency with handleGetServiceAccount.
	if _, err := s.catalog.GetTenant(r.Context(), tenantID); err != nil {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	newKey, serviceID, err := s.provisioner.RotateServiceAccountKey(r.Context(), tenantID)
	if err != nil {
		slog.Error("rotate service account key", "tenant", tenantID, "err", err)
		if errors.Is(err, catalog.ErrNotFound) {
			writeError(w, http.StatusNotFound, "service account not found")
		} else {
			writeError(w, http.StatusInternalServerError, "internal server error")
		}
		return
	}

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	writeJSON(w, http.StatusOK, map[string]string{
		"tenant_id":       tenantID,
		"service_id":      serviceID,
		"service_api_key": newKey,
	})
}

type adminQueryRequest struct {
	Query string `json:"query"`
}

// tenantQueryResult reports the outcome of running the admin query against a single tenant.
type tenantQueryResult struct {
	TenantID     string `json:"tenant_id"`
	OrgName      string `json:"org_name"`
	Success      bool   `json:"success"`
	RowsAffected int64  `json:"rows_affected,omitempty"`
	Error        string `json:"error,omitempty"`
}

type adminQueryResponse struct {
	Results []tenantQueryResult `json:"results"`
}

// handleAdminQuery godoc
// @Summary Run a SQL query across all tenants (Admin only)
// @Description Executes the provided SQL statement against every tenant's DuckDB session.
// @Description Intended for migrations and other administrative bulk operations.
// @Tags admin
// @Accept json
// @Produce json
// @Param body body adminQueryRequest true "SQL query to execute"
// @Success 200 {object} adminQueryResponse
// @Failure 400 {object} map[string]string
// @Failure 401 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Security AdminAuth
// @Router /api/v1/admin/query [post]
func (s *Server) handleAdminQuery(w http.ResponseWriter, r *http.Request) {
	var req adminQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	query := strings.TrimSpace(req.Query)
	if query == "" {
		writeError(w, http.StatusBadRequest, "query is required")
		return
	}

	tenants, err := s.listTenants(r.Context())
	if err != nil {
		slog.Error("admin query: list tenants", "err", err)
		writeError(w, http.StatusInternalServerError, "could not list tenants")
		return
	}

	results := make([]tenantQueryResult, 0, len(tenants))
	for _, t := range tenants {
		res := tenantQueryResult{TenantID: t.ID, OrgName: t.OrgName}
		rowsAffected, execErr := s.queryRunner.ExecForTenant(r.Context(), t.ID, query)
		if execErr != nil {
			slog.Warn("admin query: exec failed", "tenant", t.ID, "err", execErr)
			res.Success = false
			res.Error = execErr.Error()
		} else {
			res.Success = true
			res.RowsAffected = rowsAffected
		}
		results = append(results, res)
	}

	writeJSON(w, http.StatusOK, adminQueryResponse{Results: results})
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
