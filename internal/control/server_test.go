package control_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/satheeshds/nexus/internal/auth"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/control"
	"github.com/satheeshds/nexus/internal/tenant"
	"golang.org/x/crypto/bcrypt"
  
)

// ── Mocks ────────────────────────────────────────────────────────────────────

type mockCatalog struct {
	tenants         map[string]*catalog.Tenant
	serviceAccounts map[string]*catalog.ServiceAccount // keyed by tenantID
}

func newMockCatalog() *mockCatalog {
	return &mockCatalog{
		tenants:         make(map[string]*catalog.Tenant),
		serviceAccounts: make(map[string]*catalog.ServiceAccount),
	}
}

func (m *mockCatalog) addTenant(t *catalog.Tenant) {
	m.tenants[t.ID] = t
}

func (m *mockCatalog) addServiceAccount(sa *catalog.ServiceAccount) {
	m.serviceAccounts[sa.TenantID] = sa
}

func (m *mockCatalog) GetTenantByEmail(_ context.Context, email string) (*catalog.Tenant, error) {
	for _, t := range m.tenants {
		if t.Email == email {
			return t, nil
		}
	}
	return nil, catalog.ErrNotFound
}

func (m *mockCatalog) GetTenant(_ context.Context, id string) (*catalog.Tenant, error) {
	t, ok := m.tenants[id]
	if !ok {
		return nil, catalog.ErrNotFound
	}
	return t, nil
}

func (m *mockCatalog) ListTenants(_ context.Context) ([]catalog.Tenant, error) {
	result := make([]catalog.Tenant, 0, len(m.tenants))
	for _, t := range m.tenants {
		result = append(result, *t)
	}
	return result, nil
}

func (m *mockCatalog) GetServiceAccountByTenantID(_ context.Context, tenantID string) (*catalog.ServiceAccount, error) {
	sa, ok := m.serviceAccounts[tenantID]
	if !ok {
		return nil, catalog.ErrNotFound
	}
	return sa, nil
}

type mockProvisioner struct {
	registerFn func(ctx context.Context, req tenant.RegisterRequest) (*tenant.RegisterResponse, error)
	deleteFn   func(ctx context.Context, tenantID string) error
	rotateFn   func(ctx context.Context, tenantID string) (string, string, error)
}

func (m *mockProvisioner) Register(ctx context.Context, req tenant.RegisterRequest) (*tenant.RegisterResponse, error) {
	if m.registerFn != nil {
		return m.registerFn(ctx, req)
	}
	return &tenant.RegisterResponse{TenantID: "test-tenant"}, nil
}

func (m *mockProvisioner) Delete(ctx context.Context, tenantID string) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, tenantID)
	}
	return nil
}

func (m *mockProvisioner) RotateServiceAccountKey(ctx context.Context, tenantID string) (string, string, error) {
	if m.rotateFn != nil {
		return m.rotateFn(ctx, tenantID)
	}
	return "newkey", "svc-id", nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func newTestServer(t *testing.T, cat control.CatalogStore, prov control.TenantProvisioner) http.Handler {
	t.Helper()
	authSvc := auth.NewService("test-secret", time.Hour)
	return control.NewServer(prov, cat, authSvc, "admin-key")
}

func mustHash(password string) string {
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		panic(err)
	}
	return string(h)
}

// ── Tests ────────────────────────────────────────────────────────────────────

func TestHandleHealth(t *testing.T) {
	cat := newMockCatalog()
	srv := newTestServer(t, cat, &mockProvisioner{})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "ok") {
		t.Errorf("body = %q, want to contain %q", body, "ok")
	}
}

func TestHandleRegister_Success(t *testing.T) {
	cat := newMockCatalog()
	prov := &mockProvisioner{
		registerFn: func(_ context.Context, req tenant.RegisterRequest) (*tenant.RegisterResponse, error) {
			return &tenant.RegisterResponse{TenantID: "acme_corp_abc123"}, nil
		},
	}
	srv := newTestServer(t, cat, prov)

	body := `{"org_name":"Acme Corp","email":"admin@acme.com","password":"secret123"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d; body = %s", rr.Code, http.StatusCreated, rr.Body.String())
	}
	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["tenant_id"] != "acme_corp_abc123" {
		t.Errorf("tenant_id = %q, want %q", resp["tenant_id"], "acme_corp_abc123")
	}
}

func TestHandleRegister_MissingFields(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"missing org_name", `{"email":"e@x.com","password":"secret"}`},
		{"missing email", `{"org_name":"Org","password":"secret"}`},
		{"missing password", `{"org_name":"Org","email":"e@x.com"}`},
		{"empty body", `{}`},
	}

	cat := newMockCatalog()
	srv := newTestServer(t, cat, &mockProvisioner{})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/register", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", rr.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestHandleRegister_DuplicateEmail(t *testing.T) {
	cat := newMockCatalog()
	cat.addTenant(&catalog.Tenant{
		ID: "existing", Email: "admin@acme.com",
	})
	srv := newTestServer(t, cat, &mockProvisioner{})

	body := `{"org_name":"Acme Corp","email":"admin@acme.com","password":"secret123"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusConflict)
	}
}

func TestHandleRegister_ProvisioningError(t *testing.T) {
	cat := newMockCatalog()
	prov := &mockProvisioner{
		registerFn: func(_ context.Context, _ tenant.RegisterRequest) (*tenant.RegisterResponse, error) {
			return nil, errors.New("minio unavailable")
		},
	}
	srv := newTestServer(t, cat, prov)

	body := `{"org_name":"Acme Corp","email":"new@acme.com","password":"secret123"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
}

func TestHandleRegister_InvalidJSON(t *testing.T) {
	cat := newMockCatalog()
	srv := newTestServer(t, cat, &mockProvisioner{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/register", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestHandleLogin_Success(t *testing.T) {
	passwordHash := mustHash("password123")
	cat := newMockCatalog()
	cat.addTenant(&catalog.Tenant{
		ID:           "tenant-1",
		OrgName:      "Acme Corp",
		Email:        "admin@acme.com",
		S3Prefix:     "tenants/acme",
		PGSchema:     "ducklake_acme",
		PasswordHash: passwordHash,
		CreatedAt:    time.Now(),
	})
	srv := newTestServer(t, cat, &mockProvisioner{})

	body := `{"email":"admin@acme.com","password":"password123"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d; body = %s", rr.Code, http.StatusOK, rr.Body.String())
	}
	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["token"] == "" {
		t.Error("expected non-empty token in response")
	}
}

func TestHandleLogin_WrongPassword(t *testing.T) {
	passwordHash := mustHash("correct-password")
	cat := newMockCatalog()
	cat.addTenant(&catalog.Tenant{
		ID: "tenant-1", Email: "admin@acme.com", PasswordHash: passwordHash,
	})
	srv := newTestServer(t, cat, &mockProvisioner{})

	body := `{"email":"admin@acme.com","password":"wrong-password"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusUnauthorized)
	}
}

func TestHandleLogin_UnknownEmail(t *testing.T) {
	cat := newMockCatalog()
	srv := newTestServer(t, cat, &mockProvisioner{})

	body := `{"email":"unknown@example.com","password":"any"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusUnauthorized)
	}
}

func TestHandleLogin_MissingFields(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"missing email", `{"password":"secret"}`},
		{"missing password", `{"email":"e@x.com"}`},
	}

	cat := newMockCatalog()
	srv := newTestServer(t, cat, &mockProvisioner{})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/login", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			srv.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", rr.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestAdminMiddleware_MissingKey(t *testing.T) {
	cat := newMockCatalog()
	srv := newTestServer(t, cat, &mockProvisioner{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusUnauthorized)
	}
}

func TestAdminMiddleware_WrongKey(t *testing.T) {
	cat := newMockCatalog()
	srv := newTestServer(t, cat, &mockProvisioner{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants", nil)
	req.Header.Set("X-Admin-API-Key", "wrong-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusUnauthorized)
	}
}

func TestHandleListTenants_Success(t *testing.T) {
	cat := newMockCatalog()
	cat.addTenant(&catalog.Tenant{ID: "t1", OrgName: "Org1", Email: "a@b.com"})
	cat.addTenant(&catalog.Tenant{ID: "t2", OrgName: "Org2", Email: "c@d.com"})
	srv := newTestServer(t, cat, &mockProvisioner{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants", nil)
	req.Header.Set("X-Admin-API-Key", "admin-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d; body = %s", rr.Code, http.StatusOK, rr.Body.String())
	}
	var tenants []map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&tenants); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(tenants) != 2 {
		t.Errorf("got %d tenants, want 2", len(tenants))
	}
}

func TestHandleGetTenant_Success(t *testing.T) {
	cat := newMockCatalog()
	cat.addTenant(&catalog.Tenant{
		ID: "tenant-1", OrgName: "Acme Corp", Email: "a@acme.com",
	})
	srv := newTestServer(t, cat, &mockProvisioner{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/tenant-1", nil)
	req.Header.Set("X-Admin-API-Key", "admin-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d; body = %s", rr.Code, http.StatusOK, rr.Body.String())
	}
}

func TestHandleGetTenant_NotFound(t *testing.T) {
	cat := newMockCatalog()
	srv := newTestServer(t, cat, &mockProvisioner{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/no-such-id", nil)
	req.Header.Set("X-Admin-API-Key", "admin-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestHandleDeleteTenant_Success(t *testing.T) {
	cat := newMockCatalog()
	cat.addTenant(&catalog.Tenant{ID: "tenant-1"})
	prov := &mockProvisioner{
		deleteFn: func(_ context.Context, tenantID string) error {
			if tenantID != "tenant-1" {
				return errors.New("unknown tenant")
			}
			return nil
		},
	}
	srv := newTestServer(t, cat, prov)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/tenants/tenant-1", nil)
	req.Header.Set("X-Admin-API-Key", "admin-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d; body = %s", rr.Code, http.StatusNoContent, rr.Body.String())
	}
}

func TestHandleDeleteTenant_Error(t *testing.T) {
	cat := newMockCatalog()
	prov := &mockProvisioner{
		deleteFn: func(_ context.Context, _ string) error {
			return errors.New("storage failure")
		},
	}
	srv := newTestServer(t, cat, prov)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/tenants/tenant-1", nil)
	req.Header.Set("X-Admin-API-Key", "admin-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
}

func TestHandleGetServiceAccount_Success(t *testing.T) {
	cat := newMockCatalog()
	cat.addTenant(&catalog.Tenant{ID: "tenant-1"})
	cat.addServiceAccount(&catalog.ServiceAccount{
		ID:       "tenant-1_svc",
		TenantID: "tenant-1",
		S3Prefix: "tenants/tenant-1",
		PGSchema: "ducklake_tenant_1",
	})
	srv := newTestServer(t, cat, &mockProvisioner{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/tenant-1/service-account", nil)
	req.Header.Set("X-Admin-API-Key", "admin-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d; body = %s", rr.Code, http.StatusOK, rr.Body.String())
	}
	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["tenant_id"] != "tenant-1" {
		t.Errorf("tenant_id = %q, want %q", resp["tenant_id"], "tenant-1")
	}
	if resp["service_id"] != "tenant-1_svc" {
		t.Errorf("service_id = %q, want %q", resp["service_id"], "tenant-1_svc")
	}
}

func TestHandleGetServiceAccount_TenantNotFound(t *testing.T) {
	cat := newMockCatalog()
	srv := newTestServer(t, cat, &mockProvisioner{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/no-such/service-account", nil)
	req.Header.Set("X-Admin-API-Key", "admin-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestHandleGetServiceAccount_ServiceAccountNotFound(t *testing.T) {
	cat := newMockCatalog()
	cat.addTenant(&catalog.Tenant{ID: "tenant-1"})
	srv := newTestServer(t, cat, &mockProvisioner{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants/tenant-1/service-account", nil)
	req.Header.Set("X-Admin-API-Key", "admin-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestHandleRotateServiceAccountKey_Success(t *testing.T) {
	cat := newMockCatalog()
	cat.addTenant(&catalog.Tenant{ID: "tenant-1"})
	prov := &mockProvisioner{
		rotateFn: func(_ context.Context, tenantID string) (string, string, error) {
			return "newplainkey", "tenant-1_svc", nil
		},
	}
	srv := newTestServer(t, cat, prov)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/tenant-1/service-account/rotate", bytes.NewReader(nil))
	req.Header.Set("X-Admin-API-Key", "admin-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d; body = %s", rr.Code, http.StatusOK, rr.Body.String())
	}
	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["service_api_key"] != "newplainkey" {
		t.Errorf("service_api_key = %q, want %q", resp["service_api_key"], "newplainkey")
	}
	if rr.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control header = %q, want %q", rr.Header().Get("Cache-Control"), "no-store")
	}
}

func TestHandleRotateServiceAccountKey_TenantNotFound(t *testing.T) {
	cat := newMockCatalog()
	prov := &mockProvisioner{}
	srv := newTestServer(t, cat, prov)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/no-such/service-account/rotate", nil)
	req.Header.Set("X-Admin-API-Key", "admin-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestHandleRotateServiceAccountKey_ServiceAccountNotFound(t *testing.T) {
	cat := newMockCatalog()
	cat.addTenant(&catalog.Tenant{ID: "tenant-1"})
	prov := &mockProvisioner{
		rotateFn: func(_ context.Context, _ string) (string, string, error) {
			return "", "", catalog.ErrNotFound
		},
	}
	srv := newTestServer(t, cat, prov)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants/tenant-1/service-account/rotate", nil)
	req.Header.Set("X-Admin-API-Key", "admin-key")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNotFound)
  }
}


// ── Stubs ─────────────────────────────────────────────────────────────────────

// stubQueryRunner records the tenants it was asked to run the query against.
type stubQueryRunner struct {
	called       []string // tenant IDs that were queried
	rowsAffected int64
	failFor      string // if non-empty, return an error for this tenant ID
}

func (r *stubQueryRunner) ExecForTenant(_ context.Context, tenantID, _ string) (int64, error) {
	r.called = append(r.called, tenantID)
	if tenantID == r.failFor {
		return 0, stubError("stub exec error")
	}
	return r.rowsAffected, nil
}

type stubError string

func (e stubError) Error() string { return string(e) }

// newAdminQueryServer builds a minimal Server wired for the admin query tests.
// It injects a pre-built tenant list via listTenantsFunc so that no real
// Postgres connection is required.
func newAdminQueryServer(tenants []catalog.Tenant, qr TenantQueryRunner) *Server {
	s := &Server{
		adminAPIKey: "secret",
		queryRunner: qr,
		listTenantsFunc: func(_ context.Context) ([]catalog.Tenant, error) {
			return tenants, nil
		},
	}
	s.router = s.buildRouter()
	return s
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestHandleAdminQuery_MissingKey(t *testing.T) {
	s := newAdminQueryServer(nil, &stubQueryRunner{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/query",
		bytes.NewBufferString(`{"query":"SELECT 1"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandleAdminQuery_WrongKey(t *testing.T) {
	s := newAdminQueryServer(nil, &stubQueryRunner{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/query",
		bytes.NewBufferString(`{"query":"SELECT 1"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-API-Key", "wrong")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandleAdminQuery_EmptyQuery(t *testing.T) {
	s := newAdminQueryServer(nil, &stubQueryRunner{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/query",
		bytes.NewBufferString(`{"query":""}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-API-Key", "secret")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleAdminQuery_WhitespaceQuery(t *testing.T) {
	s := newAdminQueryServer(nil, &stubQueryRunner{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/query",
		bytes.NewBufferString(`{"query":"   "}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-API-Key", "secret")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleAdminQuery_Success(t *testing.T) {
	tenants := []catalog.Tenant{
		{ID: "t1", OrgName: "Acme", Email: "a@acme.com", CreatedAt: time.Now()},
		{ID: "t2", OrgName: "Globex", Email: "b@globex.com", CreatedAt: time.Now()},
	}
	runner := &stubQueryRunner{rowsAffected: 3}
	s := newAdminQueryServer(tenants, runner)

	body := `{"query":"ALTER TABLE foo ADD COLUMN bar INT"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/query",
		bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-API-Key", "secret")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp adminQueryResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(resp.Results))
	}
	for _, r := range resp.Results {
		if !r.Success {
			t.Errorf("tenant %s should succeed, got error: %s", r.TenantID, r.Error)
		}
		if r.RowsAffected != 3 {
			t.Errorf("tenant %s: expected rows_affected=3, got %d", r.TenantID, r.RowsAffected)
		}
	}
	if len(runner.called) != 2 {
		t.Fatalf("expected runner called for 2 tenants, got %v", runner.called)
	}
}

// TestHandleAdminQuery_PartialFailure verifies that a per-tenant execution
// error is captured in the result without aborting the remaining tenants.
func TestHandleAdminQuery_PartialFailure(t *testing.T) {
	tenants := []catalog.Tenant{
		{ID: "t1", OrgName: "Acme", CreatedAt: time.Now()},
		{ID: "t2", OrgName: "Globex", CreatedAt: time.Now()},
		{ID: "t3", OrgName: "Initech", CreatedAt: time.Now()},
	}
	runner := &stubQueryRunner{rowsAffected: 1, failFor: "t2"}
	s := newAdminQueryServer(tenants, runner)

	body := `{"query":"UPDATE foo SET bar = 1"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/query",
		bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-API-Key", "secret")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp adminQueryResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(resp.Results))
	}

	byID := map[string]tenantQueryResult{}
	for _, r := range resp.Results {
		byID[r.TenantID] = r
	}

	if !byID["t1"].Success {
		t.Error("t1 should succeed")
	}
	if byID["t2"].Success {
		t.Error("t2 should fail")
	}
	if byID["t2"].Error == "" {
		t.Error("t2 should have a non-empty error message")
	}
	if !byID["t3"].Success {
		t.Error("t3 should succeed")
	}
	if len(runner.called) != 3 {
		t.Errorf("expected runner called for all 3 tenants, got %v", runner.called)
	}
}
