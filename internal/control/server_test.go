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
