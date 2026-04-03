package control

import (
"bytes"
"context"
"encoding/json"
"net/http"
"net/http/httptest"
"testing"
"time"

"github.com/satheeshds/nexus/internal/catalog"
)

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
