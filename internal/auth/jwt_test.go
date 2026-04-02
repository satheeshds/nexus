package auth_test

import (
	"testing"
	"time"

	"github.com/satheeshds/nexus/internal/auth"
)

func TestIssueAndValidate(t *testing.T) {
	svc := auth.NewService("test-secret-key-that-is-long-enough", time.Hour)

	token, err := svc.Issue("tenant-1", "Acme Corp", "tenants/acme", "ducklake_acme")
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	if token == "" {
		t.Fatal("Issue() returned empty token")
	}

	claims, err := svc.Validate(token)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if claims.TenantID != "tenant-1" {
		t.Errorf("TenantID = %q, want %q", claims.TenantID, "tenant-1")
	}
	if claims.OrgName != "Acme Corp" {
		t.Errorf("OrgName = %q, want %q", claims.OrgName, "Acme Corp")
	}
	if claims.S3Prefix != "tenants/acme" {
		t.Errorf("S3Prefix = %q, want %q", claims.S3Prefix, "tenants/acme")
	}
	if claims.PGSchema != "ducklake_acme" {
		t.Errorf("PGSchema = %q, want %q", claims.PGSchema, "ducklake_acme")
	}
}

func TestValidate_WrongSecret(t *testing.T) {
	svc := auth.NewService("secret-a-long-enough-key-here", time.Hour)
	other := auth.NewService("secret-b-long-enough-key-here", time.Hour)

	token, err := svc.Issue("t1", "Org", "prefix", "schema")
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}

	if _, err := other.Validate(token); err == nil {
		t.Error("Validate() with wrong secret should return error")
	}
}

func TestValidate_ExpiredToken(t *testing.T) {
	svc := auth.NewService("test-secret-key-long-enough-here", -time.Second)

	token, err := svc.Issue("t1", "Org", "prefix", "schema")
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}

	if _, err := svc.Validate(token); err == nil {
		t.Error("Validate() with expired token should return error")
	}
}

func TestValidate_MalformedToken(t *testing.T) {
	svc := auth.NewService("test-secret-key-long-enough-here", time.Hour)
	if _, err := svc.Validate("not.a.valid.token"); err == nil {
		t.Error("Validate() with malformed token should return error")
	}
}

func TestValidate_EmptyToken(t *testing.T) {
	svc := auth.NewService("test-secret-key-long-enough-here", time.Hour)
	if _, err := svc.Validate(""); err == nil {
		t.Error("Validate() with empty token should return error")
	}
}
