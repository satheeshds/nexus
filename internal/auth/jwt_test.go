package auth_test

import (
	"strings"
	"testing"
	"time"

	"github.com/satheeshds/nexus/internal/auth"
)

func TestIssueAndValidate(t *testing.T) {
	svc := auth.NewService("test-secret-key-32-bytes-padding!!", time.Hour)

	token, err := svc.Issue("tenant1", "Acme Corp", "tenants/tenant1", "ducklake_tenant1")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if token == "" {
		t.Fatal("Issue: got empty token")
	}

	claims, err := svc.Validate(token)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if claims.TenantID != "tenant1" {
		t.Errorf("TenantID: got %q, want %q", claims.TenantID, "tenant1")
	}
	if claims.OrgName != "Acme Corp" {
		t.Errorf("OrgName: got %q, want %q", claims.OrgName, "Acme Corp")
	}
	if claims.S3Prefix != "tenants/tenant1" {
		t.Errorf("S3Prefix: got %q, want %q", claims.S3Prefix, "tenants/tenant1")
	}
	if claims.PGSchema != "ducklake_tenant1" {
		t.Errorf("PGSchema: got %q, want %q", claims.PGSchema, "ducklake_tenant1")
	}
}

func TestValidateExpiredToken(t *testing.T) {
	svc := auth.NewService("test-secret-key-32-bytes-padding!!", time.Nanosecond)

	token, err := svc.Issue("tenant1", "Acme", "s3://bucket/tenant1", "schema1")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// Wait for the token to expire.
	time.Sleep(10 * time.Millisecond)

	_, err = svc.Validate(token)
	if err == nil {
		t.Fatal("Validate expired token: expected error, got nil")
	}
}

func TestValidateWrongSecret(t *testing.T) {
	issuer := auth.NewService("secret-one-32-bytes-long-padding!!", time.Hour)
	validator := auth.NewService("secret-two-32-bytes-long-padding!!", time.Hour)

	token, err := issuer.Issue("tenant1", "Acme", "prefix", "schema")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	_, err = validator.Validate(token)
	if err == nil {
		t.Fatal("Validate with wrong secret: expected error, got nil")
	}
}

func TestValidateInvalidTokenFormat(t *testing.T) {
	svc := auth.NewService("test-secret-key-32-bytes-padding!!", time.Hour)

	for _, bad := range []string{"", "notavalidtoken", "header.payload", "a.b.c.d.e"} {
		_, err := svc.Validate(bad)
		if err == nil {
			t.Errorf("Validate(%q): expected error, got nil", bad)
		}
	}
}

func TestTokenContainsClaims(t *testing.T) {
	svc := auth.NewService("test-secret-key-32-bytes-padding!!", time.Hour)

	token, err := svc.Issue("acme_corp_abc12345", "Acme Corp", "tenants/acme_corp", "ducklake_acme_corp")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// A JWT has exactly 3 dot-separated parts.
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Errorf("expected 3 JWT parts, got %d", len(parts))
	}
}
