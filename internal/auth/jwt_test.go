package auth_test

import (
	"strings"
	"testing"
	"time"

	"github.com/satheeshds/nexus/internal/auth"
)

func TestIssueAndValidate(t *testing.T) {
	svc := auth.NewService("test-secret-key", time.Hour)

	token, err := svc.Issue("tenant-1", "Acme Corp", "tenants/acme", "ducklake_acme")
	if err != nil {
		t.Fatalf("Issue() unexpected error: %v", err)
	}
	if token == "" {
		t.Fatal("Issue() returned empty token")
	}

	claims, err := svc.Validate(token)
	if err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
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

func TestValidate_ExpiredToken(t *testing.T) {
	svc := auth.NewService("test-secret-key", -time.Minute)

	token, err := svc.Issue("tenant-1", "Acme Corp", "tenants/acme", "ducklake_acme")
	if err != nil {
		t.Fatalf("Issue() unexpected error: %v", err)
	}

	_, err = svc.Validate(token)
	if err == nil {
		t.Fatal("Validate() expected error for expired token, got nil")
	}
}

func TestValidate_WrongSecret(t *testing.T) {
	svc := auth.NewService("correct-secret", time.Hour)
	token, err := svc.Issue("tenant-1", "Acme Corp", "tenants/acme", "ducklake_acme")
	if err != nil {
		t.Fatalf("Issue() unexpected error: %v", err)
	}

	wrongSvc := auth.NewService("wrong-secret", time.Hour)
	_, err = wrongSvc.Validate(token)
	if err == nil {
		t.Fatal("Validate() expected error for wrong secret, got nil")
	}
}

func TestValidate_MalformedToken(t *testing.T) {
	svc := auth.NewService("test-secret-key", time.Hour)

	_, err := svc.Validate("not.a.valid.jwt")
	if err == nil {
		t.Fatal("Validate() expected error for malformed token, got nil")
	}
}

func TestValidate_EmptyToken(t *testing.T) {
	svc := auth.NewService("test-secret-key", time.Hour)

	_, err := svc.Validate("")
	if err == nil {
		t.Fatal("Validate() expected error for empty token, got nil")
	}
}

func TestIssue_TokenContainsThreeParts(t *testing.T) {
	svc := auth.NewService("test-secret-key", time.Hour)

	token, err := svc.Issue("t", "org", "s3", "schema")
	if err != nil {
		t.Fatalf("Issue() unexpected error: %v", err)
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Errorf("JWT token should have 3 parts, got %d", len(parts))
	}
}

func TestValidate_TamperedToken(t *testing.T) {
	svc := auth.NewService("test-secret-key", time.Hour)

	token, err := svc.Issue("tenant-1", "Acme Corp", "tenants/acme", "ducklake_acme")
	if err != nil {
		t.Fatalf("Issue() unexpected error: %v", err)
	}

	// Tamper with the signature (last part)
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatal("expected 3-part JWT")
	}
	parts[2] = parts[2] + "tampered"
	tampered := strings.Join(parts, ".")

	_, err = svc.Validate(tampered)
	if err == nil {
		t.Fatal("Validate() expected error for tampered token, got nil")
	}
}
