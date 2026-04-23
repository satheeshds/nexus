package tenant

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"unicode"

	"github.com/satheeshds/nexus/internal/config"
)

func TestNewProvisioner(t *testing.T) {
	p := NewProvisioner(nil, nil, config.PostgresConfig{}, config.MinIOConfig{}, config.DuckLakeConfig{}, 0, "test-secret-at-least-16")
	if p == nil {
		t.Fatal("NewProvisioner() returned nil")
	}
}

func TestMakeSlug_Basic(t *testing.T) {
	slug := makeSlug("Acme Corp")
	// should start with "acme_corp_"
	if !strings.HasPrefix(slug, "acme_corp_") {
		t.Errorf("makeSlug(%q) = %q, want prefix %q", "Acme Corp", slug, "acme_corp_")
	}
}

func TestMakeSlug_StripsSpecialChars(t *testing.T) {
	slug := makeSlug("Acme Corp!")
	// Find the last underscore that separates the slug body from the UUID suffix
	lastUnderscore := strings.LastIndex(slug, "_")
	if lastUnderscore < 0 {
		t.Fatalf("makeSlug(%q) = %q has no underscore separator", "Acme Corp!", slug)
	}
	slugBody := slug[:lastUnderscore]
	// '!' should be stripped; the slug body should be lower-case alphanumeric + underscore
	for _, r := range slugBody {
		if r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			t.Errorf("makeSlug produced unexpected character %q in slug %q", r, slug)
		}
	}
}

func TestMakeSlug_IsLowerCase(t *testing.T) {
	slug := makeSlug("Acme Corp")
	if slug != strings.ToLower(slug) {
		t.Errorf("makeSlug(%q) = %q is not all lowercase", "Acme Corp", slug)
	}
}

func TestMakeSlug_ReplacesSpacesWithUnderscores(t *testing.T) {
	slug := makeSlug("hello world")
	// before the UUID suffix, spaces become underscores
	parts := strings.Split(slug, "_")
	if parts[0] != "hello" || parts[1] != "world" {
		t.Errorf("makeSlug(%q) = %q; expected 'hello_world_...'", "hello world", slug)
	}
}

func TestMakeSlug_ReplacesDashesWithUnderscores(t *testing.T) {
	slug := makeSlug("my-org")
	if !strings.HasPrefix(slug, "my_org_") {
		t.Errorf("makeSlug(%q) = %q, expected prefix %q", "my-org", slug, "my_org_")
	}
}

func TestMakeSlug_UniquePerCall(t *testing.T) {
	// Each call should produce a different suffix (UUID-based)
	s1 := makeSlug("Acme Corp")
	s2 := makeSlug("Acme Corp")
	if s1 == s2 {
		t.Errorf("makeSlug produced identical slugs on two calls: %q", s1)
	}
}

func TestGenerateAPIKey_Length(t *testing.T) {
	key, err := generateAPIKey()
	if err != nil {
		t.Fatalf("generateAPIKey() unexpected error: %v", err)
	}
	// 32 random bytes encoded as hex = 64 characters
	if len(key) != 64 {
		t.Errorf("generateAPIKey() len = %d, want 64", len(key))
	}
}

func TestGenerateAPIKey_IsHex(t *testing.T) {
	key, err := generateAPIKey()
	if err != nil {
		t.Fatalf("generateAPIKey() unexpected error: %v", err)
	}
	decoded, err := hex.DecodeString(key)
	if err != nil {
		t.Errorf("generateAPIKey() = %q is not valid hex: %v", key, err)
	}
	if len(decoded) != 32 {
		t.Errorf("decoded key length = %d, want 32 bytes", len(decoded))
	}
}

func TestGenerateAPIKey_UniquePerCall(t *testing.T) {
	k1, err := generateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	k2, err := generateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	if k1 == k2 {
		t.Errorf("generateAPIKey produced identical keys on two calls: %q", k1)
	}
}

func TestRegister_PasswordRequired(t *testing.T) {
	// Provisioner with nil deps is fine here: password validation runs before any db call.
	p := NewProvisioner(nil, nil, config.PostgresConfig{}, config.MinIOConfig{}, config.DuckLakeConfig{}, 0, "test-secret-at-least-16")
	_, err := p.Register(context.Background(), RegisterRequest{
		OrgName: "Acme Corp",
		Email:   "admin@acme.com",
		// Password intentionally omitted
	})
	if err == nil {
		t.Fatal("Register() expected error when password is empty, got nil")
	}
}
