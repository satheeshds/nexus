package tenant

import (
	"strings"
	"testing"
)

func TestMakeSlug(t *testing.T) {
	tests := []struct {
		name    string
		orgName string
		// makeSlug appends a random UUID suffix, so we check the prefix only.
		wantPrefix string
	}{
		{"spaces_in_name", "Acme Corp", "acme_corp_"},
		{"exclamation_stripped", "Hello World!", "hello_world_"},
		{"hyphen_to_underscore", "My-Org", "my_org_"},
		{"alphanumeric", "test123", "test123_"},
		{"uppercase_lowered", "UPPERCASE", "uppercase_"},
		{"special_chars_stripped", "special@#chars", "specialchars_"},
		{"leading_trailing_spaces", "  spaces  ", "__spaces__"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := makeSlug(tc.orgName)
			if !strings.HasPrefix(got, tc.wantPrefix) {
				t.Errorf("makeSlug(%q) = %q, want prefix %q", tc.orgName, got, tc.wantPrefix)
			}
			// The suffix should be 8 hex characters (UUID fragment).
			parts := strings.Split(got, "_")
			suffix := parts[len(parts)-1]
			if len(suffix) != 8 {
				t.Errorf("makeSlug(%q) suffix = %q, want 8 chars", tc.orgName, suffix)
			}
		})
	}
}

func TestMakeSlug_Uniqueness(t *testing.T) {
	// Two calls with the same input should produce different slugs (UUID suffix).
	a := makeSlug("Acme Corp")
	b := makeSlug("Acme Corp")
	if a == b {
		t.Errorf("makeSlug produced identical slugs on two calls: %q", a)
	}
}

func TestGenerateAPIKey(t *testing.T) {
	key, err := generateAPIKey()
	if err != nil {
		t.Fatalf("generateAPIKey() error = %v", err)
	}
	// 32 bytes → 64 hex characters
	if len(key) != 64 {
		t.Errorf("generateAPIKey() len = %d, want 64", len(key))
	}
	// Keys must be unique across calls
	key2, err := generateAPIKey()
	if err != nil {
		t.Fatalf("generateAPIKey() error = %v", err)
	}
	if key == key2 {
		t.Error("generateAPIKey() produced identical keys on two calls")
	}
}
