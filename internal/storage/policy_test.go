package storage

import (
	"strings"
	"testing"

	"github.com/satheeshds/nexus/internal/config"
)

func TestBuildPolicy(t *testing.T) {
	c := &Client{
		cfg: config.MinIOConfig{
			Bucket: "lakehouse",
		},
	}

	prefix := "tenants/acme_corp"
	policy := c.buildPolicy(prefix)

	// Must contain the bucket and prefix in the expected places.
	if !strings.Contains(policy, "lakehouse") {
		t.Error("buildPolicy() should contain the bucket name")
	}
	if !strings.Contains(policy, prefix) {
		t.Error("buildPolicy() should contain the s3 prefix")
	}
	if !strings.Contains(policy, "s3:GetObject") {
		t.Error("buildPolicy() should allow s3:GetObject")
	}
	if !strings.Contains(policy, "s3:PutObject") {
		t.Error("buildPolicy() should allow s3:PutObject")
	}
	if !strings.Contains(policy, "s3:DeleteObject") {
		t.Error("buildPolicy() should allow s3:DeleteObject")
	}
	if !strings.Contains(policy, "s3:ListBucket") {
		t.Error("buildPolicy() should allow s3:ListBucket")
	}
	// Policy must be non-empty valid-looking JSON
	if !strings.HasPrefix(strings.TrimSpace(policy), "{") {
		t.Error("buildPolicy() should return a JSON object")
	}
}

func TestBuildPolicy_PrefixScoping(t *testing.T) {
	c := &Client{
		cfg: config.MinIOConfig{
			Bucket: "mybucket",
		},
	}

	prefix := "tenants/tenant-a"
	policy := c.buildPolicy(prefix)

	// The policy must reference the bucket name.
	if !strings.Contains(policy, "mybucket") {
		t.Errorf("buildPolicy() should reference bucket name, got:\n%s", policy)
	}
	// The policy must reference the tenant prefix.
	if !strings.Contains(policy, prefix) {
		t.Errorf("buildPolicy() should reference the tenant prefix, got:\n%s", policy)
	}
	// Object-level access should be scoped under the prefix (not the whole bucket).
	if strings.Contains(policy, `"arn:aws:s3:::mybucket/*"`) {
		t.Errorf("buildPolicy() grants access to entire bucket instead of scoped prefix")
	}
}
