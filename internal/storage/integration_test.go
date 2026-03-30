//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/satheeshds/nexus/internal/config"
	"github.com/satheeshds/nexus/internal/storage"
)

// testMinIOConfig builds a MinIOConfig from environment variables,
// falling back to the defaults that match docker-compose.test.yml.
func testMinIOConfig() config.MinIOConfig {
	return config.MinIOConfig{
		Endpoint:     envOrDefault("TEST_MINIO_ENDPOINT", "localhost:9002"),
		AccessKey:    envOrDefault("TEST_MINIO_ACCESS_KEY", "minioadmin_test"),
		SecretKey:    envOrDefault("TEST_MINIO_SECRET_KEY", "testpassword"),
		Bucket:       envOrDefault("TEST_MINIO_BUCKET", "lakehouse-test"),
		UseSSL:       false,
		UsePathStyle: true,
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestNew verifies that a storage.Client can be created with valid config.
func TestNew(t *testing.T) {
	_, err := storage.New(testMinIOConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
}

// TestEnsureBucket verifies that EnsureBucket creates the bucket when it doesn't exist.
func TestEnsureBucket(t *testing.T) {
	client, err := storage.New(testMinIOConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	if err := client.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}
}

// TestEnsureBucketIdempotent verifies that calling EnsureBucket twice does not error.
func TestEnsureBucketIdempotent(t *testing.T) {
	client, err := storage.New(testMinIOConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	if err := client.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket (first): %v", err)
	}
	if err := client.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket (second, idempotent): %v", err)
	}
}

// TestProvisionDeprovisionTenant verifies that a MinIO service account can be
// created for a tenant prefix and then deleted.
func TestProvisionDeprovisionTenant(t *testing.T) {
	client, err := storage.New(testMinIOConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	tenantID := fmt.Sprintf("test_tenant_%s", uuid.New().String()[:8])
	s3Prefix := fmt.Sprintf("tenants/%s", tenantID)

	creds, err := client.ProvisionTenant(ctx, tenantID, s3Prefix)
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	if creds.AccessKey == "" {
		t.Error("ProvisionTenant: got empty AccessKey")
	}
	if creds.SecretKey == "" {
		t.Error("ProvisionTenant: got empty SecretKey")
	}

	// Cleanup: deprovision the service account.
	if err := client.DeprovisionTenant(ctx, creds.AccessKey); err != nil {
		t.Errorf("DeprovisionTenant: %v", err)
	}
}

// TestProvisionMultipleTenantsIsolated verifies that each tenant gets a distinct
// service account with a distinct AccessKey.
func TestProvisionMultipleTenantsIsolated(t *testing.T) {
	client, err := storage.New(testMinIOConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	prefix := fmt.Sprintf("tenants/iso_test_%s", uuid.New().String()[:8])

	creds1, err := client.ProvisionTenant(ctx, "iso_tenant_a", prefix+"/a")
	if err != nil {
		t.Fatalf("ProvisionTenant(a): %v", err)
	}
	t.Cleanup(func() { _ = client.DeprovisionTenant(ctx, creds1.AccessKey) })

	creds2, err := client.ProvisionTenant(ctx, "iso_tenant_b", prefix+"/b")
	if err != nil {
		t.Fatalf("ProvisionTenant(b): %v", err)
	}
	t.Cleanup(func() { _ = client.DeprovisionTenant(ctx, creds2.AccessKey) })

	if creds1.AccessKey == creds2.AccessKey {
		t.Error("expected distinct AccessKeys for different tenants")
	}
	if creds1.SecretKey == creds2.SecretKey {
		t.Error("expected distinct SecretKeys for different tenants")
	}
}
