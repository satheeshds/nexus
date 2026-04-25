//go:build integration

package catalog_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/testutil"
)

// TestCatalog_TenantCRUD covers the full lifecycle of a tenant record.
func TestCatalog_TenantCRUD(t *testing.T) {
	pg := testutil.StartPostgres(t)
	db := testutil.NewCatalogDB(t, pg)
	ctx := context.Background()

	tenant := catalog.Tenant{
		ID:           "tenant_crud_01",
		OrgName:      "CRUD Corp",
		Email:        "crud@example.com",
		S3Prefix:     "tenants/tenant_crud_01",
		PGSchema:     "ducklake_tenant_crud_01",
		PasswordHash: "$2a$10$fakehashfortest",
		CreatedAt:    time.Now().UTC().Truncate(time.Second),
	}

	t.Run("InsertTenant", func(t *testing.T) {
		if err := db.InsertTenant(ctx, tenant); err != nil {
			t.Fatalf("InsertTenant: %v", err)
		}
	})

	t.Run("GetTenant", func(t *testing.T) {
		got, err := db.GetTenant(ctx, tenant.ID)
		if err != nil {
			t.Fatalf("GetTenant: %v", err)
		}
		if got.ID != tenant.ID {
			t.Errorf("ID: want %q got %q", tenant.ID, got.ID)
		}
		if got.OrgName != tenant.OrgName {
			t.Errorf("OrgName: want %q got %q", tenant.OrgName, got.OrgName)
		}
		if got.Email != tenant.Email {
			t.Errorf("Email: want %q got %q", tenant.Email, got.Email)
		}
		if got.S3Prefix != tenant.S3Prefix {
			t.Errorf("S3Prefix: want %q got %q", tenant.S3Prefix, got.S3Prefix)
		}
		if got.PGSchema != tenant.PGSchema {
			t.Errorf("PGSchema: want %q got %q", tenant.PGSchema, got.PGSchema)
		}
	})

	t.Run("GetTenantByEmail", func(t *testing.T) {
		got, err := db.GetTenantByEmail(ctx, tenant.Email)
		if err != nil {
			t.Fatalf("GetTenantByEmail: %v", err)
		}
		if got.ID != tenant.ID {
			t.Errorf("ID: want %q got %q", tenant.ID, got.ID)
		}
	})

	t.Run("GetTenant_NotFound", func(t *testing.T) {
		_, err := db.GetTenant(ctx, "nonexistent_id")
		if !errors.Is(err, catalog.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("ListTenants", func(t *testing.T) {
		tenants, err := db.ListTenants(ctx)
		if err != nil {
			t.Fatalf("ListTenants: %v", err)
		}
		found := false
		for _, tnt := range tenants {
			if tnt.ID == tenant.ID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected tenant %q in list", tenant.ID)
		}
	})

	t.Run("DeleteTenant", func(t *testing.T) {
		if err := db.DeleteTenant(ctx, tenant.ID); err != nil {
			t.Fatalf("DeleteTenant: %v", err)
		}
		_, err := db.GetTenant(ctx, tenant.ID)
		if !errors.Is(err, catalog.ErrNotFound) {
			t.Errorf("expected ErrNotFound after delete, got %v", err)
		}
	})
}

// TestCatalog_DuplicateTenantEmail verifies that inserting two tenants with the
// same email returns an error (UNIQUE constraint on the email column).
func TestCatalog_DuplicateTenantEmail(t *testing.T) {
	pg := testutil.StartPostgres(t)
	db := testutil.NewCatalogDB(t, pg)
	ctx := context.Background()

	tenant1 := catalog.Tenant{
		ID:        "dup_email_01",
		OrgName:   "Dupe Corp",
		Email:     "dupe@example.com",
		S3Prefix:  "tenants/dup_email_01",
		PGSchema:  "ducklake_dup_email_01",
		CreatedAt: time.Now(),
	}
	tenant2 := catalog.Tenant{
		ID:        "dup_email_02",
		OrgName:   "Dupe Corp 2",
		Email:     "dupe@example.com", // same email
		S3Prefix:  "tenants/dup_email_02",
		PGSchema:  "ducklake_dup_email_02",
		CreatedAt: time.Now(),
	}

	if err := db.InsertTenant(ctx, tenant1); err != nil {
		t.Fatalf("insert tenant1: %v", err)
	}
	if err := db.InsertTenant(ctx, tenant2); err == nil {
		t.Error("expected error inserting tenant with duplicate email, got nil")
	}
}

// TestCatalog_ServiceAccountCRUD covers the full lifecycle of a service account.
func TestCatalog_ServiceAccountCRUD(t *testing.T) {
	pg := testutil.StartPostgres(t)
	db := testutil.NewCatalogDB(t, pg)
	ctx := context.Background()

	// A service account requires a parent tenant.
	tenant := catalog.Tenant{
		ID:        "sa_tenant_01",
		OrgName:   "SA Corp",
		Email:     "sa@example.com",
		S3Prefix:  "tenants/sa_tenant_01",
		PGSchema:  "ducklake_sa_tenant_01",
		CreatedAt: time.Now(),
	}
	if err := db.InsertTenant(ctx, tenant); err != nil {
		t.Fatalf("setup tenant: %v", err)
	}

	sa := catalog.ServiceAccount{
		ID:               "sa_tenant_01_svc",
		TenantID:         tenant.ID,
		S3Prefix:         tenant.S3Prefix,
		PGSchema:         tenant.PGSchema,
		MinioAccessKey:   "AKIATEST123",
		MinioSecretKey:   "secretkey123",
		APIKeyHash:       "$2a$10$fakehashfortest",
		APIKeyCiphertext: "ciphertextfortest",
		APIKeyRotatedAt:  time.Now().UTC().Truncate(time.Second),
		CreatedAt:        time.Now().UTC().Truncate(time.Second),
	}

	t.Run("InsertServiceAccount", func(t *testing.T) {
		if err := db.InsertServiceAccount(ctx, sa); err != nil {
			t.Fatalf("InsertServiceAccount: %v", err)
		}
	})

	t.Run("GetServiceAccount", func(t *testing.T) {
		got, err := db.GetServiceAccount(ctx, sa.ID)
		if err != nil {
			t.Fatalf("GetServiceAccount: %v", err)
		}
		if got.ID != sa.ID {
			t.Errorf("ID: want %q got %q", sa.ID, got.ID)
		}
		if got.TenantID != sa.TenantID {
			t.Errorf("TenantID: want %q got %q", sa.TenantID, got.TenantID)
		}
		if got.MinioAccessKey != sa.MinioAccessKey {
			t.Errorf("MinioAccessKey: want %q got %q", sa.MinioAccessKey, got.MinioAccessKey)
		}
		if got.MinioSecretKey != sa.MinioSecretKey {
			t.Errorf("MinioSecretKey: want %q got %q", sa.MinioSecretKey, got.MinioSecretKey)
		}
		if got.APIKeyCiphertext != sa.APIKeyCiphertext {
			t.Errorf("APIKeyCiphertext: want %q got %q", sa.APIKeyCiphertext, got.APIKeyCiphertext)
		}
	})

	t.Run("GetServiceAccountByTenantID", func(t *testing.T) {
		got, err := db.GetServiceAccountByTenantID(ctx, tenant.ID)
		if err != nil {
			t.Fatalf("GetServiceAccountByTenantID: %v", err)
		}
		if got.ID != sa.ID {
			t.Errorf("ID: want %q got %q", sa.ID, got.ID)
		}
	})

	t.Run("GetServiceAccount_NotFound", func(t *testing.T) {
		_, err := db.GetServiceAccount(ctx, "nonexistent_sa_id")
		if !errors.Is(err, catalog.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("UpdateServiceAccountKey", func(t *testing.T) {
		newHash := "$2a$10$newhashfortest"
		newCiphertext := "newciphertext"
		newRotatedAt := time.Now().UTC().Truncate(time.Second)
		if err := db.UpdateServiceAccountKey(ctx, tenant.ID, newHash, newCiphertext, newRotatedAt); err != nil {
			t.Fatalf("UpdateServiceAccountKey: %v", err)
		}
		got, err := db.GetServiceAccount(ctx, sa.ID)
		if err != nil {
			t.Fatalf("GetServiceAccount after update: %v", err)
		}
		if got.APIKeyHash != newHash {
			t.Errorf("APIKeyHash: want %q got %q", newHash, got.APIKeyHash)
		}
		if got.APIKeyCiphertext != newCiphertext {
			t.Errorf("APIKeyCiphertext: want %q got %q", newCiphertext, got.APIKeyCiphertext)
		}
		if !got.APIKeyRotatedAt.Equal(newRotatedAt) {
			t.Errorf("APIKeyRotatedAt: want %v got %v", newRotatedAt, got.APIKeyRotatedAt)
		}
	})

	t.Run("UpdateServiceAccountKey_NotFound", func(t *testing.T) {
		err := db.UpdateServiceAccountKey(ctx, "nonexistent_tenant", "hash", "plain", time.Now())
		if !errors.Is(err, catalog.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("DeleteServiceAccountByTenantID", func(t *testing.T) {
		if err := db.DeleteServiceAccountByTenantID(ctx, tenant.ID); err != nil {
			t.Fatalf("DeleteServiceAccountByTenantID: %v", err)
		}
		_, err := db.GetServiceAccount(ctx, sa.ID)
		if !errors.Is(err, catalog.ErrNotFound) {
			t.Errorf("expected ErrNotFound after delete, got %v", err)
		}
	})
}

// TestCatalog_ServiceAccountCascadeDelete verifies that deleting a tenant also
// removes its associated service accounts (ON DELETE CASCADE).
func TestCatalog_ServiceAccountCascadeDelete(t *testing.T) {
	pg := testutil.StartPostgres(t)
	db := testutil.NewCatalogDB(t, pg)
	ctx := context.Background()

	tenant := catalog.Tenant{
		ID:        "cascade_tenant_01",
		OrgName:   "Cascade Corp",
		Email:     "cascade@example.com",
		S3Prefix:  "tenants/cascade_tenant_01",
		PGSchema:  "ducklake_cascade_01",
		CreatedAt: time.Now(),
	}
	if err := db.InsertTenant(ctx, tenant); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}

	sa := catalog.ServiceAccount{
		ID:        "cascade_tenant_01_svc",
		TenantID:  tenant.ID,
		S3Prefix:  tenant.S3Prefix,
		PGSchema:  tenant.PGSchema,
		CreatedAt: time.Now(),
	}
	if err := db.InsertServiceAccount(ctx, sa); err != nil {
		t.Fatalf("insert service account: %v", err)
	}

	// Deleting the tenant should cascade-delete the service account.
	if err := db.DeleteTenant(ctx, tenant.ID); err != nil {
		t.Fatalf("delete tenant: %v", err)
	}

	_, err := db.GetServiceAccount(ctx, sa.ID)
	if !errors.Is(err, catalog.ErrNotFound) {
		t.Errorf("expected service account to be cascade-deleted, got %v", err)
	}
}

// TestCatalog_TenantSchema covers creating and dropping the Postgres schema
// used by DuckLake for a tenant.
func TestCatalog_TenantSchema(t *testing.T) {
	pg := testutil.StartPostgres(t)
	db := testutil.NewCatalogDB(t, pg)
	ctx := context.Background()

	schemaName := "ducklake_schema_test_01"

	t.Run("CreateTenantSchema", func(t *testing.T) {
		if err := db.CreateTenantSchema(ctx, schemaName); err != nil {
			t.Fatalf("CreateTenantSchema: %v", err)
		}
		// Idempotent: calling again should not fail.
		if err := db.CreateTenantSchema(ctx, schemaName); err != nil {
			t.Errorf("CreateTenantSchema idempotent: %v", err)
		}
	})

	t.Run("DropTenantSchema", func(t *testing.T) {
		if err := db.DropTenantSchema(ctx, schemaName); err != nil {
			t.Fatalf("DropTenantSchema: %v", err)
		}
		// Idempotent: calling again should not fail (IF EXISTS).
		if err := db.DropTenantSchema(ctx, schemaName); err != nil {
			t.Errorf("DropTenantSchema idempotent: %v", err)
		}
	})
}
