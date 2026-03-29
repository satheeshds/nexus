package storage

import (
	"context"
	"fmt"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/satheeshds/nexus/internal/config"
	"log/slog"
)

// Client wraps both the MinIO object client and admin client.
type Client struct {
	obj    *minio.Client
	admin  *madmin.AdminClient
	cfg    config.MinIOConfig
}

func New(cfg config.MinIOConfig) (*Client, error) {
	obj, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}

	admin, err := madmin.New(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.UseSSL)
	if err != nil {
		return nil, fmt.Errorf("create minio admin client: %w", err)
	}

	return &Client{obj: obj, admin: admin, cfg: cfg}, nil
}

// EnsureBucket creates the lakehouse bucket if it doesn't exist.
func (c *Client) EnsureBucket(ctx context.Context) error {
	exists, err := c.obj.BucketExists(ctx, c.cfg.Bucket)
	if err != nil {
		return fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		if err := c.obj.MakeBucket(ctx, c.cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("make bucket: %w", err)
		}
	}
	return nil
}

// TenantCredentials holds the MinIO service account keys for a tenant.
type TenantCredentials struct {
	AccessKey string
	SecretKey string
}

// ProvisionTenant creates a MinIO service account scoped to the tenant's S3 prefix.
func (c *Client) ProvisionTenant(ctx context.Context, tenantID, s3Prefix string) (*TenantCredentials, error) {
	// policy := c.buildPolicy(s3Prefix)

	slog.Debug("creating minio service account", "tenant", tenantID, "endpoint", c.cfg.Endpoint)
	resp, err := c.admin.AddServiceAccount(ctx, madmin.AddServiceAccountReq{
		// Policy:      json.RawMessage(policy),
		Description: fmt.Sprintf("nexus-tenant-%s", tenantID),
	})
	if err != nil {
		return nil, fmt.Errorf("create service account for %q on %s: %w", tenantID, c.cfg.Endpoint, err)
	}

	return &TenantCredentials{
		AccessKey: resp.AccessKey,
		SecretKey: resp.SecretKey,
	}, nil
}

// DeprovisionTenant deletes the MinIO service account for the tenant.
func (c *Client) DeprovisionTenant(ctx context.Context, accessKey string) error {
	if err := c.admin.DeleteServiceAccount(ctx, accessKey); err != nil {
		return fmt.Errorf("delete service account %q: %w", accessKey, err)
	}
	return nil
}

// buildPolicy returns a scoped IAM policy JSON for a tenant prefix.
func (c *Client) buildPolicy(s3Prefix string) string {
	bucket := c.cfg.Bucket
	return fmt.Sprintf(`{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::%s/%s/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::%s",
      "Condition": {
        "StringLike": {
          "s3:prefix": ["%s/*"]
        }
      }
    }
  ]
}`, bucket, s3Prefix, bucket, s3Prefix)
}
