package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// TenantClaims are embedded in every JWT issued by the control plane.
type TenantClaims struct {
	TenantID  string `json:"tenant_id"`
	OrgName   string `json:"org_name"`
	S3Prefix  string `json:"s3_prefix"`
	PGSchema  string `json:"pg_schema"`
	jwt.RegisteredClaims
}

type Service struct {
	secret        []byte
	tokenDuration time.Duration
}

func NewService(secret string, duration time.Duration) *Service {
	return &Service{
		secret:        []byte(secret),
		tokenDuration: duration,
	}
}

// Issue creates a signed JWT for a tenant.
func (s *Service) Issue(tenantID, orgName, s3Prefix, pgSchema string) (string, error) {
	claims := TenantClaims{
		TenantID: tenantID,
		OrgName:  orgName,
		S3Prefix: s3Prefix,
		PGSchema: pgSchema,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   tenantID,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(s.tokenDuration)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(s.secret)
	if err != nil {
		return "", fmt.Errorf("sign token: %w", err)
	}
	return signed, nil
}

// Validate parses and validates a JWT, returning the embedded claims.
func (s *Service) Validate(tokenString string) (*TenantClaims, error) {
	token, err := jwt.ParseWithClaims(
		tokenString,
		&TenantClaims{},
		func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return s.secret, nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("parse token: %w", err)
	}
	claims, ok := token.Claims.(*TenantClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token claims")
	}
	return claims, nil
}
