package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Gateway  GatewayConfig  `mapstructure:"gateway"`
	Control  ControlConfig  `mapstructure:"control"`
	Postgres PostgresConfig `mapstructure:"postgres"`
	MinIO    MinIOConfig    `mapstructure:"minio"`
	DuckLake DuckLakeConfig `mapstructure:"ducklake"`
	Auth     AuthConfig     `mapstructure:"auth"`
	Pool     PoolConfig     `mapstructure:"pool"`
}

type GatewayConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type ControlConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type PostgresConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	DBName   string `mapstructure:"dbname"`
	SSLMode  string `mapstructure:"sslmode"`
}

func (p PostgresConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		p.Host, p.Port, p.User, p.Password, p.DBName, p.SSLMode,
	)
}

func (p PostgresConfig) URL() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		p.User, p.Password, p.Host, p.Port, p.DBName, p.SSLMode,
	)
}

type MinIOConfig struct {
	Endpoint     string `mapstructure:"endpoint"`
	AccessKey    string `mapstructure:"access_key"`    // Admin credentials for provisioning tenant service accounts
	SecretKey    string `mapstructure:"secret_key"`    // Admin credentials for provisioning tenant service accounts
	Bucket       string `mapstructure:"bucket"`
	UseSSL       bool   `mapstructure:"use_ssl"`
	UsePathStyle bool   `mapstructure:"use_path_style"`
}

type DuckLakeConfig struct {
	// Base S3 path where all tenant data lives: s3://lakehouse/tenants/
	TenantBasePath string `mapstructure:"tenant_base_path"`
}

type AuthConfig struct {
	JWTSecret     string        `mapstructure:"jwt_secret"`
	TokenDuration time.Duration `mapstructure:"token_duration"`
	AdminAPIKey   string        `mapstructure:"admin_api_key"` // For platform operator endpoints
}

type PoolConfig struct {
	// MaxIdleSessions is the maximum number of idle DuckDB sessions per tenant.
	// DuckDB's single-writer OLAP model means one session per tenant is optimal;
	// this field is kept for configuration completeness and future multi-reader support.
	MaxIdleSessions int `mapstructure:"max_idle_sessions"`
	// Sessions unused longer than this are evicted
	SessionTTL time.Duration `mapstructure:"session_ttl"`
	// How often the eviction goroutine runs
	EvictionInterval time.Duration `mapstructure:"eviction_interval"`
}

func Load() (*Config, error) {
	v := viper.New()

	// Defaults
	v.SetDefault("gateway.host", "0.0.0.0")
	v.SetDefault("gateway.port", 5433)
	v.SetDefault("control.host", "0.0.0.0")
	v.SetDefault("control.port", 8080)
	v.SetDefault("postgres.host", "localhost")
	v.SetDefault("postgres.port", 5432)
	v.SetDefault("postgres.user", "nexus")
	v.SetDefault("postgres.password", "changeme")
	v.SetDefault("postgres.sslmode", "disable")
	v.SetDefault("postgres.dbname", "lake_catalog")
	v.SetDefault("minio.endpoint", "localhost:9000")
	v.SetDefault("minio.bucket", "lakehouse")
	v.SetDefault("minio.use_ssl", false)
	v.SetDefault("minio.use_path_style", true)
	v.SetDefault("ducklake.tenant_base_path", "tenants")
	v.SetDefault("auth.jwt_secret", "supersecretkey_change_in_production")
	v.SetDefault("auth.token_duration", "24h")
	v.SetDefault("pool.max_idle_sessions", 1)
	v.SetDefault("pool.session_ttl", "30m")
	v.SetDefault("pool.eviction_interval", "5m")

	// Read from config file
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath("./config")
	v.AddConfigPath(".")
	_ = v.ReadInConfig() // OK if not found; env vars take precedence

	// Env var overrides: NEXUS_POSTGRES_HOST, NEXUS_MINIO_BUCKET, etc.
	v.SetEnvPrefix("NEXUS")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Explicitly bind critical keys to ensure mapping works without a config file
	v.BindEnv("postgres.host", "NEXUS_POSTGRES_HOST")
	v.BindEnv("postgres.port", "NEXUS_POSTGRES_PORT")
	v.BindEnv("postgres.user", "NEXUS_POSTGRES_USER")
	v.BindEnv("postgres.password", "NEXUS_POSTGRES_PASSWORD")
	v.BindEnv("postgres.dbname", "NEXUS_POSTGRES_DBNAME")
	v.BindEnv("minio.endpoint", "NEXUS_MINIO_ENDPOINT")
	v.BindEnv("minio.access_key", "NEXUS_MINIO_ACCESS_KEY")
	v.BindEnv("minio.secret_key", "NEXUS_MINIO_SECRET_KEY")
	v.BindEnv("minio.bucket", "NEXUS_MINIO_BUCKET")
	v.BindEnv("auth.jwt_secret", "NEXUS_AUTH_JWT_SECRET")
	v.BindEnv("auth.admin_api_key", "NEXUS_AUTH_ADMIN_API_KEY")

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	return &cfg, nil
}
