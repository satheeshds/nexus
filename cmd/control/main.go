package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/satheeshds/nexus/migrations"
	"github.com/satheeshds/nexus/internal/auth"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
	"github.com/satheeshds/nexus/internal/control"
	"github.com/satheeshds/nexus/internal/pool"
	"github.com/satheeshds/nexus/internal/storage"
	"github.com/satheeshds/nexus/internal/tenant"
)

// @title Nexus Control Plane API
// @version 1.0
// @description The Nexus control plane manages customer tenants, registration, and authentication.
// @host localhost:8080
// @BasePath /

// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name Authorization
// @description "Bearer <token>"

// @securityDefinitions.apikey AdminAuth
// @in header
// @name X-Admin-API-Key
// @description Admin API key for sensitive operations

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Run DB migrations
	if err := runMigrations(cfg.Postgres.URL()); err != nil {
		slog.Error("migrations failed", "err", err)
		os.Exit(1)
	}

	// Catalog DB
	catalogDB, err := catalog.New(ctx, cfg.Postgres)
	if err != nil {
		slog.Error("connect to postgres", "err", err)
		os.Exit(1)
	}
	defer catalogDB.Close()

	// Storage client (MinIO)
	storageClient, err := storage.New(cfg.MinIO)
	if err != nil {
		slog.Error("connect to minio", "err", err)
		os.Exit(1)
	}
	if err := storageClient.EnsureBucket(ctx); err != nil {
		slog.Error("ensure bucket", "err", err)
		os.Exit(1)
	}

	// Auth service
	authSvc := auth.NewService(cfg.Auth.JWTSecret, cfg.Auth.TokenDuration)

	// Tenant provisioner
	provisioner := tenant.NewProvisioner(
		catalogDB, storageClient,
		cfg.Postgres, cfg.MinIO, cfg.DuckLake,
	)

	// Session pool – used by the admin query endpoint to execute SQL across tenants
	sessionPool := pool.New(catalogDB, cfg.Postgres, cfg.MinIO, cfg.Pool)
	defer sessionPool.Close()

	// HTTP control plane server
	srv := control.NewServer(provisioner, catalogDB, authSvc, cfg.Auth.AdminAPIKey, sessionPool)
	addr := fmt.Sprintf("%s:%d", cfg.Control.Host, cfg.Control.Port)
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      srv,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		slog.Info("nexus control plane starting", "addr", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down control plane...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

func runMigrations(dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open db for migrations: %w", err)
	}
	defer db.Close()

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.Up(db, ".")
}
