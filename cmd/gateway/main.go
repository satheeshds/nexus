package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/satheeshds/nexus/internal/auth"
	"github.com/satheeshds/nexus/internal/catalog"
	"github.com/satheeshds/nexus/internal/config"
	"github.com/satheeshds/nexus/internal/gateway"
	"github.com/satheeshds/nexus/internal/pool"
)

// @title Nexus Query Gateway API
// @version 1.0
// @description The Nexus query gateway provides a Postgres-compatible entry point for customer data queries.
// @host localhost:8081
// @BasePath /

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

	// Catalog DB
	catalogDB, err := catalog.New(ctx, cfg.Postgres)
	if err != nil {
		slog.Error("connect to postgres", "err", err)
		os.Exit(1)
	}
	defer catalogDB.Close()

	// Auth service
	authSvc := auth.NewService(cfg.Auth.JWTSecret, cfg.Auth.TokenDuration)

	// Session pool
	sessionPool := pool.New(cfg.Postgres, cfg.MinIO, cfg.Pool)
	defer sessionPool.Close()

	// Gateway server
	addr := fmt.Sprintf("%s:%d", cfg.Gateway.Host, cfg.Gateway.Port)
	httpAddr := fmt.Sprintf("%s:%d", cfg.Gateway.Host, cfg.Gateway.HTTPPort)
	srv := gateway.NewServer(addr, httpAddr, sessionPool, authSvc, catalogDB)

	slog.Info("nexus gateway starting", "addr", addr)
	if err := srv.ListenAndServe(ctx); err != nil {
		slog.Error("gateway error", "err", err)
		os.Exit(1)
	}
}
