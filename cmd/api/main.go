package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/dataplane-control-plane/internal/api"
	"github.com/example/dataplane-control-plane/internal/config"
	appdb "github.com/example/dataplane-control-plane/internal/db"
	"github.com/example/dataplane-control-plane/internal/metrics"
	"github.com/example/dataplane-control-plane/internal/provisioner"
	"github.com/example/dataplane-control-plane/internal/repository"
	"github.com/example/dataplane-control-plane/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sqlDB, err := appdb.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("database", "error", err)
		os.Exit(1)
	}
	defer sqlDB.Close()
	if err := appdb.Migrate(ctx, sqlDB); err != nil {
		slog.Error("migration", "error", err)
		os.Exit(1)
	}

	repo := repository.New(sqlDB)
	var prov provisioner.Provisioner = provisioner.Mock{}
	if cfg.Provisioner == "kubernetes" {
		prov, err = provisioner.NewKubernetes(cfg.Kubeconfig, cfg.K8sNamespace, cfg.OpenSearchImage)
		if err != nil {
			slog.Error("kubernetes provisioner", "error", err)
			os.Exit(1)
		}
	}

	metrics.Register()
	workers := worker.New(repo, prov, cfg.Workers, cfg.JobPollInterval)
	go workers.Run(ctx)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(repo).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		slog.Info("api listening", "addr", cfg.HTTPAddr, "provisioner", cfg.Provisioner)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	slog.Info("shutdown complete")
}
