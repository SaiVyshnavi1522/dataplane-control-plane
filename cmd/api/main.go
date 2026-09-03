package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/api"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/auth"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/config"
	appdb "github.com/SaiVyshnavi1522/dataplane-control-plane/internal/db"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/grpcapi"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/metrics"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/provisioner"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/repository"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/service"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/snapshot"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/telemetry"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/worker"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
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
	shutdownTracing, err := telemetry.Init(ctx, cfg.OTLPEndpoint)
	if err != nil {
		slog.Error("tracing", "error", err)
		os.Exit(1)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(flushCtx); err != nil {
			slog.Error("flush tracing", "error", err)
		}
	}()

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
		prov, err = provisioner.NewKubernetes(cfg.Kubeconfig, provisioner.KubernetesOptions{
			Namespace:    cfg.K8sNamespace,
			Image:        cfg.OpenSearchImage,
			StorageSize:  cfg.StorageSize,
			StorageClass: cfg.StorageClass,
		})
		if err != nil {
			slog.Error("kubernetes provisioner", "error", err)
			os.Exit(1)
		}
	}
	prov = provisioner.NewFailureInjector(prov, cfg.FailureAttempts)
	snapshotter, err := snapshot.New(snapshot.Config{
		EndpointTemplate: cfg.SnapshotURL,
		Bucket:           cfg.SnapshotBucket,
		S3Endpoint:       cfg.SnapshotS3,
		Region:           cfg.SnapshotRegion,
	})
	if err != nil {
		slog.Error("snapshot configuration", "error", err)
		os.Exit(1)
	}

	metrics.Register()
	application := service.New(repo, prov, snapshotter)
	authorizer, err := auth.New(cfg.APIKeys)
	if err != nil {
		slog.Error("authentication configuration", "error", err)
		os.Exit(1)
	}
	authorizer.SetAuditSink(repo)
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		slog.Error("gRPC listener", "error", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(metrics.GRPCUnaryServerInterceptor(), authorizer.UnaryServerInterceptor()),
	)
	grpcapi.Register(grpcServer, application)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("dataplane.v1.ClusterService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)
	go func() {
		slog.Info("gRPC listening", "addr", cfg.GRPCAddr)
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			slog.Error("gRPC server", "error", err)
			stop()
		}
	}()

	workers := worker.New(application, cfg.Workers, cfg.JobPollInterval)
	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		workers.Run(ctx)
	}()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(application, authorizer).Handler(),
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
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP shutdown", "error", err)
	}
	healthServer.SetServingStatus("dataplane.v1.ClusterService", healthpb.HealthCheckResponse_NOT_SERVING)
	grpcDone := make(chan struct{})
	go func() {
		defer close(grpcDone)
		grpcServer.GracefulStop()
	}()
	select {
	case <-grpcDone:
		slog.Info("gRPC stopped")
	case <-shutdownCtx.Done():
		grpcServer.Stop()
		slog.Error("gRPC graceful shutdown timed out")
	}
	select {
	case <-workersDone:
		slog.Info("workers stopped")
	case <-time.After(cfg.WorkerShutdown):
		slog.Error("worker shutdown timed out", "timeout", cfg.WorkerShutdown)
	}
	slog.Info("shutdown complete")
}
