package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr        string
	DatabaseURL     string
	Provisioner     string
	Kubeconfig      string
	K8sNamespace    string
	OpenSearchImage string
	Workers         int
	JobPollInterval time.Duration
	WorkerShutdown  time.Duration
	FailureAttempts int
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:        env("HTTP_ADDR", ":8080"),
		DatabaseURL:     env("DATABASE_URL", "postgres://dataplane:dataplane@localhost:55432/dataplane?sslmode=disable"),
		Provisioner:     env("PROVISIONER", "mock"),
		Kubeconfig:      os.Getenv("KUBECONFIG"),
		K8sNamespace:    env("K8S_NAMESPACE", "dataplane-clusters"),
		OpenSearchImage: env("OPENSEARCH_IMAGE", "opensearchproject/opensearch:3.8.0"),
		Workers:         envInt("WORKERS", 4),
		JobPollInterval: envDuration("JOB_POLL_INTERVAL", time.Second),
		WorkerShutdown:  envDuration("WORKER_SHUTDOWN_TIMEOUT", 10*time.Second),
		FailureAttempts: envInt("FAILURE_INJECTION_ATTEMPTS", 0),
	}
	if cfg.Workers < 1 || cfg.Workers > 32 {
		return Config{}, fmt.Errorf("WORKERS must be between 1 and 32")
	}
	if cfg.Provisioner != "mock" && cfg.Provisioner != "kubernetes" {
		return Config{}, fmt.Errorf("PROVISIONER must be mock or kubernetes")
	}
	if cfg.JobPollInterval <= 0 {
		return Config{}, fmt.Errorf("JOB_POLL_INTERVAL must be greater than zero")
	}
	if cfg.WorkerShutdown <= 0 || cfg.WorkerShutdown > time.Minute {
		return Config{}, fmt.Errorf("WORKER_SHUTDOWN_TIMEOUT must be greater than zero and at most 1m")
	}
	if cfg.FailureAttempts < 0 || cfg.FailureAttempts > 10 {
		return Config{}, fmt.Errorf("FAILURE_INJECTION_ATTEMPTS must be between 0 and 10")
	}
	if cfg.FailureAttempts > 0 && cfg.Provisioner != "mock" {
		return Config{}, fmt.Errorf("FAILURE_INJECTION_ATTEMPTS is allowed only with the mock provisioner")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
