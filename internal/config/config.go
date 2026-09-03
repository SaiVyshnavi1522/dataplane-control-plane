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
	}
	if cfg.Workers < 1 || cfg.Workers > 32 {
		return Config{}, fmt.Errorf("WORKERS must be between 1 and 32")
	}
	if cfg.Provisioner != "mock" && cfg.Provisioner != "kubernetes" {
		return Config{}, fmt.Errorf("PROVISIONER must be mock or kubernetes")
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
