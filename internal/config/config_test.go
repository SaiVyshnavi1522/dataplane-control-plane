package config

import (
	"strings"
	"testing"
)

func TestLoadRejectsFailureInjectionForKubernetes(t *testing.T) {
	t.Setenv("PROVISIONER", "kubernetes")
	t.Setenv("FAILURE_INJECTION_ATTEMPTS", "1")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "allowed only with the mock provisioner") {
		t.Fatalf("error=%v, want Kubernetes safety validation", err)
	}
}

func TestLoadAcceptsBoundedMockFailureInjection(t *testing.T) {
	t.Setenv("PROVISIONER", "mock")
	t.Setenv("FAILURE_INJECTION_ATTEMPTS", "2")
	t.Setenv("WORKER_SHUTDOWN_TIMEOUT", "15s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.FailureAttempts != 2 || cfg.WorkerShutdown.String() != "15s" {
		t.Fatalf("config=%+v", cfg)
	}
}
