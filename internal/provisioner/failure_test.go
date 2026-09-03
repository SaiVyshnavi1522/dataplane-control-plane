package provisioner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/example/dataplane-control-plane/internal/model"
)

type countingProvisioner struct {
	provisions atomic.Int32
	scales     atomic.Int32
	deletes    atomic.Int32
}

func (p *countingProvisioner) Provision(context.Context, model.Cluster) error {
	p.provisions.Add(1)
	return nil
}

func (p *countingProvisioner) Scale(context.Context, model.Cluster) error {
	p.scales.Add(1)
	return nil
}

func (p *countingProvisioner) Delete(context.Context, model.Cluster) error {
	p.deletes.Add(1)
	return nil
}

func TestFailureInjectorFailsConfiguredAttemptsThenDelegates(t *testing.T) {
	next := &countingProvisioner{}
	injected := NewFailureInjector(next, 2)
	cluster := model.Cluster{ID: "01j-checkout-search", Name: "checkout-search"}

	for attempt := 1; attempt <= 2; attempt++ {
		if err := injected.Provision(context.Background(), cluster); !errors.Is(err, ErrInjectedFailure) {
			t.Fatalf("attempt %d error=%v, want injected failure", attempt, err)
		}
	}
	if err := injected.Provision(context.Background(), cluster); err != nil {
		t.Fatalf("third attempt: %v", err)
	}
	if got := next.provisions.Load(); got != 1 {
		t.Fatalf("delegate calls=%d, want 1", got)
	}
}

func TestFailureInjectorTracksOperationsIndependently(t *testing.T) {
	next := &countingProvisioner{}
	injected := NewFailureInjector(next, 1)
	cluster := model.Cluster{ID: "01j-checkout-search", Name: "checkout-search"}

	if err := injected.Provision(context.Background(), cluster); !errors.Is(err, ErrInjectedFailure) {
		t.Fatalf("provision error=%v, want injected failure", err)
	}
	if err := injected.Scale(context.Background(), cluster); !errors.Is(err, ErrInjectedFailure) {
		t.Fatalf("scale error=%v, want injected failure", err)
	}
	if err := injected.Delete(context.Background(), cluster); !errors.Is(err, ErrInjectedFailure) {
		t.Fatalf("delete error=%v, want injected failure", err)
	}
}
