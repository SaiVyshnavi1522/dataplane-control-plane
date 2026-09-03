package provisioner

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
)

var ErrInjectedFailure = errors.New("injected provisioner failure")

// FailureInjector deterministically fails the first N calls for each cluster
// and operation. It is intended for local resilience exercises only.
type FailureInjector struct {
	next     Provisioner
	attempts int
	mu       sync.Mutex
	seen     map[string]int
}

func NewFailureInjector(next Provisioner, attempts int) Provisioner {
	if attempts == 0 {
		return next
	}
	return &FailureInjector{next: next, attempts: attempts, seen: make(map[string]int)}
}

func (f *FailureInjector) Provision(ctx context.Context, cluster model.Cluster) error {
	if err := f.inject(ctx, "provision", cluster.ID); err != nil {
		return err
	}
	return f.next.Provision(ctx, cluster)
}

func (f *FailureInjector) Scale(ctx context.Context, cluster model.Cluster) error {
	if err := f.inject(ctx, "scale", cluster.ID); err != nil {
		return err
	}
	return f.next.Scale(ctx, cluster)
}

func (f *FailureInjector) Delete(ctx context.Context, cluster model.Cluster) error {
	if err := f.inject(ctx, "delete", cluster.ID); err != nil {
		return err
	}
	return f.next.Delete(ctx, cluster)
}

func (f *FailureInjector) inject(ctx context.Context, operation, clusterID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := operation + ":" + clusterID
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen[key]++
	if f.seen[key] <= f.attempts {
		return fmt.Errorf("%w: %s attempt %d for cluster %s", ErrInjectedFailure, operation, f.seen[key], clusterID)
	}
	return nil
}
