package provisioner

import (
	"context"
	"log/slog"
	"time"

	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
)

type Mock struct{}

func (Mock) Provision(ctx context.Context, c model.Cluster) error {
	slog.Info("mock provision", "cluster_id", c.ID, "nodes", c.DesiredNodes)
	return wait(ctx, 250*time.Millisecond)
}
func (Mock) Scale(ctx context.Context, c model.Cluster) error {
	slog.Info("mock scale", "cluster_id", c.ID, "nodes", c.DesiredNodes)
	return wait(ctx, 150*time.Millisecond)
}
func (Mock) Delete(ctx context.Context, c model.Cluster) error {
	slog.Info("mock delete", "cluster_id", c.ID)
	return wait(ctx, 100*time.Millisecond)
}
func wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
