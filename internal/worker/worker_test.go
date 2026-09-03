package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/dataplane-control-plane/internal/model"
	"github.com/example/dataplane-control-plane/internal/service"
)

type blockingProcessor struct {
	started chan struct{}
	calls   atomic.Int32
}

func (p *blockingProcessor) ProcessNextJob(ctx context.Context, _ int) (service.JobOutcome, error) {
	p.calls.Add(1)
	close(p.started)
	startedAt := time.Now()
	<-ctx.Done()
	return service.JobOutcome{
		Job:       model.Job{ID: 85, ClusterID: "01j-ledger-search", Type: model.JobProvision, Attempts: 1},
		StartedAt: startedAt,
		Released:  true,
		Cause:     ctx.Err(),
	}, nil
}

func TestRunWaitsForInFlightCancellationAndStops(t *testing.T) {
	processor := &blockingProcessor{started: make(chan struct{})}
	workers := New(processor, 1, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		workers.Run(ctx)
		close(done)
	}()

	<-processor.started
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
	if got := processor.calls.Load(); got != 1 {
		t.Fatalf("processor calls=%d, want 1", got)
	}
}
