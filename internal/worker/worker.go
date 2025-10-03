package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/metrics"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/service"
)

type Worker struct {
	processor  JobProcessor
	count      int
	poll       time.Duration
	maxRetries int
}

type JobProcessor interface {
	ProcessNextJob(context.Context, int) (service.JobOutcome, error)
}

func New(processor JobProcessor, count int, poll time.Duration) *Worker {
	return &Worker{processor: processor, count: count, poll: poll, maxRetries: 3}
}

func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < w.count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			metrics.ActiveWorkers.Inc()
			defer metrics.ActiveWorkers.Dec()
			w.loop(ctx, id)
		}(i + 1)
	}
	wg.Wait()
}

func (w *Worker) loop(ctx context.Context, id int) {
	slog.Info("worker started", "worker", id)
	ticker := time.NewTicker(w.poll)
	defer ticker.Stop()
	for {
		if err := w.processOne(ctx, id); err != nil && !errors.Is(err, service.ErrNoWork) && !errors.Is(err, context.Canceled) {
			slog.Error("worker iteration failed", "worker", id, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) processOne(ctx context.Context, workerID int) error {
	outcome, err := w.processor.ProcessNextJob(ctx, w.maxRetries)
	if err != nil {
		return err
	}
	metrics.JobDuration.WithLabelValues(string(outcome.Job.Type)).Observe(time.Since(outcome.StartedAt).Seconds())
	if outcome.Cause == nil {
		metrics.JobsProcessed.WithLabelValues(string(outcome.Job.Type), "success").Inc()
		return nil
	}
	if outcome.Released {
		metrics.JobsProcessed.WithLabelValues(string(outcome.Job.Type), "canceled").Inc()
		slog.Info("job released during shutdown", "worker", workerID, "job", outcome.Job.ID, "type", outcome.Job.Type)
		return nil
	}
	metrics.JobsProcessed.WithLabelValues(string(outcome.Job.Type), "error").Inc()
	if outcome.Retry {
		metrics.JobRetries.WithLabelValues(string(outcome.Job.Type)).Inc()
	}
	slog.Warn("job failed", "worker", workerID, "job", outcome.Job.ID, "type", outcome.Job.Type, "attempt", outcome.Job.Attempts, "retry", outcome.Retry, "error", outcome.Cause)
	return nil
}
