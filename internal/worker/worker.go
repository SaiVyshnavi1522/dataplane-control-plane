package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/example/dataplane-control-plane/internal/metrics"
	"github.com/example/dataplane-control-plane/internal/model"
	"github.com/example/dataplane-control-plane/internal/provisioner"
	"github.com/example/dataplane-control-plane/internal/repository"
)

type Worker struct {
	repo       *repository.Repository
	prov       provisioner.Provisioner
	count      int
	poll       time.Duration
	maxRetries int
}

func New(repo *repository.Repository, prov provisioner.Provisioner, count int, poll time.Duration) *Worker {
	return &Worker{repo: repo, prov: prov, count: count, poll: poll, maxRetries: 3}
}

func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < w.count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
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
		if err := w.processOne(ctx, id); err != nil && !errors.Is(err, repository.ErrNotFound) && !errors.Is(err, context.Canceled) {
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
	job, err := w.repo.ClaimJob(ctx)
	if err != nil {
		return err
	}
	started := time.Now()
	cluster, err := w.repo.GetCluster(ctx, job.ClusterID)
	if err == nil {
		err = w.execute(ctx, job, cluster)
	}
	metrics.JobDuration.WithLabelValues(string(job.Type)).Observe(time.Since(started).Seconds())
	if err == nil {
		metrics.JobsProcessed.WithLabelValues(string(job.Type), "success").Inc()
		return w.repo.CompleteJob(ctx, job.ID)
	}
	metrics.JobsProcessed.WithLabelValues(string(job.Type), "error").Inc()
	retry, repoErr := w.repo.RetryOrFailJob(ctx, job, err, w.maxRetries)
	if repoErr != nil {
		return repoErr
	}
	if !retry {
		if statusErr := w.markClusterFailed(ctx, job, cluster, err); statusErr != nil {
			return errors.Join(err, statusErr)
		}
	}
	slog.Warn("job failed", "worker", workerID, "job", job.ID, "type", job.Type, "attempt", job.Attempts, "retry", retry, "error", err)
	return err
}

func (w *Worker) execute(ctx context.Context, job model.Job, c model.Cluster) error {
	jobCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	switch job.Type {
	case model.JobProvision:
		switch c.Status {
		case model.StatusRequested:
			if err := w.repo.TransitionClusterStatus(ctx, c.ID, model.StatusRequested, model.StatusProvisioning, ""); err != nil {
				return err
			}
		case model.StatusProvisioning:
			// A retry resumes the operation from its in-progress state.
		default:
			return unexpectedJobState(job, c)
		}
		if err := w.prov.Provision(jobCtx, c); err != nil {
			return err
		}
		return w.repo.TransitionClusterStatus(ctx, c.ID, model.StatusProvisioning, model.StatusRunning, "")
	case model.JobScale:
		if c.Status != model.StatusScaling {
			return unexpectedJobState(job, c)
		}
		if err := w.prov.Scale(jobCtx, c); err != nil {
			return err
		}
		return w.repo.TransitionClusterStatus(ctx, c.ID, model.StatusScaling, model.StatusRunning, "")
	case model.JobDelete:
		if c.Status != model.StatusDeleting {
			return unexpectedJobState(job, c)
		}
		if err := w.prov.Delete(jobCtx, c); err != nil {
			return err
		}
		return w.repo.TransitionClusterStatus(ctx, c.ID, model.StatusDeleting, model.StatusDeleted, "")
	default:
		return errors.New("unsupported job type")
	}
}

func (w *Worker) markClusterFailed(ctx context.Context, job model.Job, cluster model.Cluster, cause error) error {
	if cluster.ID == "" {
		return nil
	}
	var from model.ClusterStatus
	switch job.Type {
	case model.JobProvision:
		from = model.StatusProvisioning
	case model.JobScale:
		from = model.StatusScaling
	case model.JobDelete:
		from = model.StatusDeleting
	default:
		return nil
	}
	if err := w.repo.TransitionClusterStatus(ctx, cluster.ID, from, model.StatusFailed, cause.Error()); err != nil {
		return fmt.Errorf("mark cluster failed: %w", err)
	}
	return nil
}

func unexpectedJobState(job model.Job, cluster model.Cluster) error {
	return fmt.Errorf(
		"%w: %s job %d found cluster in %s state",
		repository.ErrInvalidTransition,
		job.Type,
		job.ID,
		cluster.Status,
	)
}
