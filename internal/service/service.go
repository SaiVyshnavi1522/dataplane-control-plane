// Package service contains the application use cases shared by every transport
// and background processor. It is the only layer that coordinates repository
// transactions, lifecycle transitions, and infrastructure provisioning.
package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/example/dataplane-control-plane/internal/model"
	"github.com/example/dataplane-control-plane/internal/provisioner"
	"github.com/example/dataplane-control-plane/internal/repository"
)

var (
	ErrInvalidArgument     = errors.New("invalid argument")
	ErrNotFound            = repository.ErrNotFound
	ErrIdempotencyConflict = repository.ErrIdempotencyConflict
	ErrInvalidTransition   = repository.ErrInvalidTransition
	ErrNoWork              = errors.New("no work available")
)

const operationTimeout = 6 * time.Minute

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{2,39}$`)

// InvalidArgumentError preserves a safe client-facing validation message while
// still supporting errors.Is(err, ErrInvalidArgument).
type InvalidArgumentError struct {
	Message string
}

func (e *InvalidArgumentError) Error() string { return e.Message }
func (e *InvalidArgumentError) Unwrap() error { return ErrInvalidArgument }

type Store interface {
	Ping(context.Context) error
	CreateCluster(context.Context, repository.CreateClusterInput) (model.Cluster, bool, error)
	GetCluster(context.Context, string) (model.Cluster, error)
	ListClusters(context.Context) ([]model.Cluster, error)
	RequestScale(context.Context, string, int) (model.Cluster, error)
	RequestDelete(context.Context, string) (model.Cluster, error)
	TransitionClusterStatus(context.Context, string, model.ClusterStatus, model.ClusterStatus, string) error
	ClaimJob(context.Context) (model.Job, error)
	CompleteJob(context.Context, int64) error
	RetryOrFailJob(context.Context, model.Job, error, int) (bool, error)
	ReleaseJob(context.Context, model.Job, error) error
}

type Service struct {
	store       Store
	provisioner provisioner.Provisioner
	newID       func() (string, error)
}

type CreateClusterInput struct {
	Name           string
	Engine         string
	Version        string
	Nodes          int
	IdempotencyKey string
}

type JobOutcome struct {
	Job       model.Job
	StartedAt time.Time
	Retry     bool
	Released  bool
	Cause     error
}

func New(store Store, infrastructure provisioner.Provisioner) *Service {
	return &Service{store: store, provisioner: infrastructure, newID: randomID}
}

func (s *Service) Ready(ctx context.Context) error {
	return s.store.Ping(ctx)
}

func (s *Service) CreateCluster(ctx context.Context, input CreateClusterInput) (model.Cluster, bool, error) {
	if err := normalizeCreate(&input); err != nil {
		return model.Cluster{}, false, err
	}
	id, err := s.newID()
	if err != nil {
		return model.Cluster{}, false, fmt.Errorf("generate cluster ID: %w", err)
	}
	return s.store.CreateCluster(ctx, repository.CreateClusterInput{
		ID:             id,
		Name:           input.Name,
		Engine:         input.Engine,
		Version:        input.Version,
		DesiredNodes:   input.Nodes,
		IdempotencyKey: input.IdempotencyKey,
	})
}

func (s *Service) ListClusters(ctx context.Context) ([]model.Cluster, error) {
	return s.store.ListClusters(ctx)
}

func (s *Service) GetCluster(ctx context.Context, id string) (model.Cluster, error) {
	if strings.TrimSpace(id) == "" {
		return model.Cluster{}, invalidArgument("cluster ID is required")
	}
	return s.store.GetCluster(ctx, id)
}

func (s *Service) ScaleCluster(ctx context.Context, id string, nodes int) (model.Cluster, error) {
	if strings.TrimSpace(id) == "" {
		return model.Cluster{}, invalidArgument("cluster ID is required")
	}
	if nodes < 1 || nodes > 3 {
		return model.Cluster{}, invalidArgument("nodes must be between 1 and 3")
	}
	return s.store.RequestScale(ctx, id, nodes)
}

func (s *Service) DeleteCluster(ctx context.Context, id string) (model.Cluster, error) {
	if strings.TrimSpace(id) == "" {
		return model.Cluster{}, invalidArgument("cluster ID is required")
	}
	return s.store.RequestDelete(ctx, id)
}

// ProcessNextJob runs one durable lifecycle operation. Polling policy and
// concurrency remain in the worker package; business transitions remain here.
func (s *Service) ProcessNextJob(ctx context.Context, maxAttempts int) (JobOutcome, error) {
	job, err := s.store.ClaimJob(ctx)
	if errors.Is(err, repository.ErrNotFound) {
		return JobOutcome{}, ErrNoWork
	}
	if err != nil {
		return JobOutcome{}, fmt.Errorf("claim job: %w", err)
	}

	outcome := JobOutcome{Job: job, StartedAt: time.Now()}
	cluster, operationErr := s.store.GetCluster(ctx, job.ClusterID)
	if operationErr == nil {
		operationErr = s.execute(ctx, job, cluster)
	}
	if operationErr == nil {
		if err := s.store.CompleteJob(ctx, job.ID); err != nil {
			return outcome, fmt.Errorf("complete job %d: %w", job.ID, err)
		}
		return outcome, nil
	}

	outcome.Cause = operationErr
	if ctx.Err() != nil && errors.Is(operationErr, context.Canceled) {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.store.ReleaseJob(releaseCtx, job, operationErr); err != nil {
			return outcome, fmt.Errorf("release canceled job %d: %w", job.ID, err)
		}
		outcome.Released = true
		return outcome, nil
	}
	retry, err := s.store.RetryOrFailJob(ctx, job, operationErr, maxAttempts)
	if err != nil {
		return outcome, fmt.Errorf("record failure for job %d: %w", job.ID, err)
	}
	outcome.Retry = retry
	if !retry {
		if err := s.markClusterFailed(ctx, job, cluster, operationErr); err != nil {
			return outcome, errors.Join(operationErr, err)
		}
	}
	return outcome, nil
}

func (s *Service) execute(ctx context.Context, job model.Job, cluster model.Cluster) error {
	jobCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()

	switch job.Type {
	case model.JobProvision:
		switch cluster.Status {
		case model.StatusRequested:
			if err := s.store.TransitionClusterStatus(ctx, cluster.ID, model.StatusRequested, model.StatusProvisioning, ""); err != nil {
				return err
			}
		case model.StatusProvisioning:
			// A retry resumes the operation from its in-progress state.
		default:
			return unexpectedJobState(job, cluster)
		}
		if err := s.provisioner.Provision(jobCtx, cluster); err != nil {
			return err
		}
		return s.store.TransitionClusterStatus(ctx, cluster.ID, model.StatusProvisioning, model.StatusRunning, "")
	case model.JobScale:
		if cluster.Status != model.StatusScaling {
			return unexpectedJobState(job, cluster)
		}
		if err := s.provisioner.Scale(jobCtx, cluster); err != nil {
			return err
		}
		return s.store.TransitionClusterStatus(ctx, cluster.ID, model.StatusScaling, model.StatusRunning, "")
	case model.JobDelete:
		if cluster.Status != model.StatusDeleting {
			return unexpectedJobState(job, cluster)
		}
		if err := s.provisioner.Delete(jobCtx, cluster); err != nil {
			return err
		}
		return s.store.TransitionClusterStatus(ctx, cluster.ID, model.StatusDeleting, model.StatusDeleted, "")
	default:
		return fmt.Errorf("unsupported job type %q", job.Type)
	}
}

func (s *Service) markClusterFailed(ctx context.Context, job model.Job, cluster model.Cluster, cause error) error {
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
	if err := s.store.TransitionClusterStatus(ctx, cluster.ID, from, model.StatusFailed, cause.Error()); err != nil {
		return fmt.Errorf("mark cluster failed: %w", err)
	}
	return nil
}

func normalizeCreate(input *CreateClusterInput) error {
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.IdempotencyKey == "" || len(input.IdempotencyKey) > 128 {
		return invalidArgument("Idempotency-Key header is required and must be <= 128 characters")
	}
	input.Name = strings.ToLower(strings.TrimSpace(input.Name))
	if !namePattern.MatchString(input.Name) {
		return invalidArgument("name must be 3-40 lowercase letters, numbers, or hyphens and start with a letter")
	}
	if input.Engine == "" {
		input.Engine = "opensearch"
	}
	if input.Engine != "opensearch" {
		return invalidArgument("only opensearch is supported in v1")
	}
	if input.Version == "" {
		input.Version = "3.8.0"
	}
	if input.Version != "3.8.0" {
		return invalidArgument("v1 currently supports OpenSearch 3.8.0 only")
	}
	if input.Nodes == 0 {
		input.Nodes = 1
	}
	if input.Nodes < 1 || input.Nodes > 3 {
		return invalidArgument("nodes must be between 1 and 3")
	}
	return nil
}

func invalidArgument(message string) error {
	return &InvalidArgumentError{Message: message}
}

func randomID() (string, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func unexpectedJobState(job model.Job, cluster model.Cluster) error {
	return fmt.Errorf(
		"%w: %s job %d found cluster in %s state",
		ErrInvalidTransition,
		job.Type,
		job.ID,
		cluster.Status,
	)
}
