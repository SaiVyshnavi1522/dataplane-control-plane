package service

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/example/dataplane-control-plane/internal/model"
	"github.com/example/dataplane-control-plane/internal/repository"
)

type storeStub struct {
	Store
	create     func(context.Context, repository.CreateClusterInput) (model.Cluster, bool, error)
	claim      func(context.Context) (model.Job, error)
	get        func(context.Context, string) (model.Cluster, error)
	transition func(context.Context, string, model.ClusterStatus, model.ClusterStatus, string) error
	complete   func(context.Context, int64) error
	retry      func(context.Context, model.Job, error, int) (bool, error)
	release    func(context.Context, model.Job, error) error
}

func (s storeStub) CreateCluster(ctx context.Context, input repository.CreateClusterInput) (model.Cluster, bool, error) {
	return s.create(ctx, input)
}

func (s storeStub) ClaimJob(ctx context.Context) (model.Job, error) { return s.claim(ctx) }

func (s storeStub) GetCluster(ctx context.Context, id string) (model.Cluster, error) {
	return s.get(ctx, id)
}

func (s storeStub) TransitionClusterStatus(ctx context.Context, id string, from, to model.ClusterStatus, lastError string) error {
	return s.transition(ctx, id, from, to, lastError)
}

func (s storeStub) CompleteJob(ctx context.Context, id int64) error { return s.complete(ctx, id) }

func (s storeStub) RetryOrFailJob(ctx context.Context, job model.Job, cause error, maxAttempts int) (bool, error) {
	return s.retry(ctx, job, cause, maxAttempts)
}

func (s storeStub) ReleaseJob(ctx context.Context, job model.Job, cause error) error {
	return s.release(ctx, job, cause)
}

type provisionerStub struct {
	provision func(context.Context, model.Cluster) error
}

func (p provisionerStub) Provision(ctx context.Context, cluster model.Cluster) error {
	return p.provision(ctx, cluster)
}

func (provisionerStub) Scale(context.Context, model.Cluster) error  { return nil }
func (provisionerStub) Delete(context.Context, model.Cluster) error { return nil }

func TestCreateClusterNormalizesDefaultsBeforePersistence(t *testing.T) {
	var recorded repository.CreateClusterInput
	store := storeStub{create: func(_ context.Context, input repository.CreateClusterInput) (model.Cluster, bool, error) {
		recorded = input
		return model.Cluster{ID: input.ID, Name: input.Name}, false, nil
	}}
	application := New(store, provisionerStub{})
	application.newID = func() (string, error) { return "01j-orders-search", nil }

	cluster, reused, err := application.CreateCluster(context.Background(), CreateClusterInput{
		Name:           " Orders-Search ",
		IdempotencyKey: " create-orders-primary ",
	})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	if reused {
		t.Fatal("new request was reported as a replay")
	}
	want := repository.CreateClusterInput{
		ID:             "01j-orders-search",
		Name:           "orders-search",
		Engine:         "opensearch",
		Version:        "3.8.0",
		DesiredNodes:   1,
		IdempotencyKey: "create-orders-primary",
	}
	if !reflect.DeepEqual(recorded, want) {
		t.Fatalf("persisted input = %+v, want %+v", recorded, want)
	}
	if cluster.ID != want.ID {
		t.Fatalf("cluster ID = %q, want %q", cluster.ID, want.ID)
	}
}

func TestCreateClusterRejectsInvalidInputBeforePersistence(t *testing.T) {
	application := New(storeStub{create: func(context.Context, repository.CreateClusterInput) (model.Cluster, bool, error) {
		t.Fatal("persistence called for invalid input")
		return model.Cluster{}, false, nil
	}}, provisionerStub{})

	_, _, err := application.CreateCluster(context.Background(), CreateClusterInput{
		Name:           "orders-search",
		Nodes:          4,
		IdempotencyKey: "create-orders-primary",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v, want invalid argument", err)
	}
}

func TestProcessNextJobCompletesProvisioningLifecycle(t *testing.T) {
	job := model.Job{ID: 41, ClusterID: "01j-orders-search", Type: model.JobProvision, Attempts: 1}
	cluster := model.Cluster{ID: job.ClusterID, Name: "orders-search", Status: model.StatusRequested, DesiredNodes: 1}
	var transitions [][2]model.ClusterStatus
	completed := false
	provisioned := false

	store := storeStub{
		claim: func(context.Context) (model.Job, error) { return job, nil },
		get:   func(context.Context, string) (model.Cluster, error) { return cluster, nil },
		transition: func(_ context.Context, _ string, from, to model.ClusterStatus, _ string) error {
			transitions = append(transitions, [2]model.ClusterStatus{from, to})
			return nil
		},
		complete: func(_ context.Context, id int64) error {
			completed = id == job.ID
			return nil
		},
	}
	application := New(store, provisionerStub{provision: func(_ context.Context, got model.Cluster) error {
		provisioned = got.ID == cluster.ID
		return nil
	}})

	outcome, err := application.ProcessNextJob(context.Background(), 3)
	if err != nil {
		t.Fatalf("process job: %v", err)
	}
	if outcome.Cause != nil || !completed || !provisioned {
		t.Fatalf("outcome=%+v completed=%v provisioned=%v", outcome, completed, provisioned)
	}
	wantTransitions := [][2]model.ClusterStatus{
		{model.StatusRequested, model.StatusProvisioning},
		{model.StatusProvisioning, model.StatusRunning},
	}
	if !reflect.DeepEqual(transitions, wantTransitions) {
		t.Fatalf("transitions = %v, want %v", transitions, wantTransitions)
	}
}

func TestProcessNextJobMapsAnEmptyQueue(t *testing.T) {
	application := New(storeStub{claim: func(context.Context) (model.Job, error) {
		return model.Job{}, repository.ErrNotFound
	}}, provisionerStub{})

	_, err := application.ProcessNextJob(context.Background(), 3)
	if !errors.Is(err, ErrNoWork) {
		t.Fatalf("error = %v, want no work", err)
	}
}

func TestProcessNextJobSchedulesRetryAfterProvisionerFailure(t *testing.T) {
	injected := errors.New("transient infrastructure failure")
	job := model.Job{ID: 52, ClusterID: "01j-catalog-search", Type: model.JobProvision, Attempts: 1}
	cluster := model.Cluster{ID: job.ClusterID, Name: "catalog-search", Status: model.StatusProvisioning}
	recordedMaxAttempts := 0
	store := storeStub{
		claim: func(context.Context) (model.Job, error) { return job, nil },
		get:   func(context.Context, string) (model.Cluster, error) { return cluster, nil },
		retry: func(_ context.Context, got model.Job, cause error, maxAttempts int) (bool, error) {
			if got.ID != job.ID || !errors.Is(cause, injected) {
				t.Fatalf("retry received job=%+v cause=%v", got, cause)
			}
			recordedMaxAttempts = maxAttempts
			return true, nil
		},
	}
	application := New(store, provisionerStub{provision: func(context.Context, model.Cluster) error { return injected }})

	outcome, err := application.ProcessNextJob(context.Background(), 3)
	if err != nil {
		t.Fatalf("process job: %v", err)
	}
	if !outcome.Retry || !errors.Is(outcome.Cause, injected) || recordedMaxAttempts != 3 {
		t.Fatalf("outcome=%+v maxAttempts=%d", outcome, recordedMaxAttempts)
	}
}

func TestProcessNextJobMarksClusterFailedAfterFinalAttempt(t *testing.T) {
	injected := errors.New("persistent infrastructure failure")
	job := model.Job{ID: 63, ClusterID: "01j-events-search", Type: model.JobScale, Attempts: 3}
	cluster := model.Cluster{ID: job.ClusterID, Name: "events-search", Status: model.StatusScaling}
	var transition [2]model.ClusterStatus
	store := storeStub{
		claim: func(context.Context) (model.Job, error) { return job, nil },
		get:   func(context.Context, string) (model.Cluster, error) { return cluster, nil },
		retry: func(context.Context, model.Job, error, int) (bool, error) { return false, nil },
		transition: func(_ context.Context, _ string, from, to model.ClusterStatus, lastError string) error {
			transition = [2]model.ClusterStatus{from, to}
			if lastError != injected.Error() {
				t.Fatalf("last error = %q", lastError)
			}
			return nil
		},
	}
	application := New(store, provisionerStub{})
	application.provisioner = provisionerWithScaleError{err: injected}

	outcome, err := application.ProcessNextJob(context.Background(), 3)
	if err != nil {
		t.Fatalf("process job: %v", err)
	}
	if outcome.Retry || !errors.Is(outcome.Cause, injected) {
		t.Fatalf("outcome=%+v", outcome)
	}
	want := [2]model.ClusterStatus{model.StatusScaling, model.StatusFailed}
	if transition != want {
		t.Fatalf("transition=%v, want %v", transition, want)
	}
}

type provisionerWithScaleError struct{ err error }

func (p provisionerWithScaleError) Provision(context.Context, model.Cluster) error { return nil }
func (p provisionerWithScaleError) Scale(context.Context, model.Cluster) error     { return p.err }
func (p provisionerWithScaleError) Delete(context.Context, model.Cluster) error    { return nil }

func TestProcessNextJobReleasesCanceledWorkWithoutConsumingRetry(t *testing.T) {
	job := model.Job{ID: 74, ClusterID: "01j-metrics-search", Type: model.JobProvision, Attempts: 1}
	cluster := model.Cluster{ID: job.ClusterID, Name: "metrics-search", Status: model.StatusProvisioning}
	started := make(chan struct{})
	released := make(chan struct{})
	store := storeStub{
		claim: func(context.Context) (model.Job, error) { return job, nil },
		get:   func(context.Context, string) (model.Cluster, error) { return cluster, nil },
		release: func(ctx context.Context, got model.Job, cause error) error {
			if ctx.Err() != nil {
				t.Fatalf("release context is already canceled: %v", ctx.Err())
			}
			if got.ID != job.ID || !errors.Is(cause, context.Canceled) {
				t.Fatalf("release received job=%+v cause=%v", got, cause)
			}
			close(released)
			return nil
		},
	}
	application := New(store, provisionerStub{provision: func(ctx context.Context, _ model.Cluster) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan JobOutcome, 1)
	errs := make(chan error, 1)
	go func() {
		outcome, err := application.ProcessNextJob(ctx, 3)
		done <- outcome
		errs <- err
	}()
	<-started
	cancel()

	outcome := <-done
	if err := <-errs; err != nil {
		t.Fatalf("process canceled job: %v", err)
	}
	<-released
	if !outcome.Released || outcome.Retry || !errors.Is(outcome.Cause, context.Canceled) {
		t.Fatalf("outcome=%+v", outcome)
	}
}
