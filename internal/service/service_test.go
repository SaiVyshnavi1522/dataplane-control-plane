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
