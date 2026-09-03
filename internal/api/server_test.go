package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/example/dataplane-control-plane/internal/model"
	"github.com/example/dataplane-control-plane/internal/service"
)

type serviceStub struct {
	ClusterService
	create func(context.Context, service.CreateClusterInput) (model.Cluster, bool, error)
}

func (s serviceStub) CreateCluster(ctx context.Context, input service.CreateClusterInput) (model.Cluster, bool, error) {
	return s.create(ctx, input)
}

func TestCreateEndpointDelegatesToApplicationService(t *testing.T) {
	var received service.CreateClusterInput
	application := serviceStub{create: func(_ context.Context, input service.CreateClusterInput) (model.Cluster, bool, error) {
		received = input
		return model.Cluster{ID: "01j-orders-search", Name: "orders-search", Status: model.StatusRequested}, false, nil
	}}
	handler := New(application).Handler()
	request := httptest.NewRequest(http.MethodPost, "/v1/clusters", strings.NewReader(`{"name":"orders-search","nodes":1}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "create-orders-primary")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusAccepted, response.Body.String())
	}
	if received.Name != "orders-search" || received.Nodes != 1 || received.IdempotencyKey != "create-orders-primary" {
		t.Fatalf("service input = %+v", received)
	}
}

func TestCreateEndpointMapsApplicationValidationError(t *testing.T) {
	application := serviceStub{create: func(context.Context, service.CreateClusterInput) (model.Cluster, bool, error) {
		return model.Cluster{}, false, &service.InvalidArgumentError{Message: "nodes must be between 1 and 3"}
	}}
	handler := New(application).Handler()
	request := httptest.NewRequest(http.MethodPost, "/v1/clusters", strings.NewReader(`{"name":"orders-search","nodes":4}`))
	request.Header.Set("Idempotency-Key", "create-orders-primary")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusBadRequest, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"code":"INVALID_REQUEST"`) {
		t.Fatalf("unexpected error body: %s", response.Body.String())
	}
}
