package grpcapi

import (
	"context"
	"net"
	"testing"
	"time"

	dataplanev1 "github.com/SaiVyshnavi1522/dataplane-control-plane/gen/dataplane/v1"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type applicationStub struct {
	ClusterService
	create func(context.Context, service.CreateClusterInput) (model.Cluster, bool, error)
	get    func(context.Context, string) (model.Cluster, error)
}

func (s applicationStub) CreateCluster(ctx context.Context, input service.CreateClusterInput) (model.Cluster, bool, error) {
	return s.create(ctx, input)
}

func (s applicationStub) GetCluster(ctx context.Context, id string) (model.Cluster, error) {
	return s.get(ctx, id)
}

func grpcClient(t *testing.T, application ClusterService) dataplanev1.ClusterServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	Register(server, application)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	connection, err := grpc.NewClient(
		"passthrough:///buffer",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatalf("create gRPC client: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return dataplanev1.NewClusterServiceClient(connection)
}

func TestGRPCCreateClusterUsesSharedApplicationService(t *testing.T) {
	var received service.CreateClusterInput
	now := time.Date(2026, time.September, 2, 20, 0, 0, 0, time.UTC)
	client := grpcClient(t, applicationStub{create: func(_ context.Context, input service.CreateClusterInput) (model.Cluster, bool, error) {
		received = input
		return model.Cluster{
			ID: "01j-orders-search", Name: input.Name, Engine: "opensearch", Version: "3.8.0",
			DesiredNodes: input.Nodes, Status: model.StatusRequested, CreatedAt: now, UpdatedAt: now,
		}, false, nil
	}})

	response, err := client.CreateCluster(context.Background(), &dataplanev1.CreateClusterRequest{
		IdempotencyKey: "create-orders-primary",
		Name:           "orders-search",
		Nodes:          1,
	})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	if received.Name != "orders-search" || received.Nodes != 1 || received.IdempotencyKey != "create-orders-primary" {
		t.Fatalf("application input=%+v", received)
	}
	if response.GetCluster().GetStatus() != dataplanev1.ClusterStatus_CLUSTER_STATUS_REQUESTED || response.GetCluster().GetCreatedAt().AsTime() != now {
		t.Fatalf("response=%+v", response)
	}
}

func TestGRPCMapsDomainErrorsToCanonicalCodes(t *testing.T) {
	client := grpcClient(t, applicationStub{get: func(context.Context, string) (model.Cluster, error) {
		return model.Cluster{}, service.ErrNotFound
	}})

	_, err := client.GetCluster(context.Background(), &dataplanev1.GetClusterRequest{Id: "missing-cluster"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code=%s error=%v, want NotFound", status.Code(err), err)
	}
}
