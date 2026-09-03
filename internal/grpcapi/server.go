package grpcapi

import (
	"context"
	"errors"
	"log/slog"

	dataplanev1 "github.com/SaiVyshnavi1522/dataplane-control-plane/gen/dataplane/v1"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ClusterService interface {
	CreateCluster(context.Context, service.CreateClusterInput) (model.Cluster, bool, error)
	ListClusters(context.Context) ([]model.Cluster, error)
	GetCluster(context.Context, string) (model.Cluster, error)
	ScaleCluster(context.Context, string, int) (model.Cluster, error)
	DeleteCluster(context.Context, string) (model.Cluster, error)
}

type Server struct {
	dataplanev1.UnimplementedClusterServiceServer
	service ClusterService
}

func Register(registrar grpc.ServiceRegistrar, application ClusterService) {
	dataplanev1.RegisterClusterServiceServer(registrar, &Server{service: application})
}

func (s *Server) CreateCluster(ctx context.Context, request *dataplanev1.CreateClusterRequest) (*dataplanev1.CreateClusterResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	cluster, reused, err := s.service.CreateCluster(ctx, service.CreateClusterInput{
		Name:           request.GetName(),
		Engine:         request.GetEngine(),
		Version:        request.GetVersion(),
		Nodes:          int(request.GetNodes()),
		IdempotencyKey: request.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, mapError("create cluster", err)
	}
	return &dataplanev1.CreateClusterResponse{Cluster: toProtoCluster(cluster), Reused: reused}, nil
}

func (s *Server) GetCluster(ctx context.Context, request *dataplanev1.GetClusterRequest) (*dataplanev1.Cluster, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	cluster, err := s.service.GetCluster(ctx, request.GetId())
	if err != nil {
		return nil, mapError("get cluster", err)
	}
	return toProtoCluster(cluster), nil
}

func (s *Server) ListClusters(ctx context.Context, _ *dataplanev1.ListClustersRequest) (*dataplanev1.ListClustersResponse, error) {
	clusters, err := s.service.ListClusters(ctx)
	if err != nil {
		return nil, mapError("list clusters", err)
	}
	response := &dataplanev1.ListClustersResponse{Clusters: make([]*dataplanev1.Cluster, 0, len(clusters))}
	for _, cluster := range clusters {
		response.Clusters = append(response.Clusters, toProtoCluster(cluster))
	}
	return response, nil
}

func (s *Server) ScaleCluster(ctx context.Context, request *dataplanev1.ScaleClusterRequest) (*dataplanev1.Cluster, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	cluster, err := s.service.ScaleCluster(ctx, request.GetId(), int(request.GetNodes()))
	if err != nil {
		return nil, mapError("scale cluster", err)
	}
	return toProtoCluster(cluster), nil
}

func (s *Server) DeleteCluster(ctx context.Context, request *dataplanev1.DeleteClusterRequest) (*dataplanev1.Cluster, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	cluster, err := s.service.DeleteCluster(ctx, request.GetId())
	if err != nil {
		return nil, mapError("delete cluster", err)
	}
	return toProtoCluster(cluster), nil
}

func mapError(operation string, err error) error {
	switch {
	case errors.Is(err, service.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, service.ErrNotFound):
		return status.Error(codes.NotFound, "cluster not found")
	case errors.Is(err, service.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, service.ErrInvalidTransition):
		return status.Error(codes.FailedPrecondition, "cluster lifecycle state does not allow this operation")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		slog.Error(operation, "error", err)
		return status.Error(codes.Internal, "internal server error")
	}
}

func toProtoCluster(cluster model.Cluster) *dataplanev1.Cluster {
	return &dataplanev1.Cluster{
		Id:           cluster.ID,
		Name:         cluster.Name,
		Engine:       cluster.Engine,
		Version:      cluster.Version,
		DesiredNodes: int32(cluster.DesiredNodes),
		Status:       toProtoStatus(cluster.Status),
		LastError:    cluster.LastError,
		CreatedAt:    timestamppb.New(cluster.CreatedAt),
		UpdatedAt:    timestamppb.New(cluster.UpdatedAt),
	}
}

func toProtoStatus(clusterStatus model.ClusterStatus) dataplanev1.ClusterStatus {
	switch clusterStatus {
	case model.StatusRequested:
		return dataplanev1.ClusterStatus_CLUSTER_STATUS_REQUESTED
	case model.StatusProvisioning:
		return dataplanev1.ClusterStatus_CLUSTER_STATUS_PROVISIONING
	case model.StatusRunning:
		return dataplanev1.ClusterStatus_CLUSTER_STATUS_RUNNING
	case model.StatusScaling:
		return dataplanev1.ClusterStatus_CLUSTER_STATUS_SCALING
	case model.StatusDeleting:
		return dataplanev1.ClusterStatus_CLUSTER_STATUS_DELETING
	case model.StatusDeleted:
		return dataplanev1.ClusterStatus_CLUSTER_STATUS_DELETED
	case model.StatusFailed:
		return dataplanev1.ClusterStatus_CLUSTER_STATUS_FAILED
	default:
		return dataplanev1.ClusterStatus_CLUSTER_STATUS_UNSPECIFIED
	}
}
