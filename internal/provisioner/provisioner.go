package provisioner

import (
	"context"

	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
)

type Provisioner interface {
	Provision(context.Context, model.Cluster) error
	Scale(context.Context, model.Cluster) error
	Delete(context.Context, model.Cluster) error
}
