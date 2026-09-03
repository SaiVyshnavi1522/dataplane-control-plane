package model

import "time"

type ClusterStatus string

const (
	StatusRequested    ClusterStatus = "REQUESTED"
	StatusProvisioning ClusterStatus = "PROVISIONING"
	StatusRunning      ClusterStatus = "RUNNING"
	StatusScaling      ClusterStatus = "SCALING"
	StatusDeleting     ClusterStatus = "DELETING"
	StatusDeleted      ClusterStatus = "DELETED"
	StatusFailed       ClusterStatus = "FAILED"
)

// CanTransition reports whether a lifecycle transition is part of the control
// plane's state machine. Callers still use database compare-and-set updates to
// ensure that an allowed transition is applied to the state they observed.
func CanTransition(from, to ClusterStatus) bool {
	switch from {
	case StatusRequested:
		return to == StatusProvisioning
	case StatusProvisioning:
		return to == StatusRunning || to == StatusFailed
	case StatusRunning:
		return to == StatusScaling || to == StatusDeleting
	case StatusScaling:
		return to == StatusRunning || to == StatusFailed
	case StatusDeleting:
		return to == StatusDeleted || to == StatusFailed
	case StatusFailed:
		return to == StatusDeleting
	default:
		return false
	}
}

type Cluster struct {
	ID             string        `json:"id"`
	Name           string        `json:"name"`
	Engine         string        `json:"engine"`
	Version        string        `json:"version"`
	DesiredNodes   int           `json:"desired_nodes"`
	Status         ClusterStatus `json:"status"`
	IdempotencyKey string        `json:"-"`
	LastError      string        `json:"last_error,omitempty"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
}

type JobType string

const (
	JobProvision JobType = "PROVISION"
	JobScale     JobType = "SCALE"
	JobDelete    JobType = "DELETE"
)

type Job struct {
	ID        int64
	ClusterID string
	Type      JobType
	Attempts  int
}
