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
	JobBackup    JobType = "BACKUP"
	JobRestore   JobType = "RESTORE"
)

type Job struct {
	ID        int64
	ClusterID string
	Type      JobType
	Attempts  int
	BackupID  string
}

type BackupStatus string

const (
	BackupRequested BackupStatus = "REQUESTED"
	BackupCreating  BackupStatus = "CREATING"
	BackupAvailable BackupStatus = "AVAILABLE"
	BackupRestoring BackupStatus = "RESTORING"
	BackupRestored  BackupStatus = "RESTORED"
	BackupFailed    BackupStatus = "FAILED"
)

type Backup struct {
	ID           string       `json:"id"`
	ClusterID    string       `json:"cluster_id"`
	SnapshotName string       `json:"snapshot_name"`
	Status       BackupStatus `json:"status"`
	LastError    string       `json:"last_error,omitempty"`
	CreatedAt    time.Time    `json:"created_at"`
	UpdatedAt    time.Time    `json:"updated_at"`
}

type AuditEvent struct {
	ID           int64          `json:"id"`
	RequestID    string         `json:"request_id"`
	Actor        string         `json:"actor"`
	Role         string         `json:"role"`
	Action       string         `json:"action"`
	ResourceType string         `json:"resource_type"`
	ResourceID   string         `json:"resource_id"`
	Outcome      string         `json:"outcome"`
	Details      map[string]any `json:"details"`
	CreatedAt    time.Time      `json:"created_at"`
}
