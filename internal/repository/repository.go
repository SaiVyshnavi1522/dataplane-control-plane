package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
)

const createClusterOperation = "CREATE_CLUSTER"

var (
	ErrNotFound            = errors.New("not found")
	ErrIdempotencyConflict = errors.New("idempotency key already used with different request")
	ErrInvalidTransition   = errors.New("invalid cluster lifecycle transition")
)

type CreateClusterInput struct {
	ID             string
	Name           string
	Engine         string
	Version        string
	DesiredNodes   int
	IdempotencyKey string
}

type createClusterPayload struct {
	Name    string `json:"name"`
	Engine  string `json:"engine"`
	Version string `json:"version"`
	Nodes   int    `json:"nodes"`
}

type Repository struct {
	db *sql.DB
}

func New(db *sql.DB) *Repository { return &Repository{db: db} }

func (r *Repository) Ping(ctx context.Context) error { return r.db.PingContext(ctx) }

func (r *Repository) CreateCluster(ctx context.Context, in CreateClusterInput) (model.Cluster, bool, error) {
	requested := createClusterPayload{
		Name:    in.Name,
		Engine:  in.Engine,
		Version: in.Version,
		Nodes:   in.DesiredNodes,
	}
	requestJSON, err := json.Marshal(requested)
	if err != nil {
		return model.Cluster{}, false, fmt.Errorf("encode idempotency request: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Cluster{}, false, fmt.Errorf("begin create cluster: %w", err)
	}
	defer tx.Rollback()

	// Serialize requests sharing a key across all control-plane replicas. The
	// transaction-scoped lock is released automatically on commit or rollback.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, in.IdempotencyKey); err != nil {
		return model.Cluster{}, false, fmt.Errorf("lock idempotency key: %w", err)
	}

	if existing, operation, recorded, err := getByIdempotencyKey(ctx, tx, in.IdempotencyKey); err == nil {
		if operation != createClusterOperation || recorded != requested {
			return model.Cluster{}, false, ErrIdempotencyConflict
		}
		return existing, true, nil
	} else if !errors.Is(err, ErrNotFound) {
		return model.Cluster{}, false, fmt.Errorf("lookup idempotency key: %w", err)
	}

	row := tx.QueryRowContext(ctx, `
		INSERT INTO clusters(id, name, engine, version, desired_nodes, status, idempotency_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		RETURNING id,name,engine,version,desired_nodes,status,idempotency_key,last_error,created_at,updated_at`,
		in.ID, in.Name, in.Engine, in.Version, in.DesiredNodes, model.StatusRequested, in.IdempotencyKey)
	cluster, err := scanCluster(row)
	if err != nil {
		return model.Cluster{}, false, fmt.Errorf("insert cluster: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO jobs(cluster_id,job_type) VALUES ($1,$2)`, cluster.ID, model.JobProvision); err != nil {
		return model.Cluster{}, false, fmt.Errorf("enqueue provision job: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO idempotency_keys(key,operation,request_payload,resource_id)
		VALUES ($1,$2,$3::jsonb,$4)`, in.IdempotencyKey, createClusterOperation, requestJSON, cluster.ID); err != nil {
		return model.Cluster{}, false, fmt.Errorf("store idempotency key: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return model.Cluster{}, false, fmt.Errorf("commit create cluster: %w", err)
	}
	return cluster, false, nil
}

func (r *Repository) GetCluster(ctx context.Context, id string) (model.Cluster, error) {
	row := r.db.QueryRowContext(ctx, `SELECT id,name,engine,version,desired_nodes,status,idempotency_key,last_error,created_at,updated_at FROM clusters WHERE id=$1`, id)
	return scanCluster(row)
}

func (r *Repository) ListClusters(ctx context.Context) ([]model.Cluster, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id,name,engine,version,desired_nodes,status,idempotency_key,last_error,created_at,updated_at FROM clusters ORDER BY created_at DESC LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Cluster
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (r *Repository) RequestScale(ctx context.Context, id string, nodes int) (model.Cluster, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Cluster{}, fmt.Errorf("begin scale request: %w", err)
	}
	defer tx.Rollback()

	current, err := getClusterForUpdate(ctx, tx, id)
	if err != nil {
		return model.Cluster{}, err
	}
	if current.Status == model.StatusScaling && current.DesiredNodes == nodes {
		return current, nil
	}
	if current.Status != model.StatusRunning {
		return model.Cluster{}, invalidTransition("scale", current.Status)
	}
	if current.DesiredNodes == nodes {
		return current, nil
	}

	row := tx.QueryRowContext(ctx, `
		UPDATE clusters SET desired_nodes=$2,status=$3,last_error='',updated_at=NOW()
		WHERE id=$1 AND status = $4
		RETURNING id,name,engine,version,desired_nodes,status,idempotency_key,last_error,created_at,updated_at`,
		id, nodes, model.StatusScaling, model.StatusRunning)
	cluster, err := scanCluster(row)
	if err != nil {
		return model.Cluster{}, fmt.Errorf("transition cluster to scaling: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO jobs(cluster_id,job_type) VALUES ($1,$2)`, id, model.JobScale); err != nil {
		return model.Cluster{}, fmt.Errorf("enqueue scale job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return model.Cluster{}, fmt.Errorf("commit scale request: %w", err)
	}
	return cluster, nil
}

func (r *Repository) RequestDelete(ctx context.Context, id string) (model.Cluster, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Cluster{}, fmt.Errorf("begin delete request: %w", err)
	}
	defer tx.Rollback()

	current, err := getClusterForUpdate(ctx, tx, id)
	if err != nil {
		return model.Cluster{}, err
	}
	if current.Status == model.StatusDeleting || current.Status == model.StatusDeleted {
		return current, nil
	}
	if current.Status != model.StatusRunning && current.Status != model.StatusFailed {
		return model.Cluster{}, invalidTransition("delete", current.Status)
	}

	row := tx.QueryRowContext(ctx, `
		UPDATE clusters SET status=$2,last_error='',updated_at=NOW()
		WHERE id=$1 AND status=$3
		RETURNING id,name,engine,version,desired_nodes,status,idempotency_key,last_error,created_at,updated_at`,
		id, model.StatusDeleting, current.Status)
	cluster, err := scanCluster(row)
	if err != nil {
		return model.Cluster{}, fmt.Errorf("transition cluster to deleting: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO jobs(cluster_id,job_type) VALUES ($1,$2)`, id, model.JobDelete); err != nil {
		return model.Cluster{}, fmt.Errorf("enqueue delete job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return model.Cluster{}, fmt.Errorf("commit delete request: %w", err)
	}
	return cluster, nil
}

func (r *Repository) CreateBackup(ctx context.Context, clusterID, backupID, snapshotName string) (model.Backup, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Backup{}, fmt.Errorf("begin backup request: %w", err)
	}
	defer tx.Rollback()
	cluster, err := getClusterForUpdate(ctx, tx, clusterID)
	if err != nil {
		return model.Backup{}, err
	}
	if cluster.Status != model.StatusRunning {
		return model.Backup{}, invalidTransition("back up", cluster.Status)
	}
	row := tx.QueryRowContext(ctx, `
		INSERT INTO backups(id,cluster_id,snapshot_name,status)
		VALUES ($1,$2,$3,$4)
		RETURNING id,cluster_id,snapshot_name,status,last_error,created_at,updated_at`,
		backupID, clusterID, snapshotName, model.BackupRequested)
	backup, err := scanBackup(row)
	if err != nil {
		return model.Backup{}, fmt.Errorf("insert backup: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO jobs(cluster_id,backup_id,job_type) VALUES ($1,$2,$3)`, clusterID, backupID, model.JobBackup); err != nil {
		return model.Backup{}, fmt.Errorf("enqueue backup job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return model.Backup{}, fmt.Errorf("commit backup request: %w", err)
	}
	return backup, nil
}

func (r *Repository) ListBackups(ctx context.Context, clusterID string) ([]model.Backup, error) {
	if _, err := r.GetCluster(ctx, clusterID); err != nil {
		return nil, err
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT id,cluster_id,snapshot_name,status,last_error,created_at,updated_at
		FROM backups WHERE cluster_id=$1 ORDER BY created_at DESC LIMIT 100`, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	backups := make([]model.Backup, 0)
	for rows.Next() {
		backup, err := scanBackup(rows)
		if err != nil {
			return nil, err
		}
		backups = append(backups, backup)
	}
	return backups, rows.Err()
}

func (r *Repository) GetBackup(ctx context.Context, id string) (model.Backup, error) {
	return scanBackup(r.db.QueryRowContext(ctx, `
		SELECT id,cluster_id,snapshot_name,status,last_error,created_at,updated_at
		FROM backups WHERE id=$1`, id))
}

func (r *Repository) RequestRestore(ctx context.Context, clusterID, backupID string) (model.Backup, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Backup{}, fmt.Errorf("begin restore request: %w", err)
	}
	defer tx.Rollback()
	cluster, err := getClusterForUpdate(ctx, tx, clusterID)
	if err != nil {
		return model.Backup{}, err
	}
	if cluster.Status != model.StatusRunning {
		return model.Backup{}, invalidTransition("restore", cluster.Status)
	}
	row := tx.QueryRowContext(ctx, `
		UPDATE backups SET status=$3,last_error='',updated_at=NOW()
		WHERE id=$1 AND cluster_id=$2 AND status IN ($4,$5)
		RETURNING id,cluster_id,snapshot_name,status,last_error,created_at,updated_at`,
		backupID, clusterID, model.BackupRestoring, model.BackupAvailable, model.BackupRestored)
	backup, err := scanBackup(row)
	if errors.Is(err, ErrNotFound) {
		return model.Backup{}, fmt.Errorf("%w: backup is missing or not restorable", ErrInvalidTransition)
	}
	if err != nil {
		return model.Backup{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO jobs(cluster_id,backup_id,job_type) VALUES ($1,$2,$3)`, clusterID, backupID, model.JobRestore); err != nil {
		return model.Backup{}, fmt.Errorf("enqueue restore job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return model.Backup{}, fmt.Errorf("commit restore request: %w", err)
	}
	return backup, nil
}

func (r *Repository) TransitionBackupStatus(ctx context.Context, id string, from, to model.BackupStatus, lastError string) error {
	result, err := r.db.ExecContext(ctx, `
		UPDATE backups SET status=$3,last_error=$4,updated_at=NOW()
		WHERE id=$1 AND status=$2`, id, from, to, lastError)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("%w: backup %s is not %s", ErrInvalidTransition, id, from)
	}
	return nil
}

func (r *Repository) RecordAudit(ctx context.Context, event model.AuditEvent) error {
	details, err := json.Marshal(event.Details)
	if err != nil {
		return fmt.Errorf("encode audit details: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO audit_events(request_id,actor,role,action,resource_type,resource_id,outcome,details)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)`,
		event.RequestID, event.Actor, event.Role, event.Action, event.ResourceType, event.ResourceID, event.Outcome, details)
	if err != nil {
		return fmt.Errorf("record audit event: %w", err)
	}
	return nil
}

func (r *Repository) ListAuditEvents(ctx context.Context, limit int) ([]model.AuditEvent, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id,request_id,actor,role,action,resource_type,resource_id,outcome,details,created_at
		FROM audit_events ORDER BY created_at DESC,id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list audit events: %w", err)
	}
	defer rows.Close()
	events := make([]model.AuditEvent, 0)
	for rows.Next() {
		var event model.AuditEvent
		var details []byte
		if err := rows.Scan(&event.ID, &event.RequestID, &event.Actor, &event.Role, &event.Action,
			&event.ResourceType, &event.ResourceID, &event.Outcome, &details, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		if err := json.Unmarshal(details, &event.Details); err != nil {
			return nil, fmt.Errorf("decode audit details: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit events: %w", err)
	}
	return events, nil
}

func (r *Repository) TransitionClusterStatus(ctx context.Context, id string, from, to model.ClusterStatus, lastError string) error {
	if !model.CanTransition(from, to) {
		return fmt.Errorf("%w: %s to %s is not defined", ErrInvalidTransition, from, to)
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE clusters
		SET status=$3,last_error=$4,updated_at=NOW()
		WHERE id=$1 AND status=$2`, id, from, to, lastError)
	if err != nil {
		return fmt.Errorf("transition cluster from %s to %s: %w", from, to, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read transition result: %w", err)
	}
	if n == 0 {
		cluster, lookupErr := r.GetCluster(ctx, id)
		if errors.Is(lookupErr, ErrNotFound) {
			return ErrNotFound
		}
		if lookupErr != nil {
			return fmt.Errorf("inspect rejected transition: %w", lookupErr)
		}
		return fmt.Errorf("%w: cluster is %s, expected %s", ErrInvalidTransition, cluster.Status, from)
	}
	return nil
}

func (r *Repository) ClaimJob(ctx context.Context) (model.Job, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Job{}, err
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `
		SELECT id,cluster_id,job_type,attempts,COALESCE(backup_id,'')
		FROM jobs
		WHERE (status='PENDING' AND available_at <= NOW())
		   OR (status='RUNNING' AND locked_at < NOW() - INTERVAL '10 minutes')
		ORDER BY id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`)
	var j model.Job
	if err := row.Scan(&j.ID, &j.ClusterID, &j.Type, &j.Attempts, &j.BackupID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Job{}, ErrNotFound
		}
		return model.Job{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET status='RUNNING',locked_at=NOW(),attempts=attempts+1,updated_at=NOW() WHERE id=$1`, j.ID); err != nil {
		return model.Job{}, err
	}
	j.Attempts++
	if err := tx.Commit(); err != nil {
		return model.Job{}, err
	}
	return j, nil
}

func (r *Repository) CompleteJob(ctx context.Context, id int64) error {
	result, err := r.db.ExecContext(ctx, `UPDATE jobs SET status='SUCCEEDED',locked_at=NULL,updated_at=NOW() WHERE id=$1 AND status='RUNNING'`, id)
	if err != nil {
		return err
	}
	return requireAffectedJob(result, id, "complete")
}

func (r *Repository) RetryOrFailJob(ctx context.Context, job model.Job, cause error, maxAttempts int) (bool, error) {
	if job.Attempts >= maxAttempts {
		result, err := r.db.ExecContext(ctx, `UPDATE jobs SET status='FAILED',locked_at=NULL,last_error=$2,updated_at=NOW() WHERE id=$1 AND status='RUNNING'`, job.ID, cause.Error())
		if err != nil {
			return false, err
		}
		return false, requireAffectedJob(result, job.ID, "fail")
	}
	backoff := time.Duration(1<<(job.Attempts-1)) * time.Second
	result, err := r.db.ExecContext(ctx, `UPDATE jobs SET status='PENDING',locked_at=NULL,available_at=NOW()+($2::int * INTERVAL '1 second'),last_error=$3,updated_at=NOW() WHERE id=$1 AND status='RUNNING'`, job.ID, int(backoff.Seconds()), cause.Error())
	if err != nil {
		return false, err
	}
	return true, requireAffectedJob(result, job.ID, "retry")
}

// ReleaseJob returns interrupted work to the queue immediately. The canceled
// claim does not consume an attempt because the operation did not get a fair
// chance to complete before process shutdown.
func (r *Repository) ReleaseJob(ctx context.Context, job model.Job, cause error) error {
	result, err := r.db.ExecContext(ctx, `
		UPDATE jobs
		SET status='PENDING',attempts=GREATEST(attempts-1,0),locked_at=NULL,
		    available_at=NOW(),last_error=$2,updated_at=NOW()
		WHERE id=$1 AND status='RUNNING'`, job.ID, cause.Error())
	if err != nil {
		return err
	}
	return requireAffectedJob(result, job.ID, "release")
}

func requireAffectedJob(result sql.Result, id int64, operation string) error {
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read %s job result: %w", operation, err)
	}
	if count == 0 {
		return fmt.Errorf("%w: cannot %s job %d because it is not running", ErrInvalidTransition, operation, id)
	}
	return nil
}

func getByIdempotencyKey(ctx context.Context, q queryer, key string) (model.Cluster, string, createClusterPayload, error) {
	row := q.QueryRowContext(ctx, `
		SELECT i.operation,i.request_payload,
		       c.id,c.name,c.engine,c.version,c.desired_nodes,c.status,c.idempotency_key,c.last_error,c.created_at,c.updated_at
		FROM idempotency_keys i
		JOIN clusters c ON c.id=i.resource_id
		WHERE i.key=$1`, key)
	var (
		cluster   model.Cluster
		operation string
		payload   createClusterPayload
		raw       []byte
	)
	if err := row.Scan(
		&operation,
		&raw,
		&cluster.ID,
		&cluster.Name,
		&cluster.Engine,
		&cluster.Version,
		&cluster.DesiredNodes,
		&cluster.Status,
		&cluster.IdempotencyKey,
		&cluster.LastError,
		&cluster.CreatedAt,
		&cluster.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Cluster{}, "", createClusterPayload{}, ErrNotFound
		}
		return model.Cluster{}, "", createClusterPayload{}, fmt.Errorf("scan idempotency record: %w", err)
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return model.Cluster{}, "", createClusterPayload{}, fmt.Errorf("decode idempotency request: %w", err)
	}
	return cluster, operation, payload, nil
}

func getClusterForUpdate(ctx context.Context, q queryer, id string) (model.Cluster, error) {
	row := q.QueryRowContext(ctx, `
		SELECT id,name,engine,version,desired_nodes,status,idempotency_key,last_error,created_at,updated_at
		FROM clusters
		WHERE id=$1
		FOR UPDATE`, id)
	return scanCluster(row)
}

func invalidTransition(operation string, status model.ClusterStatus) error {
	return fmt.Errorf("%w: cannot %s cluster in %s state", ErrInvalidTransition, operation, status)
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type scanner interface{ Scan(...any) error }

func scanCluster(s scanner) (model.Cluster, error) {
	var c model.Cluster
	if err := s.Scan(&c.ID, &c.Name, &c.Engine, &c.Version, &c.DesiredNodes, &c.Status, &c.IdempotencyKey, &c.LastError, &c.CreatedAt, &c.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Cluster{}, ErrNotFound
		}
		return model.Cluster{}, fmt.Errorf("scan cluster: %w", err)
	}
	return c, nil
}

func scanBackup(s scanner) (model.Backup, error) {
	var backup model.Backup
	if err := s.Scan(&backup.ID, &backup.ClusterID, &backup.SnapshotName, &backup.Status, &backup.LastError, &backup.CreatedAt, &backup.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Backup{}, ErrNotFound
		}
		return model.Backup{}, fmt.Errorf("scan backup: %w", err)
	}
	return backup, nil
}
