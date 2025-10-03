package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	appdb "github.com/SaiVyshnavi1522/dataplane-control-plane/internal/db"
	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
)

func TestCreateClusterReplayUsesOriginalRequestAfterScaling(t *testing.T) {
	repo, sqlDB := openRepositoryDatabase(t)
	ctx := context.Background()
	original := CreateClusterInput{
		ID:             "01JPRIMARYSEARCH000000000001",
		Name:           "search-primary",
		Engine:         "opensearch",
		Version:        "3.8.0",
		DesiredNodes:   1,
		IdempotencyKey: "create-search-primary-2026-09",
	}

	created, reused, err := repo.CreateCluster(ctx, original)
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	if reused {
		t.Fatal("first request was reported as a replay")
	}
	completeProvisioning(t, repo, created.ID)
	if _, err := repo.RequestScale(ctx, created.ID, 2); err != nil {
		t.Fatalf("request scale: %v", err)
	}

	replay := original
	replay.ID = "01JPRIMARYSEARCH000000000002"
	returned, reused, err := repo.CreateCluster(ctx, replay)
	if err != nil {
		t.Fatalf("replay original create request: %v", err)
	}
	if !reused {
		t.Fatal("replayed request was not identified as a replay")
	}
	if returned.ID != created.ID {
		t.Fatalf("replay returned cluster %q, want %q", returned.ID, created.ID)
	}
	if returned.DesiredNodes != 2 {
		t.Fatalf("replay returned %d desired nodes, want current value 2", returned.DesiredNodes)
	}

	assertRowCount(t, sqlDB, "clusters", 1)
	assertRowCount(t, sqlDB, "jobs", 2)
	assertRowCount(t, sqlDB, "idempotency_keys", 1)
}

func TestCreateClusterRejectsChangedPayloadForExistingKey(t *testing.T) {
	repo, _ := openRepositoryDatabase(t)
	ctx := context.Background()
	original := CreateClusterInput{
		ID:             "01JCATALOGSEARCH00000000001",
		Name:           "catalog-search",
		Engine:         "opensearch",
		Version:        "3.8.0",
		DesiredNodes:   1,
		IdempotencyKey: "create-catalog-search-2026-09",
	}
	if _, _, err := repo.CreateCluster(ctx, original); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	changed := original
	changed.ID = "01JCATALOGSEARCH00000000002"
	changed.DesiredNodes = 2
	if _, _, err := repo.CreateCluster(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed replay error = %v, want %v", err, ErrIdempotencyConflict)
	}
}

func TestConcurrentCreateClusterReplayCreatesOneResourceAndJob(t *testing.T) {
	repo, sqlDB := openRepositoryDatabase(t)
	const callers = 12
	start := make(chan struct{})
	type result struct {
		cluster model.Cluster
		reused  bool
		err     error
	}
	results := make(chan result, callers)
	var group sync.WaitGroup

	for index := 0; index < callers; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			cluster, reused, err := repo.CreateCluster(context.Background(), CreateClusterInput{
				ID:             fmt.Sprintf("01JORDERSSEARCH%012d", index),
				Name:           "orders-search",
				Engine:         "opensearch",
				Version:        "3.8.0",
				DesiredNodes:   1,
				IdempotencyKey: "create-orders-search-2026-09",
			})
			results <- result{cluster: cluster, reused: reused, err: err}
		}(index)
	}
	close(start)
	group.Wait()
	close(results)

	created := 0
	resourceID := ""
	for outcome := range results {
		if outcome.err != nil {
			t.Fatalf("concurrent create: %v", outcome.err)
		}
		if !outcome.reused {
			created++
		}
		if resourceID == "" {
			resourceID = outcome.cluster.ID
		} else if outcome.cluster.ID != resourceID {
			t.Fatalf("received resource IDs %q and %q for one key", resourceID, outcome.cluster.ID)
		}
	}
	if created != 1 {
		t.Fatalf("new resource responses = %d, want 1", created)
	}

	assertRowCount(t, sqlDB, "clusters", 1)
	assertRowCount(t, sqlDB, "jobs", 1)
	assertRowCount(t, sqlDB, "idempotency_keys", 1)
}

func TestScaleRejectsClusterBeforeProvisioningCompletes(t *testing.T) {
	repo, sqlDB := openRepositoryDatabase(t)
	cluster, _, err := repo.CreateCluster(context.Background(), CreateClusterInput{
		ID:             "01JINVENTORYSEARCH000000001",
		Name:           "inventory-search",
		Engine:         "opensearch",
		Version:        "3.8.0",
		DesiredNodes:   1,
		IdempotencyKey: "create-inventory-search-2026-09",
	})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	if _, err := repo.RequestScale(context.Background(), cluster.ID, 2); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("scale requested cluster error = %v, want %v", err, ErrInvalidTransition)
	}
	assertRowCount(t, sqlDB, "jobs", 1)
}

func TestDeleteRejectsClusterWhileProvisioning(t *testing.T) {
	repo, sqlDB := openRepositoryDatabase(t)
	cluster, _, err := repo.CreateCluster(context.Background(), CreateClusterInput{
		ID:             "01JACTIVITYSEARCH0000000001",
		Name:           "activity-search",
		Engine:         "opensearch",
		Version:        "3.8.0",
		DesiredNodes:   1,
		IdempotencyKey: "create-activity-search-2026-09",
	})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	if err := repo.TransitionClusterStatus(context.Background(), cluster.ID, model.StatusRequested, model.StatusProvisioning, ""); err != nil {
		t.Fatalf("start provisioning: %v", err)
	}

	if _, err := repo.RequestDelete(context.Background(), cluster.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("delete provisioning cluster error = %v, want %v", err, ErrInvalidTransition)
	}
	assertRowCount(t, sqlDB, "jobs", 1)
}

func TestConcurrentScaleAndDeleteQueuesOneLifecycleOperation(t *testing.T) {
	repo, sqlDB := openRepositoryDatabase(t)
	cluster, _, err := repo.CreateCluster(context.Background(), CreateClusterInput{
		ID:             "01JSESSIONSSEARCH0000000001",
		Name:           "sessions-search",
		Engine:         "opensearch",
		Version:        "3.8.0",
		DesiredNodes:   1,
		IdempotencyKey: "create-sessions-search-2026-09",
	})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	completeProvisioning(t, repo, cluster.ID)

	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, err := repo.RequestScale(context.Background(), cluster.ID, 2)
		results <- err
	}()
	go func() {
		<-start
		_, err := repo.RequestDelete(context.Background(), cluster.ID)
		results <- err
	}()
	close(start)

	succeeded := 0
	conflicted := 0
	for index := 0; index < 2; index++ {
		err := <-results
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrInvalidTransition):
			conflicted++
		default:
			t.Fatalf("concurrent lifecycle request: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("lifecycle results: succeeded=%d conflicted=%d, want 1 each", succeeded, conflicted)
	}

	assertRowCount(t, sqlDB, "jobs", 2)
	current, err := repo.GetCluster(context.Background(), cluster.ID)
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if current.Status != model.StatusScaling && current.Status != model.StatusDeleting {
		t.Fatalf("cluster status = %s, want SCALING or DELETING", current.Status)
	}
}

func TestDeleteReplayDoesNotQueueAnotherJob(t *testing.T) {
	repo, sqlDB := openRepositoryDatabase(t)
	cluster, _, err := repo.CreateCluster(context.Background(), CreateClusterInput{
		ID:             "01JMESSAGESSEARCH0000000001",
		Name:           "messages-search",
		Engine:         "opensearch",
		Version:        "3.8.0",
		DesiredNodes:   1,
		IdempotencyKey: "create-messages-search-2026-09",
	})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	completeProvisioning(t, repo, cluster.ID)

	first, err := repo.RequestDelete(context.Background(), cluster.ID)
	if err != nil {
		t.Fatalf("request delete: %v", err)
	}
	second, err := repo.RequestDelete(context.Background(), cluster.ID)
	if err != nil {
		t.Fatalf("replay delete: %v", err)
	}
	if first.Status != model.StatusDeleting || second.Status != model.StatusDeleting {
		t.Fatalf("delete statuses = %s and %s, want DELETING", first.Status, second.Status)
	}
	assertRowCount(t, sqlDB, "jobs", 2)
}

func TestTransitionClusterStatusRejectsStaleSourceState(t *testing.T) {
	repo, _ := openRepositoryDatabase(t)
	cluster, _, err := repo.CreateCluster(context.Background(), CreateClusterInput{
		ID:             "01JLOGSSEARCH00000000000001",
		Name:           "logs-search",
		Engine:         "opensearch",
		Version:        "3.8.0",
		DesiredNodes:   1,
		IdempotencyKey: "create-logs-search-2026-09",
	})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	if err := repo.TransitionClusterStatus(context.Background(), cluster.ID, model.StatusRequested, model.StatusProvisioning, ""); err != nil {
		t.Fatalf("start provisioning: %v", err)
	}
	if err := repo.TransitionClusterStatus(context.Background(), cluster.ID, model.StatusRequested, model.StatusProvisioning, ""); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("stale transition error = %v, want %v", err, ErrInvalidTransition)
	}
}

func TestReleaseJobMakesCanceledWorkImmediatelyClaimable(t *testing.T) {
	repo, sqlDB := openRepositoryDatabase(t)
	cluster, _, err := repo.CreateCluster(context.Background(), CreateClusterInput{
		ID:             "01JREPORTSSEARCH00000000001",
		Name:           "reports-search",
		Engine:         "opensearch",
		Version:        "3.8.0",
		DesiredNodes:   1,
		IdempotencyKey: "create-reports-search-2026-09",
	})
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	claimed, err := repo.ClaimJob(context.Background())
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}
	if claimed.ClusterID != cluster.ID || claimed.Attempts != 1 {
		t.Fatalf("claimed job=%+v", claimed)
	}
	if err := repo.ReleaseJob(context.Background(), claimed, context.Canceled); err != nil {
		t.Fatalf("release job: %v", err)
	}

	var status string
	var attempts int
	var lockedAt sql.NullTime
	if err := sqlDB.QueryRow(`SELECT status,attempts,locked_at FROM jobs WHERE id=$1`, claimed.ID).Scan(&status, &attempts, &lockedAt); err != nil {
		t.Fatalf("inspect released job: %v", err)
	}
	if status != "PENDING" || attempts != 0 || lockedAt.Valid {
		t.Fatalf("released job status=%s attempts=%d locked_at=%v", status, attempts, lockedAt)
	}

	reclaimed, err := repo.ClaimJob(context.Background())
	if err != nil {
		t.Fatalf("reclaim job: %v", err)
	}
	if reclaimed.ID != claimed.ID || reclaimed.Attempts != 1 {
		t.Fatalf("reclaimed job=%+v, want id=%d attempts=1", reclaimed, claimed.ID)
	}
}

func TestBackupAndRestoreRequestsCreateDurableJobs(t *testing.T) {
	repo, sqlDB := openRepositoryDatabase(t)
	ctx := context.Background()
	cluster, _, err := repo.CreateCluster(ctx, CreateClusterInput{
		ID:             "01JARCHIVESEARCH00000000001",
		Name:           "archive-search",
		Engine:         "opensearch",
		Version:        "3.8.0",
		DesiredNodes:   1,
		IdempotencyKey: "create-archive-search-2026-09",
	})
	if err != nil {
		t.Fatal(err)
	}
	completeProvisioning(t, repo, cluster.ID)
	backup, err := repo.CreateBackup(ctx, cluster.ID, "01JARCHIVEBACKUP00000000001", "snapshot-archive-primary")
	if err != nil {
		t.Fatalf("create backup: %v", err)
	}
	job, err := repo.ClaimJob(ctx)
	if err != nil {
		t.Fatalf("claim backup job: %v", err)
	}
	if job.Type != model.JobBackup || job.BackupID != backup.ID {
		t.Fatalf("backup job=%+v", job)
	}
	if err := repo.TransitionBackupStatus(ctx, backup.ID, model.BackupRequested, model.BackupCreating, ""); err != nil {
		t.Fatal(err)
	}
	if err := repo.TransitionBackupStatus(ctx, backup.ID, model.BackupCreating, model.BackupAvailable, ""); err != nil {
		t.Fatal(err)
	}
	if err := repo.CompleteJob(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	restoring, err := repo.RequestRestore(ctx, cluster.ID, backup.ID)
	if err != nil {
		t.Fatalf("request restore: %v", err)
	}
	if restoring.Status != model.BackupRestoring {
		t.Fatalf("backup state=%s, want RESTORING", restoring.Status)
	}
	restoreJob, err := repo.ClaimJob(ctx)
	if err != nil {
		t.Fatalf("claim restore job: %v", err)
	}
	if restoreJob.Type != model.JobRestore || restoreJob.BackupID != backup.ID {
		t.Fatalf("restore job=%+v", restoreJob)
	}
	assertRowCount(t, sqlDB, "backups", 1)
	assertRowCount(t, sqlDB, "jobs", 3)
}

func TestAuditEventsAreDurableAndReturnedNewestFirst(t *testing.T) {
	repo, _ := openRepositoryDatabase(t)
	ctx := context.Background()
	for _, event := range []model.AuditEvent{
		{RequestID: "request-security-0001", Actor: "anonymous", Role: "none", Action: "HTTP_GET", ResourceType: "http_path", ResourceID: "/v1/clusters", Outcome: "FAILURE", Details: map[string]any{"status_code": 401}},
		{RequestID: "request-security-0002", Actor: "operations-admin", Role: "admin", Action: "HTTP_POST", ResourceType: "http_path", ResourceID: "/v1/clusters", Outcome: "SUCCESS", Details: map[string]any{"status_code": 202}},
	} {
		if err := repo.RecordAudit(ctx, event); err != nil {
			t.Fatalf("record audit event: %v", err)
		}
	}
	events, err := repo.ListAuditEvents(ctx, 10)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if len(events) != 2 || events[0].RequestID != "request-security-0002" || events[1].RequestID != "request-security-0001" {
		t.Fatalf("audit events=%+v", events)
	}
	if events[0].Details["status_code"] != float64(202) {
		t.Fatalf("audit details=%+v", events[0].Details)
	}
}

func openRepositoryDatabase(t *testing.T) (*Repository, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not configured")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	if err := admin.PingContext(ctx); err != nil {
		admin.Close()
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	schema := fmt.Sprintf("repository_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA "`+schema+`"`); err != nil {
		admin.Close()
		t.Fatalf("create isolated schema: %v", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()

	sqlDB, err := appdb.Open(ctx, parsed.String())
	if err != nil {
		_, _ = admin.ExecContext(context.Background(), `DROP SCHEMA "`+schema+`" CASCADE`)
		admin.Close()
		t.Fatalf("open isolated database: %v", err)
	}
	if err := appdb.Migrate(ctx, sqlDB); err != nil {
		sqlDB.Close()
		_, _ = admin.ExecContext(context.Background(), `DROP SCHEMA "`+schema+`" CASCADE`)
		admin.Close()
		t.Fatalf("migrate isolated database: %v", err)
	}

	t.Cleanup(func() {
		_ = sqlDB.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupCtx, `DROP SCHEMA "`+schema+`" CASCADE`); err != nil {
			t.Errorf("drop isolated schema: %v", err)
		}
		_ = admin.Close()
	})
	return New(sqlDB), sqlDB
}

func assertRowCount(t *testing.T, sqlDB *sql.DB, table string, expected int) {
	t.Helper()
	var count int
	if err := sqlDB.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if count != expected {
		t.Fatalf("%s row count = %d, want %d", table, count, expected)
	}
}

func completeProvisioning(t *testing.T, repo *Repository, clusterID string) {
	t.Helper()
	ctx := context.Background()
	job, err := repo.ClaimJob(ctx)
	if err != nil {
		t.Fatalf("claim provision job: %v", err)
	}
	if job.Type != model.JobProvision || job.ClusterID != clusterID {
		t.Fatalf("claimed job type=%s cluster=%s, want PROVISION for %s", job.Type, job.ClusterID, clusterID)
	}
	if err := repo.TransitionClusterStatus(ctx, clusterID, model.StatusRequested, model.StatusProvisioning, ""); err != nil {
		t.Fatalf("start provisioning: %v", err)
	}
	if err := repo.TransitionClusterStatus(ctx, clusterID, model.StatusProvisioning, model.StatusRunning, ""); err != nil {
		t.Fatalf("complete provisioning: %v", err)
	}
	if err := repo.CompleteJob(ctx, job.ID); err != nil {
		t.Fatalf("complete provision job: %v", err)
	}
}
