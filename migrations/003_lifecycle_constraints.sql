ALTER TABLE clusters
    ADD CONSTRAINT clusters_status_check
    CHECK (status IN (
        'REQUESTED',
        'PROVISIONING',
        'RUNNING',
        'SCALING',
        'DELETING',
        'FAILED',
        'DELETED'
    )) NOT VALID;

ALTER TABLE clusters VALIDATE CONSTRAINT clusters_status_check;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_type_check
    CHECK (job_type IN ('PROVISION', 'SCALE', 'DELETE')) NOT VALID;

ALTER TABLE jobs VALIDATE CONSTRAINT jobs_type_check;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')) NOT VALID;

ALTER TABLE jobs VALIDATE CONSTRAINT jobs_status_check;

-- The row-level lifecycle lock in the repository is the primary concurrency
-- control. This partial unique index is a database-level invariant that also
-- protects against future callers accidentally queuing overlapping work.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_one_active_per_cluster
    ON jobs(cluster_id)
    WHERE status IN ('PENDING', 'RUNNING');
