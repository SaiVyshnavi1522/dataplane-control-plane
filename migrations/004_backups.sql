CREATE TABLE backups (
    id TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,
    snapshot_name TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('REQUESTED','CREATING','AVAILABLE','RESTORING','RESTORED','FAILED')),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_backups_cluster_created ON backups(cluster_id, created_at DESC);

ALTER TABLE jobs ADD COLUMN backup_id TEXT REFERENCES backups(id) ON DELETE CASCADE;
ALTER TABLE jobs DROP CONSTRAINT jobs_type_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_type_check
    CHECK (job_type IN ('PROVISION', 'SCALE', 'DELETE', 'BACKUP', 'RESTORE'));
