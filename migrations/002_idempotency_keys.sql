CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    request_payload JSONB NOT NULL,
    resource_id TEXT NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_resource
    ON idempotency_keys(resource_id);

-- Preserve idempotency records created before this table was introduced.
-- For an existing database, desired_nodes is the best available historical
-- value because the original create payload was not previously retained.
INSERT INTO idempotency_keys(key, operation, request_payload, resource_id)
SELECT
    idempotency_key,
    'CREATE_CLUSTER',
    jsonb_build_object(
        'name', name,
        'engine', engine,
        'version', version,
        'nodes', desired_nodes
    ),
    id
FROM clusters
ON CONFLICT (key) DO NOTHING;
