# Runbook

## Cluster stuck in PROVISIONING

1. `kubectl -n dataplane-clusters get pods,sts,svc`
2. `kubectl -n dataplane-clusters describe pod <pod>`
3. `kubectl -n dataplane-clusters logs <pod>`
4. Check laptop memory. OpenSearch requests 768Mi per node and can use up to 1.5Gi.
5. Confirm the image can be pulled.
6. Check claim binding with `kubectl -n dataplane-clusters get pvc,pv` and inspect StorageClass events.

Run `make kind-verify` for a complete local storage and reconciliation exercise. It creates or reuses the `dataplane` Kind cluster, verifies a bound PVC, writes an OpenSearch document, replaces the pod, confirms the document survived, repairs deliberate image drift while scaling to two nodes, and validates cleanup.

## API not ready

- `curl http://localhost:8080/healthz`
- `curl http://localhost:8080/readyz`
- Check PostgreSQL with `docker compose ps postgres`.
- Check gRPC with `grpcurl -plaintext localhost:9091 grpc.health.v1.Health/Check`.

Run `make benchmark-recovery` to stop PostgreSQL, confirm readiness returns HTTP 503, restart it, and measure time to HTTP 200. The script always attempts to restart PostgreSQL on exit.

## Authentication or authorization failure

- REST uses `Authorization: Bearer <key>`; gRPC uses the lowercase `authorization` metadata key.
- The Compose development identities are `local-admin` and `local-viewer`; their keys are defined by `API_KEYS` in `docker-compose.yml` and `.env.example`.
- `viewer` can issue read methods. Cluster mutations, backup/restore, and `GET /v1/audit-events` require `admin`.
- Every response returns `X-Request-ID`. A valid inbound request ID is preserved; otherwise the server generates one.
- Inspect the latest records with `curl -H 'Authorization: Bearer local-admin-key-change-me' 'http://localhost:8080/v1/audit-events?limit=20'`.
- Run `make verify-security` to exercise anonymous rejection, viewer denial, request-ID persistence, and durable failure audits.

## Backup or restore failure

1. Fetch backup state and `last_error` from `GET /v1/clusters/{id}/backups`.
2. Confirm MinIO is healthy with `docker compose ps minio minio-init` and inspect its console at `http://localhost:9001`.
3. Confirm the repository-s3 plugin with `curl http://localhost:9200/_cat/plugins?v`.
4. Inspect registered repositories with `curl http://localhost:9200/_snapshot/_all`.
5. Inspect API/worker and OpenSearch logs with `docker compose logs api opensearch minio`.

`make verify-backup` writes a document, creates a native S3 snapshot, restores it to isolated index names, and reads the restored document. Restore deliberately never overwrites live indices.

## Alerts, metrics, and traces

- Prometheus targets: `http://localhost:9090/targets`
- Prometheus rules: `http://localhost:9090/rules`
- Grafana operations dashboard: `http://localhost:3000`
- Jaeger traces for `dataplane-control-plane`: `http://localhost:16686`
- Raw application metrics: `http://localhost:8080/metrics`

The provisioned alerts cover an unavailable API, HTTP 5xx ratio above 5%, p95 latency above one second, repeated terminal job failures, and absence of active workers. For a production deployment, route alerts to an external Alertmanager and define SLO windows from real traffic.

## Retry behavior

Jobs make at most three attempts. Failed attempts are rescheduled with exponential delays of 1s and 2s; the third failure is terminal. After the final failure the job and cluster are marked `FAILED` and `last_error` is exposed by the cluster API.

Run a deterministic recovery exercise against the local mock provisioner:

```bash
make verify-retries
```

This temporarily configures two failures for each provision, scale, and delete operation, confirms that all three recover on their third persisted attempt, and restores fault injection to zero. `FAILURE_INJECTION_ATTEMPTS` is rejected when Kubernetes provisioning is enabled.

## Graceful shutdown

On `SIGINT` or `SIGTERM`, the HTTP server drains and in-flight workers receive cancellation. Interrupted jobs are immediately returned to `PENDING`, their lock is cleared, and the interrupted claim does not consume a retry attempt. The process waits up to `WORKER_SHUTDOWN_TIMEOUT` (10 seconds by default) for cleanup. Jobs abandoned by a hard crash are reclaimed after the ten-minute lease expires.

## Local security warning

Kubernetes OpenSearch resources set `DISABLE_SECURITY_PLUGIN=true`. The Compose stack also uses static keys, plaintext traffic, and development MinIO/Grafana credentials. Never expose this configuration to an untrusted network. A production deployment requires external identity, TLS/mTLS, secret rotation, network policy, encryption controls, and highly available stateful dependencies.
