# Measured benchmark results

These measurements are reproducible observations, not capacity guarantees. They exercise the full authenticated REST-to-PostgreSQL path with mock infrastructure provisioning so API and durable-queue behavior can be measured separately from OpenSearch startup time.

## Environment

- Measurement date: 2026-09-02
- Host: Apple M1 Pro, 8 logical CPUs, 16 GiB memory
- OS/runtime: macOS arm64, Go 1.27.1
- Containers: Docker 27.4.0, Docker Compose 2.31.0
- Stack: PostgreSQL 17 plus the full Compose observability and snapshot stack
- API mode: mock provisioner, 4 workers, bearer authentication and audit persistence enabled

## Authenticated create workload

Command: `REQUESTS=1000 CONCURRENCY=50 RUNS=3 make benchmark-load`

Each request used a unique idempotency key and created a cluster plus durable provisioning job in one transaction. All 3,000 requests completed successfully.

| Run | Requests | Concurrency | Failures | Elapsed | Throughput | p50 | p95 | p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,000 | 50 | 0 | 610 ms | 1,638.9 req/s | 22.367 ms | 73.937 ms | 130.442 ms |
| 2 | 1,000 | 50 | 0 | 458 ms | 2,183.0 req/s | 19.137 ms | 48.910 ms | 67.454 ms |
| 3 | 1,000 | 50 | 0 | 418 ms | 2,391.1 req/s | 17.917 ms | 39.003 ms | 51.225 ms |

The median run produced 2,183.0 requests/second with 48.910 ms p95 latency. A post-run database query confirmed 3,020 successful jobs and zero active jobs; the extra 20 were earlier verification work in the same persistent local database.

## Injected-failure recovery

Command: `make verify-retries`

The provision, scale, and delete operations each failed deterministically twice, were durably rescheduled with exponential delays, and succeeded on their third persisted attempt. The complete scenario took 26.02 seconds. No operation entered terminal `FAILED` state.

## PostgreSQL outage and recovery

Command: `make benchmark-recovery`

Stopping PostgreSQL changed `/readyz` from HTTP 200 to HTTP 503. Three runs took 0.62-0.79 seconds to restart the container and return to HTTP 200 (latest: 0.62 seconds). The measurement includes container startup and connection recovery. The script first requires an idle job queue so the readiness drill cannot strand an in-flight benchmark claim behind its safety lease.

## Storage and backup recovery checks

- Kind: a document survived StatefulSet pod deletion and recreation on the same bound persistent volume. Deliberate image drift was repaired, scale-out reached two ready replicas, and deletion removed controller-owned StatefulSet, Service, pods, and PVCs.
- MinIO/S3: an OpenSearch document was snapshotted, the durable backup reached `AVAILABLE`, restore reached `RESTORED`, and the document was read from its isolated restored index.

## Interpretation and limits

The create workload is intentionally local and write-heavy. It does not model WAN latency, TLS termination, multi-replica PostgreSQL, cloud storage, noisy neighbors, or real Kubernetes/OpenSearch provisioning. Use the included commands as regression baselines on comparable hardware; production sizing requires sustained mixed workloads, longer soak tests, and infrastructure-specific failure drills.
