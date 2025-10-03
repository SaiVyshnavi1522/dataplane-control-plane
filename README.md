# DataPlane Control Plane

A production-style **Go database-as-a-service control plane** for provisioning and managing OpenSearch clusters through asynchronous, durable lifecycle operations.

> Local-development project. It does not use or require employer infrastructure, credentials, code, or datasets.

## Capabilities

The repository demonstrates software-engineering behaviors that are difficult to show with a CRUD application:

- idempotent APIs
- asynchronous lifecycle operations
- concurrent Go workers
- durable job claiming with PostgreSQL `FOR UPDATE SKIP LOCKED`
- retry/backoff and timeouts
- deterministic failure injection and graceful worker cancellation
- explicit lifecycle state machines
- Kubernetes API integration with `client-go`
- ownership-safe reconciliation and PVC-backed stateful provisioning
- protobuf-generated gRPC API with health and reflection
- MinIO/S3-backed OpenSearch snapshots and restore workflows
- bearer authentication, role-based authorization, request IDs, and durable audit events
- Prometheus metrics, alert rules, Grafana dashboards, and OpenTelemetry traces
- health/readiness checks, integration tests, CI, and load testing

## Architecture

```mermaid
flowchart LR
    Client -->|REST or gRPC| API[Go API]
    API --> APP[Application service]
    APP --> DB[(PostgreSQL)]
    DB --> W[Concurrent worker pool]
    W --> APP
    APP --> P{Provisioner}
    P -->|mock| Mock[Simulation]
    P -->|kubernetes| K8s[Kubernetes API]
    K8s --> OS[OpenSearch StatefulSet]
    APP --> SNAP[Snapshot service]
    SNAP --> MinIO[(MinIO / S3)]
    API --> Prom[Prometheus]
    W --> Prom
    Prom --> Grafana
    API --> Jaeger[Jaeger / OTLP]
```

More detail: [`docs/architecture.md`](docs/architecture.md)

## API

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/v1/clusters` | Create a cluster; requires `Idempotency-Key` |
| `GET` | `/v1/clusters` | List clusters |
| `GET` | `/v1/clusters/{id}` | Get lifecycle status |
| `POST` | `/v1/clusters/{id}/scale` | Scale to 1-3 nodes |
| `DELETE` | `/v1/clusters/{id}` | Delete cluster asynchronously |
| `POST` | `/v1/clusters/{id}/backups` | Start an S3 snapshot |
| `GET` | `/v1/clusters/{id}/backups` | List backup status |
| `POST` | `/v1/clusters/{id}/backups/{backup_id}/restore` | Restore under isolated index names |
| `GET` | `/v1/audit-events?limit=100` | Admin-only audit event feed |
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | Database readiness |
| `GET` | `/metrics` | Prometheus metrics |

The same lifecycle use cases are available through the generated `dataplane.v1.ClusterService` gRPC API on port `9091`. Server reflection and the standard health service are enabled:

```bash
grpcurl -plaintext localhost:9091 list
grpcurl -plaintext localhost:9091 grpc.health.v1.Health/Check
grpcurl -plaintext -H 'authorization: Bearer local-admin-key-change-me' \
  -d '{"idempotencyKey":"create-grpc-primary","name":"grpc-search","nodes":1}' \
  localhost:9091 dataplane.v1.ClusterService/CreateCluster
```

All `/v1` operations and application gRPC methods require a bearer key. The local stack provides an `admin` identity for mutations and a `viewer` identity for reads. Replace both development keys before using the service outside an isolated laptop environment.

## Local quickstart

Prerequisites: Docker + Docker Compose.

```bash
docker compose up --build -d
curl http://localhost:8080/readyz
make verify-local
```

Then open:

- API: `http://localhost:8080`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- Jaeger: `http://localhost:16686`
- MinIO console: `http://localhost:9001` (`dataplane` / `dataplane-secret`)

Docker Compose uses **mock provisioning** so the entire API/job/observability path runs without Kubernetes.

Exercise real retry and recovery behavior by failing the first two attempts of every lifecycle operation:

```bash
make verify-retries
make verify-security
make verify-backup
```

The verification checks persisted attempt counts for provision, scale, and delete, then restores failure injection to zero.

To stop the stack while preserving PostgreSQL data:

```bash
docker compose stop
```

## Run real Kubernetes provisioning locally

Install Docker, `kubectl`, and Kind. Then:

```bash
kind create cluster --name dataplane --config deploy/kind/kind.yaml
docker compose up -d postgres
export DATABASE_URL='postgres://dataplane:dataplane@localhost:55432/dataplane?sslmode=disable'
export API_KEYS='local-admin:admin:local-admin-key-change-me'
export PROVISIONER=kubernetes
go run ./cmd/api
```

Create a cluster:

```bash
curl -X POST http://localhost:8080/v1/clusters \
  -H 'Authorization: Bearer local-admin-key-change-me' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: create-search-primary-001' \
  -d '{"name":"search-prod","engine":"opensearch","version":"3.8.0","nodes":1}'
```

Inspect it:

```bash
kubectl -n dataplane-clusters get statefulsets,pods,services
```

Or run the complete Kind verification, including PVC binding, data survival after pod replacement, drift repair, two-node scaling, and resource cleanup:

```bash
make kind-verify
```

> Kubernetes mode disables the OpenSearch security plugin for **local development only**.

## Lifecycle

```text
REQUESTED -> PROVISIONING -> RUNNING
                         \-> FAILED
RUNNING -> SCALING -> RUNNING
                  \-> FAILED
RUNNING -> DELETING -> DELETED
FAILED  -> DELETING
```

Lifecycle changes use database compare-and-set updates. Scale and delete lock the cluster row while changing state and enqueuing work, and PostgreSQL also enforces at most one active job per cluster. Conflicting operations return `409 Conflict`; safe replays do not enqueue duplicate work.

## Idempotency

Cluster creation requires `Idempotency-Key`. The normalized create payload is stored separately from mutable cluster state, so repeating the same request returns the current representation of the original cluster without creating another provisioning job—even after that cluster has been scaled. Reusing the key with a different payload returns `409 Conflict`.

## Worker model

Multiple goroutines claim jobs using:

```sql
SELECT ...
FROM jobs
WHERE status = 'PENDING'
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

This is a compact demonstration of concurrent work claiming, transactional consistency, retry behavior, and horizontal-worker concepts.

Requests sharing an idempotency key are serialized with a PostgreSQL transaction-level advisory lock. This protects against duplicate resources when multiple API replicas receive the same request concurrently.

## Backup and restore

Backup and restore are durable worker jobs, not blocking API calls. The worker registers a cluster-scoped OpenSearch S3 repository, snapshots into a cluster-specific MinIO/S3 prefix, persists every status transition, and restores indices with a `restored-<backup-id>-` prefix so existing indices are never overwritten. Run the document-level recovery proof with:

```bash
make verify-backup
```

## Security and observability

`API_KEYS` configures comma-separated `subject:admin|viewer:key` credentials. Keys are SHA-256 hashed in memory, compared in constant time, and never persisted. Admins can mutate resources and inspect audit events; viewers can read cluster and backup state. Every protected REST and gRPC call records actor, role, request ID, action, outcome, and details in PostgreSQL, including denied requests.

Prometheus scrapes HTTP, gRPC, worker, retry, outcome, and latency metrics. Five alert rules cover availability, errors, latency, terminal job failures, and worker loss. Grafana provisions the operations dashboard and both Prometheus and Jaeger data sources. W3C trace context flows through HTTP/gRPC and worker-to-OpenSearch calls.

## Load test

With Docker Compose running:

```bash
ADMIN_API_KEY=local-admin-key-change-me go run ./cmd/loadtest -n 500 -c 25
```

The command prints throughput and p50/p95/p99 request latency. Reproduce the three-run workload and database recovery scenario with `make benchmark-load` and `make benchmark-recovery`. Recorded environment, raw results, interpretation, and limitations are in [`docs/benchmark-results.md`](docs/benchmark-results.md).

## Project structure

```text
cmd/api                 API entrypoint
cmd/loadtest            small concurrent benchmark client
api/proto               versioned protobuf contracts
gen                     generated Go protobuf and gRPC bindings
internal/api            REST handlers + middleware
internal/grpcapi        gRPC transport and canonical error mapping
internal/service        transport-neutral application use cases
internal/repository     PostgreSQL lifecycle + job persistence
internal/worker         concurrent worker pool + retries
internal/provisioner    mock, fault-injection, and Kubernetes implementations
internal/snapshot       OpenSearch S3 snapshot and restore adapter
internal/auth           HTTP/gRPC authentication, RBAC, request IDs, audit hooks
internal/metrics        Prometheus metrics
internal/telemetry      OpenTelemetry initialization
migrations              schema
deploy                   Kind, Prometheus, Grafana, RBAC
docs                     architecture, ADRs, and operational runbook
```

## Verification

With Go and PostgreSQL available locally:

```bash
go fmt ./...
go vet ./...
TEST_DATABASE_URL='postgres://dataplane:dataplane@localhost:55432/dataplane?sslmode=disable' go test -race -cover ./...
go build ./...
make verify-local
make verify-security
make verify-retries
make verify-backup
```

CI runs the same format, vet, race, integration, coverage, and build checks against PostgreSQL 17.

## Production boundaries

This is a production-style reference implementation, not a hosted production service. Kubernetes reconciliation runs as part of durable lifecycle jobs rather than a continuously watching operator. The local stack intentionally uses static development credentials, single-instance dependencies, plaintext service traffic, and OpenSearch with its security plugin disabled. A real deployment must use a secret manager/workload identity, TLS/mTLS, network policies, highly available PostgreSQL and object storage, encrypted persistent volumes, external ingress controls, backup retention policies, and managed credential rotation.

## Technology choices

- Go 1.27
- PostgreSQL
- Kubernetes / Kind
- OpenSearch 3.8
- Prometheus
- Grafana
- OpenTelemetry / Jaeger
- MinIO / S3 snapshots
- Docker / Docker Compose
- GitHub Actions

## License

MIT
