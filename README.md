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
- explicit lifecycle state machines
- Kubernetes API integration with `client-go`
- stateful-service provisioning
- Prometheus metrics and Grafana dashboards
- health/readiness checks, integration tests, CI, and load testing

## Architecture

```mermaid
flowchart LR
    Client -->|REST| API[Go API]
    API --> APP[Application service]
    APP --> DB[(PostgreSQL)]
    DB --> W[Concurrent worker pool]
    W --> APP
    APP --> P{Provisioner}
    P -->|mock| Mock[Simulation]
    P -->|kubernetes| K8s[Kubernetes API]
    K8s --> OS[OpenSearch StatefulSet]
    API --> Prom[Prometheus]
    W --> Prom
    Prom --> Grafana
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
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | Database readiness |
| `GET` | `/metrics` | Prometheus metrics |

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

Docker Compose uses **mock provisioning** so the entire API/job/observability path runs without Kubernetes.

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
export PROVISIONER=kubernetes
go run ./cmd/api
```

Create a cluster:

```bash
curl -X POST http://localhost:8080/v1/clusters \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: create-search-primary-001' \
  -d '{"name":"search-prod","engine":"opensearch","version":"3.8.0","nodes":1}'
```

Inspect it:

```bash
kubectl -n dataplane-clusters get statefulsets,pods,services
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

## Load test

With Docker Compose running:

```bash
go run ./cmd/loadtest -n 500 -c 25
```

The command prints throughput and p50/p95/p99 request latency. Performance results belong in this README only after they have been measured on a documented environment; the repository does not contain placeholder benchmark claims.

## Project structure

```text
cmd/api                 API entrypoint
cmd/loadtest            small concurrent benchmark client
internal/api            REST handlers + middleware
internal/service        transport-neutral application use cases
internal/repository     PostgreSQL lifecycle + job persistence
internal/worker         concurrent worker pool + retries
internal/provisioner    mock and Kubernetes implementations
internal/metrics        Prometheus metrics
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
```

CI runs the same format, vet, race, integration, coverage, and build checks against PostgreSQL 17.

## Current boundaries

The local Kubernetes configuration disables the OpenSearch security plugin and currently uses ephemeral `emptyDir` storage. It must not be exposed to untrusted networks. Persistent volumes, snapshot/restore, gRPC, authentication, and multi-replica controller coordination are not yet implemented.

## Technology choices

- Go 1.27
- PostgreSQL
- Kubernetes / Kind
- OpenSearch 3.8
- Prometheus
- Grafana
- Docker / Docker Compose
- GitHub Actions

## License

MIT
