# Design decisions

## ADR-001: PostgreSQL-backed durable queue

**Decision:** Store lifecycle jobs in PostgreSQL instead of introducing Kafka/SQS in v1.

**Why:** Cluster creation and job creation can be committed atomically. `FOR UPDATE SKIP LOCKED` provides safe concurrent claims. This keeps the project runnable on one laptop while preserving a clean queue abstraction for a later SQS/Kafka implementation.

## ADR-002: Provisioner interface

The API/worker layer depends on a `Provisioner` interface. `mock` mode is fast for development and load tests; `kubernetes` mode creates real OpenSearch resources. This keeps business logic independent of infrastructure.

## ADR-003: Idempotent create API

`Idempotency-Key` records are durable and store the immutable normalized create payload separately from mutable cluster state. Requests sharing a key acquire the same PostgreSQL transaction-level advisory lock, which serializes concurrent replays across API replicas. A matching replay returns the current cluster representation; a different payload returns a conflict.

## ADR-004: Reconciliation over shell commands

Kubernetes resources are created through `client-go`, not by spawning `kubectl`. This makes provisioning testable and closer to real control-plane software.

## ADR-005: Ordered database migrations

SQL migrations are embedded from the top-level `migrations` directory and recorded in `schema_migrations`. A PostgreSQL advisory lock ensures only one control-plane replica applies migrations at a time, and each migration is committed transactionally.

## ADR-006: Lifecycle transitions are database invariants

Allowed transitions are explicit in the domain model. Repository updates use expected-state compare-and-set conditions, while scale and delete requests take a row lock around the state change and job insert. PostgreSQL check constraints reject unknown states and job types, and a partial unique index permits only one `PENDING` or `RUNNING` job per cluster. These layers prevent stale workers and concurrent API requests from producing impossible lifecycle states.

## ADR-007: Transport-neutral application service

REST, background workers, and future gRPC handlers call one application service. The service owns request normalization, validation, lifecycle use cases, job execution, and provisioner coordination. HTTP code is limited to decoding, status-code mapping, and encoding; worker code is limited to polling, concurrency, and telemetry. This prevents protocol-specific business rules and makes the core use cases independently verifiable.

## ADR-008: At-least-once work with cancellation release

Lifecycle work is at-least-once. Claims have a stale-worker lease, provisioner operations are reconciling/idempotent, and retry updates require the job to still be `RUNNING`. A graceful shutdown immediately returns canceled work to `PENDING`, clears its lock, and does not consume an attempt. This avoids both ten-minute restart delays and false terminal failures caused by routine deployments.

## ADR-009: PVC-backed, ownership-safe Kubernetes reconciliation

Every OpenSearch replica receives a `ReadWriteOnce` claim from the StatefulSet template. Storage size and class are configurable, scaled-down claims are retained, and cluster deletion removes managed claims after the StatefulSet terminates. Reconciliation repairs mutable Service and pod-template drift but refuses to adopt resources without matching controller and cluster ownership labels. Immutable storage-template drift is surfaced for a controlled migration instead of being silently ignored.

## ADR-010: Protobuf-first gRPC transport

The versioned `dataplane.v1.ClusterService` contract generates both client and server bindings. gRPC handlers call the same application service as REST and translate domain errors to canonical status codes. Reflection supports local discovery, the standard health protocol supports orchestration, and shutdown drains in-flight RPCs within the process shutdown deadline. CI regenerates bindings and rejects drift.

## ADR-011: Native OpenSearch snapshots over S3-compatible storage

Backup and restore remain durable lifecycle jobs and use OpenSearch's repository-s3 plugin instead of copying data through the control plane. Repository base paths are isolated by cluster ID, backup transitions are compare-and-set updates, and restore renames indices to prevent accidental overwrite. MinIO makes the full flow locally reproducible while the adapter remains compatible with S3 endpoints.

## ADR-012: Transport-level RBAC with durable audits

REST middleware and a gRPC interceptor share bearer authentication, `admin`/`viewer` authorization, request-ID propagation, and audit recording. Credentials are validated at startup, retained only as SHA-256 hashes, and compared in constant time. This demonstrates the security boundary without coupling the application service to one identity provider; a deployed service would replace static keys with workload identity or OIDC.

## ADR-013: Vendor-neutral tracing and metrics-first alerting

OpenTelemetry provides W3C propagation and OTLP export so trace storage can change without rewriting business code. Prometheus remains the alerting source because counters, gauges, and histograms support stable SLO queries. The local stack provisions Jaeger, Prometheus rules, and Grafana dashboards as code.
