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
