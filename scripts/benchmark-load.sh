#!/usr/bin/env bash
set -euo pipefail

ADMIN_API_KEY=${ADMIN_API_KEY:-local-admin-key-change-me}
REQUESTS=${REQUESTS:-1000}
CONCURRENCY=${CONCURRENCY:-50}
RUNS=${RUNS:-3}

for ((run = 1; run <= RUNS; run++)); do
  printf 'run=%d requests=%d concurrency=%d\n' "$run" "$REQUESTS" "$CONCURRENCY"
  go run ./cmd/loadtest -n "$REQUESTS" -c "$CONCURRENCY" -api-key "$ADMIN_API_KEY"
done
