#!/usr/bin/env bash
set -euo pipefail

BASE_URL=${BASE_URL:-http://localhost:8080}
ADMIN_API_KEY=${ADMIN_API_KEY:-local-admin-key-change-me}
IDEMPOTENCY_KEY="retry-verification-$(date +%s)-$$"
CLUSTER_NAME="reliability-search"

restore_api() {
  FAILURE_INJECTION_ATTEMPTS=0 docker compose up -d --force-recreate api >/dev/null
}
trap restore_api EXIT

cluster_status() {
  curl -fsS -H "Authorization: Bearer $ADMIN_API_KEY" "$BASE_URL/v1/clusters/$1" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p'
}

wait_for_status() {
  local cluster_id=$1
  local expected=$2
  local status=""

  for ((attempt = 1; attempt <= 30; attempt++)); do
    status=$(cluster_status "$cluster_id")
    printf 'cluster=%s status=%s\n' "$cluster_id" "$status"
    if [[ "$status" == "$expected" ]]; then
      return 0
    fi
    if [[ "$status" == "FAILED" ]]; then
      printf 'cluster entered FAILED while waiting for %s\n' "$expected" >&2
      return 1
    fi
    sleep 1
  done

  printf 'timed out waiting for %s; last status was %s\n' "$expected" "$status" >&2
  return 1
}

assert_attempts() {
  local cluster_id=$1
  local job_type=$2
  local attempts
  attempts=$(docker compose exec -T postgres psql -U dataplane -d dataplane -Atc \
    "SELECT attempts FROM jobs WHERE cluster_id='$cluster_id' AND job_type='$job_type' ORDER BY id DESC LIMIT 1")
  if [[ "$attempts" != "3" ]]; then
    printf '%s attempts=%s, want 3\n' "$job_type" "$attempts" >&2
    return 1
  fi
  printf '%s recovered on attempt %s\n' "$job_type" "$attempts"
}

printf 'Starting API with two injected failures per lifecycle operation\n'
FAILURE_INJECTION_ATTEMPTS=2 docker compose up -d --force-recreate api >/dev/null
for ((attempt = 1; attempt <= 30; attempt++)); do
  if curl -fsS "$BASE_URL/readyz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "$BASE_URL/readyz" >/dev/null

response=$(curl -fsS -X POST "$BASE_URL/v1/clusters" \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  -d "{\"name\":\"$CLUSTER_NAME\",\"nodes\":1}")
cluster_id=$(printf '%s' "$response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
if [[ -z "$cluster_id" ]]; then
  printf 'create response did not contain a cluster id: %s\n' "$response" >&2
  exit 1
fi
wait_for_status "$cluster_id" RUNNING
assert_attempts "$cluster_id" PROVISION

curl -fsS -X POST "$BASE_URL/v1/clusters/$cluster_id/scale" \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H 'Content-Type: application/json' -d '{"nodes":2}' >/dev/null
wait_for_status "$cluster_id" RUNNING
assert_attempts "$cluster_id" SCALE

curl -fsS -X DELETE -H "Authorization: Bearer $ADMIN_API_KEY" "$BASE_URL/v1/clusters/$cluster_id" >/dev/null
wait_for_status "$cluster_id" DELETED
assert_attempts "$cluster_id" DELETE

printf 'Retry and recovery verification passed; restoring fault injection to zero\n'
