#!/usr/bin/env bash
set -euo pipefail

BASE_URL=${BASE_URL:-http://localhost:8080}
ADMIN_API_KEY=${ADMIN_API_KEY:-local-admin-key-change-me}
IDEMPOTENCY_KEY="local-verification-$(date +%s)-$$"
CLUSTER_NAME="payments-search"

cluster_status() {
  curl -fsS -H "Authorization: Bearer $ADMIN_API_KEY" "$BASE_URL/v1/clusters/$1" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p'
}

wait_for_status() {
  local cluster_id=$1
  local expected=$2
  local attempts=${3:-30}
  local status=""

  for ((attempt = 1; attempt <= attempts; attempt++)); do
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

printf 'Creating cluster\n'
response=$(curl -fsS -X POST "$BASE_URL/v1/clusters" \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  -d "{\"name\":\"$CLUSTER_NAME\",\"engine\":\"opensearch\",\"version\":\"3.8.0\",\"nodes\":1}")
printf '%s\n' "$response"
cluster_id=$(printf '%s' "$response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
if [[ -z "$cluster_id" ]]; then
  printf 'create response did not contain a cluster id\n' >&2
  exit 1
fi

wait_for_status "$cluster_id" RUNNING

printf 'Replaying create request\n'
curl -fsS -X POST "$BASE_URL/v1/clusters" \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  -d "{\"name\":\"$CLUSTER_NAME\",\"engine\":\"opensearch\",\"version\":\"3.8.0\",\"nodes\":1}"
printf '\n'

printf 'Scaling cluster\n'
curl -fsS -X POST "$BASE_URL/v1/clusters/$cluster_id/scale" \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"nodes":2}'
printf '\n'
wait_for_status "$cluster_id" RUNNING

printf 'Deleting cluster\n'
curl -fsS -X DELETE -H "Authorization: Bearer $ADMIN_API_KEY" "$BASE_URL/v1/clusters/$cluster_id"
printf '\n'
wait_for_status "$cluster_id" DELETED

printf 'Local lifecycle verification passed\n'
