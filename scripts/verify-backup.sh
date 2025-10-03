#!/usr/bin/env bash
set -euo pipefail

BASE_URL=${BASE_URL:-http://localhost:8080}
ADMIN_API_KEY=${ADMIN_API_KEY:-local-admin-key-change-me}
suffix="$(date +%s)-$$"
index_name="catalog-$suffix"
idempotency_key="backup-verification-$suffix"

wait_for_cluster() {
  local cluster_id=$1
  for ((attempt = 1; attempt <= 30; attempt++)); do
    cluster_state=$(curl -fsS -H "Authorization: Bearer $ADMIN_API_KEY" "$BASE_URL/v1/clusters/$cluster_id" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')
    [[ "$cluster_state" == RUNNING ]] && return 0
    sleep 1
  done
  return 1
}

wait_for_backup() {
  local cluster_id=$1
  local expected=$2
  for ((attempt = 1; attempt <= 90; attempt++)); do
    backup_response=$(curl -fsS -H "Authorization: Bearer $ADMIN_API_KEY" "$BASE_URL/v1/clusters/$cluster_id/backups")
    backup_state=$(printf '%s' "$backup_response" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')
    printf 'backup status=%s\n' "$backup_state"
    [[ "$backup_state" == "$expected" ]] && return 0
    [[ "$backup_state" == FAILED ]] && return 1
    sleep 1
  done
  return 1
}

docker compose up --build -d
curl -fsS -X PUT "http://localhost:9200/$index_name/_doc/product-1001?refresh=true" \
  -H 'Content-Type: application/json' -d '{"name":"mechanical keyboard","inventory":42}' >/dev/null
cluster_response=$(curl -fsS -X POST "$BASE_URL/v1/clusters" \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $idempotency_key" \
  -d '{"name":"backup-search","nodes":1}')
cluster_id=$(printf '%s' "$cluster_response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
wait_for_cluster "$cluster_id"

backup_response=$(curl -fsS -X POST -H "Authorization: Bearer $ADMIN_API_KEY" "$BASE_URL/v1/clusters/$cluster_id/backups")
backup_id=$(printf '%s' "$backup_response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
wait_for_backup "$cluster_id" AVAILABLE
curl -fsS -X POST -H "Authorization: Bearer $ADMIN_API_KEY" "$BASE_URL/v1/clusters/$cluster_id/backups/$backup_id/restore" >/dev/null
wait_for_backup "$cluster_id" RESTORED

restore_prefix="restored-${backup_id:0:12}-"
curl -fsS "http://localhost:9200/$restore_prefix$index_name/_doc/product-1001" | grep -q '"found":true'
printf 'MinIO snapshot and restored-document verification passed\n'
