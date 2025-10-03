#!/usr/bin/env bash
set -euo pipefail

BASE_URL=${BASE_URL:-http://localhost:8080}
ADMIN_API_KEY=${ADMIN_API_KEY:-local-admin-key-change-me}
CLUSTER_NAME=kind-search
IDEMPOTENCY_KEY="kind-pvc-$(date +%s)-$$"
CLUSTER_ID=""
API_PID=""
API_LOG=$(mktemp -t dataplane-kind-api.XXXXXX)

cleanup() {
  if [[ -n "$CLUSTER_ID" ]]; then
    curl -fsS -X DELETE -H "Authorization: Bearer $ADMIN_API_KEY" "$BASE_URL/v1/clusters/$CLUSTER_ID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$API_PID" ]]; then
    kill -TERM "$API_PID" >/dev/null 2>&1 || true
    wait "$API_PID" >/dev/null 2>&1 || true
  fi
  docker compose up -d api prometheus >/dev/null 2>&1 || true
  rm -f "$API_LOG"
}
trap cleanup EXIT

for command_name in docker kind kubectl go curl; do
  command -v "$command_name" >/dev/null || { printf '%s is required\n' "$command_name" >&2; exit 1; }
done

if ! kind get clusters | grep -qx dataplane; then
  kind create cluster --name dataplane --config deploy/kind/kind.yaml
fi
kubectl config use-context kind-dataplane >/dev/null
kubectl wait --for=condition=Ready node/dataplane-control-plane --timeout=120s
docker compose up -d postgres
docker compose stop api >/dev/null 2>&1 || true

env DATABASE_URL='postgres://dataplane:dataplane@localhost:55432/dataplane?sslmode=disable' \
  API_KEYS="local-admin:admin:$ADMIN_API_KEY" \
  PROVISIONER=kubernetes WORKERS=2 JOB_POLL_INTERVAL=1s OPENSEARCH_STORAGE_SIZE=1Gi \
  go run ./cmd/api >"$API_LOG" 2>&1 &
API_PID=$!

for ((attempt = 1; attempt <= 90; attempt++)); do
  if curl -fsS "$BASE_URL/readyz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "$BASE_URL/readyz" >/dev/null || { cat "$API_LOG"; exit 1; }

response=$(curl -fsS -X POST "$BASE_URL/v1/clusters" \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  -d "{\"name\":\"$CLUSTER_NAME\",\"nodes\":1}")
CLUSTER_ID=$(printf '%s' "$response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
resource_name="os-${CLUSTER_ID:0:12}"

wait_for_state() {
  local expected=$1
  for ((attempt = 1; attempt <= 180; attempt++)); do
    cluster_state=$(curl -fsS -H "Authorization: Bearer $ADMIN_API_KEY" "$BASE_URL/v1/clusters/$CLUSTER_ID" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')
    printf 'cluster=%s status=%s\n' "$CLUSTER_ID" "$cluster_state"
    [[ "$cluster_state" == "$expected" ]] && return 0
    [[ "$cluster_state" == FAILED ]] && { cat "$API_LOG"; return 1; }
    sleep 2
  done
  return 1
}

wait_for_state RUNNING
claim_name="data-$resource_name-0"
claim_state=$(kubectl -n dataplane-clusters get pvc "$claim_name" -o jsonpath='{.status.phase}')
[[ "$claim_state" == Bound ]] || { printf 'PVC state=%s, want Bound\n' "$claim_state" >&2; exit 1; }

kubectl -n dataplane-clusters exec "$resource_name-0" -- \
  curl -fsS -X PUT 'http://localhost:9200/orders/_doc/order-1001?refresh=true' \
  -H 'Content-Type: application/json' -d '{"status":"confirmed","total":149.95}' >/dev/null
volume_name=$(kubectl -n dataplane-clusters get pvc "$claim_name" -o jsonpath='{.spec.volumeName}')
kubectl -n dataplane-clusters delete pod "$resource_name-0" --wait=true
kubectl -n dataplane-clusters wait --for=condition=Ready "pod/$resource_name-0" --timeout=300s
rebound_volume=$(kubectl -n dataplane-clusters get pvc "$claim_name" -o jsonpath='{.spec.volumeName}')
[[ "$volume_name" == "$rebound_volume" ]] || { printf 'PVC rebound to a different volume\n' >&2; exit 1; }
kubectl -n dataplane-clusters exec "$resource_name-0" -- \
  curl -fsS 'http://localhost:9200/orders/_doc/order-1001' | grep -q '"found":true'

kubectl -n dataplane-clusters patch statefulset "$resource_name" --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/image","value":"opensearchproject/opensearch:3.7.0"}]' >/dev/null
curl -fsS -X POST "$BASE_URL/v1/clusters/$CLUSTER_ID/scale" \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H 'Content-Type: application/json' -d '{"nodes":2}' >/dev/null
wait_for_state RUNNING
reconciled_image=$(kubectl -n dataplane-clusters get statefulset "$resource_name" -o jsonpath='{.spec.template.spec.containers[0].image}')
[[ "$reconciled_image" == 'opensearchproject/opensearch:3.8.0' ]] || { printf 'image drift was not reconciled\n' >&2; exit 1; }
kubectl -n dataplane-clusters wait --for=condition=Ready pod -l "dataplane.io/cluster=$CLUSTER_ID" --timeout=300s

curl -fsS -X DELETE -H "Authorization: Bearer $ADMIN_API_KEY" "$BASE_URL/v1/clusters/$CLUSTER_ID" >/dev/null
wait_for_state DELETED
deleted_cluster_id=$CLUSTER_ID
CLUSTER_ID=""
remaining=$(kubectl -n dataplane-clusters get statefulset,service,pod,pvc -l "dataplane.io/cluster=$deleted_cluster_id" --no-headers 2>/dev/null | wc -l | tr -d ' ')
[[ "$remaining" == 0 ]] || { printf 'managed resources remain after delete\n' >&2; exit 1; }
printf 'Kind PVC, recovery, reconciliation, scale, and cleanup verification passed\n'
