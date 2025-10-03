#!/usr/bin/env bash
set -euo pipefail

BASE_URL=${BASE_URL:-http://localhost:8080}
ADMIN_API_KEY=${ADMIN_API_KEY:-local-admin-key-change-me}
VIEWER_API_KEY=${VIEWER_API_KEY:-local-viewer-key-change-me}
suffix="$(date +%s)-$$"
anonymous_request="security-anonymous-$suffix"
forbidden_request="security-forbidden-$suffix"

anonymous_code=$(curl -sS -o /dev/null -w '%{http_code}' "$BASE_URL/v1/clusters" \
  -H "X-Request-ID: $anonymous_request")
[[ "$anonymous_code" == 401 ]] || { printf 'anonymous status=%s, want 401\n' "$anonymous_code" >&2; exit 1; }

viewer_code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/v1/clusters" \
  -H "Authorization: Bearer $VIEWER_API_KEY" \
  -H "X-Request-ID: $forbidden_request" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: security-$suffix" \
  -d '{"name":"security-search","nodes":1}')
[[ "$viewer_code" == 403 ]] || { printf 'viewer mutation status=%s, want 403\n' "$viewer_code" >&2; exit 1; }

audit_response=$(curl -fsS "$BASE_URL/v1/audit-events?limit=200" \
  -H "Authorization: Bearer $ADMIN_API_KEY")
printf '%s' "$audit_response" | grep -q "\"request_id\":\"$anonymous_request\""
printf '%s' "$audit_response" | grep -q "\"request_id\":\"$forbidden_request\""
printf '%s' "$audit_response" | grep -q '"outcome":"FAILURE"'
printf 'Authentication, authorization, request ID, and durable audit verification passed\n'
