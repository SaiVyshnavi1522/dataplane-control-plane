#!/usr/bin/env bash
set -euo pipefail

BASE_URL=${BASE_URL:-http://localhost:8080}

restore_postgres() {
  docker compose start postgres >/dev/null 2>&1 || true
}
trap restore_postgres EXIT

before=$(curl -sS -o /dev/null -w '%{http_code}' "$BASE_URL/readyz")
[[ "$before" == 200 ]] || { printf 'readiness before outage=%s, want 200\n' "$before" >&2; exit 1; }

for ((attempt = 1; attempt <= 120; attempt++)); do
  active_jobs=$(docker compose exec -T postgres psql -U dataplane -d dataplane -Atc \
    "SELECT COUNT(*) FROM jobs WHERE status IN ('PENDING','RUNNING')")
  [[ "$active_jobs" == 0 ]] && break
  sleep 1
done
[[ "$active_jobs" == 0 ]] || { printf 'queue did not become idle; active jobs=%s\n' "$active_jobs" >&2; exit 1; }

docker compose stop postgres >/dev/null
during=$(curl -sS -o /dev/null -w '%{http_code}' "$BASE_URL/readyz")
[[ "$during" == 503 ]] || { printf 'readiness during outage=%s, want 503\n' "$during" >&2; exit 1; }

printf 'PostgreSQL recovery wall time (seconds):\n'
/usr/bin/time -p sh -c 'docker compose start postgres >/dev/null; until [ "$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/readyz)" = "200" ]; do sleep 0.1; done'
after=$(curl -sS -o /dev/null -w '%{http_code}' "$BASE_URL/readyz")
[[ "$after" == 200 ]] || { printf 'readiness after recovery=%s, want 200\n' "$after" >&2; exit 1; }
printf 'Readiness detected the outage and recovered\n'
