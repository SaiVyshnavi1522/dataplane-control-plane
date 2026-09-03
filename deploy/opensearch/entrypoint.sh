#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  printf '%s' "$AWS_ACCESS_KEY_ID" | /usr/share/opensearch/bin/opensearch-keystore add --stdin --force s3.client.default.access_key
  printf '%s' "$AWS_SECRET_ACCESS_KEY" | /usr/share/opensearch/bin/opensearch-keystore add --stdin --force s3.client.default.secret_key
fi

exec /usr/share/opensearch/opensearch-docker-entrypoint.sh "$@"
