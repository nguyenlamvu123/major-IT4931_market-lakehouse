#!/usr/bin/env bash
# Create the contract buckets (lake, checkpoints). The compose service "minio-init" runs
# the same commands on `docker compose up`; this re-runs it on demand.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT}/infra/docker-compose.dev.yml}"

docker compose -f "${COMPOSE_FILE}" run --rm minio-init </dev/null
