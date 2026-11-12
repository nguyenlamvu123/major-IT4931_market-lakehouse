#!/usr/bin/env bash
# Compose: khởi động lại mọi service mà các script thử lỗi đã dừng.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT}/infra/docker-compose.full.yml}"
docker compose -f "${COMPOSE_FILE}" start cassandra spark-worker minio kafka-connect
docker compose -f "${COMPOSE_FILE}" ps
