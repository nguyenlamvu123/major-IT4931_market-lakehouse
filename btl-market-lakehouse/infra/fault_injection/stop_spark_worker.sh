#!/usr/bin/env bash
# Compose: dừng spark-worker. Khôi phục bằng restore.sh. Bản K8s nằm trong k8s/.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT}/infra/docker-compose.full.yml}"
docker compose -f "${COMPOSE_FILE}" stop spark-worker
echo "spark-worker stopped at $(date -u +%Y-%m-%dT%H:%M:%SZ); record detection/recovery in docs/lessons/D-fault-injection.md"
