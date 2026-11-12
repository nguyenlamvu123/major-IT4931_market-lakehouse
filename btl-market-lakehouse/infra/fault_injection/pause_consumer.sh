#!/usr/bin/env bash
# Compose: dừng Kafka Connect để consumer lag của connect-s3-sink-bronze* tăng
# (bản K8s: k8s/stop_minio.sh hoặc k8s/flood_replay.sh). Khôi phục bằng restore.sh.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT}/infra/docker-compose.full.yml}"
docker compose -f "${COMPOSE_FILE}" stop kafka-connect
echo "Kafka Connect stopped: watch 'Kafka consumer lag' grow on the monitoring dashboard."
