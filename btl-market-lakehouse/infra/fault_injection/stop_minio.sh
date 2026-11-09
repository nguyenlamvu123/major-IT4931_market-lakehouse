#!/usr/bin/env bash
set -euo pipefail
COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose.full.yml}"
docker compose -f "$COMPOSE_FILE" stop minio
echo "MinIO stopped. Kafka Connect S3 sink and batch jobs should retry/fail."
