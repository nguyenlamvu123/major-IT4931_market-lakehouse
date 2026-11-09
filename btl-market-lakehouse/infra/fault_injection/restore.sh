#!/usr/bin/env bash
set -euo pipefail
COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose.full.yml}"
docker compose -f "$COMPOSE_FILE" start cassandra spark-worker minio
docker compose -f "$COMPOSE_FILE" ps
