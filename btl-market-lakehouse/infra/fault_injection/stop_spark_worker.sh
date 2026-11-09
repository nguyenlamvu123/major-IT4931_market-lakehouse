#!/usr/bin/env bash
set -euo pipefail
COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose.full.yml}"
docker compose -f "$COMPOSE_FILE" stop spark-worker
echo "Spark worker stopped. Watch Spark UI/streaming lag."
