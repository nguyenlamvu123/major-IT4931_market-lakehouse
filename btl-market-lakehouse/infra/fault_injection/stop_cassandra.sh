#!/usr/bin/env bash
set -euo pipefail
COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose.full.yml}"
docker compose -f "$COMPOSE_FILE" stop cassandra
echo "Cassandra stopped. Restart market-api to force demo fallback if needed."
