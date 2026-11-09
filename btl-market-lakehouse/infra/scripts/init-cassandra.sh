#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-$(dirname "$0")/../docker-compose.dev.yml}"
CONTAINER="${CASSANDRA_CONTAINER:-btl-cassandra}"

docker compose -f "${COMPOSE_FILE}" exec -T "${CONTAINER}" \
  cqlsh -f /contracts/cassandra/schema.cql

docker compose -f "${COMPOSE_FILE}" exec -T "${CONTAINER}" \
  cqlsh -e "DESCRIBE TABLES;"
