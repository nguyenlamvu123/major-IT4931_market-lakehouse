#!/usr/bin/env bash
# Re-apply contracts/cassandra.cql to the dev Cassandra (idempotent: IF NOT EXISTS).
# The compose service "cassandra-init" already does this on `docker compose up`; use this
# script after editing the contract without restarting the stack.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT}/infra/docker-compose.dev.yml}"

docker compose -f "${COMPOSE_FILE}" exec -T cassandra \
  cqlsh -f /contracts/cassandra.cql </dev/null
docker compose -f "${COMPOSE_FILE}" exec -T cassandra \
  cqlsh -e "DESCRIBE TABLES;" </dev/null
