#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-$(dirname "$0")/../docker-compose.dev.yml}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"

topics=(
  "crypto.trades:3"
  "equity.trades:3"
  "commodity.trades:3"
  "fx.trades:3"
  "rates.ticks:1"
  "market.alerts:3"
  "dead-letter:1"
)

for item in "${topics[@]}"; do
  topic="${item%%:*}"
  partitions="${item##*:}"
  docker compose -f "${COMPOSE_FILE}" exec -T kafka \
    kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists --topic "${topic}" \
    --partitions "${partitions}" --replication-factor 1
done

docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list
