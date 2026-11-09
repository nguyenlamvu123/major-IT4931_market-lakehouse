#!/usr/bin/env bash
set -euo pipefail
COMPOSE_FILE="${COMPOSE_FILE:-infra/docker-compose.full.yml}"
echo "Pause the streaming consumer with Ctrl-C in its terminal, or stop it:"
echo "  docker compose -f $COMPOSE_FILE stop streaming-job"
echo "Observe Kafka consumer-group lag, then restart it."
