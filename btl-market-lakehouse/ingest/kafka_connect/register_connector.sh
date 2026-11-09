#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_FILE="${1:-$(dirname "$0")/s3-sink.json}"

echo "Waiting for Kafka Connect at ${CONNECT_URL}..."
for _ in $(seq 1 30); do
  if curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Registering ${CONFIG_FILE}"
curl -fsS -X PUT \
  -H "Content-Type: application/json" \
  --data @"${CONFIG_FILE}" \
  "${CONNECT_URL}/connectors/s3-sink-bronze/config"

echo
curl -fsS "${CONNECT_URL}/connectors/s3-sink-bronze/status"
echo
