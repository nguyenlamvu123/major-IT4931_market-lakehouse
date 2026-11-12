#!/usr/bin/env bash
# Register the bronze S3 sinks on a Kafka Connect REST endpoint (Docker Compose dev stack).
# On Kubernetes the Strimzi KafkaConnector resources in ingest/k8s/kafka-connect/ do this.
#
# PUT /connectors/<name>/config expects the flat config map, not {"name", "config"};
# the previous script sent the whole file and Connect rejected it.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIGS=("$@")
if [ ${#CONFIGS[@]} -eq 0 ]; then
  CONFIGS=("${DIR}/s3-sink.json" "${DIR}/s3-sink-alerts.json")
fi

echo "Waiting for Kafka Connect at ${CONNECT_URL}..."
for _ in $(seq 1 60); do
  if curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

for file in "${CONFIGS[@]}"; do
  name="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "${file}")"
  echo "Registering ${name} from ${file}"
  python3 -c 'import json,sys; json.dump(json.load(open(sys.argv[1]))["config"], sys.stdout)' "${file}" \
    | curl -fsS -X PUT -H "Content-Type: application/json" --data @- \
        "${CONNECT_URL}/connectors/${name}/config"
  echo
  curl -fsS "${CONNECT_URL}/connectors/${name}/status"
  echo
done
