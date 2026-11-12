#!/usr/bin/env bash
# Create the contract topics in the dev Kafka container.
#
# The topic list comes from contracts/topics.yaml (via infra.scripts.topics_contract);
# nothing is hard-coded here, so renaming a topic in the contract is enough.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT}/infra/docker-compose.dev.yml}"
PYTHON="${PYTHON:-python3}"
# Dev runs one broker; the demo cluster on K8s uses RF 3 (see infra/k8s/kafka).
REPLICATION="${REPLICATION:-1}"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

cd "${ROOT}"
mapfile -t topics < <("${PYTHON}" -m infra.scripts.topics_contract --format cli)
if [[ ${#topics[@]} -eq 0 ]]; then
  echo "no topics found in contracts/topics.yaml" >&2
  exit 1
fi

for line in "${topics[@]}"; do
  read -r name partitions retention <<<"${line}"
  # </dev/null: `docker compose exec` forwards stdin and would otherwise swallow input.
  docker compose -f "${COMPOSE_FILE}" exec -T kafka \
    "${KAFKA_TOPICS}" --bootstrap-server kafka:29092 \
    --create --if-not-exists --topic "${name}" \
    --partitions "${partitions}" --replication-factor "${REPLICATION}" \
    --config "retention.ms=${retention}" </dev/null
done

docker compose -f "${COMPOSE_FILE}" exec -T kafka \
  "${KAFKA_TOPICS}" --bootstrap-server kafka:29092 --list </dev/null
