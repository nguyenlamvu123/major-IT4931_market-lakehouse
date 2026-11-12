#!/usr/bin/env bash
# make port-forward: expose the UIs on localhost until Ctrl-C.
set -euo pipefail
# shellcheck source=infra/scripts/lib.sh
source "$(dirname "$0")/lib.sh"
need kubectl

# local port|namespace|service|service port
FORWARDS=(
  "3000|monitoring|kube-prometheus-stack-grafana|80"
  "9090|monitoring|kube-prometheus-stack-prometheus|9090"
  "8000|market|market-api|8000"
  "8080|airflow|airflow-webserver|8080"
  "9001|minio|minio|9001"
)

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

for entry in "${FORWARDS[@]}"; do
  IFS='|' read -r local_port namespace service port <<<"${entry}"
  kubectl -n "${namespace}" port-forward "svc/${service}" "${local_port}:${port}" >/dev/null &
  pids+=("$!")
  echo "http://localhost:${local_port}  -> ${namespace}/${service}:${port}"
done
wait
