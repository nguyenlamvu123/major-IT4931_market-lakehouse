#!/usr/bin/env bash
# make secrets: create the Kubernetes Secrets of contracts/infra.yaml from .env.
#
#   minio-credentials  (MINIO_ACCESS_KEY, MINIO_SECRET_KEY) in ns minio, spark, airflow, market
#   market-api-keys    (ALPACA_API_KEY, ALPACA_SECRET_KEY, FINNHUB_API_KEY, FRED_API_KEY) in ns market
#
# Idempotent (create --dry-run | apply). Values never touch git: .env is gitignored.
set -euo pipefail
# shellcheck source=infra/scripts/lib.sh
source "$(dirname "$0")/lib.sh"
need kubectl

access_key="$(env_value MINIO_ACCESS_KEY)"
secret_key="$(env_value MINIO_SECRET_KEY)"
if [[ -z "${access_key}" || -z "${secret_key}" ]]; then
  # Dev convenience only: the same defaults as docker compose. Never use them on a cluster
  # reachable from outside the laptop.
  warn "MINIO_ACCESS_KEY/MINIO_SECRET_KEY not set in .env; using dev defaults minioadmin"
  access_key="${access_key:-minioadmin}"
  secret_key="${secret_key:-minioadmin}"
fi

for namespace in minio spark airflow market; do
  kubectl create namespace "${namespace}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n "${namespace}" create secret generic minio-credentials \
    --from-literal=MINIO_ACCESS_KEY="${access_key}" \
    --from-literal=MINIO_SECRET_KEY="${secret_key}" \
    --dry-run=client -o yaml | kubectl apply -f -
done

# Missing API keys are allowed: producers fall back to keyless sources (Binance, Yahoo,
# Treasury CSV, replay) and log which feed is disabled.
kubectl -n market create secret generic market-api-keys \
  --from-literal=ALPACA_API_KEY="$(env_value ALPACA_API_KEY)" \
  --from-literal=ALPACA_SECRET_KEY="$(env_value ALPACA_SECRET_KEY)" \
  --from-literal=FINNHUB_API_KEY="$(env_value FINNHUB_API_KEY)" \
  --from-literal=FRED_API_KEY="$(env_value FRED_API_KEY)" \
  --dry-run=client -o yaml | kubectl apply -f -
