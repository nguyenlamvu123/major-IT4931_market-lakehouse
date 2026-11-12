#!/usr/bin/env bash
# Smoke test of the serving/monitoring layer once a stack is up
# (compose full: default URLs; K8s: run `make port-forward` first).
#
# Fails if an endpoint the dashboards use does not answer, and prints which data source
# answered (cassandra = real data, demo = synthetic laptop fallback).
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
# Set REQUIRE_REAL_DATA=1 on the cluster: demo data must not be accepted there.
REQUIRE_REAL_DATA="${REQUIRE_REAL_DATA:-0}"

echo "== market-api =="
curl -fsS "${API_URL}/health" | python3 -m json.tool
for path in \
  "/api/v1/ohlcv?symbol=BTCUSDT&limit=5" \
  "/api/v1/indicators/latest" \
  "/api/v1/alerts?limit=5" \
  "/api/v1/daily?symbol=SPY&limit=5" \
  "/api/v1/correlation?window_days=90" \
  "/api/v1/yield-curve/latest"; do
  source="$(curl -fsS -o /dev/null -D - "${API_URL}${path}" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-data-source"{print $2}')"
  echo "${path} -> ${source}"
  if [[ "${REQUIRE_REAL_DATA}" == "1" && "${source}" != "cassandra" ]]; then
    echo "expected real data from Cassandra, got '${source}'" >&2
    exit 1
  fi
done
curl -fsS "${API_URL}/metrics" | grep -m 1 market_api_request_duration_seconds

echo "== prometheus =="
curl -fsS "${PROM_URL}/-/ready"
echo

echo "== grafana =="
curl -fsS "${GRAFANA_URL}/api/health" | python3 -m json.tool

echo "SMOKE OK"
