#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"

echo "== market-api =="
curl -fsS "$API_URL/health" | python3 -m json.tool
curl -fsS "$API_URL/api/v1/bars?symbol=BTCUSDT&limit=2" \
  | python3 -c 'import json,sys; rows=json.load(sys.stdin); assert rows; print(f"bars: {len(rows)}")'
curl -fsS "$API_URL/metrics" | head -n 3

echo "== prometheus =="
curl -fsS "$PROM_URL/-/ready"
echo

echo "== grafana =="
curl -fsS "$GRAFANA_URL/api/health" | python3 -m json.tool

echo "SMOKE OK"
