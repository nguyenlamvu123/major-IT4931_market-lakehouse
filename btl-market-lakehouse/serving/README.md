# Market API (role D)

Small read-only API that turns Cassandra bars/alerts into JSON for Grafana. If
Cassandra is not reachable, it serves deterministic demo data from `common/`, so the
dashboard can be developed on the current machine.

## Run locally

```bash
pip install -r serving/requirements.txt
uvicorn serving.market_api:app --reload --port 8000
curl localhost:8000/health
curl 'localhost:8000/api/v1/bars?symbol=BTCUSDT&limit=5'
```

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | liveness + Cassandra/demo mode |
| `GET /api/v1/bars?symbol=BTCUSDT&limit=60` | OHLCV + indicators |
| `GET /api/v1/alerts?symbol=BTCUSDT&limit=50` | price/volume alerts |
| `GET /api/v1/latest?symbol=BTCUSDT` | latest close |
| `GET /api/v1/summary` | dashboard symbol list |
| `GET /metrics` | Prometheus metrics |

## Docker

Build from the project root (the Dockerfile copies `common/` and `serving/`):

```bash
docker build -f serving/Dockerfile -t btl/market-api .
docker run --rm -p 8000:8000 -e CASSANDRA_HOSTS=cassandra btl/market-api
```

## Grafana

Use the Infinity datasource with base URL `http://market-api:8000` and the JSON
endpoints above. The provisioned dashboard in `infra/monitoring/grafana/dashboards/`
uses the `${symbol}` template variable.
