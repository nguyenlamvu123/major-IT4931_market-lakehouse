# Monitoring and dashboards (role D)

## Components

- `prometheus.yml`: scrapes the API, Kafka exporter and Spark master.
- `grafana/provisioning/`: datasource + dashboard provisioning for Compose and K8s.
- `grafana/dashboards/market-overview.json`: OHLCV table, alerts table, API metrics.
- `grafana/dashboards/infra-health.json`: scrape health, request rate, memory.

## Run with Compose

```bash
docker compose -f infra/docker-compose.full.yml up -d market-api prometheus grafana
```

Open Grafana at `http://localhost:3000` (`admin` / `admin` by default). The Infinity
plugin is installed by Compose; the API is used as a JSON datasource.

Build the API first from the project root:

```bash
docker build -f serving/Dockerfile -t btl/market-api .
```

## Run the API without Docker

```bash
pip install -r serving/requirements.txt
uvicorn serving.market_api:app --port 8000
```

The API falls back to deterministic demo data when Cassandra is absent and retries
the connection every 5 seconds.

## Prometheus queries

- `sum(rate(market_api_requests_total[5m]))` - request rate.
- `process_resident_memory_bytes{job="market-api"}` - API memory.
- `up{job="market-api"}` - API scrape health.

## K8s

`infra/k8s/kustomization.yaml` generates ConfigMaps from the provisioning files.
`kubectl apply -k infra/k8s` mounts them into Grafana automatically.

## Limitations

- The dashboard's tables depend on the Infinity plugin; install it if running Grafana
  outside the provided Compose/K8s manifests.
- Kafka/Spark exporters are not deployed by the dev stack; those panels stay empty
  until exporters are added.
- No alert rules are provisioned; alerts are stored in Cassandra and shown as a table.
