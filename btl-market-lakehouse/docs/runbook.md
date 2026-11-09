# Runbook

## 1. Local demo (no Docker)

```bash
cd btl-market-lakehouse
python3 -m venv .venv && . .venv/bin/activate
pip install pandas pyarrow numpy pytest PyYAML
make samples
make demo
make test
make verify
```

Expected demo output: `data/demo/trades.jsonl`, `bars.jsonl`, `alerts.json`,
`daily_summary.csv`, `correlation_matrix.csv`, `yield_curve.csv`, `ml_features.csv`,
`summary.json`.

## 2. API only

```bash
pip install -r serving/requirements.txt
make api
curl localhost:8000/health
```

`/health` reports `"source": "demo"` when Cassandra is unavailable. This is expected
on the current machine.

## 3. Dev Compose

```bash
cp .env.example .env
make dev-up
make topics
make cassandra-init
docker compose -f infra/docker-compose.dev.yml ps
```

Wait 60-90 seconds for Cassandra. If Kafka restarts, check the KRaft volume:
`docker compose -f infra/docker-compose.dev.yml down -v` then start again.

## 4. Full stack

```bash
docker build -f serving/Dockerfile -t btl/market-api .
docker compose -f infra/docker-compose.full.yml up -d
```

Ports: Grafana 3000, Prometheus 9090, Airflow 8081, API 8000, Spark UI 8080.
Full stack is not runnable on the current 3.6 GB free disk; use `config -q` only.

## 5. Kubernetes

```bash
kubectl apply -k infra/k8s
kubectl -n market get pods,svc
kubectl -n market port-forward svc/market-api 8000:8000
```

Build images first (`btl/market-api`, `btl/airflow`) and push to a registry reachable
by the cluster.

## 6. Health checks

| Check | Command | Expected |
|---|---|---|
| API | `curl -fsS localhost:8000/health` | `status: ok` |
| Metrics | `curl -fsS localhost:8000/metrics` | Prometheus text |
| Kafka topics | `kafka-topics.sh --bootstrap-server localhost:9092 --list` | market.* |
| Cassandra | `cqlsh localhost 9042 -e "describe keyspaces"` | market |
| Grafana | `curl -fsS localhost:3000/api/health` | database ok |
| Prometheus | `curl -fsS localhost:9090/-/ready` | Ready |

## 7. Fault scenarios

See `infra/fault_injection/README.md`. After each test, restart the affected service
and verify the API `/health` and Grafana panels recover.

## 8. Rebuild a role branch

```bash
git checkout feature/D-infra
git merge --no-ff feature/D-ci
make test
```

Do not merge role branches into `main`; use an `integration` branch if needed.
