# Fault injection scenarios

These scripts assume the full Compose stack and are intentionally **not** run by CI.
Run them on a disposable dev machine, observe the dashboards, then restore.

```bash
export COMPOSE_FILE=infra/docker-compose.full.yml
bash infra/fault_injection/stop_cassandra.sh
# /health reports source=demo; the API retries every 5s and switches back to
# Cassandra after restore, so no restart is required.
bash infra/fault_injection/restore.sh
```

## Scenarios

| Scenario | Script | Expected observable behavior | Recovery |
|---|---|---|---|
| Cassandra outage | `stop_cassandra.sh` | `/health` reports demo mode; API retries and recovers | `restore.sh` |
| Spark worker loss | `stop_spark_worker.sh` | Spark retries tasks; streaming may lag | `restore.sh`, check Spark UI |
| MinIO outage | `stop_minio.sh` | Kafka Connect S3 sink retries; batch jobs fail | `restore.sh`, replay batch |
| Consumer lag | `pause_consumer.sh` | Kafka group lag grows; streaming freshness drops | resume consumer / restart job |

## What to record

- Time to detect (Grafana/Prometheus), time to recover, data loss/duplicates.
- Whether the API stayed available and whether Grafana showed stale data.
- Any manual step needed; add it to `docs/runbook.md`.

## Known limitation

`market-api` retries the Cassandra connection every 5 seconds. A request that races
with a disconnect can return an empty list before the next request falls back to demo
data. Cassandra-backed requests are read-only and safe to retry.
