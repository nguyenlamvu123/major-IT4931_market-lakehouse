# Fault injection scenarios

These scripts assume the full Compose stack and are intentionally **not** run by CI.
Run them on a disposable dev machine, observe the dashboards, then restore.

```bash
export COMPOSE_FILE=infra/docker-compose.full.yml
bash infra/fault_injection/stop_cassandra.sh
# API /health still works if it started in demo mode; restart it to force fallback:
docker compose -f "$COMPOSE_FILE" restart market-api
bash infra/fault_injection/restore.sh
```

## Scenarios

| Scenario | Script | Expected observable behavior | Recovery |
|---|---|---|---|
| Cassandra outage | `stop_cassandra.sh` | API starts in demo mode if Cassandra is down at boot | `restore.sh`, restart API |
| Spark worker loss | `stop_spark_worker.sh` | Spark retries tasks; streaming may lag | `restore.sh`, check Spark UI |
| MinIO outage | `stop_minio.sh` | Kafka Connect S3 sink retries; batch jobs fail | `restore.sh`, replay batch |
| Consumer lag | `pause_consumer.sh` | Kafka group lag grows; streaming freshness drops | resume consumer / restart job |

## What to record

- Time to detect (Grafana/Prometheus), time to recover, data loss/duplicates.
- Whether the API stayed available and whether Grafana showed stale data.
- Any manual step needed; add it to `docs/runbook.md`.

## Known limitation

`market-api` chooses Cassandra vs demo mode at startup. If Cassandra fails later,
requests to Cassandra-backed endpoints return errors until the pod/container restarts.
