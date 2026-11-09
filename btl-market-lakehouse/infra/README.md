# Infrastructure (role D)

## Dev stack: Kafka + MinIO + Cassandra

```bash
cp infra/.env.example .env
docker compose -f infra/docker-compose.dev.yml up -d
bash infra/scripts/init-topics.sh
bash infra/scripts/init-cassandra.sh
bash infra/scripts/init-minio.sh
```

Ports:

| Service | Port |
|---|---|
| Kafka (host) | 9092 |
| MinIO API / console | 9000 / 9001 |
| Cassandra | 9042 |

Stop and remove volumes:

```bash
docker compose -f infra/docker-compose.dev.yml down -v
```

## Full stack

`infra/docker-compose.full.yml` adds Kafka Connect, Spark master/worker, Airflow,
market-api, Prometheus and Grafana.

```bash
docker compose -f infra/docker-compose.full.yml up -d
```

**Resource warning:** the full stack needs roughly 12-16 GB RAM and 12+ GB free disk
for images. The current machine has ~7.6 GB RAM and ~3.6 GB free disk, so use the dev
stack + local demo. The full compose file is still validated in CI with
`docker compose config`.

## Kafka Connect S3 sink

The Connect image does **not** include the Confluent S3 plugin by default. Build a
custom Connect image with `confluentinc-kafka-connect-s3`, then register:

```bash
bash ingest/kafka_connect/register_connector.sh
```

## Troubleshooting

- `Kafka` unhealthy: check `docker compose logs kafka`; KRaft requires a clean volume
  after changing `KAFKA_CFG_NODE_ID`.
- `Cassandra` takes 60-90s to start; wait for the healthcheck before running CQL.
- MinIO bucket creation uses the `minio/mc` image and the compose network
  `btl-market-dev_market`; override with `MINIO_NETWORK=...`.
- If `docker compose config` complains about the monitoring mount in the full stack,
  the dashboards branch must be checked out first.
