# Kafka Connect S3 sink (bronze)

Role A owns this connector. It copies Kafka topics to MinIO/S3 as JSON files in the
bronze layer. Role C reads these files as the batch source of truth.

## Prerequisites

- Kafka Connect image with the Confluent S3 connector installed, e.g.
  `confluentinc/cp-kafka-connect:7.6.1` plus `confluentinc-kafka-connect-s3`.
- MinIO bucket `lake` and a network alias `minio` reachable from Connect.
- `dead-letter` topic created (or remove the DLQ field in `s3-sink.json`).

## Register

```bash
export CONNECT_URL=http://localhost:8083
bash ingest/kafka_connect/register_connector.sh
```

## Output layout

```text
s3a://lake/bronze/<topic>/dt=YYYY-MM-DD/hour=HH/<topic>+<partition>+<offset>.json
```

`timestamp.field=event_time` means late events go to the hour of the market event, not
the hour of ingestion. This makes replay deterministic.

## Verify

```bash
curl -s http://localhost:8083/connectors/s3-sink-bronze/status | jq
mc ls -r local/lake/bronze | head
```

## Failure handling

- `errors.tolerance=all` keeps the connector running; inspect the `dead-letter` topic.
- If the S3 plugin is missing, Connect returns `Connector class not found`; rebuild the
  image with the plugin before registering.
