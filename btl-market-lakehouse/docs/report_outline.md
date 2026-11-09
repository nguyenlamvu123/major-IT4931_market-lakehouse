# Report outline

## 1. Problem and scope

- Monitor US equities, gold, crypto and US Treasury yields in near realtime.
- Two paths: speed layer for 1-minute bars/alerts, batch layer for daily analytics/ML.
- Four roles with separate branches; no merge into `main` for submission.

## 2. Architecture

- Ingestion: producers -> Kafka -> Kafka Connect S3 sink.
- Speed: Spark Structured Streaming -> Cassandra.
- Batch: MinIO bronze/silver/gold -> Spark jobs -> Airflow.
- Serving: FastAPI -> Grafana; Prometheus for infra health.
- Include the diagram from `docs/architecture.md`.

## 3. Role implementation

| Role | Evidence |
|---|---|
| A | `ingest/`, `contracts/`, producer tests, Kafka Connect config |
| B | `streaming/`, OHLCV/indicator logic, Cassandra sink, streaming job |
| C | `batch/`, medallion jobs, MLlib, Airflow DAG |
| D | `infra/`, `serving/`, K8s manifests, monitoring, CI |

## 4. Data contracts

- Topics: `market.trades`, `market.quotes`, `market.rates`.
- Schemas and CQL in `contracts/`; at-least-once delivery; idempotent batch writes.
- Bronze/silver/gold paths and partitioning.

## 5. Demo results

- Local pipeline: 2,397 trades, 17 symbols, 2,397 bars, 8 alerts.
- Batch: 1,833 silver trades, 273 daily rows, 78 correlation pairs, 20 ML rows,
  30 yield-curve rows.
- Tests: 7 passing on D, 9 passing at C-airflow.
- Screenshots to add: Grafana overview, alerts table, Airflow DAG, Kafka topics.

## 6. Operations

- Dev/full Compose, Kubernetes manifests, health checks, fault injection.
- Resource limits and why the full stack was not run locally (3.6 GB free disk).
- Recovery steps from `docs/runbook.md`.

## 7. Limitations and lessons

- Single-broker Kafka, single-node Cassandra, no schema registry.
- Spark/Cassandra integration not executed on the current machine.
- API falls back to demo data only when Cassandra is unavailable at startup.
- Lessons: contracts first, local demo unblocks integration, branch isolation helps
  parallel work but requires an explicit integration step.

## 8. References

- Course assignment PDF and role split.
- Spark Structured Streaming, Kafka Connect S3, Cassandra, Airflow, Grafana docs.
- Repository branch map in `docs/branching.md`.
