# Architecture

## Data flow

```text
Binance/Alpaca/Finnhub/Yahoo/FRED
              |
              v
      A: producers (Python)
              |
              v
      Kafka topics: market.trades, market.quotes, market.rates
              |
      +-------+-------------------------------+
      |                                       |
      v                                       v
B: Spark Structured Streaming          A: Kafka Connect S3 sink
  OHLCV 1m + indicators                  |
  alerts                                 v
      |                            MinIO bronze/
      v                                       |
Cassandra: ohlcv_1m, alerts,               v
latest_price                        C: bronze -> silver -> gold
                                            |
                                            v
                                   MinIO silver/, gold/
                                            |
                          +-----------------+-----------------+
                          |                                   |
                          v                                   v
                  C: Spark MLlib                    C: Airflow DAGs
                  (KMeans regimes)                  (orchestration/retry)
                          |
                          v
                  D: market-api (FastAPI)
                          |
              +-----------+-----------+
              |                       |
              v                       v
      Grafana dashboards        Prometheus metrics
```

## Components

| Layer | Component | Responsibility |
|---|---|---|
| Ingestion | A producers | REST/WebSocket pulls, normalise to contracts, publish Kafka |
| Ingestion | Kafka + Connect | Durable topic buffer, S3 sink to bronze |
| Speed | Spark Structured Streaming | 1-minute OHLCV, VWAP, SMA/RSI/MACD, z-score, alerts |
| Speed | Cassandra | Low-latency latest bars, alerts, latest price |
| Batch | Spark jobs | Clean/dedupe, daily aggregates, correlation, yield curve, ML |
| Batch | Airflow | DAG scheduling, retries, backfills |
| Serving | market-api | Read Cassandra or demo fallback, JSON for Grafana |
| Observability | Prometheus/Grafana | Service health, request rate, memory, dashboards |
| Runtime | Compose / Kubernetes | Dev stack and single-node K8s manifests |

## Contracts

Topics, JSON schemas, Cassandra CQL and S3 paths live in `contracts/`. Any change must
be reviewed by another role before merge. Producers and consumers validate against the
same contract names to avoid silent schema drift.

## Consistency model

- Kafka is the source of truth for realtime events; Cassandra is a serving view.
- Bronze is append-only raw data; silver is deduplicated/typed; gold is analytics.
- Re-running batch jobs is idempotent by partition overwrite or `MERGE`-style
  dedupe keys (symbol + window).
- The speed and batch layers may disagree briefly; the batch layer wins after
  reconciliation.

## Deployment modes

1. Local demo: pandas only, no Kafka/Spark, used on the current 8 GB machine.
2. Dev Compose: Kafka + MinIO + Cassandra.
3. Full Compose/K8s: all components; needs ~12-16 GB RAM and 12+ GB free disk.

## Known architectural limits

- Single Kafka broker and single Cassandra node in the provided manifests.
- No schema registry; contracts are validated in application code.
- No exactly-once end-to-end guarantee; streaming uses checkpoints and idempotent
  writes where possible.
- Grafana tables depend on the Infinity plugin; Prometheus covers infra panels.
