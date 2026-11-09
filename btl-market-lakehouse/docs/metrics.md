# Metrics and SLOs

| Metric | Definition | Target (demo) | How measured |
|---|---|---|---|
| Ingestion throughput | trades/s accepted by producer | >= 50/s mock | producer log / Kafka offsets |
| Streaming latency | event time to Cassandra write | < 5 s p95 | Spark query progress |
| Batch freshness | gold partition age | < 24 h | Airflow run timestamp |
| API latency | `/api/v1/bars` p95 | < 300 ms demo | Prometheus histogram (add in prod) |
| Availability | API `/health` success | 99% dev | Prometheus `up` |
| Data correctness | duplicate trade_id in silver | 0 | batch dedupe check |
| Alert precision | alerts reviewed manually | >= 80% | sample audit |
| Resource use | full stack RAM | <= 10 GB dev | docker stats |

## Demo evidence

- Local pipeline: 2,397 trades, 17 symbols, 2,397 one-minute bars, 8 alerts.
- Batch: 1,833 silver trades, 273 daily rows, 78 correlation pairs, 20 ML rows,
  30 yield-curve rows.
- Tests: 7 passing on D branch (5 shared + 2 serving); 9 passing at C-airflow.

## Notes

- Targets are for the course demo, not production. Production would add histogram
  metrics, alert rules, and load tests.
- Prometheus counters are available at `/metrics`; the Grafana infra dashboard uses
  `up`, request rate, RSS and scrape duration.
