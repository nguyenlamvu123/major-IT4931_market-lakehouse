# Ingest (role A)

Producers read market data, normalise it to `contracts/schemas/trade.schema.json` and
publish JSON to Kafka with the **symbol/tenor as key**. The key preserves per-symbol
ordering, which the streaming layer relies on.

## Source matrix

| Source | Asset class | Mode | Required env |
|---|---|---|---|
| `mock` | crypto/equity/commodity/fx | deterministic random walk | none |
| `replay` | any JSONL sample | replay file | none |
| `binance` | crypto | websocket | none |
| `alpaca` | equity | REST poll | `ALPACA_API_KEY`, `ALPACA_SECRET_KEY` |
| `finnhub` | equity | REST poll | `FINNHUB_API_KEY` |
| `yahoo` | equity/ETF | yfinance poll | none |
| `fred` | rates | REST poll | `FRED_API_KEY` |

## Dry-run without Kafka

```bash
python ingest/run_producer.py --source mock --rows 20 --dry-run
python ingest/run_producer.py --source replay --file contracts/samples/equity.trades.jsonl --dry-run
```

## Run against Kafka

```bash
export KAFKA_BOOTSTRAP=localhost:9092
python ingest/run_producer.py --source binance --symbols BTCUSDT,ETHUSDT
python ingest/run_producer.py --source fred --interval 3600
```

## Delivery guarantees

- `enable.idempotence=true`, `acks=all`, retries = 5.
- `trade_id` is deterministic, so downstream Cassandra upserts are idempotent.
- Invalid records should be logged and routed to a dead-letter topic by the caller; the
  current producers skip malformed source messages and keep the stream alive.

## Docker

Build from the project root:

```bash
docker build -f ingest/Dockerfile -t market-ingest .
docker run --rm --env-file .env market-ingest
```
