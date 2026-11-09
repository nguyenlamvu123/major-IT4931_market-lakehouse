# Data contracts (v1)

Owner: role A. Every producer, streaming job, batch job and dashboard must use these
names and shapes. Changes require review from at least one other role.

## Event conventions

- `event_time`: epoch **milliseconds**, UTC, when the market event happened.
- `ingest_time`: epoch milliseconds when the producer received the event (optional).
- `trade_id`: deterministic idempotency key. Re-sending the same trade must produce the
  same id.
- Topic names are lowercase, dot-separated, and never contain team/person names.

## Topics

See `topics.yaml`. Bronze objects use the same topic name as the path prefix, e.g.
`s3a://lake/bronze/crypto.trades/...`.

## Files

```text
contracts/
├── schemas/          JSON Schema for every event type
├── samples/          deterministic JSONL examples (generated)
├── cassandra/        logical Cassandra table contract
└── topics.yaml       Kafka topic configuration
```

## Generate samples

```bash
python scripts/generate_samples.py
```

## Compatibility rules

1. Additive fields only in v1; never rename or change the type of an existing field.
2. New required fields require a v2 topic and a migration note.
3. `asset_class` must be one of `crypto`, `equity`, `commodity`, `fx`, `rates`.
4. Invalid records go to `dead-letter` handling in the producer, not silently dropped.
