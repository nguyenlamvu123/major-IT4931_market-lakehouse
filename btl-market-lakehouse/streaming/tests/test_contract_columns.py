"""Speed-layer writes must match contracts/cassandra.cql exactly."""

from __future__ import annotations

import pytest

from common.cql import load_tables
from streaming.columns import CASSANDRA_WRITES, check_columns

# ROOT CAUSE:
#
# The v1 job wrote its enriched bar frame (21 columns: ema_12, ema_26, macd_hist,
# return_1m, volatility_30m, volume_zscore_30m, ...) straight into ohlcv_1m, whose CQL
# only had 15 columns, and wrote alerts without the `day` partition key of
# alerts_by_day. The Spark Cassandra Connector rejects such writes, so the job would
# have failed on its first micro-batch on the cluster. Unit tests never compared the
# frame with the CQL.
#
#   streaming/columns.py (v1): BAR_COLUMNS = [..., "ema_12", "ema_26", "macd_hist", ...]
#   self.sink.write_bars(bars_df)   # -> ohlcv_1m
#
# We fixed this by splitting the writes per contract table (ohlcv_1m/5m only carry
# OHLCV, indicators go to indicators_latest, alerts to alerts_by_day with `day`) and by
# checking every frame against the CQL here and at runtime in the sinks.


@pytest.mark.parametrize("table", sorted(CASSANDRA_WRITES))
def test_written_columns_exist_in_contract_table(table: str) -> None:
    contract = load_tables()[table]
    written = CASSANDRA_WRITES[table]
    assert set(written) <= set(contract.columns), set(written) - set(contract.columns)
    assert set(contract.primary_key) <= set(written)


def test_ohlcv_writes_cover_every_contract_column() -> None:
    # ohlcv_1m has no optional columns: a missing one would stay null forever.
    contract = load_tables()["ohlcv_1m"]
    assert set(CASSANDRA_WRITES["ohlcv_1m"]) == set(contract.columns)


def test_check_columns_rejects_drift() -> None:
    columns = list(CASSANDRA_WRITES["ohlcv_1m"])
    check_columns("ohlcv_1m", list(reversed(columns)))
    with pytest.raises(ValueError, match="extra=\\['ema_12'\\]"):
        check_columns("ohlcv_1m", columns + ["ema_12"])
    with pytest.raises(ValueError, match="missing=\\['day'\\]"):
        check_columns("ohlcv_1m", [column for column in columns if column != "day"])
    with pytest.raises(ValueError, match="does not write"):
        check_columns("daily_summary", columns)


def test_spark_schemas_follow_column_lists() -> None:
    pytest.importorskip("pyspark")
    from streaming.columns import ALERT_COLUMNS, INDICATOR_COLUMNS
    from streaming.schemas import ALERT_SCHEMA, INDICATOR_SCHEMA, TRADE_SCHEMA

    assert ALERT_SCHEMA.fieldNames() == ALERT_COLUMNS
    assert INDICATOR_SCHEMA.fieldNames() == INDICATOR_COLUMNS
    assert {"trade_id", "exchange_trade_id", "ingest_time"} <= set(TRADE_SCHEMA.fieldNames())
