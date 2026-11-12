"""Spark schemas for contract v2 trade events and the frames built from pandas."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .columns import ALERT_COLUMNS, INDICATOR_COLUMNS

#: ``contracts/schemas/trade.schema.json`` (crypto.trades, stocks.trades, gold.quotes,
#: fx.dxy). Unknown extra fields such as ``metadata`` are ignored by ``from_json``.
TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("asset_class", StringType(), True),
        StructField("event_time", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("source", StringType(), True),
        StructField("ingest_time", LongType(), True),
        StructField("exchange_trade_id", StringType(), True),
    ]
)

_TYPES = {
    "day": DateType(),
    "alert_time": TimestampType(),
    "window_start": TimestampType(),
    "updated_at": TimestampType(),
    "symbol": StringType(),
    "alert_id": StringType(),
    "asset_class": StringType(),
    "alert_type": StringType(),
    "severity": StringType(),
    "message": StringType(),
}

# Frames computed on the driver with pandas (indicators, alerts) are converted back to
# Spark with an explicit schema: inferring it from a batch where a column is all-null
# would produce NullType, which the Cassandra connector cannot write.
INDICATOR_SCHEMA = StructType(
    [StructField(name, _TYPES.get(name, DoubleType()), True) for name in INDICATOR_COLUMNS]
)
ALERT_SCHEMA = StructType(
    [StructField(name, _TYPES.get(name, DoubleType()), True) for name in ALERT_COLUMNS]
)
