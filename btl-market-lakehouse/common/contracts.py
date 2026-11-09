"""Data contracts and small synthetic-data helpers (shared by all layers)."""

from __future__ import annotations

import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any

TOPICS: dict[str, dict[str, Any]] = {
    "crypto.trades": {"partitions": 3, "replication": 1, "key": "symbol"},
    "equity.trades": {"partitions": 3, "replication": 1, "key": "symbol"},
    "commodity.trades": {"partitions": 3, "replication": 1, "key": "symbol"},
    "fx.trades": {"partitions": 3, "replication": 1, "key": "symbol"},
    "rates.ticks": {"partitions": 1, "replication": 1, "key": "tenor"},
    "market.alerts": {"partitions": 3, "replication": 1, "key": "symbol"},
}

ASSET_CLASSES = {
    "crypto": ("BTCUSDT", "ETHUSDT", "SOLUSDT"),
    "equity": ("AAPL", "MSFT", "NVDA", "SPY"),
    "commodity": ("GLD", "SLV", "XAUUSD"),
    "fx": ("DXY", "EURUSD", "USDJPY"),
    "rates": ("2Y", "5Y", "10Y", "30Y"),
}

BASE_PRICES = {
    "BTCUSDT": 67_000.0,
    "ETHUSDT": 3_400.0,
    "SOLUSDT": 170.0,
    "AAPL": 190.0,
    "MSFT": 420.0,
    "NVDA": 115.0,
    "SPY": 520.0,
    "GLD": 210.0,
    "SLV": 28.0,
    "XAUUSD": 2_350.0,
    "DXY": 104.5,
    "EURUSD": 1.08,
    "USDJPY": 155.0,
}

REQUIRED_TRADE_FIELDS = (
    "symbol",
    "asset_class",
    "event_time",
    "price",
    "volume",
    "source",
)


def validate_trade(record: dict[str, Any]) -> list[str]:
    """Return a list of validation errors; an empty list means the record is valid."""
    errors: list[str] = []
    for field in REQUIRED_TRADE_FIELDS:
        if field not in record or record[field] in (None, ""):
            errors.append(f"missing:{field}")
    if errors:
        return errors

    try:
        price = float(record["price"])
        volume = float(record["volume"])
    except (TypeError, ValueError):
        return ["price/volume must be numeric"]

    if price <= 0:
        errors.append("price must be > 0")
    if volume < 0:
        errors.append("volume must be >= 0")
    if not isinstance(record["event_time"], (int, float)):
        errors.append("event_time must be epoch milliseconds")
    if record["asset_class"] not in ASSET_CLASSES:
        errors.append(f"unknown asset_class: {record['asset_class']}")
    return errors


def trade_id(record: dict[str, Any]) -> str:
    raw = "|".join(
        str(record.get(key, ""))
        for key in ("source", "symbol", "event_time", "price", "volume")
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def make_trade(
    symbol: str,
    asset_class: str,
    event_time: int,
    price: float,
    volume: float,
    source: str,
    **extra: Any,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "symbol": symbol,
        "asset_class": asset_class,
        "event_time": int(event_time),
        "price": float(price),
        "volume": float(volume),
        "source": source,
    }
    record.update(extra)
    record.setdefault("trade_id", trade_id(record))
    return record


def topic_for(asset_class: str, kind: str = "trades") -> str:
    mapping = {
        ("crypto", "trades"): "crypto.trades",
        ("equity", "trades"): "equity.trades",
        ("commodity", "trades"): "commodity.trades",
        ("fx", "trades"): "fx.trades",
        ("rates", "ticks"): "rates.ticks",
        ("market", "alerts"): "market.alerts",
    }
    return mapping.get((asset_class, kind), f"{asset_class}.{kind}")


def synthetic_trades(
    rows: int = 2_400,
    seed: int = 42,
    start: datetime | None = None,
    days: int = 20,
) -> list[dict[str, Any]]:
    """Generate a deterministic random-walk stream spread over several days."""
    rng = random.Random(seed)
    start = start or datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc)
    symbols = [symbol for values in ASSET_CLASSES.values() for symbol in values]
    prices = {symbol: BASE_PRICES.get(symbol, 100.0) for symbol in symbols}
    asset_class_of = {
        symbol: asset_class
        for asset_class, values in ASSET_CLASSES.items()
        for symbol in values
    }
    per_symbol = max(1, rows // len(symbols))
    events_per_day = max(1, per_symbol // max(1, days))
    interval_seconds = max(60, 86_400 // events_per_day)

    records: list[dict[str, Any]] = []
    for symbol in symbols:
        for index in range(per_symbol):
            shock = rng.gauss(0, 0.0015)
            prices[symbol] = max(0.01, prices[symbol] * (1 + shock))
            day = index // events_per_day
            slot = index % events_per_day
            event_time = start + timedelta(days=day, seconds=slot * interval_seconds)
            records.append(
                make_trade(
                    symbol=symbol,
                    asset_class=asset_class_of[symbol],
                    event_time=int(event_time.timestamp() * 1000),
                    price=round(prices[symbol], 6),
                    volume=round(rng.uniform(0.01, 25.0), 6),
                    source="demo",
                )
            )
    return sorted(records, key=lambda record: (record["event_time"], record["symbol"]))


def synthetic_yields(days: int = 30, seed: int = 7) -> list[dict[str, Any]]:
    """Generate a small daily Treasury curve for the local demo."""
    rng = random.Random(seed)
    tenors = {"2Y": 4.7, "5Y": 4.3, "10Y": 4.2, "30Y": 4.4}
    start = datetime(2026, 1, 2, tzinfo=timezone.utc)
    records: list[dict[str, Any]] = []
    for day in range(days):
        date = (start + timedelta(days=day)).strftime("%Y-%m-%d")
        for tenor, base in tenors.items():
            records.append(
                {
                    "day": date,
                    "tenor": tenor,
                    "yield": round(base + rng.gauss(0, 0.03), 5),
                    "source": "demo",
                }
            )
    return records
