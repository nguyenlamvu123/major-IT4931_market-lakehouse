"""Executable mirror of data contract v2 (shared by producers, streaming, batch, demo).

Source of truth: ``BTL-IT4931_Kien-truc-Phan-cong-GitHub.pdf`` (09/11/2026), section
"Hợp đồng giữa các vai". The reviewed JSON Schemas / CQL live in ``contracts/`` on the
A branch. Renaming a topic, field or table here requires the same change there, and a
review from the consuming roles.

Contract v2 in one screen:

- Topics: ``crypto.trades``, ``stocks.trades``, ``stocks.bars``, ``gold.quotes``,
  ``fx.dxy``, ``rates.yields``, ``alerts`` (+ ``dead-letter`` for rejected records).
- A topic is chosen by *feed* (where the event comes from), ``asset_class`` describes
  the *underlying*. Example: GLD trades arrive on ``stocks.trades`` (Alpaca feed) but
  carry ``asset_class="gold"`` so the batch layer can compare gold with yields.
- Every price event is JSON keyed by ``symbol`` (rates are keyed by ``tenor``), with
  ``event_time`` in epoch milliseconds UTC and a deterministic id for de-duplication.
"""

from __future__ import annotations

import hashlib
import math
import random
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

CONTRACT_VERSION = 2

#: In-cluster Kafka address (Strimzi cluster ``market`` in namespace ``kafka``).
KAFKA_BOOTSTRAP_K8S = "market-kafka-bootstrap.kafka.svc:9092"

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS

TOPICS: dict[str, dict[str, Any]] = {
    "crypto.trades": {
        "partitions": 3,
        "retention_ms": WEEK_MS,
        "key": "symbol",
        "schema": "trade",
        "producer": "A",
    },
    "stocks.trades": {
        "partitions": 3,
        "retention_ms": WEEK_MS,
        "key": "symbol",
        "schema": "trade",
        "producer": "A",
    },
    "stocks.bars": {
        "partitions": 3,
        "retention_ms": 30 * DAY_MS,
        "key": "symbol",
        "schema": "bar",
        "producer": "A",
    },
    "gold.quotes": {
        "partitions": 1,
        "retention_ms": WEEK_MS,
        "key": "symbol",
        "schema": "trade",
        "producer": "A",
    },
    "fx.dxy": {
        "partitions": 1,
        "retention_ms": WEEK_MS,
        "key": "symbol",
        "schema": "trade",
        "producer": "A",
    },
    "rates.yields": {
        "partitions": 1,
        "retention_ms": 30 * DAY_MS,
        "key": "tenor",
        "schema": "rate_yield",
        "producer": "A",
    },
    "alerts": {
        "partitions": 3,
        "retention_ms": 3 * DAY_MS,
        "key": "symbol",
        "schema": "alert",
        "producer": "B",
    },
    "dead-letter": {
        "partitions": 1,
        "retention_ms": WEEK_MS,
        "key": "source",
        "schema": "any",
        "producer": "A",
    },
}

#: Topics whose records follow the trade schema (price event with symbol/price/volume).
TRADE_TOPICS = ("crypto.trades", "stocks.trades", "gold.quotes", "fx.dxy")

ASSET_CLASSES = ("crypto", "equity", "gold", "bond", "fx", "rates")

ALERT_TYPES = ("PRICE_SPIKE", "VOLUME_SPIKE", "VOLATILITY_SPIKE")
SEVERITIES = ("low", "medium", "high")
BAR_TIMEFRAMES = ("1Min", "1Day")

SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.=_-]{2,20}$")
DAY_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class Instrument:
    """One tradable symbol and where its events are published."""

    asset_class: str
    topic: str
    #: Feeds expected to publish this symbol, in preference order.
    sources: tuple[str, ...]
    #: Starting price for the synthetic generator only.
    base_price: float


INSTRUMENTS: dict[str, Instrument] = {
    "BTCUSDT": Instrument("crypto", "crypto.trades", ("binance",), 92_400.0),
    "ETHUSDT": Instrument("crypto", "crypto.trades", ("binance",), 3_200.0),
    "AAPL": Instrument("equity", "stocks.trades", ("alpaca", "finnhub"), 230.0),
    "MSFT": Instrument("equity", "stocks.trades", ("alpaca", "finnhub"), 430.0),
    "NVDA": Instrument("equity", "stocks.trades", ("alpaca", "finnhub"), 140.0),
    "SPY": Instrument("equity", "stocks.trades", ("alpaca", "finnhub"), 590.0),
    "GLD": Instrument("gold", "stocks.trades", ("alpaca", "finnhub"), 250.0),
    "TLT": Instrument("bond", "stocks.trades", ("alpaca", "finnhub"), 92.0),
    "IEF": Instrument("bond", "stocks.trades", ("alpaca", "finnhub"), 95.0),
    "SHY": Instrument("bond", "stocks.trades", ("alpaca", "finnhub"), 82.0),
    "GC=F": Instrument("gold", "gold.quotes", ("yahoo",), 2_650.0),
    "DXY": Instrument("fx", "fx.dxy", ("yahoo",), 105.0),
}

#: Contract symbol -> Yahoo Finance ticker (Yahoo calls the dollar index DX-Y.NYB).
YAHOO_TICKERS = {"GC=F": "GC=F", "DXY": "DX-Y.NYB"}

#: FRED series id -> contract tenor. The Treasury CSV producer reuses these series ids
#: so batch jobs can treat both sources the same way.
RATE_SERIES: dict[str, str] = {
    "DGS1MO": "1M",
    "DGS3MO": "3M",
    "DGS6MO": "6M",
    "DGS1": "1Y",
    "DGS2": "2Y",
    "DGS5": "5Y",
    "DGS10": "10Y",
    "DGS30": "30Y",
    "DFII10": "REAL10Y",
}
TENORS = tuple(RATE_SERIES.values())

#: Rate tenors that enter the cross-asset correlation as pseudo-symbols.
YIELD_SYMBOLS = {"2Y": "US2Y", "10Y": "US10Y", "REAL10Y": "REAL10Y"}

REQUIRED_TRADE_FIELDS = (
    "trade_id",
    "symbol",
    "asset_class",
    "event_time",
    "price",
    "volume",
    "source",
)
REQUIRED_BAR_FIELDS = (
    "bar_id",
    "symbol",
    "asset_class",
    "timeframe",
    "event_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
)
REQUIRED_RATE_FIELDS = ("series_id", "tenor", "day", "event_time", "yield", "source")


def _hash_id(*parts: Any) -> str:
    raw = "|".join(str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _missing(record: dict[str, Any], fields: tuple[str, ...]) -> list[str]:
    return [f"missing:{name}" for name in fields if record.get(name) in (None, "")]


def _is_epoch_ms(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def validate_trade(record: dict[str, Any]) -> list[str]:
    """Return contract violations of a trade-schema record; empty means valid."""
    errors = _missing(record, REQUIRED_TRADE_FIELDS)
    if errors:
        return errors

    try:
        price = float(record["price"])
        volume = float(record["volume"])
    except (TypeError, ValueError):
        return ["price/volume must be numeric"]

    if not math.isfinite(price) or price <= 0:
        errors.append("price must be > 0")
    if not math.isfinite(volume) or volume < 0:
        errors.append("volume must be >= 0")
    if not _is_epoch_ms(record["event_time"]):
        errors.append("event_time must be epoch milliseconds")
    if not SYMBOL_PATTERN.match(str(record["symbol"])):
        errors.append(f"bad symbol: {record['symbol']}")
    # Trades never carry the "rates" class; yields use the rate_yield schema instead.
    if record["asset_class"] not in ASSET_CLASSES or record["asset_class"] == "rates":
        errors.append(f"unknown asset_class: {record['asset_class']}")
    return errors


def validate_bar(record: dict[str, Any]) -> list[str]:
    """Return contract violations of a ``stocks.bars`` record."""
    errors = _missing(record, REQUIRED_BAR_FIELDS)
    if errors:
        return errors
    try:
        open_, high, low, close, volume = (
            float(record[name]) for name in ("open", "high", "low", "close", "volume")
        )
    except (TypeError, ValueError):
        return ["ohlcv must be numeric"]
    if min(open_, high, low, close) <= 0:
        errors.append("prices must be > 0")
    if high < max(open_, close, low) or low > min(open_, close):
        errors.append("high/low do not bound open/close")
    if volume < 0:
        errors.append("volume must be >= 0")
    if record["timeframe"] not in BAR_TIMEFRAMES:
        errors.append(f"unknown timeframe: {record['timeframe']}")
    if not _is_epoch_ms(record["event_time"]):
        errors.append("event_time must be epoch milliseconds")
    return errors


def validate_rate(record: dict[str, Any]) -> list[str]:
    """Return contract violations of a ``rates.yields`` record."""
    errors = _missing(record, REQUIRED_RATE_FIELDS)
    if errors:
        return errors
    if record["tenor"] not in TENORS:
        errors.append(f"unknown tenor: {record['tenor']}")
    if RATE_SERIES.get(record["series_id"]) != record["tenor"]:
        errors.append("series_id does not match tenor")
    if not DAY_PATTERN.match(str(record["day"])):
        errors.append("day must be YYYY-MM-DD")
    if not _is_epoch_ms(record["event_time"]):
        errors.append("event_time must be epoch milliseconds")
    try:
        float(record["yield"])
    except (TypeError, ValueError):
        errors.append("yield must be numeric")
    return errors


def trade_id(record: dict[str, Any]) -> str:
    """Deterministic de-duplication key for a trade-schema record.

    When the exchange provides its own trade id (Binance ``t``, Alpaca ``i``), the key is
    built from it: two genuine trades with the same time/price/size stay distinct, while
    a re-sent trade after a reconnect collapses to the same key. Feeds without an
    exchange id (Yahoo/Finnhub quotes) fall back to the event's content.
    """
    exchange_id = record.get("exchange_trade_id")
    if exchange_id not in (None, ""):
        return _hash_id(record.get("source", ""), record.get("symbol", ""), exchange_id)
    return _hash_id(
        *(record.get(key, "") for key in ("source", "symbol", "event_time", "price", "volume"))
    )


def bar_id(record: dict[str, Any]) -> str:
    return _hash_id(
        *(record.get(key, "") for key in ("source", "symbol", "timeframe", "event_time"))
    )


def make_trade(
    symbol: str,
    asset_class: str,
    event_time: int,
    price: float,
    volume: float,
    source: str,
    *,
    exchange_trade_id: str | int | None = None,
    ingest_time: int | None = None,
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
    if exchange_trade_id not in (None, ""):
        record["exchange_trade_id"] = str(exchange_trade_id)
    if ingest_time is not None:
        record["ingest_time"] = int(ingest_time)
    record.update(extra)
    record.setdefault("trade_id", trade_id(record))
    return record


def make_bar(
    symbol: str,
    asset_class: str,
    timeframe: str,
    event_time: int,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    source: str,
    *,
    vwap: float | None = None,
    trade_count: int | None = None,
    ingest_time: int | None = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "symbol": symbol,
        "asset_class": asset_class,
        "timeframe": timeframe,
        "event_time": int(event_time),
        "open": float(open_),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "volume": float(volume),
        "source": source,
    }
    if vwap is not None:
        record["vwap"] = float(vwap)
    if trade_count is not None:
        record["trade_count"] = int(trade_count)
    if ingest_time is not None:
        record["ingest_time"] = int(ingest_time)
    record["bar_id"] = bar_id(record)
    return record


def day_to_epoch_ms(day: str) -> int:
    """Midnight UTC of ``YYYY-MM-DD`` in epoch milliseconds."""
    parsed = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def make_rate(
    series_id: str,
    day: str,
    yield_value: float,
    source: str,
    *,
    ingest_time: int | None = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "series_id": series_id,
        "tenor": RATE_SERIES[series_id],
        "day": day,
        # Yields are daily observations; the event time is the start of that UTC day so
        # bronze partitioning (dt=YYYY-MM-DD) lands on the observation date.
        "event_time": day_to_epoch_ms(day),
        "yield": float(yield_value),
        "source": source,
    }
    if ingest_time is not None:
        record["ingest_time"] = int(ingest_time)
    return record


def topic_for_symbol(symbol: str) -> str:
    instrument = INSTRUMENTS.get(symbol.upper())
    if instrument is None:
        raise KeyError(f"symbol {symbol!r} is not in the contract registry")
    return instrument.topic


def topic_for_record(record: dict[str, Any]) -> str:
    """Route a contract record to its Kafka topic.

    Precedence: record shape first (rates/bars/alerts have distinct required fields),
    then the instrument registry, then an asset-class fallback for symbols outside the
    registry (e.g. an extra crypto pair added on the command line).
    """
    if "tenor" in record and "yield" in record:
        return "rates.yields"
    if "timeframe" in record and "bar_id" in record:
        return "stocks.bars"
    if "alert_type" in record:
        return "alerts"
    symbol = str(record.get("symbol", "")).upper()
    if symbol in INSTRUMENTS:
        return INSTRUMENTS[symbol].topic
    fallback = {
        "crypto": "crypto.trades",
        "equity": "stocks.trades",
        "bond": "stocks.trades",
        "gold": "gold.quotes",
        "fx": "fx.dxy",
    }
    asset_class = str(record.get("asset_class", ""))
    if asset_class not in fallback:
        raise KeyError(f"cannot route record with asset_class={asset_class!r}")
    return fallback[asset_class]


# Deterministic synthetic market used by samples, tests, the local demo and the API's
# demo fallback. It is shaped so the batch questions from the PDF have visible answers:
# bond ETFs move against the 10Y yield, gold moves against the real 10Y yield, equities
# share a market factor, and stock/bond calendars differ from crypto's 24/7 calendar.

#: NYSE full-day closures inside the synthetic window (bond market also closed).
SYNTHETIC_STOCK_HOLIDAYS = frozenset({"2026-09-07", "2026-11-26"})
#: SIFMA bond-market-only closures: stocks trade but no Treasury yield is published.
SYNTHETIC_BOND_ONLY_HOLIDAYS = frozenset({"2026-10-12", "2026-11-11"})
SYNTHETIC_START = datetime(2026, 9, 1, 14, 30, tzinfo=timezone.utc)

# (beta to market factor, beta to 10Y yield change in % points, idiosyncratic daily vol)
_FACTOR_LOADINGS: dict[str, tuple[float, float, float]] = {
    "BTCUSDT": (1.6, -0.02, 0.025),
    "ETHUSDT": (1.9, -0.02, 0.03),
    "AAPL": (1.1, -0.01, 0.008),
    "MSFT": (1.0, -0.01, 0.007),
    "NVDA": (1.7, -0.02, 0.015),
    "SPY": (1.0, -0.005, 0.002),
    "GLD": (0.1, 0.0, 0.0025),
    "TLT": (-0.2, -0.16, 0.003),
    "IEF": (-0.1, -0.07, 0.002),
    "SHY": (0.0, -0.02, 0.0005),
    "GC=F": (0.1, 0.0, 0.0025),
    "DXY": (-0.1, 0.02, 0.002),
}
#: Gold additionally loads on the *real* yield change (negative, as in the PDF question).
_GOLD_REAL_YIELD_BETA = -0.1


def _trades_on(symbol: str, day: date) -> bool:
    iso = day.isoformat()
    if INSTRUMENTS[symbol].asset_class == "crypto":
        return True
    return day.weekday() < 5 and iso not in SYNTHETIC_STOCK_HOLIDAYS


def _bond_market_open(day: date) -> bool:
    iso = day.isoformat()
    return (
        day.weekday() < 5
        and iso not in SYNTHETIC_STOCK_HOLIDAYS
        and iso not in SYNTHETIC_BOND_ONLY_HOLIDAYS
    )


def _daily_factors(days: int, seed: int) -> list[dict[str, float]]:
    """Daily market return, 10Y yield change and real-yield change (% points)."""
    rng = random.Random(seed * 7_919 + 1)
    factors: list[dict[str, float]] = []
    for _ in range(days):
        d10 = rng.gauss(0, 0.05)
        factors.append(
            {
                "mkt": rng.gauss(0.0003, 0.009),
                "d10": d10,
                "dreal": 0.8 * d10 + rng.gauss(0, 0.02),
            }
        )
    return factors


def synthetic_yields(
    days: int = 60,
    seed: int = 42,
    start: datetime | None = None,
) -> list[dict[str, Any]]:
    """Daily Treasury curve (all contract tenors) on the bond-market calendar."""
    start = start or SYNTHETIC_START
    factors = _daily_factors(days, seed)
    rng = random.Random(seed * 31 + 5)
    # Mildly inverted short end, as seen in 2023-2025, so spread/inversion logic is used.
    levels = {
        "DGS1MO": 4.45,
        "DGS3MO": 4.40,
        "DGS6MO": 4.30,
        "DGS1": 4.15,
        "DGS2": 4.05,
        "DGS5": 4.00,
        "DGS10": 4.15,
        "DGS30": 4.45,
        "DFII10": 1.85,
    }
    # Shorter tenors react less to the long-end factor.
    sensitivity = {
        "DGS1MO": 0.1,
        "DGS3MO": 0.2,
        "DGS6MO": 0.35,
        "DGS1": 0.5,
        "DGS2": 0.7,
        "DGS5": 0.9,
        "DGS10": 1.0,
        "DGS30": 0.95,
    }
    records: list[dict[str, Any]] = []
    for offset, factor in enumerate(factors):
        current = (start + timedelta(days=offset)).date()
        # Yields only move on US business days. On bond-only holidays the level still
        # moves (stocks trade and price it in) but nothing is published, which is what
        # the batch layer's forward-fill has to cope with.
        if not _trades_on("SPY", current):
            continue
        for series_id in levels:
            if series_id == "DFII10":
                levels[series_id] += factor["dreal"]
            else:
                levels[series_id] += sensitivity[series_id] * factor["d10"] + rng.gauss(
                    0, 0.005
                )
        if not _bond_market_open(current):
            continue
        for series_id, level in levels.items():
            records.append(make_rate(series_id, current.isoformat(), round(level, 4), "demo"))
    return records


def synthetic_trades(
    rows: int = 2_400,
    seed: int = 42,
    start: datetime | None = None,
    days: int = 60,
    symbols: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    """Deterministic multi-asset trade stream, at most ``rows`` records.

    Events are spread evenly across each symbol's trading days; within a day they are
    one minute apart starting at 14:30 UTC (US open). Each day's drift follows the shared
    factors from :func:`_daily_factors`, so the same ``seed`` used with
    :func:`synthetic_yields` reproduces the cross-asset relationships.
    """
    start = start or SYNTHETIC_START
    rng = random.Random(seed)
    factors = _daily_factors(days, seed)
    chosen = symbols or tuple(INSTRUMENTS)
    calendar = {
        symbol: [
            offset
            for offset in range(days)
            if _trades_on(symbol, (start + timedelta(days=offset)).date())
        ]
        for symbol in chosen
    }
    trading_days = sum(len(offsets) for offsets in calendar.values()) or 1
    per_day = max(1, rows // trading_days)

    records: list[dict[str, Any]] = []
    for symbol in chosen:
        instrument = INSTRUMENTS[symbol]
        beta_mkt, beta_rate, idio = _FACTOR_LOADINGS[symbol]
        price = instrument.base_price
        for offset in calendar[symbol]:
            factor = factors[offset]
            daily = beta_mkt * factor["mkt"] + beta_rate * factor["d10"]
            if instrument.asset_class == "gold":
                daily += _GOLD_REAL_YIELD_BETA * factor["dreal"]
            day_start = start + timedelta(days=offset)
            for slot in range(per_day):
                shock = daily / per_day + rng.gauss(0, idio / math.sqrt(per_day))
                price = max(0.01, price * (1 + shock))
                # The dollar index has no traded volume on Yahoo; keep that realistic so
                # consumers exercise the "VWAP undefined" path.
                volume = 0.0 if symbol == "DXY" else round(rng.uniform(0.01, 25.0), 6)
                event_time = int((day_start + timedelta(minutes=slot)).timestamp() * 1000)
                records.append(
                    make_trade(
                        symbol=symbol,
                        asset_class=instrument.asset_class,
                        event_time=event_time,
                        price=round(price, 6),
                        volume=volume,
                        source="demo",
                        exchange_trade_id=f"{symbol}-{offset}-{slot}",
                    )
                )
    records.sort(key=lambda record: (record["event_time"], record["symbol"]))
    return records[:rows]
