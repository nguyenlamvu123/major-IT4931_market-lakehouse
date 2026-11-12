"""Pure reshaping of repository rows into what Grafana panels consume.

The repositories return table-shaped rows (newest first, the clustering order of the
Cassandra tables). Panels want ascending time, wide frames for multi-series charts and a
full matrix for the correlation table; doing that here keeps the queries simple and the
behaviour identical for Cassandra and demo data.
"""

from __future__ import annotations

from typing import Any

from .repository import Row

#: Maturity of each yield_curve column in years, for the curve-shape chart.
CURVE_TENORS = (
    ("1M", "y_1m", 1 / 12),
    ("3M", "y_3m", 0.25),
    ("6M", "y_6m", 0.5),
    ("1Y", "y_1y", 1.0),
    ("2Y", "y_2y", 2.0),
    ("5Y", "y_5y", 5.0),
    ("10Y", "y_10y", 10.0),
    ("30Y", "y_30y", 30.0),
)


def ascending(rows: list[Row], key: str) -> list[Row]:
    return sorted(rows, key=lambda row: row[key])


def normalized_compare(series: dict[str, list[Row]]) -> list[Row]:
    """Daily closes of several symbols rebased to 100 on each symbol's first day.

    Before (per symbol, any order):
    - SPY: [{"day": "2026-11-02", "close": 590}, {"day": "2026-11-03", "close": 595.9}]
    - GLD: [{"day": "2026-11-03", "close": 250}]

    After (one wide row per day, missing values stay null):
    - [{"day": "2026-11-02", "SPY": 100.0, "GLD": None},
       {"day": "2026-11-03", "SPY": 101.0, "GLD": 100.0}]
    """
    rebased: dict[str, dict[str, float]] = {}
    for symbol, rows in series.items():
        ordered = [row for row in ascending(rows, "day") if row.get("close")]
        if not ordered:
            continue
        base = float(ordered[0]["close"])
        rebased[symbol] = {
            str(row["day"]): round(float(row["close"]) / base * 100, 4) for row in ordered
        }
    days = sorted({day for values in rebased.values() for day in values})
    return [
        {"day": day, **{symbol: rebased[symbol].get(day) for symbol in series if symbol in rebased}}
        for day in days
    ]


def correlation_matrix(rows: list[Row]) -> list[Row]:
    """Pair rows (base < quote) -> full symmetric matrix, one row per symbol.

    Before:
    - [{"base_symbol": "GLD", "quote_symbol": "US10Y", "correlation": -0.4}]

    After:
    - [{"symbol": "GLD", "GLD": 1.0, "US10Y": -0.4},
       {"symbol": "US10Y", "GLD": -0.4, "US10Y": 1.0}]
    """
    values: dict[tuple[str, str], Any] = {}
    symbols: set[str] = set()
    for row in rows:
        base, quote = row["base_symbol"], row["quote_symbol"]
        symbols.update((base, quote))
        values[(base, quote)] = values[(quote, base)] = row["correlation"]
    ordered = sorted(symbols)
    return [
        {
            "symbol": base,
            **{quote: 1.0 if base == quote else values.get((base, quote)) for quote in ordered},
        }
        for base in ordered
    ]


def curve_points(row: Row | None) -> list[Row]:
    """One yield_curve row -> points (tenor, years, yield) for the curve-shape chart."""
    if not row:
        return []
    return [
        {"day": row["day"], "tenor": tenor, "years": years, "yield": row.get(column)}
        for tenor, column, years in CURVE_TENORS
        if row.get(column) is not None
    ]
