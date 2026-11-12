#!/usr/bin/env python3
"""Backfill years of daily OHLCV from Yahoo Finance into ``stocks.bars`` (``1Day``).

Why: the 90-day cross-asset correlation (PDF journey step 4) and the 60-day stock/bond
correlation (idea PDF) need months of daily closes, while live trades only start
accumulating when the producers are deployed. Contract v2 therefore lets
``stocks.bars`` carry ``timeframe="1Day"`` bars for every registry symbol
(``contracts/topics.yaml`` -> ``backfill_symbols``).

Run once (or as a Kubernetes Job) before the first nightly batch::

    python -m ingest.backfill_yfinance --years 5 --dry-run
    python -m ingest.backfill_yfinance --start 2021-01-01 --symbols SPY,TLT,GC=F

Re-running is safe: ``bar_id`` is deterministic per (source, symbol, timeframe, day),
so the batch layer keeps one row per day.
"""

from __future__ import annotations

import logging
import math
import sys
import threading
from datetime import date, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.contracts import INSTRUMENTS, YAHOO_TICKERS, day_to_epoch_ms, make_bar  # noqa: E402
from ingest.base import BaseProducer, KafkaSink, now_ms  # noqa: E402

LOGGER = logging.getLogger(__name__)

#: Contract symbol -> Yahoo ticker for history. Binance pairs map to Yahoo's USD pairs
#: (USDT trades within a fraction of a percent of USD, fine for daily correlations).
BACKFILL_TICKERS = {"BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD", **YAHOO_TICKERS}


def backfill_ticker(symbol: str) -> str:
    return BACKFILL_TICKERS.get(symbol, symbol)


def history_to_bars(
    frame: Any,
    symbol: str,
    ingest_time: int | None = None,
) -> list[dict[str, Any]]:
    """yfinance daily history -> contract ``1Day`` bars.

    yfinance indexes daily rows at local midnight of the exchange (America/New_York for
    US listings, UTC for crypto). The calendar date of that local timestamp is the
    trading day, and the bar's ``event_time`` is midnight UTC of that date, so it lands
    in ``bronze/stocks.bars/dt=<trading day>/``. Rows without a full OHLC are skipped.
    """
    ingest_time = ingest_time if ingest_time is not None else now_ms()
    instrument = INSTRUMENTS.get(symbol)
    asset_class = instrument.asset_class if instrument else "equity"
    bars: list[dict[str, Any]] = []
    skipped = 0
    for timestamp, row in frame.iterrows():
        values = [row.get(name) for name in ("Open", "High", "Low", "Close")]
        if any(value is None or math.isnan(float(value)) for value in values):
            skipped += 1
            continue
        open_, high, low, close = (float(value) for value in values)
        volume = row.get("Volume", 0.0)
        volume = 0.0 if volume is None or math.isnan(float(volume)) else float(volume)
        day = timestamp.date().isoformat()
        bars.append(
            make_bar(
                symbol=symbol,
                asset_class=asset_class,
                timeframe="1Day",
                event_time=day_to_epoch_ms(day),
                open_=open_,
                high=high,
                low=low,
                close=close,
                volume=volume,
                source="yahoo",
                ingest_time=ingest_time,
            )
        )
    if skipped:
        LOGGER.info("%s: skipped %d incomplete daily rows", symbol, skipped)
    return bars


class BackfillProducer(BaseProducer):
    source = "yahoo"

    def __init__(
        self,
        kafka: KafkaSink,
        symbols: list[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        years: int = 5,
    ) -> None:
        super().__init__(kafka)
        self.symbols = [symbol.upper() for symbol in (symbols or INSTRUMENTS)]
        self.end = end or date.today()
        self.start = start or self.end - timedelta(days=365 * years)

    def history(self, symbol: str) -> Any:
        import yfinance as yf

        return yf.Ticker(backfill_ticker(symbol)).history(
            start=self.start.isoformat(),
            end=self.end.isoformat(),
            interval="1d",
            auto_adjust=False,
            actions=False,
        )

    def run(self, stop_event: threading.Event) -> None:
        for symbol in self.symbols:
            if stop_event.is_set():
                return
            bars = history_to_bars(self.history(symbol), symbol)
            sent = sum(1 for bar in bars if self.publish(bar))
            LOGGER.info("backfill %s (%s): %d daily bars %s..%s", symbol,
                        backfill_ticker(symbol), sent, self.start, self.end)


def main(argv: list[str] | None = None) -> None:
    from ingest.run_producer import main as run_main

    run_main(["--source", "backfill", *(sys.argv[1:] if argv is None else argv)])


if __name__ == "__main__":
    main()
