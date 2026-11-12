"""Yahoo Finance poller: gold futures and the dollar index -> ``gold.quotes``, ``fx.dxy``.

Yahoo has no streaming API; every ``interval`` seconds the poller downloads today's
1-minute candles and publishes the newest close of each ticker as a quote. The last
candle is still forming, so consecutive polls may publish the same minute with an
updated price; the content-hash ``trade_id`` differs, which is correct for quotes.

Contract symbols are mapped to Yahoo tickers by ``common.contracts.YAHOO_TICKERS``
(``DXY`` is ``DX-Y.NYB`` on Yahoo). The dollar index has no volume, so its quotes carry
``volume = 0``.
"""

from __future__ import annotations

import logging
import math
import threading
from typing import Any

from common.contracts import INSTRUMENTS, YAHOO_TICKERS, make_trade

from ..base import BaseProducer, KafkaSink, now_ms

LOGGER = logging.getLogger(__name__)

DEFAULT_SYMBOLS = ("GC=F", "DXY")


def yahoo_ticker(symbol: str) -> str:
    return YAHOO_TICKERS.get(symbol, symbol)


def _ticker_frame(frame: Any, ticker: str, single: bool) -> Any:
    """Columns of one ticker from ``yf.download`` output.

    yfinance returns ``(ticker, field)`` MultiIndex columns when ``group_by="ticker"``
    (and, since 0.2.48, even for a single ticker); older versions return flat columns
    for one ticker. Both shapes are accepted.
    """
    columns = frame.columns
    if getattr(columns, "nlevels", 1) > 1:
        if ticker in columns.get_level_values(0):
            return frame[ticker]
        return frame.xs(ticker, axis=1, level=1)
    if single:
        return frame
    raise KeyError(ticker)


def latest_quotes(
    frame: Any,
    symbols: list[str],
    ingest_time: int | None = None,
) -> list[dict[str, Any]]:
    """Newest complete row per symbol of a yfinance 1-minute frame -> contract trades."""
    ingest_time = ingest_time if ingest_time is not None else now_ms()
    records: list[dict[str, Any]] = []
    if frame is None or len(frame) == 0:
        return records
    for symbol in symbols:
        try:
            rows = _ticker_frame(frame, yahoo_ticker(symbol), single=len(symbols) == 1)
            rows = rows.dropna(subset=["Close"])
            if len(rows) == 0:
                continue
            row = rows.iloc[-1]
            volume = row.get("Volume", 0.0)
            volume = 0.0 if volume is None or math.isnan(float(volume)) else float(volume)
            instrument = INSTRUMENTS.get(symbol)
            records.append(
                make_trade(
                    symbol=symbol,
                    asset_class=instrument.asset_class if instrument else "equity",
                    event_time=int(row.name.timestamp() * 1000),
                    price=float(row["Close"]),
                    volume=volume,
                    source="yahoo",
                    ingest_time=ingest_time,
                    metadata={"ticker": yahoo_ticker(symbol)},
                )
            )
        except (KeyError, IndexError, TypeError, ValueError):
            LOGGER.warning("no usable Yahoo row for %s", symbol)
    return records


class YahooProducer(BaseProducer):
    source = "yahoo"

    def __init__(
        self,
        kafka: KafkaSink,
        symbols: list[str] | None = None,
        interval: float = 60.0,
    ) -> None:
        super().__init__(kafka)
        self.symbols = [symbol.upper() for symbol in (symbols or DEFAULT_SYMBOLS)]
        self.interval = interval

    def download(self) -> Any:
        import yfinance as yf

        return yf.download(
            tickers=[yahoo_ticker(symbol) for symbol in self.symbols],
            period="1d",
            interval="1m",
            progress=False,
            auto_adjust=False,
            group_by="ticker",
            threads=False,
        )

    def poll_once(self) -> int:
        records = latest_quotes(self.download(), self.symbols)
        return sum(1 for record in records if self.publish(record))

    def run(self, stop_event: threading.Event) -> None:
        self.poll_forever(self.poll_once, self.interval, stop_event)
