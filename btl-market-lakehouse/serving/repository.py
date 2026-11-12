"""Read-side contract shared by the Cassandra repository and the demo repository.

Both return plain JSON-able dicts whose keys are exactly the contract columns listed in
``serving.queries``; the API never has to know which backend answered, except for the
``X-Data-Source`` header it sets from :attr:`MarketRepository.source`.
"""

from __future__ import annotations

from typing import Any, Protocol

Row = dict[str, Any]


class RepositoryUnavailable(RuntimeError):
    """The backend cannot answer right now (connection lost, timeout, not started)."""


class MarketRepository(Protocol):
    #: "cassandra" or "demo"; surfaced to clients in the X-Data-Source header.
    source: str

    def default_day(self) -> str:
        """Day used when a request omits ``day`` (YYYY-MM-DD)."""

    def ohlcv(self, symbol: str, day: str, interval: str, limit: int) -> list[Row]:
        """Bars of one (symbol, day) partition, newest first."""

    def indicators(self, symbol: str | None) -> list[Row]:
        """Latest indicator snapshot, for one symbol or all symbols."""

    def alerts(self, day: str, limit: int) -> list[Row]:
        """Alerts of one day, newest first."""

    def daily(self, symbol: str, limit: int) -> list[Row]:
        """Daily summary rows of one symbol, newest first."""

    def correlation(self, window_days: int) -> list[Row]:
        """Pairs of the most recent ``as_of`` for this window (empty if never computed)."""

    def yield_curve(self, limit: int) -> list[Row]:
        """Treasury curve rows, newest day first."""

    def clusters(self) -> list[Row]:
        """Asset clusters of the most recent model run."""
