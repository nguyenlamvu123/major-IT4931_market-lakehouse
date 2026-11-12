"""US Treasury yields from FRED (needs ``FRED_API_KEY``) -> ``rates.yields``.

Publishes all nine contract series (``common.contracts.RATE_SERIES``): the nominal curve
1M..30Y plus ``DFII10``, the 10-year real (TIPS) yield used by the "gold vs real yield"
question. Each record is keyed by tenor.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

from common.contracts import RATE_SERIES, make_rate

from ..base import BaseProducer, KafkaSink, now_ms
from ..config import ApiSettings

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"


def parse_fred_observations(
    payload: dict[str, Any],
    series_id: str,
    ingest_time: int | None = None,
    on_reject: Callable[[Any, str], None] | None = None,
) -> list[dict[str, Any]]:
    """FRED observations -> contract rate records; ``"."`` (holiday/no data) is skipped."""
    ingest_time = ingest_time if ingest_time is not None else now_ms()
    output: list[dict[str, Any]] = []
    for item in payload.get("observations", []):
        value = item.get("value")
        if value in (None, "", "."):
            continue
        try:
            output.append(make_rate(series_id, item["date"], float(value), "fred",
                                    ingest_time=ingest_time))
        except (KeyError, TypeError, ValueError) as exc:
            if on_reject is not None:
                on_reject({"series_id": series_id, **item}, f"fred observation: {exc!r}")
    return output


class RatesPublisher(BaseProducer):
    """Shared "publish only days not sent yet" policy for daily yield sources.

    Yields are published once per (series, day). The first poll after start re-sends a
    lookback window so a restarted pod fills gaps; the batch layer de-duplicates by
    (series_id, day, source), so re-sent days are harmless.
    """

    def __init__(self, kafka: KafkaSink, interval: float, lookback_days: int) -> None:
        super().__init__(kafka)
        self.interval = interval
        self.lookback_days = lookback_days
        #: series_id -> newest day (YYYY-MM-DD) already published.
        self.last_day: dict[str, str] = {}

    def publish_new(self, records: list[dict[str, Any]]) -> int:
        sent = 0
        for record in sorted(records, key=lambda item: item["day"]):
            if record["day"] <= self.last_day.get(record["series_id"], ""):
                continue
            if self.publish(record):
                sent += 1
            self.last_day[record["series_id"]] = record["day"]
        return sent

    def reject(self, payload: Any, error: str) -> None:
        self.dead_letter(payload, error, "rates.yields")

    def poll_once(self) -> int:
        raise NotImplementedError

    def run(self, stop_event: threading.Event) -> None:
        self.poll_forever(self.poll_once, self.interval, stop_event)


class FredYieldProducer(RatesPublisher):
    source = "fred"

    def __init__(
        self,
        kafka: KafkaSink,
        settings: ApiSettings,
        interval: float = 3600.0,
        lookback_days: int = 10,
        http_get: Any = None,
    ) -> None:
        super().__init__(kafka, interval, lookback_days)
        if not settings.fred_key:
            raise RuntimeError("FRED_API_KEY is required (or use --source treasury)")
        if http_get is None:
            import requests

            http_get = requests.get
        self.settings = settings
        self.http_get = http_get

    def poll_once(self) -> int:
        start = (date.today() - timedelta(days=self.lookback_days)).isoformat()
        records: list[dict[str, Any]] = []
        for series_id in RATE_SERIES:
            response = self.http_get(
                FRED_URL,
                params={
                    "series_id": series_id,
                    "api_key": self.settings.fred_key,
                    "file_type": "json",
                    "observation_start": start,
                },
                timeout=10,
            )
            response.raise_for_status()
            records.extend(
                parse_fred_observations(response.json(), series_id, on_reject=self.reject)
            )
        return self.publish_new(records)
