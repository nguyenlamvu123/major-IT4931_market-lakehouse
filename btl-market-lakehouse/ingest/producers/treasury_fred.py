"""US Treasury yield curve poller via FRED."""

from __future__ import annotations

import threading
from typing import Any

import requests

from ..base import BaseProducer
from ..config import ApiSettings

FRED_SERIES = {"DGS2": "2Y", "DGS5": "5Y", "DGS10": "10Y", "DGS30": "30Y"}


def parse_fred_observations(
    payload: dict[str, Any], tenor: str
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for item in payload.get("observations", []):
        value = item.get("value")
        if value in (None, "."):
            continue
        try:
            output.append(
                {
                    "day": item["date"],
                    "tenor": tenor,
                    "yield": float(value),
                    "source": "fred",
                }
            )
        except (KeyError, TypeError, ValueError):
            continue
    return output


class FredYieldProducer(BaseProducer):
    def __init__(
        self,
        kafka: Any,
        settings: ApiSettings,
        interval: float = 3600.0,
    ) -> None:
        super().__init__(kafka)
        if not settings.fred_key:
            raise RuntimeError("FRED_API_KEY is required")
        self.settings = settings
        self.interval = interval

    def run(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            for series_id, tenor in FRED_SERIES.items():
                response = requests.get(
                    "https://api.stlouisfed.org/fred/series/observations",
                    params={
                        "series_id": series_id,
                        "api_key": self.settings.fred_key,
                        "file_type": "json",
                        "sort_order": "desc",
                        "limit": 5,
                    },
                    timeout=10,
                )
                if not response.ok:
                    continue
                for record in parse_fred_observations(response.json(), tenor):
                    self.kafka.send("rates.ticks", tenor, record)
            stop_event.wait(self.interval)
