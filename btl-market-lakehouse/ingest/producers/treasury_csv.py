"""US Treasury daily yield curve CSV (no API key) -> ``rates.yields``.

home.treasury.gov publishes one CSV per month for each curve type:

- ``daily_treasury_yield_curve``: nominal par yields, columns ``Date, 1 Mo, 1.5 Month,
  2 Mo, 3 Mo, 4 Mo, 6 Mo, 1 Yr, 2 Yr, 3 Yr, 5 Yr, 7 Yr, 10 Yr, 20 Yr, 30 Yr``
- ``daily_treasury_real_yield_curve``: TIPS real yields, columns
  ``Date, 5 YR, 7 YR, 10 YR, 20 YR, 30 YR``

Only the contract tenors are kept. Records reuse the FRED series ids (``DGS10``,
``DFII10`` ...) with ``source="treasury"`` so the batch layer treats both sources alike;
FRED's ``DFII10`` is itself Treasury's 10-year real yield.
"""

from __future__ import annotations

import csv
import io
from datetime import date, datetime
from typing import Any

from common.contracts import make_rate

from ..base import KafkaSink, now_ms
from .treasury_fred import RatesPublisher

CSV_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "daily-treasury-rates.csv/all/{month}"
)
NOMINAL_TYPE = "daily_treasury_yield_curve"
REAL_TYPE = "daily_treasury_real_yield_curve"

#: Normalised (stripped, upper-case) CSV header -> contract series id.
NOMINAL_COLUMNS = {
    "1 MO": "DGS1MO",
    "3 MO": "DGS3MO",
    "6 MO": "DGS6MO",
    "1 YR": "DGS1",
    "2 YR": "DGS2",
    "5 YR": "DGS5",
    "10 YR": "DGS10",
    "30 YR": "DGS30",
}
REAL_COLUMNS = {"10 YR": "DFII10"}


def parse_treasury_csv(
    text: str,
    columns: dict[str, str],
    ingest_time: int | None = None,
) -> list[dict[str, Any]]:
    """Treasury CSV text -> contract rate records for the mapped columns.

    Dates come as ``MM/DD/YYYY``; empty cells (tenor not quoted that day) are skipped.
    """
    ingest_time = ingest_time if ingest_time is not None else now_ms()
    reader = csv.DictReader(io.StringIO(text.lstrip("﻿")))
    records: list[dict[str, Any]] = []
    for row in reader:
        normalised = {str(key).strip().upper(): value for key, value in row.items() if key}
        raw_date = normalised.get("DATE", "")
        try:
            day = datetime.strptime(raw_date.strip(), "%m/%d/%Y").date().isoformat()
        except ValueError:
            continue
        for column, series_id in columns.items():
            value = (normalised.get(column) or "").strip()
            if not value:
                continue
            try:
                records.append(make_rate(series_id, day, float(value), "treasury",
                                         ingest_time=ingest_time))
            except ValueError:
                continue
    return records


def months_to_fetch(today: date, lookback_days: int) -> list[str]:
    """``YYYYMM`` strings covering ``[today - lookback_days, today]``, oldest first."""
    first = date.fromordinal(today.toordinal() - lookback_days)
    months: list[str] = []
    year, month = first.year, first.month
    while (year, month) <= (today.year, today.month):
        months.append(f"{year:04d}{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


class TreasuryYieldProducer(RatesPublisher):
    source = "treasury"

    def __init__(
        self,
        kafka: KafkaSink,
        interval: float = 3600.0,
        lookback_days: int = 10,
        http_get: Any = None,
    ) -> None:
        super().__init__(kafka, interval, lookback_days)
        if http_get is None:
            import requests

            http_get = requests.get
        self.http_get = http_get

    def _download(self, month: str, curve_type: str) -> str:
        response = self.http_get(
            CSV_URL.format(month=month),
            params={
                "type": curve_type,
                "field_tdr_date_value_month": month,
                "page": "",
                "_format": "csv",
            },
            timeout=20,
        )
        response.raise_for_status()
        return response.text

    def poll_once(self) -> int:
        records: list[dict[str, Any]] = []
        for month in months_to_fetch(date.today(), self.lookback_days):
            records.extend(parse_treasury_csv(self._download(month, NOMINAL_TYPE),
                                              NOMINAL_COLUMNS))
            records.extend(parse_treasury_csv(self._download(month, REAL_TYPE), REAL_COLUMNS))
        return self.publish_new(records)
