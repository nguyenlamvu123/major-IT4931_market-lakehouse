#!/usr/bin/env python3
"""CLI entry point for every market-data producer of role A.

Call stack::

    main
      -> parse_args
      -> build_producer (Kafka client or dry-run printer)
      -> build_source -> <Source>Producer
      -> <Source>Producer.run(stop_event)   # until SIGTERM/SIGINT
      -> <Source>Producer.close()           # flush in-flight records

Examples::

    python -m ingest.run_producer --source mock --rows 20 --dry-run
    python -m ingest.run_producer --source binance
    python -m ingest.run_producer --source treasury --interval 3600
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingest.config import ApiSettings, KafkaSettings  # noqa: E402
from ingest.kafka_producer import build_producer  # noqa: E402

SOURCES = ("mock", "replay", "binance", "alpaca", "finnhub", "yahoo", "fred", "treasury")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market data producer runner")
    parser.add_argument("--source", choices=SOURCES, default="mock")
    parser.add_argument("--bootstrap", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--dry-run", action="store_true", help="Print records instead of sending")
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated contract symbols; default = the source's registry symbols",
    )
    parser.add_argument("--file", default="contracts/samples/crypto.trades.jsonl")
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--days", type=int, default=5, help="mock: synthetic days")
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="Seconds between polls (REST sources) or between records (mock/replay)",
    )
    parser.add_argument("--alpaca-mode", choices=["ws", "rest"], default="ws")
    parser.add_argument("--loop", action="store_true")
    return parser.parse_args(argv)


def _interval(args: argparse.Namespace, default: float) -> float:
    return default if args.interval is None else args.interval


def build_source(args: argparse.Namespace, kafka: object) -> object:
    symbols = (
        [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
        if args.symbols
        else None
    )
    if args.source == "mock":
        from ingest.producers.mock import MockProducer

        return MockProducer(
            kafka, rows=args.rows, interval=_interval(args, 0.02), loop=args.loop, days=args.days
        )
    if args.source == "replay":
        from ingest.producers.replay import ReplayProducer

        return ReplayProducer(kafka, args.file, interval=_interval(args, 0.0), loop=args.loop)
    if args.source == "binance":
        from ingest.producers.binance_ws import BinanceProducer

        return BinanceProducer(kafka, symbols=symbols)
    if args.source == "alpaca":
        from ingest.producers.alpaca import AlpacaProducer

        return AlpacaProducer(
            kafka,
            ApiSettings.from_env(),
            symbols,
            mode=args.alpaca_mode,
            interval=_interval(args, 15.0),
        )
    if args.source == "finnhub":
        from ingest.producers.finnhub import FinnhubProducer

        return FinnhubProducer(kafka, ApiSettings.from_env(), symbols)
    if args.source == "yahoo":
        from ingest.producers.yahoo import YahooProducer

        return YahooProducer(kafka, symbols, _interval(args, 60.0))
    if args.source == "fred":
        from ingest.producers.treasury_fred import FredYieldProducer

        return FredYieldProducer(kafka, ApiSettings.from_env(), _interval(args, 3600.0))
    if args.source == "treasury":
        from ingest.producers.treasury_csv import TreasuryYieldProducer

        return TreasuryYieldProducer(kafka, _interval(args, 3600.0))
    raise ValueError(args.source)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = KafkaSettings.from_env()
    if args.bootstrap:
        settings = KafkaSettings(bootstrap=args.bootstrap, security_protocol=settings.security_protocol)
    kafka = build_producer(settings, dry_run=args.dry_run)
    source = build_source(args, kafka)
    stop_event = threading.Event()

    def stop(*_: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        source.run(stop_event)  # type: ignore[attr-defined]
    finally:
        source.close()  # type: ignore[attr-defined]


if __name__ == "__main__":
    main()
