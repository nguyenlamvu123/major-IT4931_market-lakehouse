#!/usr/bin/env python3
"""CLI entry point for all market-data producers."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ingest.config import ApiSettings, KafkaSettings  # noqa: E402
from ingest.kafka_producer import build_producer  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market data producer runner")
    parser.add_argument(
        "--source",
        choices=["mock", "replay", "binance", "alpaca", "finnhub", "yahoo", "fred"],
        default="mock",
    )
    parser.add_argument("--bootstrap", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--dry-run", action="store_true", help="Print records instead of sending")
    parser.add_argument("--symbols", default="BTCUSDT,AAPL,GLD", help="Comma-separated symbols")
    parser.add_argument("--file", default="contracts/samples/crypto.trades.jsonl")
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--interval", type=float, default=0.05)
    parser.add_argument("--loop", action="store_true")
    return parser.parse_args()


def build_source(args: argparse.Namespace, kafka: object) -> object:
    symbols = [item.strip() for item in args.symbols.split(",") if item.strip()]
    if args.source == "mock":
        from ingest.producers.mock import MockProducer

        return MockProducer(kafka, rows=args.rows, interval=args.interval, loop=args.loop)
    if args.source == "replay":
        from ingest.producers.replay import ReplayProducer

        return ReplayProducer(kafka, args.file, interval=args.interval, loop=args.loop)
    if args.source == "binance":
        from ingest.producers.binance_ws import BinanceProducer

        return BinanceProducer(kafka, symbols=symbols)
    if args.source == "alpaca":
        from ingest.producers.alpaca import AlpacaProducer

        return AlpacaProducer(kafka, ApiSettings.from_env(), symbols, args.interval)
    if args.source == "finnhub":
        from ingest.producers.finnhub import FinnhubProducer

        return FinnhubProducer(kafka, ApiSettings.from_env(), symbols, args.interval)
    if args.source == "yahoo":
        from ingest.producers.yahoo import YahooProducer

        return YahooProducer(kafka, symbols, args.interval)
    if args.source == "fred":
        from ingest.producers.treasury_fred import FredYieldProducer

        return FredYieldProducer(kafka, ApiSettings.from_env(), args.interval)
    raise ValueError(args.source)


def main() -> None:
    args = parse_args()
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
