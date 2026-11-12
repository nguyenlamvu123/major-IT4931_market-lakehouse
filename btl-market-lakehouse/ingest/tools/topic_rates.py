#!/usr/bin/env python3
"""Messages per second of each Kafka topic, measured from partition high watermarks.

PDF A task 6: "Đo số tin nhắn/giây ... mỗi topic". Producer-side counters
(``market_producer_messages_total``) show what each pod sent; this tool measures what
the broker actually appended, which is the number the report needs.

Call stack::

    main
      -> watermarks(consumer, topics)      # sample 1: sum of high offsets per topic
      -> sleep(interval)
      -> watermarks(consumer, topics)      # sample 2
      -> rates_between(before, after, elapsed)

Example::

    python -m ingest.tools.topic_rates --bootstrap localhost:9092 --interval 60 --samples 5
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.contracts import TOPICS  # noqa: E402


def rates_between(
    before: dict[str, int],
    after: dict[str, int],
    seconds: float,
) -> dict[str, float]:
    """Per-topic append rate between two high-watermark samples.

    High watermarks only grow, except when a topic is deleted and recreated between the
    samples; a negative delta is reported as 0 rather than a negative rate.
    """
    if seconds <= 0:
        raise ValueError("seconds must be > 0")
    return {
        topic: max(0, after.get(topic, 0) - before.get(topic, 0)) / seconds
        for topic in sorted(set(before) | set(after))
    }


def watermarks(consumer: Any, topics: list[str], timeout: float = 5.0) -> dict[str, int]:
    """Sum of the high watermark over all partitions of each topic."""
    from confluent_kafka import TopicPartition

    metadata = consumer.list_topics(timeout=timeout)
    totals: dict[str, int] = {}
    for topic in topics:
        if topic not in metadata.topics:
            continue
        total = 0
        for partition in metadata.topics[topic].partitions:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=timeout, cached=False
            )
            total += max(high, 0)
        totals[topic] = total
    return totals


def markdown_row(rates: dict[str, float], interval: float) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cells = " | ".join(f"{topic}={rate:.2f}" for topic, rate in rates.items())
    return f"| {stamp} | {interval:.0f}s | {cells} |"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topics", default=",".join(t for t in TOPICS if t != "dead-letter"))
    parser.add_argument("--interval", type=float, default=60.0)
    parser.add_argument("--samples", type=int, default=1)
    args = parser.parse_args(argv)

    from confluent_kafka import Consumer

    # A consumer is only used for metadata/watermark calls; it never subscribes, so the
    # group id does not create offsets or lag.
    consumer = Consumer({"bootstrap.servers": args.bootstrap, "group.id": "topic-rates-probe",
                         "enable.auto.commit": False})
    topics = [topic for topic in args.topics.split(",") if topic]
    try:
        previous = watermarks(consumer, topics)
        started = time.monotonic()
        for _ in range(args.samples):
            time.sleep(args.interval)
            current = watermarks(consumer, topics)
            now = time.monotonic()
            print(markdown_row(rates_between(previous, current, now - started), args.interval))
            previous, started = current, now
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
