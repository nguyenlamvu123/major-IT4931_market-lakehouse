"""Backoff, circuit breaker, dead-letter routing, de-duplication and metrics."""

from __future__ import annotations

import json
import random
import threading
from typing import Any

import pytest

from common.contracts import make_trade
from ingest.base import BaseProducer
from ingest.config import ApiSettings
from ingest.dedupe import RecentIds
from ingest.metrics import ProducerMetrics
from ingest.producers.alpaca import AlpacaProducer
from ingest.producers.binance_ws import BinanceProducer
from ingest.resilience import Backoff, CircuitBreaker, CircuitOpenError


class FakeKafka:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict[str, Any]]] = []

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.sent.append((topic, key, value))

    def flush(self, timeout: float = 10.0) -> None:
        del timeout


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _metric(producer: BaseProducer, name: str, **labels: str) -> float:
    value = producer.metrics.registry.get_sample_value(name, labels)
    return 0.0 if value is None else value


def test_backoff_doubles_until_cap_and_resets() -> None:
    backoff = Backoff(base=1.0, factor=2.0, cap=10.0, jitter=0.0)
    assert [backoff.next_delay() for _ in range(6)] == [1.0, 2.0, 4.0, 8.0, 10.0, 10.0]
    backoff.reset()
    assert backoff.next_delay() == 1.0


def test_backoff_jitter_stays_within_bounds() -> None:
    backoff = Backoff(base=4.0, factor=1.0, cap=60.0, jitter=0.25, rng=random.Random(3))
    delays = [backoff.next_delay() for _ in range(200)]
    assert min(delays) >= 3.0
    assert max(delays) <= 5.0
    assert len(set(delays)) > 1


def test_circuit_breaker_closed_open_half_open_cycle() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=30.0, clock=clock)

    def fail() -> None:
        raise TimeoutError("api down")

    for _ in range(2):
        with pytest.raises(TimeoutError):
            breaker.call(fail)
    assert breaker.state == CircuitBreaker.OPEN
    with pytest.raises(CircuitOpenError):
        breaker.call(lambda: None)

    clock.now = 30.0
    assert breaker.state == CircuitBreaker.HALF_OPEN
    with pytest.raises(TimeoutError):
        breaker.call(fail)  # failed trial re-opens immediately
    assert breaker.state == CircuitBreaker.OPEN

    clock.now = 60.0
    assert breaker.call(lambda: "ok") == "ok"
    assert breaker.state == CircuitBreaker.CLOSED
    assert breaker.failures == 0


def test_invalid_record_goes_to_dead_letter_with_envelope() -> None:
    kafka = FakeKafka()
    producer = BinanceProducer(kafka)
    bad = make_trade("BTCUSDT", "crypto", 1_794_218_403_120, 92400.0, 0.05, "binance")
    bad["price"] = -1.0
    assert producer.publish(bad) is None
    topic, key, envelope = kafka.sent[0]
    assert topic == "dead-letter"
    assert key == "binance"
    assert set(envelope) == {"source", "topic", "error", "payload", "ingest_time"}
    assert envelope["topic"] == "crypto.trades"
    assert "price must be > 0" in envelope["error"]
    assert _metric(producer, "market_producer_dead_letter_total", source="binance") == 1.0


def test_unroutable_and_malformed_payloads_are_not_dropped_silently() -> None:
    kafka = FakeKafka()
    producer = BinanceProducer(kafka)
    producer.publish({"symbol": "XYZ", "asset_class": "rates"})
    producer.handle_message(json.dumps({"e": "trade", "s": "BTCUSDT", "T": "oops"}))
    producer.handle_message("{not json")
    assert [topic for topic, _, _ in kafka.sent] == ["dead-letter"] * 3
    assert "unroutable" in kafka.sent[0][2]["error"]


def test_alpaca_malformed_bar_dead_letters_to_bars_topic() -> None:
    kafka = FakeKafka()
    producer = AlpacaProducer(kafka, ApiSettings(alpaca_key="k", alpaca_secret="s"))
    sent = producer.handle_message(json.dumps([{"T": "b", "S": "SPY", "t": "2026-11-09T15:30:00Z"}]))
    assert sent == 0
    assert kafka.sent[0][0] == "dead-letter"
    assert kafka.sent[0][2]["topic"] == "stocks.bars"


def test_reconnect_replay_is_deduplicated_in_process() -> None:
    kafka = FakeKafka()
    producer = BinanceProducer(kafka)
    frame = json.dumps({"e": "trade", "s": "BTCUSDT", "t": 1, "p": "92400", "q": "0.05",
                        "T": 1_794_218_403_120})
    assert producer.handle_message(frame) == 1
    assert producer.handle_message(frame) == 0  # same exchange trade after reconnect
    assert len(kafka.sent) == 1
    assert _metric(producer, "market_producer_duplicates_total", source="binance") == 1.0
    assert _metric(producer, "market_producer_messages_total",
                   topic="crypto.trades", source="binance") == 1.0


def test_recent_ids_is_bounded() -> None:
    ids = RecentIds(capacity=3)
    for value in "abcd":
        assert ids.seen(value) is False
    assert len(ids) == 3
    assert ids.seen("a") is False  # evicted as the oldest
    assert ids.seen("d") is True


def test_run_with_reconnect_backs_off_then_resets_after_healthy_session() -> None:
    clock = FakeClock()
    stop = threading.Event()
    waits: list[float] = []
    sessions = {"count": 0}

    async def consume(stop_event: threading.Event) -> None:
        sessions["count"] += 1
        if sessions["count"] == 3:
            clock.now += 120.0  # third session stays up two minutes before dropping
        if sessions["count"] == 5:
            stop_event.set()
            return
        raise ConnectionError("socket closed")

    def wait(delay: float) -> bool:
        waits.append(delay)
        return False

    producer = BinanceProducer(FakeKafka())
    producer.run_with_reconnect(
        consume,
        stop,
        backoff=Backoff(base=1.0, factor=2.0, cap=60.0, jitter=0.0),
        healthy_after=60.0,
        wait=wait,
        clock=clock,
    )
    # Sessions 1, 2 fail fast (1s, 2s); session 3 was healthy -> reset to 1s; then 2s.
    assert waits == [1.0, 2.0, 1.0, 2.0]
    assert _metric(producer, "market_producer_reconnects_total", source="binance") == 4.0


def test_poll_forever_stops_calling_api_while_breaker_open() -> None:
    clock = FakeClock()
    stop = threading.Event()
    calls = {"count": 0}
    loops = {"count": 0}

    def poll_once() -> int:
        calls["count"] += 1
        raise TimeoutError("api down")

    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=100.0, clock=clock)
    original_allow = breaker.allow

    def counting_allow() -> bool:
        loops["count"] += 1
        if loops["count"] == 6:
            stop.set()
        return original_allow()

    breaker.allow = counting_allow  # type: ignore[method-assign]
    producer = BinanceProducer(FakeKafka())
    producer.poll_forever(poll_once, 0.0, stop, breaker=breaker)
    # Two failures open the circuit; later loop iterations skip the API entirely.
    assert calls["count"] == 2
    assert _metric(producer, "market_producer_errors_total", source="binance", kind="poll") == 2.0


def test_throughput_rates_per_topic() -> None:
    clock = FakeClock()
    metrics = ProducerMetrics("demo", log_every=10.0, clock=clock)
    for _ in range(20):
        metrics.message("crypto.trades")
    clock.now = 4.0
    assert metrics.rates() == {"crypto.trades": 5.0}
    clock.now = 10.0
    metrics.message("crypto.trades")  # crosses log_every -> logs and opens a new window
    assert metrics.rates() == {}
