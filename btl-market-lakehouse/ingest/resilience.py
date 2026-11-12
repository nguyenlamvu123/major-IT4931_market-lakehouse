"""Retry policies for unreliable market APIs: reconnect backoff and a circuit breaker.

Idea PDF, slide "Sự cố về dữ liệu": "API giới hạn tần suất hoặc mất kết nối -> Retry với
exponential backoff; circuit breaker". Websocket feeds use :class:`Backoff` between
reconnects; REST pollers use :class:`CircuitBreaker` so a dead API is not hammered every
interval (and rate limits are not burned while it recovers).
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar

T = TypeVar("T")


@dataclass
class Backoff:
    """Exponential backoff with bounded jitter.

    Delay for attempt ``n`` (0-based) is ``min(cap, base * factor**n)`` moved by up to
    ``±jitter`` of itself, then clamped to ``cap``. The jitter spreads reconnects of
    several pods so they do not hit the exchange in the same second after an outage.
    """

    base: float = 1.0
    factor: float = 2.0
    cap: float = 60.0
    jitter: float = 0.2
    rng: random.Random = field(default_factory=random.Random)
    attempt: int = 0

    def next_delay(self) -> float:
        raw = min(self.cap, self.base * self.factor**self.attempt)
        self.attempt += 1
        spread = raw * self.jitter
        return max(0.0, min(self.cap, raw + self.rng.uniform(-spread, spread)))

    def reset(self) -> None:
        self.attempt = 0


class CircuitOpenError(RuntimeError):
    """Raised by :meth:`CircuitBreaker.call` while the circuit is open."""


class CircuitBreaker:
    """Three-state breaker around one external API.

    State model:

    - ``closed``: calls pass; ``failure_threshold`` consecutive failures open the circuit.
    - ``open``: calls are refused until ``reset_timeout`` seconds have passed.
    - ``half_open``: the next call is a trial; success closes the circuit, failure
      re-opens it for another ``reset_timeout``.

    ``clock`` is injectable so tests can move time without sleeping.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        failure_threshold: int = 3,
        reset_timeout: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.clock = clock
        self.failures = 0
        self._state = self.CLOSED
        self._opened_at = 0.0

    @property
    def state(self) -> str:
        # The open -> half_open transition is time-based, so it is evaluated lazily.
        if self._state == self.OPEN and self.clock() - self._opened_at >= self.reset_timeout:
            self._state = self.HALF_OPEN
        return self._state

    def allow(self) -> bool:
        return self.state != self.OPEN

    def record_success(self) -> None:
        self.failures = 0
        self._state = self.CLOSED

    def record_failure(self) -> None:
        self.failures += 1
        if self.state == self.HALF_OPEN or self.failures >= self.failure_threshold:
            self._state = self.OPEN
            self._opened_at = self.clock()

    def call(self, function: Callable[[], T]) -> T:
        if not self.allow():
            raise CircuitOpenError("circuit open")
        try:
            result = function()
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result
