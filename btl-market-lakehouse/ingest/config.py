"""Environment-backed settings for producers."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"

    @classmethod
    def from_env(cls) -> KafkaSettings:
        return cls(
            bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        )

    def producer_config(self) -> dict[str, object]:
        return {
            "bootstrap.servers": self.bootstrap,
            "security.protocol": self.security_protocol,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
            "linger.ms": 20,
        }


@dataclass(frozen=True)
class ApiSettings:
    alpaca_key: str = ""
    alpaca_secret: str = ""
    finnhub_key: str = ""
    fred_key: str = ""

    @classmethod
    def from_env(cls) -> ApiSettings:
        return cls(
            alpaca_key=os.getenv("ALPACA_API_KEY", ""),
            alpaca_secret=os.getenv("ALPACA_SECRET_KEY", ""),
            finnhub_key=os.getenv("FINNHUB_API_KEY", ""),
            fred_key=os.getenv("FRED_API_KEY", ""),
        )
