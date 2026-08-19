from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from time import monotonic
from typing import Iterable

from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.envelope import validate_raw_envelope

try:
    from confluent_kafka import Producer
except ImportError:  # Dependency readiness must fail closed at construction.
    Producer = None


@dataclass(frozen=True)
class KafkaRawPublisherConfig:
    bootstrap_servers: str
    client_id: str
    topic: str
    ca_path: Path
    certificate_path: Path
    key_path: Path
    delivery_timeout_seconds: float = 30.0
    queue_max_messages: int = 100_000
    linger_ms: int = 5

    def validate(self) -> None:
        if not self.bootstrap_servers.strip() or not self.client_id.strip() or not self.topic.strip():
            raise ValueError("Kafka raw publisher identity/topic is required")
        if self.delivery_timeout_seconds <= 0 or self.queue_max_messages <= 0:
            raise ValueError("Kafka raw publisher bounds must be positive")
        if not 0 <= self.linger_ms <= 1000:
            raise ValueError("Kafka raw publisher linger_ms is invalid")
        for value in (self.ca_path, self.certificate_path, self.key_path):
            if not value.is_file():
                raise ValueError(f"Kafka TLS file is unavailable: {value}")


@dataclass(frozen=True)
class RawPublishAck:
    capture_id: bytes
    partition: int
    offset: int


class KafkaRawPublisher:
    """Idempotent TLS Kafka producer for Python vendor-SDK acquisition edges."""

    def __init__(self, config: KafkaRawPublisherConfig, *, producer_factory=None):
        config.validate()
        factory = producer_factory or Producer
        if factory is None:
            raise RuntimeError("confluent-kafka runtime dependency is unavailable")
        self.config = config
        self._producer = factory({
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": config.client_id,
            "security.protocol": "ssl",
            "ssl.ca.location": str(config.ca_path),
            "ssl.certificate.location": str(config.certificate_path),
            "ssl.key.location": str(config.key_path),
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "retries": 2_147_483_647,
            "compression.type": "zstd",
            "linger.ms": config.linger_ms,
            "queue.buffering.max.messages": config.queue_max_messages,
            "delivery.timeout.ms": int(config.delivery_timeout_seconds * 1000),
        })
        self._lock = Lock()
        self._closed = False

    @staticmethod
    def _key(value: raw_provider_pb2.RawProviderEnvelope) -> str:
        return "/".join((
            value.venue,
            value.market,
            value.native_symbol,
            value.native_channel,
        ))

    def publish_many(
        self, values: Iterable[raw_provider_pb2.RawProviderEnvelope]
    ) -> tuple[RawPublishAck, ...]:
        envelopes = tuple(values)
        if not envelopes:
            raise ValueError("raw publish batch must not be empty")
        for value in envelopes:
            validate_raw_envelope(value)
        with self._lock:
            if self._closed:
                raise RuntimeError("Kafka raw publisher is closed")
            pending = {bytes(value.capture_id) for value in envelopes}
            acknowledgements: list[RawPublishAck] = []
            failures: list[str] = []

            def delivered(error, message, *, capture_id):
                pending.discard(capture_id)
                if error is not None:
                    failures.append(str(error))
                    return
                acknowledgements.append(RawPublishAck(
                    capture_id=capture_id,
                    partition=int(message.partition()),
                    offset=int(message.offset()),
                ))

            deadline = monotonic() + self.config.delivery_timeout_seconds
            for value in envelopes:
                capture_id = bytes(value.capture_id)
                while True:
                    try:
                        self._producer.produce(
                            self.config.topic,
                            key=self._key(value).encode(),
                            value=value.SerializeToString(deterministic=True),
                            headers=[("qdl-event-id", capture_id)],
                            on_delivery=lambda error, message, capture_id=capture_id: delivered(
                                error, message, capture_id=capture_id
                            ),
                        )
                        break
                    except BufferError:
                        if monotonic() >= deadline:
                            raise TimeoutError("Kafka raw producer queue remained full")
                        self._producer.poll(0.05)
                self._producer.poll(0)
            remaining = max(0.0, deadline - monotonic())
            undelivered = self._producer.flush(remaining)
            if undelivered or pending or failures:
                raise RuntimeError(
                    "Kafka raw durable ACK failed "
                    f"undelivered={undelivered} pending={len(pending)} failures={failures[:3]}"
                )
            acknowledgements.sort(key=lambda item: item.capture_id)
            return tuple(acknowledgements)

    async def publish_many_async(
        self, values: Iterable[raw_provider_pb2.RawProviderEnvelope]
    ) -> tuple[RawPublishAck, ...]:
        batch = tuple(values)
        return await asyncio.to_thread(self.publish_many, batch)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            remaining = self._producer.flush(self.config.delivery_timeout_seconds)
            self._closed = True
            if remaining:
                raise RuntimeError(f"Kafka raw publisher closed with {remaining} undelivered records")
