from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

try:
    from confluent_kafka import Consumer, KafkaException, TopicPartition
except ImportError:  # Construction fails closed when the production dependency is absent.
    Consumer = None
    KafkaException = RuntimeError
    TopicPartition = None


_EVENT_ID_HEADER = "qdl-event-id"


@dataclass(frozen=True, slots=True)
class KafkaProjectorRecord:
    topic: str
    partition: int
    offset: int
    key: str
    event_id: bytes
    payload: bytes
    accepted_at_ns: int
    assignment_epoch: int = 1

    def __post_init__(self) -> None:
        if (
            not self.topic
            or self.partition < 0
            or self.offset < 0
            or not self.key
            or len(self.event_id) not in {16, 32}
            or not self.payload
            or self.accepted_at_ns <= 0
            or self.assignment_epoch < 1
        ):
            raise ValueError("Kafka projector record is invalid")


class ProjectorBroker(Protocol):
    def poll(self, timeout_seconds: float) -> KafkaProjectorRecord | None: ...
    def checkpoint(self, record: KafkaProjectorRecord) -> None: ...
    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class KafkaProjectorConfig:
    bootstrap_servers: str
    client_id: str
    group_id: str
    raw_topics: tuple[str, ...]
    canonical_topic: str
    ca_path: Path
    certificate_path: Path
    key_path: Path
    session_timeout_ms: int = 45_000
    max_poll_interval_ms: int = 300_000

    def validate(self) -> None:
        if not all((
            self.bootstrap_servers.strip(), self.client_id.strip(), self.group_id.strip(),
            self.canonical_topic.strip(),
        )) or not self.raw_topics:
            raise ValueError("Kafka stable projector identity/topics are required")
        topics = (*self.raw_topics, self.canonical_topic)
        if any(not value.strip() for value in topics) or len(topics) != len(set(topics)):
            raise ValueError("Kafka stable projector topics must be non-empty and unique")
        if not 6_000 <= self.session_timeout_ms < self.max_poll_interval_ms:
            raise ValueError("Kafka stable projector timeout policy is invalid")
        for value in (self.ca_path, self.certificate_path, self.key_path):
            if not value.is_file():
                raise ValueError(f"Kafka stable projector TLS file is unavailable: {value}")


class ConfluentProjectorBroker:
    """Read-committed, manual-checkpoint Kafka source for the stable projector."""

    def __init__(self, config: KafkaProjectorConfig, *, consumer_factory=None) -> None:
        config.validate()
        factory = consumer_factory or Consumer
        if factory is None:
            raise RuntimeError("confluent-kafka runtime dependency is unavailable")
        self.config = config
        self._assignment_epoch = 1
        self._closed = False
        self._consumer = factory({
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": config.client_id,
            "group.id": config.group_id,
            "security.protocol": "ssl",
            "ssl.ca.location": str(config.ca_path),
            "ssl.certificate.location": str(config.certificate_path),
            "ssl.key.location": str(config.key_path),
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            "session.timeout.ms": config.session_timeout_ms,
            "max.poll.interval.ms": config.max_poll_interval_ms,
        })
        self._consumer.subscribe(
            [*config.raw_topics, config.canonical_topic],
            on_assign=self._on_assignment,
            on_revoke=self._on_assignment,
            on_lost=self._on_assignment,
        )

    def _on_assignment(self, _consumer, _partitions) -> None:
        self._assignment_epoch += 1

    def poll(self, timeout_seconds: float) -> KafkaProjectorRecord | None:
        if self._closed:
            raise RuntimeError("Kafka stable projector consumer is closed")
        if timeout_seconds <= 0:
            raise ValueError("Kafka poll timeout must be positive")
        message = self._consumer.poll(timeout_seconds)
        if message is None:
            return None
        if message.error():
            raise KafkaException(message.error())
        headers = dict(message.headers() or ())
        event_id = headers.get(_EVENT_ID_HEADER)
        key = message.key()
        payload = message.value()
        timestamp_ms = message.timestamp()[1]
        if event_id is None or key is None or payload is None or timestamp_ms is None:
            raise ValueError("Kafka stable projector record is missing required metadata")
        try:
            decoded_key = bytes(key).decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("Kafka stable projector partition key is not UTF-8") from error
        return KafkaProjectorRecord(
            topic=message.topic(),
            partition=int(message.partition()),
            offset=int(message.offset()),
            key=decoded_key,
            event_id=bytes(event_id),
            payload=bytes(payload),
            accepted_at_ns=max(1, int(timestamp_ms) * 1_000_000),
            assignment_epoch=self._assignment_epoch,
        )

    def ping(self, timeout_seconds: float = 1.0) -> bool:
        if self._closed:
            return False
        if timeout_seconds <= 0:
            raise ValueError("Kafka metadata timeout must be positive")
        metadata = self._consumer.list_topics(timeout=timeout_seconds)
        return metadata is not None

    def checkpoint(self, record: KafkaProjectorRecord) -> None:
        if record.assignment_epoch != self._assignment_epoch:
            raise RuntimeError("Kafka assignment changed before stable checkpoint")
        assert TopicPartition is not None
        self._consumer.commit(
            offsets=[TopicPartition(record.topic, record.partition, record.offset + 1)],
            asynchronous=False,
        )

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._consumer.close()
