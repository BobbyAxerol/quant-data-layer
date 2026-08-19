from __future__ import annotations

import time
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
_RAW_ENVELOPE_HEADER = "qdl-raw-provider-envelope"


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
    raw_provider_envelope: bytes | None = None

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
            or self.raw_provider_envelope == b""
        ):
            raise ValueError("Kafka projector record is invalid")


class ProjectorBroker(Protocol):
    def poll(self, timeout_seconds: float) -> KafkaProjectorRecord | None: ...
    def checkpoint(self, record: KafkaProjectorRecord) -> None: ...
    def checkpoint_many(
        self, records: tuple[KafkaProjectorRecord, ...] | list[KafkaProjectorRecord]
    ) -> None: ...
    def pause_canonical(self) -> None: ...
    def resume_canonical(self) -> None: ...
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
    checkpoint_batch_size: int = 128
    checkpoint_interval_ms: int = 100

    def validate(self) -> None:
        if not all((
            self.bootstrap_servers.strip(), self.client_id.strip(), self.group_id.strip(),
            self.canonical_topic.strip(),
        )):
            raise ValueError("Kafka stable projector identity/topics are required")
        topics = (*self.raw_topics, self.canonical_topic)
        if any(not value.strip() for value in topics) or len(topics) != len(set(topics)):
            raise ValueError("Kafka stable projector topics must be non-empty and unique")
        if not 6_000 <= self.session_timeout_ms < self.max_poll_interval_ms:
            raise ValueError("Kafka stable projector timeout policy is invalid")
        if not 1 <= self.checkpoint_batch_size <= 10_000 or not (
            10 <= self.checkpoint_interval_ms <= 5_000
        ):
            raise ValueError("Kafka stable projector checkpoint policy is invalid")
        for value in (self.ca_path, self.certificate_path, self.key_path):
            if not value.is_file():
                raise ValueError(f"Kafka stable projector TLS file is unavailable: {value}")


class ConfluentProjectorBroker:
    """Read-committed source with bounded post-ACK checkpoint coalescing."""

    def __init__(self, config: KafkaProjectorConfig, *, consumer_factory=None) -> None:
        config.validate()
        factory = consumer_factory or Consumer
        if factory is None:
            raise RuntimeError("confluent-kafka runtime dependency is unavailable")
        self.config = config
        self._assignment_epoch = 1
        self._closed = False
        self._pending_offsets: dict[tuple[str, int], int] = {}
        self._pending_checkpoint_calls = 0
        self._last_checkpoint_flush = time.monotonic()
        self._commit_error: BaseException | None = None
        self._canonical_pause_requested = False
        self._canonical_pause_applied = False
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
            "on_commit": self._on_commit,
        })
        self._consumer.subscribe(
            [*config.raw_topics, config.canonical_topic],
            on_assign=self._on_assignment,
            on_revoke=self._on_assignment,
            on_lost=self._on_assignment,
        )

    def _on_commit(self, error, _partitions) -> None:
        if error is not None:
            self._commit_error = KafkaException(error)

    def _raise_commit_error(self) -> None:
        if self._commit_error is not None:
            raise RuntimeError("asynchronous stable checkpoint failed") from self._commit_error

    def _on_assignment(self, _consumer, _partitions) -> None:
        self._assignment_epoch += 1
        # These records were downstream-ACKed but not broker-checkpointed. The
        # new owner must replay them through the idempotent spool/projection path.
        self._pending_offsets.clear()
        self._pending_checkpoint_calls = 0
        self._last_checkpoint_flush = time.monotonic()
        self._canonical_pause_applied = False

    def _canonical_assignments(self):
        assignment = getattr(self._consumer, "assignment", None)
        if assignment is None:
            raise RuntimeError("Kafka consumer does not expose assignment flow control")
        return [
            item for item in assignment()
            if getattr(item, "topic", None) == self.config.canonical_topic
        ]

    def _apply_canonical_flow_control(self) -> None:
        partitions = self._canonical_assignments()
        if not partitions:
            self._canonical_pause_applied = False
            return
        if self._canonical_pause_requested and not self._canonical_pause_applied:
            self._consumer.pause(partitions)
            self._canonical_pause_applied = True
        elif not self._canonical_pause_requested and self._canonical_pause_applied:
            self._consumer.resume(partitions)
            self._canonical_pause_applied = False

    def pause_canonical(self) -> None:
        if self._closed:
            raise RuntimeError("Kafka stable projector consumer is closed")
        self._canonical_pause_requested = True
        self._apply_canonical_flow_control()

    def resume_canonical(self) -> None:
        if self._closed:
            raise RuntimeError("Kafka stable projector consumer is closed")
        self._canonical_pause_requested = False
        self._apply_canonical_flow_control()

    def _flush_checkpoints(self, *, asynchronous: bool) -> None:
        if not self._pending_offsets:
            return
        self._raise_commit_error()
        assert TopicPartition is not None
        offsets = [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in sorted(self._pending_offsets.items())
        ]
        result = self._consumer.commit(offsets=offsets, asynchronous=asynchronous)
        if not asynchronous and result:
            errors = [getattr(item, "error", None) for item in result]
            if any(error is not None for error in errors):
                raise RuntimeError("synchronous stable checkpoint failed")
        self._pending_offsets.clear()
        self._pending_checkpoint_calls = 0
        self._last_checkpoint_flush = time.monotonic()

    def poll(self, timeout_seconds: float) -> KafkaProjectorRecord | None:
        if self._closed:
            raise RuntimeError("Kafka stable projector consumer is closed")
        if timeout_seconds <= 0:
            raise ValueError("Kafka poll timeout must be positive")
        self._raise_commit_error()
        self._apply_canonical_flow_control()
        elapsed_ms = (time.monotonic() - self._last_checkpoint_flush) * 1000
        if self._pending_offsets and elapsed_ms >= self.config.checkpoint_interval_ms:
            self._flush_checkpoints(asynchronous=True)
        message = self._consumer.poll(timeout_seconds)
        self._raise_commit_error()
        # Assignment callbacks run inside poll. Apply an already-requested pause
        # immediately after a new assignment; the engine keeps one poll-batch of
        # headroom for the record that may have triggered the callback.
        self._apply_canonical_flow_control()
        if message is None:
            return None
        if message.error():
            raise KafkaException(message.error())
        headers = dict(message.headers() or ())
        event_id = headers.get(_EVENT_ID_HEADER)
        raw_provider_envelope = headers.get(_RAW_ENVELOPE_HEADER)
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
            raw_provider_envelope=(
                bytes(raw_provider_envelope)
                if raw_provider_envelope is not None
                else None
            ),
        )

    def ping(self, timeout_seconds: float = 1.0) -> bool:
        if self._closed:
            return False
        if timeout_seconds <= 0:
            raise ValueError("Kafka metadata timeout must be positive")
        self._raise_commit_error()
        metadata = self._consumer.list_topics(timeout=timeout_seconds)
        return metadata is not None

    def checkpoint(self, record: KafkaProjectorRecord) -> None:
        self.checkpoint_many((record,))

    def checkpoint_many(
        self, records: tuple[KafkaProjectorRecord, ...] | list[KafkaProjectorRecord]
    ) -> None:
        values = tuple(records)
        if not values:
            return
        if any(record.assignment_epoch != self._assignment_epoch for record in values):
            raise RuntimeError("Kafka assignment changed before stable checkpoint")
        self._raise_commit_error()
        for record in values:
            key = (record.topic, record.partition)
            next_offset = record.offset + 1
            current = self._pending_offsets.get(key)
            if current is not None and next_offset < current:
                raise RuntimeError("Kafka stable projector checkpoint regressed")
            self._pending_offsets[key] = max(current or 0, next_offset)
        self._pending_checkpoint_calls += len(values)
        elapsed_ms = (time.monotonic() - self._last_checkpoint_flush) * 1000
        if (
            self._pending_checkpoint_calls >= self.config.checkpoint_batch_size
            or elapsed_ms >= self.config.checkpoint_interval_ms
        ):
            self._flush_checkpoints(asynchronous=True)

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._raise_commit_error()
            self._flush_checkpoints(asynchronous=False)
        finally:
            self._closed = True
            self._consumer.close()
