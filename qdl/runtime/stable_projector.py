from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Awaitable, Callable, Protocol

from qdl.marketdata.v2 import market_data_pb2
from qdl.projection.stable import StableCompatibilityProjector, StableProjectionTarget
from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.envelope import validate_raw_envelope
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.stream import DurableStreamGateway
from qdl.transport import DurableEvent, SQLiteDurableSpool, StoredEvent
from qdl.transport.kafka_projector import KafkaProjectorRecord, ProjectorBroker


logger = logging.getLogger(__name__)


class StableCanonicalSink(Protocol):
    async def publish(self, event: DurableEvent) -> StoredEvent: ...
    async def publish_many(
        self, events: tuple[DurableEvent, ...] | list[DurableEvent]
    ) -> tuple[StoredEvent, ...]: ...


class LocalStableCanonicalSink:
    """In-process implementation used by isolated tests and single-node rehearsal."""

    def __init__(self, gateway: DurableStreamGateway, spool: SQLiteDurableSpool) -> None:
        self.gateway = gateway
        self.spool = spool

    async def publish(self, event: DurableEvent) -> StoredEvent:
        return (await self.publish_many((event,)))[0]

    async def publish_many(
        self, events: tuple[DurableEvent, ...] | list[DurableEvent]
    ) -> tuple[StoredEvent, ...]:
        values = tuple(events)
        stored_values = await self.gateway.publish_many(values)
        resolved = []
        for event, stored in zip(values, stored_values, strict=True):
            if stored is None:
                stored = await asyncio.to_thread(
                    self.spool.find_event,
                    stream=event.stream,
                    event_id=event.event_id,
                )
            if stored is None:
                raise RuntimeError("duplicate canonical ACK has no shared cache record")
            resolved.append(stored)
        return tuple(resolved)


@dataclass(frozen=True, slots=True)
class StableProjectorStats:
    raw_committed: int
    canonical_committed: int
    duplicate_projections: int
    pending_canonical: int
    pending_bytes: int


@dataclass(frozen=True, slots=True)
class _ReadyCanonical:
    partition: tuple[str, int]
    record: KafkaProjectorRecord
    envelope: market_data_pb2.EventEnvelope
    raw: StoredEvent
    event: DurableEvent


class StableProjectorEngine:
    """Kafka-authoritative raw/canonical join with downstream-before-checkpoint ordering."""

    def __init__(
        self,
        *,
        broker: ProjectorBroker,
        spool: SQLiteDurableSpool,
        catalog: StableSourceCatalog,
        canonical_topic: str,
        raw_topics: tuple[str, ...],
        sink: StableCanonicalSink,
        projector: StableCompatibilityProjector,
        target: StableProjectionTarget,
        max_pending_records: int = 10_000,
        max_pending_bytes: int = 256 * 1024 * 1024,
        max_batch_records: int = 128,
        batch_wait_seconds: float = 0.025,
    ) -> None:
        if (
            not canonical_topic
            or not raw_topics
            or canonical_topic in raw_topics
            or len(raw_topics) != len(set(raw_topics))
        ):
            raise ValueError("stable projector topics are invalid")
        if max_pending_records <= 0 or max_pending_bytes <= 0:
            raise ValueError("stable projector pending bounds must be positive")
        if not 1 <= max_batch_records <= 1000 or not 0 < batch_wait_seconds <= 1:
            raise ValueError("stable projector batch policy is invalid")
        self.broker = broker
        self.spool = spool
        self.catalog = catalog
        self.canonical_topic = canonical_topic
        self.raw_topics = raw_topics
        self.sink = sink
        self.projector = projector
        self.target = target
        self.max_pending_records = max_pending_records
        self.max_pending_bytes = max_pending_bytes
        self.max_batch_records = max_batch_records
        self.batch_wait_seconds = batch_wait_seconds
        self._queues: dict[tuple[str, int], deque[KafkaProjectorRecord]] = defaultdict(deque)
        self._waiting: dict[bytes, set[tuple[str, int]]] = defaultdict(set)
        self._assignment_epoch: int | None = None
        self._pending_records = 0
        self._pending_bytes = 0
        self._raw_committed = 0
        self._canonical_committed = 0
        self._duplicate_projections = 0

    async def accept(self, record: KafkaProjectorRecord) -> None:
        await self.accept_many((record,))

    async def accept_many(
        self, records: tuple[KafkaProjectorRecord, ...] | list[KafkaProjectorRecord]
    ) -> None:
        values = tuple(records)
        if not values:
            return
        start = 0
        while start < len(values):
            epoch = values[start].assignment_epoch
            end = start + 1
            while end < len(values) and values[end].assignment_epoch == epoch:
                end += 1
            await self._accept_assignment_batch(values[start:end], epoch)
            start = end

    async def _accept_assignment_batch(
        self, records: tuple[KafkaProjectorRecord, ...], epoch: int
    ) -> None:
        self._handle_assignment(epoch)
        raw_values: list[tuple[KafkaProjectorRecord, bytes, DurableEvent]] = []
        canonical_values: list[KafkaProjectorRecord] = []
        for record in records:
            if record.topic in self.raw_topics:
                capture_id, event = self._raw_event(record)
                raw_values.append((record, capture_id, event))
            elif record.topic == self.canonical_topic:
                canonical_values.append(record)
            else:
                raise ValueError("stable projector received an unconfigured topic")

        if raw_values:
            await asyncio.to_thread(
                self.spool.append_many, [item[2] for item in raw_values]
            )
            for record, _capture_id, _event in raw_values:
                await asyncio.to_thread(self.broker.checkpoint, record)
            self._raw_committed += len(raw_values)
            await self._drain_ready()

        for record in canonical_values:
            envelope = market_data_pb2.EventEnvelope.FromString(record.payload)
            binding = self.catalog.binding_for_envelope(envelope)
            if (
                bytes(envelope.event_id) != record.event_id
                or binding.partition_key != record.key
            ):
                raise ValueError("Kafka canonical metadata differs from stable envelope")
            partition = (record.topic, record.partition)
            queue = self._queues[partition]
            if queue and record.offset <= queue[-1].offset:
                raise ValueError("Kafka canonical partition order regressed")
            self._admit_pending(record)
            queue.append(record)
        await self._drain_ready()

    async def run_once(self, timeout_seconds: float = 1.0) -> bool:
        record = await asyncio.to_thread(self.broker.poll, timeout_seconds)
        if record is None:
            # Another projector replica may have persisted the correlated raw
            # envelope into the shared cache. Retry bounded local partitions so
            # cross-replica raw/canonical ordering cannot stall indefinitely.
            await self._drain_ready()
            return False
        records = [record]
        deadline = asyncio.get_running_loop().time() + self.batch_wait_seconds
        while len(records) < self.max_batch_records:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            item = await asyncio.to_thread(self.broker.poll, remaining)
            if item is None:
                break
            records.append(item)
        await self.accept_many(records)
        return True

    def _raw_event(self, record: KafkaProjectorRecord) -> tuple[bytes, DurableEvent]:
        raw = raw_provider_pb2.RawProviderEnvelope.FromString(record.payload)
        validate_raw_envelope(raw)
        capture_id = bytes(raw.capture_id)
        if capture_id != record.event_id:
            raise ValueError("Kafka raw event ID differs from capture ID")
        return capture_id, DurableEvent(
            stream=record.topic,
            partition_key=record.key,
            event_id=capture_id,
            payload=record.payload,
            accepted_at_ns=record.accepted_at_ns,
            headers={
                "kafka_partition": str(record.partition),
                "kafka_offset": str(record.offset),
                "schema": f"{raw.raw_schema_name}/{raw.raw_schema_major}",
            },
        )

    async def _drain_ready(self) -> None:
        while True:
            ready = self._ready_batch()
            if not ready:
                return
            stored_values = await self.sink.publish_many(
                [item.event for item in ready]
            )
            projections = [
                self.projector.build(stored, item.raw.event.payload)
                for item, stored in zip(ready, stored_values, strict=True)
            ]
            applied = await asyncio.to_thread(self.target.apply_many, projections)
            if len(applied) != len(ready):
                raise RuntimeError("stable projection target returned an invalid result count")
            for item in ready:
                await asyncio.to_thread(self.broker.checkpoint, item.record)
            for item, was_applied in zip(ready, applied, strict=True):
                if not was_applied:
                    self._duplicate_projections += 1
                self._canonical_committed += 1
                queue = self._queues[item.partition]
                current = queue.popleft()
                if current.offset != item.record.offset:
                    raise RuntimeError("stable canonical queue order changed during batch")
                self._pending_records -= 1
                self._pending_bytes -= len(item.record.payload)
                capture_id = bytes(item.envelope.raw_capture_id)
                waiting = self._waiting.get(capture_id)
                if waiting is not None:
                    waiting.discard(item.partition)
                    if not waiting:
                        self._waiting.pop(capture_id, None)
                if not queue:
                    self._queues.pop(item.partition, None)

    def _ready_batch(self) -> tuple[_ReadyCanonical, ...]:
        ready = []
        for partition in sorted(self._queues):
            queue = self._queues[partition]
            for record in queue:
                if len(ready) >= self.max_batch_records:
                    return tuple(ready)
                envelope = market_data_pb2.EventEnvelope.FromString(record.payload)
                raw = self._find_raw(bytes(envelope.raw_capture_id))
                if raw is None:
                    self._waiting[bytes(envelope.raw_capture_id)].add(partition)
                    break
                ready.append(_ReadyCanonical(
                    partition=partition,
                    record=record,
                    envelope=envelope,
                    raw=raw,
                    event=DurableEvent(
                        stream=self.catalog.canonical_stream,
                        partition_key=record.key,
                        event_id=record.event_id,
                        payload=record.payload,
                        accepted_at_ns=record.accepted_at_ns,
                        headers={
                            "raw_stream": raw.event.stream,
                            "raw_event_id": raw.event.event_id.hex(),
                            "kafka_topic": record.topic,
                            "kafka_partition": str(record.partition),
                            "kafka_offset": str(record.offset),
                        },
                    ),
                ))
        return tuple(ready)

    def _find_raw(self, capture_id: bytes) -> StoredEvent | None:
        for stream in self.raw_topics:
            found = self.spool.find_event(stream=stream, event_id=capture_id)
            if found is not None:
                return found
        return None

    def _admit_pending(self, record: KafkaProjectorRecord) -> None:
        if (
            self._pending_records + 1 > self.max_pending_records
            or self._pending_bytes + len(record.payload) > self.max_pending_bytes
        ):
            raise RuntimeError("stable projector canonical-before-raw buffer exhausted")
        self._pending_records += 1
        self._pending_bytes += len(record.payload)

    def _handle_assignment(self, epoch: int) -> None:
        if self._assignment_epoch is None:
            self._assignment_epoch = epoch
            return
        if epoch == self._assignment_epoch:
            return
        # Uncheckpointed broker records are discarded locally and replayed by
        # the new assignment. Durable cache/projection duplicates are idempotent.
        self._queues.clear()
        self._waiting.clear()
        self._pending_records = 0
        self._pending_bytes = 0
        self._assignment_epoch = epoch

    @property
    def stats(self) -> StableProjectorStats:
        return StableProjectorStats(
            raw_committed=self._raw_committed,
            canonical_committed=self._canonical_committed,
            duplicate_projections=self._duplicate_projections,
            pending_canonical=self._pending_records,
            pending_bytes=self._pending_bytes,
        )


async def supervise_stable_projector(
    *,
    broker_factory: Callable[[], tuple[ProjectorBroker, StableProjectorEngine]],
    should_stop: Callable[[], bool],
    on_broker: Callable[[ProjectorBroker | None], None],
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    retry_initial_seconds: float = 0.25,
    retry_max_seconds: float = 5.0,
) -> None:
    """Recreate poisoned Kafka generations without weakening ACK ordering."""

    if (
        retry_initial_seconds <= 0
        or retry_max_seconds < retry_initial_seconds
    ):
        raise ValueError("stable projector retry policy is invalid")
    failures = 0
    while not should_stop():
        broker = None
        try:
            broker, engine = broker_factory()
            on_broker(broker)
            while not should_stop():
                if await engine.run_once(timeout_seconds=1.0):
                    failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - supervisor boundary
            failures += 1
            logger.warning(
                "stable projector generation failed; reconnecting attempt=%s error=%s",
                failures,
                error,
            )
        finally:
            on_broker(None)
            if broker is not None:
                try:
                    await asyncio.to_thread(broker.close)
                except Exception as error:  # noqa: BLE001 - poisoned generation cleanup
                    logger.warning(
                        "stable projector generation close failed during recovery error=%s",
                        error,
                    )
        if not should_stop():
            delay = min(
                retry_max_seconds,
                retry_initial_seconds * (2 ** min(max(failures - 1, 0), 8)),
            )
            await sleep(delay)
