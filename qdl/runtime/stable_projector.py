from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Awaitable, Callable, Protocol

from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.projection.stable import StableCompatibilityProjector, StableProjectionTarget
from qdl.provider.v1 import raw_provider_pb2
from qdl.runtime.mark_index_lineage import (
    DERIVED_MARK_INDEX_COMPONENT_V1,
    is_derived_mark_index_lineage,
    validate_derived_mark_index_component,
)
from qdl.raw.envelope import validate_raw_envelope
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.stream import DurableStreamGateway
from qdl.transport import (
    DurableEvent,
    EventIdCollision,
    SQLiteDurableSpool,
    StoredEvent,
)
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
        duplicate_ids = [
            event.event_id
            for event, stored in zip(values, stored_values, strict=True)
            if stored is None
        ]
        duplicates = await asyncio.to_thread(
            self.spool.find_events,
            stream=values[0].stream,
            event_ids=duplicate_ids,
        )
        resolved = []
        for event, stored in zip(values, stored_values, strict=True):
            stored = stored or duplicates.get(event.event_id)
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
    raw_envelope: bytes
    raw_stream: str
    raw_event_id: bytes
    event: DurableEvent
    existing: StoredEvent | None = None
    semantic_duplicate: bool = False
    project_latest: bool = True
    terminal_reason: str | None = None
    derived_mark_index_component: bool = False


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
        max_batch_bytes: int | None = None,
        batch_wait_seconds: float = 0.025,
    ) -> None:
        if (
            not canonical_topic
            or canonical_topic in raw_topics
            or len(raw_topics) != len(set(raw_topics))
        ):
            raise ValueError("stable projector topics are invalid")
        if max_pending_records <= 0 or max_pending_bytes <= 0:
            raise ValueError("stable projector pending bounds must be positive")
        if not 1 <= max_batch_records <= 1000 or not 0 < batch_wait_seconds <= 1:
            raise ValueError("stable projector batch policy is invalid")
        if max_batch_bytes is None:
            max_batch_bytes = min(8 * 1024 * 1024, max_pending_bytes)
        if not 1 <= max_batch_bytes <= max_pending_bytes:
            raise ValueError("stable projector batch byte bound is invalid")
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
        self.max_batch_bytes = max_batch_bytes
        self.batch_wait_seconds = batch_wait_seconds
        poll_headroom = min(max_batch_records, max(1, max_pending_records // 4))
        self._canonical_pause_high_records = max(
            1, max_pending_records - poll_headroom
        )
        self._canonical_resume_low_records = self._canonical_pause_high_records // 2
        self._canonical_pause_high_bytes = max(1, max_pending_bytes * 3 // 4)
        self._canonical_resume_low_bytes = self._canonical_pause_high_bytes // 2
        self._canonical_paused = False
        self._queues: dict[tuple[str, int], deque[KafkaProjectorRecord]] = defaultdict(deque)
        self._waiting: dict[bytes, set[tuple[str, int]]] = defaultdict(set)
        self._assignment_epoch: int | None = None
        self._pending_records = 0
        self._pending_bytes = 0
        self._raw_committed = 0
        self._canonical_committed = 0
        self._duplicate_projections = 0
        self._deferred_record: KafkaProjectorRecord | None = None

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
            await asyncio.to_thread(
                self._checkpoint_records, [item[0] for item in raw_values]
            )
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
        record = self._deferred_record
        self._deferred_record = None
        if record is None:
            record = await asyncio.to_thread(self.broker.poll, timeout_seconds)
        if record is None:
            # Another projector replica may have persisted the correlated raw
            # envelope into the shared cache. Retry bounded local partitions so
            # cross-replica raw/canonical ordering cannot stall indefinitely.
            await self._drain_ready()
            return False
        records = [record]
        batch_bytes = len(record.payload)
        deadline = asyncio.get_running_loop().time() + self.batch_wait_seconds
        while len(records) < self.max_batch_records:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            item = await asyncio.to_thread(self.broker.poll, remaining)
            if item is None:
                break
            if batch_bytes + len(item.payload) > self.max_batch_bytes:
                # Kafka has already delivered this record. Keep it in-order for
                # the next bounded drain instead of overfilling a Python batch.
                self._deferred_record = item
                break
            records.append(item)
            batch_bytes += len(item.payload)
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
            ready = await self._ready_batch()
            if not ready:
                return
            fresh = tuple(
                item
                for item in ready
                if not item.semantic_duplicate and item.terminal_reason is None
            )
            fresh_stored = (
                await self.sink.publish_many([item.event for item in fresh])
                if fresh
                else ()
            )
            stored_iterator = iter(fresh_stored)
            resolved: list[tuple[_ReadyCanonical, StoredEvent]] = []
            for item in ready:
                stored = (
                    item.existing
                    if item.semantic_duplicate or item.terminal_reason is not None
                    else next(stored_iterator)
                )
                if stored is None:
                    raise RuntimeError("stable retained canonical record has no cache record")
                resolved.append((item, stored))

            projected = tuple(
                (item, stored)
                for item, stored in resolved
                if (
                    item.project_latest
                    and not item.semantic_duplicate
                    and item.terminal_reason is None
                )
            )
            projections = [
                self.projector.build(
                    stored,
                    item.raw_envelope,
                    derived_mark_index_component=item.derived_mark_index_component,
                )
                for item, stored in projected
            ]
            applied = (
                await asyncio.to_thread(self.target.apply_many, projections)
                if projections
                else ()
            )
            if len(applied) != len(projected):
                raise RuntimeError(
                    "stable projection target returned an invalid result count"
                )
            terminalized = tuple(
                item for item in ready if item.terminal_reason is not None
            )
            if terminalized:
                await asyncio.to_thread(
                    self._quarantine_terminal_recovery_overlaps, terminalized
                )
                logger.warning(
                    "terminalized stale BAR recovery overlaps count=%s reason=%s",
                    len(terminalized),
                    terminalized[0].terminal_reason,
                )
            applied_by_event = {
                item.record.event_id: was_applied
                for (item, _stored), was_applied in zip(
                    projected, applied, strict=True
                )
            }
            await asyncio.to_thread(
                self._checkpoint_records, [item.record for item in ready]
            )
            for item in ready:
                if item.semantic_duplicate or (
                    item.record.event_id in applied_by_event
                    and not applied_by_event[item.record.event_id]
                ):
                    self._duplicate_projections += 1
                self._canonical_committed += 1
                queue = self._queues[item.partition]
                current = queue.popleft()
                if current.offset != item.record.offset:
                    raise RuntimeError(
                        "stable canonical queue order changed during batch"
                    )
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
            self._update_canonical_backpressure()

    @staticmethod
    def _verified_payload_hash(
        envelope: market_data_pb2.EventEnvelope,
    ) -> bytes:
        payload_name = envelope.WhichOneof("payload")
        if not payload_name:
            raise EventIdCollision("canonical duplicate has no market payload")
        declared = bytes(envelope.canonical_payload_hash)
        observed = hashlib.sha256(
            getattr(envelope, payload_name).SerializeToString(deterministic=True)
        ).digest()
        if len(declared) != 32 or not hmac.compare_digest(declared, observed):
            raise EventIdCollision(
                "canonical duplicate payload hash is missing or invalid"
            )
        return declared

    @staticmethod
    def _decimal_semantic(value) -> Decimal:
        coefficient_name = value.WhichOneof("coefficient")
        if coefficient_name == "mantissa":
            coefficient = int(value.mantissa)
        elif coefficient_name == "mantissa_text":
            try:
                coefficient = int(value.mantissa_text)
            except ValueError as error:
                raise EventIdCollision(
                    "canonical decimal coefficient is invalid"
                ) from error
        else:
            raise EventIdCollision("canonical decimal coefficient is missing")
        try:
            observed = Decimal(coefficient).scaleb(-int(value.scale))
            declared = Decimal(value.source_text)
        except (InvalidOperation, ValueError) as error:
            raise EventIdCollision("canonical decimal text is invalid") from error
        if not declared.is_finite() or declared != observed:
            raise EventIdCollision(
                "canonical decimal audit text differs from its exact value"
            )
        return observed

    @classmethod
    def _bar_semantics(cls, bar: market_data_pb2.Bar) -> tuple:
        def optional_decimal(field: str):
            return (
                cls._decimal_semantic(getattr(bar, field))
                if bar.HasField(field)
                else None
            )

        return (
            bar.interval,
            int(bar.open_time_ns),
            int(bar.close_time_ns),
            cls._decimal_semantic(bar.open),
            cls._decimal_semantic(bar.high),
            cls._decimal_semantic(bar.low),
            cls._decimal_semantic(bar.close),
            cls._decimal_semantic(bar.volume),
            int(bar.trade_count),
            bool(bar.is_final),
            int(bar.revision),
            int(bar.lifecycle),
            (
                bytes(bar.supersedes_event_id)
                if bar.HasField("supersedes_event_id")
                else None
            ),
            int(bar.volume_unit),
            optional_decimal("base_volume"),
            optional_decimal("quote_volume"),
            optional_decimal("contract_volume"),
        )

    @classmethod
    def _same_market_semantics(
        cls,
        existing: market_data_pb2.EventEnvelope,
        candidate: market_data_pb2.EventEnvelope,
    ) -> bool:
        existing_name = existing.WhichOneof("payload")
        candidate_name = candidate.WhichOneof("payload")
        if existing_name != candidate_name:
            return False
        existing_hash = cls._verified_payload_hash(existing)
        candidate_hash = cls._verified_payload_hash(candidate)
        if existing_name == "bar":
            # Decimal spelling and acquisition origin are audit provenance, not
            # a different OHLCV observation. Every actual BAR value remains
            # strict and is compared above exact Decimal arithmetic.
            return cls._bar_semantics(existing.bar) == cls._bar_semantics(candidate.bar)
        return hmac.compare_digest(existing_hash, candidate_hash)

    @classmethod
    def _semantic_duplicate(
        cls,
        existing: StoredEvent,
        record: KafkaProjectorRecord,
        envelope: market_data_pb2.EventEnvelope,
    ) -> bool:
        if existing.event.payload == record.payload:
            return False
        existing_envelope = market_data_pb2.EventEnvelope.FromString(
            existing.event.payload
        )
        if (
            existing.cursor.partition_key != record.key
            or bytes(existing_envelope.event_id) != record.event_id
            or not cls._same_market_semantics(existing_envelope, envelope)
        ):
            raise EventIdCollision(
                "canonical event ID maps to different market semantics"
            )
        return True

    @staticmethod
    def _recovery_binding_identity(
        envelope: market_data_pb2.EventEnvelope,
    ) -> tuple:
        """Identity that must not change across a stale recovery overlap."""

        return (
            envelope.schema_name,
            int(envelope.schema_major),
            int(envelope.schema_minor),
            envelope.instrument_uid,
            envelope.instrument_id,
            int(envelope.instrument_revision),
            envelope.venue,
            envelope.market,
            envelope.product_type,
            envelope.native_symbol,
            envelope.provider,
            envelope.source_id,
            int(envelope.source_role),
            envelope.source_sequence,
            int(envelope.source_event_time_ns),
        )

    @classmethod
    def _is_terminal_recovery_backfill_overlap(
        cls,
        existing: StoredEvent,
        record: KafkaProjectorRecord,
        candidate: market_data_pb2.EventEnvelope,
    ) -> bool:
        """Recognize exactly one historical replay defect and nothing broader.

        A captured final BAR is already durable.  A later recovery REST row may
        display settled OHLCV under the same revision-zero source sequence.  It
        is forensic evidence, not a revision: retain the native BAR and record
        the stale candidate before checkpointing it.  Any non-identical domain
        shape deliberately returns ``False`` so generic collision fencing wins.
        """

        if existing.event.payload == record.payload:
            return False
        existing_envelope = market_data_pb2.EventEnvelope.FromString(
            existing.event.payload
        )
        if (
            existing.cursor.partition_key != record.key
            or bytes(existing_envelope.event_id) != record.event_id
            or existing_envelope.WhichOneof("payload") != "bar"
            or candidate.WhichOneof("payload") != "bar"
            or len(bytes(existing_envelope.raw_capture_id)) not in {16, 32}
            or len(bytes(candidate.raw_capture_id)) not in {16, 32}
            or len(bytes(existing_envelope.raw_payload_hash)) != 32
            or len(bytes(candidate.raw_payload_hash)) != 32
        ):
            return False
        cls._verified_payload_hash(existing_envelope)
        cls._verified_payload_hash(candidate)
        existing_bar = existing_envelope.bar
        candidate_bar = candidate.bar
        if (
            cls._recovery_binding_identity(existing_envelope)
            != cls._recovery_binding_identity(candidate)
            or not existing_envelope.source_sequence
            or existing_bar.interval != candidate_bar.interval
            or int(existing_bar.open_time_ns) != int(candidate_bar.open_time_ns)
            or int(existing_bar.close_time_ns) != int(candidate_bar.close_time_ns)
            or not existing_bar.is_final
            or not candidate_bar.is_final
            or int(existing_bar.revision) != 0
            or int(candidate_bar.revision) != 0
            or existing_bar.lifecycle != market_data_pb2.BAR_LIFECYCLE_FINAL
            or candidate_bar.lifecycle != market_data_pb2.BAR_LIFECYCLE_FINAL
            or existing_bar.HasField("supersedes_event_id")
            or candidate_bar.HasField("supersedes_event_id")
            or existing_bar.origin != common_pb2.BAR_ORIGIN_VENUE_NATIVE
            or candidate_bar.origin != common_pb2.BAR_ORIGIN_BACKFILLED
        ):
            return False
        return True

    def _quarantine_terminal_recovery_overlaps(
        self, items: tuple[_ReadyCanonical, ...]
    ) -> None:
        for item in items:
            if item.terminal_reason != "RECOVERY_BACKFILL_OVERLAP_CONFLICT":
                raise RuntimeError("unknown stable projector terminal reason")
            self.spool.quarantine_once(
                event=item.event,
                reason_code=item.terminal_reason,
                reason_message=(
                    "retained native final BAR wins over stale recovery backfill"
                ),
                retry_count=0,
            )

    def _latest_bar_close_ns(self, partition_key: str) -> int | None:
        rows = self.spool.read_tail(
            stream=self.catalog.canonical_stream,
            partition_key=partition_key,
            limit=10_000,
        )
        closes = []
        for stored in rows:
            envelope = market_data_pb2.EventEnvelope.FromString(
                stored.event.payload
            )
            if envelope.WhichOneof("payload") == "bar":
                closes.append(int(envelope.bar.close_time_ns))
        return max(closes) if closes else None

    async def _ready_batch(self) -> tuple[_ReadyCanonical, ...]:
        candidates = []
        for partition, record in self._round_robin_candidates(
            self._queues, self.max_batch_records
        ):
            envelope = market_data_pb2.EventEnvelope.FromString(record.payload)
            candidates.append((
                partition, record, envelope, bytes(envelope.raw_capture_id)
            ))

        existing_by_id = await asyncio.to_thread(
            self.spool.find_events,
            stream=self.catalog.canonical_stream,
            event_ids=tuple(record.event_id for _p, record, _e, _c in candidates),
        )
        semantic_duplicates = {}
        terminal_recovery_overlaps = {}
        for _partition, record, envelope, _capture_id in candidates:
            existing = existing_by_id.get(record.event_id)
            if existing is None:
                continue
            try:
                is_duplicate = self._semantic_duplicate(existing, record, envelope)
            except EventIdCollision:
                if self._is_terminal_recovery_backfill_overlap(
                    existing, record, envelope
                ):
                    terminal_recovery_overlaps[record.event_id] = existing
                    continue
                raise
            if is_duplicate:
                semantic_duplicates[record.event_id] = existing
        fallback_ids = tuple(
            capture_id
            for _partition, record, _envelope, capture_id in candidates
            if record.event_id not in semantic_duplicates
            and record.event_id not in terminal_recovery_overlaps
            and record.raw_provider_envelope is None
        )
        raw_by_id = await asyncio.to_thread(self._find_raw_many, fallback_ids)
        bar_high_watermarks: dict[str, int | None] = {}
        ready = []
        blocked_partitions = set()
        for partition, record, envelope, capture_id in candidates:
            if partition in blocked_partitions:
                continue
            existing = semantic_duplicates.get(record.event_id)
            if existing is not None:
                ready.append(_ReadyCanonical(
                    partition=partition,
                    record=record,
                    envelope=envelope,
                    raw_envelope=b"",
                    raw_stream="semantic-duplicate",
                    raw_event_id=capture_id,
                    event=DurableEvent(
                        stream=self.catalog.canonical_stream,
                        partition_key=record.key,
                        event_id=record.event_id,
                        payload=record.payload,
                        accepted_at_ns=record.accepted_at_ns,
                    ),
                    existing=existing,
                    semantic_duplicate=True,
                    project_latest=False,
                ))
                continue

            retained = terminal_recovery_overlaps.get(record.event_id)
            if retained is not None:
                ready.append(_ReadyCanonical(
                    partition=partition,
                    record=record,
                    envelope=envelope,
                    raw_envelope=b"",
                    raw_stream="recovery-backfill-overlap",
                    raw_event_id=capture_id,
                    event=DurableEvent(
                        stream=self.catalog.canonical_stream,
                        partition_key=record.key,
                        event_id=record.event_id,
                        payload=record.payload,
                        accepted_at_ns=record.accepted_at_ns,
                    ),
                    existing=retained,
                    project_latest=False,
                    terminal_reason="RECOVERY_BACKFILL_OVERLAP_CONFLICT",
                ))
                continue

            if record.raw_provider_envelope is not None:
                raw_envelope = record.raw_provider_envelope
                raw = raw_provider_pb2.RawProviderEnvelope.FromString(raw_envelope)
                validate_raw_envelope(raw)
                binding = self.catalog.binding_for_envelope(envelope)
                derived_mark_index_component = is_derived_mark_index_lineage(
                    envelope
                )
                if derived_mark_index_component:
                    validate_derived_mark_index_component(envelope, raw, binding)
                elif bytes(raw.capture_id) != capture_id:
                    raise ValueError(
                        "private Kafka raw lineage differs from canonical capture ID"
                    )
                raw_stream = "kafka-header:qdl-raw-provider-envelope"
                raw_event_id = capture_id
            else:
                stored_raw = raw_by_id.get(capture_id)
                if stored_raw is None:
                    if not self.raw_topics:
                        raise ValueError(
                            "canonical record is missing private Kafka raw lineage"
                        )
                    self._waiting[capture_id].add(partition)
                    blocked_partitions.add(partition)
                    continue
                raw_envelope = stored_raw.event.payload
                raw_stream = stored_raw.event.stream
                raw_event_id = stored_raw.event.event_id
                derived_mark_index_component = False

            project_latest = True
            if envelope.WhichOneof("payload") == "bar":
                if record.key not in bar_high_watermarks:
                    bar_high_watermarks[record.key] = await asyncio.to_thread(
                        self._latest_bar_close_ns, record.key
                    )
                current = bar_high_watermarks[record.key]
                close_ns = int(envelope.bar.close_time_ns)
                project_latest = current is None or close_ns >= current
                bar_high_watermarks[record.key] = (
                    close_ns if current is None else max(current, close_ns)
                )
            ready.append(_ReadyCanonical(
                partition=partition,
                record=record,
                envelope=envelope,
                raw_envelope=raw_envelope,
                raw_stream=raw_stream,
                raw_event_id=raw_event_id,
                event=DurableEvent(
                    stream=self.catalog.canonical_stream,
                    partition_key=record.key,
                    event_id=record.event_id,
                    payload=record.payload,
                    accepted_at_ns=record.accepted_at_ns,
                    headers={
                        "raw_stream": raw_stream,
                        "raw_event_id": raw_event_id.hex(),
                        "raw_provider_envelope": base64.b64encode(
                            raw_envelope
                        ).decode("ascii"),
                        **(
                            {
                                "raw_lineage_kind": (
                                    DERIVED_MARK_INDEX_COMPONENT_V1
                                )
                            }
                            if derived_mark_index_component
                            else {}
                        ),
                        "kafka_topic": record.topic,
                        "kafka_partition": str(record.partition),
                        "kafka_offset": str(record.offset),
                    },
                ),
                project_latest=project_latest,
                derived_mark_index_component=derived_mark_index_component,
            ))
        return tuple(ready)

    @staticmethod
    def _round_robin_candidates(
        queues: dict[tuple[str, int], deque[KafkaProjectorRecord]],
        limit: int,
    ) -> tuple[tuple[tuple[str, int], KafkaProjectorRecord], ...]:
        """Select a bounded, fair prefix while preserving each partition's FIFO."""

        if limit <= 0:
            return ()
        active = deque(
            (partition, iter(queues[partition]))
            for partition in sorted(queues)
            if queues[partition]
        )
        selected = []
        while active and len(selected) < limit:
            partition, records = active.popleft()
            try:
                record = next(records)
            except StopIteration:
                continue
            selected.append((partition, record))
            active.append((partition, records))
        return tuple(selected)

    def _find_raw_many(
        self, capture_ids: tuple[bytes, ...]
    ) -> dict[bytes, StoredEvent]:
        missing = set(capture_ids)
        resolved = {}
        for stream in self.raw_topics:
            if not missing:
                break
            found = self.spool.find_events(
                stream=stream, event_ids=tuple(missing)
            )
            resolved.update(found)
            missing.difference_update(found)
        return resolved

    def _checkpoint_records(
        self, records: tuple[KafkaProjectorRecord, ...] | list[KafkaProjectorRecord]
    ) -> None:
        values = tuple(records)
        checkpoint_many = getattr(self.broker, "checkpoint_many", None)
        if callable(checkpoint_many):
            checkpoint_many(values)
            return
        for record in values:
            self.broker.checkpoint(record)

    def _admit_pending(self, record: KafkaProjectorRecord) -> None:
        if (
            self._pending_records + 1 > self.max_pending_records
            or self._pending_bytes + len(record.payload) > self.max_pending_bytes
        ):
            raise RuntimeError("stable projector canonical-before-raw buffer exhausted")
        self._pending_records += 1
        self._pending_bytes += len(record.payload)
        self._update_canonical_backpressure()

    def _update_canonical_backpressure(self) -> None:
        if (
            not self._canonical_paused
            and (
                self._pending_records >= self._canonical_pause_high_records
                or self._pending_bytes >= self._canonical_pause_high_bytes
            )
        ):
            self.broker.pause_canonical()
            self._canonical_paused = True
        elif (
            self._canonical_paused
            and self._pending_records <= self._canonical_resume_low_records
            and self._pending_bytes <= self._canonical_resume_low_bytes
        ):
            self.broker.resume_canonical()
            self._canonical_paused = False

    def _handle_assignment(self, epoch: int) -> None:
        if self._assignment_epoch is None:
            self._assignment_epoch = epoch
            return
        if epoch == self._assignment_epoch:
            return
        # Uncheckpointed broker records are discarded locally and replayed by
        # the new assignment. Durable cache/projection duplicates are idempotent.
        if self._canonical_paused:
            self.broker.resume_canonical()
            self._canonical_paused = False
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
