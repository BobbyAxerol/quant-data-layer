from __future__ import annotations

import hashlib
import time
from dataclasses import replace
from datetime import time as wall_time

from qdl.common.v1 import common_pb2
from qdl.domain.calendar import MarketSession, TradingCalendar
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.quantity import quantity_unit_name
from qdl.marketdata.v2 import market_data_pb2
from qdl.query import (
    AccessPurpose,
    BarLifecycle,
    ContractMetadata,
    ConsumerGrade,
    CoverageStatus,
    DataProduct,
    DataRequirement,
    FeedType,
    GapRecord,
    HistoryResult,
    InstrumentQuery,
    MarketDataItem,
    QualityMetadata,
    SourceMetadata,
    V2QueryService,
)
from qdl.replay import GapFreeHandoff
from qdl.runtime.stable_catalog import StableSourceBinding, StableSourceCatalog
from qdl.stream import GrpcSnapshot
from qdl.transport import Cursor, SQLiteDurableSpool, StoredEvent


def _decimal_text(value) -> str:
    if value.source_text:
        parsed = CanonicalDecimal.from_text(value.source_text)
        coefficient = (
            value.mantissa_text
            if value.WhichOneof("coefficient") == "mantissa_text"
            else value.mantissa
        )
        if str(parsed.coefficient) != str(coefficient) or parsed.scale != value.scale:
            raise ValueError("canonical decimal text differs from coefficient/scale")
        return value.source_text
    coefficient = (
        value.mantissa_text
        if value.WhichOneof("coefficient") == "mantissa_text"
        else value.mantissa
    )
    return format(
        CanonicalDecimal(coefficient, value.scale, str(coefficient)).as_decimal(),
        "f",
    )


def _interval_ns(interval: str) -> int:
    units = {
        "s": 1_000_000_000,
        "m": 60 * 1_000_000_000,
        "h": 60 * 60 * 1_000_000_000,
        "d": 24 * 60 * 60 * 1_000_000_000,
    }
    if not interval or interval[-1] not in units:
        raise ValueError("stable BAR interval is unsupported")
    count = int(interval[:-1])
    if count <= 0:
        raise ValueError("stable BAR interval must be positive")
    return count * units[interval[-1]]


def _vn_calendar(calendar_id: str) -> TradingCalendar:
    if not calendar_id.startswith("VN_"):
        raise ValueError(f"unknown non-continuous calendar: {calendar_id}")
    return TradingCalendar(
        calendar_id=calendar_id,
        timezone_name="Asia/Ho_Chi_Minh",
        weekly_sessions=(
            MarketSession(wall_time(9, 0), wall_time(11, 30), "MORNING"),
            MarketSession(wall_time(13, 0), wall_time(14, 30), "AFTERNOON"),
            MarketSession(wall_time(14, 45), wall_time(14, 46), "ATC_CLOSE"),
        ),
    )


def _quality_flag_names(envelope: market_data_pb2.EventEnvelope) -> tuple[str, ...]:
    return tuple(
        common_pb2.QualityFlag.Name(value).removeprefix("QUALITY_FLAG_")
        for value in envelope.quality_flags
    )


class StableSpoolQueryBackend:
    """Provider-neutral stable query view over a Kafka-rebuildable SQLite cache."""

    def __init__(
        self,
        spool: SQLiteDurableSpool,
        catalog: StableSourceCatalog,
        *,
        schema_digest: str,
        config_revision: int = 1,
        clock_ns=time.time_ns,
    ) -> None:
        if len(schema_digest) != 64:
            raise ValueError("stable query schema digest must be SHA-256")
        self.spool = spool
        self.catalog = catalog
        self.schema_digest = schema_digest
        self.config_revision = config_revision
        self._clock_ns = clock_ns

    def latest(self, requirement: DataRequirement) -> MarketDataItem | None:
        records = self._records(requirement)
        items = self._items(requirement, records)
        return items[-1] if items else None

    def history(self, requirement: DataRequirement) -> HistoryResult | None:
        all_records = self._records(requirement)
        if not all_records:
            return None
        requested = requirement.warmup_limit or 1
        binding = self.catalog.binding_for(requirement)
        gap_open = bool(self._gaps(binding, all_records))
        records = all_records[-requested:]
        items = self._items(requirement, records, gap_open=gap_open)
        last = records[-1]
        snapshot_hash = hashlib.sha256(
            f"{last.cursor.stream}|{last.cursor.partition_key}|{last.cursor.offset}|"
            f"{last.event.event_id.hex()}".encode()
        ).hexdigest()
        return HistoryResult(
            items=items,
            coverage=(
                CoverageStatus.FULL
                if len(items) >= requested
                else CoverageStatus.PARTIAL
            ),
            snapshot_id=f"qdl-v2-{snapshot_hash[:32]}",
            stream_cursor="CONSUMER_CURSOR_PENDING",
            watermark_offset=last.cursor.offset,
            data_as_of_ns=items[-1].observed_at_ns,
        )

    def feed_status(self, requirement: DataRequirement) -> QualityMetadata | None:
        item = self.latest(requirement)
        return item.quality if item else None

    def open_gaps(self) -> tuple[GapRecord, ...]:
        gaps = []
        for binding in self.catalog.bindings:
            records = tuple(self.spool.read(
                stream=binding.canonical_stream,
                partition_key=binding.partition_key,
                limit=10_000,
            ))
            gaps.extend(self._gaps(binding, records))
        return tuple(sorted(gaps, key=lambda item: (item.detected_at_ns, item.gap_id)))

    def stored_events(self, requirement: DataRequirement) -> tuple[StoredEvent, ...]:
        records = self._records(requirement)
        requested = requirement.warmup_limit or 1
        return records[-requested:]

    def _records(self, requirement: DataRequirement) -> tuple[StoredEvent, ...]:
        binding = self.catalog.binding_for(requirement)
        rows = self.spool.read(
            stream=binding.canonical_stream,
            partition_key=binding.partition_key,
            limit=10_000,
        )
        selected = []
        for row in rows:
            envelope = market_data_pb2.EventEnvelope.FromString(row.event.payload)
            self.catalog.binding_for_envelope(envelope)
            if binding.feed is not FeedType.BAR or envelope.bar.interval == binding.interval:
                selected.append(row)
        return tuple(selected)

    def _items(
        self,
        requirement: DataRequirement,
        records: tuple[StoredEvent, ...],
        *,
        gap_open: bool | None = None,
    ) -> tuple[MarketDataItem, ...]:
        if not records:
            return ()
        binding = self.catalog.binding_for(requirement)
        effective_gap = (
            bool(self._gaps(binding, records)) if gap_open is None else gap_open
        )
        return tuple(
            self._item(binding, stored, market_data_pb2.EventEnvelope.FromString(
                stored.event.payload
            ), effective_gap)
            for stored in records
        )

    def _quality(
        self,
        binding: StableSourceBinding,
        envelope: market_data_pb2.EventEnvelope,
        *,
        gap_open: bool,
    ) -> QualityMetadata:
        observed_ns = (
            envelope.bar.close_time_ns
            if envelope.WhichOneof("payload") == "bar"
            else envelope.source_event_time_ns
        )
        freshness_ms = max(0, (self._clock_ns() - observed_ns) // 1_000_000)
        flags = _quality_flag_names(envelope)
        explicit_gap = any(
            value in {"SEQUENCE_GAP_BEFORE", "OUT_OF_ORDER", "RESYNC_REQUIRED"}
            for value in flags
        )
        gap_open = gap_open or explicit_gap
        market_closed = False
        if not binding.continuous_calendar:
            market_closed = not _vn_calendar(
                binding.instrument.session_calendar_id
            ).is_open_ns(self._clock_ns())
        stale = freshness_ms > binding.stale_after_ms
        if market_closed:
            state = "MARKET_CLOSED"
        elif gap_open:
            state = "GAPPED"
        elif stale:
            state = "STALE"
        else:
            state = "LIVE"
        complete = not gap_open
        execution_eligible = (
            binding.authoritative
            and binding.source_role == "PRIMARY"
            and state == "LIVE"
            and complete
        )
        return QualityMetadata(
            state=state,
            freshness_ms=int(freshness_ms),
            gap_open=gap_open,
            complete=complete,
            execution_eligible=execution_eligible,
            policy_id=binding.source_policy_id,
            flags=flags + (("MARKET_CLOSED",) if market_closed else ()),
        )

    def _gaps(
        self,
        binding: StableSourceBinding,
        records: tuple[StoredEvent, ...],
    ) -> tuple[GapRecord, ...]:
        detected_at_ns = self._clock_ns()
        result = []
        for stored in records:
            envelope = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
            if common_pb2.QUALITY_FLAG_SEQUENCE_GAP_BEFORE in envelope.quality_flags:
                result.append(self._gap(
                    binding,
                    f"sequence:{envelope.source_sequence}",
                    envelope.source_sequence,
                    detected_at_ns,
                ))
        if binding.feed is not FeedType.BAR or not binding.continuous_calendar:
            return tuple(result)
        opens = sorted({
            market_data_pb2.EventEnvelope.FromString(item.event.payload).bar.open_time_ns
            for item in records
        })
        step = _interval_ns(binding.interval or "")
        for previous, current in zip(opens, opens[1:], strict=False):
            expected = previous + step
            if current != expected:
                result.append(self._gap(
                    binding, str(expected), str(current), detected_at_ns
                ))
        return tuple(result)

    @staticmethod
    def _gap(
        binding: StableSourceBinding,
        expected: str,
        observed: str,
        detected_at_ns: int,
    ) -> GapRecord:
        digest = hashlib.sha256(
            f"{binding.binding_id}|{expected}|{observed}".encode()
        ).hexdigest()[:24]
        return GapRecord(
            gap_id=f"stable-{digest}",
            instrument_uid=binding.instrument.instrument_uid,
            feed=binding.feed,
            source_id=binding.source_id,
            expected_sequence=expected,
            observed_sequence=observed,
            detected_at_ns=detected_at_ns,
        )

    def _item(
        self,
        binding: StableSourceBinding,
        stored: StoredEvent,
        envelope: market_data_pb2.EventEnvelope,
        gap_open: bool,
    ) -> MarketDataItem:
        payload_name = envelope.WhichOneof("payload")
        quality = self._quality(binding, envelope, gap_open=gap_open)
        source_role = common_pb2.SourceRole.Name(envelope.source_role).removeprefix(
            "SOURCE_ROLE_"
        )
        common = dict(
            instrument_uid=envelope.instrument_uid,
            instrument_id=envelope.instrument_id,
            instrument_revision=int(envelope.instrument_revision),
            observed_at_ns=int(envelope.source_event_time_ns),
            source=SourceMetadata(
                venue=envelope.venue,
                provider=envelope.provider,
                source_id=envelope.source_id,
                source_role=source_role,
                authoritative=binding.authoritative,
            ),
            quality=quality,
            contract=ContractMetadata(
                schema_digest=self.schema_digest,
                contract_version="2.0.0",
                normalizer_version=envelope.normalizer_version,
                adapter_version=envelope.adapter_version,
                instrument_catalog_revision=self.catalog.catalog_revision,
                source_policy_revision=self.catalog.source_policy_revision,
                authority_revision=self.catalog.authority_revision,
                config_revision=max(1, int(envelope.config_revision or self.config_revision)),
                correlation_id=envelope.correlation_id or stored.event.event_id.hex(),
            ),
            watermark_offset=stored.cursor.offset,
        )
        if payload_name == "trade":
            side = common_pb2.AggressorSide.Name(
                envelope.trade.aggressor_side
            ).removeprefix("AGGRESSOR_SIDE_")
            if side == "UNSPECIFIED":
                side = "UNKNOWN"
            identity = market_data_pb2.TradeIdentityKind.Name(
                envelope.trade.identity_kind
            ).removeprefix("TRADE_IDENTITY_KIND_")
            return MarketDataItem(
                feed=FeedType.TRADE,
                payload={
                    "native_trade_id": envelope.trade.native_trade_id,
                    "price": _decimal_text(envelope.trade.price),
                    "quantity": _decimal_text(envelope.trade.quantity),
                    "quantity_unit": quantity_unit_name(envelope.trade.quantity_unit),
                    "aggressor_side": side,
                    "identity_kind": identity,
                    "is_block_trade": bool(envelope.trade.is_block_trade),
                    "is_buyer_maker": bool(envelope.trade.is_buyer_maker),
                },
                **common,
            )
        if payload_name == "quote":
            return MarketDataItem(
                feed=FeedType.QUOTE,
                payload={
                    "bid_price": _decimal_text(envelope.quote.bid_price),
                    "bid_quantity": _decimal_text(envelope.quote.bid_quantity),
                    "ask_price": _decimal_text(envelope.quote.ask_price),
                    "ask_quantity": _decimal_text(envelope.quote.ask_quantity),
                    "quantity_unit": quantity_unit_name(envelope.quote.quantity_unit),
                    "level": int(envelope.quote.level),
                },
                **common,
            )
        if payload_name == "bar":
            lifecycle = market_data_pb2.BarLifecycle.Name(
                envelope.bar.lifecycle
            ).removeprefix("BAR_LIFECYCLE_")
            origin = common_pb2.BarOrigin.Name(envelope.bar.origin).removeprefix(
                "BAR_ORIGIN_"
            )
            return MarketDataItem(
                feed=FeedType.BAR,
                interval=envelope.bar.interval,
                revision=int(envelope.bar.revision),
                bar_lifecycle=BarLifecycle(lifecycle),
                supersedes_event_id=(
                    bytes(envelope.bar.supersedes_event_id).hex()
                    if envelope.bar.HasField("supersedes_event_id")
                    else None
                ),
                payload={
                    "open_time_ns": int(envelope.bar.open_time_ns),
                    "close_time_ns": int(envelope.bar.close_time_ns),
                    "open": _decimal_text(envelope.bar.open),
                    "high": _decimal_text(envelope.bar.high),
                    "low": _decimal_text(envelope.bar.low),
                    "close": _decimal_text(envelope.bar.close),
                    "volume": _decimal_text(envelope.bar.volume),
                    "volume_unit": quantity_unit_name(envelope.bar.volume_unit),
                    "base_volume": (
                        _decimal_text(envelope.bar.base_volume)
                        if envelope.bar.HasField("base_volume")
                        else None
                    ),
                    "quote_volume": (
                        _decimal_text(envelope.bar.quote_volume)
                        if envelope.bar.HasField("quote_volume")
                        else None
                    ),
                    "contract_volume": (
                        _decimal_text(envelope.bar.contract_volume)
                        if envelope.bar.HasField("contract_volume")
                        else None
                    ),
                    "trade_count": int(envelope.bar.trade_count),
                    "origin": origin,
                    "is_final": bool(envelope.bar.is_final),
                },
                **common,
            )
        raise ValueError("stable query backend supports only TRADE/QUOTE/BAR")


class StableConsumerCursorIssuer:
    def __init__(
        self,
        handoff: GapFreeHandoff,
        catalog: StableSourceCatalog,
        *,
        ttl_seconds: int,
    ) -> None:
        self.handoff = handoff
        self.catalog = catalog
        self.ttl_seconds = ttl_seconds

    def bind_item(
        self,
        requirement: DataRequirement,
        item: MarketDataItem,
        *,
        consumer_id: str,
    ) -> MarketDataItem:
        snapshot_id = self._snapshot_id(requirement, item.watermark_offset)
        token = self._issue(
            requirement, consumer_id, snapshot_id, item.watermark_offset
        )
        return replace(item, snapshot_id=snapshot_id, cursor=token)

    def bind_history(
        self,
        requirement: DataRequirement,
        history: HistoryResult,
        *,
        consumer_id: str,
    ) -> HistoryResult:
        token = self._issue(
            requirement,
            consumer_id,
            history.snapshot_id,
            history.watermark_offset,
        )
        return replace(
            history,
            stream_cursor=token,
            items=tuple(
                replace(item, snapshot_id=history.snapshot_id, cursor=token)
                for item in history.items
            ),
        )

    def _issue(
        self,
        requirement: DataRequirement,
        consumer_id: str,
        snapshot_id: str,
        offset: int,
    ) -> str:
        binding = self.catalog.binding_for(requirement)
        return self.handoff.issue(
            consumer_id=consumer_id,
            snapshot_id=snapshot_id,
            snapshot_watermark=Cursor(
                binding.canonical_stream,
                binding.partition_key,
                offset,
            ),
            ttl_seconds=self.ttl_seconds,
        ).token

    @staticmethod
    def _snapshot_id(requirement: DataRequirement, offset: int) -> str:
        digest = hashlib.sha256(
            f"{requirement.instrument_uid}|{requirement.feed.value}|"
            f"{requirement.interval or ''}|{offset}".encode()
        ).hexdigest()
        return f"qdl-v2-{digest[:32]}"


class StableGrpcSnapshotLoader:
    def __init__(
        self,
        *,
        service: V2QueryService,
        backend: StableSpoolQueryBackend,
        issuer: StableConsumerCursorIssuer,
    ) -> None:
        self.service = service
        self.backend = backend
        self.issuer = issuer

    def load(self, requirement: DataRequirement, *, consumer_id: str) -> GrpcSnapshot:
        purpose = {
            ConsumerGrade.ALPHA: AccessPurpose.INTERNAL_ALPHA,
            ConsumerGrade.RESEARCH: AccessPurpose.INTERNAL_RESEARCH,
            ConsumerGrade.EXECUTION: AccessPurpose.INTERNAL_EXECUTION,
        }[requirement.consumer_grade]
        result = self.service.warmup(requirement, purpose=purpose)
        history = self.issuer.bind_history(
            requirement, result.history, consumer_id=consumer_id
        )
        events = tuple(
            market_data_pb2.EventEnvelope.FromString(item.event.payload)
            for item in self.backend.stored_events(requirement)
        )
        return GrpcSnapshot(
            request_id=result.request_id,
            snapshot_id=history.snapshot_id,
            stream_cursor=history.stream_cursor,
            data_as_of_ns=history.data_as_of_ns,
            watermark_offset=history.watermark_offset,
            events=events,
        )


def build_stable_query_stack(
    *,
    spool: SQLiteDurableSpool,
    catalog: StableSourceCatalog,
    schema_digest: str,
    handoff: GapFreeHandoff,
    cursor_ttl_seconds: int,
) -> tuple[V2QueryService, StableSpoolQueryBackend, StableConsumerCursorIssuer]:
    backend = StableSpoolQueryBackend(spool, catalog, schema_digest=schema_digest)
    service = V2QueryService(
        instruments=InstrumentQuery(catalog.instrument_registry()),
        backend=backend,
        entitlements=catalog.entitlements(),
    )
    issuer = StableConsumerCursorIssuer(
        handoff, catalog, ttl_seconds=cursor_ttl_seconds
    )
    return service, backend, issuer
