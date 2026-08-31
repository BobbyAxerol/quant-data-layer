from __future__ import annotations

import hashlib
import time
from dataclasses import replace

from qdl.adapters.intervals import (
    canonical_interval_ms,
    latest_closed_boundary_ms,
)
from qdl.common.v1 import common_pb2
from qdl.domain.calendar import trading_calendar_for_id
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
    RecoveryPolicy,
    SourceMetadata,
    StalePolicy,
    V2QueryService,
)
from qdl.query.results import NON_REPLAYABLE_STREAM_CURSOR
from qdl.replay import GapFreeHandoff
from qdl.runtime.stable_catalog import (
    StableSourceBinding,
    StableSourceCatalog,
    canonical_payload_interval,
)
from qdl.runtime.session_liveness import StableSessionLivenessReader
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
    try:
        return canonical_interval_ms(interval) * 1_000_000
    except ValueError as error:
        raise ValueError(f"stable BAR interval is unsupported: {interval}") from error


def _quality_flag_names(envelope: market_data_pb2.EventEnvelope) -> tuple[str, ...]:
    return tuple(
        common_pb2.QualityFlag.Name(value).removeprefix("QUALITY_FLAG_")
        for value in envelope.quality_flags
    )


def _metric_unit_name(value: int) -> str:
    name = market_data_pb2.MetricUnit.Name(value)
    if name == "METRIC_UNIT_UNSPECIFIED":
        raise ValueError("canonical metric unit cannot be UNSPECIFIED")
    return name.removeprefix("METRIC_UNIT_")


def _book_level_fields(values) -> list[dict[str, object]]:
    return [
        {
            "side": common_pb2.BookSide.Name(value.side).removeprefix("BOOK_SIDE_"),
            "price": _decimal_text(value.price),
            "quantity": _decimal_text(value.quantity),
            "quantity_unit": quantity_unit_name(value.quantity_unit),
            "order_count": int(value.order_count),
        }
        for value in values
    ]


def bar_item_fields(
    envelope: market_data_pb2.EventEnvelope,
) -> dict[str, Any]:
    """Return the feed-specific `MarketDataItem` fields for a canonical bar.

    Shared so the spool-backed backend and the provider pass-through cannot
    drift on decimal text, quantity units or lifecycle naming, which is exactly
    where a second implementation would go wrong quietly.
    """
    lifecycle = market_data_pb2.BarLifecycle.Name(
        envelope.bar.lifecycle
    ).removeprefix("BAR_LIFECYCLE_")
    origin = common_pb2.BarOrigin.Name(envelope.bar.origin).removeprefix(
        "BAR_ORIGIN_"
    )
    return {
        "feed": FeedType.BAR,
        "interval": envelope.bar.interval,
        "revision": int(envelope.bar.revision),
        "bar_lifecycle": BarLifecycle(lifecycle),
        "supersedes_event_id": (
            bytes(envelope.bar.supersedes_event_id).hex()
            if envelope.bar.HasField("supersedes_event_id")
            else None
        ),
        "payload": {
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
    }


class StableSpoolQueryBackend:
    """Provider-neutral stable query view over a Kafka-rebuildable SQLite cache."""

    def __init__(
        self,
        spool: SQLiteDurableSpool,
        catalog: StableSourceCatalog,
        *,
        schema_digest: str,
        config_revision: int = 1,
        session_liveness_root: str | None = None,
        clock_ns=time.time_ns,
    ) -> None:
        if len(schema_digest) != 64:
            raise ValueError("stable query schema digest must be SHA-256")
        self.spool = spool
        self.catalog = catalog
        self.schema_digest = schema_digest
        self.config_revision = config_revision
        self._session_liveness = (
            StableSessionLivenessReader(session_liveness_root)
            if session_liveness_root is not None
            else None
        )
        self._clock_ns = clock_ns

    def latest(self, requirement: DataRequirement) -> MarketDataItem | None:
        binding = self.catalog.binding_for(requirement)
        records = self._records(
            requirement, limit=10_000 if requirement.feed is FeedType.BAR else 1
        )
        if not records:
            return None
        if binding.feed is FeedType.BAR:
            requested, _start_ns, _end_ns, _expected_opens = self._requested_window(
                requirement
            )
            # A BAR's execution quality is scoped to its declared warmup
            # horizon. Retained late backfills outside that horizon remain
            # observable through open_gaps(), but cannot poison fresh data.
            quality_records = records[-max(2, requested):]
        else:
            quality_records = records[-1:]
        self._validate_records(binding, quality_records)
        items = self._items(requirement, quality_records)
        return items[-1] if items else None

    def history(self, requirement: DataRequirement) -> HistoryResult | None:
        requested, start_ns, end_ns, expected_opens = self._requested_window(requirement)
        binding = self.catalog.binding_for(requirement)
        # A provider history repair can be appended after newer live BARs. Read
        # the bounded retained BAR window before selecting the market-time tail;
        # selecting logical append offsets first can manufacture a false gap.
        read_limit = (
            10_000
            if start_ns is not None or binding.feed is FeedType.BAR
            else requested
        )
        all_records = self._records(requirement, limit=read_limit)
        if not all_records:
            return None
        records = all_records
        if start_ns is not None:
            records = tuple(
                stored
                for stored in all_records
                if start_ns
                <= market_data_pb2.EventEnvelope.FromString(
                    stored.event.payload
                ).bar.open_time_ns
                < end_ns
            )
        records = records[-requested:]
        if not records:
            return None
        self._validate_records(binding, records)
        gap_open = bool(self._gaps(binding, records))
        items = self._items(requirement, records, gap_open=gap_open)
        # Logical offsets are append order, not market chronology. A bounded
        # provider backfill may legitimately append older final bars after live
        # ones, so the handoff cursor must fence the greatest durable offset
        # while the returned BAR window stays ordered by open time.
        last = max(records, key=lambda item: item.cursor.offset)
        snapshot_hash = hashlib.sha256(
            f"{last.cursor.stream}|{last.cursor.partition_key}|{last.cursor.offset}|"
            f"{last.event.event_id.hex()}".encode()
        ).hexdigest()
        exact_boundary = True
        if start_ns is not None and items:
            observed_opens = tuple(
                int(item.payload["open_time_ns"]) for item in items
            )
            exact_boundary = (
                observed_opens == expected_opens
                if expected_opens is not None
                else (
                    observed_opens[0] == start_ns
                    and observed_opens[-1]
                    + _interval_ns(requirement.interval or "")
                    == end_ns
                )
            )
        full = len(items) == requested and not gap_open and exact_boundary
        return HistoryResult(
            items=items,
            coverage=CoverageStatus.FULL if full else CoverageStatus.PARTIAL,
            snapshot_id=f"qdl-v2-{snapshot_hash[:32]}",
            stream_cursor="CONSUMER_CURSOR_PENDING",
            watermark_offset=last.cursor.offset,
            data_as_of_ns=(
                int(items[-1].payload["close_time_ns"])
                if binding.feed is FeedType.BAR
                else items[-1].observed_at_ns
            ),
        )

    def feed_status(self, requirement: DataRequirement) -> QualityMetadata | None:
        item = self.latest(requirement)
        return item.quality if item else None

    def open_gaps(self) -> tuple[GapRecord, ...]:
        gaps = []
        for binding in self.catalog.bindings:
            records = tuple(self.spool.read_tail(
                stream=binding.canonical_stream,
                partition_key=binding.partition_key,
                limit=10_000,
            ))
            gaps.extend(self._gaps(binding, records))
        return tuple(sorted(gaps, key=lambda item: (item.detected_at_ns, item.gap_id)))

    def stored_events(self, requirement: DataRequirement) -> tuple[StoredEvent, ...]:
        requested, start_ns, end_ns, _ = self._requested_window(requirement)
        binding = self.catalog.binding_for(requirement)
        rows = self._records(
            requirement,
            limit=(
                10_000
                if start_ns is not None or binding.feed is FeedType.BAR
                else requested
            ),
        )
        if start_ns is None:
            selected = rows[-requested:]
        else:
            selected = tuple(
                stored
                for stored in rows
                if start_ns
                <= market_data_pb2.EventEnvelope.FromString(
                    stored.event.payload
                ).bar.open_time_ns
                < end_ns
            )
        self._validate_records(binding, selected)
        return selected

    def _validate_records(
        self,
        binding: StableSourceBinding,
        records: tuple[StoredEvent, ...],
    ) -> None:
        """Fail closed on lineage mismatch within the returned data window."""

        for stored in records:
            resolved = self.catalog.binding_for_envelope(
                market_data_pb2.EventEnvelope.FromString(stored.event.payload)
            )
            if resolved.binding_id != binding.binding_id:
                raise ValueError("canonical event resolves to a different stable binding")

    def _requested_window(
        self,
        requirement: DataRequirement,
    ) -> tuple[int, int | None, int | None, tuple[int, ...] | None]:
        specification = requirement.warmup_specification
        if specification is None:
            return 1, None, None, None
        if specification.rows is not None:
            return specification.rows, None, None, None
        if requirement.feed is not FeedType.BAR or not requirement.interval:
            raise ValueError("time-range warmup requires a BAR interval")
        binding = self.catalog.binding_for(requirement)
        interval_ns = _interval_ns(requirement.interval)
        latest_boundary_ns = latest_closed_boundary_ms(
            requirement.interval,
            self._clock_ns() // 1_000_000,
            provider=binding.instrument.identity.venue,
        ) * 1_000_000
        assert specification.time_range is not None
        start_ns = specification.time_range.start_time_ns
        end_ns = specification.time_range.end_time_ns
        if end_ns > latest_boundary_ns:
            raise ValueError("warmup time range includes an unfinished bar")
        expected_opens = None
        if binding.continuous_calendar:
            start_ns, end_ns, rows = specification.resolved_window(
                interval_ns=interval_ns,
                latest_closed_boundary_ns=latest_boundary_ns,
            )
        else:
            calendar = trading_calendar_for_id(
                binding.instrument.session_calendar_id
            )
            expected_opens = calendar.bar_opens_between_ns(
                start_ns=start_ns,
                end_ns=end_ns,
                interval_ns=interval_ns,
                max_rows=10_000,
            )
            rows = len(expected_opens)
            if rows < 1:
                raise ValueError("warmup time range contains no governed session bars")
        if rows > 10_000:
            raise ValueError("stable spool time range exceeds bounded query rows")
        return rows, start_ns, end_ns, expected_opens

    def _records(
        self, requirement: DataRequirement, *, limit: int
    ) -> tuple[StoredEvent, ...]:
        binding = self.catalog.binding_for(requirement)
        # BOOK_SNAPSHOT and BOOK_DELTA deliberately share one physical
        # partition for replay ordering.  A one-row ``latest`` read can
        # therefore land on a delta and incorrectly report that the most
        # recent verified snapshot does not exist.  Scan a bounded physical
        # tail before applying the public logical-feed filter.  The runtime
        # refreshes Binance anchors at most every 30 seconds and this cap is
        # explicit; it is not an unbounded recovery scan.
        physical_limit = limit
        if binding.feed in {FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA}:
            physical_limit = min(10_000, max(limit, limit * 512))
        rows = self.spool.read_tail(
            stream=binding.canonical_stream,
            partition_key=binding.partition_key,
            limit=physical_limit,
        )
        selected = []
        for row in rows:
            envelope = market_data_pb2.EventEnvelope.FromString(row.event.payload)
            # BOOK_SNAPSHOT and BOOK_DELTA intentionally share a durable
            # partition.  Keep the public logical feed exact at the query
            # boundary so a snapshot read can never return a delta (or vice
            # versa) merely because both belong to the same physical book.
            if (
                envelope.WhichOneof("payload") == binding.feed.value.lower()
                and canonical_payload_interval(envelope) == binding.interval
            ):
                selected.append(row)
        if binding.feed is FeedType.BAR:
            selected.sort(key=lambda item: (
                market_data_pb2.EventEnvelope.FromString(
                    item.event.payload
                ).bar.open_time_ns,
                item.cursor.offset,
            ))
        # ``read_tail`` is chronological. Keep only the requested logical
        # tail after filtering the shared physical book partition so callers
        # retain the same bounded/latest semantics as every other feed.
        return tuple(selected[-limit:])

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
            self._item(
                requirement,
                binding,
                stored,
                market_data_pb2.EventEnvelope.FromString(stored.event.payload),
                effective_gap,
            )
            for stored in records
        )

    def _quality(
        self,
        requirement: DataRequirement,
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
            market_closed = not trading_calendar_for_id(
                binding.instrument.session_calendar_id
            ).is_open_ns(self._clock_ns())
        event_limit_ms = (
            binding.stale_after_ms
            if requirement.max_freshness_ms is None
            else min(binding.stale_after_ms, requirement.max_freshness_ms)
        )
        event_stale = freshness_ms > event_limit_ms
        event_recency_state = "STALE" if event_stale else "LIVE"
        session_state = "NOT_APPLICABLE"
        session_liveness_ms = None
        session_flags: tuple[str, ...] = ()
        if requirement.max_session_liveness_ms is not None:
            session_state = "UNKNOWN"
            if self._session_liveness is None:
                session_flags = ("SOURCE_SESSION_UNAVAILABLE",)
            else:
                status = self._session_liveness.status(
                    venue=envelope.venue,
                    market=envelope.market,
                    source_session_id=envelope.source_session_id,
                    connection_generation=int(envelope.connection_generation),
                    config_revision=max(
                        1, int(envelope.config_revision or self.config_revision)
                    ),
                    now_ns=self._clock_ns(),
                )
                session_state = status.state
                session_liveness_ms = status.liveness_ms
                session_flags = status.flags
                if (
                    session_state == "LIVE"
                    and session_liveness_ms is not None
                    and session_liveness_ms > requirement.max_session_liveness_ms
                ):
                    session_state = "STALE"
                    session_flags += ("SOURCE_SESSION_HEARTBEAT_EXPIRED",)
        payload_name = envelope.WhichOneof("payload")
        book_unverified = payload_name in {"book_snapshot", "book_delta"} and not (
            bool(getattr(envelope, payload_name).sequence_verified)
            and int(getattr(envelope, payload_name).book_generation) >= 1
        )
        if book_unverified:
            flags = flags + ("BOOK_SEQUENCE_UNVERIFIED",)
        if market_closed:
            state = "MARKET_CLOSED"
        elif gap_open:
            state = "GAPPED"
        elif book_unverified:
            state = "SYNCING"
        elif session_state in {"STALE", "DISCONNECTED", "UNKNOWN"}:
            state = "STALE"
        elif (
            event_stale
            and requirement.effective_event_recency_policy
            in {StalePolicy.BLOCK, StalePolicy.PAUSE}
        ):
            state = "STALE"
        else:
            state = "LIVE"
        complete = not gap_open and not book_unverified
        execution_eligible = (
            binding.authoritative
            and binding.source_role == "PRIMARY"
            and state == "LIVE"
            and complete
            and event_recency_state != "STALE"
            and session_state in {"LIVE", "NOT_APPLICABLE"}
        )
        return QualityMetadata(
            state=state,
            freshness_ms=int(freshness_ms),
            gap_open=gap_open,
            complete=complete,
            execution_eligible=execution_eligible,
            policy_id=binding.source_policy_id,
            flags=(
                flags
                + session_flags
                + (("LAST_EVENT_STALE",) if event_stale else ())
                + (("MARKET_CLOSED",) if market_closed else ())
            ),
            event_recency_state=event_recency_state,
            provider_session_state=session_state,
            provider_session_liveness_ms=session_liveness_ms,
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
        if binding.feed is not FeedType.BAR:
            return tuple(result)
        opens = sorted({
            market_data_pb2.EventEnvelope.FromString(item.event.payload).bar.open_time_ns
            for item in records
        })
        if not opens:
            return tuple(result)
        step = _interval_ns(binding.interval or "")
        if binding.continuous_calendar:
            expected_opens = tuple(
                range(opens[0], opens[-1] + step, step)
            )
        else:
            calendar = trading_calendar_for_id(
                binding.instrument.session_calendar_id
            )
            expected_opens = calendar.bar_opens_between_ns(
                start_ns=opens[0],
                end_ns=opens[-1] + step,
                interval_ns=step,
            )
        observed = set(opens)
        for expected in expected_opens:
            if expected not in observed:
                result.append(self._gap(
                    binding, str(expected), "MISSING", detected_at_ns
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
        requirement: DataRequirement,
        binding: StableSourceBinding,
        stored: StoredEvent,
        envelope: market_data_pb2.EventEnvelope,
        gap_open: bool,
    ) -> MarketDataItem:
        payload_name = envelope.WhichOneof("payload")
        quality = self._quality(requirement, binding, envelope, gap_open=gap_open)
        source_role = common_pb2.SourceRole.Name(envelope.source_role).removeprefix(
            "SOURCE_ROLE_"
        )
        common = dict(
            instrument_uid=envelope.instrument_uid,
            instrument_id=envelope.instrument_id,
            instrument_revision=int(envelope.instrument_revision),
            observed_at_ns=int(envelope.source_event_time_ns),
            received_at_ns=max(1, int(envelope.received_at_ns)),
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
            return MarketDataItem(**bar_item_fields(envelope), **common)
        if payload_name == "book_snapshot":
            return MarketDataItem(
                feed=FeedType.BOOK_SNAPSHOT,
                payload={
                    "native_sequence": envelope.book_snapshot.native_sequence,
                    "checksum": envelope.book_snapshot.checksum or None,
                    "levels": _book_level_fields(envelope.book_snapshot.levels),
                    "depth": int(envelope.book_snapshot.depth),
                    "book_generation": int(envelope.book_snapshot.book_generation),
                    "sequence_verified": bool(envelope.book_snapshot.sequence_verified),
                    "truncated": bool(envelope.book_snapshot.truncated),
                },
                **common,
            )
        if payload_name == "book_delta":
            return MarketDataItem(
                feed=FeedType.BOOK_DELTA,
                payload={
                    "native_sequence_start": envelope.book_delta.native_sequence_start,
                    "native_sequence_end": envelope.book_delta.native_sequence_end,
                    "snapshot_sequence": envelope.book_delta.snapshot_sequence,
                    "checksum": envelope.book_delta.checksum or None,
                    "updates": _book_level_fields(envelope.book_delta.updates),
                    "reset": bool(envelope.book_delta.reset),
                    "book_generation": int(envelope.book_delta.book_generation),
                    "sequence_verified": bool(envelope.book_delta.sequence_verified),
                },
                **common,
            )
        if payload_name == "funding_rate":
            return MarketDataItem(
                feed=FeedType.FUNDING_RATE,
                payload={
                    "rate": _decimal_text(envelope.funding_rate.rate),
                    "funding_time_ns": int(envelope.funding_rate.funding_time_ns),
                    "next_funding_time_ns": (
                        int(envelope.funding_rate.next_funding_time_ns)
                        if envelope.funding_rate.HasField("next_funding_time_ns")
                        else None
                    ),
                },
                **common,
            )
        if payload_name == "open_interest":
            return MarketDataItem(
                feed=FeedType.OPEN_INTEREST,
                interval=envelope.open_interest.sampling_interval or None,
                payload={
                    "quantity": _decimal_text(envelope.open_interest.quantity),
                    "quantity_unit": quantity_unit_name(envelope.open_interest.quantity_unit),
                    "notional": (
                        _decimal_text(envelope.open_interest.notional)
                        if envelope.open_interest.HasField("notional")
                        else None
                    ),
                    "sampling_interval": envelope.open_interest.sampling_interval or None,
                },
                **common,
            )
        if payload_name == "mark_index_price":
            return MarketDataItem(
                feed=FeedType.MARK_INDEX_PRICE,
                payload={
                    "mark_price": _decimal_text(envelope.mark_index_price.mark_price),
                    "index_price": _decimal_text(envelope.mark_index_price.index_price),
                },
                **common,
            )
        if payload_name == "long_short_ratio":
            return MarketDataItem(
                feed=FeedType.LONG_SHORT_RATIO,
                interval=envelope.long_short_ratio.sampling_interval,
                payload={
                    "population": market_data_pb2.LongShortRatioPopulation.Name(
                        envelope.long_short_ratio.population
                    ).removeprefix("LONG_SHORT_RATIO_POPULATION_"),
                    "sampling_interval": envelope.long_short_ratio.sampling_interval,
                    "long_value": _decimal_text(envelope.long_short_ratio.long_value),
                    "short_value": _decimal_text(envelope.long_short_ratio.short_value),
                    "long_short_ratio": _decimal_text(envelope.long_short_ratio.long_short_ratio),
                    "value_unit": _metric_unit_name(envelope.long_short_ratio.value_unit),
                },
                **common,
            )
        if payload_name == "taker_flow":
            return MarketDataItem(
                feed=FeedType.TAKER_FLOW,
                interval=envelope.taker_flow.sampling_interval,
                payload={
                    "sampling_interval": envelope.taker_flow.sampling_interval,
                    "buy_volume": _decimal_text(envelope.taker_flow.buy_volume),
                    "sell_volume": _decimal_text(envelope.taker_flow.sell_volume),
                    "buy_sell_ratio": _decimal_text(envelope.taker_flow.buy_sell_ratio),
                    "quantity_unit": quantity_unit_name(envelope.taker_flow.quantity_unit),
                },
                **common,
            )
        if payload_name == "basis":
            return MarketDataItem(
                feed=FeedType.BASIS,
                interval=envelope.basis.sampling_interval,
                payload={
                    "kind": market_data_pb2.BasisKind.Name(
                        envelope.basis.kind
                    ).removeprefix("BASIS_KIND_"),
                    "sampling_interval": envelope.basis.sampling_interval,
                    "basis": _decimal_text(envelope.basis.basis),
                    "basis_unit": _metric_unit_name(envelope.basis.basis_unit),
                    "annualized_basis": (
                        _decimal_text(envelope.basis.annualized_basis)
                        if envelope.basis.HasField("annualized_basis")
                        else None
                    ),
                    "reference_instrument_uid": envelope.basis.reference_instrument_uid,
                    "formula_id": envelope.basis.formula_id,
                    "input_instrument_uids": list(envelope.basis.input_instrument_uids),
                },
                **common,
            )
        if payload_name == "contract_metadata":
            return MarketDataItem(
                feed=FeedType.CONTRACT_METADATA,
                payload={
                    "contract_kind": envelope.contract_metadata.contract_kind,
                    "settlement_asset": envelope.contract_metadata.settlement_asset,
                    "contract_multiplier": _decimal_text(
                        envelope.contract_metadata.contract_multiplier
                    ),
                    "price_tick": _decimal_text(envelope.contract_metadata.price_tick),
                    "quantity_step": _decimal_text(envelope.contract_metadata.quantity_step),
                    "expiry_time_ns": (
                        int(envelope.contract_metadata.expiry_time_ns)
                        if envelope.contract_metadata.HasField("expiry_time_ns")
                        else None
                    ),
                    "funding_interval_ns": (
                        int(envelope.contract_metadata.funding_interval_ns)
                        if envelope.contract_metadata.HasField("funding_interval_ns")
                        else None
                    ),
                    "continuous": bool(envelope.contract_metadata.continuous),
                    "underlying_instrument_uid": envelope.contract_metadata.underlying_instrument_uid,
                },
                **common,
            )
        if payload_name == "ticker":
            payload = {"last_price": _decimal_text(envelope.ticker.last_price)}
            for field in ("last_quantity", "open_24h", "high_24h", "low_24h", "volume_24h"):
                payload[field] = (
                    _decimal_text(getattr(envelope.ticker, field))
                    if envelope.ticker.HasField(field)
                    else None
                )
            payload["last_quantity_unit"] = (
                quantity_unit_name(envelope.ticker.last_quantity_unit)
                if envelope.ticker.HasField("last_quantity")
                else None
            )
            payload["volume_24h_unit"] = (
                quantity_unit_name(envelope.ticker.volume_24h_unit)
                if envelope.ticker.HasField("volume_24h")
                else None
            )
            return MarketDataItem(feed=FeedType.TICKER, payload=payload, **common)
        raise ValueError(f"stable query backend has no payload projection for {payload_name}")


class StableCatalogCursorScopeValidator:
    """Bind public cursor scope to the exact stable catalog requirement."""

    def __init__(self, catalog: StableSourceCatalog) -> None:
        self.catalog = catalog

    def validate(
        self,
        requirement: DataRequirement,
        *,
        stream: str,
        partition_key: str,
    ) -> None:
        try:
            binding = self.catalog.binding_for(requirement)
        except KeyError as error:
            raise ValueError(
                "cursor requirement has no matching stable binding"
            ) from error
        if stream != binding.canonical_stream:
            raise ValueError("cursor stream does not match the stable binding")
        if partition_key != binding.partition_key:
            raise ValueError("cursor partition does not match the stable binding")


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
        if self._preserve_non_replayable(
            requirement, cursor=item.cursor, offset=item.watermark_offset
        ):
            return item
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
        if self._preserve_non_replayable(
            requirement,
            cursor=history.stream_cursor,
            offset=history.watermark_offset,
        ):
            return history
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

    @staticmethod
    def _preserve_non_replayable(
        requirement: DataRequirement, *, cursor: str | None, offset: int
    ) -> bool:
        if cursor != NON_REPLAYABLE_STREAM_CURSOR:
            return False
        if requirement.recovery is not RecoveryPolicy.FRESH_SNAPSHOT or offset != 0:
            raise ValueError(
                "non-replayable cursor requires FRESH_SNAPSHOT and zero watermark"
            )
        return True

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
    pass_through_enabled: bool = False,
    reference_data_enabled: bool = False,
    provider_admission_url: str | None = None,
    provider_admission_secret: bytes | None = None,
    session_liveness_root: str | None = None,
) -> tuple[V2QueryService, StableSpoolQueryBackend, StableConsumerCursorIssuer]:
    """Build the query stack, optionally including the pass-through product.

    The pass-through is off unless a deployment enables it, so declaring catalog
    metadata for an instrument never opens a data product by itself. When it is
    off, the registry, the entitlements and the backend are exactly what they
    were before, which keeps the default deployment unchanged.
    """
    backend = StableSpoolQueryBackend(
        spool,
        catalog,
        schema_digest=schema_digest,
        session_liveness_root=session_liveness_root,
    )
    served: MarketDataQueryBackend = backend
    if pass_through_enabled:
        from qdl.runtime.provider_history import ProviderBarHistorySource
        from qdl.runtime.routed_query import RoutedQueryBackend

        served = RoutedQueryBackend(backend, ProviderBarHistorySource(catalog))
    entitlements = catalog.entitlements(include_unbound=pass_through_enabled)
    reference_batch = None
    reference_source_id = None
    if reference_data_enabled:
        # This is deliberately independent from BAR pass-through.  It only
        # enables catalog-bound provider reference products and grants no
        # execution purpose; no catalog declaration can turn it on by itself.
        from qdl.admission import RustHttpProviderAdmission
        from qdl.reference.runtime import build_default_reference_runtime

        native_basis_admission = None
        if provider_admission_url is not None:
            if provider_admission_secret is None:
                raise ValueError("stable Rust provider admission secret is unavailable")
            native_basis_admission = RustHttpProviderAdmission(
                base_url=provider_admission_url,
                secret=provider_admission_secret,
            )
        reference_runtime = build_default_reference_runtime(
            native_basis_admission=native_basis_admission
        )
        entitlements = entitlements.with_grants(reference_runtime.entitlement_grants())
        reference_batch = reference_runtime.batch
        reference_source_id = reference_runtime.source_id_for
    service = V2QueryService(
        instruments=InstrumentQuery(
            catalog.instrument_registry(
                include_unbound=pass_through_enabled or reference_data_enabled
            )
        ),
        backend=served,
        entitlements=entitlements,
        reference_batch=reference_batch,
        reference_source_id=reference_source_id,
    )
    issuer = StableConsumerCursorIssuer(
        handoff, catalog, ttl_seconds=cursor_ttl_seconds
    )
    return service, backend, issuer
