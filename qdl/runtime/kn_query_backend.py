"""KN-4 K4.1 Query backend over the Kafka-native market cache (decisions D25-D28).

Purpose: serve the existing Query API from the market cache the Rust
projector writes, without a second semantic owner. ``KnMarketCacheQueryBackend``
subclasses ``StableSpoolQueryBackend``: request windows, record selection,
lineage validation, gap detection, quality (``evaluate_binding_quality``),
item projection and the public history/coverage rules are the unchanged
spool code. Only the record source differs: each product's view comes from
``KnMarketCacheReader`` (one READY generation, a proven source boundary),
never from SQLite or Kafka.

Semantics that differ from the spool because the cache differs (D25/D27):

* one row per BAR open (the KN-3 revision rule), so a revised BAR is never
  returned twice;
* a latest-state product (every non-BAR feed) keeps only its latest record;
  its history is that one record (every declared non-BAR requirement has
  ``warmup_limit`` 0 and the SDK consumers read ``data[-1]``);
* every item and the history carry the view's source boundary as
  ``watermark_offset`` - the SDK starts the stream handoff there - and the
  cursor placeholder ``kn3-source:<topic id>:<partition>:<offset>`` that only
  ``KnCursorV3Issuer`` turns into a signed cursor v3 (the placeholder never
  leaves the process: the issuer refuses anything else);
* ``snapshot_id`` hashes (product, topic id, partition, boundary); the cache
  generation never enters it (contract section 5).

``KnCursorV3Issuer`` signs with the Stream's own key set and expectation
(same env names as ``qdl-stream-gateway``: ``QDL_KN_CURSOR_KEYS_FILE``,
``QDL_KN_CURSOR_ACTIVE_KEY_ID``, ``QDL_KN_TOPIC_ID``,
``QDL_KN_PARTITION_PLAN_EPOCH``, ``QDL_KN_ROUTE_GENERATION``,
``QDL_KN_CURSOR_TTL_SECONDS``): Query issues, Stream verifies.

Boundary: read-only; no Kafka reader, no SQLite, no write to any Redis.
Selected by ``QDL_STABLE_QUERY_BACKEND=kn3`` (``qdl/runtime/stable.py``); the
spool stays the default until the KN-5 cutover.
"""
from __future__ import annotations

from dataclasses import dataclass, fields, replace
import hashlib
import json
from pathlib import Path
import re
import time
import threading
from typing import Callable, Mapping

from google.protobuf.message import DecodeError

from qdl.adapters.intervals import canonical_interval_ms
from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.query import CoverageStatus, DataRequirement, FeedType, GapRecord, HistoryResult, MarketDataItem, RecoveryPolicy
from qdl.query.contracts import CanonicalErrorCode, QueryProblem
from qdl.query.results import NON_REPLAYABLE_STREAM_CURSOR, QueryBackendError, GapScanResult
from qdl.query.row_cache import BoundedRowCache
from qdl.replay.cursor_v3 import CursorV3Claims, SignedCursorV3Codec, requirement_digest
from qdl.runtime.kn_hot_view import HOT_FEEDS, HotViewUnavailable
from qdl.runtime.kn_bar_readback import binding_product_key
from qdl.runtime.kn_market_cache import (
    KnCacheError,
    KnCacheIntegrityError,
    KnCacheNotReady,
    KnMarketCacheReader,
    ProductView,
)
from qdl.runtime.stable_capacity import (
    STABLE_GAP_DIAGNOSTIC_MAX_EXPECTED_BARS,
    STABLE_GAP_DIAGNOSTIC_MAX_PAGE_PAYLOAD_BYTES,
    STABLE_GAP_DIAGNOSTIC_MAX_RESULTS,
    STABLE_GAP_DIAGNOSTIC_MAX_WORK_MS,
    STABLE_GAP_DIAGNOSTIC_PAGE_ROWS,
    STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW,
)
from qdl.runtime.stable_catalog import StableSourceBinding, StableSourceCatalog
from qdl.runtime.stable_source import (
    StableSpoolQueryBackend,
    _GapDiagnosticIncomplete,
    _interval_ns,
    _ParsedStoredEvent,
)
from qdl.domain.calendar import trading_calendar_for_id
from qdl.transport import Cursor, StoredEvent
from qdl.transport.contracts import DurableEvent

PLACEHOLDER_PREFIX = "kn3-source:"
_PLACEHOLDER = re.compile(r"kn3-source:([A-Za-z0-9._@|+=-]{1,256}):([0-9]{1,10}):([0-9]{1,19})")
QUERY_BACKEND_ENV = "QDL_STABLE_QUERY_BACKEND"
QUERY_BACKENDS = ("spool", "kn3")
CURSOR_KEYS_FILE_ENV = "QDL_KN_CURSOR_KEYS_FILE"
CURSOR_ACTIVE_KEY_ENV = "QDL_KN_CURSOR_ACTIVE_KEY_ID"
TOPIC_ID_ENV = "QDL_KN_TOPIC_ID"
PARTITION_PLAN_EPOCH_ENV = "QDL_KN_PARTITION_PLAN_EPOCH"
ROUTE_GENERATION_ENV = "QDL_KN_ROUTE_GENERATION"
CURSOR_TTL_ENV = "QDL_KN_CURSOR_TTL_SECONDS"
DEFAULT_ROW_CACHE_ENTRIES = 20_000
# Item fields rebuilt per request (time/request dependent); never cached.
_DYNAMIC_ITEM_FIELDS = frozenset({"quality", "watermark_offset", "cursor", "snapshot_id", "render_key"})


@dataclass
class _RowEntry:
    """Derivations of one immutable row (D30): its envelope, whether its
    lineage was proven against the catalog binding, and its static item
    fields. Quality, cursor and watermark are not here."""

    envelope: market_data_pb2.EventEnvelope
    validated: bool = False
    static: dict | None = None


def source_placeholder(view: ProductView) -> str:
    boundary = view.boundary
    return f"{PLACEHOLDER_PREFIX}{boundary.topic_id}:{boundary.partition}:{boundary.offset}"


def parse_placeholder(value: str | None) -> tuple[str, int, int]:
    match = _PLACEHOLDER.fullmatch(value or "")
    if match is None:
        raise ValueError("kn3 Query cursor has no source coordinate to sign")
    return match.group(1), int(match.group(2)), int(match.group(3))


def view_snapshot_id(view: ProductView) -> str:
    boundary = view.boundary
    digest = hashlib.sha256(
        f"kn3|{view.lpk.encode()}|{boundary.topic_id}|{boundary.partition}|{boundary.offset}".encode()
    ).hexdigest()
    return f"qdl-v2-{digest[:32]}"


def _not_ready(detail: str) -> QueryBackendError:
    return QueryBackendError(QueryProblem(
        CanonicalErrorCode.DATA_NOT_READY, detail, True, retry_after_ms=1_000,
    ))


class KnMarketCacheQueryBackend(StableSpoolQueryBackend):
    """The stable Query semantics over the KN-3 market cache."""

    def __init__(
        self,
        reader: KnMarketCacheReader,
        catalog: StableSourceCatalog,
        *,
        schema_digest: str,
        topic_id: str,
        config_revision: int = 1,
        session_liveness_root: str | None = None,
        clock_ns=time.time_ns,
        monotonic_ns=time.monotonic_ns,
        gap_scan_max_results: int = STABLE_GAP_DIAGNOSTIC_MAX_RESULTS,
        gap_scan_max_expected_bars: int = STABLE_GAP_DIAGNOSTIC_MAX_EXPECTED_BARS,
        gap_scan_max_work_ms: int = STABLE_GAP_DIAGNOSTIC_MAX_WORK_MS,
        row_cache_entries: int = DEFAULT_ROW_CACHE_ENTRIES,
        diagnostic_exclusions: Mapping[str, str] | None = None,
        hot_client=None,
    ) -> None:
        self._diagnostic_exclusions = dict(diagnostic_exclusions or {})
        unknown = self._diagnostic_exclusions.keys() - {b.binding_id for b in catalog.bindings}
        if unknown:
            raise ValueError("diagnostic exclusions must reference catalog bindings")
        super().__init__(
            None,  # no spool: every read below goes to the market cache
            catalog,
            schema_digest=schema_digest,
            config_revision=config_revision,
            session_liveness_root=session_liveness_root,
            clock_ns=clock_ns,
            monotonic_ns=monotonic_ns,
            gap_scan_max_results=gap_scan_max_results,
            gap_scan_max_expected_bars=gap_scan_max_expected_bars,
            gap_scan_max_work_ms=gap_scan_max_work_ms,
            gap_scan_page_rows=STABLE_GAP_DIAGNOSTIC_PAGE_ROWS,
            gap_scan_max_page_payload_bytes=STABLE_GAP_DIAGNOSTIC_MAX_PAGE_PAYLOAD_BYTES,
        )
        if not topic_id or ":" in topic_id:
            raise ValueError("kn3 Query backend needs the canonical topic id")
        self.reader = reader
        self.topic_id = topic_id
        self.environment = reader.environment
        self.rows = BoundedRowCache(row_cache_entries)
        self.hot_client = hot_client
        # Bounded by the immutable catalog, coordinates only (no second cache).
        self._hot_boundaries: dict[str, tuple[object, int, str]] = {}
        self._hot_boundary_lock = threading.Lock()

    # ------------------------------------------------------------ cache view

    def product_key(self, binding: StableSourceBinding):
        return binding_product_key(binding, self.environment)

    def _view(
        self,
        binding: StableSourceBinding,
        *,
        last: int | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        check_budget: Callable[[], None] | None = None,
    ) -> ProductView | None:
        """The product's view; ``None`` when it has no state (DATA_NOT_READY)."""

        lpk = self.product_key(binding)
        try:
            if binding.feed is not FeedType.BAR:
                view = self.reader.latest(lpk)
            elif start_ns is not None:
                view = self.reader.bars(
                    lpk, canonical_interval_ms(binding.interval or ""),
                    start_ms=start_ns // 1_000_000, end_ms=end_ns // 1_000_000,
                    check_budget=check_budget,
                )
            else:
                view = self.reader.bars(
                    lpk, canonical_interval_ms(binding.interval or ""), last=last or 1,
                    check_budget=check_budget,
                )
        except KnCacheNotReady as error:
            if error.state == "SOURCE_BOUNDARY_UNKNOWN":
                raise _not_ready(
                    f"SOURCE_BOUNDARY_UNKNOWN: {lpk.encode()} has no canonical fact in the cache yet"
                ) from error
            return None
        except KnCacheIntegrityError as error:
            raise QueryBackendError(QueryProblem(
                CanonicalErrorCode.INTERNAL_ERROR, f"market cache integrity: {error}", False,
            )) from error
        except KnCacheError as error:
            raise QueryBackendError(QueryProblem(
                CanonicalErrorCode.DEPENDENCY_UNAVAILABLE, f"market cache: {error}", True,
                retry_after_ms=500,
            )) from error
        if view.boundary.topic_id != self.topic_id:
            raise _not_ready(
                f"SOURCE_TOPIC_GENERATION: {lpk.encode()} was applied from another canonical topic"
            )
        return view

    def _hot_monotonic(self, binding, view, *, record=False) -> bool:
        if view is None:
            return False
        if len(view.rows) != 1 or view.rows[0].source_offset is None:
            raise _not_ready("HOT_RECORD_COORDINATE_UNKNOWN")
        record_offset = view.rows[0].source_offset
        digest = hashlib.sha256(view.rows[0].canonical).hexdigest()
        with self._hot_boundary_lock:
            previous = self._hot_boundaries.get(binding.binding_id)
            if previous is not None:
                boundary, prior_record_offset, prior_digest = previous
                if (view.boundary.topic_id, view.boundary.partition) != (boundary.topic_id, boundary.partition):
                    raise _not_ready("HOT_SOURCE_GENERATION_MISMATCH")
                if view.boundary.offset < boundary.offset or record_offset < prior_record_offset:
                    return False
                if record_offset == prior_record_offset and digest != prior_digest:
                    raise QueryBackendError(QueryProblem(
                        CanonicalErrorCode.INTERNAL_ERROR, "HOT_SOURCE_PAYLOAD_MISMATCH", False,
                    ))
            if record:
                self._hot_boundaries[binding.binding_id] = (view.boundary, record_offset, digest)
            return True

    def _hot_candidate(self, binding, primary=None):
        """Authenticated committed bytes, validated by the same lineage oracle."""
        if self.hot_client is None or binding.feed.value not in HOT_FEEDS:
            return None
        candidate = self.hot_client.latest(binding, self.product_key(binding))
        if primary is not None:
            if candidate.boundary.topic_id != primary.boundary.topic_id or candidate.boundary.partition != primary.boundary.partition:
                raise _not_ready("HOT_SOURCE_GENERATION_MISMATCH")
            if candidate.boundary.offset < primary.boundary.offset or candidate.rows[0].source_offset < primary.rows[0].source_offset:
                raise HotViewUnavailable("HOT_BACKUP_BEHIND_PRIMARY")
            if candidate.rows[0].source_offset == primary.rows[0].source_offset and candidate.rows[0].canonical != primary.rows[0].canonical:
                raise QueryBackendError(QueryProblem(
                    CanonicalErrorCode.INTERNAL_ERROR, "HOT_SOURCE_PAYLOAD_MISMATCH", False,
                ))
        try:
            records = self._parsed(binding, candidate)
            self._validate_records(binding, records)
        except (ValueError, DecodeError) as error:
            raise QueryBackendError(QueryProblem(
                CanonicalErrorCode.INTERNAL_ERROR, "HOT_BACKUP_LINEAGE_INVALID", False,
            )) from error
        if len(records) != 1 or not self._hot_monotonic(binding, candidate):
            raise HotViewUnavailable("HOT_BACKUP_BEHIND_RETURNED_VIEW")
        return candidate

    def _latest_view(self, requirement, binding):
        primary_error = None
        try:
            primary = self._view(binding, last=1)
        except QueryBackendError as error:
            if error.problem.code is not CanonicalErrorCode.DEPENDENCY_UNAVAILABLE:
                raise
            primary_error = error
            primary = None
        if self.hot_client is None or binding.feed.value not in HOT_FEEDS:
            if primary_error is not None:
                raise primary_error
            return primary
        primary_records = self._parsed(binding, primary) if primary is not None else ()
        self._validate_records(binding, primary_records)
        items = self._items(requirement, primary_records)
        quality = items[-1].quality if items else None
        requires_execution = requirement.consumer_grade.value == "EXECUTION"
        usable = quality is not None and quality.state == "LIVE" and quality.complete and not quality.gap_open and (
            not requires_execution or quality.execution_eligible)
        monotonic = self._hot_monotonic(binding, primary)
        # A live provider session cannot prove that an event-stale cache has
        # consumed newer canonical records while its projector is unavailable.
        quiet_primary = bool(requires_execution and usable and quality.event_recency_state == "STALE")
        if usable and monotonic and not quiet_primary:
            if self._hot_monotonic(binding, primary, record=True):
                return primary
        # An explicit corrupt/gapped primary is not merely a missing cache update.
        if quality is not None and quality.gap_open:
            return primary if monotonic else None
        try:
            candidate = self._hot_candidate(binding, primary)
        except HotViewUnavailable:
            candidate = None
        if candidate is not None:
            candidate_records = self._parsed(binding, candidate)
            candidate_items = self._items(requirement, candidate_records)
            q = candidate_items[-1].quality if candidate_items else None
            if q is not None and q.state == "LIVE" and q.complete and not q.gap_open and (
                not requires_execution or q.execution_eligible
            ) and self._hot_monotonic(binding, candidate, record=True):
                return candidate
        if primary_error is not None:
            raise primary_error
        if quiet_primary:
            raise _not_ready("HOT_QUIET_PRIMARY_UNVERIFIED")
        if primary is not None and not self._hot_monotonic(binding, primary):
            raise _not_ready("HOT_BACKUP_UNAVAILABLE_PRIMARY_BEHIND")
        return primary

    @staticmethod
    def _row_key(binding: StableSourceBinding, payload_sha256: str) -> str:
        return f"{binding.binding_id}|{payload_sha256}"

    def _row(self, binding: StableSourceBinding, payload_sha256: str, canonical: bytes) -> _RowEntry:
        key = self._row_key(binding, payload_sha256)
        entry = self.rows.get(key)
        if entry is None:
            entry = _RowEntry(market_data_pb2.EventEnvelope.FromString(canonical))
            self.rows.put(key, entry)
        return entry

    def _parsed(
        self, binding: StableSourceBinding, view: ProductView
    ) -> tuple[_ParsedStoredEvent, ...]:
        cursor = Cursor(binding.canonical_stream, binding.partition_key, view.boundary.offset)
        parsed = []
        for row in view.rows:
            payload_sha256 = hashlib.sha256(row.canonical).hexdigest()
            envelope = self._row(binding, payload_sha256, row.canonical).envelope
            stored = StoredEvent(
                event=DurableEvent(
                    stream=binding.canonical_stream,
                    partition_key=binding.partition_key,
                    event_id=bytes(envelope.event_id),
                    payload=row.canonical,
                    accepted_at_ns=max(1, int(envelope.received_at_ns)),
                ),
                # D27: every item carries the view boundary (the handoff start).
                cursor=cursor,
                committed_at_ns=max(1, int(envelope.received_at_ns)),
                payload_sha256=payload_sha256,
            )
            parsed.append(_ParsedStoredEvent(stored=stored, envelope=envelope))
        return self._select_records(binding, tuple(parsed), limit=max(1, len(parsed)))

    def latest_stored_event(self, binding: StableSourceBinding, *, prefer_hot: bool = False) -> tuple[StoredEvent | None, str]:
        """The product's latest record for the MARK/INDEX views (D31/D36).

        Returns ``(stored, "OK")`` or ``(None, state)`` with a state the view
        must not paper over with a remembered price: ``NOT_READY`` (no READY
        generation or no entry - the product has no state), ``UNAVAILABLE``
        (the cache cannot answer now or its generation kept changing),
        ``INTEGRITY`` (a value failed its trailer/identity check) or
        ``FENCED`` (the entry was applied from another canonical topic
        generation or has no provable source boundary).
        """

        try:
            view = self._view(binding, last=1)
        except QueryBackendError as error:
            code = error.problem.code
            if code is CanonicalErrorCode.DATA_NOT_READY:
                return None, "FENCED"
            if code is CanonicalErrorCode.INTERNAL_ERROR:
                return None, "INTEGRITY"
            if not prefer_hot:
                return None, "UNAVAILABLE"
            view = None
        if self.hot_client is not None and (prefer_hot or (view is not None and not self._hot_monotonic(binding, view))):
            try:
                view = self._hot_candidate(binding, view)
            except HotViewUnavailable:
                return None, "BACKUP_UNAVAILABLE"
            except QueryBackendError as error:
                return None, "FENCED" if error.problem.code is CanonicalErrorCode.DATA_NOT_READY else "INTEGRITY"
        if view is None:
            return None, "NOT_READY"
        records = self._parsed(binding, view)
        if not records:
            return None, "NOT_READY"
        if self.hot_client is not None:
            try:
                self._validate_records(binding, records)
            except ValueError:
                return None, "INTEGRITY"
            if not self._hot_monotonic(binding, view, record=True):
                return None, "BACKUP_UNAVAILABLE"
        return records[-1].stored, "OK"

    # ------------------------------------------------------------ row derivations (D30)

    def _validate_records(self, binding, records) -> None:
        """Prove each row's lineage once per content; the verdict is immutable."""

        for parsed in records:
            entry = self._row(binding, parsed.stored.payload_sha256, parsed.stored.event.payload)
            if not entry.validated:
                super()._validate_records(binding, (parsed,))
                entry.validated = True

    def _item(self, requirement, binding, stored, envelope, gap_open):
        """Static fields from the row cache; quality rebuilt for this request."""

        key = self._row_key(binding, stored.payload_sha256)
        entry = self._row(binding, stored.payload_sha256, stored.event.payload)
        if entry.static is None:
            item = super()._item(requirement, binding, stored, envelope, gap_open)
            entry.static = {
                item_field.name: getattr(item, item_field.name)
                for item_field in fields(item)
                if item_field.name not in _DYNAMIC_ITEM_FIELDS
            }
            return replace(item, render_key=key)
        quality = self._quality(
            requirement, binding, envelope, gap_open=gap_open, watermark_offset=stored.cursor.offset,
        )
        return MarketDataItem(
            **entry.static, quality=quality, watermark_offset=stored.cursor.offset, render_key=key,
        )

    def warmup_stats(self) -> dict[str, int]:
        return {f"row_cache_{name}": value for name, value in self.rows.stats().items()}

    # ------------------------------------------------------------ backend API

    def latest(self, requirement: DataRequirement) -> MarketDataItem | None:
        binding = self.catalog.binding_for(requirement)
        requested = 1
        if binding.feed is FeedType.BAR:
            requested, _start, _end, _opens = self._requested_window(requirement)
        view = (self._view(binding, last=max(2, requested)) if binding.feed is FeedType.BAR
                else self._latest_view(requirement, binding))
        if view is None:
            return None
        records = self._parsed(binding, view)
        if not records:
            return None
        quality_records = (
            records[-max(2, requested):] if binding.feed is FeedType.BAR else records[-1:]
        )
        self._validate_records(binding, quality_records)
        items = self._items(requirement, quality_records)
        if not items:
            return None
        return replace(items[-1], cursor=source_placeholder(view), snapshot_id=view_snapshot_id(view))

    def history(self, requirement: DataRequirement) -> HistoryResult | None:
        return self.history_with_envelopes(requirement)[0]

    def history_with_envelopes(
        self, requirement: DataRequirement
    ) -> tuple[HistoryResult | None, tuple[market_data_pb2.EventEnvelope, ...]]:
        """The history and the canonical envelopes of its rows, from ONE view
        (the Stream's GetSnapshot read view, D29)."""

        requested, start_ns, end_ns, expected_opens = self._requested_window(requirement)
        binding = self.catalog.binding_for(requirement)
        if binding.feed.value in HOT_FEEDS and self.hot_client is not None:
            view = self._latest_view(requirement, binding)
            records = self._parsed(binding, view) if view is not None else ()
        else:
            view, records = self._history_view(binding, requested, start_ns, end_ns)
        if view is None:
            return None, ()
        result = self._history_from_records(
            requirement, binding, records,
            requested=requested, start_ns=start_ns, end_ns=end_ns, expected_opens=expected_opens,
        )
        if result is None:
            return None, ()
        # Native Binance 3d has overlapping historical grids. A retained recent
        # suffix does not prove listing exhaustion or certify an older prefix.
        if (binding.feed is FeedType.BAR and binding.interval == "3d"
                and binding.instrument.identity.venue == "BINANCE"
                and start_ns is None and len(result.items) < requested):
            result = replace(result, coverage=CoverageStatus.PARTIAL)
        selected = records
        if start_ns is not None:
            selected = tuple(
                parsed for parsed in records if start_ns <= parsed.envelope.bar.open_time_ns < end_ns
            )
        envelopes = tuple(parsed.envelope for parsed in selected[-requested:])
        snapshot_id = view_snapshot_id(view)
        return replace(
            result,
            snapshot_id=snapshot_id,
            stream_cursor=source_placeholder(view),
            watermark_offset=view.boundary.offset,
            items=tuple(replace(item, snapshot_id=snapshot_id) for item in result.items),
        ), envelopes

    def _history_view(self, binding, requested, start_ns, end_ns):
        if binding.feed is not FeedType.BAR:
            view = self._view(binding, last=1)
        elif start_ns is not None:
            view = self._view(binding, start_ns=start_ns, end_ns=end_ns)
        else:
            view = self._view(binding, last=requested)
        if view is None:
            return None, ()
        return view, self._parsed(binding, view)

    def history_many(
        self,
        requirements: tuple[DataRequirement, ...],
    ) -> dict[DataRequirement, HistoryResult | None | Exception]:
        """One bounded batch; every item is its own product view (per-item
        watermark, never an atomic global snapshot - contract section 5)."""

        if len(requirements) > 100:
            raise ValueError("stable history batch exceeds the public request bound")
        results: dict[DataRequirement, HistoryResult | None | Exception] = {}
        for requirement in requirements:
            try:
                results[requirement] = self.history(requirement)
            except Exception as error:  # per-item outcome, as the spool batch
                results[requirement] = error
        return results

    def stored_events(self, requirement: DataRequirement) -> tuple[StoredEvent, ...]:
        requested, start_ns, end_ns, _ = self._requested_window(requirement)
        binding = self.catalog.binding_for(requirement)
        _view, rows = self._history_view(binding, requested, start_ns, end_ns)
        if start_ns is None:
            selected = rows[-requested:]
        else:
            selected = tuple(
                parsed for parsed in rows if start_ns <= parsed.envelope.bar.open_time_ns < end_ns
            )
        self._validate_records(binding, selected)
        return tuple(item.stored for item in selected)

    # ------------------------------------------------------------ diagnostics

    def open_gaps_bounded(self, *, cancelled=None):
        stop = cancelled or (lambda: False)
        deadline = self._monotonic_ns() + self._gap_scan_max_work_ns
        detected = self._clock_ns()
        gaps, coverage = [], []

        def budget():
            if stop() or self._monotonic_ns() >= deadline:
                raise _GapDiagnosticIncomplete("retained-window diagnostic cancelled or deadline exceeded")

        def append(gap):
            budget()
            if len(gaps) >= self._gap_scan_max_results:
                raise _GapDiagnosticIncomplete("retained-window diagnostic result bound exceeded")
            gaps.append(gap)

        try:
            for binding in self.catalog.bindings:
                budget()
                row = {"binding_id": binding.binding_id, "instrument_uid": binding.instrument.instrument_uid,
                       "feed": binding.feed.value, "interval": binding.interval,
                       "state": "SCANNED", "retained_rows": None,
                       "first_open_ns": None, "last_open_ns": None}
                if binding.binding_id in self._diagnostic_exclusions:
                    row.update(state="EXCLUDED", reason=self._diagnostic_exclusions[binding.binding_id])
                else:
                    try:
                        self._scan_binding_gaps_bounded(binding, detected_at_ns=detected,
                            append_gap=append, check_budget=budget, cancelled=stop, coverage=row)
                    except QueryBackendError as error:
                        if error.problem.code is not CanonicalErrorCode.DATA_NOT_READY:
                            raise
                        row.update(state="UNAVAILABLE", reason=error.problem.detail)
                coverage.append(row)
        except _GapDiagnosticIncomplete as error:
            raise QueryBackendError(QueryProblem(CanonicalErrorCode.PARTIAL_RESULT,
                str(error), True, retry_after_ms=1000)) from error
        return GapScanResult(sorted(gaps, key=lambda g: (g.detected_at_ns, g.gap_id)), coverage)

    def _scan_binding_gaps_bounded(
        self,
        binding: StableSourceBinding,
        *,
        detected_at_ns: int,
        append_gap: Callable[[GapRecord], None],
        check_budget: Callable[[], None],
        cancelled: Callable[[], bool],
        coverage: dict | None = None,
    ) -> None:
        """One product's retained window, inside the existing global budget."""

        from qdl.runtime.kn_market_cache import KnDiagnosticIndexMissing

        observed_opens: set[int] = set()
        indexed = False
        if binding.feed is FeedType.BAR:
            try:
                boundary, spans, flags = self.reader.bar_diagnostic_ranges(
                    self.product_key(binding), canonical_interval_ms(binding.interval or ""),
                    last=STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW, check_budget=check_budget,
                )
                if not spans:
                    raise _not_ready(f"empty retained BAR view: {binding.binding_id}")
                if coverage is not None:
                    step = canonical_interval_ms(binding.interval or "")
                    coverage.update(retained_rows=sum((end-first)//step+1 for first,end in spans),
                        first_open_ns=spans[0][0]*1_000_000, last_open_ns=spans[-1][1]*1_000_000)
                if boundary.topic_id != self.topic_id:
                    raise _not_ready("diagnostic source topic generation mismatch")
                for _opened_ms, sequence in flags:
                    append_gap(self._gap(binding, f"sequence:{sequence}", sequence, detected_at_ns))
                if binding.continuous_calendar:
                    step_ms = canonical_interval_ms(binding.interval or "")
                    if spans and (spans[-1][1] - spans[0][0]) // step_ms + 1 > self._gap_scan_max_expected_bars:
                        raise _GapDiagnosticIncomplete("global gap diagnostic expected-bar window exceeds its bound")
                    for (_first, prior_end), (next_start, _end) in zip(spans, spans[1:]):
                        for opened in range(prior_end + step_ms, next_start, step_ms):
                            check_budget()
                            ns = opened * 1_000_000
                            append_gap(self._gap(binding, str(ns), "MISSING", detected_at_ns))
                    return
                observed_opens.update(opened * 1_000_000 for first, end in spans
                    for opened in range(first, end + canonical_interval_ms(binding.interval or ""),
                                        canonical_interval_ms(binding.interval or "")))
                indexed = True
            except KnDiagnosticIndexMissing:
                pass  # old projector: exact verified scan, never an empty success
            except KnCacheNotReady as error:
                raise _not_ready(str(error)) from error
            except KnCacheIntegrityError as error:
                raise QueryBackendError(QueryProblem(
                    CanonicalErrorCode.INTERNAL_ERROR, f"diagnostic integrity: {error}", False,
                )) from error
            except KnCacheError as error:
                raise QueryBackendError(QueryProblem(
                    CanonicalErrorCode.DEPENDENCY_UNAVAILABLE, str(error), True,
                )) from error
        if not indexed:
            try:
                view = self._view(
                    binding,
                    last=STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW if binding.feed is FeedType.BAR else 1,
                    check_budget=check_budget,
                )
            except QueryBackendError:
                raise
            if view is None or not view.rows:
                raise _not_ready(f"empty retained view: {binding.binding_id}")
            if coverage is not None:
                coverage["retained_rows"] = len(view.rows)
            for row in view.rows:
                check_budget()
                envelope = market_data_pb2.EventEnvelope.FromString(row.canonical)
                if common_pb2.QUALITY_FLAG_SEQUENCE_GAP_BEFORE in envelope.quality_flags:
                    append_gap(self._gap(
                        binding, f"sequence:{envelope.source_sequence}", envelope.source_sequence,
                        detected_at_ns,
                    ))
                if binding.feed is FeedType.BAR:
                    observed_opens.add(int(envelope.bar.open_time_ns))
        if binding.feed is not FeedType.BAR or not observed_opens:
            return
        step = _interval_ns(binding.interval or "")
        first_open, last_open = min(observed_opens), max(observed_opens)
        if coverage is not None:
            coverage.update(first_open_ns=first_open, last_open_ns=last_open)
        if binding.continuous_calendar:
            if ((last_open - first_open) // step) + 1 > self._gap_scan_max_expected_bars:
                raise _GapDiagnosticIncomplete(
                    "global gap diagnostic expected-bar window exceeds its bound"
                )
            expected_opens = range(first_open, last_open + step, step)
        else:
            try:
                expected_opens = trading_calendar_for_id(
                    binding.instrument.session_calendar_id
                ).bar_opens_between_ns(
                    start_ns=first_open, end_ns=last_open + step, interval_ns=step,
                    max_rows=self._gap_scan_max_expected_bars,
                )
            except ValueError as error:
                raise _GapDiagnosticIncomplete(
                    "global gap diagnostic expected-bar window exceeds its bound"
                ) from error
        for expected_open in expected_opens:
            check_budget()
            if expected_open not in observed_opens:
                append_gap(self._gap(binding, str(expected_open), "MISSING", detected_at_ns))

    # ------------------------------------------------------------ readiness

    def readiness_summary(self) -> tuple[int, int]:
        """(READY products, bound products) of the catalog - per product, never
        a global flag (D32)."""

        lpks = [self.product_key(binding) for binding in self.catalog.bindings]
        generations = self.reader.ready_generations(lpks)
        return sum(1 for generation in generations if generation is not None), len(lpks)


@dataclass(frozen=True)
class KnCursorSettings:
    keys: Mapping[str, bytes]
    active_key_id: str
    environment: str
    topic_id: str
    partition_plan_epoch: int
    route_generation: str
    ttl_seconds: int

    @classmethod
    def from_environment(cls, environ: Mapping[str, str], *, environment: str) -> "KnCursorSettings":
        path = environ.get(CURSOR_KEYS_FILE_ENV, "").strip()
        if not path:
            raise ValueError(f"the kn3 Query backend requires {CURSOR_KEYS_FILE_ENV}")
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or not raw:
            raise ValueError("cursor key file must map key ids to hex secrets")
        keys = {str(key_id): bytes.fromhex(str(secret)) for key_id, secret in raw.items()}
        for name in (CURSOR_ACTIVE_KEY_ENV, TOPIC_ID_ENV, ROUTE_GENERATION_ENV):
            if not environ.get(name, "").strip():
                raise ValueError(f"the kn3 Query backend requires {name}")
        return cls(
            keys=keys,
            active_key_id=environ[CURSOR_ACTIVE_KEY_ENV].strip(),
            environment=environment.lower(),
            topic_id=environ[TOPIC_ID_ENV].strip(),
            partition_plan_epoch=int(environ.get(PARTITION_PLAN_EPOCH_ENV, "1")),
            route_generation=environ[ROUTE_GENERATION_ENV].strip(),
            ttl_seconds=int(environ.get(CURSOR_TTL_ENV, "3600")),
        )


class KnCursorV3Issuer:
    """Signs the backend's source coordinate as a cursor v3 (D28).

    Same ``bind_item``/``bind_history`` surface as ``StableConsumerCursorIssuer``.
    The claims are exactly what the Rust Stream expects: bundle canonical
    stream and revisions come from the same catalog, the product key from the
    bundle LPK rule, the digest from the normalized requirement.
    """

    def __init__(
        self,
        settings: KnCursorSettings,
        catalog: StableSourceCatalog,
        *,
        clock_ns=time.time_ns,
    ) -> None:
        if settings.ttl_seconds < 1:
            raise ValueError("cursor TTL must be positive")
        self.settings = settings
        self.catalog = catalog
        self.codec = SignedCursorV3Codec(settings.keys, active_key_id=settings.active_key_id)
        self._clock_ns = clock_ns

    def bind_item(
        self, requirement: DataRequirement, item: MarketDataItem, *, consumer_id: str
    ) -> MarketDataItem:
        if self._preserve_non_replayable(requirement, item.cursor, item.watermark_offset):
            return item
        token = self._issue(requirement, consumer_id, item.snapshot_id or "", item.cursor)
        return replace(item, cursor=token)

    def bind_history(
        self, requirement: DataRequirement, history: HistoryResult, *, consumer_id: str
    ) -> HistoryResult:
        if self._preserve_non_replayable(requirement, history.stream_cursor, history.watermark_offset):
            return history
        token = self._issue(requirement, consumer_id, history.snapshot_id, history.stream_cursor)
        return replace(
            history,
            stream_cursor=token,
            items=tuple(
                replace(item, snapshot_id=history.snapshot_id, cursor=token) for item in history.items
            ),
        )

    @staticmethod
    def _preserve_non_replayable(requirement: DataRequirement, cursor: str | None, offset: int) -> bool:
        if cursor != NON_REPLAYABLE_STREAM_CURSOR:
            return False
        if requirement.recovery is not RecoveryPolicy.FRESH_SNAPSHOT or offset != 0:
            raise ValueError("non-replayable cursor requires FRESH_SNAPSHOT and zero watermark")
        return True

    def _issue(
        self, requirement: DataRequirement, consumer_id: str, snapshot_id: str, placeholder: str | None
    ) -> str:
        topic_id, partition, offset = parse_placeholder(placeholder)
        if topic_id != self.settings.topic_id:
            raise ValueError("kn3 Query cursor coordinate belongs to another canonical topic")
        binding = self.catalog.binding_for(requirement)
        now = self._clock_ns()
        claims = CursorV3Claims(
            key_id=self.codec.active_key_id,
            environment=self.settings.environment,
            consumer_id=consumer_id,
            requirement_digest=requirement_digest(requirement),
            schema_major=2,
            stream=self.catalog.canonical_stream,
            product_key=binding_product_key(binding, self.settings.environment).encode(),
            snapshot_id=snapshot_id,
            source_topic_id=topic_id,
            source_partition=partition,
            source_offset=offset,
            partition_plan_epoch=self.settings.partition_plan_epoch,
            source_policy_revision=self.catalog.source_policy_revision,
            catalog_revision=self.catalog.catalog_revision,
            route_generation=self.settings.route_generation,
            issued_at_ns=now,
            expires_at_ns=now + self.settings.ttl_seconds * 1_000_000_000,
        )
        return self.codec.encode(claims)
