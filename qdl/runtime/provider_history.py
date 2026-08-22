"""Serve BAR history straight from the venue for consumers that ask for it.

A materialised binding acquires a feed continuously and stores it, which is what
a consumer needs when it declares `SNAPSHOT_AND_REPLAY` and resumes from a
durable cursor. A consumer that only warms up and then appends closed bars needs
none of that, and making it pay for a binding is what makes wide instrument and
interval coverage look expensive (plan section C.14).

This source answers such a request by fetching the venue's own closed-bar
history and canonicalising it with the same functions the golden parity suite
uses. It publishes nothing, holds no cursor and claims no authority.

**It is a distinct data product.** The result never passed through the Rust
canonical core, so it is reported as non-authoritative and never
execution-eligible, and a consumer only receives it by declaring
`recovery: FRESH_SNAPSHOT`. Nothing reaches it by accident, and it never
competes with the authoritative path for the same requirement.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
from typing import Any, Callable

from qdl.adapters.binance.bar_edge import (
    BinanceBarRawBinding,
    fetch_closed_bar_history_raw_envelopes as fetch_binance_history,
)
from qdl.adapters.intervals import canonical_interval_ms
from qdl.adapters.okx.bar_edge import (
    OkxBarRawBinding,
    fetch_closed_bar_history_raw_envelopes as fetch_okx_history,
)
from qdl.canonical.market import (
    canonicalize_binance_usdm_rest_bar,
    canonicalize_okx_bar,
)
from qdl.canonical.trade import TradeContext
from qdl.runtime.closed_bar_cache import ClosedBarWindowCache
from qdl.domain.instrument import InstrumentRecord
from qdl.marketdata.v2 import market_data_pb2
from qdl.query.contracts import DataRequirement, FeedType, RecoveryPolicy
from qdl.query.results import (
    ContractMetadata,
    CoverageStatus,
    HistoryResult,
    MarketDataItem,
    QualityMetadata,
    SourceMetadata,
)
from qdl.runtime.stable_source import bar_item_fields
from qdl.runtime.stable_catalog import StableSourceCatalog

PASS_THROUGH_SOURCE_ROLE = "REFERENCE"
PASS_THROUGH_LICENSE_REVISION = "internal-pass-through-v2"


def pass_through_source_id(instrument_uid: str) -> str:
    """Source id for a pass-through answer.

    Shared with the entitlement grant: if the two ever disagreed, the grant
    would silently stop matching and every pass-through request would be
    refused as unlicensed for a reason nobody could see.
    """
    return f"pass-through-{instrument_uid}"
PASS_THROUGH_QUALITY_FLAG = "PROVIDER_PASS_THROUGH"
# A pass-through window is re-fetched, never resumed, so it must not hand a
# consumer anything that looks like a durable replay position.
PASS_THROUGH_STREAM_CURSOR = "PASS_THROUGH_NO_REPLAY"
_SUPPORTED = {
    ("BINANCE", "USDM"): "binance",
    ("BINANCE", "SPOT"): "binance",
    ("OKX", "SWAP"): "okx",
    ("OKX", "SPOT"): "okx",
}
_MAX_ROWS = 1000
# A venue round-trip must never outlive the request that asked for it. Without
# a bound, one unanswered socket holds a query worker for as long as the kernel
# keeps the connection, which is far longer than any consumer will wait.
_FETCH_TIMEOUT_SECONDS = 20.0


class ProviderHistoryUnavailable(RuntimeError):
    """The request cannot be served from a provider pass-through."""


def _fetch_off_caller_thread(work: Callable[[], Any], *, timeout: float) -> Any:
    """Run a venue fetch on a thread of its own and bound how long it may take.

    The query and stream roles both serve from inside a running event loop, so
    a fetch cannot drive its own loop on the calling thread (`asyncio.run`
    refuses) and blocking socket I/O there would stall every other request the
    worker is serving. Both fetchers therefore run here, on one path, whether
    or not a loop happens to be running: production and tests then exercise the
    same code rather than only production meeting the loop.

    The thread is a daemon because the timeout has to mean something. A venue
    that never answers must not keep the process alive after the request that
    started it has already been refused.
    """
    outcome: dict[str, Any] = {}

    def run() -> None:
        try:
            outcome["value"] = work()
        except BaseException as error:  # re-raised on the calling thread
            outcome["error"] = error

    thread = threading.Thread(
        target=run, name="qdl-pass-through-fetch", daemon=True
    )
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise ProviderHistoryUnavailable(
            f"provider pass-through fetch exceeded {timeout:g}s"
        )
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


class ProviderBarHistorySource:
    def __init__(
        self,
        catalog: StableSourceCatalog,
        *,
        adapter_version: str = "qdl-pass-through/2.0.0",
        normalizer_version: str = "qdl-canonical/2.0.0",
        config_revision: int = 1,
        clock_ns: Callable[[], int] = time.time_ns,
        binance_fetcher: Callable[..., Any] = fetch_binance_history,
        okx_fetcher: Callable[..., Any] = fetch_okx_history,
        cache: ClosedBarWindowCache | None = None,
        fetch_timeout_seconds: float = _FETCH_TIMEOUT_SECONDS,
    ) -> None:
        self.catalog = catalog
        self.adapter_version = adapter_version
        self.normalizer_version = normalizer_version
        self.config_revision = config_revision
        self._clock_ns = clock_ns
        self._binance_fetcher = binance_fetcher
        self._okx_fetcher = okx_fetcher
        self.cache = cache if cache is not None else ClosedBarWindowCache()
        if fetch_timeout_seconds <= 0:
            raise ValueError("pass-through fetch timeout must be positive")
        self.fetch_timeout_seconds = float(fetch_timeout_seconds)
        # One in-flight fetch per window. Without this the gRPC role, which
        # serves on a real thread pool, would send one venue request per
        # concurrent consumer for the same bar period and spend its rate limit
        # on answers it already had in flight.
        self._inflight_lock = threading.Lock()
        self._inflight: dict[tuple[str, str, int], threading.Lock] = {}

    def serves(self, requirement: DataRequirement) -> bool:
        """Whether this source may answer the requirement at all."""
        if requirement.feed is not FeedType.BAR:
            return False
        if requirement.recovery is not RecoveryPolicy.FRESH_SNAPSHOT:
            return False
        try:
            instrument = self.catalog.instrument_for(requirement.instrument_uid)
        except KeyError:
            return False
        identity = instrument.identity
        return (identity.venue, identity.market) in _SUPPORTED

    def history(
        self, requirement: DataRequirement
    ) -> tuple[market_data_pb2.EventEnvelope, ...]:
        """Return canonical closed bars fetched from the venue.

        Fails closed rather than returning a short or ambiguous window: the
        underlying fetchers already reject a gap inside the requested range.
        """
        if not self.serves(requirement):
            raise ProviderHistoryUnavailable(
                "requirement is not eligible for provider pass-through history"
            )
        interval = requirement.interval
        if not interval:
            raise ProviderHistoryUnavailable("BAR history requires an interval")
        try:
            canonical_interval_ms(interval)
        except ValueError as error:
            # Callers distinguish "cannot be served here" from a programming
            # fault, so one refusal type covers every unservable requirement.
            raise ProviderHistoryUnavailable(
                f"interval is not a canonical fixed duration: {error}"
            ) from error
        limit = int(requirement.warmup_limit or 0)
        if not 1 <= limit <= _MAX_ROWS:
            raise ProviderHistoryUnavailable(
                f"pass-through warmup limit must be between 1 and {_MAX_ROWS}"
            )
        instrument = self.catalog.instrument_for(requirement.instrument_uid)
        venue = _SUPPORTED[(instrument.identity.venue, instrument.identity.market)]
        # The boundary is part of the cache identity, so a window fetched for an
        # earlier bar period can never be served into a later one.
        interval_ms = canonical_interval_ms(interval)
        boundary_ms = (self._clock_ns() // 1_000_000) // interval_ms * interval_ms
        cached = self.cache.get(
            requirement.instrument_uid, interval, boundary_ms, limit
        )
        if cached is not None:
            return cached
        with self._window_lock(requirement.instrument_uid, interval, boundary_ms):
            # Another caller may have filled this window while this one
            # waited for the lock, so re-check before spending a request.
            cached = self.cache.get(
                requirement.instrument_uid, interval, boundary_ms, limit
            )
            if cached is not None:
                return cached
            raw_envelopes = (
                self._fetch_binance(instrument, interval, limit)
                if venue == "binance"
                else self._fetch_okx(instrument, interval, limit)
            )
            canonicalize = (
                canonicalize_binance_usdm_rest_bar
                if venue == "binance"
                else canonicalize_okx_bar
            )
            received_ns = self._clock_ns()
            envelopes = []
            for index, raw in enumerate(raw_envelopes):
                frame = json.loads(bytes(raw.raw_frame_bytes))
                context = self._context(instrument, raw, received_ns + index, index)
                envelope = canonicalize(frame, context)
                if envelope.bar.interval != interval:
                    raise ProviderHistoryUnavailable(
                        "provider returned a bar for a different interval: "
                        f"{envelope.bar.interval!r} != {interval!r}"
                    )
                if not envelope.bar.is_final:
                    raise ProviderHistoryUnavailable(
                        "provider pass-through never returns an unfinished bar"
                    )
                envelopes.append(envelope)
            if len(envelopes) != limit:
                raise ProviderHistoryUnavailable(
                    f"pass-through history is incomplete: {len(envelopes)} of {limit}"
                )
            window = tuple(envelopes)
            self.cache.put(
                requirement.instrument_uid, interval, boundary_ms, window
            )
            return window

    def _window_lock(self, instrument_uid: str, interval: str, boundary_ms: int):
        """Return the lock guarding one window, creating it once.

        Keyed exactly like the cache, so two callers collapse only when they
        want the identical window. The entry is dropped once no one holds it,
        which keeps the map from growing one key per bar period forever.
        """
        key = (instrument_uid, interval, int(boundary_ms))
        with self._inflight_lock:
            lock = self._inflight.get(key)
            if lock is None:
                lock = threading.Lock()
                self._inflight[key] = lock
        return _WindowGuard(self, key, lock)

    def _descriptor_fields(self, instrument: InstrumentRecord) -> dict[str, Any]:
        return {
            "market": instrument.identity.market,
            "product_type": instrument.identity.product_type.value,
            "native_symbol": instrument.native_symbol,
            "source_session_id": "qdl-v2-pass-through",
            "connection_generation": 1,
            "lease_epoch": 1,
            "authority_revision": max(1, self.catalog.authority_revision),
            "partition_plan_epoch": 1,
            "adapter_version": self.adapter_version,
            "config_revision": self.config_revision,
            "instrument_catalog_revision": self.catalog.catalog_revision,
        }

    def _fetch_binance(self, instrument, interval, limit):
        binding = BinanceBarRawBinding(
            interval=interval,
            subscription_id=f"pass-through:{instrument.identity.instrument_uid}",
            **self._descriptor_fields(instrument),
        )
        return _fetch_off_caller_thread(
            lambda: self._binance_fetcher(
                binding, limit=limit, test_provenance=False
            ),
            timeout=self.fetch_timeout_seconds,
        )

    def _fetch_okx(self, instrument, interval, limit):
        binding = OkxBarRawBinding(
            interval=interval,
            subscription_id=f"pass-through:{instrument.identity.instrument_uid}",
            **self._descriptor_fields(instrument),
        )
        return _fetch_off_caller_thread(
            lambda: asyncio.run(
                self._okx_fetcher(binding, limit=limit, test_provenance=False)
            ),
            timeout=self.fetch_timeout_seconds,
        )

    def _context(
        self,
        instrument: InstrumentRecord,
        raw: Any,
        received_ns: int,
        sequence: int,
    ) -> TradeContext:
        identity = instrument.identity
        return TradeContext(
            instrument_uid=identity.instrument_uid,
            instrument_id=identity.instrument_id,
            instrument_revision=instrument.metadata_revision,
            venue=identity.venue,
            market=identity.market,
            product_type=identity.product_type.value,
            native_symbol=instrument.native_symbol,
            provider=raw.provider,
            source_id=pass_through_source_id(identity.instrument_uid),
            lease_epoch=1,
            received_at_ns=received_ns,
            normalized_at_ns=received_ns,
            published_at_ns=received_ns,
            partition_sequence=sequence,
            normalizer_version=self.normalizer_version,
            adapter_version=self.adapter_version,
            config_revision=self.config_revision,
            correlation_id=f"pass-through:{identity.instrument_uid}:{sequence}",
            source_session_id="qdl-v2-pass-through",
            connection_generation=1,
            authority_revision=max(1, self.catalog.authority_revision),
            partition_plan_epoch=1,
            raw_capture_id=bytes(raw.capture_id),
            raw_frame_sha256=bytes(raw.raw_frame_sha256),
            source_role=PASS_THROUGH_SOURCE_ROLE,
        )

    def history_result(
        self, requirement: DataRequirement, *, schema_digest: str
    ) -> HistoryResult:
        """Return the pass-through window as a complete history response.

        The response is deliberately unable to masquerade as authoritative
        output: the source is marked non-authoritative, every item is reported
        not execution-eligible, the stream cursor is an explicit non-resumable
        sentinel and the watermark offset is zero because no durable position
        exists.
        """
        envelopes = self.history(requirement)
        last = envelopes[-1]
        data_as_of_ns = int(last.bar.close_time_ns)
        freshness_ms = max(0, (self._clock_ns() - data_as_of_ns) // 1_000_000)
        limit_ms = int(requirement.max_freshness_ms or 0)
        stale = bool(limit_ms) and freshness_ms > limit_ms
        quality = QualityMetadata(
            state="STALE" if stale else "LIVE",
            freshness_ms=int(freshness_ms),
            gap_open=False,
            complete=True,
            # Never eligible: this window never passed the canonical core and
            # is covered by no authority record.
            execution_eligible=False,
            policy_id=requirement.source_policy_id,
            flags=(PASS_THROUGH_QUALITY_FLAG,),
        )
        snapshot_id = "qdl-v2-passthrough-" + hashlib.sha256(
            "|".join((
                requirement.instrument_uid,
                str(requirement.interval),
                str(envelopes[0].bar.open_time_ns),
                str(last.bar.open_time_ns),
                str(len(envelopes)),
            )).encode()
        ).hexdigest()[:32]
        items = tuple(
            MarketDataItem(
                instrument_uid=envelope.instrument_uid,
                instrument_id=envelope.instrument_id,
                instrument_revision=int(envelope.instrument_revision),
                observed_at_ns=int(envelope.source_event_time_ns),
                source=SourceMetadata(
                    venue=envelope.venue,
                    provider=envelope.provider,
                    source_id=envelope.source_id,
                    source_role=PASS_THROUGH_SOURCE_ROLE,
                    authoritative=False,
                ),
                quality=quality,
                contract=ContractMetadata(
                    schema_digest=schema_digest,
                    contract_version="2.0.0",
                    normalizer_version=envelope.normalizer_version,
                    adapter_version=envelope.adapter_version,
                    instrument_catalog_revision=self.catalog.catalog_revision,
                    source_policy_revision=self.catalog.source_policy_revision,
                    authority_revision=self.catalog.authority_revision,
                    config_revision=max(1, int(envelope.config_revision or 1)),
                    correlation_id=envelope.correlation_id,
                ),
                snapshot_id=snapshot_id,
                cursor=PASS_THROUGH_STREAM_CURSOR,
                watermark_offset=0,
                **bar_item_fields(envelope),
            )
            for envelope in envelopes
        )
        return HistoryResult(
            items=items,
            coverage=CoverageStatus.FULL,
            snapshot_id=snapshot_id,
            stream_cursor=PASS_THROUGH_STREAM_CURSOR,
            watermark_offset=0,
            data_as_of_ns=data_as_of_ns,
        )


class _WindowGuard:
    """Hold one window lock and forget it when the last holder leaves."""

    def __init__(self, source: "ProviderBarHistorySource", key, lock) -> None:
        self._source = source
        self._key = key
        self._lock = lock

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, *exc_info) -> None:
        self._lock.release()
        with self._source._inflight_lock:
            if not self._lock.locked():
                self._source._inflight.pop(self._key, None)
