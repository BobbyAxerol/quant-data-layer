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
from dataclasses import replace
import hashlib
import json
import threading
import time
from typing import Any, Callable, Mapping

from qdl.adapters.binance.bar_edge import (
    BinanceBarRawBinding,
    fetch_closed_bar_history_raw_envelopes as fetch_binance_history,
)
from qdl.adapters.intervals import (
    BINANCE_SPOT_NATIVE_INTERVALS,
    BINANCE_USDM_NATIVE_INTERVALS,
    OKX_NATIVE_INTERVALS,
    canonical_interval_ms,
    latest_closed_boundary_ms,
)
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
from qdl.query.contracts import (
    CanonicalErrorCode,
    DataRequirement,
    FeedType,
    QueryProblem,
    RecoveryPolicy,
)
from qdl.query.results import (
    ContractMetadata,
    CoverageStatus,
    HistoryResult,
    MarketDataItem,
    NON_REPLAYABLE_STREAM_CURSOR,
    QualityMetadata,
    SourceMetadata,
)
from qdl.runtime.stable_source import bar_item_fields
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.warmup.contracts import IntervalSourcePolicy
from qdl.warmup.executor import ProviderBudgetPolicy
from qdl.warmup.handoff import resample_final_bars

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
PASS_THROUGH_STREAM_CURSOR = NON_REPLAYABLE_STREAM_CURSOR
_SUPPORTED = {
    ("BINANCE", "USDM"): "binance",
    ("BINANCE", "SPOT"): "binance",
    ("OKX", "SWAP"): "okx",
    ("OKX", "SPOT"): "okx",
}
_MAX_ROWS = 10_000
# A venue round-trip must never outlive the request that asked for it. Without
# a bound, one unanswered socket holds a query worker for as long as the kernel
# keeps the connection, which is far longer than any consumer will wait.
_FETCH_TIMEOUT_SECONDS = 20.0

_PROVIDER_PAGE_POLICIES = {
    "BINANCE": ProviderBudgetPolicy(
        max_concurrency=8,
        requests_per_second=8.0,
        burst_requests=8,
    ),
    "OKX": ProviderBudgetPolicy(
        max_concurrency=4,
        requests_per_second=5.0,
        burst_requests=5,
    ),
}


class ProviderHistoryUnavailable(RuntimeError):
    """The request cannot be served from a provider pass-through."""

    def __init__(
        self,
        detail: str,
        *,
        code: CanonicalErrorCode = CanonicalErrorCode.DATA_NOT_READY,
        retryable: bool = False,
        retry_after_ms: int | None = None,
    ) -> None:
        super().__init__(detail)
        self.problem = QueryProblem(code, detail, retryable, retry_after_ms)


class _ProviderFetchDeadlineExceeded(ProviderHistoryUnavailable):
    """The caller timed out while the provider worker still owns its slot."""


def _fetch_off_caller_thread(
    work: Callable[[], Any],
    *,
    timeout: float,
    release_when_finished: Callable[[], None] | None = None,
) -> Any:
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
    completed = threading.Event()
    ownership_lock = threading.Lock()
    release_in_worker = False

    def run() -> None:
        nonlocal release_in_worker
        try:
            outcome["value"] = work()
        except BaseException as error:  # re-raised on the calling thread
            outcome["error"] = error
        finally:
            completed.set()
            with ownership_lock:
                should_release = release_in_worker
            if should_release and release_when_finished is not None:
                release_when_finished()

    thread = threading.Thread(
        target=run, name="qdl-pass-through-fetch", daemon=True
    )
    thread.start()
    if not completed.wait(timeout):
        with ownership_lock:
            if not completed.is_set() and release_when_finished is not None:
                release_in_worker = True
                delegated = True
            else:
                delegated = False
        if not delegated and completed.is_set():
            # The worker completed between Event.wait() timing out and the
            # ownership handoff. Its result is safe to consume normally.
            pass
        else:
            raise _ProviderFetchDeadlineExceeded(
                f"provider pass-through fetch exceeded {timeout:g}s",
                code=CanonicalErrorCode.SOURCE_UNAVAILABLE,
                retryable=True,
            )
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


def pass_through_eligible(catalog: StableSourceCatalog, requirement) -> bool:
    """Whether the pass-through is able to answer this requirement at all.

    One definition, used by the runtime router and by the release-time check
    that a consumer manifest is servable. If the two ever drifted, a manifest
    would pass validation and then be refused in production, or the reverse.
    """
    if requirement.feed is not FeedType.BAR:
        return False
    if requirement.recovery is not RecoveryPolicy.FRESH_SNAPSHOT:
        return False
    try:
        instrument = catalog.instrument_for(requirement.instrument_uid)
    except KeyError:
        return False
    identity = instrument.identity
    return (identity.venue, identity.market) in _SUPPORTED


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
        provider_policies: Mapping[str, ProviderBudgetPolicy] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
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
        configured_policies = dict(_PROVIDER_PAGE_POLICIES)
        if provider_policies is not None:
            configured_policies.update(
                {key.upper(): value for key, value in provider_policies.items()}
            )
        self._provider_policies = configured_policies
        self._monotonic = monotonic
        self._sleep = sleep
        self._provider_semaphores = {
            key: threading.BoundedSemaphore(value.max_concurrency)
            for key, value in self._provider_policies.items()
        }
        self._provider_rate_locks = {
            key: threading.Lock() for key in self._provider_policies
        }
        self._provider_tokens = {
            key: (float(value.burst_requests), self._monotonic())
            for key, value in self._provider_policies.items()
        }
        self._provider_budget_waits = 0
        self._provider_budget_wait_ms = 0.0
        # One in-flight fetch per window. Without this the gRPC role, which
        # serves on a real thread pool, would send one venue request per
        # concurrent consumer for the same bar period and spend its rate limit
        # on answers it already had in flight.
        self._inflight_lock = threading.Lock()
        self._inflight: dict[tuple[str, str, int, str, str], threading.Lock] = {}
        self._stats_lock = threading.Lock()
        self._source_calls = 0
        self._source_rows = 0
        self._source_failures = 0
        self._rate_limited = 0
        self._server_errors = 0

    def serves(self, requirement: DataRequirement) -> bool:
        """Whether this source may answer the requirement at all."""
        return pass_through_eligible(self.catalog, requirement)

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
            target_interval_ms = canonical_interval_ms(interval)
        except ValueError as error:
            # Callers distinguish "cannot be served here" from a programming
            # fault, so one refusal type covers every unservable requirement.
            raise ProviderHistoryUnavailable(
                f"interval is not a canonical fixed duration: {error}"
            ) from error
        specification = requirement.warmup_specification
        if specification is None:
            raise ProviderHistoryUnavailable("warmup specification is required")
        deadline_at = self._monotonic() + specification.deadline_ms / 1000
        instrument = self.catalog.instrument_for(requirement.instrument_uid)
        source_interval = self._source_interval(
            instrument.identity.venue,
            instrument.identity.market,
            interval,
            specification.interval_source_policy,
        )
        source_interval_ms = canonical_interval_ms(source_interval)
        if target_interval_ms % source_interval_ms:
            raise ProviderHistoryUnavailable(
                "provider source interval does not divide the requested interval"
            )
        factor = target_interval_ms // source_interval_ms
        observed_ns = self._clock_ns()
        observed_ms = observed_ns // 1_000_000
        latest_boundary_ns = latest_closed_boundary_ms(
            interval,
            observed_ms,
            provider=instrument.identity.venue,
        ) * 1_000_000
        if specification.rows is not None:
            limit = specification.rows
            explicit_start_ns = None
            # Native venue calendars own their anchor (notably weekly bars).
            # Asking at observation time lets the adapter select the latest
            # already-closed native bar. Exact resampling still needs an exact
            # target boundary so no incomplete constituent can enter a group.
            end_time_ns = observed_ns if factor == 1 else latest_boundary_ns
        else:
            assert specification.time_range is not None
            explicit_start_ns = specification.time_range.start_time_ns
            end_time_ns = specification.time_range.end_time_ns
            try:
                _, _, limit = specification.resolved_window(
                    interval_ns=target_interval_ms * 1_000_000,
                    latest_closed_boundary_ns=latest_boundary_ns,
                )
            except ValueError as error:
                raise ProviderHistoryUnavailable(str(error)) from error
        if not 1 <= limit <= _MAX_ROWS:
            raise ProviderHistoryUnavailable(
                f"pass-through warmup limit must be between 1 and {_MAX_ROWS}"
            )
        venue = _SUPPORTED[(instrument.identity.venue, instrument.identity.market)]
        # The boundary is part of the cache identity, so a window fetched for an
        # earlier bar period can never be served into a later one.
        boundary_ms = int(
            (latest_boundary_ns if specification.rows is not None else end_time_ns)
            // 1_000_000
        )
        source_limit = limit * factor
        if source_limit > 100_000:
            raise ProviderHistoryUnavailable(
                "resampled warmup exceeds the bounded constituent-row limit"
            )
        cached = self.cache.get(
            requirement.instrument_uid,
            interval,
            boundary_ms,
            source_limit,
            source_policy_id=requirement.source_policy_id,
            source_interval=source_interval,
            max_age_ms=specification.max_cache_age_ms,
        )
        if cached is not None:
            return cached
        with self._window_lock(
            requirement.instrument_uid,
            interval,
            boundary_ms,
            requirement.source_policy_id,
            source_interval,
            deadline_at,
        ):
            # Another caller may have filled this window while this one
            # waited for the lock, so re-check before spending a request.
            cached = self.cache.get(
                requirement.instrument_uid,
                interval,
                boundary_ms,
                source_limit,
                source_policy_id=requirement.source_policy_id,
                source_interval=source_interval,
                max_age_ms=specification.max_cache_age_ms,
            )
            if cached is not None:
                return cached
            try:
                raw_envelopes = self._fetch_chunks(
                    instrument,
                    venue=venue,
                    interval=source_interval,
                    limit=source_limit,
                    end_time_ms=(
                        observed_ms
                        if specification.rows is not None and factor == 1
                        else boundary_ms
                    ),
                    deadline_at=deadline_at,
                )
            except ProviderHistoryUnavailable:
                raise
            except BaseException as error:
                retry_after_ms = getattr(error, "retry_after_ms", None)
                cause = error.__cause__
                while retry_after_ms is None and cause is not None:
                    retry_after_ms = getattr(cause, "retry_after_ms", None)
                    cause = cause.__cause__
                raise ProviderHistoryUnavailable(
                    str(error),
                    code=CanonicalErrorCode.SOURCE_UNAVAILABLE,
                    retryable=True,
                    retry_after_ms=retry_after_ms,
                ) from error
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
                if envelope.bar.interval != source_interval:
                    raise ProviderHistoryUnavailable(
                        "provider returned a bar for a different interval: "
                        f"{envelope.bar.interval!r} != {source_interval!r}"
                    )
                if not envelope.bar.is_final:
                    raise ProviderHistoryUnavailable(
                        "provider pass-through never returns an unfinished bar"
                    )
                if int(envelope.bar.close_time_ns) >= received_ns:
                    raise ProviderHistoryUnavailable(
                        "provider pass-through returned a bar not closed at observation time"
                    )
                envelopes.append(envelope)
            if len(envelopes) != source_limit:
                raise ProviderHistoryUnavailable(
                    "pass-through history is incomplete: "
                    f"{len(envelopes)} of {source_limit}"
                )
            window = tuple(sorted(envelopes, key=lambda item: item.bar.open_time_ns))
            opens = [int(item.bar.open_time_ns) for item in window]
            if any(
                current - previous != source_interval_ms * 1_000_000
                for previous, current in zip(opens, opens[1:])
            ):
                raise ProviderHistoryUnavailable(
                    "pass-through history contains a source-interval gap"
                )
            if explicit_start_ns is not None:
                if opens[0] != explicit_start_ns:
                    raise ProviderHistoryUnavailable(
                        "pass-through history does not start at the requested boundary"
                    )
                if opens[-1] + source_interval_ms * 1_000_000 != end_time_ns:
                    raise ProviderHistoryUnavailable(
                        "pass-through history does not end at the requested boundary"
                    )
            self.cache.put(
                requirement.instrument_uid,
                interval,
                boundary_ms,
                window,
                source_policy_id=requirement.source_policy_id,
                source_interval=source_interval,
            )
            return window

    @staticmethod
    def _source_interval(
        venue: str,
        market: str,
        target: str,
        policy: IntervalSourcePolicy,
    ) -> str:
        native = {
            ("BINANCE", "USDM"): BINANCE_USDM_NATIVE_INTERVALS,
            ("BINANCE", "SPOT"): BINANCE_SPOT_NATIVE_INTERVALS,
            ("OKX", "SWAP"): OKX_NATIVE_INTERVALS,
            ("OKX", "SPOT"): OKX_NATIVE_INTERVALS,
        }[(venue, market)]
        if target in native:
            return target
        if policy is IntervalSourcePolicy.NATIVE_ONLY:
            raise ProviderHistoryUnavailable(
                f"{venue}/{market} does not expose native interval {target}"
            )
        target_ms = canonical_interval_ms(target)
        candidates = sorted(
            (
                (canonical_interval_ms(value), value)
                for value in native
                if target_ms % canonical_interval_ms(value) == 0
            ),
            reverse=True,
        )
        if not candidates:
            raise ProviderHistoryUnavailable(
                f"{venue}/{market} cannot exactly resample interval {target}"
            )
        return candidates[0][1]

    def _fetch_chunks(
        self,
        instrument,
        *,
        venue: str,
        interval: str,
        limit: int,
        end_time_ms: int,
        deadline_at: float,
    ):
        page_limit = 1000 if venue == "binance" else 300
        remaining = limit
        cursor_ms = end_time_ms
        pages = []
        while remaining:
            count = min(remaining, page_limit)
            provider = venue.upper()
            policy = self._provider_policies[provider]
            self._acquire_provider_token(provider, policy, deadline_at)
            semaphore = self._provider_semaphores[provider]
            if not semaphore.acquire(timeout=self._remaining_timeout(deadline_at)):
                raise ProviderHistoryUnavailable(
                    f"provider concurrency deadline exceeded for {provider}",
                    code=CanonicalErrorCode.SOURCE_UNAVAILABLE,
                    retryable=True,
                )
            try:
                release_here = True
                timeout = min(
                    self.fetch_timeout_seconds,
                    self._remaining_timeout(deadline_at),
                )
                page = (
                    self._fetch_binance(
                        instrument,
                        interval,
                        count,
                        cursor_ms,
                        timeout,
                        semaphore.release,
                    )
                    if venue == "binance"
                    else self._fetch_okx(
                        instrument,
                        interval,
                        count,
                        cursor_ms,
                        timeout,
                        semaphore.release,
                    )
                )
            except _ProviderFetchDeadlineExceeded as error:
                release_here = False
                self._record_source_result(error=error)
                raise
            except BaseException as error:
                self._record_source_result(error=error)
                raise
            finally:
                if release_here:
                    semaphore.release()
            self._record_source_result(rows=len(page))
            if len(page) != count:
                raise ProviderHistoryUnavailable(
                    f"provider page is incomplete: {len(page)} of {count}"
                )
            pages.extend(page)
            earliest = min(self._raw_open_ms(item, venue=venue) for item in page)
            if earliest >= cursor_ms:
                raise ProviderHistoryUnavailable(
                    "provider history pagination made no backward progress"
                )
            cursor_ms = earliest
            remaining -= count
        unique = {}
        for item in pages:
            open_ms = self._raw_open_ms(item, venue=venue)
            previous = unique.get(open_ms)
            if previous is not None and bytes(previous.raw_frame_bytes) != bytes(
                item.raw_frame_bytes
            ):
                raise ProviderHistoryUnavailable(
                    "provider returned conflicting payloads for one bar boundary"
                )
            unique[open_ms] = item
        if len(unique) != limit:
            raise ProviderHistoryUnavailable(
                "provider history pages overlap or contain duplicate payloads"
            )
        return tuple(unique[key] for key in sorted(unique))

    def _acquire_provider_token(
        self,
        provider: str,
        policy: ProviderBudgetPolicy,
        deadline_at: float,
    ) -> None:
        lock = self._provider_rate_locks[provider]
        while True:
            with lock:
                now = self._monotonic()
                tokens, updated_at = self._provider_tokens[provider]
                tokens = min(
                    float(policy.burst_requests),
                    tokens
                    + max(0.0, now - updated_at) * policy.requests_per_second,
                )
                if tokens >= 1.0:
                    self._provider_tokens[provider] = (tokens - 1.0, now)
                    return
                wait = (1.0 - tokens) / policy.requests_per_second
                if now + wait > deadline_at:
                    raise ProviderHistoryUnavailable(
                        f"provider token deadline exceeded for {provider}",
                        code=CanonicalErrorCode.RATE_LIMITED,
                        retryable=True,
                        retry_after_ms=max(1, int(wait * 1000)),
                    )
                self._provider_tokens[provider] = (tokens, now)
                with self._stats_lock:
                    self._provider_budget_waits += 1
                    self._provider_budget_wait_ms += wait * 1000
            self._sleep(wait)

    def _remaining_timeout(self, deadline_at: float) -> float:
        remaining = deadline_at - self._monotonic()
        if remaining <= 0:
            raise ProviderHistoryUnavailable(
                "provider history deadline exceeded",
                code=CanonicalErrorCode.SOURCE_UNAVAILABLE,
                retryable=True,
            )
        return remaining

    def _record_source_result(
        self,
        *,
        rows: int = 0,
        error: BaseException | None = None,
    ) -> None:
        status_code = None
        current = error
        while current is not None:
            response = getattr(current, "response", None)
            status_code = getattr(response, "status_code", status_code)
            if status_code is not None:
                break
            current = current.__cause__
        with self._stats_lock:
            self._source_calls += 1
            self._source_rows += max(0, int(rows))
            if error is not None:
                self._source_failures += 1
            if status_code == 429 or (
                error is not None and getattr(error, "retry_after_ms", None) is not None
            ):
                self._rate_limited += 1
            if status_code is not None and int(status_code) >= 500:
                self._server_errors += 1

    def stats(self) -> dict[str, int]:
        with self._stats_lock:
            result = {
                "provider_source_calls": self._source_calls,
                "provider_source_rows": self._source_rows,
                "provider_source_failures": self._source_failures,
                "provider_429": self._rate_limited,
                "provider_5xx": self._server_errors,
                "provider_budget_waits": self._provider_budget_waits,
                "provider_budget_wait_ms": int(self._provider_budget_wait_ms),
            }
        cache = self.cache.stats()
        result.update({
            "cache_entries": cache["entries"],
            "cache_hits": cache["hits"],
            "cache_misses": cache["misses"],
        })
        return result

    @staticmethod
    def _raw_open_ms(item, *, venue: str) -> int:
        frame = json.loads(bytes(item.raw_frame_bytes))
        return int(frame["row"][0] if venue == "binance" else frame["data"][0][0])

    def _window_lock(
        self,
        instrument_uid: str,
        interval: str,
        boundary_ms: int,
        source_policy_id: str,
        source_interval: str,
        deadline_at: float,
    ):
        """Return the lock guarding one window, creating it once.

        Keyed exactly like the cache, so two callers collapse only when they
        want the identical window. The entry is dropped once no one holds it,
        which keeps the map from growing one key per bar period forever.
        """
        key = (
            instrument_uid,
            interval,
            int(boundary_ms),
            source_policy_id,
            source_interval,
        )
        with self._inflight_lock:
            lock = self._inflight.get(key)
            if lock is None:
                lock = threading.Lock()
                self._inflight[key] = lock
        return _WindowGuard(self, key, lock, deadline_at)

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

    def _fetch_binance(
        self,
        instrument,
        interval,
        limit,
        end_time_ms,
        timeout,
        release_when_finished,
    ):
        binding = BinanceBarRawBinding(
            interval=interval,
            subscription_id=f"pass-through:{instrument.identity.instrument_uid}",
            **self._descriptor_fields(instrument),
        )
        return _fetch_off_caller_thread(
            lambda: self._binance_fetcher(
                binding,
                limit=limit,
                now_ms=end_time_ms,
                test_provenance=False,
            ),
            timeout=timeout,
            release_when_finished=release_when_finished,
        )

    def _fetch_okx(
        self,
        instrument,
        interval,
        limit,
        end_time_ms,
        timeout,
        release_when_finished,
    ):
        binding = OkxBarRawBinding(
            interval=interval,
            subscription_id=f"pass-through:{instrument.identity.instrument_uid}",
            **self._descriptor_fields(instrument),
        )
        return _fetch_off_caller_thread(
            lambda: asyncio.run(
                self._okx_fetcher(
                    binding,
                    limit=limit,
                    now_ms=end_time_ms,
                    test_provenance=False,
                )
            ),
            timeout=timeout,
            release_when_finished=release_when_finished,
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
        last_source = envelopes[-1]
        specification = requirement.warmup_specification
        assert specification is not None
        target_interval = requirement.interval or ""
        target_ns = canonical_interval_ms(target_interval) * 1_000_000
        instrument = self.catalog.instrument_for(requirement.instrument_uid)
        latest_boundary_ns = latest_closed_boundary_ms(
            target_interval,
            self._clock_ns() // 1_000_000,
            provider=instrument.identity.venue,
        ) * 1_000_000
        if specification.rows is not None:
            expected_start_ns = expected_end_ns = None
            expected_count = specification.rows
        else:
            try:
                expected_start_ns, expected_end_ns, expected_count = (
                    specification.resolved_window(
                        interval_ns=target_ns,
                        latest_closed_boundary_ns=latest_boundary_ns,
                    )
                )
            except ValueError as error:
                raise ProviderHistoryUnavailable(str(error)) from error
        data_as_of_ns = int(last_source.bar.close_time_ns)
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
        source_items = tuple(
            MarketDataItem(
                instrument_uid=envelope.instrument_uid,
                instrument_id=envelope.instrument_id,
                instrument_revision=int(envelope.instrument_revision),
                observed_at_ns=int(envelope.source_event_time_ns),
                received_at_ns=max(1, int(envelope.received_at_ns)),
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
                cursor=PASS_THROUGH_STREAM_CURSOR,
                watermark_offset=0,
                **bar_item_fields(envelope),
            )
            for envelope in envelopes
        )
        if source_items[0].interval == target_interval:
            items = source_items
        else:
            try:
                items = resample_final_bars(
                    source_items,
                    target_interval=target_interval,
                )
            except ValueError as error:
                raise ProviderHistoryUnavailable(str(error)) from error
            items = tuple(
                replace(
                    item,
                    quality=replace(
                        item.quality,
                        flags=item.quality.flags + ("EXACT_FINAL_RESAMPLE",),
                    ),
                )
                for item in items
            )
        if len(items) != expected_count:
            raise ProviderHistoryUnavailable(
                "pass-through target history is incomplete: "
                f"{len(items)} of {expected_count}"
            )
        if expected_start_ns is not None and (
            int(items[0].payload["open_time_ns"]) != expected_start_ns
            or int(items[-1].payload["open_time_ns"]) + target_ns
            != expected_end_ns
        ):
            raise ProviderHistoryUnavailable(
                "pass-through target history differs from the requested window"
            )
        data_as_of_ns = int(items[-1].payload["close_time_ns"])
        freshness_ms = max(0, (self._clock_ns() - data_as_of_ns) // 1_000_000)
        stale = bool(limit_ms) and freshness_ms > limit_ms
        quality = replace(
            quality,
            state="STALE" if stale else "LIVE",
            freshness_ms=int(freshness_ms),
        )
        items = tuple(
            replace(
                item,
                quality=replace(quality, flags=item.quality.flags),
            )
            for item in items
        )
        snapshot_id = "qdl-v2-passthrough-" + hashlib.sha256(
            "|".join((
                requirement.instrument_uid,
                target_interval,
                str(items[0].payload["open_time_ns"]),
                str(items[-1].payload["open_time_ns"]),
                str(len(items)),
                items[-1].contract.correlation_id,
            )).encode()
        ).hexdigest()[:32]
        items = tuple(replace(item, snapshot_id=snapshot_id) for item in items)
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

    def __init__(
        self,
        source: "ProviderBarHistorySource",
        key,
        lock,
        deadline_at: float,
    ) -> None:
        self._source = source
        self._key = key
        self._lock = lock
        self._deadline_at = deadline_at
        self._acquired = False

    def __enter__(self):
        try:
            timeout = self._source._remaining_timeout(self._deadline_at)
        except BaseException:
            self._forget_if_unlocked()
            raise
        self._acquired = self._lock.acquire(timeout=timeout)
        if not self._acquired:
            self._forget_if_unlocked()
            raise ProviderHistoryUnavailable(
                "identical provider window did not complete before the deadline",
                code=CanonicalErrorCode.SOURCE_UNAVAILABLE,
                retryable=True,
            )
        return self

    def __exit__(self, *exc_info) -> None:
        if self._acquired:
            self._lock.release()
            self._acquired = False
        self._forget_if_unlocked()

    def _forget_if_unlocked(self) -> None:
        with self._source._inflight_lock:
            if (
                not self._lock.locked()
                and self._source._inflight.get(self._key) is self._lock
            ):
                self._source._inflight.pop(self._key, None)
