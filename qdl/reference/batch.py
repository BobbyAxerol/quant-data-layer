from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from typing import Protocol

from qdl.domain.capabilities import (
    CapabilityAvailability,
    FeedCapability,
    VenueCapabilityProfile,
    binance_usdm_capabilities,
    okx_global_capabilities,
)
from qdl.domain.instrument import InstrumentRecord
from qdl.reference.contracts import (
    ReferenceBatchResult,
    ReferenceCoverage,
    ReferenceFetch,
    ReferenceLineage,
    ReferenceProduct,
    ReferenceProviderError,
    ReferenceProviderExhausted,
    ReferenceRequest,
    ReferenceStatus,
    ReferenceUnavailable,
    product_feed_name,
    provider_lineage,
)


class ReferenceAdapter(Protocol):
    """Thin provider edge; the batch core owns cache/coalescing semantics."""

    async def fetch(
        self,
        request: ReferenceRequest,
        *,
        capability: FeedCapability,
        received_at_ns: int,
    ) -> ReferenceFetch: ...


CapabilityResolver = Callable[[InstrumentRecord], VenueCapabilityProfile]


@dataclass(frozen=True, slots=True)
class ReferenceBatchPolicy:
    max_concurrency: int = 8
    max_provider_concurrency: int = 4
    request_timeout_seconds: float = 20.0
    max_cache_entries: int = 512
    history_ttl_seconds: float = 30.0
    snapshot_ttl_seconds: float = 2.0
    metadata_ttl_seconds: float = 300.0
    missing_ttl_seconds: float = 2.0
    unavailable_ttl_seconds: float = 60.0

    def __post_init__(self) -> None:
        if not 1 <= self.max_concurrency <= 64:
            raise ValueError("reference batch concurrency must be between 1 and 64")
        if not 1 <= self.max_provider_concurrency <= self.max_concurrency:
            raise ValueError(
                "reference provider concurrency must be between 1 and global concurrency"
            )
        if not 0.1 <= self.request_timeout_seconds <= 120.0:
            raise ValueError("reference request timeout must be between 0.1 and 120 seconds")
        if not 1 <= self.max_cache_entries <= 10_000:
            raise ValueError("reference batch cache capacity must be between 1 and 10000")
        if any(
            value < 0
            for value in (
                self.history_ttl_seconds,
                self.snapshot_ttl_seconds,
                self.metadata_ttl_seconds,
                self.missing_ttl_seconds,
                self.unavailable_ttl_seconds,
            )
        ):
            raise ValueError("reference cache TTL values cannot be negative")

    def ttl_seconds(self, result: ReferenceBatchResult) -> float:
        if result.status is ReferenceStatus.MISSING:
            return self.missing_ttl_seconds
        if result.status is ReferenceStatus.UNAVAILABLE:
            return self.unavailable_ttl_seconds
        if result.status is not ReferenceStatus.OK:
            return 0.0
        if result.request.product is ReferenceProduct.CONTRACT_METADATA:
            return self.metadata_ttl_seconds
        if result.request.is_history:
            return self.history_ttl_seconds
        return self.snapshot_ttl_seconds


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    expires_at: float
    result: ReferenceBatchResult


def default_capability_resolver(record: InstrumentRecord) -> VenueCapabilityProfile:
    identity = record.identity
    if (identity.venue, identity.market) == ("BINANCE", "USDM"):
        return binance_usdm_capabilities()
    if (identity.venue, identity.market) == ("OKX", "SWAP"):
        return okx_global_capabilities("SWAP")
    if (identity.venue, identity.market) == ("OKX", "FUTURES"):
        return okx_global_capabilities("FUTURES")
    raise ReferenceUnavailable(
        f"reference batch has no approved adapter for {identity.venue}/{identity.market}"
    )


class ReferenceBatch:
    """Bounded, provider-neutral multi-instrument reference-data acquisition.

    The core has no HTTP endpoint knowledge. An adapter is selected by the
    registry-owned venue/market identity and is never allowed to replace an
    unavailable provider capability with a different provider's value.
    """

    def __init__(
        self,
        adapters: Mapping[tuple[str, str], ReferenceAdapter],
        *,
        capability_resolver: CapabilityResolver = default_capability_resolver,
        policy: ReferenceBatchPolicy = ReferenceBatchPolicy(),
        clock_ns: Callable[[], int] = time.time_ns,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._adapters = {
            (venue.upper(), market.upper()): adapter
            for (venue, market), adapter in adapters.items()
        }
        self._capability_resolver = capability_resolver
        self._policy = policy
        self._clock_ns = clock_ns
        self._monotonic = monotonic
        self._semaphore = asyncio.Semaphore(policy.max_concurrency)
        self._provider_semaphores: dict[str, asyncio.Semaphore] = {}
        self._lock = asyncio.Lock()
        self._cache: OrderedDict[tuple[object, ...], _CacheEntry] = OrderedDict()
        self._inflight: dict[tuple[object, ...], asyncio.Task[ReferenceBatchResult]] = {}
        self._source_calls = 0
        self._cache_hits = 0
        self._cache_misses = 0
        self._provider_timeouts = 0

    async def fetch(self, requests: tuple[ReferenceRequest, ...]) -> tuple[ReferenceBatchResult, ...]:
        """Resolve every item concurrently while preserving caller order."""

        return tuple(await asyncio.gather(*(self.fetch_one(request) for request in requests)))

    async def fetch_one(
        self,
        request: ReferenceRequest,
        *,
        bypass_cache: bool = False,
    ) -> ReferenceBatchResult:
        """Resolve one request, optionally replacing a stale cached snapshot.

        ``bypass_cache`` is intentionally an internal query-service recovery
        primitive. It preserves the request identity and provider lane while
        preventing an already diagnosed stale cache entry from being returned
        a second time. It is not a generic retry policy.
        """
        key = request.cache_key
        now = self._monotonic()
        async with self._lock:
            self._evict_expired(now)
            if bypass_cache:
                self._cache.pop(key, None)
                cached = None
            else:
                cached = self._cache.get(key)
            if cached is not None:
                self._cache.move_to_end(key)
                self._cache_hits += 1
                return replace(cached.result, cache_hit=True, coalesced=False)
            self._cache_misses += 1
            task = self._inflight.get(key)
            coalesced = task is not None
            if task is None:
                task = asyncio.create_task(self._fetch_uncached(key, request))
                self._inflight[key] = task
        result = await asyncio.shield(task)
        return replace(result, cache_hit=False, coalesced=coalesced)

    async def _fetch_uncached(
        self, key: tuple[object, ...], request: ReferenceRequest
    ) -> ReferenceBatchResult:
        try:
            result = await self._resolve_request(request)
            ttl = self._policy.ttl_seconds(result)
            if ttl > 0:
                async with self._lock:
                    self._cache[key] = _CacheEntry(self._monotonic() + ttl, result)
                    self._cache.move_to_end(key)
                    while len(self._cache) > self._policy.max_cache_entries:
                        self._cache.popitem(last=False)
            return result
        finally:
            async with self._lock:
                self._inflight.pop(key, None)

    async def _resolve_request(self, request: ReferenceRequest) -> ReferenceBatchResult:
        request_started_at_ns = self._clock_ns()
        try:
            profile = self._capability_resolver(request.instrument)
            capability_name = product_feed_name(request.product)
            capability = profile.capability(capability_name)
        except ReferenceUnavailable as error:
            return self._unavailable_result(
                request,
                FeedCapability(CapabilityAvailability.UNAVAILABLE, constraint=str(error)),
                request_started_at_ns,
                "ADAPTER_UNAVAILABLE",
                str(error),
            )
        except KeyError as error:
            return self._unavailable_result(
                request,
                FeedCapability(CapabilityAvailability.UNAVAILABLE, constraint=str(error)),
                request_started_at_ns,
                "CAPABILITY_UNDECLARED",
                str(error),
            )

        if not capability.enabled:
            return self._unavailable_result(
                request,
                capability,
                request_started_at_ns,
                "CAPABILITY_UNAVAILABLE",
                capability.constraint or "provider capability is unavailable",
            )

        adapter = self._adapters.get(request.provider_key)
        if adapter is None:
            return self._unavailable_result(
                request,
                capability,
                request_started_at_ns,
                "ADAPTER_UNAVAILABLE",
                f"no registered reference adapter for {request.provider_key[0]}/{request.provider_key[1]}",
            )

        try:
            provider = request.instrument.identity.venue.upper()
            provider_semaphore = self._provider_semaphores.setdefault(
                provider, asyncio.Semaphore(self._policy.max_provider_concurrency)
            )
            async with self._semaphore, provider_semaphore:
                self._source_calls += 1
                fetched = await asyncio.wait_for(
                    adapter.fetch(
                        request,
                        capability=capability,
                        received_at_ns=request_started_at_ns,
                    ),
                    timeout=self._policy.request_timeout_seconds,
                )
        except TimeoutError as error:
            self._provider_timeouts += 1
            return self._error_result(
                request,
                capability,
                request_started_at_ns,
                "PROVIDER_TIMEOUT",
                "reference provider request exceeded its bounded deadline",
            )
        except ReferenceUnavailable as error:
            return self._unavailable_result(
                request,
                capability,
                request_started_at_ns,
                "CAPABILITY_UNAVAILABLE",
                str(error),
            )
        except ReferenceProviderExhausted as error:
            return self._error_result(
                request,
                capability,
                request_started_at_ns,
                "PROVIDER_RETRY_EXHAUSTED",
                str(error),
                retry_after_ms=getattr(error, "retry_after_ms", None),
            )
        except ReferenceProviderError as error:
            return self._error_result(
                request,
                capability,
                request_started_at_ns,
                "PROVIDER_PROTOCOL",
                str(error),
            )
        except Exception as error:  # adapter failures are isolated to this item
            return self._error_result(
                request,
                capability,
                request_started_at_ns,
                "PROVIDER_FAILURE",
                type(error).__name__,
            )

        received_at_ns = self._clock_ns()
        if not fetched.observations:
            return ReferenceBatchResult(
                request=request,
                status=ReferenceStatus.MISSING,
                capability=capability,
                lineage=fetched.lineage,
                coverage=fetched.coverage,
                received_at_ns=received_at_ns,
            )
        return ReferenceBatchResult(
            request=request,
            status=ReferenceStatus.OK,
            capability=capability,
            lineage=fetched.lineage,
            coverage=fetched.coverage,
            received_at_ns=received_at_ns,
            observations=fetched.observations,
        )

    def _unavailable_result(
        self,
        request: ReferenceRequest,
        capability: FeedCapability,
        received_at_ns: int,
        code: str,
        detail: str,
    ) -> ReferenceBatchResult:
        return ReferenceBatchResult(
            request=request,
            status=ReferenceStatus.UNAVAILABLE,
            capability=capability,
            lineage=(
                provider_lineage(
                    provider=self._provider_name(request),
                    endpoint="capability",
                    capability_name=product_feed_name(request.product),
                    capability=capability,
                    adapter_version="qdl-reference-batch/1",
                ),
            ),
            coverage=self._empty_coverage(request, "CAPABILITY_UNAVAILABLE"),
            received_at_ns=received_at_ns,
            error_code=code,
            error_detail=detail,
        )

    def _error_result(
        self,
        request: ReferenceRequest,
        capability: FeedCapability,
        received_at_ns: int,
        code: str,
        detail: str,
        *,
        retry_after_ms: int | None = None,
    ) -> ReferenceBatchResult:
        return ReferenceBatchResult(
            request=request,
            status=ReferenceStatus.ERROR,
            capability=capability,
            lineage=(
                provider_lineage(
                    provider=self._provider_name(request),
                    endpoint="adapter",
                    capability_name=product_feed_name(request.product),
                    capability=capability,
                    adapter_version="qdl-reference-batch/1",
                ),
            ),
            coverage=self._empty_coverage(request, "PROVIDER_ERROR"),
            received_at_ns=received_at_ns,
            error_code=code,
            error_detail=detail,
            retry_after_ms=retry_after_ms,
        )

    @staticmethod
    def _provider_name(request: ReferenceRequest) -> str:
        return f"{request.instrument.identity.venue}_DIRECT"

    @staticmethod
    def _empty_coverage(request: ReferenceRequest, reason: str) -> ReferenceCoverage:
        return ReferenceCoverage(
            requested_start_ms=request.start_ms,
            requested_end_ms=request.end_ms,
            observed_min_ms=None,
            observed_max_ms=None,
            complete_left=False,
            complete_right=False,
            truncated=False,
            terminal_reason=reason,
        )

    def _evict_expired(self, now: float) -> None:
        for key, entry in tuple(self._cache.items()):
            if entry.expires_at <= now:
                self._cache.pop(key, None)

    def stats(self) -> dict[str, int]:
        """Bounded scheduler/caching counters for batch evidence and SLOs."""

        return {
            "source_calls": self._source_calls,
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "provider_timeouts": self._provider_timeouts,
            "cache_entries": len(self._cache),
            "inflight": len(self._inflight),
            "provider_lanes": len(self._provider_semaphores),
        }
