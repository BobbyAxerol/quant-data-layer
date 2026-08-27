from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
import time
from typing import Any

from app.providers.binance import derivatives as legacy_derivatives
from app.providers.binance import basis_continuous as legacy_continuous_basis
from app.providers.binance.rest import BinanceProviderError
from qdl.adapters.intervals import canonical_interval_ms
from qdl.domain.capabilities import FeedCapability
from qdl.domain.instrument import ProductType
from qdl.reference.contracts import (
    BasisSeries,
    MarkIndexKind,
    ReferenceCoverage,
    ReferenceFetch,
    ReferenceObservation,
    ReferenceProduct,
    ReferenceProviderError,
    ReferenceProviderExhausted,
    ReferenceRequest,
    ReferenceUnavailable,
    decimal_field,
    provider_lineage,
    require_product,
)


_ADAPTER_VERSION = "qdl-binance-reference/1"
# Binance emits a provider-truthful funding settlement timestamp.  Its public
# clock can land a few milliseconds either side of the nominal settlement
# boundary, so coverage must not manufacture a gap from that clock jitter.
# This does not alter the raw observation timestamp or relax any other product.
_FUNDING_BOUNDARY_TOLERANCE_MS = 60_000
_NATIVE_BASIS_TRANSIENT_ATTEMPTS = 4
_NATIVE_BASIS_TRANSIENT_BACKOFF_SECONDS = 0.5
_NATIVE_BASIS_MIN_START_INTERVAL_NS = 500_000_000


def _fields(*items):
    return tuple(item for item in items if item is not None)


class BinanceUsdmReferenceAdapter:
    """Bounded V2 edge over the existing Binance USD-M REST wrappers.

    The legacy wrapper still owns vendor HTTP details. This adapter adds the
    registry identity, exact decimal/unit mapping, bounded cursor progression
    and per-request lineage required by the canonical V2 boundary.
    """

    def __init__(
        self,
        *,
        funding_fetcher: Callable[..., dict[str, Any]] = legacy_derivatives.fetch_funding_rate,
        open_interest_fetcher: Callable[..., dict[str, Any]] = legacy_derivatives.fetch_open_interest,
        metric_history_fetcher: Callable[..., dict[str, Any]] = legacy_derivatives.fetch_metric_history,
        long_short_fetcher: Callable[..., dict[str, Any]] = legacy_derivatives.fetch_long_short_ratio,
        taker_fetcher: Callable[..., dict[str, Any]] = legacy_derivatives.fetch_taker_long_short_ratio,
        mark_index_fetcher: Callable[..., dict[str, Any]] = legacy_derivatives.fetch_mark_index_price,
        exchange_info_fetcher: Callable[..., dict[str, Any]] = legacy_derivatives.fetch_exchange_info,
        basis_fetcher: Callable[..., dict[str, Any]] = legacy_derivatives.fetch_basis,
        continuous_basis_fetcher: Callable[..., dict[str, Any]] = legacy_continuous_basis.fetch_continuous_basis_bundle,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 0.25,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if not 1 <= max_attempts <= 5:
            raise ValueError("Binance reference attempts must be between 1 and 5")
        if retry_backoff_seconds < 0:
            raise ValueError("Binance reference retry backoff cannot be negative")
        self._funding_fetcher = funding_fetcher
        self._open_interest_fetcher = open_interest_fetcher
        self._metric_history_fetcher = metric_history_fetcher
        self._long_short_fetcher = long_short_fetcher
        self._taker_fetcher = taker_fetcher
        self._mark_index_fetcher = mark_index_fetcher
        self._exchange_info_fetcher = exchange_info_fetcher
        self._basis_fetcher = basis_fetcher
        self._continuous_basis_fetcher = continuous_basis_fetcher
        self._max_attempts = max_attempts
        self._retry_backoff_seconds = retry_backoff_seconds
        self._sleep = sleep
        self._clock_ns = clock_ns
        self._monotonic_ns = monotonic_ns
        # Binance's public native-basis endpoint may emit an HTTP-200 error
        # envelope under concurrent pair pressure. Keep that one fragile lane
        # serial; all other reference products retain ReferenceBatch limits.
        self._native_basis_lock = asyncio.Lock()
        self._next_native_basis_start_ns = 0

    async def fetch(
        self,
        request: ReferenceRequest,
        *,
        capability: FeedCapability,
        received_at_ns: int,
    ) -> ReferenceFetch:
        if request.provider_key != ("BINANCE", "USDM"):
            raise ReferenceUnavailable("Binance USD-M adapter received a different venue/market")
        product = request.product
        if product is ReferenceProduct.FUNDING_RATE:
            require_product(request.instrument, ProductType.PERPETUAL)
            return await self._funding_history(request, capability)
        if product is ReferenceProduct.OPEN_INTEREST:
            require_product(request.instrument, ProductType.PERPETUAL)
            if request.is_history:
                return await self._open_interest_history(request, capability)
            return await self._open_interest_snapshot(request, capability, received_at_ns)
        if product is ReferenceProduct.LONG_SHORT_RATIO:
            require_product(request.instrument, ProductType.PERPETUAL)
            return await self._long_short_history(request, capability)
        if product is ReferenceProduct.TAKER_FLOW:
            require_product(request.instrument, ProductType.PERPETUAL)
            return await self._taker_history(request, capability)
        if product is ReferenceProduct.MARK_INDEX_PRICE:
            require_product(request.instrument, ProductType.PERPETUAL, ProductType.FUTURE)
            return await self._mark_index_snapshot(request, capability, received_at_ns)
        if product is ReferenceProduct.CONTRACT_METADATA:
            require_product(request.instrument, ProductType.PERPETUAL, ProductType.FUTURE)
            return await self._contract_metadata(request, capability, received_at_ns)
        if product is ReferenceProduct.BASIS:
            return await self._basis_history(request, capability)
        raise ReferenceUnavailable(f"Binance USD-M does not implement {product.value}")

    async def _funding_history(
        self, request: ReferenceRequest, capability: FeedCapability
    ) -> ReferenceFetch:
        async def page(start_ms: int | None, end_ms: int | None, limit: int) -> dict[str, Any]:
            return await self._call(
                "funding_rate",
                self._funding_fetcher,
                request.instrument.native_symbol,
                limit=limit,
                start_time=start_ms,
                end_time=end_ms,
            )

        return await self._paginate(
            request,
            capability,
            endpoint="/fapi/v1/fundingRate",
            page=page,
            parser=lambda row: self._funding_observation(request, row),
            page_limit=1000,
            boundary_tolerance_ms=_FUNDING_BOUNDARY_TOLERANCE_MS,
        )

    async def _open_interest_history(
        self, request: ReferenceRequest, capability: FeedCapability
    ) -> ReferenceFetch:
        async def page(start_ms: int | None, end_ms: int | None, limit: int) -> dict[str, Any]:
            return await self._call(
                "open_interest_hist",
                self._metric_history_fetcher,
                "open_interest_hist",
                request.instrument.native_symbol,
                request.interval,
                limit,
                start_ms,
                end_ms,
            )

        return await self._paginate(
            request,
            capability,
            endpoint="/futures/data/openInterestHist",
            page=page,
            parser=lambda row: self._open_interest_history_observation(request, row),
            page_limit=500,
            direction="BACKWARD",
            expected_interval_ms=canonical_interval_ms(request.interval or ""),
        )

    async def _open_interest_snapshot(
        self,
        request: ReferenceRequest,
        capability: FeedCapability,
        received_at_ns: int,
    ) -> ReferenceFetch:
        response = await self._call(
            "open_interest", self._open_interest_fetcher, request.instrument.native_symbol
        )
        data = self._response_data(response, "open_interest")
        if not isinstance(data, Mapping):
            raise ReferenceProviderError("Binance open-interest snapshot has invalid shape")
        observation = self._open_interest_snapshot_observation(request, data, received_at_ns)
        return ReferenceFetch(
            observations=(observation,),
            lineage=(self._lineage("/fapi/v1/openInterest", capability),),
            coverage=self._snapshot_coverage(observation),
        )

    async def _long_short_history(
        self, request: ReferenceRequest, capability: FeedCapability
    ) -> ReferenceFetch:
        kind = request.long_short_kind
        assert kind is not None
        legacy_kind = {
            "GLOBAL_ACCOUNT": "global_account",
            "TOP_ACCOUNT": "top_account",
            "TOP_POSITION": "top_position",
        }[kind.value]
        endpoint = {
            "GLOBAL_ACCOUNT": "/futures/data/globalLongShortAccountRatio",
            "TOP_ACCOUNT": "/futures/data/topLongShortAccountRatio",
            "TOP_POSITION": "/futures/data/topLongShortPositionRatio",
        }[kind.value]

        async def page(start_ms: int | None, end_ms: int | None, limit: int) -> dict[str, Any]:
            return await self._call(
                "long_short_ratio",
                self._long_short_fetcher,
                legacy_kind,
                request.instrument.native_symbol,
                request.interval,
                limit,
                start_ms,
                end_ms,
            )

        return await self._paginate(
            request,
            capability,
            endpoint=endpoint,
            page=page,
            parser=lambda row: self._long_short_observation(request, row),
            page_limit=500,
            direction="BACKWARD",
            expected_interval_ms=canonical_interval_ms(request.interval or ""),
        )

    async def _taker_history(
        self, request: ReferenceRequest, capability: FeedCapability
    ) -> ReferenceFetch:
        async def page(start_ms: int | None, end_ms: int | None, limit: int) -> dict[str, Any]:
            return await self._call(
                "taker_long_short_ratio",
                self._taker_fetcher,
                request.instrument.native_symbol,
                request.interval,
                limit,
                start_ms,
                end_ms,
            )

        return await self._paginate(
            request,
            capability,
            endpoint="/futures/data/takerlongshortRatio",
            page=page,
            parser=lambda row: self._taker_observation(request, row),
            page_limit=500,
            direction="BACKWARD",
            expected_interval_ms=canonical_interval_ms(request.interval or ""),
        )

    async def _mark_index_snapshot(
        self,
        request: ReferenceRequest,
        capability: FeedCapability,
        received_at_ns: int,
    ) -> ReferenceFetch:
        response = await self._call(
            "mark_index_price", self._mark_index_fetcher, request.instrument.native_symbol
        )
        data = self._response_data(response, "mark_index_price")
        if not isinstance(data, Mapping):
            raise ReferenceProviderError("Binance mark/index snapshot has invalid shape")
        observation = self._mark_index_observation(request, data, received_at_ns)
        return ReferenceFetch(
            observations=(observation,),
            lineage=(self._lineage("/fapi/v1/premiumIndex", capability),),
            coverage=self._snapshot_coverage(observation),
        )

    async def _contract_metadata(
        self,
        request: ReferenceRequest,
        capability: FeedCapability,
        received_at_ns: int,
    ) -> ReferenceFetch:
        response = await self._call(
            "exchange_info", self._exchange_info_fetcher, request.instrument.native_symbol
        )
        data = self._response_data(response, "exchange_info")
        if not isinstance(data, Mapping):
            raise ReferenceProviderError("Binance exchange-info response has invalid shape")
        symbols = data.get("symbols")
        if not isinstance(symbols, list):
            raise ReferenceProviderError("Binance exchange-info response has no symbols list")
        native = request.instrument.native_symbol.upper()
        matches = [row for row in symbols if isinstance(row, Mapping) and str(row.get("symbol") or "").upper() == native]
        if len(matches) > 1:
            raise ReferenceProviderError("Binance exchange-info returned duplicate instrument metadata")
        if not matches:
            return ReferenceFetch(
                observations=(),
                lineage=(self._lineage("/fapi/v1/exchangeInfo", capability),),
                coverage=self._empty_snapshot_coverage("PROVIDER_EMPTY"),
            )
        observation = self._metadata_observation(request, matches[0], received_at_ns)
        return ReferenceFetch(
            observations=(observation,),
            lineage=(self._lineage("/fapi/v1/exchangeInfo", capability),),
            coverage=self._snapshot_coverage(observation),
        )

    async def _basis_history(
        self, request: ReferenceRequest, capability: FeedCapability
    ) -> ReferenceFetch:
        if request.basis_series is BasisSeries.NATIVE:
            if (
                request.instrument.identity.product_type is not ProductType.PERPETUAL
                or str(request.basis_contract_type).upper() != "PERPETUAL"
            ):
                raise ReferenceUnavailable(
                    "Binance provider-native basis cannot claim one dated contract; use a declared continuous selector"
                )
        elif request.instrument.identity.product_type is not ProductType.PERPETUAL:
            raise ReferenceUnavailable(
                "a Binance continuous basis request must use the perpetual pair anchor, not a dated future identity"
            )
        if request.basis_series is BasisSeries.CONTINUOUS:
            return await self._continuous_basis_history(request, capability)
        pair = f"{request.instrument.base_asset}{request.instrument.quote_asset}".upper()

        async def page(start_ms: int | None, end_ms: int | None, limit: int) -> dict[str, Any]:
            return await self._call(
                "basis",
                self._basis_fetcher,
                pair,
                str(request.basis_contract_type).upper(),
                request.interval,
                limit,
                start_ms,
                end_ms,
            )

        async def fetch_once() -> ReferenceFetch:
            return await self._paginate(
                request,
                capability,
                endpoint="/futures/data/basis",
                page=page,
                parser=lambda row: self._basis_observation(request, row, pair),
                page_limit=500,
            )

        async with self._native_basis_lock:
            await self._pace_native_basis_start()
            return await self._retry_transient_native_basis_envelope(fetch_once)

    async def _pace_native_basis_start(self) -> None:
        """Respect a tiny process-local spacing for Binance's fragile basis lane."""

        now_ns = self._monotonic_ns()
        wait_ns = self._next_native_basis_start_ns - now_ns
        if wait_ns > 0:
            await self._sleep(wait_ns / 1_000_000_000)
        self._next_native_basis_start_ns = (
            self._monotonic_ns() + _NATIVE_BASIS_MIN_START_INTERVAL_NS
        )

    async def _retry_transient_native_basis_envelope(
        self,
        fetch_once: Callable[[], Awaitable[ReferenceFetch]],
    ) -> ReferenceFetch:
        """Retry only Binance's occasional HTTP-200, non-list basis envelope."""

        last_error: ReferenceProviderError | None = None
        for attempt in range(_NATIVE_BASIS_TRANSIENT_ATTEMPTS):
            try:
                return await fetch_once()
            except ReferenceProviderError as error:
                if str(error) != "Binance /futures/data/basis history response is not a list":
                    raise
                last_error = error
                if attempt + 1 < _NATIVE_BASIS_TRANSIENT_ATTEMPTS:
                    await self._sleep(_NATIVE_BASIS_TRANSIENT_BACKOFF_SECONDS * (2**attempt))
        assert last_error is not None
        raise ReferenceProviderExhausted(
            "Binance USD-M basis exhausted "
            f"{_NATIVE_BASIS_TRANSIENT_ATTEMPTS} transient-envelope attempts"
        ) from last_error

    async def _continuous_basis_history(
        self, request: ReferenceRequest, capability: FeedCapability
    ) -> ReferenceFetch:
        """Build the research-style 1d rolling basis from Binance Vision.

        Binance's public ``/futures/data/basis`` endpoint is intentionally
        retained for native/latest history only.  It cannot provide the 365d
        roll-aware series required by the basis strategy.  The established
        provider builder is invoked memory-only in a worker thread and the
        returned rows are converted immediately into canonical observations.
        """

        assert request.start_ms is not None and request.end_ms is not None
        if request.interval != "1d":
            raise ReferenceUnavailable(
                "continuous Binance basis is currently certified only for interval=1d"
            )
        day_ms = 86_400_000
        if request.start_ms % day_ms or request.end_ms % day_ms:
            raise ReferenceProviderError(
                "continuous Binance basis boundaries must align to UTC daily opens"
            )
        requested_rows = ((request.end_ms - request.start_ms) // day_ms) + 1
        if requested_rows < 30:
            raise ReferenceUnavailable(
                "continuous Binance basis requires at least 30 complete daily observations"
            )
        if requested_rows > request.limit:
            raise ReferenceProviderError(
                "continuous Binance basis window exceeds the caller's bounded limit"
            )
        if request.end_ms + day_ms - 1 > self._clock_ns() // 1_000_000:
            raise ReferenceProviderError(
                "continuous Binance basis request includes a daily bar that is not final"
            )
        pair = f"{request.instrument.base_asset}{request.instrument.quote_asset}".upper()

        payload = await self._continuous_basis_call(
            pair=pair,
            lookback_days=requested_rows,
            end_ms=request.end_ms,
        )
        rows = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(rows, list):
            raise ReferenceProviderError("continuous Binance basis response has invalid data rows")
        observations = tuple(
            self._continuous_basis_observation(request, row, pair)
            for row in rows
            if isinstance(row, Mapping)
        )
        if len(observations) != len(rows):
            raise ReferenceProviderError("continuous Binance basis response has malformed rows")
        observations = tuple(sorted(observations, key=lambda item: item.observed_at_ns))
        try:
            observed_ms = tuple(
                int(dict(item.labels)["period_open_time_ms"])
                for item in observations
            )
        except (KeyError, ValueError) as error:
            raise ReferenceProviderError(
                "continuous Binance basis observation lacks a valid period-open identity"
            ) from error
        if len(set(observed_ms)) != len(observed_ms):
            raise ReferenceProviderError("continuous Binance basis response has duplicate daily observations")
        expected_ms = tuple(range(request.start_ms, request.end_ms + day_ms, day_ms))
        in_window = tuple(value for value in observed_ms if request.start_ms <= value <= request.end_ms)
        if in_window != observed_ms:
            raise ReferenceProviderError("continuous Binance basis response escaped its requested window")
        complete = observed_ms == expected_ms
        coverage = ReferenceCoverage(
            requested_start_ms=request.start_ms,
            requested_end_ms=request.end_ms,
            observed_min_ms=min(observed_ms) if observed_ms else None,
            observed_max_ms=max(observed_ms) if observed_ms else None,
            complete_left=complete,
            complete_right=complete,
            truncated=not complete,
            terminal_reason=(
                "VISION_CONTINUOUS_COMPLETE"
                if complete
                else "VISION_CONTINUOUS_INCOMPLETE"
            ),
        )
        components = set()
        if isinstance(payload, Mapping) and isinstance(payload.get("meta"), Mapping):
            raw_components = payload["meta"].get("source_components", ())
            if isinstance(raw_components, (list, tuple)):
                components = {str(value) for value in raw_components}
        lineage = [self._lineage("https://data.binance.vision/data/futures/um", capability)]
        if "BINANCE_USDM_REST" in components:
            lineage.append(self._lineage("/fapi/v1/klines", capability))
        return ReferenceFetch(
            observations=observations,
            lineage=tuple(lineage),
            coverage=coverage,
        )

    async def _continuous_basis_call(
        self,
        *,
        pair: str,
        lookback_days: int,
        end_ms: int,
    ) -> dict[str, Any]:
        from datetime import datetime, timezone

        last_error: BaseException | None = None
        for attempt in range(self._max_attempts):
            try:
                payload = await asyncio.to_thread(
                    self._continuous_basis_fetcher,
                    pair,
                    interval="1d",
                    lookback_days=lookback_days,
                    end_time=datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc),
                    persist_cache=False,
                    fallback_url=None,
                    max_attempts=1,
                    backoff_seconds=0,
                )
                if not isinstance(payload, Mapping):
                    raise ReferenceProviderError("continuous Binance basis wrapper returned a non-object")
                return dict(payload)
            except (BinanceProviderError, OSError, ReferenceProviderError, RuntimeError, ValueError) as error:
                last_error = error
                if attempt + 1 < self._max_attempts:
                    await self._sleep(self._retry_backoff_seconds * (2**attempt))
        raise ReferenceProviderExhausted(
            f"Binance USD-M continuous_basis exhausted {self._max_attempts} bounded attempts"
        ) from last_error

    async def _paginate(
        self,
        request: ReferenceRequest,
        capability: FeedCapability,
        *,
        endpoint: str,
        page: Callable[[int | None, int | None, int], Awaitable[dict[str, Any]]],
        parser: Callable[[Mapping[str, Any]], ReferenceObservation],
        page_limit: int,
        direction: str = "FORWARD",
        expected_interval_ms: int | None = None,
        boundary_tolerance_ms: int = 0,
    ) -> ReferenceFetch:
        assert request.start_ms is not None and request.end_ms is not None
        if direction not in {"FORWARD", "BACKWARD"}:
            raise ValueError("Binance reference pagination direction is invalid")
        if expected_interval_ms is not None and expected_interval_ms <= 0:
            raise ValueError("Binance reference expected interval must be positive")
        if not 0 <= boundary_tolerance_ms <= 300_000:
            raise ValueError("Binance reference boundary tolerance must be between 0 and 300000ms")
        per_page = min(request.page_size or request.limit, page_limit)
        cursor_start = request.start_ms
        cursor_end = request.end_ms
        selected: dict[int, ReferenceObservation] = {}
        pages = 0
        terminal_reason = "MAX_PAGES"
        truncated = False
        while pages < request.max_pages and len(selected) < request.limit:
            response = await page(
                cursor_start if direction == "FORWARD" else request.start_ms,
                request.end_ms if direction == "FORWARD" else cursor_end,
                min(per_page, request.limit - len(selected)),
            )
            data = self._response_data(response, endpoint)
            if not isinstance(data, list):
                raise ReferenceProviderError(f"Binance {endpoint} history response is not a list")
            pages += 1
            if not data:
                terminal_reason = "PROVIDER_EXHAUSTED"
                break
            page_observations = []
            for row in data:
                if not isinstance(row, Mapping):
                    raise ReferenceProviderError(f"Binance {endpoint} history row has invalid shape")
                page_observations.append(parser(row))
            page_times = [item.observed_at_ns // 1_000_000 for item in page_observations]
            oldest = min(page_times)
            newest = max(page_times)
            if direction == "FORWARD" and newest < cursor_start:
                raise ReferenceProviderError(f"Binance {endpoint} forward pagination made no progress")
            if direction == "BACKWARD" and oldest > cursor_end:
                raise ReferenceProviderError(f"Binance {endpoint} backward pagination made no progress")
            for item, timestamp_ms in zip(page_observations, page_times):
                if request.start_ms <= timestamp_ms <= request.end_ms:
                    prior = selected.get(timestamp_ms)
                    if prior is not None and prior != item:
                        raise ReferenceProviderError(
                            f"Binance {endpoint} returned conflicting rows for one timestamp"
                        )
                    selected[timestamp_ms] = item
            if direction == "FORWARD":
                if newest >= request.end_ms:
                    terminal_reason = "REACHED_REQUEST_END"
                    break
            elif oldest <= request.start_ms:
                terminal_reason = "REACHED_REQUEST_START"
                break
            if len(selected) >= request.limit:
                terminal_reason = "MAX_RECORDS"
                truncated = True
                break
            if len(data) < per_page:
                terminal_reason = "PROVIDER_EXHAUSTED"
                break
            if direction == "FORWARD":
                cursor_start = newest + 1
            else:
                cursor_end = oldest - 1
        else:
            truncated = True

        ordered = tuple(selected[key] for key in sorted(selected))
        observed = tuple(key for key in sorted(selected))
        if expected_interval_ms is not None and observed:
            cadence_ok = (
                all((value - request.start_ms) % expected_interval_ms == 0 for value in observed)
                and all(
                    right - left == expected_interval_ms
                    for left, right in zip(observed, observed[1:])
                )
            )
            if not cadence_ok:
                truncated = True
                terminal_reason = "INTERNAL_GAP"
        coverage = ReferenceCoverage(
            requested_start_ms=request.start_ms,
            requested_end_ms=request.end_ms,
            observed_min_ms=min(observed) if observed else None,
            observed_max_ms=max(observed) if observed else None,
            complete_left=(
                bool(observed)
                and min(observed) <= request.start_ms + boundary_tolerance_ms
            ),
            complete_right=(
                bool(observed)
                and max(observed) >= request.end_ms - boundary_tolerance_ms
            ),
            truncated=truncated,
            terminal_reason=terminal_reason,
        )
        return ReferenceFetch(
            observations=ordered,
            lineage=(self._lineage(endpoint, capability),),
            coverage=coverage,
        )

    async def _call(
        self, operation: str, fetcher: Callable[..., dict[str, Any]], *args, **kwargs
    ) -> dict[str, Any]:
        last_error: BaseException | None = None
        retry_after_ms: int | None = None
        for attempt in range(self._max_attempts):
            try:
                return await asyncio.to_thread(
                    fetcher,
                    *args,
                    **kwargs,
                    max_attempts=1,
                    backoff_seconds=0,
                )
            except (BinanceProviderError, OSError) as error:
                last_error = error
                provider_hint = getattr(error, "retry_after_ms", None)
                if provider_hint is not None:
                    retry_after_ms = max(retry_after_ms or 0, int(provider_hint))
                    break
                if attempt + 1 < self._max_attempts:
                    await self._sleep(self._retry_backoff_seconds * (2**attempt))
        raise ReferenceProviderExhausted(
            f"Binance USD-M {operation} exhausted {self._max_attempts} bounded attempts",
            retry_after_ms=retry_after_ms,
        ) from last_error

    @staticmethod
    def _response_data(response: object, operation: str) -> object:
        if not isinstance(response, Mapping) or "data" not in response:
            raise ReferenceProviderError(f"Binance {operation} wrapper response has invalid shape")
        return response["data"]

    def _funding_observation(
        self, request: ReferenceRequest, row: Mapping[str, Any]
    ) -> ReferenceObservation:
        self._require_symbol(request, row)
        fields = _fields(
            decimal_field("funding_rate", row.get("fundingRate"), "DIMENSIONLESS_RATE"),
            decimal_field("mark_price", row.get("markPrice"), "QUOTE_PRICE"),
        )
        if not fields:
            raise ReferenceProviderError("Binance funding row has no numeric reference fields")
        return self._observation(request, self._timestamp_ns(row, "fundingTime"), fields)

    def _open_interest_history_observation(
        self, request: ReferenceRequest, row: Mapping[str, Any]
    ) -> ReferenceObservation:
        self._require_symbol(request, row)
        fields = _fields(
            decimal_field("open_interest_contracts", row.get("sumOpenInterest"), "CONTRACTS"),
            decimal_field("open_interest_quote_notional", row.get("sumOpenInterestValue"), "QUOTE_NOTIONAL"),
        )
        if not fields:
            raise ReferenceProviderError("Binance open-interest history row has no numeric fields")
        return self._observation(request, self._timestamp_ns(row, "timestamp"), fields)

    def _open_interest_snapshot_observation(
        self, request: ReferenceRequest, row: Mapping[str, Any], received_at_ns: int
    ) -> ReferenceObservation:
        self._require_symbol(request, row)
        field = decimal_field("open_interest_contracts", row.get("openInterest"), "CONTRACTS")
        if field is None:
            raise ReferenceProviderError("Binance open-interest snapshot has no openInterest")
        observed_at_ns = self._timestamp_ns_optional(row, "time") or received_at_ns
        return self._observation(
            request,
            observed_at_ns,
            (field,),
            labels=(("timestamp_origin", "PROVIDER" if "time" in row else "RECEIVED_AT"),),
        )

    def _long_short_observation(
        self, request: ReferenceRequest, row: Mapping[str, Any]
    ) -> ReferenceObservation:
        self._require_symbol(request, row)
        fields = _fields(
            decimal_field("long_short_ratio", row.get("longShortRatio"), "RATIO"),
            decimal_field("long_account_ratio", row.get("longAccount"), "RATIO"),
            decimal_field("short_account_ratio", row.get("shortAccount"), "RATIO"),
        )
        if not fields:
            raise ReferenceProviderError("Binance long/short row has no numeric fields")
        assert request.long_short_kind is not None
        return self._observation(
            request,
            self._timestamp_ns(row, "timestamp"),
            fields,
            labels=(("ratio_kind", request.long_short_kind.value),),
        )

    def _taker_observation(
        self, request: ReferenceRequest, row: Mapping[str, Any]
    ) -> ReferenceObservation:
        self._require_symbol(request, row)
        fields = _fields(
            decimal_field("buy_sell_ratio", row.get("buySellRatio"), "RATIO"),
            decimal_field("buy_volume", row.get("buyVol"), "PROVIDER_NATIVE_VOLUME"),
            decimal_field("sell_volume", row.get("sellVol"), "PROVIDER_NATIVE_VOLUME"),
        )
        if not fields:
            raise ReferenceProviderError("Binance taker-flow row has no numeric fields")
        return self._observation(request, self._timestamp_ns(row, "timestamp"), fields)

    def _mark_index_observation(
        self, request: ReferenceRequest, row: Mapping[str, Any], received_at_ns: int
    ) -> ReferenceObservation:
        self._require_symbol(request, row)
        wants_mark = request.mark_index_kind in {MarkIndexKind.MARK, MarkIndexKind.BOTH}
        wants_index = request.mark_index_kind in {MarkIndexKind.INDEX, MarkIndexKind.BOTH}
        fields = _fields(
            decimal_field("mark_price", row.get("markPrice"), "QUOTE_PRICE") if wants_mark else None,
            decimal_field("index_price", row.get("indexPrice"), "QUOTE_PRICE") if wants_index else None,
        )
        expected = int(wants_mark) + int(wants_index)
        if len(fields) != expected:
            raise ReferenceProviderError("Binance mark/index snapshot is missing a requested price type")
        observed_at_ns = self._timestamp_ns_optional(row, "time") or received_at_ns
        return self._observation(
            request,
            observed_at_ns,
            fields,
            labels=(("timestamp_origin", "PROVIDER" if "time" in row else "RECEIVED_AT"),),
        )

    def _metadata_observation(
        self, request: ReferenceRequest, row: Mapping[str, Any], received_at_ns: int
    ) -> ReferenceObservation:
        self._require_symbol(request, row)
        price_tick = self._filter_value(row, "PRICE_FILTER", "tickSize")
        quantity_step = self._filter_value(row, "LOT_SIZE", "stepSize")
        fields = _fields(
            decimal_field("price_tick", price_tick, "QUOTE_PRICE"),
            decimal_field("quantity_step", quantity_step, "CONTRACTS"),
            decimal_field("onboard_time_ms", row.get("onboardDate"), "EPOCH_MILLISECONDS"),
            decimal_field("delivery_time_ms", row.get("deliveryDate"), "EPOCH_MILLISECONDS"),
        )
        if len(fields) < 2:
            raise ReferenceProviderError("Binance instrument metadata lacks price/quantity filters")
        contract_type = str(row.get("contractType") or "").strip().upper()
        if not contract_type:
            raise ReferenceProviderError("Binance instrument metadata lacks contractType")
        return self._observation(
            request,
            received_at_ns,
            fields,
            labels=(
                ("contract_type", contract_type),
                ("timestamp_origin", "RECEIVED_AT"),
            ),
        )

    def _basis_observation(
        self, request: ReferenceRequest, row: Mapping[str, Any], pair: str
    ) -> ReferenceObservation:
        row_pair = str(row.get("pair") or "").strip().upper()
        if row_pair and row_pair != pair:
            raise ReferenceProviderError("Binance basis row belongs to a different pair")
        contract_type = str(row.get("contractType") or "").strip().upper()
        expected_contract = str(request.basis_contract_type).upper()
        if contract_type and contract_type != expected_contract:
            raise ReferenceProviderError("Binance basis row belongs to a different contract selector")
        fields = _fields(
            decimal_field("index_price", row.get("indexPrice"), "QUOTE_PRICE"),
            decimal_field("futures_price", row.get("futuresPrice"), "QUOTE_PRICE"),
            decimal_field("basis", row.get("basis"), "QUOTE_PRICE"),
            decimal_field("basis_rate", row.get("basisRate"), "DIMENSIONLESS_RATE"),
            decimal_field(
                "annualized_basis_rate",
                row.get("annualizedBasisRate"),
                "DIMENSIONLESS_RATE",
            ),
        )
        if not fields:
            raise ReferenceProviderError("Binance basis row has no numeric fields")
        return self._observation(
            request,
            self._timestamp_ns(row, "timestamp"),
            fields,
            labels=(
                ("basis_series", request.basis_series.value),
                ("contract_selector", expected_contract),
                ("pair", pair),
            ),
        )

    def _continuous_basis_observation(
        self, request: ReferenceRequest, row: Mapping[str, Any], pair: str
    ) -> ReferenceObservation:
        """Translate the approved Vision roll builder output without zero-fill."""

        from datetime import datetime, timezone

        timestamp = str(row.get("timestamp") or "").strip()
        if not timestamp:
            raise ReferenceProviderError("continuous Binance basis row is missing timestamp")
        try:
            parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError as error:
            raise ReferenceProviderError("continuous Binance basis timestamp is invalid") from error
        if parsed.tzinfo is None:
            raise ReferenceProviderError("continuous Binance basis timestamp must be timezone-aware")
        utc = parsed.astimezone(timezone.utc)
        if any((utc.hour, utc.minute, utc.second, utc.microsecond)):
            raise ReferenceProviderError("continuous Binance basis timestamp is not a UTC daily open")
        period_open_ms = int(utc.timestamp()) * 1_000
        period_close_ms = period_open_ms + 86_400_000 - 1
        observed_at_ns = period_close_ms * 1_000_000
        active_contract = str(row.get("active_contract") or "").strip().upper()
        if not active_contract:
            raise ReferenceProviderError("continuous Binance basis row lacks active_contract lineage")
        fields = _fields(
            decimal_field("perpetual_close", row.get("perpetual_close"), "QUOTE_PRICE"),
            decimal_field("quarterly_close", row.get("quarterly_close"), "QUOTE_PRICE"),
            decimal_field("basis", row.get("basis"), "QUOTE_PRICE"),
            decimal_field("days_to_expiry", row.get("days_to_expiry"), "DAYS"),
        )
        if len(fields) != 4:
            raise ReferenceProviderError("continuous Binance basis row lacks a required derived field")
        return self._observation(
            request,
            observed_at_ns,
            fields,
            labels=(
                ("basis_series", BasisSeries.CONTINUOUS.value),
                ("contract_selector", str(request.basis_contract_type).upper()),
                ("pair", pair),
                ("active_contract", active_contract),
                ("roll_policy", "research_volume_crossover"),
                ("derivation", "quarterly_close_minus_perpetual_close"),
                ("period_open_time_ms", str(period_open_ms)),
                ("period_close_time_ms", str(period_close_ms)),
                ("timestamp_origin", "FINAL_BAR_CLOSE"),
            ),
        )

    def _lineage(self, endpoint: str, capability: FeedCapability):
        return provider_lineage(
            provider="BINANCE_DIRECT",
            endpoint=endpoint,
            capability_name="reference_data",
            capability=capability,
            adapter_version=_ADAPTER_VERSION,
        )

    @staticmethod
    def _require_symbol(request: ReferenceRequest, row: Mapping[str, Any]) -> None:
        value = str(row.get("symbol") or "").strip().upper()
        if value and value != request.instrument.native_symbol.upper():
            raise ReferenceProviderError("Binance reference row belongs to a different instrument")

    @staticmethod
    def _timestamp_ns(row: Mapping[str, Any], key: str) -> int:
        value = BinanceUsdmReferenceAdapter._timestamp_ns_optional(row, key)
        if value is None:
            raise ReferenceProviderError(f"Binance reference row is missing {key}")
        return value

    @staticmethod
    def _timestamp_ns_optional(row: Mapping[str, Any], key: str) -> int | None:
        value = row.get(key)
        if value in (None, ""):
            return None
        try:
            timestamp_ms = int(str(value))
        except (TypeError, ValueError) as error:
            raise ReferenceProviderError(f"Binance reference timestamp {key} is invalid") from error
        if timestamp_ms <= 0:
            raise ReferenceProviderError(f"Binance reference timestamp {key} must be positive")
        return timestamp_ms * 1_000_000

    @staticmethod
    def _filter_value(row: Mapping[str, Any], filter_type: str, key: str) -> object | None:
        filters = row.get("filters")
        if not isinstance(filters, list):
            return None
        for item in filters:
            if isinstance(item, Mapping) and item.get("filterType") == filter_type:
                return item.get(key)
        return None

    @staticmethod
    def _observation(
        request: ReferenceRequest,
        observed_at_ns: int,
        fields,
        *,
        labels: tuple[tuple[str, str], ...] = (),
    ) -> ReferenceObservation:
        return ReferenceObservation(
            instrument_uid=request.instrument.instrument_uid,
            instrument_revision=request.instrument.metadata_revision,
            product=request.product,
            observed_at_ns=observed_at_ns,
            fields=tuple(fields),
            labels=(
                ("native_symbol", request.instrument.native_symbol.upper()),
                *labels,
            ),
        )

    @staticmethod
    def _snapshot_coverage(observation: ReferenceObservation) -> ReferenceCoverage:
        observed_ms = observation.observed_at_ns // 1_000_000
        return ReferenceCoverage(
            requested_start_ms=None,
            requested_end_ms=None,
            observed_min_ms=observed_ms,
            observed_max_ms=observed_ms,
            complete_left=True,
            complete_right=True,
            truncated=False,
            terminal_reason="SNAPSHOT",
        )

    @staticmethod
    def _empty_snapshot_coverage(reason: str) -> ReferenceCoverage:
        return ReferenceCoverage(
            requested_start_ms=None,
            requested_end_ms=None,
            observed_min_ms=None,
            observed_max_ms=None,
            complete_left=False,
            complete_right=False,
            truncated=False,
            terminal_reason=reason,
        )
