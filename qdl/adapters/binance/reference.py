from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from app.providers.binance import derivatives as legacy_derivatives
from app.providers.binance.rest import BinanceProviderError
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
        max_attempts: int = 3,
        retry_backoff_seconds: float = 0.25,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
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
        self._max_attempts = max_attempts
        self._retry_backoff_seconds = retry_backoff_seconds
        self._sleep = sleep

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

        return await self._paginate(
            request,
            capability,
            endpoint="/futures/data/basis",
            page=page,
            parser=lambda row: self._basis_observation(request, row, pair),
            page_limit=500,
        )

    async def _paginate(
        self,
        request: ReferenceRequest,
        capability: FeedCapability,
        *,
        endpoint: str,
        page: Callable[[int | None, int | None, int], Awaitable[dict[str, Any]]],
        parser: Callable[[Mapping[str, Any]], ReferenceObservation],
        page_limit: int,
    ) -> ReferenceFetch:
        assert request.start_ms is not None and request.end_ms is not None
        per_page = min(request.page_size or request.limit, page_limit)
        cursor = request.start_ms
        selected: dict[int, ReferenceObservation] = {}
        pages = 0
        terminal_reason = "MAX_PAGES"
        truncated = False
        while pages < request.max_pages and len(selected) < request.limit:
            response = await page(cursor, request.end_ms, min(per_page, request.limit - len(selected)))
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
            newest = max(page_times)
            if newest < cursor:
                raise ReferenceProviderError(f"Binance {endpoint} pagination made no progress")
            for item, timestamp_ms in zip(page_observations, page_times):
                if request.start_ms <= timestamp_ms <= request.end_ms:
                    prior = selected.get(timestamp_ms)
                    if prior is not None and prior != item:
                        raise ReferenceProviderError(
                            f"Binance {endpoint} returned conflicting rows for one timestamp"
                        )
                    selected[timestamp_ms] = item
            if newest >= request.end_ms:
                terminal_reason = "REACHED_REQUEST_END"
                break
            if len(selected) >= request.limit:
                terminal_reason = "MAX_RECORDS"
                truncated = True
                break
            if len(data) < per_page:
                terminal_reason = "PROVIDER_EXHAUSTED"
                break
            cursor = newest + 1
        else:
            truncated = True

        ordered = tuple(selected[key] for key in sorted(selected))
        observed = tuple(key for key in sorted(selected))
        coverage = ReferenceCoverage(
            requested_start_ms=request.start_ms,
            requested_end_ms=request.end_ms,
            observed_min_ms=min(observed) if observed else None,
            observed_max_ms=max(observed) if observed else None,
            complete_left=bool(observed) and min(observed) <= request.start_ms,
            complete_right=bool(observed) and max(observed) >= request.end_ms,
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
                if attempt + 1 < self._max_attempts:
                    await self._sleep(self._retry_backoff_seconds * (2**attempt))
        raise ReferenceProviderExhausted(
            f"Binance USD-M {operation} exhausted {self._max_attempts} bounded attempts"
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
