from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

from qdl.admission.contracts import ProviderAdmissionRuntime
from qdl.admission.edge import AdmissionDeadlineExceeded, ProviderRateLimited
from qdl.adapters.intervals import canonical_interval_ms, okx_bar_size
from qdl.adapters.okx.admitted_rest import AsyncAdmittedOkxStatisticsClient
from qdl.adapters.okx.client import OkxRestClient
from qdl.adapters.okx.history import (
    HistoryCoverage,
    OkxHistoricalClient,
    OkxOpenInterestSnapshot,
    PaginationStalled,
)
from qdl.domain.capabilities import FeedCapability, OKX_REFERENCE_INTERVALS
from qdl.domain.instrument import ProductType
from qdl.reference.contracts import (
    LongShortKind,
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


_ADAPTER_VERSION = "qdl-okx-reference/2"
_CONTRACT_STATISTICS = "/api/v5/rubik/stat/contracts/"
_LONG_SHORT_ENDPOINTS = {
    LongShortKind.GLOBAL_ACCOUNT: _CONTRACT_STATISTICS + "long-short-account-ratio-contract",
    LongShortKind.TOP_ACCOUNT: _CONTRACT_STATISTICS + "long-short-account-ratio-contract-top-trader",
    LongShortKind.TOP_POSITION: _CONTRACT_STATISTICS + "long-short-position-ratio-contract-top-trader",
}


def _fields(*items):
    return tuple(item for item in items if item is not None)


class OkxSwapReferenceAdapter:
    """OKX V5 swap/futures reference adapter; class name stays API-compatible."""

    def __init__(
        self,
        client: OkxRestClient,
        *,
        history: OkxHistoricalClient | None = None,
        statistics_client: OkxRestClient | None = None,
        statistics_admission: ProviderAdmissionRuntime | None = None,
        require_statistics_admission: bool = False,
    ) -> None:
        self._client = client
        self._statistics_admission_missing = require_statistics_admission and statistics_admission is None
        self._history = history if history is not None else OkxHistoricalClient(client)
        if statistics_admission is not None:
            self._statistics_history = OkxHistoricalClient(
                AsyncAdmittedOkxStatisticsClient(statistics_client or client, statistics_admission)
            )
        else:
            self._statistics_history = (
                OkxHistoricalClient(statistics_client) if statistics_client is not None else self._history
            )

    async def fetch(
        self,
        request: ReferenceRequest,
        *,
        capability: FeedCapability,
        received_at_ns: int,
    ) -> ReferenceFetch:
        if request.provider_key not in {("OKX", "SWAP"), ("OKX", "FUTURES")}:
            raise ReferenceUnavailable("OKX reference adapter received a different venue/market")
        product = request.product
        if product is ReferenceProduct.FUNDING_RATE:
            require_product(request.instrument, ProductType.PERPETUAL)
            return await self._funding_history(request, capability)
        if product is ReferenceProduct.OPEN_INTEREST:
            require_product(request.instrument, ProductType.PERPETUAL, ProductType.FUTURE)
            if request.is_history:
                return await self._contract_statistics(request, capability)
            return await self._open_interest_snapshot(request, capability)
        if product in {ReferenceProduct.LONG_SHORT_RATIO, ReferenceProduct.TAKER_FLOW}:
            require_product(request.instrument, ProductType.PERPETUAL, ProductType.FUTURE)
            return await self._contract_statistics(request, capability)
        if product is ReferenceProduct.MARK_INDEX_PRICE:
            require_product(request.instrument, ProductType.PERPETUAL, ProductType.FUTURE)
            return await self._mark_index_snapshot(request, capability, received_at_ns)
        if product is ReferenceProduct.CONTRACT_METADATA:
            require_product(request.instrument, ProductType.PERPETUAL, ProductType.FUTURE)
            return await self._contract_metadata(request, capability, received_at_ns)
        if product is ReferenceProduct.BASIS:
            raise ReferenceUnavailable(
                "OKX basis is DERIVED_ONLY and requires explicit input instruments/formula"
            )
        raise ReferenceUnavailable(
            f"OKX has no provider-equivalent public {product.value} reference product"
        )

    async def _contract_statistics(
        self, request: ReferenceRequest, capability: FeedCapability
    ) -> ReferenceFetch:
        assert request.start_ms is not None and request.end_ms is not None
        if self._statistics_admission_missing:
            raise ReferenceUnavailable("OKX contract statistics requires configured shared Rust admission")
        if request.interval not in OKX_REFERENCE_INTERVALS:
            raise ReferenceUnavailable(
                "OKX contract statistics requires a supported canonical fixed-duration interval"
            )
        period = okx_bar_size(request.interval)
        interval_ms = canonical_interval_ms(request.interval)
        params = {"instId": request.instrument.native_symbol, "period": period}
        labels = [
            ("native_symbol", request.instrument.native_symbol),
            ("inst_type", request.instrument.identity.market),
            ("scope", "EXACT_CONTRACT"),
            ("identity_origin", "REQUEST_INST_ID"),
            ("interval", request.interval),
            ("provider_period", period),
            ("timestamp_origin", "PROVIDER"),
            ("finality", "PROVIDER_NOT_SUPPLIED"),
            ("history_limit_samples", "1440"),
        ]
        if request.product is ReferenceProduct.OPEN_INTEREST:
            endpoint = _CONTRACT_STATISTICS + "open-interest-history"
            columns = (
                ("open_interest_contracts", "CONTRACTS"),
                ("open_interest_ccy", "BASE_ASSET_QUANTITY"),
                ("open_interest_usd", "USD_NOTIONAL"),
            )
        elif request.product is ReferenceProduct.LONG_SHORT_RATIO:
            assert request.long_short_kind is not None
            endpoint = _LONG_SHORT_ENDPOINTS[request.long_short_kind]
            columns = (("long_short_ratio", "RATIO"),)
            labels.extend((
                ("ratio_kind", request.long_short_kind.value),
                ("ratio_population", "ALL_TRADERS" if request.long_short_kind is LongShortKind.GLOBAL_ACCOUNT
                 else "TOP_5_PERCENT_BY_OPEN_POSITION_VALUE"),
                ("ratio_measure", "POSITION" if request.long_short_kind is LongShortKind.TOP_POSITION else "ACCOUNT"),
            ))
        else:
            endpoint = "/api/v5/rubik/stat/taker-volume-contract"
            # The provider returns SELL before BUY. Pin contracts explicitly;
            # do not relabel native units or manufacture a buy/sell ratio.
            params["unit"] = "1"
            labels.append(("provider_unit", "1"))
            columns = (("sell_volume", "CONTRACTS"), ("buy_volume", "CONTRACTS"))

        def parse(row: object) -> ReferenceObservation:
            if not isinstance(row, list) or len(row) != len(columns) + 1:
                raise ReferenceProviderError("OKX contract statistics row has invalid shape")
            timestamp_ns = self._timestamp_ns_optional({"ts": row[0]}, "ts")
            if timestamp_ns is None or timestamp_ns // 1_000_000 > request.end_ms:
                raise ReferenceProviderError("OKX contract statistics timestamp is missing or beyond request end")
            fields = _fields(*(decimal_field(name, value, unit)
                               for (name, unit), value in zip(columns, row[1:], strict=True)))
            if not fields:
                raise ReferenceProviderError("OKX contract statistics row has no numeric fields")
            if any(field.value.as_decimal() < 0 for field in fields):
                raise ReferenceProviderError("OKX contract statistics values must be nonnegative")
            return self._observation(request, timestamp_ns, fields, labels=tuple(labels))

        try:
            observations, history = await self._statistics_history._paginate_time_window(
                endpoint=endpoint,
                base_params=params,
                bucket="public",
                start_ms=request.start_ms,
                end_ms=request.end_ms,
                page_limit=min(100, request.page_size or request.limit),
                max_records=min(request.limit, 1440),
                max_pages=request.max_pages,
                parser=parse,
                timestamp=lambda item: item.observed_at_ns // 1_000_000,
                merge=self._history._merge_identical,
                cursor_parameter="end",
            )
        except (AdmissionDeadlineExceeded, ProviderRateLimited) as error:
            raise ReferenceProviderExhausted(
                "OKX contract statistics deferred by shared provider admission",
                retry_after_ms=(error.decision.retry_after_ms if isinstance(error, AdmissionDeadlineExceeded)
                                else error.retry_after_ms),
            ) from error
        except (ValueError, PaginationStalled) as error:
            raise ReferenceProviderError(str(error)) from error

        times = [item.observed_at_ns // 1_000_000 for item in observations]
        gaps = any(right - left != interval_ms for left, right in zip(times, times[1:]))
        # Coverage describes supplied sample timestamps, never candle closure.
        # Retention, listing and internal gaps must not inherit the paginator's
        # unconditional right-edge flag or imply padded/continuous history.
        complete_left = bool(times) and times[0] - request.start_ms < interval_ms
        complete_right = bool(times) and request.end_ms - times[-1] < interval_ms
        reason = history.terminal_reason
        if history.truncated and request.limit > 1440 and reason == "MAX_RECORDS":
            reason = "PROVIDER_HISTORY_LIMIT"
        elif not history.truncated:
            if gaps:
                reason = "PROVIDER_GAP"
            elif complete_left and complete_right:
                reason = "REQUEST_WINDOW_COVERED"
            elif times:
                reason = "PROVIDER_PARTIAL"
        return ReferenceFetch(
            observations=tuple(observations),
            lineage=(self._lineage(endpoint, capability),),
            coverage=ReferenceCoverage(
                requested_start_ms=request.start_ms,
                requested_end_ms=request.end_ms,
                observed_min_ms=times[0] if times else None,
                observed_max_ms=times[-1] if times else None,
                complete_left=complete_left and not gaps and not history.truncated,
                complete_right=complete_right and not gaps,
                truncated=history.truncated,
                terminal_reason=reason,
            ),
        )

    async def _funding_history(
        self, request: ReferenceRequest, capability: FeedCapability
    ) -> ReferenceFetch:
        assert request.start_ms is not None and request.end_ms is not None
        result = await self._history.funding_history(
            inst_id=request.instrument.native_symbol,
            start_ms=request.start_ms,
            end_ms=request.end_ms,
            max_records=request.limit,
            max_pages=request.max_pages,
        )
        observations = []
        for row in result.records:
            self._require_inst_id(request, row.inst_id)
            fields = _fields(
                decimal_field("funding_rate", row.funding_rate, "DIMENSIONLESS_RATE"),
                decimal_field("realized_rate", row.realized_rate, "DIMENSIONLESS_RATE"),
            )
            if not fields:
                raise ReferenceProviderError("OKX funding row has no numeric fields")
            labels = [("native_symbol", request.instrument.native_symbol)]
            if row.formula_type:
                labels.append(("formula_type", row.formula_type))
            if row.method:
                labels.append(("method", row.method))
            observations.append(
                ReferenceObservation(
                    instrument_uid=request.instrument.instrument_uid,
                    instrument_revision=request.instrument.metadata_revision,
                    product=request.product,
                    observed_at_ns=row.funding_time_ms * 1_000_000,
                    fields=fields,
                    labels=tuple(labels),
                )
            )
        return ReferenceFetch(
            observations=tuple(observations),
            lineage=(self._lineage("/api/v5/public/funding-rate-history", capability),),
            coverage=self._history_coverage(result.coverage),
        )

    async def _open_interest_snapshot(
        self, request: ReferenceRequest, capability: FeedCapability
    ) -> ReferenceFetch:
        records = await self._history.open_interest_snapshot(
            inst_type=request.instrument.identity.market,
            inst_id=request.instrument.native_symbol,
        )
        exact = [row for row in records if row.inst_id.upper() == request.instrument.native_symbol.upper()]
        if len(exact) > 1:
            raise ReferenceProviderError("OKX open-interest returned duplicate instrument snapshots")
        if not exact:
            return ReferenceFetch(
                observations=(),
                lineage=(self._lineage("/api/v5/public/open-interest", capability),),
                coverage=self._empty_snapshot_coverage("PROVIDER_EMPTY"),
            )
        observation = self._open_interest_observation(request, exact[0])
        return ReferenceFetch(
            observations=(observation,),
            lineage=(self._lineage("/api/v5/public/open-interest", capability),),
            coverage=self._snapshot_coverage((observation,)),
        )

    @staticmethod
    def _open_interest_observation(
        request: ReferenceRequest, row: OkxOpenInterestSnapshot
    ) -> ReferenceObservation:
        fields = _fields(
            decimal_field(
                "open_interest_contracts", row.open_interest_contracts, "CONTRACTS"
            ),
            decimal_field(
                "open_interest_ccy", row.open_interest_ccy, "BASE_ASSET_QUANTITY"
            ),
            decimal_field(
                "open_interest_usd", row.open_interest_usd, "USD_NOTIONAL"
            ),
        )
        if not fields:
            raise ReferenceProviderError("OKX open-interest snapshot has no numeric fields")
        return ReferenceObservation(
            instrument_uid=request.instrument.instrument_uid,
            instrument_revision=request.instrument.metadata_revision,
            product=request.product,
            observed_at_ns=row.observed_ts_ms * 1_000_000,
            fields=fields,
            labels=(
                ("native_symbol", request.instrument.native_symbol),
                ("inst_type", row.inst_type.upper()),
                ("coverage", row.coverage),
            ),
        )

    async def _mark_index_snapshot(
        self,
        request: ReferenceRequest,
        capability: FeedCapability,
        received_at_ns: int,
    ) -> ReferenceFetch:
        observations: list[ReferenceObservation] = []
        lineage = []
        mark_rows: list[dict[str, Any]] | None = None
        index_rows: list[dict[str, Any]] | None = None
        if request.mark_index_kind is MarkIndexKind.BOTH:
            index_id = self._index_id(request)
            mark_rows, index_rows = await asyncio.gather(
                self._client.get(
                    "/api/v5/public/mark-price",
                    params={
                        "instType": request.instrument.identity.market,
                        "instId": request.instrument.native_symbol,
                    },
                    bucket="public",
                ),
                self._client.get(
                    "/api/v5/market/index-tickers",
                    params={"instId": index_id},
                    bucket="market",
                ),
            )
        if request.mark_index_kind in {MarkIndexKind.MARK, MarkIndexKind.BOTH}:
            rows = mark_rows if mark_rows is not None else await self._client.get(
                "/api/v5/public/mark-price",
                params={
                    "instType": request.instrument.identity.market,
                    "instId": request.instrument.native_symbol,
                },
                bucket="public",
            )
            row = self._exact_row(rows, request.instrument.native_symbol, "instId")
            price = decimal_field("mark_price", row.get("markPx"), "QUOTE_PRICE")
            if price is None:
                raise ReferenceProviderError("OKX mark-price response lacks markPx")
            observations.append(
                self._observation(
                    request,
                    self._timestamp_ns_optional(row, "ts") or received_at_ns,
                    (price,),
                    labels=(
                        ("native_symbol", request.instrument.native_symbol),
                        ("price_type", "MARK"),
                        ("timestamp_origin", "PROVIDER" if row.get("ts") else "RECEIVED_AT"),
                    ),
                )
            )
            lineage.append(self._lineage("/api/v5/public/mark-price", capability))
        if request.mark_index_kind in {MarkIndexKind.INDEX, MarkIndexKind.BOTH}:
            index_id = self._index_id(request)
            rows = index_rows if index_rows is not None else await self._client.get(
                "/api/v5/market/index-tickers",
                params={"instId": index_id},
                bucket="market",
            )
            row = self._exact_row(rows, index_id, "instId")
            price = decimal_field("index_price", row.get("idxPx"), "QUOTE_PRICE")
            if price is None:
                raise ReferenceProviderError("OKX index-ticker response lacks idxPx")
            observations.append(
                self._observation(
                    request,
                    self._timestamp_ns_optional(row, "ts") or received_at_ns,
                    (price,),
                    labels=(
                        ("index_id", index_id),
                        ("price_type", "INDEX"),
                        ("timestamp_origin", "PROVIDER" if row.get("ts") else "RECEIVED_AT"),
                    ),
                )
            )
            lineage.append(self._lineage("/api/v5/market/index-tickers", capability))
        return ReferenceFetch(
            observations=tuple(observations),
            lineage=tuple(lineage),
            coverage=self._snapshot_coverage(tuple(observations)),
        )

    async def _contract_metadata(
        self,
        request: ReferenceRequest,
        capability: FeedCapability,
        received_at_ns: int,
    ) -> ReferenceFetch:
        rows = await self._client.get(
            "/api/v5/public/instruments",
            params={
                "instType": request.instrument.identity.market,
                "instId": request.instrument.native_symbol,
            },
            bucket="instruments",
        )
        row = self._exact_row(rows, request.instrument.native_symbol, "instId")
        fields = _fields(
            decimal_field("price_tick", row.get("tickSz"), "QUOTE_PRICE"),
            decimal_field("quantity_step", row.get("lotSz"), "CONTRACTS"),
            decimal_field("minimum_quantity", row.get("minSz"), "CONTRACTS"),
            decimal_field("contract_value", row.get("ctVal"), "CONTRACT_VALUE_NATIVE"),
            decimal_field("contract_multiplier", row.get("ctMult"), "MULTIPLIER"),
            decimal_field("expiry_time_ms", row.get("expTime"), "EPOCH_MILLISECONDS"),
        )
        if len(fields) < 2:
            raise ReferenceProviderError("OKX instrument metadata lacks price/quantity fields")
        labels = [("native_symbol", request.instrument.native_symbol)]
        for label in ("instType", "instFamily", "uly", "ctType", "state"):
            value = str(row.get(label) or "").strip()
            if value:
                labels.append((label.lower(), value))
        observation = self._observation(
            request,
            received_at_ns,
            fields,
            labels=tuple(labels + [("timestamp_origin", "RECEIVED_AT")]),
        )
        return ReferenceFetch(
            observations=(observation,),
            lineage=(self._lineage("/api/v5/public/instruments", capability),),
            coverage=self._snapshot_coverage((observation,)),
        )

    @staticmethod
    def _require_inst_id(request: ReferenceRequest, inst_id: str) -> None:
        if inst_id.strip().upper() != request.instrument.native_symbol.upper():
            raise ReferenceProviderError("OKX reference row belongs to a different instrument")

    @staticmethod
    def _exact_row(rows: object, expected: str, key: str) -> Mapping[str, Any]:
        if not isinstance(rows, list):
            raise ReferenceProviderError("OKX reference response is not a list")
        matches = [
            row for row in rows
            if isinstance(row, Mapping) and str(row.get(key) or "").strip().upper() == expected.upper()
        ]
        if len(matches) != 1:
            raise ReferenceProviderError("OKX reference response lacks one exact instrument row")
        return matches[0]

    @staticmethod
    def _index_id(request: ReferenceRequest) -> str:
        attributes = request.instrument.attributes
        value = str(attributes.get("index_id") or attributes.get("instFamily") or "").strip()
        if not value:
            raise ReferenceUnavailable(
                "OKX index snapshot requires a registry-supplied index_id or instFamily"
            )
        return value.upper()

    @staticmethod
    def _timestamp_ns_optional(row: Mapping[str, Any], key: str) -> int | None:
        value = row.get(key)
        if value in (None, ""):
            return None
        try:
            timestamp_ms = int(str(value))
        except (TypeError, ValueError) as error:
            raise ReferenceProviderError(f"OKX reference timestamp {key} is invalid") from error
        if timestamp_ms <= 0:
            raise ReferenceProviderError(f"OKX reference timestamp {key} must be positive")
        return timestamp_ms * 1_000_000

    @staticmethod
    def _observation(
        request: ReferenceRequest,
        observed_at_ns: int,
        fields,
        *,
        labels: tuple[tuple[str, str], ...],
    ) -> ReferenceObservation:
        return ReferenceObservation(
            instrument_uid=request.instrument.instrument_uid,
            instrument_revision=request.instrument.metadata_revision,
            product=request.product,
            observed_at_ns=observed_at_ns,
            fields=tuple(fields),
            labels=labels,
        )

    @staticmethod
    def _history_coverage(coverage: HistoryCoverage) -> ReferenceCoverage:
        return ReferenceCoverage(
            requested_start_ms=coverage.requested_start_ms,
            requested_end_ms=coverage.requested_end_ms,
            observed_min_ms=coverage.observed_min_ts_ms,
            observed_max_ms=coverage.observed_max_ts_ms,
            complete_left=coverage.complete_left,
            complete_right=coverage.complete_right,
            truncated=coverage.truncated,
            terminal_reason=coverage.terminal_reason,
        )

    @staticmethod
    def _snapshot_coverage(observations: tuple[ReferenceObservation, ...]) -> ReferenceCoverage:
        observed_ms = [item.observed_at_ns // 1_000_000 for item in observations]
        return ReferenceCoverage(
            requested_start_ms=None,
            requested_end_ms=None,
            observed_min_ms=min(observed_ms) if observed_ms else None,
            observed_max_ms=max(observed_ms) if observed_ms else None,
            complete_left=bool(observed_ms),
            complete_right=bool(observed_ms),
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

    @staticmethod
    def _lineage(endpoint: str, capability: FeedCapability):
        return provider_lineage(
            provider="OKX_DIRECT",
            endpoint=endpoint,
            capability_name="reference_data",
            capability=capability,
            adapter_version=_ADAPTER_VERSION,
        )
