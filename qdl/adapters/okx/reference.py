from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from qdl.adapters.okx.client import OkxRestClient
from qdl.adapters.okx.history import (
    HistoryCoverage,
    OkxHistoricalClient,
    OkxOpenInterestSnapshot,
)
from qdl.domain.capabilities import FeedCapability
from qdl.domain.instrument import ProductType
from qdl.reference.contracts import (
    MarkIndexKind,
    ReferenceCoverage,
    ReferenceFetch,
    ReferenceObservation,
    ReferenceProduct,
    ReferenceProviderError,
    ReferenceRequest,
    ReferenceUnavailable,
    decimal_field,
    provider_lineage,
    require_product,
)


_ADAPTER_VERSION = "qdl-okx-reference/1"


def _fields(*items):
    return tuple(item for item in items if item is not None)


class OkxSwapReferenceAdapter:
    """OKX V5 reference adapter that preserves endpoint-specific semantics."""

    def __init__(
        self,
        client: OkxRestClient,
        *,
        history: OkxHistoricalClient | None = None,
    ) -> None:
        self._client = client
        self._history = history if history is not None else OkxHistoricalClient(client)

    async def fetch(
        self,
        request: ReferenceRequest,
        *,
        capability: FeedCapability,
        received_at_ns: int,
    ) -> ReferenceFetch:
        if request.provider_key != ("OKX", "SWAP"):
            raise ReferenceUnavailable("OKX Swap adapter received a different venue/market")
        product = request.product
        if product is ReferenceProduct.FUNDING_RATE:
            require_product(request.instrument, ProductType.PERPETUAL)
            return await self._funding_history(request, capability)
        if product is ReferenceProduct.OPEN_INTEREST:
            require_product(request.instrument, ProductType.PERPETUAL, ProductType.FUTURE)
            if request.is_history:
                raise ReferenceUnavailable(
                    "OKX public open-interest is snapshot-only; no historical series is certified"
                )
            return await self._open_interest_snapshot(request, capability)
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
        if request.mark_index_kind in {MarkIndexKind.MARK, MarkIndexKind.BOTH}:
            rows = await self._client.get(
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
            rows = await self._client.get(
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
