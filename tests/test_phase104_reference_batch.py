from __future__ import annotations

import asyncio
import json
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.providers.binance.rest import BinanceProviderError
from qdl.adapters.binance.reference import BinanceUsdmReferenceAdapter
from qdl.adapters.okx.history import OkxHistoricalClient
from qdl.adapters.okx.reference import OkxSwapReferenceAdapter
from qdl.domain.capabilities import binance_usdm_capabilities
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    ProductType,
)
from qdl.reference.batch import ReferenceBatch, ReferenceBatchPolicy
from qdl.reference.contracts import (
    BasisSeries,
    LongShortKind,
    ReferenceCoverage,
    ReferenceFetch,
    ReferenceObservation,
    ReferenceProduct,
    ReferenceRequest,
    ReferenceStatus,
    decimal_field,
    provider_lineage,
)
from qdl.query import ConsumerGrade, ReferenceDataRequirement, V2QueryService


FIXTURE = Path(__file__).with_name("fixtures") / "phase104" / "binance_funding_pages.json"


def instrument(
    *,
    venue: str,
    market: str,
    product_type: ProductType,
    native_symbol: str,
    base: str,
    quote: str = "USDT",
    attributes: dict[str, str] | None = None,
) -> InstrumentRecord:
    canonical = f"{base}-{quote}"
    if product_type is ProductType.FUTURE:
        canonical = f"{canonical}-260926"
    return InstrumentRecord(
        identity=InstrumentIdentity.create(
            venue=venue,
            market=market,
            product_type=product_type,
            canonical_symbol=canonical,
        ),
        metadata_revision=7,
        asset_class=AssetClass.DERIVATIVE,
        native_symbol=native_symbol,
        base_asset=base,
        quote_asset=quote,
        settlement_asset=quote,
        price_tick=CanonicalDecimal.from_text("0.01"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
        expiry_time_ns=1_900_000_000_000_000_000 if product_type is ProductType.FUTURE else None,
        attributes=attributes or {},
    )


async def no_sleep(_: float) -> None:
    return None


class FakeOkxRest:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str], str]] = []

    async def get(self, path, *, params, bucket, attempts=3):
        copied = dict(params)
        self.calls.append((path, copied, bucket))
        if path == "/api/v5/public/funding-rate-history":
            pages = {
                "401": [
                    {"instId": "BTC-USDT-SWAP", "fundingTime": "400", "fundingRate": "0.0004", "realizedRate": "0.0003", "formulaType": "withRate", "method": "current_period"},
                    {"instId": "BTC-USDT-SWAP", "fundingTime": "300", "fundingRate": "0.0003", "realizedRate": "0.0002", "formulaType": "withRate", "method": "current_period"},
                    {"instId": "BTC-USDT-SWAP", "fundingTime": "200", "fundingRate": "0.0002", "realizedRate": "0.0001", "formulaType": "withRate", "method": "current_period"},
                ],
                "200": [
                    {"instId": "BTC-USDT-SWAP", "fundingTime": "100", "fundingRate": "0.0001", "realizedRate": "0.0000", "formulaType": "withRate", "method": "current_period"},
                ],
            }
            return pages.get(copied.get("after"), [])
        if path == "/api/v5/public/open-interest":
            return [{"instId": "BTC-USDT-SWAP", "instType": "SWAP", "oi": "100.5", "oiCcy": "10.5", "oiUsd": "600000", "ts": "500"}]
        if path == "/api/v5/public/mark-price":
            return [{"instId": "BTC-USDT-SWAP", "markPx": "60010.01", "ts": "501"}]
        if path == "/api/v5/market/index-tickers":
            return [{"instId": "BTC-USDT", "idxPx": "60000.01", "ts": "502"}]
        if path == "/api/v5/public/instruments":
            return [{
                "instId": "BTC-USDT-SWAP", "instType": "SWAP", "instFamily": "BTC-USDT",
                "tickSz": "0.1", "lotSz": "0.01", "minSz": "0.01", "ctVal": "0.01", "ctMult": "1", "state": "live",
            }]
        raise AssertionError(f"unexpected OKX endpoint: {path}")


class BlockingAdapter:
    def __init__(self) -> None:
        self.calls = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def fetch(self, request, *, capability, received_at_ns):
        self.calls += 1
        self.started.set()
        await self.release.wait()
        observation = ReferenceObservation(
            instrument_uid=request.instrument.instrument_uid,
            instrument_revision=request.instrument.metadata_revision,
            product=request.product,
            observed_at_ns=received_at_ns,
            fields=(decimal_field("open_interest_contracts", "1", "CONTRACTS"),),
            labels=(("native_symbol", request.instrument.native_symbol),),
        )
        return ReferenceFetch(
            observations=(observation,),
            lineage=(provider_lineage(provider="BINANCE_DIRECT", endpoint="test", capability_name="open_interest", capability=capability, adapter_version="test"),),
            coverage=ReferenceCoverage(None, None, received_at_ns // 1_000_000, received_at_ns // 1_000_000, True, True, False, "TEST"),
        )


class ReferenceRequestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.btc = instrument(
            venue="BINANCE", market="USDM", product_type=ProductType.PERPETUAL,
            native_symbol="BTCUSDT", base="BTC",
        )

    def test_history_products_require_bounded_window_and_interval_when_applicable(self):
        with self.assertRaisesRegex(ValueError, "FUNDING_RATE requires"):
            ReferenceRequest(instrument=self.btc, product=ReferenceProduct.FUNDING_RATE)
        with self.assertRaisesRegex(ValueError, "sampling interval"):
            ReferenceRequest(
                instrument=self.btc,
                product=ReferenceProduct.LONG_SHORT_RATIO,
                start_ms=100,
                end_ms=200,
                long_short_kind=LongShortKind.GLOBAL_ACCOUNT,
            )


class BinanceReferenceBatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.btc = instrument(
            venue="BINANCE", market="USDM", product_type=ProductType.PERPETUAL,
            native_symbol="BTCUSDT", base="BTC",
        )
        self.eth = instrument(
            venue="BINANCE", market="USDM", product_type=ProductType.PERPETUAL,
            native_symbol="ETHUSDT", base="ETH",
        )
        self.fixture_bytes = FIXTURE.read_bytes()

    def _funding_fetcher(self, calls):
        payload = json.loads(self.fixture_bytes)

        def fetch(symbol, *, start_time, **kwargs):
            calls.append((symbol.upper(), start_time))
            return {"data": payload[symbol.upper()].get(str(start_time), [])}

        return fetch

    async def test_paginated_overlap_dedup_and_concurrent_symbol_isolation(self):
        calls = []
        adapter = BinanceUsdmReferenceAdapter(
            funding_fetcher=self._funding_fetcher(calls), max_attempts=1, sleep=no_sleep
        )
        batch = ReferenceBatch({("BINANCE", "USDM"): adapter})
        request = lambda item: ReferenceRequest(
            instrument=item, product=ReferenceProduct.FUNDING_RATE,
            start_ms=100, end_ms=400, limit=4, page_size=2,
        )
        btc_result, eth_result = await batch.fetch((request(self.btc), request(self.eth)))
        self.assertEqual(btc_result.status, ReferenceStatus.OK)
        self.assertEqual(eth_result.status, ReferenceStatus.OK)
        self.assertEqual([item.observed_at_ns // 1_000_000 for item in btc_result.observations], [100, 200, 300, 400])
        self.assertEqual([item.instrument_uid for item in btc_result.observations], [self.btc.instrument_uid] * 4)
        self.assertEqual([item.instrument_uid for item in eth_result.observations], [self.eth.instrument_uid] * 4)
        self.assertEqual(btc_result.observations[0].fields[0].value.source_text, "0.00010000")
        self.assertTrue(btc_result.coverage.complete_left)
        self.assertTrue(btc_result.coverage.complete_right)
        self.assertEqual(sorted(calls), [("BTCUSDT", 100), ("BTCUSDT", 201), ("BTCUSDT", 301), ("ETHUSDT", 100), ("ETHUSDT", 201), ("ETHUSDT", 301)])

    async def test_funding_boundary_jitter_is_tolerated_without_rewriting_raw_time(self):
        calls = []

        def funding(symbol, *, start_time, end_time, limit, **kwargs):
            del end_time, limit, kwargs
            calls.append((symbol, start_time))
            rows = {
                1_000: [
                    {"symbol": symbol, "fundingRate": "0.1", "fundingTime": "1003"},
                    {"symbol": symbol, "fundingRate": "0.2", "fundingTime": "2000"},
                ],
                2_001: [
                    {"symbol": symbol, "fundingRate": "0.3", "fundingTime": "3003"},
                ],
            }
            return {"data": rows[start_time]}

        result = await ReferenceBatch({("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(
            funding_fetcher=funding, max_attempts=1, sleep=no_sleep,
        )}).fetch_one(ReferenceRequest(
            instrument=self.btc,
            product=ReferenceProduct.FUNDING_RATE,
            start_ms=1_000,
            end_ms=3_005,
            limit=3,
            page_size=2,
            max_pages=2,
        ))

        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertEqual(
            [item.observed_at_ns // 1_000_000 for item in result.observations],
            [1_003, 2_000, 3_003],
        )
        self.assertTrue(result.coverage.complete_left)
        self.assertTrue(result.coverage.complete_right)
        self.assertEqual(calls, [("BTCUSDT", 1_000), ("BTCUSDT", 2_001)])

    async def test_funding_boundary_gap_beyond_tolerance_remains_partial(self):
        def funding(symbol, **kwargs):
            del kwargs
            return {"data": [
                {"symbol": symbol, "fundingRate": "0.1", "fundingTime": "61001"},
                {"symbol": symbol, "fundingRate": "0.2", "fundingTime": "200000"},
            ]}

        result = await ReferenceBatch({("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(
            funding_fetcher=funding, max_attempts=1, sleep=no_sleep,
        )}).fetch_one(ReferenceRequest(
            instrument=self.btc,
            product=ReferenceProduct.FUNDING_RATE,
            start_ms=1_000,
            end_ms=200_000,
            limit=2,
            page_size=2,
        ))

        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertFalse(result.coverage.complete_left)
        self.assertTrue(result.coverage.complete_right)

    async def test_retry_exhaustion_is_isolated_error_not_zero(self):
        calls = {"count": 0}

        def failing(*args, **kwargs):
            calls["count"] += 1
            raise BinanceProviderError("timeout")

        batch = ReferenceBatch({("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(funding_fetcher=failing, max_attempts=2, sleep=no_sleep)})
        result = await batch.fetch_one(ReferenceRequest(
            instrument=self.btc, product=ReferenceProduct.FUNDING_RATE,
            start_ms=100, end_ms=200,
        ))
        self.assertEqual(calls["count"], 2)
        self.assertEqual(result.status, ReferenceStatus.ERROR)
        self.assertEqual(result.error_code, "PROVIDER_RETRY_EXHAUSTED")
        self.assertEqual(result.observations, ())

    async def test_binance_metric_family_preserves_units_and_explicit_missing(self):
        def funding(*args, **kwargs):
            return {"data": []}

        def metric(endpoint, symbol, period, limit, start_time, end_time, **kwargs):
            self.assertEqual(symbol, "BTCUSDT")
            self.assertEqual(period, "1h")
            self.assertEqual(endpoint, "open_interest_hist")
            return {"data": [{"symbol": symbol, "sumOpenInterest": "123", "sumOpenInterestValue": "456.70", "timestamp": "200"}]}

        def long_short(kind, symbol, period, limit, start_time, end_time, **kwargs):
            self.assertEqual(kind, "top_position")
            return {"data": [{"symbol": symbol, "longShortRatio": "1.25", "longAccount": "0.55", "shortAccount": "0.45", "timestamp": "200"}]}

        def taker(symbol, period, limit, start_time, end_time, **kwargs):
            return {"data": [{"symbol": symbol, "buySellRatio": "1.10", "buyVol": "100.25", "sellVol": "91.25", "timestamp": "200"}]}

        adapter = BinanceUsdmReferenceAdapter(
            funding_fetcher=funding,
            metric_history_fetcher=metric,
            long_short_fetcher=long_short,
            taker_fetcher=taker,
            max_attempts=1,
            sleep=no_sleep,
        )
        batch = ReferenceBatch({("BINANCE", "USDM"): adapter})
        missing, oi, ratios, taker_flow = await batch.fetch((
            ReferenceRequest(instrument=self.btc, product=ReferenceProduct.FUNDING_RATE, start_ms=100, end_ms=200),
            ReferenceRequest(instrument=self.btc, product=ReferenceProduct.OPEN_INTEREST, start_ms=100, end_ms=200, interval="1h"),
            ReferenceRequest(instrument=self.btc, product=ReferenceProduct.LONG_SHORT_RATIO, start_ms=100, end_ms=200, interval="1h", long_short_kind=LongShortKind.TOP_POSITION),
            ReferenceRequest(instrument=self.btc, product=ReferenceProduct.TAKER_FLOW, start_ms=100, end_ms=200, interval="1h"),
        ))
        self.assertEqual(missing.status, ReferenceStatus.MISSING)
        self.assertEqual(oi.status, ReferenceStatus.OK)
        self.assertEqual({field.name: field.unit for field in oi.observations[0].fields}["open_interest_quote_notional"], "QUOTE_NOTIONAL")
        self.assertEqual(ratios.status, ReferenceStatus.OK)
        self.assertIn(("ratio_kind", "TOP_POSITION"), ratios.observations[0].labels)
        self.assertEqual(taker_flow.status, ReferenceStatus.OK)
        self.assertEqual({field.name: field.unit for field in taker_flow.observations[0].fields}["buy_volume"], "PROVIDER_NATIVE_VOLUME")

    async def test_period_start_taker_and_basis_cover_their_completed_right_boundary(self):
        hour_ms = 3_600_000

        def taker(symbol, period, limit, start_time, end_time, **kwargs):
            del period, limit, start_time, end_time, kwargs
            return {"data": [
                {"symbol": symbol, "buySellRatio": "1.1", "buyVol": "2", "sellVol": "3", "timestamp": str(hour_ms)},
                {"symbol": symbol, "buySellRatio": "1.2", "buyVol": "4", "sellVol": "5", "timestamp": str(2 * hour_ms)},
            ]}

        def basis(pair, contract_type, period, limit, start_time, end_time, **kwargs):
            del contract_type, period, limit, start_time, end_time, kwargs
            return {"data": [
                {"pair": pair, "contractType": "PERPETUAL", "basis": "1", "timestamp": str(hour_ms)},
                {"pair": pair, "contractType": "PERPETUAL", "basis": "2", "timestamp": str(2 * hour_ms)},
            ]}

        adapter = BinanceUsdmReferenceAdapter(
            taker_fetcher=taker,
            basis_fetcher=basis,
            max_attempts=1,
            sleep=no_sleep,
        )
        batch = ReferenceBatch({("BINANCE", "USDM"): adapter})
        taker_result, basis_result = await batch.fetch((
            ReferenceRequest(
                instrument=self.btc,
                product=ReferenceProduct.TAKER_FLOW,
                start_ms=hour_ms,
                end_ms=3 * hour_ms - 1,
                interval="1h",
                limit=2,
            ),
            ReferenceRequest(
                instrument=self.btc,
                product=ReferenceProduct.BASIS,
                start_ms=hour_ms,
                end_ms=3 * hour_ms - 1,
                interval="1h",
                limit=2,
                basis_contract_type="PERPETUAL",
            ),
        ))

        self.assertTrue(taker_result.coverage.complete_left)
        self.assertTrue(taker_result.coverage.complete_right)
        self.assertFalse(taker_result.coverage.truncated)
        self.assertTrue(basis_result.coverage.complete_left)
        self.assertTrue(basis_result.coverage.complete_right)
        self.assertFalse(basis_result.coverage.truncated)

    async def test_taker_provider_window_advances_one_period_without_changing_logical_coverage(self):
        hour_ms = 3_600_000
        calls = []

        def taker(symbol, period, limit, start_time, end_time, **kwargs):
            del period, kwargs
            calls.append((symbol, limit, start_time, end_time))
            return {"data": [
                {"symbol": symbol, "buySellRatio": "1.1", "buyVol": "2", "sellVol": "3", "timestamp": str(hour_ms)},
                {"symbol": symbol, "buySellRatio": "1.2", "buyVol": "4", "sellVol": "5", "timestamp": str(2 * hour_ms)},
            ]}

        adapter = BinanceUsdmReferenceAdapter(
            taker_fetcher=taker,
            max_attempts=1,
            sleep=no_sleep,
        )
        result = await ReferenceBatch({("BINANCE", "USDM"): adapter}).fetch_one(
            ReferenceRequest(
                instrument=self.btc,
                product=ReferenceProduct.TAKER_FLOW,
                start_ms=hour_ms,
                end_ms=3 * hour_ms - 1,
                interval="1h",
                limit=2,
            )
        )

        self.assertEqual(calls, [("BTCUSDT", 2, 2 * hour_ms, 4 * hour_ms - 1)])
        self.assertEqual(
            [item.observed_at_ns // 1_000_000 for item in result.observations],
            [hour_ms, 2 * hour_ms],
        )
        self.assertTrue(result.coverage.complete_left)
        self.assertTrue(result.coverage.complete_right)
        self.assertFalse(result.coverage.truncated)

    async def test_latest_first_metric_history_paginates_backward_without_losing_left_coverage(self):
        calls = []

        def metric(endpoint, symbol, period, limit, start_time, end_time, **kwargs):
            self.assertEqual(endpoint, "open_interest_hist")
            self.assertEqual(symbol, "BTCUSDT")
            self.assertEqual(period, "1h")
            calls.append((start_time, end_time, limit))
            rows = {
                400: [
                    {"symbol": symbol, "sumOpenInterest": "30", "timestamp": "300"},
                    {"symbol": symbol, "sumOpenInterest": "40", "timestamp": "400"},
                ],
                299: [
                    {"symbol": symbol, "sumOpenInterest": "10", "timestamp": "100"},
                    {"symbol": symbol, "sumOpenInterest": "20", "timestamp": "200"},
                ],
            }
            return {"data": rows[end_time]}

        adapter = BinanceUsdmReferenceAdapter(
            metric_history_fetcher=metric,
            max_attempts=1,
            sleep=no_sleep,
        )
        result = await ReferenceBatch({("BINANCE", "USDM"): adapter}).fetch_one(
            ReferenceRequest(
                instrument=self.btc,
                product=ReferenceProduct.OPEN_INTEREST,
                start_ms=100,
                end_ms=400,
                interval="1h",
                limit=4,
                page_size=2,
                max_pages=2,
            )
        )

        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertEqual(
            [item.observed_at_ns // 1_000_000 for item in result.observations],
            [100, 200, 300, 400],
        )
        self.assertTrue(result.coverage.complete_left)
        self.assertTrue(result.coverage.complete_right)
        self.assertEqual(calls, [(100, 400, 2), (100, 299, 2)])

    async def test_metric_internal_gap_is_typed_partial_even_when_boundaries_match(self):
        def metric(endpoint, symbol, period, limit, start_time, end_time, **kwargs):
            del endpoint, symbol, period, limit, start_time, kwargs
            rows = {
                4_000: [
                    {"symbol": "BTCUSDT", "sumOpenInterest": "30", "timestamp": "3000"},
                    {"symbol": "BTCUSDT", "sumOpenInterest": "40", "timestamp": "4000"},
                ],
                2_999: [{"symbol": "BTCUSDT", "sumOpenInterest": "10", "timestamp": "1000"}],
            }
            return {"data": rows[end_time]}

        adapter = BinanceUsdmReferenceAdapter(
            metric_history_fetcher=metric,
            max_attempts=1,
            sleep=no_sleep,
        )
        result = await ReferenceBatch({("BINANCE", "USDM"): adapter}).fetch_one(
            ReferenceRequest(
                instrument=self.btc,
                product=ReferenceProduct.OPEN_INTEREST,
                start_ms=1_000,
                end_ms=4_000,
                interval="1s",
                limit=4,
                page_size=2,
                max_pages=2,
            )
        )

        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertTrue(result.coverage.complete_left)
        self.assertTrue(result.coverage.complete_right)
        self.assertTrue(result.coverage.truncated)
        self.assertEqual(result.coverage.terminal_reason, "INTERNAL_GAP")

    async def test_cross_instrument_provider_row_is_rejected(self):
        def wrong_symbol(*args, **kwargs):
            return {"data": [{"symbol": "ETHUSDT", "fundingRate": "0.1", "fundingTime": "100"}]}

        batch = ReferenceBatch({("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(funding_fetcher=wrong_symbol, max_attempts=1, sleep=no_sleep)})
        result = await batch.fetch_one(ReferenceRequest(
            instrument=self.btc, product=ReferenceProduct.FUNDING_RATE,
            start_ms=100, end_ms=200,
        ))
        self.assertEqual(result.status, ReferenceStatus.ERROR)
        self.assertEqual(result.error_code, "PROVIDER_PROTOCOL")

    async def test_malformed_provider_decimal_is_protocol_error(self):
        def malformed_decimal(*args, **kwargs):
            return {
                "data": [
                    {
                        "symbol": "BTCUSDT",
                        "fundingRate": "not-a-decimal",
                        "fundingTime": "100",
                    }
                ]
            }

        batch = ReferenceBatch({
            ("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(
                funding_fetcher=malformed_decimal,
                max_attempts=1,
                sleep=no_sleep,
            )
        })
        result = await batch.fetch_one(ReferenceRequest(
            instrument=self.btc,
            product=ReferenceProduct.FUNDING_RATE,
            start_ms=100,
            end_ms=200,
        ))
        self.assertEqual(result.status, ReferenceStatus.ERROR)
        self.assertEqual(result.error_code, "PROVIDER_PROTOCOL")
        self.assertEqual(result.observations, ())

    async def test_mark_metadata_and_native_basis_preserve_selector_and_decimal_text(self):
        def mark(*args, **kwargs):
            return {"data": {"symbol": "BTCUSDT", "markPrice": "60000.0100", "indexPrice": "59999.9900", "time": "600"}}

        def exchange(*args, **kwargs):
            return {"data": {"symbols": [{"symbol": "BTCUSDT", "contractType": "PERPETUAL", "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.10"}, {"filterType": "LOT_SIZE", "stepSize": "0.001"}]}]}}

        def basis(*args, **kwargs):
            return {"data": [{"pair": "BTCUSDT", "contractType": "PERPETUAL", "indexPrice": "60000.00", "basis": "10.0000", "annualizedBasisRate": "0.123400", "timestamp": "700"}]}

        adapter = BinanceUsdmReferenceAdapter(mark_index_fetcher=mark, exchange_info_fetcher=exchange, basis_fetcher=basis, max_attempts=1, sleep=no_sleep)
        batch = ReferenceBatch({("BINANCE", "USDM"): adapter})
        mark_result = await batch.fetch_one(ReferenceRequest(instrument=self.btc, product=ReferenceProduct.MARK_INDEX_PRICE))
        metadata_result = await batch.fetch_one(ReferenceRequest(instrument=self.btc, product=ReferenceProduct.CONTRACT_METADATA))
        basis_result = await batch.fetch_one(ReferenceRequest(
            instrument=self.btc, product=ReferenceProduct.BASIS, start_ms=100, end_ms=700,
            interval="1d", basis_series=BasisSeries.NATIVE, basis_contract_type="PERPETUAL",
        ))
        self.assertEqual(mark_result.status, ReferenceStatus.OK)
        self.assertEqual(mark_result.observations[0].fields[0].value.source_text, "60000.0100")
        self.assertEqual(metadata_result.observations[0].fields[0].value.source_text, "0.10")
        self.assertEqual(basis_result.status, ReferenceStatus.OK)
        self.assertIn(("basis_series", "NATIVE"), basis_result.observations[0].labels)
        self.assertIn(("contract_selector", "PERPETUAL"), basis_result.observations[0].labels)
        self.assertEqual(
            {field.name: field.value.source_text for field in basis_result.observations[0].fields}["annualized_basis_rate"],
            "0.123400",
        )

    async def test_native_basis_retries_only_a_transient_non_list_envelope(self):
        calls = {"count": 0}

        def basis(*_args, **_kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                return {"data": {"code": "TRANSIENT"}}
            return {"data": [
                {
                    "pair": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "basis": "10.0000",
                    "timestamp": "701",
                }
            ]}

        result = await ReferenceBatch({
            ("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(
                basis_fetcher=basis,
                max_attempts=2,
                sleep=no_sleep,
            )
        }).fetch_one(ReferenceRequest(
            instrument=self.btc,
            product=ReferenceProduct.BASIS,
            start_ms=700,
            end_ms=701,
            interval="1h",
            limit=1,
            page_size=1,
            max_pages=1,
            basis_series=BasisSeries.NATIVE,
            basis_contract_type="PERPETUAL",
        ))

        self.assertEqual(calls["count"], 2)
        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertEqual(result.observations[0].fields[0].value.source_text, "10.0000")

    async def test_native_basis_transient_envelope_exhaustion_stays_typed_error(self):
        calls = {"count": 0}

        def basis(*_args, **_kwargs):
            calls["count"] += 1
            return {"data": {"code": "TRANSIENT"}}

        result = await ReferenceBatch({
            ("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(
                basis_fetcher=basis,
                max_attempts=1,
                sleep=no_sleep,
            )
        }).fetch_one(ReferenceRequest(
            instrument=self.btc,
            product=ReferenceProduct.BASIS,
            start_ms=700,
            end_ms=701,
            interval="1h",
            limit=1,
            page_size=1,
            max_pages=1,
            basis_series=BasisSeries.NATIVE,
            basis_contract_type="PERPETUAL",
        ))

        self.assertEqual(calls["count"], 4)
        self.assertEqual(result.status, ReferenceStatus.ERROR)
        self.assertEqual(result.error_code, "PROVIDER_RETRY_EXHAUSTED")
        self.assertEqual(result.observations, ())

    async def test_rate_limit_hint_propagates_without_hot_adapter_retry(self):
        calls = {"count": 0}

        def funding(*_args, **_kwargs):
            calls["count"] += 1
            raise BinanceProviderError(
                "rate limited",
                retry_after_ms=120_000,
            )

        result = await ReferenceBatch({
            ("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(
                funding_fetcher=funding,
                max_attempts=3,
                sleep=no_sleep,
            )
        }).fetch_one(ReferenceRequest(
            instrument=self.btc,
            product=ReferenceProduct.FUNDING_RATE,
            start_ms=100,
            end_ms=200,
            limit=1,
            page_size=1,
            max_pages=1,
        ))

        self.assertEqual(calls["count"], 1)
        self.assertEqual(result.status, ReferenceStatus.ERROR)
        self.assertEqual(result.error_code, "PROVIDER_RETRY_EXHAUSTED")
        self.assertEqual(result.retry_after_ms, 120_000)

        requirement = ReferenceDataRequirement(
            instrument_uid=self.btc.instrument_uid,
            product=ReferenceProduct.FUNDING_RATE,
            start_time_ns=100 * 1_000_000,
            end_time_ns=200 * 1_000_000,
            limit=1,
            page_size=1,
            max_pages=1,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="test-rate-limit",
        )
        problem = V2QueryService._reference_problem(
            None, requirement, result.request, result
        )
        self.assertIsNotNone(problem)
        assert problem is not None
        self.assertTrue(problem.retryable)
        self.assertEqual(problem.retry_after_ms, 120_000)

    async def test_native_basis_serializes_only_its_provider_pair_lane(self):
        active = {"value": 0, "maximum": 0}
        lock = threading.Lock()

        def basis(pair, *_args, **_kwargs):
            with lock:
                active["value"] += 1
                active["maximum"] = max(active["maximum"], active["value"])
            time.sleep(0.01)
            with lock:
                active["value"] -= 1
            return {"data": [
                {
                    "pair": pair,
                    "contractType": "PERPETUAL",
                    "basis": "10.0000",
                    "timestamp": "701",
                }
            ]}

        adapter = BinanceUsdmReferenceAdapter(
            basis_fetcher=basis,
            max_attempts=1,
            sleep=no_sleep,
        )
        requests = tuple(
            ReferenceRequest(
                instrument=instrument,
                product=ReferenceProduct.BASIS,
                start_ms=700,
                end_ms=701,
                interval="1h",
                limit=1,
                page_size=1,
                max_pages=1,
                basis_series=BasisSeries.NATIVE,
                basis_contract_type="PERPETUAL",
            )
            for instrument in (self.btc, self.eth)
        )
        left, right = await ReferenceBatch({("BINANCE", "USDM"): adapter}).fetch(requests)

        self.assertEqual(active["maximum"], 1)
        self.assertEqual((left.status, right.status), (ReferenceStatus.OK, ReferenceStatus.OK))

    async def test_native_basis_paces_only_the_second_start(self):
        sleeps = []
        monotonic_values = iter((0, 0, 0, 500_000_000))

        async def paced_sleep(seconds):
            sleeps.append(seconds)

        def basis(pair, *_args, **_kwargs):
            return {"data": [{
                "pair": pair,
                "contractType": "PERPETUAL",
                "basis": "10.0000",
                "timestamp": "701",
            }]}

        adapter = BinanceUsdmReferenceAdapter(
            basis_fetcher=basis,
            max_attempts=1,
            sleep=paced_sleep,
            monotonic_ns=lambda: next(monotonic_values),
        )
        requests = tuple(
            ReferenceRequest(
                instrument=instrument,
                product=ReferenceProduct.BASIS,
                start_ms=700,
                end_ms=701,
                interval="1h",
                limit=1,
                page_size=1,
                max_pages=1,
                basis_series=BasisSeries.NATIVE,
                basis_contract_type="PERPETUAL",
            )
            for instrument in (self.btc, self.eth)
        )
        left, right = await ReferenceBatch({("BINANCE", "USDM"): adapter}).fetch(requests)

        self.assertEqual((left.status, right.status), (ReferenceStatus.OK, ReferenceStatus.OK))
        self.assertEqual(sleeps, [0.5])

    async def test_continuous_vision_basis_is_memory_only_and_requires_a_complete_daily_window(self):
        day_ms = 86_400_000
        start_ms = 20_000 * day_ms
        end_ms = start_ms + 29 * day_ms
        calls = []

        def continuous(pair, **kwargs):
            calls.append((pair, kwargs))
            self.assertEqual(pair, "BTCUSDT")
            self.assertFalse(kwargs["persist_cache"])
            self.assertIsNone(kwargs["fallback_url"])
            self.assertEqual(kwargs["lookback_days"], 30)
            rows = []
            for timestamp_ms in range(start_ms, end_ms + day_ms, day_ms):
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                rows.append({
                    "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "perpetual_close": "60000.00",
                    "quarterly_close": "60010.00",
                    "basis": "10.00",
                    "days_to_expiry": "20",
                    "active_contract": "BTCUSDT_270626",
                })
            return {
                "data": rows,
                "meta": {"source_components": ("BINANCE_VISION", "BINANCE_USDM_REST")},
            }

        adapter = BinanceUsdmReferenceAdapter(
            continuous_basis_fetcher=continuous,
            max_attempts=1,
            sleep=no_sleep,
        )
        batch = ReferenceBatch({("BINANCE", "USDM"): adapter})
        result = await batch.fetch_one(ReferenceRequest(
            instrument=self.btc,
            product=ReferenceProduct.BASIS,
            start_ms=start_ms,
            end_ms=end_ms,
            interval="1d",
            limit=30,
            basis_series=BasisSeries.CONTINUOUS,
            basis_contract_type="CURRENT_QUARTER",
        ))
        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertTrue(result.coverage.complete_left)
        self.assertTrue(result.coverage.complete_right)
        self.assertFalse(result.coverage.truncated)
        self.assertEqual(len(result.observations), 30)
        self.assertEqual(
            result.observations[0].observed_at_ns // 1_000_000,
            start_ms + day_ms - 1,
        )
        self.assertEqual(calls[0][1]["end_time"], datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc))
        self.assertIn(("active_contract", "BTCUSDT_270626"), result.observations[0].labels)
        self.assertIn(("period_open_time_ms", str(start_ms)), result.observations[0].labels)
        self.assertIn(("period_close_time_ms", str(start_ms + day_ms - 1)), result.observations[0].labels)
        self.assertEqual(
            [item.provider_endpoint for item in result.lineage],
            ["https://data.binance.vision/data/futures/um", "/fapi/v1/klines"],
        )

    async def test_dated_future_cannot_be_mislabeled_as_native_basis(self):
        dated = instrument(
            venue="BINANCE", market="USDM", product_type=ProductType.FUTURE,
            native_symbol="BTCUSDT_260926", base="BTC", attributes={"contractType": "CURRENT_QUARTER"},
        )
        batch = ReferenceBatch({("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(max_attempts=1, sleep=no_sleep)})
        result = await batch.fetch_one(ReferenceRequest(
            instrument=dated, product=ReferenceProduct.BASIS, start_ms=100, end_ms=200,
            interval="1d", basis_contract_type="CURRENT_QUARTER",
        ))
        self.assertEqual(result.status, ReferenceStatus.UNAVAILABLE)
        self.assertEqual(result.error_code, "CAPABILITY_UNAVAILABLE")

    async def test_continuous_basis_rejects_an_unclosed_daily_period(self):
        day_ms = 86_400_000
        now_ms = 24_000 * day_ms
        calls = []

        def continuous(*_args, **_kwargs):
            calls.append(True)
            return {"data": []}

        adapter = BinanceUsdmReferenceAdapter(
            continuous_basis_fetcher=continuous,
            max_attempts=1,
            sleep=no_sleep,
            clock_ns=lambda: now_ms * 1_000_000,
        )
        result = await ReferenceBatch({("BINANCE", "USDM"): adapter}).fetch_one(
            ReferenceRequest(
                instrument=self.btc,
                product=ReferenceProduct.BASIS,
                start_ms=now_ms - 30 * day_ms,
                end_ms=now_ms,
                interval="1d",
                limit=31,
                basis_series=BasisSeries.CONTINUOUS,
                basis_contract_type="CURRENT_QUARTER",
            )
        )

        self.assertEqual(result.status, ReferenceStatus.ERROR)
        self.assertEqual(result.error_code, "PROVIDER_PROTOCOL")
        self.assertFalse(calls)

    async def test_inflight_coalescing_uses_one_provider_fetch(self):
        adapter = BlockingAdapter()
        batch = ReferenceBatch({("BINANCE", "USDM"): adapter}, policy=ReferenceBatchPolicy(snapshot_ttl_seconds=0))
        request = ReferenceRequest(instrument=self.btc, product=ReferenceProduct.OPEN_INTEREST)
        first = asyncio.create_task(batch.fetch_one(request))
        await adapter.started.wait()
        second = asyncio.create_task(batch.fetch_one(request))
        await asyncio.sleep(0)
        adapter.release.set()
        left, right = await asyncio.gather(first, second)
        self.assertEqual(adapter.calls, 1)
        self.assertEqual({left.coalesced, right.coalesced}, {False, True})


class OkxReferenceBatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.btc = instrument(
            venue="OKX", market="SWAP", product_type=ProductType.PERPETUAL,
            native_symbol="BTC-USDT-SWAP", base="BTC", attributes={"instFamily": "BTC-USDT"},
        )
        self.rest = FakeOkxRest()
        self.adapter = OkxSwapReferenceAdapter(self.rest, history=OkxHistoricalClient(self.rest))
        self.batch = ReferenceBatch({("OKX", "SWAP"): self.adapter})

    async def test_funding_history_and_snapshot_units_are_provider_truthful(self):
        funding = await self.batch.fetch_one(ReferenceRequest(
            instrument=self.btc, product=ReferenceProduct.FUNDING_RATE,
            start_ms=100, end_ms=400, limit=4,
        ))
        oi = await self.batch.fetch_one(ReferenceRequest(instrument=self.btc, product=ReferenceProduct.OPEN_INTEREST))
        self.assertEqual(funding.status, ReferenceStatus.OK)
        self.assertEqual([item.observed_at_ns // 1_000_000 for item in funding.observations], [100, 200, 300, 400])
        self.assertEqual(oi.status, ReferenceStatus.OK)
        self.assertEqual({item.name: item.unit for item in oi.observations[0].fields}["open_interest_usd"], "USD_NOTIONAL")

    async def test_mark_index_and_metadata_do_not_cross_mix_index_identity(self):
        prices = await self.batch.fetch_one(ReferenceRequest(instrument=self.btc, product=ReferenceProduct.MARK_INDEX_PRICE))
        metadata = await self.batch.fetch_one(ReferenceRequest(instrument=self.btc, product=ReferenceProduct.CONTRACT_METADATA))
        self.assertEqual(prices.status, ReferenceStatus.OK)
        self.assertEqual(len(prices.observations), 2)
        self.assertIn(("index_id", "BTC-USDT"), prices.observations[1].labels)
        self.assertEqual(metadata.status, ReferenceStatus.OK)
        self.assertIn("price_tick", {item.name for item in metadata.observations[0].fields})

    async def test_unavailable_okx_products_are_explicit_and_make_no_provider_call(self):
        before = len(self.rest.calls)
        long_short = await self.batch.fetch_one(ReferenceRequest(
            instrument=self.btc, product=ReferenceProduct.LONG_SHORT_RATIO,
            start_ms=100, end_ms=200, interval="1h", long_short_kind=LongShortKind.GLOBAL_ACCOUNT,
        ))
        basis = await self.batch.fetch_one(ReferenceRequest(
            instrument=self.btc, product=ReferenceProduct.BASIS,
            start_ms=100, end_ms=200, interval="1h", basis_contract_type="CURRENT_QUARTER",
        ))
        self.assertEqual(long_short.status, ReferenceStatus.UNAVAILABLE)
        self.assertEqual(basis.status, ReferenceStatus.UNAVAILABLE)
        self.assertEqual(len(self.rest.calls), before)


if __name__ == "__main__":
    unittest.main()
