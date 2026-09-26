"""OKX exact-contract statistics. All fixtures are synthetic UNIT_TEST data.

Public smoke is opt-in, public GET only, bounded to ten calls and no raw storage.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import time
import threading
import unittest
from unittest.mock import AsyncMock, patch

import httpx
import requests

from qdl.admission.http import RustHttpProviderAdmission
from qdl.admission.contracts import AdmissionPriority, ProviderLane
from qdl.admission.edge import BlockingProviderAdmission
from qdl.adapters.okx.admitted_rest import AdmittedOkxRestClient, AsyncAdmittedOkxStatisticsClient
from qdl.adapters.okx.client import OKX_CONTRACT_STATISTICS_LIMITS, OkxRestClient, statistics_retry_after_ms
from qdl.adapters.okx.reference import OkxSwapReferenceAdapter
from qdl.domain.capabilities import OKX_REFERENCE_INTERVALS, okx_global_capabilities
from qdl.domain.instrument import ProductType
from qdl.reference.batch import ReferenceBatch
from qdl.reference.runtime import build_default_reference_runtime
from qdl.reference.contracts import LongShortKind, ReferenceProduct, ReferenceRequest, ReferenceStatus
from tests.test_phase104_reference_batch import instrument
from tests.test_kn_provider_admission_edge import FakeResponse, FakeRuntime, FakeSession


STEP = 300_000
OI = "/api/v5/rubik/stat/contracts/open-interest-history"
TAKER = "/api/v5/rubik/stat/taker-volume-contract"
RATIOS = {
    LongShortKind.GLOBAL_ACCOUNT: "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract",
    LongShortKind.TOP_ACCOUNT: "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract-top-trader",
    LongShortKind.TOP_POSITION: "/api/v5/rubik/stat/contracts/long-short-position-ratio-contract-top-trader",
}


def record(market="SWAP", quote="USDT"):
    return instrument(
        venue="OKX", market=market,
        product_type=ProductType.PERPETUAL if market == "SWAP" else ProductType.FUTURE,
        native_symbol=f"BTC-{quote}-" + ("SWAP" if market == "SWAP" else "261225"),
        base="BTC", quote=quote,
    )


def request(product=ReferenceProduct.OPEN_INTEREST, **kwargs):
    return ReferenceRequest(**dict(
        instrument=record(), product=product, interval="5m",
        start_ms=STEP, end_ms=4 * STEP, limit=10, page_size=3, **kwargs,
    ))


def oi(ts, contracts="100.50000000", base="1.005", usd="60000.00"):
    return [str(ts), contracts, base, usd]


class FixtureRest:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    async def get(self, path, *, params, bucket, attempts=3):
        self.calls.append((path, dict(params), bucket))
        return self.pages.pop(0) if self.pages else []


class OkxReferenceCompletionTests(unittest.IsolatedAsyncioTestCase):
    async def fetch(self, req, pages):
        self.rest = FixtureRest(pages)
        adapter = OkxSwapReferenceAdapter(self.rest)
        return await ReferenceBatch({req.provider_key: adapter}).fetch_one(req)

    async def test_oi_backward_pagination_overlap_inclusive_edges_exact_units(self):
        result = await self.fetch(request(), [
            [oi(4 * STEP), oi(3 * STEP), oi(2 * STEP)],
            [oi(2 * STEP), oi(STEP), oi(STEP - 1)],
        ])
        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertEqual([r.observed_at_ns // 1_000_000 for r in result.observations],
                         [STEP, 2 * STEP, 3 * STEP, 4 * STEP])
        self.assertEqual([c[1]["end"] for c in self.rest.calls], [str(4 * STEP + 1), str(2 * STEP)])
        self.assertTrue(result.coverage.complete_left and result.coverage.complete_right)
        self.assertEqual(result.coverage.terminal_reason, "REQUEST_WINDOW_COVERED")
        fields = {f.name: (f.value.source_text, f.unit) for f in result.observations[0].fields}
        self.assertEqual(fields, {
            "open_interest_contracts": ("100.50000000", "CONTRACTS"),
            "open_interest_ccy": ("1.005", "BASE_ASSET_QUANTITY"),
            "open_interest_usd": ("60000.00", "USD_NOTIONAL"),
        })
        for path, params, bucket in self.rest.calls:
            self.assertEqual((path, bucket), (OI, "public"))
            self.assertEqual(params["instId"], "BTC-USDT-SWAP")
            self.assertEqual(params["period"], "5m")
            self.assertEqual(params["limit"], "3")
            self.assertNotIn("ccy", params)
            self.assertNotIn("after", params)
        self.assertEqual(result.lineage[0].source_role, "REFERENCE")
        self.assertEqual(result.lineage[0].provider_endpoint, OI)
        self.assertEqual(result.observations[0].instrument_revision, 7)
        self.assertIn(("identity_origin", "REQUEST_INST_ID"), result.observations[0].labels)

    async def test_all_ratio_kinds_and_markets_are_distinct_exact_contracts(self):
        for market in ("SWAP", "FUTURES"):
            for kind, endpoint in RATIOS.items():
                with self.subTest(market=market, kind=kind):
                    req = replace(request(ReferenceProduct.LONG_SHORT_RATIO, long_short_kind=kind),
                                  instrument=record(market, "USD"))
                    result = await self.fetch(req, [[[str(STEP), "1.173900"]]])
                    self.assertEqual(result.status, ReferenceStatus.OK)
                    self.assertEqual(self.rest.calls[0][0], endpoint)
                    self.assertEqual(self.rest.calls[0][1]["instId"], req.instrument.native_symbol)
                    row = result.observations[0]
                    self.assertEqual([(f.name, f.unit, f.value.source_text) for f in row.fields],
                                     [("long_short_ratio", "RATIO", "1.173900")])
                    labels = dict(row.labels)
                    self.assertEqual(labels["ratio_kind"], kind.value)
                    self.assertEqual(labels["scope"], "EXACT_CONTRACT")
                    self.assertEqual(labels["ratio_measure"], "POSITION" if kind is LongShortKind.TOP_POSITION else "ACCOUNT")
                    self.assertEqual(labels["ratio_population"], "ALL_TRADERS" if kind is LongShortKind.GLOBAL_ACCOUNT
                                     else "TOP_5_PERCENT_BY_OPEN_POSITION_VALUE")

    async def test_taker_sell_before_buy_explicit_contract_unit_and_no_invented_ratio(self):
        for quote in ("USDT", "USD"):
            with self.subTest(quote=quote):
                req = replace(request(ReferenceProduct.TAKER_FLOW), instrument=record(quote=quote))
                result = await self.fetch(req, [[[str(STEP), "0", "380.012300"]]])
                self.assertEqual(result.status, ReferenceStatus.OK)
                fields = {f.name: (f.value.source_text, f.unit) for f in result.observations[0].fields}
                self.assertEqual(fields, {"sell_volume": ("0", "CONTRACTS"), "buy_volume": ("380.012300", "CONTRACTS")})
                self.assertEqual(self.rest.calls[0][0], TAKER)
                self.assertEqual(self.rest.calls[0][1]["unit"], "1")
                self.assertEqual(result.observations[0].observed_at_ns, STEP * 1_000_000)
                self.assertIn(("finality", "PROVIDER_NOT_SUPPLIED"), result.observations[0].labels)

    async def test_missing_fields_not_zero_and_empty_history_not_full(self):
        result = await self.fetch(request(), [[oi(STEP, "0", "", None)]])
        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertEqual([f.name for f in result.observations[0].fields], ["open_interest_contracts"])
        result = await self.fetch(request(), [[]])
        self.assertEqual(result.status, ReferenceStatus.MISSING)
        self.assertFalse(result.coverage.complete_left or result.coverage.complete_right)
        self.assertEqual(result.coverage.terminal_reason, "PROVIDER_EXHAUSTED")

    async def test_bad_shapes_timestamps_numbers_and_provider_window_fail_closed(self):
        for row in ({"ts": STEP}, [str(STEP), "1"], oi(STEP, "NaN"), oi(STEP, "Infinity"),
                    oi(STEP, "-1"), oi(STEP, "bad"), oi(STEP, "", "", None),
                    oi(0), oi(-1), oi("1.5"), oi(""), oi(None), oi(True), oi(4 * STEP + 1)):
            with self.subTest(row=row):
                result = await self.fetch(request(), [[row]])
                self.assertEqual((result.status, result.error_code), (ReferenceStatus.ERROR, "PROVIDER_PROTOCOL"))
                self.assertFalse(result.observations)
        for page in ({}, "bad", [oi(STEP)] * 4):
            result = await self.fetch(request(), [page])
            self.assertEqual(result.error_code, "PROVIDER_PROTOCOL")

    async def test_stall_and_conflicting_overlap_fail_closed(self):
        for second in ([oi(2 * STEP)], [oi(2 * STEP, "999"), oi(STEP)]):
            result = await self.fetch(request(), [[oi(4 * STEP), oi(3 * STEP), oi(2 * STEP)], second])
            self.assertEqual(result.error_code, "PROVIDER_PROTOCOL")
            self.assertEqual(len(self.rest.calls), 2)

    async def test_internal_gaps_short_history_and_stale_right_are_partial(self):
        for pages, reason in (([[oi(4 * STEP), oi(STEP)]], "PROVIDER_GAP"),
                              ([[oi(4 * STEP), oi(3 * STEP)]], "PROVIDER_PARTIAL"),
                              ([[oi(2 * STEP), oi(STEP)]], "PROVIDER_PARTIAL")):
            result = await self.fetch(request(), pages)
            self.assertEqual(result.status, ReferenceStatus.OK)
            self.assertEqual(result.coverage.terminal_reason, reason)
            self.assertFalse(result.coverage.complete_left and result.coverage.complete_right)

    async def test_max_pages_and_records_return_latest_bounded_partial_rows(self):
        result = await self.fetch(replace(request(), max_pages=1), [[oi(4 * STEP), oi(3 * STEP)]])
        self.assertTrue(result.coverage.truncated)
        self.assertEqual(result.coverage.terminal_reason, "MAX_PAGES")
        result = await self.fetch(replace(request(), limit=2, page_size=2), [[oi(4 * STEP), oi(3 * STEP)]])
        self.assertEqual(result.coverage.terminal_reason, "MAX_RECORDS")
        self.assertFalse(result.coverage.complete_left)
        self.assertEqual(len(result.observations), 2)
        self.assertEqual(len(self.rest.calls), 1)

    async def test_provider_retention_cap_cannot_claim_unbounded_history(self):
        req = replace(request(), end_ms=2000 * STEP, limit=2000, page_size=100)
        pages = [[oi(ts * STEP) for ts in range(2000 - n * 100, 1900 - n * 100, -1)] for n in range(15)]
        result = await self.fetch(req, pages)
        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertEqual(len(result.observations), 1440)
        self.assertEqual(len(self.rest.calls), 15)
        self.assertTrue(result.coverage.truncated)
        self.assertEqual(result.coverage.terminal_reason, "PROVIDER_HISTORY_LIMIT")
        self.assertFalse(result.coverage.complete_left)

    async def test_interval_mapping_daily_utc_and_unsupported_fail_before_io(self):
        for interval, native in (("1h", "1H"), ("6h", "6Hutc"), ("1d", "1Dutc"), ("1w", "1Wutc")):
            result = await self.fetch(replace(request(), interval=interval), [[oi(STEP)]])
            self.assertEqual(result.status, ReferenceStatus.OK)
            self.assertEqual(self.rest.calls[0][1]["period"], native)
        for interval in ("1m", "3m", "1M", "1D", "1Dutc", "8h", "5d", "1H", "1s"):
            result = await self.fetch(replace(request(), interval=interval), [])
            self.assertEqual(result.status, ReferenceStatus.UNAVAILABLE)
            self.assertFalse(self.rest.calls)

    def test_request_contract_requires_history_and_interval(self):
        for product in (ReferenceProduct.OPEN_INTEREST, ReferenceProduct.LONG_SHORT_RATIO, ReferenceProduct.TAKER_FLOW):
            kwargs = {"long_short_kind": LongShortKind.GLOBAL_ACCOUNT} if product is ReferenceProduct.LONG_SHORT_RATIO else {}
            with self.assertRaises(ValueError):
                ReferenceRequest(record(), product, start_ms=STEP, end_ms=2 * STEP, **kwargs)
            if product is not ReferenceProduct.OPEN_INTEREST:
                with self.assertRaises(ValueError):
                    ReferenceRequest(record(), product, interval="5m", **kwargs)

    def test_capabilities_are_contract_only_and_do_not_grant_execution(self):
        for market in ("SWAP", "FUTURES", "SPOT", "OPTION", "EVENTS"):
            profile = okx_global_capabilities(market)
            contract = market in {"SWAP", "FUTURES"}
            for feed in ("open_interest", "long_short_ratio", "taker_flow"):
                cap = profile.capability(feed)
                self.assertEqual(cap.rest_history, contract)
                self.assertEqual(cap.native_intervals, OKX_REFERENCE_INTERVALS if contract else ())
                if feed != "open_interest":
                    self.assertEqual(cap.enabled, contract)
                self.assertFalse(cap.live)
            self.assertEqual(profile.source_authority, "SHADOW")
            self.assertFalse(profile.capability("l2_deep").enabled)

    async def test_reuses_shared_rest_bounded_retries_and_bucket(self):
        client = OkxRestClient()
        batch = ReferenceBatch({("OKX", "SWAP"): OkxSwapReferenceAdapter(client)})
        bucket = await client._bucket_for(OI, {"instId": "BTC-USDT-SWAP"}, "public")
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps({"code": "0", "data": [oi(STEP)]}).encode()
        with patch("qdl.adapters.okx.client.requests.get", side_effect=[requests.Timeout(), response]) as get, \
             patch("qdl.adapters.okx.client.asyncio.sleep", new_callable=AsyncMock) as sleep, \
             patch.object(bucket, "acquire", new_callable=AsyncMock) as acquire:
            result = await batch.fetch_one(request())
            self.assertEqual(result.status, ReferenceStatus.OK)
            self.assertEqual(get.call_count, 2)
            self.assertEqual(acquire.await_count, 2)
            self.assertEqual(sleep.await_count, 1)
        with patch("qdl.adapters.okx.client.requests.get", side_effect=requests.Timeout()) as get, \
             patch("qdl.adapters.okx.client.asyncio.sleep", new_callable=AsyncMock), \
             patch.object(bucket, "acquire", new_callable=AsyncMock):
            result = await ReferenceBatch({("OKX", "SWAP"): OkxSwapReferenceAdapter(client)}).fetch_one(request())
            self.assertEqual(result.status, ReferenceStatus.ERROR)
            self.assertEqual(get.call_count, 3)

    async def test_all_statistics_buckets_are_exact_scoped_and_have_no_burst(self):
        client = OkxRestClient()
        for path, limit in OKX_CONTRACT_STATISTICS_LIMITS.items():
            bucket = await client._bucket_for(path, {"instId": "BTC-USDT-SWAP"}, "public")
            self.assertIsNot(bucket, client._buckets["public"])
            self.assertEqual(bucket._capacity, 1)
            self.assertEqual(bucket._refill, limit * 0.4)
            self.assertLessEqual(bucket._capacity + 2 * bucket._refill, limit)
            self.assertIs(bucket, await client._bucket_for(path, {"instId": "btc-usdt-swap"}, "public"))
            self.assertIsNot(bucket, await client._bucket_for(path, {"instId": "ETH-USDT-SWAP"}, "public"))
        with self.assertRaises(ValueError):
            await client._bucket_for(OI, {}, "public")

    async def test_statistics_rate_limit_never_retries_and_preserves_retry_after(self):
        for status, code in ((429, "0"), (200, "50011")):
            response = requests.Response()
            response.status_code = status
            response.headers["Retry-After"] = "7"
            response._content = json.dumps({"code": code, "data": []}).encode()
            client = OkxRestClient()
            with patch("qdl.adapters.okx.client.requests.get", return_value=response) as get:
                result = await ReferenceBatch({("OKX", "SWAP"): OkxSwapReferenceAdapter(client)}).fetch_one(request())
            self.assertEqual(result.error_code, "PROVIDER_RETRY_EXHAUSTED")
            self.assertEqual(result.retry_after_ms, 7000)
            self.assertEqual(get.call_count, 1)
        for header in ("invalid", "NaN", "inf"):
            self.assertIsNone(statistics_retry_after_ms(FakeResponse(429, {}, {"Retry-After": header})))
        self.assertEqual(statistics_retry_after_ms(FakeResponse(429, {}, {"Retry-After": "0.125"})), 125)
        self.assertEqual(statistics_retry_after_ms(FakeResponse(429, {}, {"Retry-After": "Thu, 01 Jan 1970 00:00:00 GMT"})), 0)

    async def test_injected_shared_admission_grants_every_page_and_keeps_other_products_separate(self):
        for market in ("SWAP", "FUTURES"):
            runtime = FakeRuntime()
            admission = BlockingProviderAdmission(lambda: runtime)
            try:
                session = FakeSession(FakeResponse(200, {"code": "0", "data": [oi(2 * STEP)]}),
                                      FakeResponse(200, {"code": "0", "data": [oi(STEP)]}))
                admitted = AdmittedOkxRestClient(admission, priority=AdmissionPriority.BATCH, session=session)
                direct = FixtureRest([])
                adapter = OkxSwapReferenceAdapter(direct, statistics_client=admitted)
                req = replace(request(), instrument=record(market), end_ms=2 * STEP, page_size=1)
                result = await ReferenceBatch({req.provider_key: adapter}).fetch_one(req)
                self.assertEqual(result.status, ReferenceStatus.OK)
                self.assertEqual(len(result.observations), 2)
                self.assertEqual((len(runtime.admits), len(runtime.completed), len(session.calls)), (2, 2, 2))
                self.assertFalse(direct.calls)
                self.assertEqual({r.lane for r in runtime.admits},
                                 {ProviderLane("OKX", market, "REFERENCE_CONTRACT_STATISTICS")})
                self.assertEqual({r.token_cost for r in runtime.admits}, {1})
                for path in OKX_CONTRACT_STATISTICS_LIMITS:
                    self.assertEqual(admitted.lane_for(path, {"instId": req.instrument.native_symbol}), runtime.admits[0].lane)
            finally:
                admission.close()
                admission._loop.close()
        for symbol in ("BTC-USDT", "BTC-USDT-SWAP,ETH-USDT-SWAP", "BTC-USDT-bad", ""):
            with self.assertRaises(ValueError):
                AdmittedOkxRestClient.lane_for(OI, {"instId": symbol})

    async def test_shared_admission_deferral_has_zero_provider_calls(self):
        runtime = FakeRuntime(defers=10, retry_after_ms=1000)
        admission = BlockingProviderAdmission(lambda: runtime, max_wait_s=0.1)
        try:
            session = FakeSession()
            admitted = AdmittedOkxRestClient(admission, priority=AdmissionPriority.BATCH, session=session)
            adapter = OkxSwapReferenceAdapter(FixtureRest([]), statistics_client=admitted)
            result = await ReferenceBatch({("OKX", "SWAP"): adapter}).fetch_one(request())
            self.assertEqual(result.error_code, "PROVIDER_RETRY_EXHAUSTED")
            self.assertEqual(result.retry_after_ms, 1000)
            self.assertFalse(session.calls)
        finally:
            admission.close()
            admission._loop.close()

    async def test_shared_admission_receives_rate_limit_and_does_not_retry(self):
        for status, code in ((429, "0"), (200, "50011")):
            runtime = FakeRuntime()
            admission = BlockingProviderAdmission(lambda: runtime)
            try:
                session = FakeSession(FakeResponse(status, {"code": code, "data": []}, {"Retry-After": "9"}))
                admitted = AdmittedOkxRestClient(admission, priority=AdmissionPriority.BATCH, session=session)
                adapter = OkxSwapReferenceAdapter(FixtureRest([]), statistics_client=admitted)
                result = await ReferenceBatch({("OKX", "SWAP"): adapter}).fetch_one(request())
                self.assertEqual(result.error_code, "PROVIDER_RETRY_EXHAUSTED")
                self.assertEqual(result.retry_after_ms, 9000)
                self.assertEqual(len(session.calls), 1)
                self.assertEqual(len(runtime.completed), 1)
                self.assertEqual(runtime.rate_limits, [(ProviderLane("OKX", "SWAP", "REFERENCE_CONTRACT_STATISTICS"),
                                                       429 if status == 429 else None, 50011 if code == "50011" else None, 9000)])
            finally:
                admission.close()
                admission._loop.close()

    async def test_default_runtime_requires_shared_admission_for_new_statistics_only(self):
        runtime = build_default_reference_runtime()
        with patch.object(OkxRestClient, "get", new_callable=AsyncMock) as get:
            for product in (ReferenceProduct.OPEN_INTEREST, ReferenceProduct.LONG_SHORT_RATIO, ReferenceProduct.TAKER_FLOW):
                kwargs = {"long_short_kind": LongShortKind.GLOBAL_ACCOUNT} if product is ReferenceProduct.LONG_SHORT_RATIO else {}
                result = await runtime.batch.fetch_one(request(product, **kwargs))
                self.assertEqual(result.status, ReferenceStatus.UNAVAILABLE)
            get.assert_not_awaited()
            get.return_value = [{"instId": "BTC-USDT-SWAP", "instType": "SWAP", "ts": str(STEP), "oi": "1"}]
            snapshot = await runtime.batch.fetch_one(ReferenceRequest(record(), ReferenceProduct.OPEN_INTEREST))
            self.assertEqual(snapshot.status, ReferenceStatus.OK)
            self.assertEqual(get.call_args.args[0], "/api/v5/public/open-interest")

    async def test_default_builder_shares_async_admission_for_all_five_series_and_two_markets(self):
        cases = [(ReferenceProduct.OPEN_INTEREST, None), (ReferenceProduct.TAKER_FLOW, None)]
        cases.extend((ReferenceProduct.LONG_SHORT_RATIO, kind) for kind in LongShortKind)
        for market in ("SWAP", "FUTURES"):
            for product, kind in cases:
                with self.subTest(market=market, product=product, kind=kind):
                    admission = FakeRuntime()
                    runtime = build_default_reference_runtime(native_basis_admission=admission)
                    async def page(path, *, params, bucket, attempts):
                        self.assertEqual(attempts, 1)
                        ts = 2 * STEP if int(params["end"]) > 2 * STEP else STEP
                        return [oi(ts) if path == OI else [str(ts), "10", "20"] if path == TAKER else [str(ts), "1.2"]]
                    req = ReferenceRequest(record(market), product, start_ms=STEP, end_ms=2 * STEP,
                                           interval="5m", limit=2, page_size=1, long_short_kind=kind)
                    with patch.object(OkxRestClient, "get", side_effect=page) as get:
                        result = await runtime.batch.fetch_one(req)
                    self.assertEqual(result.status, ReferenceStatus.OK)
                    self.assertEqual((get.await_count, len(admission.admits), len(admission.completed)), (2, 2, 2))
                    self.assertEqual({r.lane for r in admission.admits},
                                     {ProviderLane("OKX", market, "REFERENCE_CONTRACT_STATISTICS")})
                    self.assertEqual({r.priority for r in admission.admits}, {AdmissionPriority.BATCH})

    async def test_default_builder_deferred_and_unconfigured_rust_lane_never_call_okx(self):
        admission = FakeRuntime(defers=1, retry_after_ms=2500)
        with patch.object(OkxRestClient, "get", new_callable=AsyncMock) as get:
            result = await build_default_reference_runtime(native_basis_admission=admission).batch.fetch_one(request())
            self.assertEqual(result.error_code, "PROVIDER_RETRY_EXHAUSTED")
            self.assertEqual(result.retry_after_ms, 2500)
            get.assert_not_awaited()
        calls = []
        def missing_lane(req):
            calls.append(json.loads(req.content))
            return httpx.Response(400, json={"error": "lane not configured"})
        http = httpx.AsyncClient(base_url="http://rust_core:8300", transport=httpx.MockTransport(missing_lane))
        try:
            rust = RustHttpProviderAdmission(base_url="http://rust_core:8300", secret=b"unit-test-only-secret-32-characters", client=http)
            with patch.object(OkxRestClient, "get", new_callable=AsyncMock) as get:
                result = await build_default_reference_runtime(native_basis_admission=rust).batch.fetch_one(request())
            self.assertEqual(result.error_code, "PROVIDER_RETRY_EXHAUSTED")
            get.assert_not_awaited()
            self.assertEqual(len(calls), 1)
        finally:
            await http.aclose()

    async def test_async_admission_rate_limit_relay_and_retry_attempts_are_bounded(self):
        for status, code in ((429, "0"), (200, "50011")):
            admission = FakeRuntime()
            response = requests.Response()
            response.status_code = status
            response.headers["Retry-After"] = "6"
            response._content = json.dumps({"code": code, "data": []}).encode()
            with patch("qdl.adapters.okx.client.requests.get", return_value=response) as get:
                result = await build_default_reference_runtime(native_basis_admission=admission).batch.fetch_one(request())
            self.assertEqual((result.error_code, result.retry_after_ms), ("PROVIDER_RETRY_EXHAUSTED", 6000))
            self.assertEqual((get.call_count, len(admission.admits), len(admission.completed)), (1, 1, 1))
            self.assertEqual(admission.rate_limits[0], (ProviderLane("OKX", "SWAP", "REFERENCE_CONTRACT_STATISTICS"),
                                                       status if status == 429 else None, 50011 if code == "50011" else None, 6000))
        admission = FakeRuntime()
        response.status_code = 200
        response._content = json.dumps({"code": "0", "data": [oi(STEP)]}).encode()
        with patch("qdl.adapters.okx.client.requests.get", side_effect=[requests.Timeout(), response]) as get, \
             patch("qdl.adapters.okx.admitted_rest.asyncio.sleep", new_callable=AsyncMock), \
             patch("qdl.adapters.okx.client.AsyncTokenBucket.acquire", new_callable=AsyncMock):
            result = await build_default_reference_runtime(native_basis_admission=admission).batch.fetch_one(request())
        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertEqual((get.call_count, len(admission.admits), len(admission.completed)), (2, 2, 2))

    def test_candidate_policy_retains_stable_lane_and_bounds_only_new_statistics(self):
        root = Path(__file__).resolve().parents[1]
        old = json.loads((root / "config/v2/provider-admission-policy-v1.json").read_text())
        candidate = json.loads((root / "config/v2/provider-admission-policy-kn-reference-v1.json").read_text())
        self.assertEqual(candidate["lanes"][:len(old["lanes"])], old["lanes"])
        added = candidate["lanes"][len(old["lanes"]):]
        self.assertEqual({(r["lane"]["provider"], r["lane"]["market"], r["lane"]["endpoint_family"]) for r in added},
                         {("OKX", market, "REFERENCE_CONTRACT_STATISTICS") for market in ("SWAP", "FUTURES")})
        for item in added:
            self.assertEqual(item["policy"]["token_capacity"], 1)
            self.assertEqual(item["policy"]["refill_tokens"], 1)
            self.assertEqual(item["policy"]["refill_interval_ns"], 1_000_000_000)
            self.assertEqual(item["policy"]["max_inflight"], 1)
        self.assertNotIn("provider-admission-policy-kn-reference-v1.json", (root / "docker-compose.v2-stable.yml").read_text())

    async def test_cancel_waits_worker_and_relays_late_rate_limit_before_one_completion(self):
        for status, code in ((200, "0"), (429, "0"), (200, "50011")):
            with self.subTest(status=status, code=code):
                admission = FakeRuntime()
                client = AsyncAdmittedOkxStatisticsClient(OkxRestClient(timeout_seconds=0.25), admission)
                started = asyncio.Event()
                release = threading.Event()
                loop = asyncio.get_running_loop()
                response = requests.Response()
                response.status_code = status
                response.headers["Retry-After"] = "7"
                response._content = json.dumps({"code": code, "data": [oi(STEP)]}).encode()
                def blocked_get(*args, **kwargs):
                    self.assertEqual(kwargs["timeout"], 0.25)
                    loop.call_soon_threadsafe(started.set)
                    if not release.wait(2):
                        raise requests.Timeout("unit worker guard")
                    return response
                with patch("qdl.adapters.okx.client.requests.get", side_effect=blocked_get) as get:
                    task = asyncio.create_task(client.get(OI, params={"instId": "BTC-USDT-SWAP"}, bucket="public"))
                    try:
                        await asyncio.wait_for(started.wait(), 1)
                        task.cancel()
                        await asyncio.sleep(0.01)
                        task.cancel()
                        await asyncio.sleep(0.01)
                        self.assertFalse(task.done())
                        self.assertEqual(admission.completed, [])
                        self.assertEqual(get.call_count, 1)
                    finally:
                        release.set()
                        with self.assertRaises(asyncio.CancelledError):
                            await asyncio.wait_for(task, 1)
                    self.assertEqual((len(admission.admits), len(admission.completed), get.call_count), (1, 1, 1))
                    self.assertEqual(len(admission.rate_limits), int(status == 429 or code == "50011"))
                    if admission.rate_limits:
                        self.assertEqual(admission.rate_limits[0][-1], 7000)

    def test_candidate_compilers_add_exactly_fifteen_entitlements_in_memory(self):
        from qdl.demand import ActiveDemandSourceRegistry, admit_provider_metadata, converge_active_demand
        from qdl.demand.contracts import DemandFeed
        from qdl.demand.inventory import AdmissionBudget
        from qdl.runtime.reference_l2_materializer import build_reference_l2_materialization
        from scripts import phase24315_materialize_alpha_reference_entitlements as alpha_compiler
        from tests import test_reference_l2_materializer as fixture

        root = Path(__file__).resolve().parents[1]
        paths = [fixture.DEMAND_PATH, fixture.REGISTRY_PATH, fixture.CATALOG_PATH, fixture.ACQUISITION_PATH,
                 fixture.SCOPE_PATH, root / "consumers/stable/reference-l2-stable.yaml",
                 root / "consumers/stable/alpha-okx-paper.yaml", root / "consumers/stable/alpha-binance-paper.yaml",
                 root / "config/v2/stable-v2-release-routing.yaml", root / "config/v2/stable-primary-consumer-routing.yaml"]
        before = {path: path.read_bytes() for path in paths}
        inventory = fixture.ReferenceL2MaterializerTests._inventory()
        base = next(r for r in inventory.requirements if r.universe.venue == "OKX"
                    and r.feed is DemandFeed.OPEN_INTEREST and r.interval is None)
        additions = tuple(replace(base, feed=feed, interval="1d", max_freshness_ms=86_400_000)
                          for feed in (DemandFeed.OPEN_INTEREST, DemandFeed.LONG_SHORT_RATIO, DemandFeed.TAKER_FLOW))
        candidate = replace(inventory, requirements=inventory.requirements + additions)
        policy = ActiveDemandSourceRegistry.load(fixture.REGISTRY_PATH).admission_policy
        policy = replace(policy, budgets=policy.budgets + tuple(
            AdmissionBudget("OKX", "SWAP", feed, 128) for feed in (DemandFeed.LONG_SHORT_RATIO, DemandFeed.TAKER_FLOW)))
        admitted = admit_provider_metadata(candidate, fixture._metadata())
        convergence = converge_active_demand(candidate, admitted, policy)
        catalog, acquisition, scope = fixture._fixture_baseline()
        # Keep the production strict loaders; virtualize only their temporary
        # YAML documents so this source candidate smoke performs no writes.
        import yaml
        original_text, original_bytes = Path.read_text, Path.read_bytes
        def validate_memory(value, *, filename, loader):
            path = Path("/tmp/qdl-okx-unit-memory") / filename
            content = yaml.safe_dump(dict(value), sort_keys=False)
            def read_text(target, *args, **kwargs):
                return content if target == path else original_text(target, *args, **kwargs)
            def read_bytes(target, *args, **kwargs):
                return content.encode() if target == path else original_bytes(target, *args, **kwargs)
            with patch.object(Path, "read_text", read_text), patch.object(Path, "read_bytes", read_bytes):
                return loader(path)
        with patch("qdl.runtime.reference_l2_materializer._validate_temporary", side_effect=validate_memory), \
             patch.object(Path, "write_text", side_effect=AssertionError("candidate smoke must not write")), \
             patch.object(Path, "write_bytes", side_effect=AssertionError("candidate smoke must not write")):
            materialized = build_reference_l2_materialization(
                inventory=candidate, admission=admitted, convergence=convergence,
                current_catalog_document=catalog, current_acquisition_document=acquisition,
                current_promotion_scope_document=scope,
            )
            self.assertEqual(materialized.summary["reference_entitlement_count"], 70)
            self.assertEqual(len(materialized.consumer_manifest["spec"]["requirements"]), 94)
            self.assertEqual(materialized.summary["runtime_mutations"], 0)
            manifests, _route, _primary, summary = alpha_compiler.build_documents(
                reference_manifest=materialized.consumer_manifest,
                alpha_manifests={cid: fixture._load(root / "consumers/stable" / name) for cid, name in alpha_compiler._TARGETS.items()},
                release_route=fixture._load(root / "config/v2/stable-v2-release-routing.yaml"),
                primary_route=fixture._load(root / "config/v2/stable-primary-consumer-routing.yaml"),
            )
        self.assertEqual(summary["alpha_reference_counts"]["alpha.okx.paper.stable"], 35)
        old = fixture._load(root / "consumers/stable/alpha-okx-paper.yaml")["spec"]["requirements"]
        new = manifests["alpha.okx.paper.stable"]["spec"]["requirements"]
        key = lambda row: (row["instrument_uid"], row["feed"], row.get("interval"))
        added_keys = {key(r) for r in new} - {key(r) for r in old}
        self.assertEqual(len(added_keys), 15)
        self.assertEqual({(feed, interval) for _uid, feed, interval in added_keys},
                         {("OPEN_INTEREST", "1d"), ("LONG_SHORT_RATIO", "1d"), ("TAKER_FLOW", "1d")})
        self.assertEqual({key(r) for r in old} - {key(r) for r in new}, set())
        self.assertEqual({path: path.read_bytes() for path in paths}, before)

    async def test_cancel_propagates_without_extra_provider_calls(self):
        rest = FixtureRest([])
        rest.get = AsyncMock(side_effect=asyncio.CancelledError())
        adapter = OkxSwapReferenceAdapter(rest)
        with self.assertRaises(asyncio.CancelledError):
            await adapter.fetch(request(), capability=okx_global_capabilities("SWAP").require("open_interest"),
                                received_at_ns=time.time_ns())
        self.assertEqual(rest.get.await_count, 1)


@unittest.skipUnless(os.environ.get("QDL_OKX_PUBLIC_REFERENCE_SMOKE") == "1", "opt-in public provider GET")
class OkxPublicReferenceSmoke(unittest.IsolatedAsyncioTestCase):
    async def test_five_public_contract_series_bounded_two_pages(self):
        class PublicClient(OkxRestClient):
            def __init__(self):
                super().__init__(timeout_seconds=5)
                self.receipts = []

            async def get(self, path, *, params, bucket, attempts=3):
                assert path in {OI, TAKER, *RATIOS.values()}
                assert len(self.receipts) < 10
                assert params["limit"] == "2"
                await asyncio.sleep(0.5)
                rows = await super().get(path, params=params, bucket=bucket, attempts=1)
                self.receipts.append({
                    "endpoint": path, "params": dict(params), "rows": len(rows),
                    "decoded_data_sha256": hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest(),
                })
                return rows

        client = PublicClient()
        end = (time.time_ns() // 1_000_000 // STEP - 2) * STEP
        batch = ReferenceBatch({("OKX", "SWAP"): OkxSwapReferenceAdapter(client)})
        cases = [(ReferenceProduct.OPEN_INTEREST, None), (ReferenceProduct.TAKER_FLOW, None)]
        cases.extend((ReferenceProduct.LONG_SHORT_RATIO, kind) for kind in LongShortKind)
        metrics = []
        for product, kind in cases:
            with self.subTest(product=product, kind=kind):
                req = ReferenceRequest(record(), product, start_ms=end - 3 * STEP, end_ms=end,
                                       interval="5m", limit=4, page_size=2, max_pages=2, long_short_kind=kind)
                started = time.perf_counter_ns()
                result = await batch.fetch_one(req)
                duration_ms = (time.perf_counter_ns() - started) / 1_000_000
                provider_age_ms = ((time.time_ns() - max(row.observed_at_ns for row in result.observations)) / 1_000_000
                                   if result.observations else None)
                metrics.append({"request_ms": duration_ms, "provider_age_ms": provider_age_ms})
                print(json.dumps({"product": product.value, "kind": kind.value if kind else None,
                                  "domain": "https://www.okx.com", "request_ms": duration_ms,
                                  "provider_age_ms": provider_age_ms,
                                  "status": result.status.value, "error": result.error_code,
                                  "observations": len(result.observations), "coverage": result.coverage.terminal_reason,
                                  "receipts": client.receipts[-2:]}, sort_keys=True))
                self.assertEqual(result.status, ReferenceStatus.OK)
                self.assertEqual(len(result.observations), 4)
                self.assertTrue(result.coverage.complete_left and result.coverage.complete_right)
        self.assertEqual(len(client.receipts), 10)
        print(json.dumps({"cases": len(metrics), "provider_gets": len(client.receipts),
                          "request_ms_mean": sum(m["request_ms"] for m in metrics) / len(metrics),
                          "provider_age_ms_mean": sum(m["provider_age_ms"] for m in metrics) / len(metrics)}, sort_keys=True))
