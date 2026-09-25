"""KN-4 D47-2: the BAR edge's venue calls behind the Rust provider admission.

Fault injection only - a scripted Rust runtime (the relay's async surface) and a
scripted HTTP session; no venue is called and no IP is put at risk. Proves:
request weight by page size, waiting out DEFERRED by the Rust hint, a typed
deadline without a venue call, 418/429/-1003/50011 relayed to Rust (with
Retry-After) and never retried, the IP-wide Binance weight share for BATCH
work, completion on every exit (errors, timeouts), one shared client/session
per venue and priority, and BATCH for history / REALTIME for live bars.
"""
from __future__ import annotations

import threading
import unittest

import requests

from qdl.admission.contracts import (
    AdmissionDecision,
    AdmissionDeferReason,
    AdmissionDisposition,
    AdmissionPriority,
    ProviderLane,
)
from qdl.admission.edge import AdmissionDeadlineExceeded, BlockingProviderAdmission, ProviderRateLimited
from qdl.adapters.binance.admitted_klines import AdmittedBinanceKlines, binance_klines_weight
from qdl.adapters.okx.admitted_rest import AdmittedOkxRestClient


class FakeRuntime:
    """The async surface of ``RustHttpProviderAdmission``, scripted."""

    def __init__(self, defers: int = 0, retry_after_ms: int = 250) -> None:
        self.defers = defers
        self.retry_after_ms = retry_after_ms
        self.admits: list = []
        self.completed: list = []
        self.rate_limits: list = []
        self.lock = threading.Lock()

    async def admit(self, request):
        with self.lock:
            self.admits.append(request)
            deferred = self.defers > 0
            self.defers -= 1 if deferred else 0
        return AdmissionDecision(
            lane=request.lane, request_id=request.request_id,
            disposition=AdmissionDisposition.DEFERRED if deferred else AdmissionDisposition.GRANTED,
            defer_reason=AdmissionDeferReason.TOKEN_BUDGET if deferred else None,
            retry_after_ms=self.retry_after_ms if deferred else None,
            lease_expires_at_ns=None if deferred else 1, coalesced=False)

    async def complete(self, lane, request_id):
        self.completed.append((lane, request_id))
        return True

    async def record_rate_limit(self, lane, request_id, *, http_status, provider_code, retry_after_ms):
        self.rate_limits.append((lane, http_status, provider_code, retry_after_ms))
        return AdmissionDecision(lane=lane, request_id="provider-cooldown", disposition=AdmissionDisposition.DEFERRED,
                                 defer_reason=AdmissionDeferReason.COOLDOWN, retry_after_ms=retry_after_ms or 1000,
                                 lease_expires_at_ns=None, coalesced=False)


class FakeResponse:
    def __init__(self, status: int, payload, headers=None) -> None:
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    def __init__(self, *responses) -> None:
        self.responses = list(responses)
        self.calls: list = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


ROWS = [[0, "1", "1", "1", "1", "1", 59_999, "1", 1, "1", "1", "0"]]


class AdmissionFacadeTests(unittest.TestCase):
    def facade(self, runtime, **kwargs):
        sleeps = []
        clock = [0.0]

        def sleep(seconds):
            sleeps.append(seconds)
            clock[0] += seconds

        admission = BlockingProviderAdmission(lambda: runtime, sleep=sleep, clock=lambda: clock[0], **kwargs)
        self.addCleanup(admission.close)
        return admission, sleeps

    def test_a_deferred_request_waits_for_the_rust_hint_then_completes_once(self):
        runtime = FakeRuntime(defers=2, retry_after_ms=250)
        admission, sleeps = self.facade(runtime)
        lane = ProviderLane("BINANCE", "USDM", "KLINES")
        with admission.lease(lane, "klines:BTCUSDT:1m", priority=AdmissionPriority.BATCH, token_cost=5):
            pass
        self.assertEqual(sleeps, [0.25, 0.25])
        self.assertEqual(len(runtime.admits), 3)
        self.assertEqual({item.request_id for item in runtime.admits}, {runtime.admits[0].request_id})
        self.assertEqual(len(runtime.completed), 1)

    def test_the_deadline_is_typed_and_no_provider_call_happens(self):
        runtime = FakeRuntime(defers=10_000, retry_after_ms=1_000)
        admission, _sleeps = self.facade(runtime, max_wait_s=2.5)
        session = FakeSession()
        fetch = AdmittedBinanceKlines(admission, priority=AdmissionPriority.BATCH, session=session)
        with self.assertRaises(AdmissionDeadlineExceeded):
            fetch("BTCUSDT", interval="1m", limit=1000, end_time=1, market="usdm")
        self.assertEqual(session.calls, [])
        self.assertEqual(runtime.completed, [])

    def test_a_failure_inside_the_lease_still_completes_it(self):
        runtime = FakeRuntime()
        admission, _ = self.facade(runtime)
        with self.assertRaises(RuntimeError):
            with admission.lease(ProviderLane("OKX", "SWAP", "HISTORY_CANDLES"), "x",
                                 priority=AdmissionPriority.BATCH, token_cost=1):
                raise RuntimeError("cancelled")
        self.assertEqual(len(runtime.completed), 1)

    def test_concurrent_threads_share_one_facade(self):
        runtime = FakeRuntime()
        admission, _ = self.facade(runtime)
        lane = ProviderLane("BINANCE", "USDM", "KLINES")

        def work():
            with admission.lease(lane, "k", priority=AdmissionPriority.REALTIME, token_cost=1):
                pass

        threads = [threading.Thread(target=work) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        self.assertEqual((len(runtime.admits), len(runtime.completed)), (8, 8))


class BinanceAdmittedKlinesTests(unittest.TestCase):
    def setUp(self):
        self.runtime = FakeRuntime()
        self.sleeps = []
        self.admission = BlockingProviderAdmission(lambda: self.runtime, sleep=self.sleeps.append)
        self.addCleanup(self.admission.close)

    def test_weight_follows_the_documented_page_size_brackets(self):
        self.assertEqual([binance_klines_weight("usdm", n) for n in (1, 99, 100, 499, 500, 1000, 1500)],
                         [1, 1, 2, 2, 5, 5, 10])
        self.assertEqual(binance_klines_weight("spot", 1000), 2)
        session = FakeSession(FakeResponse(200, ROWS, {"X-MBX-USED-WEIGHT-1M": "10"}))
        fetch = AdmittedBinanceKlines(self.admission, priority=AdmissionPriority.BATCH, session=session)
        result = fetch("btcusdt", interval="1m", limit=1000, end_time=5, market="futures")
        self.assertEqual((result["data"], result["used_weight_1m"]), (ROWS, 10))
        self.assertEqual(self.runtime.admits[0].token_cost, 5)
        self.assertEqual(self.runtime.admits[0].lane, ProviderLane("BINANCE", "USDM", "KLINES"))
        self.assertEqual(session.calls[0][1]["endTime"], 5)
        self.assertEqual(len(self.runtime.completed), 1)

    def test_429_418_and_minus_1003_are_relayed_and_not_retried(self):
        from qdl.adapters.binance.bar_edge import BinanceBarRawBinding, fetch_closed_bar_history_raw_envelopes

        binding = BinanceBarRawBinding(market="USDM", product_type="PERPETUAL", native_symbol="BTCUSDT",
                                       interval="1m", subscription_id="s", source_session_id="x",
                                       connection_generation=1, lease_epoch=1, authority_revision=1,
                                       partition_plan_epoch=1, adapter_version="t", config_revision=1,
                                       instrument_catalog_revision=1)
        cases = [
            (FakeResponse(429, {"code": -1003, "msg": "too many"}, {"Retry-After": "7"}), 429, -1003, 7000, False),
            (FakeResponse(418, {"code": -1003}, {"Retry-After": "120"}), 418, -1003, 120_000, True),
            (FakeResponse(400, {"code": -1003}), None, -1003, None, False),
        ]
        for response, status, code, retry_ms, banned in cases:
            with self.subTest(status=response.status_code):
                session = FakeSession(response)
                fetch = AdmittedBinanceKlines(self.admission, priority=AdmissionPriority.BATCH, session=session)
                with self.assertRaises(ProviderRateLimited) as caught:
                    fetch_closed_bar_history_raw_envelopes(binding, limit=10, now_ms=3_600_000, attempts=4,
                                                           fetcher=fetch, sleep=lambda _s: None)
                self.assertEqual(len(session.calls), 1, "a rate limit is never retried")
                self.assertEqual(caught.exception.banned, banned)
                self.assertEqual(self.runtime.rate_limits[-1][1:], (status, code, retry_ms))

    def test_a_transient_error_is_retried_under_a_fresh_grant_each_time(self):
        from qdl.adapters.binance.bar_edge import _fetch_rows, BinanceBarRawBinding

        binding = BinanceBarRawBinding(market="USDM", product_type="PERPETUAL", native_symbol="BTCUSDT",
                                       interval="1m", subscription_id="s", source_session_id="x",
                                       connection_generation=1, lease_epoch=1, authority_revision=1,
                                       partition_plan_epoch=1, adapter_version="t", config_revision=1,
                                       instrument_catalog_revision=1)
        session = FakeSession(requests.ConnectTimeout("timeout"), FakeResponse(503, {}),
                              FakeResponse(200, ROWS, {"X-MBX-USED-WEIGHT-1M": "3"}))
        fetch = AdmittedBinanceKlines(self.admission, priority=AdmissionPriority.BATCH, session=session)
        rows = _fetch_rows(binding, end_time_ms=1, limit=1, attempts=3, fetcher=fetch, sleep=lambda _s: None)
        self.assertEqual(rows, ROWS)
        self.assertEqual((len(self.runtime.admits), len(self.runtime.completed)), (3, 3))
        self.assertEqual(self.runtime.rate_limits, [])

    def test_history_yields_to_the_ip_share_and_realtime_does_not(self):
        heavy = {"X-MBX-USED-WEIGHT-1M": "1300"}  # past 50% of 2400
        clock = lambda: 125.0  # noqa: E731 - 5 s into a minute
        batch = AdmittedBinanceKlines(self.admission, priority=AdmissionPriority.BATCH,
                                      session=FakeSession(FakeResponse(200, ROWS, heavy)), clock=clock,
                                      sleep=self.sleeps.append)
        batch("BTCUSDT", interval="1m", limit=1, market="usdm")
        self.assertEqual(self.sleeps, [55.5])
        realtime = AdmittedBinanceKlines(self.admission, priority=AdmissionPriority.REALTIME,
                                         session=FakeSession(FakeResponse(200, ROWS, heavy)), clock=clock,
                                         sleep=self.sleeps.append)
        realtime("BTCUSDT", interval="1m", limit=1, market="usdm")
        self.assertEqual(self.sleeps, [55.5])
        with self.assertRaises(ValueError):
            batch("BTCUSDT", interval="1m", limit=1, market="auto")  # no silent market fallback


class OkxAdmittedRestTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = FakeRuntime()
        self.admission = BlockingProviderAdmission(lambda: self.runtime)
        self.addCleanup(self.admission.close)

    async def test_50011_and_429_are_relayed_and_not_retried(self):
        for response, status, code in ((FakeResponse(200, {"code": "50011", "msg": "Too Many Requests"}), None, 50011),
                                       (FakeResponse(429, {"code": "50011"}), 429, 50011)):
            with self.subTest(status=response.status_code):
                session = FakeSession(response)
                client = AdmittedOkxRestClient(self.admission, priority=AdmissionPriority.BATCH, session=session)
                with self.assertRaises(ProviderRateLimited):
                    await client.get("/api/v5/market/history-candles",
                                     params={"instId": "BTC-USDT-SWAP", "bar": "1m"}, bucket="market")
                self.assertEqual(len(session.calls), 1)
                self.assertEqual(self.runtime.rate_limits[-1][:3],
                                 (ProviderLane("OKX", "SWAP", "HISTORY_CANDLES"), status, code))

    async def test_transient_errors_retry_with_a_grant_per_attempt_and_one_session(self):
        session = FakeSession(FakeResponse(500, {}), FakeResponse(200, {"code": "1", "msg": "busy"}),
                              FakeResponse(200, {"code": "0", "data": [["1"]]}))
        client = AdmittedOkxRestClient(self.admission, priority=AdmissionPriority.REALTIME, session=session)
        data = await client.get("/api/v5/market/candles", params={"instId": "BTC-USDT", "bar": "1m"},
                                bucket="market", attempts=3)
        self.assertEqual(data, [["1"]])
        self.assertEqual({item.lane for item in self.runtime.admits}, {ProviderLane("OKX", "SPOT", "CANDLES")})
        self.assertEqual((len(self.runtime.admits), len(self.runtime.completed), len(session.calls)), (3, 3, 3))
        with self.assertRaises(ValueError):
            AdmittedOkxRestClient.lane_for("/api/v5/public/mark-price", {"instId": "BTC-USDT-SWAP"})


class BarEdgeLanePolicyTests(unittest.TestCase):
    """The KN bar-edge lanes (``config/v2/provider-admission-policy-kn-bar-edge-v1.json``)
    stay inside a quarter of each venue's documented per-IP limit and keep a
    realtime reserve - the rest of the IP budget is left to production."""

    def test_every_lane_is_a_bounded_share_with_a_realtime_reserve(self):
        import json
        from pathlib import Path

        root = Path(__file__).resolve().parents[1]
        policy = json.loads((root / "config/v2/provider-admission-policy-kn-bar-edge-v1.json").read_text())
        self.assertEqual(policy["schema"], "qdl.provider_admission.policy.v1")
        per_minute_limit = {("BINANCE", "USDM"): 2400, ("BINANCE", "SPOT"): 6000}
        lanes = {(item["lane"]["provider"], item["lane"]["market"], item["lane"]["endpoint_family"]): item["policy"]
                 for item in policy["lanes"]}
        self.assertIn(("BINANCE", "USDM", "KLINES"), lanes)
        self.assertIn(("OKX", "SWAP", "HISTORY_CANDLES"), lanes)
        for (provider, market, family), item in lanes.items():
            with self.subTest(lane=(provider, market, family)):
                rate_per_s = item["refill_tokens"] / (item["refill_interval_ns"] / 1e9)
                if provider == "BINANCE":
                    self.assertLessEqual(rate_per_s * 60 + item["token_capacity"],
                                         0.25 * per_minute_limit[(provider, market)] + item["token_capacity"])
                    self.assertLessEqual(rate_per_s * 60, 0.25 * per_minute_limit[(provider, market)])
                else:
                    self.assertLessEqual(rate_per_s * 2, 0.25 * 20)
                self.assertGreaterEqual(item["reserved_realtime_inflight"], 1)
                self.assertLess(item["reserved_realtime_inflight"], item["max_inflight"])
                self.assertGreaterEqual(item["token_capacity"], 10 if provider == "BINANCE" else 1)


class EdgeWiringTests(unittest.TestCase):
    def test_history_is_batch_live_bars_are_realtime_and_clients_are_shared(self):
        from qdl.runtime.stable_bar_edge import StableBinanceBarEdge

        edge = StableBinanceBarEdge.__new__(StableBinanceBarEdge)  # wiring only
        runtime = FakeRuntime()
        edge.provider_admission = BlockingProviderAdmission(lambda: runtime)
        self.addCleanup(edge.provider_admission.close)
        edge._admitted_clients = {}
        batch = edge._binance_kwargs("BATCH")["fetcher"]
        self.assertIs(edge._binance_kwargs("BATCH")["fetcher"], batch)
        self.assertIs(batch._priority, AdmissionPriority.BATCH)
        self.assertIs(edge._binance_kwargs("REALTIME")["fetcher"]._priority, AdmissionPriority.REALTIME)
        okx = edge._okx_kwargs("BATCH")["history_client"]
        self.assertIs(okx._client, edge._okx_kwargs("BATCH")["history_client"]._client)
        edge.provider_admission = None
        self.assertEqual((edge._binance_kwargs("BATCH"), edge._okx_kwargs("BATCH")), ({}, {}))


if __name__ == "__main__":
    unittest.main()
