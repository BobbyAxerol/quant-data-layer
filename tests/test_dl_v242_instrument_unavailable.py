"""DL v2.4.2: a provider-delisted instrument is one quarantined binding, not a blocked bar_edge (2026-11-19 RVN case).

Reproduces the incident: OKX answered `51001 "Instrument ID, Instrument ID code, or Spread ID doesn't exist."` for
`RVN-USDT-SWAP` on `/api/v5/market/history-candles`; before v2.4.2 the adapter retried it as a transient ValueError,
the bar edge failed the whole cycle and, with `max_bindings=1`, never reached the bindings after it. Also covers the
Binance codes (-1121, -1122, -4141), the polling path, the heartbeat counter and the failing-cycle backoff.
"""
import json
import unittest
from unittest import mock

from qdl.adapters.instrument_unavailable import (
    BINANCE_INSTRUMENT_UNAVAILABLE_CODES, OKX_INSTRUMENT_UNAVAILABLE_CODES, ProviderInstrumentUnavailable,
    find_instrument_unavailable,
)
from tests import test_kn_history_fill as history_fill

RVN = ProviderInstrumentUnavailable("OKX", "RVN-USDT-SWAP", "51001",
                                    "Instrument ID, Instrument ID code, or Spread ID doesn't exist.",
                                    "/api/v5/market/history-candles")


class _Response:
    def __init__(self, status, payload, headers=None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        return self._payload


class AdapterClassificationTests(unittest.TestCase):
    def test_okx_51001_is_typed_and_not_retried(self):
        import asyncio
        from qdl.adapters.okx import admitted_rest

        client = admitted_rest.AdmittedOkxRestClient.__new__(admitted_rest.AdmittedOkxRestClient)
        calls = []

        def leased(path, params):
            calls.append(path)
            payload = {"code": "51001", "msg": OKX_INSTRUMENT_UNAVAILABLE_CODES["51001"], "data": []}
            # Same decision as _leased_get after the HTTP 200 / non-rate-limit checks.
            code = payload["code"]
            if code in OKX_INSTRUMENT_UNAVAILABLE_CODES:
                raise ProviderInstrumentUnavailable("OKX", params["instId"], code, payload["msg"], path)
            raise ValueError("unreachable")

        client._leased_get = leased
        with self.assertRaises(ProviderInstrumentUnavailable) as caught:
            asyncio.run(client.get("/api/v5/market/history-candles", params={"instId": "RVN-USDT-SWAP"},
                                   bucket="history"))
        self.assertEqual(calls, ["/api/v5/market/history-candles"], "typed failure is never retried")
        self.assertEqual((caught.exception.provider, caught.exception.code), ("OKX", "51001"))

    def test_okx_leased_get_maps_the_venue_answer(self):
        from qdl.adapters.okx import admitted_rest

        client = admitted_rest.AdmittedOkxRestClient.__new__(admitted_rest.AdmittedOkxRestClient)
        client._base_url = "https://okx.test"
        client._timeout = 1
        client._priority = None
        client.lane_for = lambda path, params: mock.MagicMock(endpoint_family="HISTORY_CANDLES")
        lease = mock.MagicMock()
        lease.__enter__ = lambda *_: None
        lease.__exit__ = lambda *_: False
        client._admission = mock.MagicMock()
        client._admission.lease.return_value = lease
        client._session = mock.MagicMock()
        client._session.get.return_value = _Response(200, {"code": "51001", "msg": "doesn't exist", "data": []})
        with self.assertRaises(ProviderInstrumentUnavailable):
            client._leased_get("/api/v5/market/history-candles", {"instId": "RVN-USDT-SWAP"})
        client._session.get.return_value = _Response(200, {"code": "50000", "msg": "other", "data": []})
        with self.assertRaises(ValueError):
            client._leased_get("/api/v5/market/history-candles", {"instId": "BTC-USDT-SWAP"})

    def test_binance_documented_codes_are_typed(self):
        from qdl.adapters.binance.admitted_klines import AdmittedBinanceKlines

        admission = mock.MagicMock()
        lease = mock.MagicMock()
        lease.__enter__ = lambda *_: None
        lease.__exit__ = lambda *_: False
        admission.lease.return_value = lease
        for code in BINANCE_INSTRUMENT_UNAVAILABLE_CODES:
            with self.subTest(code=code):
                session = mock.MagicMock()
                session.get.return_value = _Response(400, {"code": code, "msg": "x"})
                fetch = AdmittedBinanceKlines(admission, priority=mock.MagicMock(), session=session)
                with self.assertRaises(ProviderInstrumentUnavailable) as caught:
                    fetch("RVNUSDT", interval="1d", limit=10)
                self.assertEqual(caught.exception.code, str(code))
        session = mock.MagicMock()
        session.get.return_value = _Response(400, {"code": -1100, "msg": "illegal characters"})
        from app.providers.binance.rest import BinanceProviderError
        with self.assertRaises(BinanceProviderError):
            AdmittedBinanceKlines(admission, priority=mock.MagicMock(), session=session)("BTCUSDT", interval="1d",
                                                                                        limit=10)

    def test_the_failure_is_found_through_wrapping(self):
        try:
            try:
                raise RVN
            except ProviderInstrumentUnavailable as inner:
                raise RuntimeError("OKX latest-closed BAR lookup exhausted attempts=3") from inner
        except RuntimeError as outer:
            self.assertIs(find_instrument_unavailable(outer), RVN)
        self.assertIsNone(find_instrument_unavailable(RuntimeError("other")))


class BarEdgeQuarantineTests(unittest.TestCase):
    """The RVN incident on the bar edge test double (bindings a, b, c; b is the delisted instrument)."""

    def setUp(self):
        # Reuse the history-fill double without inheriting (and re-running) its tests.
        self.helper = history_fill.EdgeFillTests("test_depth_by_demand_and_short_or_empty_history_are_reported")

    def _edge(self, **kwargs):
        edge = self.helper._edge(**kwargs)
        self.published, self.fetched = self.helper.published, self.helper.fetched
        edge._served, edge._serving_pending, edge._serve_overdue = set(), {}, set()
        return edge

    def edge_with_delisted_b(self):
        edge = self._edge(demand=None, gate=None)
        original = edge._fetch_history
        self.delisted_calls = []

        def fetch(source, acquisition, **kwargs):
            if source.binding_id == "b":
                self.delisted_calls.append(edge.clock())
                raise RVN
            return original(source, acquisition, **kwargs)

        edge._fetch_history = fetch
        return edge, original

    def test_a_delisted_instrument_does_not_block_the_bindings_after_it(self):
        edge, _ = self.edge_with_delisted_b()
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="ERROR") as logs:
            for _ in range(3):  # the production loop: one binding per turn
                edge.bootstrap_history(max_bindings=1)
        # c (an empty provider history in this double) is reached past the delisted b.
        self.assertEqual([item[0] for item in self.fetched], ["a", "c"])
        self.assertEqual([item[0] for item in self.published], ["a"])
        self.assertEqual(self.delisted_calls, [1.0], "b is not refetched every turn")
        self.assertEqual(edge._history_retry["b"], (1, 1.0 + 6 * 3600))
        self.assertEqual(edge._history_unavailable["b"]["code"], "51001")
        event = json.loads(next(line.split(":", 2)[2] for line in logs.output
                                if "qdl_bar_edge_instrument_unavailable" in line))
        self.assertEqual((event["binding"], event["symbol"], event["quarantined"]), ("b", "RVN-USDT-SWAP", 1))
        self.assertEqual(edge.serving_status()["instrument_unavailable"], 1)

    def test_the_quarantine_is_rechecked_after_its_retry_and_heals_on_relisting(self):
        edge, original = self.edge_with_delisted_b()
        edge.bootstrap_history()
        edge.clock = lambda: 1.0 + 6 * 3600 - 1
        edge.bootstrap_history()
        self.assertEqual(self.delisted_calls, [1.0])
        edge.clock = lambda: 1.0 + 6 * 3600
        edge._fetch_history = original  # relisted: the provider answers again
        edge.bootstrap_history()
        self.assertNotIn("b", edge._history_unavailable)
        self.assertNotIn("b", edge._history_retry)
        self.assertEqual(edge.serving_status()["instrument_unavailable"], 0)

    def test_other_errors_still_fail_the_cycle(self):
        edge = self._edge(demand=None, gate=None)
        edge._fetch_history = mock.Mock(side_effect=RuntimeError("OKX V5 request exhausted retries"))
        with self.assertRaises(RuntimeError):
            edge.bootstrap_history(max_bindings=1)

    def test_the_polling_path_quarantines_a_delisted_instrument(self):
        edge = self._edge(demand=None, gate=None)
        edge._retry_delay_seconds = lambda binding_id: 5.0
        wrapped = RuntimeError("OKX latest-closed BAR lookup exhausted attempts=3")
        wrapped.__cause__ = RVN
        edge._schedule_retry("b", now=100.0, error=wrapped)
        self.assertEqual(edge._next_retry_at["b"], 100.0 + 6 * 3600)
        self.assertEqual(edge._history_unavailable["b"]["attempt"], 1)
        edge._schedule_retry("a", now=100.0, error=RuntimeError("timeout"))
        self.assertEqual(edge._next_retry_at["a"], 105.0)


class FailingCycleBackoffTests(unittest.TestCase):
    def test_a_failing_cycle_waits_2_to_30_seconds_not_the_loop_delay(self):
        from qdl.runtime import stable_bar_edge as module

        class Edge(module.StableBinanceBarEdge):
            pass

        edge = Edge.__new__(Edge)
        edge.repair_only = False
        edge._history_bootstrap_active = True
        edge._heartbeat_path = None
        edge.bar_readback = None
        edge._rest_fallback_active = False
        edge.clock = lambda: 0.0
        edge._loop_sleep_seconds = lambda now: 0.1  # the history scheduler's short delay (the incident)
        waits = []

        class Stopped:
            def is_set(self):
                return len(waits) >= 7

            def wait(self, delay):
                waits.append(delay)
                return False

        edge._stopped = Stopped()
        edge.bootstrap_history = mock.Mock(side_effect=RuntimeError("provider down"))
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="ERROR"):
            edge.run_forever()
        self.assertEqual(waits, [2, 4, 8, 16, 30, 30, 30])


if __name__ == "__main__":
    unittest.main()


class VenueListingGateTests(unittest.TestCase):
    """The declaration-time check that catches the RVN delisting before any runtime role sees it."""

    def setUp(self):
        from pathlib import Path
        from scripts import catalog_venue_listing_gate as gate
        self.gate = gate
        root = Path(__file__).resolve().parent / "fixtures/v242_listings"
        self.listings = {("OKX", "SWAP"): gate.load_listing("OKX", "SWAP", root / "okx-instruments-swap.json"),
                         ("BINANCE", "USDM"): gate.load_listing("BINANCE", "USDM",
                                                                root / "binance-usdm-exchangeinfo.json")}

    @staticmethod
    def instrument(venue, market, symbol):
        return {"venue": venue, "market": market, "native_symbol": symbol,
                "instrument_id": f"{venue}.{market}.{symbol}"}

    def test_a_delisted_instrument_fails_the_gate(self):
        result = self.gate.listing_problems([
            self.instrument("OKX", "SWAP", "BTC-USDT-SWAP"), self.instrument("OKX", "SWAP", "RVN-USDT-SWAP"),
            self.instrument("BINANCE", "USDM", "RVNUSDT"), self.instrument("HOSE", "EQUITIES", "FPT")],
            self.listings)
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual([m["native_symbol"] for m in result["missing"]], ["RVN-USDT-SWAP"])
        self.assertEqual(result["checked"], 3)
        self.assertEqual(result["not_covered_by_a_listing"], {"HOSE/EQUITIES": 1})

    def test_a_listed_but_not_live_instrument_fails_the_gate(self):
        listings = {("OKX", "SWAP"): {"BTC-USDT-SWAP": "suspend"}}
        result = self.gate.listing_problems([self.instrument("OKX", "SWAP", "BTC-USDT-SWAP")], listings)
        self.assertEqual(result["not_live"][0]["state"], "suspend")

    def test_all_listed_and_live_passes(self):
        result = self.gate.listing_problems([self.instrument("OKX", "SWAP", "PEPE-USDT-SWAP"),
                                             self.instrument("BINANCE", "USDM", "BTCUSDT")], self.listings)
        self.assertEqual(result["status"], "PASS")

    def test_a_new_declaration_must_carry_venue_listings(self):
        import tempfile
        from pathlib import Path
        from scripts import dl_declaration
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "declaration.yaml"
            path.write_text(json.dumps({"schema": dl_declaration.DECLARATION_SCHEMA,
                                        "declaration_revision": dl_declaration.VENUE_LISTINGS_REQUIRED_FROM,
                                        "environment": "paper", "consumers": [{"consumer_id": "x"}]}))
            with self.assertRaisesRegex(dl_declaration.DeclarationRefused, "venue_listings"):
                dl_declaration.load_declaration(path)
