"""DL v2.4.0 A-FUND: FUNDING_RATE reference reads for the TS Paper writer, rendered with scripts/dl_declaration.py.

Base = the deployed stage C r8 repo form (declaration-stage-c-r8/repo, TS manifest r14: 102 perpetuals with
MARK_INDEX_PRICE). Checks: one FUNDING_RATE row per perpetual MARK uid (OKX SWAP 97, Binance USDM 5), ALPHA grade
(only MARK_INDEX_PRICE is execution-grade reference data), per-venue freshness, purely additive (r15, advisory
token floor), routing products added, the quota model counts the reads without streams, and the gate passes.
"""
from __future__ import annotations

from collections import Counter
from pathlib import Path
import tempfile
import unittest

import yaml

from qdl.consumer import ConsumerManifestLoader
from qdl.consumer.manifest_compat import additive_problems
from qdl.consumer.quota_model import STREAM_FEEDS, is_execution_manifest, model_manifest
from qdl.query import ConsumerGrade
from qdl.query.reference import ReferenceDataRequirement
from qdl.reference.contracts import ReferenceProduct
from qdl.runtime.stable_catalog import StableSourceCatalog
from scripts import dl_declaration as tool

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "upgrade/evidence/releases/v2.3.1/declaration-stage-c-r8/repo"
MANIFEST = "consumers/stable/trading-system-paper.yaml"


def _declaration(directory: Path, **consumer) -> Path:
    document = {
        "schema": "qdl.v2.declaration.v1", "declaration_revision": 9, "environment": "paper", "accept_set": True,
        # Same envelope as the r8 declaration; this revision adds no binding (added vCPU 0).
        "capacity": {"baseline_dl_vcpu": 6.21, "ceiling_dl_vcpu": 6.5, "venue_priority": ["OKX", "BINANCE"]},
        "base": {"repo": str(BASE)},
        "consumers": [{"consumer_id": "trading-system.paper.stable", "class": "EXECUTION", "manifest": MANIFEST,
                       "revision": 15, "reference": ["FUNDING_RATE", "MARK_INDEX_PRICE@1m"], **consumer}],
    }
    path = directory / "declaration.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return path


class TsFundingEntitlementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.out = Path(cls.tmp.name) / "out"
        cls.report = tool.render(_declaration(Path(cls.tmp.name)), cls.out)
        cls.catalog = StableSourceCatalog.load(BASE / "config/v2/stable-source-bindings.yaml")
        cls.base_doc = yaml.safe_load((BASE / MANIFEST).read_text())
        cls.doc = yaml.safe_load((cls.out / "repo" / MANIFEST).read_text())

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def funding(self):
        return [r for r in self.doc["spec"]["requirements"] if r["feed"] == "FUNDING_RATE"]

    def marks_1m(self):
        return [r for r in self.doc["spec"]["requirements"] if r["feed"] == "MARK_INDEX_PRICE" and r.get("interval")]

    def test_historical_mark_rows_for_the_97_okx_swap_perpetuals_and_binance_is_typed_blocked(self):
        # Recount from the catalog: the TS r14 MARK uids whose identity is OKX SWAP PERPETUAL.
        base_marks = [r["instrument_uid"] for r in self.base_doc["spec"]["requirements"]
                      if r["feed"] == "MARK_INDEX_PRICE"]
        okx = {u for u in base_marks if (lambda i: (i.venue, i.market, i.product_type.value))(
            self.catalog.instrument_for(u).identity) == ("OKX", "SWAP", "PERPETUAL")}
        self.assertEqual(len(okx), 97)
        rows = self.marks_1m()
        self.assertEqual({r["instrument_uid"] for r in rows}, okx)
        self.assertEqual(len(rows), 97)
        for row in rows:
            self.assertEqual((row["consumer_grade"], row["interval"], row["source_policy_id"], row["recovery"],
                              row["require_full_coverage"]), ("ALPHA", "1m", "crypto_liquid_v2", "FRESH_SNAPSHOT", True))
        blocked = self.report["blocked"]
        self.assertEqual(len(blocked), 5)
        self.assertEqual({(b["reason"], b["venue"], b["feed"]) for b in blocked},
                         {("REFERENCE_VENUE_NOT_DECLARABLE", "BINANCE", "MARK_INDEX_PRICE")})
        # The execution MARK rows are byte-unchanged.
        execution = lambda doc: [r for r in doc["spec"]["requirements"]  # noqa: E731
                                 if r["feed"] == "MARK_INDEX_PRICE" and not r.get("interval")]
        self.assertEqual(execution(self.doc), execution(self.base_doc))

    def test_one_alpha_grade_funding_row_per_perpetual_mark_uid(self):
        marks = {r["instrument_uid"] for r in self.base_doc["spec"]["requirements"] if r["feed"] == "MARK_INDEX_PRICE"
                 and not r.get("interval")}
        rows = self.funding()
        self.assertEqual({r["instrument_uid"] for r in rows}, marks)
        self.assertEqual(len(rows), len(marks))
        venues = Counter((self.catalog.instrument_for(r["instrument_uid"]).identity.venue,
                          self.catalog.instrument_for(r["instrument_uid"]).identity.market) for r in rows)
        self.assertEqual(venues, Counter({("OKX", "SWAP"): 97, ("BINANCE", "USDM"): 5}))
        for row in rows:
            identity = self.catalog.instrument_for(row["instrument_uid"]).identity
            self.assertEqual(identity.product_type.value, "PERPETUAL")
            self.assertEqual((row["consumer_grade"], row["source_policy_id"], row["interval"], row["warmup_limit"],
                              row["recovery"], row["bar_revision_policy"], row["require_final_bars"],
                              row["require_full_coverage"], row["stale_policy"], row["gap_policy"]),
                             ("ALPHA", "crypto_liquid_v2", None, 0, "FRESH_SNAPSHOT", "LATEST", False, True,
                              "BLOCK", "BLOCK"))
            self.assertEqual(row["max_freshness_ms"], 28_800_000 if identity.venue == "OKX" else 32_400_000)

    def test_purely_additive_revision_15_with_the_advisory_floor(self):
        self.assertEqual((self.doc["metadata"]["revision"], self.doc["metadata"]["min_token_revision"]), (15, 1))
        old = ConsumerManifestLoader.from_mapping(self.base_doc)
        new = ConsumerManifestLoader.from_mapping(self.doc)
        self.assertEqual(additive_problems(old, new), [])
        self.assertEqual(new.allowed_permissions, old.allowed_permissions)  # history:read already granted
        self.assertTrue(new.accepts_token_revision(14))

    def test_the_reference_read_is_entitled_at_alpha_grade_only(self):
        manifest = ConsumerManifestLoader.from_mapping(self.doc)
        uid = self.funding()[0]["instrument_uid"]
        alpha = ReferenceDataRequirement(instrument_uid=uid, product=ReferenceProduct.FUNDING_RATE,
                                         consumer_grade=ConsumerGrade.ALPHA, source_policy_id="crypto_liquid_v2",
                                         start_time_ns=1_799_740_800_000_000_000, end_time_ns=1_800_000_000_000_000_000,
                                         limit=400, max_freshness_ms=self.funding()[0]["max_freshness_ms"])
        self.assertTrue(manifest.requirement_allowed(alpha.data_requirement))
        with self.assertRaisesRegex(ValueError, "only MARK_INDEX_PRICE"):
            ReferenceDataRequirement(instrument_uid=uid, product=ReferenceProduct.FUNDING_RATE,
                                     consumer_grade=ConsumerGrade.EXECUTION, source_policy_id="crypto_liquid_v2",
                                     start_time_ns=1_799_740_800_000_000_000,
                                     end_time_ns=1_800_000_000_000_000_000, limit=400)

    def test_routing_and_quota_model_take_the_reads(self):
        routing = yaml.safe_load((self.out / "repo/config/v2/stable-v2-release-routing.yaml").read_text())
        ts = next(c for c in routing["consumers"] if c["consumer_id"] == "trading-system.paper.stable")
        self.assertEqual(ts["manifest_revision"], 15)
        funding = [p for p in ts["products"] if ":FUNDING_RATE:" in p["requirement_key"]]
        self.assertEqual(len(funding), 102)
        self.assertEqual({(p["route"], p["fallback"]) for p in funding}, {("V2_PRIMARY", "BLOCKED")})
        history = [p for p in ts["products"] if ":MARK_INDEX_PRICE:1m:" in p["requirement_key"]]
        self.assertEqual(len(history), 97)
        self.assertEqual({(p["route"], p["fallback"]) for p in history}, {("V2_PRIMARY", "BLOCKED")})
        self.assertTrue(is_execution_manifest(self.doc))
        self.assertNotIn("FUNDING_RATE", STREAM_FEEDS)
        model = model_manifest(self.doc)
        self.assertEqual(model.products_by_feed["FUNDING_RATE"], 102)
        self.assertEqual(model.products_by_feed["MARK_INDEX_HISTORY"], 97)
        self.assertEqual(model.products_by_feed["MARK_INDEX_PRICE"], 102)
        self.assertEqual(model.streams, model_manifest(self.base_doc).streams)

    def test_gate_passes(self):
        failing = [c for c in tool.gate(self.out)["checks"] if c["status"] != "PASS"]
        self.assertEqual(failing, [])

    def test_an_undeclarable_reference_feed_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(tool.DeclarationRefused, "not declarable"):
                tool.render(_declaration(Path(tmp), reference=["OPEN_INTEREST"]), None, dry_run=True)


if __name__ == "__main__":
    unittest.main()


class AFundReadTests(unittest.IsolatedAsyncioTestCase):
    """A-FUND's read as the TS writer sends it: Grade.ALPHA FUNDING_RATE reference_batch, bounded window, limit 400,
    require_all, purpose INTERNAL_EXECUTION, through the REST router with the production reference entitlements."""

    def setUp(self):
        from qdl.query import InstrumentQuery, MemoryMarketDataBackend, V2QueryService
        from qdl.domain.instrument import InstrumentRegistry
        from qdl.query.entitlement import EntitlementPolicy
        from qdl.reference.runtime import build_default_reference_runtime
        from tests.test_phase113_reference_v2 import NOW_NS, fixture_batch, record

        self.now = NOW_NS
        self.okx = record(venue="OKX", market="SWAP", symbol="ETH-USDT-SWAP", base="ETH")
        self.binance = record(venue="BINANCE", market="USDM", symbol="BTCUSDT", base="BTC")
        registry = InstrumentRegistry()
        registry.register(self.okx, [])
        registry.register(self.binance, [])
        runtime = build_default_reference_runtime()
        self.service = V2QueryService(
            instruments=InstrumentQuery(registry), backend=MemoryMarketDataBackend(),
            entitlements=EntitlementPolicy(runtime.entitlement_grants()),
            reference_batch=fixture_batch(), reference_source_id=runtime.source_id_for, clock_ns=lambda: NOW_NS,
        )

    def _client(self, purposes=("INTERNAL_EXECUTION",)):
        from fastapi.testclient import TestClient

        from qdl.api_v2 import create_v2_app
        from tests.phase7_support import make_identity

        rows = []
        for item in (self.okx, self.binance):
            rows.append({"instrument_uid": item.instrument_uid, "feed": "FUNDING_RATE", "consumer_grade": "ALPHA",
                         "source_policy_id": "crypto_liquid_v2", "interval": None, "warmup_limit": 0,
                         "max_freshness_ms": 28_800_000 if item is self.okx else 32_400_000,
                         "require_full_coverage": True, "require_final_bars": False, "stale_policy": "BLOCK",
                         "gap_policy": "BLOCK", "recovery": "FRESH_SNAPSHOT", "bar_revision_policy": "LATEST"})
        payload = {"apiVersion": "qdl/v2", "kind": "DataRequirement",
                   "metadata": {"id": "trading-system.paper.stable", "owner": "trading-platform",
                                "subject": "spiffe://qdl/paper/trading-system-stable", "environment": "paper",
                                "revision": 15, "min_token_revision": 1},
                   "spec": {"sdk_major": 2, "rollback_contract": "V1", "execution_dependency": "PAPER_ONLY",
                            "permissions": ["instruments:read", "snapshot:read", "history:read", "status:read",
                                            "quality:read", "stream:read"],
                            "purposes": list(purposes),
                            "quotas": {"requests_per_minute": 31500, "max_batch_items": 50, "max_warmup_rows": 2000,
                                       "max_streams": 370, "max_buffer_events": 2000},
                            "requirements": rows}}
        identity = make_identity(ConsumerManifestLoader.from_mapping(payload))
        return TestClient(create_v2_app(self.service, identity_service=identity))

    def _read(self, client, *, grade="ALPHA", revision=14):
        from tests.phase7_support import make_token

        return client.post("/v2/market-data/reference:batch", headers={
            "Authorization": f"Bearer {make_token('spiffe://qdl/paper/trading-system-stable', manifest_revision=revision)}",
            "X-QDL-Consumer-ID": "trading-system.paper.stable", "X-QDL-Purpose": "INTERNAL_EXECUTION",
        }, json={"consumer_id": "trading-system.paper.stable", "require_all": True, "requirements": [
            {"instrument_uid": item.instrument_uid, "product": "FUNDING_RATE", "consumer_grade": grade,
             "source_policy_id": "crypto_liquid_v2", "start_time_ns": self.now - 3 * 86_400_000_000_000,
             "end_time_ns": self.now, "limit": 400,
             "max_freshness_ms": 28_800_000 if item is self.okx else 32_400_000}
            for item in (self.okx, self.binance)]})

    def test_the_alpha_grade_funding_read_is_served_with_the_execution_purpose_alone(self):
        response = self._read(self._client(), revision=14)  # a token minted for r14 is accepted by r15 (S2)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual([item["data"]["status"] for item in response.json()["results"]], ["OK", "OK"])

    def test_execution_grade_is_refused_and_an_unloaded_revision_is_unauthenticated(self):
        client = self._client()
        refused = self._read(client, grade="EXECUTION")
        self.assertEqual((refused.status_code, refused.json()["code"]), (400, "INVALID_ARGUMENT"))
        self.assertEqual(self._read(client, revision=16).status_code, 401)


class MarkHistoryReadTests(unittest.IsolatedAsyncioTestCase):
    """A-FUND's MARK read: given T, the 1m OKX mark candle covering T comes back (bounded, cached, singleflight)."""

    T_MS = 1_800_000_037_123  # an arbitrary past time inside a minute
    OPEN_MS = T_MS - T_MS % 60_000

    def setUp(self):
        from qdl.adapters.okx.history import HistoryCoverage, OkxCandle, OkxCandleHistory
        from qdl.adapters.okx.reference import OkxSwapReferenceAdapter
        from qdl.domain.instrument import InstrumentRegistry
        from qdl.query import InstrumentQuery, MemoryMarketDataBackend, V2QueryService
        from qdl.query.entitlement import EntitlementPolicy
        from qdl.reference.batch import ReferenceBatch
        from qdl.reference.runtime import build_default_reference_runtime
        from tests.test_phase113_reference_v2 import record

        test = self

        class History:
            calls = 0

            async def candles(self, *, inst_id, bar, start_ms, end_ms, price_type, max_records, max_pages):
                History.calls += 1
                test.assertEqual((bar, price_type), ("1m", "MARK"))
                rows = tuple(OkxCandle(inst_id=inst_id, bar=bar, price_type=price_type, open_ts_ms=open_ms,
                                       open="2500.1", high="2501.5", low="2499.9", close="2500.7",
                                       volume_raw=None, volume_ccy_raw=None, volume_quote_raw=None, confirmed=True)
                             for open_ms in range(start_ms, end_ms, 60_000))[:max_records]
                coverage = HistoryCoverage(start_ms, end_ms, rows[0].open_ts_ms if rows else None,
                                           rows[-1].open_ts_ms if rows else None, True, True, False, "COMPLETE",
                                           "/api/v5/market/history-mark-price-candles")
                return OkxCandleHistory(rows, coverage)

        self.history = History
        self.okx = record(venue="OKX", market="SWAP", symbol="ETH-USDT-SWAP", base="ETH")
        self.binance = record(venue="BINANCE", market="USDM", symbol="BTCUSDT", base="BTC")
        registry = InstrumentRegistry()
        registry.register(self.okx, [])
        registry.register(self.binance, [])
        runtime = build_default_reference_runtime()
        from qdl.adapters.binance.reference import BinanceUsdmReferenceAdapter  # noqa: F401 - import check only
        adapter = OkxSwapReferenceAdapter(client=object(), history=History())
        self.batch = ReferenceBatch({("OKX", "SWAP"): adapter, ("BINANCE", "USDM"): adapter},
                                    clock_ns=lambda: (self.T_MS + 86_400_000) * 1_000_000)
        self.service = V2QueryService(
            instruments=InstrumentQuery(registry), backend=MemoryMarketDataBackend(),
            entitlements=EntitlementPolicy(runtime.entitlement_grants()), reference_batch=self.batch,
            reference_source_id=runtime.source_id_for, clock_ns=lambda: (self.T_MS + 86_400_000) * 1_000_000,
        )

    def requirement(self, **changes):
        fields = dict(instrument_uid=self.okx.instrument_uid, product=ReferenceProduct.MARK_INDEX_PRICE,
                      consumer_grade=ConsumerGrade.ALPHA, source_policy_id="crypto_liquid_v2", interval="1m",
                      mark_index_kind=__import__("qdl.reference.contracts", fromlist=["MarkIndexKind"]).MarkIndexKind.MARK,
                      start_time_ns=self.OPEN_MS * 1_000_000, end_time_ns=(self.OPEN_MS + 60_000) * 1_000_000,
                      limit=1)
        fields.update(changes)
        return ReferenceDataRequirement(**fields)

    async def read(self, *requirements):
        from qdl.query import AccessPurpose
        from qdl.query.reference import ReferenceBatchRequirement

        return await self.service.reference_data_batch_async(
            ReferenceBatchRequirement("trading-system.paper.stable", requirements, require_all=True),
            purpose=AccessPurpose.INTERNAL_EXECUTION)

    async def test_the_candle_covering_t_comes_back(self):
        result = await self.read(self.requirement())
        item = result.results[0]
        self.assertIsNone(item.problem, item.problem)
        (observation,) = item.result.observations
        self.assertEqual(observation.observed_at_ns, self.OPEN_MS * 1_000_000)
        self.assertLessEqual(observation.observed_at_ns, self.T_MS * 1_000_000)
        labels = dict(observation.labels)
        self.assertEqual((labels["price_type"], labels["interval"], labels["confirm"]), ("MARK", "1m", "1"))
        self.assertGreater(int(labels["candle_close_ns"]), self.T_MS * 1_000_000)
        self.assertEqual([f.name for f in observation.fields], ["mark_open", "mark_high", "mark_low", "mark_close"])
        self.assertEqual(item.result.lineage[0].provider_endpoint.split("/")[-1], "history-mark-price-candles")

    async def test_identical_reads_share_one_provider_call_and_a_closed_candle_is_cached_long(self):
        import asyncio

        await asyncio.gather(*(self.read(self.requirement()) for _ in range(5)))
        await self.read(self.requirement())
        self.assertEqual(self.history.calls, 1)
        from qdl.reference.batch import ReferenceBatchPolicy

        self.assertEqual(ReferenceBatchPolicy().closed_candle_ttl_seconds, 21_600.0)

    async def test_binance_history_is_typed_unavailable(self):
        result = await self.read(self.requirement(instrument_uid=self.binance.instrument_uid))
        self.assertIsNotNone(result.results[0].problem)

    def test_snapshot_and_execution_shapes_are_unchanged(self):
        from qdl.reference.contracts import MarkIndexKind

        # The current snapshot (no window, no interval) still validates for ALPHA and EXECUTION.
        for grade in (ConsumerGrade.ALPHA, ConsumerGrade.EXECUTION):
            ReferenceDataRequirement(instrument_uid="u", product=ReferenceProduct.MARK_INDEX_PRICE,
                                     consumer_grade=grade, source_policy_id="p", limit=1, max_pages=1)
        refused = {
            "execution with a window": dict(consumer_grade=ConsumerGrade.EXECUTION),
            "snapshot with an interval": dict(start_time_ns=None, end_time_ns=None),
            "BOTH": dict(mark_index_kind=MarkIndexKind.BOTH),
            "5m": dict(interval="5m"),
            "misaligned": dict(start_time_ns=(self.OPEN_MS + 1) * 1_000_000),
            "limit 101": dict(limit=101, end_time_ns=(self.OPEN_MS + 101 * 60_000) * 1_000_000),
        }
        for name, change in refused.items():
            with self.subTest(case=name), self.assertRaises(ValueError) as raised:
                self.requirement(**change)
            if name == "execution with a window":
                self.assertEqual(str(raised.exception), "MARK_INDEX_PRICE is a snapshot request")

    def test_sdk_2_9_builds_the_same_request_and_keeps_the_2_8_shapes(self):
        from qdl_sdk.reference import Grade, MarkIndexKind, ReferenceProduct as SdkProduct, ReferenceRequirement

        sdk = ReferenceRequirement(instrument_uid="u", product=SdkProduct.MARK_INDEX_PRICE, consumer_grade=Grade.ALPHA,
                                   source_policy_id="crypto_liquid_v2", interval="1m",
                                   mark_index_kind=MarkIndexKind.MARK, start_time_ns=self.OPEN_MS * 1_000_000,
                                   end_time_ns=(self.OPEN_MS + 60_000) * 1_000_000, limit=1)
        wire = sdk.model_dump(mode="json")
        self.assertEqual((wire["product"], wire["interval"], wire["mark_index_kind"]), ("MARK_INDEX_PRICE", "1m", "MARK"))
        with self.assertRaises(ValueError):
            ReferenceRequirement(instrument_uid="u", product=SdkProduct.MARK_INDEX_PRICE,
                                 consumer_grade=Grade.EXECUTION, source_policy_id="p", interval="1m",
                                 start_time_ns=self.OPEN_MS * 1_000_000, end_time_ns=(self.OPEN_MS + 60_000) * 1_000_000,
                                 limit=1)
        # A 2.8-shaped snapshot request still serializes without any new field.
        snapshot = ReferenceRequirement(instrument_uid="u", product=SdkProduct.MARK_INDEX_PRICE,
                                        consumer_grade=Grade.EXECUTION, source_policy_id="p", limit=1, max_pages=1)
        self.assertIsNone(snapshot.model_dump(mode="json")["interval"])
