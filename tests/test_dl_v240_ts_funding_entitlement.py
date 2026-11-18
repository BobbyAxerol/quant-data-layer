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
                       "revision": 15, "reference": ["FUNDING_RATE"], **consumer}],
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

    def test_one_alpha_grade_funding_row_per_perpetual_mark_uid(self):
        self.assertEqual(self.report["blocked"], [])
        marks = {r["instrument_uid"] for r in self.base_doc["spec"]["requirements"] if r["feed"] == "MARK_INDEX_PRICE"}
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
        self.assertTrue(is_execution_manifest(self.doc))
        self.assertNotIn("FUNDING_RATE", STREAM_FEEDS)
        model = model_manifest(self.doc)
        self.assertEqual(model.products_by_feed["FUNDING_RATE"], 102)
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
