"""Contract fixtures only; these tests do not activate a broker or consumer."""
from pathlib import Path
from dataclasses import replace
import tempfile
import yaml

from qdl.query import ConsumerGrade, FeedType
import unittest

from qdl.runtime.production_catalog import ProductionCatalogBuilder, ProductionDemandManifest

ROOT = Path(__file__).resolve().parents[1]
DEMAND = ROOT / "config/v2/okx-inverse-sandbox-demand.yaml"


class InverseSandboxDemandTests(unittest.TestCase):
    def test_additive_scope_preserves_existing_demand(self):
        path = ROOT / "config/v2/stable-crypto-demand.yaml"
        old = ProductionDemandManifest.load_many([path])
        inverse = ProductionDemandManifest.load_many([DEMAND])
        merged = ProductionDemandManifest.load_many([path, DEMAND])
        self.assertEqual(len(inverse.demands), 4)
        self.assertEqual(len(merged.demands), len(old.demands) + 4)
        self.assertTrue(all(row in merged.demands for row in old.demands))
        for row in inverse.demands:
            self.assertEqual(row.consumer_id, "trading-system.sandbox.stable")
            self.assertEqual(row.native_symbol, "BTC-USD-SWAP")
            self.assertIsNone(row.interval)
        self.assertEqual({r.feed.value for r in inverse.demands},
                         {"QUOTE", "MARK_INDEX_PRICE", "BOOK_SNAPSHOT", "BOOK_DELTA"})

    def _compile(self, demand):
        return ProductionCatalogBuilder(
            catalog_revision=1, source_policy_revision=1, authority_revision=1,
        ).build(demand=demand, binance_usdm=None, okx_rows=[{
            "instType": "SWAP", "instId": "BTC-USD-SWAP", "instFamily": "BTC-USD",
            "baseCcy": "", "quoteCcy": "", "ctValCcy": "USD", "ctType": "inverse",
            "settleCcy": "BTC", "ctVal": "100", "ctMult": "1", "state": "live",
            "tickSz": "0.1", "lotSz": "0.1",
        }])

    def test_native_compilation_keeps_inverse_identity_and_index(self):
        bundle = self._compile(ProductionDemandManifest.load_many([DEMAND]))
        record = bundle.source_catalog["instruments"][0]
        self.assertEqual(record["native_symbol"], "BTC-USD-SWAP")
        self.assertEqual(record["base_asset"], "BTC")
        self.assertEqual(record["quote_asset"], "USD")
        self.assertEqual(record["settlement_asset"], "BTC")
        rows = bundle.acquisition_plan["bindings"]
        self.assertEqual(len(rows), 4)
        self.assertTrue(all(r["mode"] == "RUST_NATIVE" for r in rows))
        mark = next(r for r in rows if r["provider_kind"] == "okx_mark_index")
        self.assertEqual(mark["mark_index"]["index_native_symbol"], "BTC-USD")
        books = [r for r in rows if r["provider_kind"] == "okx_book"]
        self.assertEqual(len(books), 2)
        self.assertTrue(all(r["native_channel"] == "books" for r in books))
        self.assertTrue(all(r["sequence_policy"] == "CONTIGUOUS" for r in books))

        self.assertTrue(all(r["l2"]["materialized_snapshot_interval_ms"] == 1000 for r in books))
        self.assertTrue(all(r["l2"]["snapshot_refresh_seconds"] == 30 for r in books))

    def test_research_only_keeps_provider_default_without_hot_snapshot(self):
        demand = ProductionDemandManifest.load_many([DEMAND])
        research = replace(demand, demands=tuple(replace(r, consumer_grade=ConsumerGrade.ALPHA) for r in demand.demands))
        bundle = self._compile(research)
        books = [r for r in bundle.acquisition_plan["bindings"] if "l2" in r]
        self.assertEqual(len(books), 2)
        self.assertTrue(all("materialized_snapshot_interval_ms" not in r["l2"] for r in books))
        current = self._compile(demand)
        for old, new in zip(bundle.acquisition_plan["bindings"], current.acquisition_plan["bindings"]):
            if "l2" not in old:
                self.assertEqual(old, new)

    def test_mixed_alias_grade_uses_one_physical_execution_book(self):
        demand = ProductionDemandManifest.load_many([DEMAND])
        mixed = replace(demand, demands=tuple(replace(r, consumer_grade=ConsumerGrade.ALPHA)
            if r.feed is FeedType.BOOK_SNAPSHOT else r for r in demand.demands))
        books = [r["l2"] for r in self._compile(mixed).acquisition_plan["bindings"] if "l2" in r]
        self.assertEqual(books[0], books[1])
        self.assertEqual(books[0]["materialized_snapshot_interval_ms"], 1000)

    def test_duplicate_research_consumer_cannot_hide_execution_grade(self):
        payload = yaml.safe_load(DEMAND.read_text())
        research = dict(payload["consumers"][0], consumer_id="a-research", consumer_grade="ALPHA")
        payload["consumers"].insert(0, research)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "demand.yaml"
            path.write_text(yaml.safe_dump(payload))
            merged = ProductionDemandManifest.load_many([path])
            self.assertEqual(len(merged.demands), 4)
            self.assertTrue(all(r.consumer_grade is ConsumerGrade.EXECUTION for r in merged.demands))
            books = [r["l2"] for r in self._compile(merged).acquisition_plan["bindings"] if "l2" in r]
            self.assertTrue(all(r["materialized_snapshot_interval_ms"] == 1000 for r in books))
