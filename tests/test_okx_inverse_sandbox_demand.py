"""Contract fixtures only; these tests do not activate a broker or consumer."""
from pathlib import Path
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

    def test_native_compilation_keeps_inverse_identity_and_index(self):
        demand = ProductionDemandManifest.load_many([DEMAND])
        bundle = ProductionCatalogBuilder(
            catalog_revision=1, source_policy_revision=1, authority_revision=1,
        ).build(demand=demand, binance_usdm=None, okx_rows=[{
            "instType": "SWAP", "instId": "BTC-USD-SWAP", "instFamily": "BTC-USD",
            "baseCcy": "", "quoteCcy": "", "ctValCcy": "USD", "ctType": "inverse",
            "settleCcy": "BTC", "ctVal": "100", "ctMult": "1", "state": "live",
            "tickSz": "0.1", "lotSz": "0.1",
        }])
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
