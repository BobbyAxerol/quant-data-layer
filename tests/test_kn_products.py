"""KN-3 D14: the projector's product identity on the real catalog.

Stage A (`rust/qdl-projector/src/products.rs`) maps a canonical record by
(physical key, payload feed) to exactly one binding of the gateway bundle and
refuses a bundle it cannot verify. The Rust unit tests run the transform over
the real records of ``contracts/golden/kn_v220/state_codec.json``; this test
closes the chain on the Python side: the real catalog compiles to a bundle in
which every golden record finds its binding and that binding names the same
logical product key.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import unittest

from qdl.projection.state_contract import LogicalProductKey

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("kn_gateway_bundle", ROOT / "scripts/kn_gateway_bundle.py")
assert _SPEC is not None and _SPEC.loader is not None
BUNDLE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = BUNDLE
_SPEC.loader.exec_module(BUNDLE)

CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
MANIFESTS = sorted((ROOT / "consumers/stable").glob("*.yaml"))
GOLDEN = ROOT / "contracts/golden/kn_v220/state_codec.json"
SAMPLED_FEEDS = {"OPEN_INTEREST", "LONG_SHORT_RATIO", "TAKER_FLOW", "BASIS"}


class ProjectorProductIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.bundle = BUNDLE.compile_bundle(environment="paper", catalog_path=CATALOG, manifest_paths=MANIFESTS)
        cls.bindings = cls.bundle["catalog"]["bindings"]

    def test_one_binding_per_physical_key_and_feed(self):
        pairs = [(row["physical_key"], row["feed"]) for row in self.bindings]
        self.assertEqual(len(pairs), len(set(pairs)))

    def test_the_catalog_passes_the_projector_load_checks(self):
        for row in self.bindings:
            with self.subTest(binding=row["binding_id"]):
                self.assertFalse(row["feed"] in SAMPLED_FEEDS and row["interval"] is not None)
                if row["feed"] == "BAR":
                    self.assertIsNotNone(row["interval"])
                key = LogicalProductKey.for_product(
                    environment="paper", venue=row["venue"], market=row["market"],
                    instrument_uid=row["instrument_uid"], feed=row["feed"], interval=row["interval"],
                )
                self.assertEqual(key.encode(), row["product_key"])

    def test_every_real_golden_record_finds_its_binding_and_product(self):
        by_pair = {(row["physical_key"], row["feed"]): row for row in self.bindings}
        golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
        real = [record for record in golden["records"] if not record["synthetic"]]
        self.assertEqual(len(real), 24)
        for record in real:
            with self.subTest(record=record["name"]):
                feed = record["lpk"].split("|")[5]
                row = by_pair[(record["spool_partition_key"], feed)]
                self.assertEqual(row["product_key"], record["lpk"])


if __name__ == "__main__":
    unittest.main()
