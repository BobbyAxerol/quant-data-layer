"""KN-1 K1.5: the native gateway's authorization bundle is exactly what the
Python loaders enforce, and its hash is reproducible.

The Rust gateway refuses a bundle whose canonical SHA-256 differs
(`rust/qdl-stream-gateway/src/bundle.rs`), so the hash rule is pinned here too.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest

from qdl.consumer.manifest import ConsumerManifestLoader
from qdl.runtime.stable_catalog import StableSourceCatalog

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("kn_gateway_bundle", ROOT / "scripts/kn_gateway_bundle.py")
assert _SPEC is not None and _SPEC.loader is not None
BUNDLE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = BUNDLE
_SPEC.loader.exec_module(BUNDLE)

CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
MANIFESTS = sorted((ROOT / "consumers/stable").glob("*.yaml"))


class GatewayBundleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bundle = BUNDLE.compile_bundle(environment="paper", catalog_path=CATALOG, manifest_paths=MANIFESTS)

    def test_hash_is_canonical_and_reproducible(self):
        again = BUNDLE.compile_bundle(environment="paper", catalog_path=CATALOG, manifest_paths=reversed(MANIFESTS))
        self.assertEqual(self.bundle, again)
        body = {key: value for key, value in self.bundle.items() if key != "sha256"}
        expected = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(self.bundle["sha256"], expected)

    def test_bindings_and_manifests_match_the_loaders(self):
        catalog = StableSourceCatalog.load(CATALOG)
        self.assertEqual(len(self.bundle["catalog"]["bindings"]), len(catalog.bindings))
        self.assertEqual(self.bundle["catalog"]["catalog_revision"], catalog.catalog_revision)
        by_id = {item["binding_id"]: item for item in self.bundle["catalog"]["bindings"]}
        for binding in catalog.bindings:
            row = by_id[binding.binding_id]
            self.assertEqual(row["physical_key"], binding.partition_key)
            self.assertTrue(row["product_key"].startswith("lpk1|paper|"))
        manifests = {m["consumer_id"]: m for m in self.bundle["manifests"]}
        for path in MANIFESTS:
            loaded = ConsumerManifestLoader.load(path)
            row = manifests[loaded.consumer_id]
            self.assertEqual(row["manifest_revision"], loaded.manifest_revision)
            self.assertEqual(row["subject"], loaded.subject)
            self.assertEqual(len(row["requirements"]), len(loaded.requirements))

    def test_book_snapshot_and_delta_share_a_physical_key_but_not_a_product(self):
        rows = self.bundle["catalog"]["bindings"]
        books = [r for r in rows if r["feed"] in {"BOOK_SNAPSHOT", "BOOK_DELTA"}]
        by_physical: dict[str, set[str]] = {}
        for row in books:
            by_physical.setdefault(row["physical_key"], set()).add(row["product_key"])
        self.assertTrue(by_physical)
        self.assertTrue(all(len(products) == 2 for products in by_physical.values()))


if __name__ == "__main__":
    unittest.main()
