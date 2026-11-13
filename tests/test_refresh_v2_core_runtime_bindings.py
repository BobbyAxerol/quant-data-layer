from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan, stable_authority_record
from scripts.refresh_v2_core_runtime_bindings import render, successor

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"


class CoreRuntimeBindingsRefreshTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = StableSourceCatalog.load(CATALOG)
        cls.acquisition = StableAcquisitionPlan.load(ACQUISITION, catalog=cls.catalog)
        cls.authority = stable_authority_record(
            rust_image_digest="sha256:" + "b" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION.read_bytes(),
            effective_at_ns=1, mode="RUST_PRIMARY", revision=1,
            slice_id="qdl-v2-core-bindings-refresh-test", approved_by="test")

    def _older_core(self, *, drop: int = 6) -> dict:
        """A deployed-shaped core: the generated config minus the last QUOTE/MARK bindings, one revision older."""
        core = self.acquisition.core_config(catalog=self.catalog, authority=self.authority)
        bindings = core["core"]["bindings"]
        dropped = [b for b in bindings if b["provider_kind"] in ("okx_bbo", "okx_mark_price", "okx_index_price")][-drop:]
        core["core"]["bindings"] = [b for b in bindings if b not in dropped]
        for item in core["core"]["bindings"]:
            item["instrument_catalog_revision"] = self.catalog.catalog_revision - 1
        return core

    def test_appends_only_new_bindings_and_keeps_retained_bytes(self):
        active = self._older_core()
        doc, added = successor(active, catalog=self.catalog, acquisition=self.acquisition, name="core.json")
        self.assertEqual(len(added), 6)
        self.assertEqual(doc["core"]["bindings"][: len(active["core"]["bindings"])], active["core"]["bindings"])
        self.assertTrue(all(b["instrument_catalog_revision"] == self.catalog.catalog_revision for b in added))
        self.assertEqual(doc["authority"], active["authority"])
        again, none = successor(doc, catalog=self.catalog, acquisition=self.acquisition, name="core.json")
        self.assertEqual(none, [])
        self.assertEqual(again, doc)

    def test_refuses_removal_contract_change_and_non_binding_change(self):
        removal = self._older_core()
        extra = dict(removal["core"]["bindings"][0], source_id="retired-source")
        removal["core"]["bindings"].append(extra)
        with self.assertRaisesRegex(ValueError, "would remove core binding"):
            successor(removal, catalog=self.catalog, acquisition=self.acquisition, name="core.json")
        drift = self._older_core()
        drift["core"]["bindings"][0]["sequence_policy"] = "UNEXPECTED"
        with self.assertRaisesRegex(ValueError, "changes the contract"):
            successor(drift, catalog=self.catalog, acquisition=self.acquisition, name="core.json")
        knob = self._older_core()
        knob["core"]["dedup_capacity"] = 1
        with self.assertRaisesRegex(ValueError, "non-binding core field"):
            successor(knob, catalog=self.catalog, acquisition=self.acquisition, name="core.json")

    def test_render_writes_new_files_and_requires_equal_additions(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            paths = []
            for name in ("rust_core.json", "rust_core_2.json", "rust_core_3.json"):
                path = root / name
                path.write_text(json.dumps(self._older_core()), encoding="utf-8")
                paths.append(path)
            before = [p.read_bytes() for p in paths]
            result = render(cores=paths, output_dir=root / "out")
            self.assertEqual(result["production_mutations"], 0)
            self.assertEqual({v["added"] for v in result["files"].values()}, {6})
            self.assertEqual([p.read_bytes() for p in paths], before)
            self.assertTrue((root / "out" / "receipt.json").is_file())
            with self.assertRaises(FileExistsError):
                render(cores=paths, output_dir=root / "out")
            paths[2].write_text(json.dumps(self._older_core(drop=4)), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "different additions"):
                render(cores=paths, output_dir=root / "out2")


if __name__ == "__main__":
    unittest.main()
