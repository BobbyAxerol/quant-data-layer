from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    stable_authority_record,
    write_stable_runtime_bundle,
)
from scripts.refresh_v2_l2_core_runtime import (
    CORE_FILES,
    DECLARED_ADDITIVE_BOOK_SOURCE_IDS,
    refresh,
)


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
RUST_IMAGE = "sha256:" + "a" * 64


class L2CoreRuntimeRefreshTests(unittest.TestCase):
    def _authority(self) -> dict[str, object]:
        return stable_authority_record(
            rust_image_digest=RUST_IMAGE,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION.read_bytes(),
            effective_at_ns=1,
            mode="RUST_PRIMARY",
            revision=1,
            slice_id="qdl-v2-l2-core-refresh-test",
            approved_by="test",
        )

    def _runtime(self, root: Path) -> Path:
        runtime = root / "runtime"
        catalog = StableSourceCatalog.load(CATALOG)
        acquisition = StableAcquisitionPlan.load(ACQUISITION, catalog=catalog)
        write_stable_runtime_bundle(
            runtime,
            catalog=catalog,
            acquisition=acquisition,
            authority=self._authority(),
        )
        for file_name in CORE_FILES:
            path = runtime / file_name
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["core"]["bindings"] = [
                item
                for item in payload["core"]["bindings"]
                if item["source_id"] not in DECLARED_ADDITIVE_BOOK_SOURCE_IDS
            ]
            for item in payload["core"]["bindings"]:
                item["instrument_catalog_revision"] = 7
            path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            path.chmod(0o644)
        return runtime

    def test_dry_run_adds_exact_declared_l2_scope_without_mutation(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            before = {name: (runtime / name).read_bytes() for name in CORE_FILES}
            result = refresh(
                runtime_dir=runtime,
                output_dir=None,
                apply=False,
                state_root=root / "state",
            )
            self.assertEqual(result["status"], "DRY_RUN")
            self.assertEqual(result["production_mutations"], 0)
            self.assertTrue(result["authority_bytes_preserved"])
            for file_name in CORE_FILES:
                item = result["files"][file_name]
                self.assertEqual(item["before_binding_count"], 176)
                self.assertEqual(item["after_binding_count"], 182)
                self.assertEqual(item["catalog_revision_updated_binding_count"], 176)
                self.assertEqual(item["before_catalog_revisions"], [7])
                self.assertEqual(item["after_catalog_revisions"], [8])
                self.assertEqual(set(item["added_book_source_ids"]), DECLARED_ADDITIVE_BOOK_SOURCE_IDS)
            self.assertEqual({name: (runtime / name).read_bytes() for name in CORE_FILES}, before)

    def test_apply_preserves_existing_bindings_authority_and_modes(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            authority_before = (runtime / "authority.json").read_bytes()
            before = {name: (runtime / name).read_bytes() for name in CORE_FILES}
            modes = {name: (runtime / name).stat().st_mode & 0o777 for name in CORE_FILES}
            result = refresh(
                runtime_dir=runtime,
                output_dir=root / "state" / "l2-core-refresh",
                apply=True,
                state_root=root / "state",
            )
            self.assertEqual(result["production_mutations"], 3)
            self.assertEqual((runtime / "authority.json").read_bytes(), authority_before)
            for file_name in CORE_FILES:
                backup = root / "state" / "l2-core-refresh" / "rollback" / file_name
                self.assertEqual(backup.read_bytes(), before[file_name])
                self.assertEqual(backup.stat().st_mode & 0o777, modes[file_name])
                self.assertEqual((runtime / file_name).stat().st_mode & 0o777, modes[file_name])
                active = json.loads(before[file_name])["core"]["bindings"]
                updated = json.loads((runtime / file_name).read_text(encoding="utf-8"))["core"]["bindings"]
                active_by_id = {item["source_id"]: item for item in active}
                updated_by_id = {item["source_id"]: item for item in updated}
                self.assertTrue(set(active_by_id).issubset(updated_by_id))
                self.assertTrue(all(
                    {
                        field: value
                        for field, value in updated_by_id[key].items()
                        if field != "instrument_catalog_revision"
                    }
                    == {
                        field: value
                        for field, value in active_by_id[key].items()
                        if field != "instrument_catalog_revision"
                    }
                    for key in active_by_id
                ))
                self.assertTrue(all(
                    updated_by_id[key]["instrument_catalog_revision"] == 8
                    for key in active_by_id
                ))
                self.assertEqual(set(updated_by_id) - set(active_by_id), DECLARED_ADDITIVE_BOOK_SOURCE_IDS)

    def test_dry_run_converges_stale_metadata_after_l2_was_already_added(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            catalog = StableSourceCatalog.load(CATALOG)
            acquisition = StableAcquisitionPlan.load(ACQUISITION, catalog=catalog)
            generated = root / "generated"
            write_stable_runtime_bundle(
                generated,
                catalog=catalog,
                acquisition=acquisition,
                authority=self._authority(),
            )
            for file_name in CORE_FILES:
                path = runtime / file_name
                active = json.loads(path.read_text(encoding="utf-8"))
                expected = json.loads((generated / file_name).read_text(encoding="utf-8"))
                existing_ids = {item["source_id"] for item in active["core"]["bindings"]}
                active["core"]["bindings"].extend(
                    item for item in expected["core"]["bindings"]
                    if item["source_id"] in DECLARED_ADDITIVE_BOOK_SOURCE_IDS
                    and item["source_id"] not in existing_ids
                )
                path.write_text(json.dumps(active, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            result = refresh(
                runtime_dir=runtime,
                output_dir=None,
                apply=False,
                state_root=root / "state",
            )
            for file_name in CORE_FILES:
                item = result["files"][file_name]
                self.assertEqual(item["before_binding_count"], 182)
                self.assertEqual(item["after_binding_count"], 182)
                self.assertEqual(item["catalog_revision_updated_binding_count"], 176)
                self.assertEqual(item["added_book_source_ids"], [])
                self.assertEqual(set(item["declared_book_source_ids"]), DECLARED_ADDITIVE_BOOK_SOURCE_IDS)

    def test_rejects_existing_semantic_drift(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            path = runtime / "core.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["core"]["bindings"][0]["native_symbol"] = "unexpected"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "semantic drift"):
                refresh(
                    runtime_dir=runtime,
                    output_dir=None,
                    apply=False,
                    state_root=root / "state",
                )

    def test_rejects_nonbinding_core_drift(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            path = runtime / "core.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["core"]["dedup_capacity"] = 42
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "non-binding core configuration"):
                refresh(
                    runtime_dir=runtime,
                    output_dir=None,
                    apply=False,
                    state_root=root / "state",
                )

    def test_rejects_unknown_active_binding(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            path = runtime / "core-002.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            extra = dict(payload["core"]["bindings"][0])
            extra["source_id"] = "unknown-book-source"
            payload["core"]["bindings"].append(extra)
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "absent from current catalog"):
                refresh(
                    runtime_dir=runtime,
                    output_dir=None,
                    apply=False,
                    state_root=root / "state",
                )

    def test_apply_requires_new_private_state_directory(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            with self.assertRaisesRegex(ValueError, "new private QDL state path"):
                refresh(
                    runtime_dir=self._runtime(root),
                    output_dir=root / "outside-state",
                    apply=True,
                    state_root=root / "state",
                )


if __name__ == "__main__":
    unittest.main()
