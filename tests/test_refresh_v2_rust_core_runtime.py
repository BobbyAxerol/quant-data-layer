from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    STABLE_CORE_DEDUP_CAPACITY,
    StableAcquisitionPlan,
    stable_authority_record,
    write_stable_runtime_bundle,
)
from scripts.refresh_v2_rust_core_runtime import (
    CORE_FILES,
    _HOT_L2_SOURCE_IDS,
    refresh,
)


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
OLD_IMAGE = "sha256:" + "a" * 64
NEW_IMAGE = "sha256:" + "b" * 64


class RustCoreRuntimeRefreshTests(unittest.TestCase):
    def _authority(self) -> dict[str, object]:
        return stable_authority_record(
            rust_image_digest=OLD_IMAGE,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION.read_bytes(),
            effective_at_ns=1,
            mode="RUST_PRIMARY",
            revision=1,
            slice_id="qdl-v2-rust-core-refresh-test",
            approved_by="test",
        )

    def _active_runtime(self, root: Path) -> tuple[Path, Path, bytes]:
        runtime = root / "runtime"
        catalog = StableSourceCatalog.load(CATALOG)
        acquisition = StableAcquisitionPlan.load(ACQUISITION, catalog=catalog)
        authority = self._authority()
        write_stable_runtime_bundle(runtime, catalog=catalog, acquisition=acquisition, authority=authority)
        authority_bytes = (runtime / "authority.json").read_bytes()
        for name in CORE_FILES:
            payload = json.loads((runtime / name).read_text())
            payload["core"]["dedup_capacity"] = 1_000_000
            for binding in payload["core"]["bindings"]:
                if binding.get("source_id") in _HOT_L2_SOURCE_IDS:
                    binding["l2"].pop("materialized_snapshot_interval_ms", None)
            (runtime / name).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        (runtime / "unrelated.json").write_text('{"untouched":true}\n')
        environment = root / "rollout.env"
        environment.write_text(
            "QDL_CONFIG_REVISION=phasec36-reference-l2-r13\n"
            f"QDL_STABLE_RUST_IMAGE={OLD_IMAGE}\n"
            "QDL_STABLE_RUNTIME_DIR=/runtime\n"
        )
        return runtime, environment, authority_bytes

    def _refresh(self, root: Path, *, apply: bool) -> dict[str, object]:
        runtime, environment, _ = self._active_runtime(root)
        return refresh(
            runtime_dir=runtime,
            rollout_env=environment,
            active_rust_image=OLD_IMAGE,
            new_rust_image=NEW_IMAGE,
            output_dir=root / "state" / "refresh" if apply else None,
            apply=apply,
            state_root=root / "state",
        )

    def test_dry_run_proves_only_declared_hot_l2_materialization_changes(self):
        with tempfile.TemporaryDirectory() as raw:
            result = self._refresh(Path(raw), apply=False)
            self.assertEqual(result["status"], "DRY_RUN")
            self.assertTrue(result["authority_bytes_preserved"])
            self.assertEqual(len(result["changes"]), 3)
            self.assertTrue(all(item["l2_source_ids"] for item in result["changes"]))
            self.assertTrue(
                all(
                    item["dedup_capacity"]
                    == {"before": 1_000_000, "after": STABLE_CORE_DEDUP_CAPACITY}
                    for item in result["changes"]
                )
            )
            self.assertEqual(result["production_mutations"], 0)

    def test_apply_keeps_authority_and_unrelated_runtime_file(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime, environment, authority_before = self._active_runtime(root)
            result = refresh(
                runtime_dir=runtime,
                rollout_env=environment,
                active_rust_image=OLD_IMAGE,
                new_rust_image=NEW_IMAGE,
                output_dir=root / "state" / "refresh",
                apply=True,
                state_root=root / "state",
            )
            self.assertEqual(result["status"], "APPLIED")
            self.assertEqual((runtime / "authority.json").read_bytes(), authority_before)
            self.assertEqual((runtime / "unrelated.json").read_text(), '{"untouched":true}\n')
            self.assertIn(NEW_IMAGE, environment.read_text())
            for name in CORE_FILES:
                payload = json.loads((runtime / name).read_text())
                self.assertEqual(payload["core"]["dedup_capacity"], STABLE_CORE_DEDUP_CAPACITY)
                self.assertTrue(all(
                    binding["l2"].get("snapshot_refresh_seconds") == 30
                    for binding in payload["core"]["bindings"]
                    if binding.get("l2") is not None
                ))
                self.assertEqual(
                    {
                        binding["source_id"]
                        for binding in payload["core"]["bindings"]
                        if binding.get("l2", {}).get("materialized_snapshot_interval_ms") == 1000
                    },
                    _HOT_L2_SOURCE_IDS,
                )
                self.assertTrue((root / "state" / "refresh" / "rollback" / name).is_file())

    def test_config_only_refresh_reuses_selected_immutable_image(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime, environment, _ = self._active_runtime(root)
            environment.write_text(
                "QDL_CONFIG_REVISION=phasec36-reference-l2-r13\n"
                f"QDL_STABLE_RUST_IMAGE={NEW_IMAGE}\n"
                "QDL_STABLE_RUNTIME_DIR=/runtime\n"
            )
            result = refresh(
                runtime_dir=runtime,
                rollout_env=environment,
                active_rust_image=NEW_IMAGE,
                new_rust_image=NEW_IMAGE,
                output_dir=None,
                apply=False,
                state_root=root / "state",
            )
            self.assertFalse(result["image_selector_changed"])
            self.assertEqual(result["production_mutations"], 0)

    def test_fails_closed_when_non_l2_binding_changes(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime, environment, _ = self._active_runtime(root)
            payload = json.loads((runtime / "core.json").read_text())
            binding = next(item for item in payload["core"]["bindings"] if item.get("l2") is None)
            binding["native_symbol"] = "unexpected"
            (runtime / "core.json").write_text(json.dumps(payload))
            with self.assertRaisesRegex(ValueError, "non-L2 binding"):
                refresh(
                    runtime_dir=runtime,
                    rollout_env=environment,
                    active_rust_image=OLD_IMAGE,
                    new_rust_image=NEW_IMAGE,
                    output_dir=None,
                    apply=False,
                    state_root=root / "state",
                )

    def test_fails_closed_when_dedup_capacity_is_not_an_approved_predecessor(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime, environment, _ = self._active_runtime(root)
            payload = json.loads((runtime / "core.json").read_text())
            payload["core"]["dedup_capacity"] = 100_001
            (runtime / "core.json").write_text(json.dumps(payload))
            with self.assertRaisesRegex(ValueError, "bounded dedup transition"):
                refresh(
                    runtime_dir=runtime,
                    rollout_env=environment,
                    active_rust_image=OLD_IMAGE,
                    new_rust_image=NEW_IMAGE,
                    output_dir=None,
                    apply=False,
                    state_root=root / "state",
                )


if __name__ == "__main__":
    unittest.main()
