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
from scripts.converge_v2_primary_runtime import (
    CORE_FILES,
    INGESTOR_FILES,
    RUNTIME_FILES,
    _FIVE_LIQUID_BOOK_IDS_BY_INGESTOR,
    _FIVE_LIQUID_PERPETUAL_BOOK_IDS,
    converge,
)


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
RUST_IMAGE = "sha256:" + "a" * 64


class PrimaryRuntimeConvergenceTests(unittest.TestCase):
    def _authority(self) -> dict[str, object]:
        return stable_authority_record(
            rust_image_digest=RUST_IMAGE,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION.read_bytes(),
            effective_at_ns=1,
            mode="RUST_PRIMARY",
            revision=1,
            slice_id="qdl-v2-primary-runtime-convergence-test",
            approved_by="test",
        )

    def _legacy_runtime(self, root: Path) -> Path:
        runtime = root / "runtime"
        catalog = StableSourceCatalog.load(CATALOG)
        acquisition = StableAcquisitionPlan.load(ACQUISITION, catalog=catalog)
        write_stable_runtime_bundle(
            runtime,
            catalog=catalog,
            acquisition=acquisition,
            authority=self._authority(),
        )
        for name in CORE_FILES:
            path = runtime / name
            payload = json.loads(path.read_text(encoding="utf-8"))
            retained = [
                item
                for item in payload["core"]["bindings"]
                if item["source_id"].endswith("stable-001")
                or item.get("native_symbol") in {"FPT", "VN30F1M"}
            ]
            self.assertTrue(retained)
            for item in retained:
                item["instrument_catalog_revision"] = 3
                item["instrument_revision"] = 1
            payload["core"]["bindings"] = retained
            payload["core"]["dedup_capacity"] = 1_000_000
            path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            path.chmod(0o640)
        for name in INGESTOR_FILES:
            path = runtime / name
            payload = json.loads(path.read_text(encoding="utf-8"))
            retained = [
                item
                for item in payload["bindings"]
                if item.get("native_symbol") in {"BTCUSDT", "ETHUSDT", "BTC-USDT-SWAP", "ETH-USDT-SWAP"}
                and item.get("feed") in {"BAR", "TRADE", "QUOTE"}
            ]
            self.assertTrue(retained)
            for item in retained:
                item["instrument_catalog_revision"] = 3
                item["instrument_revision"] = 1
            payload["bindings"] = retained
            payload["config_revision"] = 9
            payload.pop("session_liveness_dir", None)
            payload.pop("session_liveness_write_interval_ms", None)
            path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            path.chmod(0o640)
        return runtime

    def test_dry_run_proves_partial_runtime_converges_without_mutation(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._legacy_runtime(root)
            before = {name: (runtime / name).read_bytes() for name in RUNTIME_FILES}
            result = converge(
                runtime_dir=runtime,
                output_dir=None,
                apply=False,
                state_root=root / "state",
            )
            self.assertEqual(result["status"], "DRY_RUN")
            self.assertEqual(result["production_mutations"], 0)
            self.assertTrue(result["authority_bytes_preserved"])
            for name in CORE_FILES:
                item = result["files"][name]
                self.assertGreater(item["after_binding_count"], item["before_binding_count"])
                self.assertEqual(
                    set(item["added_five_liquid_book_source_ids"]),
                    _FIVE_LIQUID_PERPETUAL_BOOK_IDS,
                )
                self.assertEqual(item["dedup_capacity"], {"before": 1_000_000, "after": 100_000})
            for name in INGESTOR_FILES:
                item = result["files"][name]
                self.assertGreater(item["after_binding_count"], item["before_binding_count"])
                self.assertTrue(
                    _FIVE_LIQUID_BOOK_IDS_BY_INGESTOR[name]
                    <= set(item["added_book_subscription_ids"])
                )
                self.assertEqual(item["session_liveness_write_interval_ms"], 1_000)
            self.assertEqual({name: (runtime / name).read_bytes() for name in RUNTIME_FILES}, before)

    def test_apply_writes_all_five_backups_and_preserves_authority(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._legacy_runtime(root)
            authority_before = (runtime / "authority.json").read_bytes()
            before = {name: (runtime / name).read_bytes() for name in RUNTIME_FILES}
            modes = {name: (runtime / name).stat().st_mode & 0o777 for name in RUNTIME_FILES}
            result = converge(
                runtime_dir=runtime,
                output_dir=root / "state" / "primary-runtime-convergence",
                apply=True,
                state_root=root / "state",
            )
            self.assertEqual(result["production_mutations"], 5)
            self.assertEqual((runtime / "authority.json").read_bytes(), authority_before)
            for name in RUNTIME_FILES:
                backup = root / "state" / "primary-runtime-convergence" / "rollback" / name
                self.assertEqual(backup.read_bytes(), before[name])
                self.assertEqual(backup.stat().st_mode & 0o777, modes[name])
                self.assertEqual((runtime / name).stat().st_mode & 0o777, modes[name])
            self.assertTrue((root / "state" / "primary-runtime-convergence" / "receipt.json").is_file())

    def test_rejects_retained_binding_semantic_drift(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._legacy_runtime(root)
            path = runtime / "ingestor-binance-usdm.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["bindings"][0]["native_channel"] = "invalid"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "semantic drift"):
                converge(
                    runtime_dir=runtime,
                    output_dir=None,
                    apply=False,
                    state_root=root / "state",
                )


if __name__ == "__main__":
    unittest.main()
