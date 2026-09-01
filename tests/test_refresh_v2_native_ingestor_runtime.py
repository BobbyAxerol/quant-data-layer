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
from scripts.refresh_v2_native_ingestor_runtime import TARGETS, refresh


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
RUST_IMAGE = "sha256:" + "a" * 64
REMOVED_BOOKS = {
    "binance-usdm": {"SOLUSDT", "DOGEUSDT", "BNBUSDT"},
    "okx-swap": {"SOL-USDT-SWAP", "DOGE-USDT-SWAP", "BNB-USDT-SWAP"},
}


class NativeIngestorRuntimeRefreshTests(unittest.TestCase):
    def _authority(self) -> dict[str, object]:
        return stable_authority_record(
            rust_image_digest=RUST_IMAGE,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION.read_bytes(),
            effective_at_ns=1,
            mode="RUST_PRIMARY",
            revision=1,
            slice_id="qdl-v2-native-ingestor-refresh-test",
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
        for lane, file_name in TARGETS.items():
            path = runtime / file_name
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["config_revision"] = 14
            payload["bindings"] = [
                item
                for item in payload["bindings"]
                if item["feed"] != "BAR"
                and not (
                    item["feed"] == "BOOK"
                    and item["native_symbol"] in REMOVED_BOOKS[lane]
                )
            ]
            for item in payload["bindings"]:
                item["instrument_catalog_revision"] = catalog.catalog_revision - 1
            path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return runtime

    def _refresh(self, root: Path, *, apply: bool) -> dict:
        runtime = self._runtime(root)
        return refresh(
            runtime_dir=runtime,
            output_dir=root / "state" / "native-ingestor-refresh" if apply else None,
            apply=apply,
            state_root=root / "state",
        )

    def test_dry_run_adds_only_the_three_declared_l2_books_per_venue(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            original = {
                name: (runtime / name).read_bytes()
                for name in TARGETS.values()
            }

            result = refresh(
                runtime_dir=runtime,
                output_dir=None,
                apply=False,
                state_root=root / "state",
            )

            self.assertEqual(result["status"], "DRY_RUN")
            self.assertEqual(result["production_mutations"], 0)
            self.assertTrue(result["authority_bytes_preserved"])
            self.assertEqual(
                result["files"]["ingestor-binance-usdm.json"]["added_book_symbols"],
                ["BNBUSDT", "DOGEUSDT", "SOLUSDT"],
            )
            self.assertEqual(
                result["files"]["ingestor-okx-swap.json"]["added_book_symbols"],
                ["BNB-USDT-SWAP", "DOGE-USDT-SWAP", "SOL-USDT-SWAP"],
            )
            self.assertEqual(
                {
                    name: (runtime / name).read_bytes()
                    for name in TARGETS.values()
                },
                original,
            )

    def test_apply_preserves_authority_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            authority_before = (runtime / "authority.json").read_bytes()
            before_modes = {
                file_name: (runtime / file_name).stat().st_mode & 0o777
                for file_name in TARGETS.values()
            }
            result = refresh(
                runtime_dir=runtime,
                output_dir=root / "state" / "native-ingestor-refresh",
                apply=True,
                state_root=root / "state",
            )

            self.assertEqual(result["status"], "APPLIED")
            self.assertEqual(result["production_mutations"], 2)
            self.assertEqual((runtime / "authority.json").read_bytes(), authority_before)
            for file_name in TARGETS.values():
                backup = root / "state" / "native-ingestor-refresh" / "rollback" / file_name
                self.assertTrue(backup.is_file())
                self.assertEqual(
                    backup.stat().st_mode & 0o777,
                    before_modes[file_name],
                )
                self.assertEqual(
                    result["files"][file_name]["before_mode"],
                    oct(before_modes[file_name]),
                )
                payload = json.loads((runtime / file_name).read_text(encoding="utf-8"))
                self.assertEqual(len(payload["bindings"]), 19)
                self.assertFalse(any(item["feed"] == "BAR" for item in payload["bindings"]))

            repeated = refresh(
                runtime_dir=runtime,
                output_dir=None,
                apply=False,
                state_root=root / "state",
            )
            self.assertEqual(repeated["production_mutations"], 0)
            self.assertTrue(all(not item["changed"] for item in repeated["files"].values()))

    def test_refuses_trade_or_quote_contract_drift(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            path = runtime / "ingestor-binance-usdm.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            binding = next(item for item in payload["bindings"] if item["feed"] == "TRADE")
            binding["native_channel"] = "unexpected-channel"
            path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "TRADE binding contract"):
                refresh(
                    runtime_dir=runtime,
                    output_dir=None,
                    apply=False,
                    state_root=root / "state",
                )

    def test_refuses_non_binding_runtime_drift(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            path = runtime / "ingestor-okx-swap.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["heartbeat_seconds"] = 1
            path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "non-binding runtime field"):
                refresh(
                    runtime_dir=runtime,
                    output_dir=None,
                    apply=False,
                    state_root=root / "state",
                )

    def test_apply_requires_a_new_state_root_output(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            with self.assertRaisesRegex(ValueError, "new private QDL state path"):
                refresh(
                    runtime_dir=runtime,
                    output_dir=root / "outside-state",
                    apply=True,
                    state_root=root / "state",
                )


if __name__ == "__main__":
    unittest.main()
