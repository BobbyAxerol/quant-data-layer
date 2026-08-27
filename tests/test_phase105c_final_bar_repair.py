from __future__ import annotations

import io
import json
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from qdl.certification.phase105c_final_bar import (
    RECREATED_SERVICES,
    final_bar_binding_ids,
    prepare_final_bar_repair,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan, stable_authority_record
from scripts.phase105c_prepare_final_bar_repair import main


ROOT = Path(__file__).resolve().parents[1]


class Phase105CFinalBarRepairTests(unittest.TestCase):
    @staticmethod
    def _rollback(previous_checkpoint: str) -> dict[str, object]:
        images = {
            "ingestor_okx_swap": "sha256:" + "1" * 64,
            "binance_bar_edge": "sha256:" + "2" * 64,
            "rust_core": "sha256:" + "3" * 64,
            "query_v2_1": "sha256:" + "4" * 64,
            "query_v2_2": "sha256:" + "4" * 64,
            "stream_v2_active": "sha256:" + "5" * 64,
            "stream_v2_passive": "sha256:" + "5" * 64,
        }
        return {
            service: {
                "image_digest": image,
                "runtime_dir": (
                    "/home/bobby/.local/state/qdl-v2/phase105c-r10/runtime"
                    if service in {"binance_bar_edge", "stream_v2_active", "stream_v2_passive"}
                    else "/home/bobby/.local/state/qdl-v2/phase103-r1/runtime"
                ),
                "checkpoint_path": previous_checkpoint if service == "binance_bar_edge" else None,
            }
            for service, image in images.items()
        }

    def _acquisition(self) -> tuple[StableSourceCatalog, StableAcquisitionPlan]:
        catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
        return catalog, StableAcquisitionPlan.load(
            ROOT / "config/v2/stable-acquisition-bindings.yaml", catalog=catalog
        )

    def _active_runtime(self, root: Path) -> tuple[Path, bytes]:
        active = root / "active-runtime"
        active.mkdir()
        authority = stable_authority_record(
            rust_image_digest="sha256:" + "a" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=(ROOT / "config/v2/stable-acquisition-bindings.yaml").read_bytes(),
            effective_at_ns=time.time_ns(),
            mode="RUST_PRIMARY",
            revision=1,
            slice_id="qdl-v2-shared-realtime-primary",
            approved_by="BobbyAxerol",
        )
        # Deliberately non-canonical whitespace proves C1 preserves the exact
        # authority bytes that existing Rust cores mounted.
        encoded = (json.dumps(authority, indent=4, sort_keys=False) + "\n").encode()
        (active / "authority.json").write_bytes(encoded)
        return active, encoded

    def _prepare(self, root: Path):
        active, authority_bytes = self._active_runtime(root)
        output = root / "packet"
        previous_checkpoint = "/var/lib/qdl-stable/runtime/stable-crypto-bar-edge-old.json"
        packet = prepare_final_bar_repair(
            active_runtime_dir=active.resolve(),
            output_dir=output.resolve(),
            host_runtime_dir=Path("/home/bobby/.local/state/qdl-v2/phase105c-test/runtime"),
            python_image_digest="sha256:" + "b" * 64,
            rust_image_digest="sha256:" + "a" * 64,
            source_commit="c8ecb69",
            previous_bar_state_path=previous_checkpoint,
            rollback_provenance=self._rollback(previous_checkpoint),
        )
        return packet, output, authority_bytes

    def test_prepare_preserves_authority_and_moves_okx_final_bar_to_rest(self):
        with tempfile.TemporaryDirectory() as raw:
            packet, output, authority_bytes = self._prepare(Path(raw))
            catalog, acquisition = self._acquisition()
            self.assertEqual((output / "runtime" / "authority.json").read_bytes(), authority_bytes)
            self.assertTrue(packet["runtime"]["authority_bytes_preserved"])
            self.assertEqual(packet["acquisition_revision"], acquisition.revision)
            self.assertEqual(packet["recreated_services"], list(RECREATED_SERVICES))
            self.assertIn("rust_core", packet["recreated_services"])
            self.assertNotIn("rust_core", packet["excluded_services"])
            self.assertEqual(
                packet["final_bar"]["binding_ids"],
                sorted(final_bar_binding_ids(catalog, acquisition)),
            )
            self.assertEqual(packet["final_bar"]["warmup_rows_max"], 1000)
            self.assertNotEqual(
                packet["final_bar"]["previous_checkpoint_path"],
                packet["final_bar"]["new_checkpoint_path"],
            )
            okx = json.loads((output / "runtime" / "ingestor-okx-swap.json").read_text())
            self.assertEqual(okx["config_revision"], acquisition.revision)
            self.assertFalse(any(item["feed"] == "BAR" for item in okx["bindings"]))
            core = json.loads((output / "runtime" / "core.json").read_text())["core"]
            final_sources = {
                item["source_id"]
                for item in core["bindings"]
                if item["require_final_bar"]
            }
            expected_final_sources = {
                item.source_id
                for item in catalog.bindings
                if item.binding_id in final_bar_binding_ids(catalog, acquisition)
            }
            self.assertTrue(expected_final_sources.issubset(final_sources))
            compose = (output / "compose.env").read_text()
            self.assertIn(
                f"QDL_CONFIG_REVISION=phase105c-final-bar-r{acquisition.revision}\n",
                compose,
            )
            self.assertIn("QDL_STABLE_BAR_STATE_PATH=", compose)
            self.assertEqual(
                set(packet["rollback"]["services"]), set(RECREATED_SERVICES)
            )
            self.assertEqual(
                packet["rollback"]["services"]["binance_bar_edge"]["checkpoint_path"],
                packet["final_bar"]["previous_checkpoint_path"],
            )

    def test_rejects_malformed_active_authority_without_creating_packet(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            active = root / "active"
            active.mkdir()
            (active / "authority.json").write_text("{}\n")
            output = root / "packet"
            with self.assertRaisesRegex(ValueError, "authority"):
                prepare_final_bar_repair(
                    active_runtime_dir=active.resolve(),
                    output_dir=output.resolve(),
                    host_runtime_dir=Path("/home/bobby/.local/state/qdl-v2/test/runtime"),
                    python_image_digest="sha256:" + "b" * 64,
                    rust_image_digest="sha256:" + "a" * 64,
                    source_commit="c8ecb69",
                    previous_bar_state_path="/var/lib/qdl-stable/runtime/old.json",
                    rollback_provenance=self._rollback("/var/lib/qdl-stable/runtime/old.json"),
                )
            self.assertFalse(output.exists())

    def test_dry_run_retains_no_packet_directory(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            active, _ = self._active_runtime(root)
            output = root / "must-not-exist"
            rollback = root / "rollback.json"
            previous_checkpoint = "/var/lib/qdl-stable/runtime/old.json"
            rollback.write_text(json.dumps(self._rollback(previous_checkpoint)), encoding="utf-8")
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                status = main([
                    "--active-runtime-dir", str(active.resolve()),
                    "--output-dir", str(output.resolve()),
                    "--host-runtime-dir", "/home/bobby/.local/state/qdl-v2/test/runtime",
                    "--python-image-digest", "sha256:" + "b" * 64,
                    "--rust-image-digest", "sha256:" + "a" * 64,
                    "--source-commit", "c8ecb69",
                    "--previous-bar-state-path", "/var/lib/qdl-stable/runtime/old.json",
                    "--rollback-provenance", str(rollback),
                ])
            self.assertEqual(status, 0)
            self.assertFalse(output.exists())
            self.assertEqual(json.loads(stdout.getvalue())["status"], "DRY_RUN")

    def test_rejects_missing_role_or_checkpoint_mismatch(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            active, _ = self._active_runtime(root)
            previous_checkpoint = "/var/lib/qdl-stable/runtime/old.json"
            rollback = self._rollback(previous_checkpoint)
            rollback.pop("rust_core")
            with self.assertRaisesRegex(ValueError, "exactly recreated services"):
                prepare_final_bar_repair(
                    active_runtime_dir=active.resolve(),
                    output_dir=(root / "missing-role").resolve(),
                    host_runtime_dir=Path("/home/bobby/.local/state/qdl-v2/test/runtime"),
                    python_image_digest="sha256:" + "b" * 64,
                    rust_image_digest="sha256:" + "a" * 64,
                    source_commit="c8ecb69",
                    previous_bar_state_path=previous_checkpoint,
                    rollback_provenance=rollback,
                )
            rollback = self._rollback(previous_checkpoint)
            rollback["binance_bar_edge"]["checkpoint_path"] = (
                "/var/lib/qdl-stable/runtime/not-the-prior.json"
            )
            with self.assertRaisesRegex(ValueError, "checkpoint differs"):
                prepare_final_bar_repair(
                    active_runtime_dir=active.resolve(),
                    output_dir=(root / "bad-checkpoint").resolve(),
                    host_runtime_dir=Path("/home/bobby/.local/state/qdl-v2/test/runtime"),
                    python_image_digest="sha256:" + "b" * 64,
                    rust_image_digest="sha256:" + "a" * 64,
                    source_commit="c8ecb69",
                    previous_bar_state_path=previous_checkpoint,
                    rollback_provenance=rollback,
                )


if __name__ == "__main__":
    unittest.main()
