from __future__ import annotations

import io
import json
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from qdl.certification.phase105c_final_bar import (
    ACQUISITION_REVISION,
    FINAL_BAR_BINDINGS,
    RECREATED_SERVICES,
    prepare_final_bar_repair,
)
from qdl.runtime.stable_deployment import stable_authority_record
from scripts.phase105c_prepare_final_bar_repair import main


ROOT = Path(__file__).resolve().parents[1]


class Phase105CFinalBarRepairTests(unittest.TestCase):
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
        packet = prepare_final_bar_repair(
            active_runtime_dir=active.resolve(),
            output_dir=output.resolve(),
            host_runtime_dir=Path("/home/bobby/.local/state/qdl-v2/phase105c-test/runtime"),
            python_image_digest="sha256:" + "b" * 64,
            rust_image_digest="sha256:" + "a" * 64,
            source_commit="c8ecb69",
            previous_bar_state_path=(
                "/var/lib/qdl-stable/runtime/stable-crypto-bar-edge-old.json"
            ),
        )
        return packet, output, authority_bytes

    def test_prepare_preserves_authority_and_moves_okx_final_bar_to_rest(self):
        with tempfile.TemporaryDirectory() as raw:
            packet, output, authority_bytes = self._prepare(Path(raw))
            self.assertEqual((output / "runtime" / "authority.json").read_bytes(), authority_bytes)
            self.assertTrue(packet["runtime"]["authority_bytes_preserved"])
            self.assertEqual(packet["acquisition_revision"], ACQUISITION_REVISION)
            self.assertEqual(packet["recreated_services"], list(RECREATED_SERVICES))
            self.assertEqual(packet["final_bar"]["binding_ids"], sorted(FINAL_BAR_BINDINGS))
            self.assertEqual(packet["final_bar"]["warmup_rows_max"], 1000)
            self.assertNotEqual(
                packet["final_bar"]["previous_checkpoint_path"],
                packet["final_bar"]["new_checkpoint_path"],
            )
            okx = json.loads((output / "runtime" / "ingestor-okx-swap.json").read_text())
            self.assertEqual(okx["config_revision"], ACQUISITION_REVISION)
            self.assertFalse(any(item["feed"] == "BAR" for item in okx["bindings"]))
            compose = (output / "compose.env").read_text()
            self.assertIn("QDL_CONFIG_REVISION=phase105c-final-bar-r10\n", compose)
            self.assertIn("QDL_STABLE_BAR_STATE_PATH=", compose)

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
                )
            self.assertFalse(output.exists())

    def test_dry_run_retains_no_packet_directory(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            active, _ = self._active_runtime(root)
            output = root / "must-not-exist"
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
                ])
            self.assertEqual(status, 0)
            self.assertFalse(output.exists())
            self.assertEqual(json.loads(stdout.getvalue())["status"], "DRY_RUN")


if __name__ == "__main__":
    unittest.main()
