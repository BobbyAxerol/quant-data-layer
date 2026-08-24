from __future__ import annotations

from pathlib import Path
import tempfile
import time
import unittest

from scripts.phase103_consumer_receipt_acceptance import (
    _cursor_directory,
    _validated_packet,
    parser,
)
from scripts.phase103_prepare_shared_primary_packet import (
    prepare_shared_primary_packet,
    validate_prepared_shared_primary_bundle as generator_bundle_validator,
)
from scripts.phase103_apply_shared_primary_broker_scope import (
    validate_prepared_shared_primary_bundle as broker_bundle_validator,
)
from scripts.phase103_packet_contract import (
    SHARED_REALTIME_CORE_GROUP_ID,
    SHARED_REALTIME_CORE_ID_PREFIX,
    validate_prepared_shared_primary_bundle as contract_bundle_validator,
)
from qdl.runtime.stable_deployment import (
    SHARED_REALTIME_CORE_GROUP_ID as DEPLOYMENT_CORE_GROUP_ID,
    SHARED_REALTIME_CORE_ID_PREFIX as DEPLOYMENT_CORE_ID_PREFIX,
)


ROOT = Path(__file__).resolve().parents[1]


class Phase103ConsumerReceiptHarnessTests(unittest.TestCase):
    def test_parser_requires_the_sealed_handoff_coordinates(self):
        parsed = parser().parse_args(
            [
                "--primary-url", "https://query-a",
                "--secondary-url", "https://query-b",
                "--grpc-target", "stream-a:8210,stream-b:8210",
                "--handoff-packet", "/tmp/packet.json",
                "--runtime-dir", "/tmp/runtime",
                "--tls-ca-file", "/tmp/ca.crt",
                "--trading-tls-certificate-file", "/tmp/trading.crt",
                "--trading-tls-private-key-file", "/tmp/trading.key",
                "--trading-jwt-private-key-file", "/tmp/trading-jwt.key",
                "--trading-jwt-key-id", "trading-key",
                "--alpha-tls-certificate-file", "/tmp/alpha.crt",
                "--alpha-tls-private-key-file", "/tmp/alpha.key",
                "--alpha-jwt-private-key-file", "/tmp/alpha-jwt.key",
                "--alpha-jwt-key-id", "alpha-key",
            ]
        )
        self.assertEqual(parsed.handoff_packet, Path("/tmp/packet.json"))
        self.assertEqual(parsed.runtime_dir, Path("/tmp/runtime"))

    def test_cursor_directory_is_new_private_and_removed(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-receipt-") as directory:
            path = Path(directory) / "cursor-state"
            with _cursor_directory(str(path)) as state:
                self.assertTrue(state.is_dir())
                state.joinpath("cursor.json").write_text("sensitive", encoding="utf-8")
            self.assertFalse(path.exists())
            path.mkdir()
            with self.assertRaisesRegex(ValueError, "must not already exist"):
                with _cursor_directory(str(path)):
                    pass

    def test_harness_rejects_an_expired_or_tampered_packet_before_sdk_io(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-receipt-") as directory:
            root = Path(directory)
            packet = prepare_shared_primary_packet(
                output_dir=root / "packet",
                host_runtime_dir=root / "packet" / "runtime",
                rust_image_digest="sha256:" + "b" * 64,
                python_image_digest="sha256:" + "c" * 64,
                source_commit="0123456789abcdef",
                actor="BobbyAxerol",
                change_ticket="QDL-PHASE103-HARNESS-TEST",
                observation_seconds=300,
                issued_at_ns=time.time_ns(),
            )
            packet_path = root / "packet" / "shared-primary-handoff-packet.json"
            validated = _validated_packet(packet_path, root / "packet" / "runtime")
            self.assertEqual(validated["packet_sha256"], packet["packet_sha256"])
            packet_path.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "packet"):
                _validated_packet(packet_path, root / "packet" / "runtime")

    def test_runbook_preserves_the_sealed_host_runtime_path_inside_probe(self):
        runbook = (
            ROOT / "docs/runbooks/phase103-shared-rust-primary-handoff.md"
        ).read_text(encoding="utf-8")
        self.assertIn('-v "$QDL_PACKET_DIR:$QDL_PACKET_DIR:ro"', runbook)
        self.assertIn(
            '--handoff-packet "$QDL_PACKET_DIR/shared-primary-handoff-packet.json"',
            runbook,
        )
        self.assertIn('--runtime-dir "$QDL_RUNTIME_DIR"', runbook)
        self.assertNotIn("--handoff-packet /packet/", runbook)

    def test_packet_contract_identity_matches_runtime_deployment(self):
        self.assertEqual(SHARED_REALTIME_CORE_GROUP_ID, DEPLOYMENT_CORE_GROUP_ID)
        self.assertEqual(SHARED_REALTIME_CORE_ID_PREFIX, DEPLOYMENT_CORE_ID_PREFIX)

    def test_packet_validation_has_one_contract_source_of_truth(self):
        self.assertIs(generator_bundle_validator, contract_bundle_validator)
        self.assertIs(broker_bundle_validator, contract_bundle_validator)

    def test_runbook_uses_immutable_image_for_packet_preflight(self):
        runbook = (
            ROOT / "docs/runbooks/phase103-shared-rust-primary-handoff.md"
        ).read_text(encoding="utf-8")
        self.assertIn('export QDL_STABLE_PYTHON_IMAGE_REF=', runbook)
        self.assertIn('docker run --rm --read-only --network none', runbook)
        self.assertIn('-v "$QDL_PACKET_DIR:$QDL_PACKET_DIR"', runbook)
        for script in (
            "phase103_prepare_shared_primary_packet.py",
            "phase103_validate_shared_primary_packet.py",
        ):
            self.assertIn(f"python -B scripts/{script}", runbook)
            self.assertNotIn(f"python3 -B scripts/{script}", runbook)
        self.assertIn(
            "python3 -B scripts/phase103_apply_shared_primary_broker_scope.py",
            runbook,
        )
        self.assertIn(
            "  python -B scripts/phase103_consumer_receipt_acceptance.py",
            runbook,
        )
        self.assertIn("trading_system_handoff.route_lock", runbook)
        self.assertIn("QDL_TRADING_SYSTEM_SOURCE_ROOT", runbook)
        self.assertIn("docker run --rm --entrypoint sha256sum", runbook)


if __name__ == "__main__":
    unittest.main()
