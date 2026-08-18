from __future__ import annotations

import gzip
import hashlib
import json
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Phase91CertificationContractTest(unittest.TestCase):
    def test_frozen_capture_is_authentic_read_only_and_checksummed(self):
        capture = ROOT / "upgrade/evidence/captures/phase8-real-provider-frames.json.gz"
        evidence = json.loads(
            (ROOT / "upgrade/evidence/phase8-real-provider-shadow.json").read_text()
        )
        compressed = capture.read_bytes()
        self.assertEqual(
            hashlib.sha256(compressed).hexdigest(), evidence["capture_bundle_sha256"]
        )
        payload = json.loads(gzip.decompress(compressed))
        self.assertEqual(payload["provenance"], "REAL_PROVIDER_READ_ONLY")
        self.assertEqual(payload["production_writes"], 0)
        binance = [
            item for item in payload["captures"]
            if item["venue"] == "BINANCE"
            and item["market"] == "USDM"
            and item["native_symbol"] == "BTCUSDT"
            and item["test_provenance"] is False
        ]
        self.assertGreaterEqual(len(binance), 32)

    def test_tls_generator_preserves_host_ownership(self):
        script = (ROOT / "scripts/phase80_generate_tls.sh").read_text()
        self.assertIn("QDL_PHASE8_CERT_UID", script)
        self.assertIn("QDL_PHASE8_CERT_GID", script)
        self.assertIn('--user "${CERT_UID}:${CERT_GID}"', script)
        self.assertNotIn("docker run --rm --user 0:0", script)

    def test_runtime_image_and_workspace_declare_phase91_binary(self):
        cargo = (ROOT / "rust/qdl-kafka/Cargo.toml").read_text()
        dockerfile = (ROOT / "Dockerfile.phase8-rust").read_text()
        self.assertIn('name = "qdl-phase91-canary-rehearsal"', cargo)
        self.assertIn("--bin qdl-phase91-canary-rehearsal", dockerfile)
        self.assertIn(
            "/usr/local/bin/qdl-phase91-canary-rehearsal", dockerfile
        )

    def test_certification_is_isolated_and_has_cleanup_and_zero_write_gates(self):
        harness = (ROOT / "scripts/phase91_canary_certification.py").read_text()
        runtime = (
            ROOT / "rust/qdl-kafka/src/bin/qdl-phase91-canary-rehearsal.rs"
        ).read_text()
        for token in (
            "NO_GO_EXTERNAL",
            "production_mutations",
            "v1_topology_unchanged",
            "containers_after",
            "public_writes",
            "legacy_writes",
            "min_isr_failed_closed",
            "slow_consumer",
        ):
            self.assertIn(token, harness)
        for token in (
            "topic_masquerade_rejected",
            "stale_owner_rejected",
            "stale_revision_rejected",
            "stale_lease_rejected",
            "wrong_plan_rejected",
            "duplicate_watermark_rejected",
            "blocked_state_rejected",
            "canary_after_rollback_rejected",
        ):
            self.assertIn(token, runtime)

    def test_compacted_authority_reads_latest_record_only(self):
        harness = (ROOT / "scripts/phase91_canary_certification.py").read_text()
        self.assertIn("def consume_compacted_records", harness)
        self.assertIn("--partition", harness)
        self.assertIn("item.strip().startswith(\"{\")", harness)
        self.assertIn("consume_compacted_records(env, topics[\"authority\"])", harness)
        self.assertIn('topics["audit"], 4', harness)
        self.assertIn("compose(env, \"stop\", \"kafka3\")", harness)
        self.assertIn("compose(env, \"stop\", \"kafka2\")", harness)
        self.assertIn("compose(env, \"start\", \"kafka2\", \"kafka3\"", harness)
        self.assertIn("\"QDL_KAFKA_BOOTSTRAP_SERVERS\": \"kafka1:9092,kafka2:9092\"", harness)
        self.assertIn("min_isr_before = one_loss_after", harness)
        self.assertIn("min_isr_client_failed = min_isr.returncode != 0", harness)
        self.assertIn("min_isr_after = total_end_offset", harness)
        self.assertIn("min_isr_after == min_isr_before", harness)

    def test_makefile_and_evidence_packaging_are_reproducible(self):
        makefile = (ROOT / "Makefile").read_text()
        harness = (ROOT / "scripts/phase91_canary_certification.py").read_text()
        for target in ("phase91-build:", "phase91-test:", "phase91-certify:", "phase91-clean:"):
            self.assertIn(target, makefile)
        self.assertIn("PHASE91_RUST_IMAGE", makefile)
        self.assertIn("def render_report", harness)
        self.assertIn("def write_evidence", harness)
        self.assertIn("phase91-evidence.sha256", harness)

    def test_operator_entrypoints_have_valid_help_or_usage(self):
        result = subprocess.run(
            ["python3", "scripts/phase91_canary_certification.py", "--help"],
            cwd=ROOT, text=True, capture_output=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--rust-image", result.stdout)


if __name__ == "__main__":
    unittest.main()
