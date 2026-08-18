from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Phase92CertificationContractTest(unittest.TestCase):
    def test_runtime_image_declares_primary_rehearsal_binary(self):
        cargo = (ROOT / "rust/qdl-kafka/Cargo.toml").read_text()
        dockerfile = (ROOT / "Dockerfile.phase8-rust").read_text()
        self.assertIn('name = "qdl-phase92-primary-rehearsal"', cargo)
        self.assertIn("--bin qdl-phase92-primary-rehearsal", dockerfile)
        self.assertIn(
            "/usr/local/bin/qdl-phase92-primary-rehearsal", dockerfile
        )

    def test_harness_is_isolated_and_checks_handoff_recovery(self):
        harness = (ROOT / "scripts/phase92_primary_certification.py").read_text()
        runtime = (
            ROOT / "rust/qdl-kafka/src/bin/qdl-phase92-primary-rehearsal.rs"
        ).read_text()
        for token in (
            "NO_GO_EXTERNAL",
            "production_mutations",
            "v1_topology_unchanged",
            "production_public_writes",
            "production_legacy_writes",
            "projection_parity",
            "boundary_gap_free",
            "one_replica_loss_acked",
            "min_isr_failed_closed",
            "process_restart_recovery",
            "RECOVERY_VERIFY",
            "QDL_RECOVERY_LAST_WATERMARK",
        ):
            self.assertIn(token, harness)
        for token in (
            "direct_primary_without_handoff_rejected",
            "terminal_watermark_rejected",
            "gap_watermark_rejected",
            "stale_owner_rejected",
            "stale_revision_rejected",
            "stale_lease_rejected",
            "wrong_plan_rejected",
            "duplicate_after_ack_rejected",
            "rust_after_rollback_rejected",
            "restart_pre_restore_failed_closed",
            "each_target_pre_restore_failed_closed",
            "durable_target_watermarks_restored",
            "duplicate_after_restore_rejected",
            "resumed_at_exact_next_watermark",
            "restore_committed_watermark",
        ):
            self.assertIn(token, runtime)

    def test_rehearsal_topics_cannot_masquerade_as_production(self):
        runtime = (
            ROOT / "rust/qdl-kafka/src/bin/qdl-phase92-primary-rehearsal.rs"
        ).read_text()
        self.assertIn('!topic.contains(".phase92.")', runtime)
        self.assertIn("QDL_PRODUCTION_PUBLIC_TOPIC", runtime)
        self.assertIn("QDL_PRODUCTION_LEGACY_TOPIC", runtime)

    def test_makefile_and_operator_entrypoints_are_reproducible(self):
        makefile = (ROOT / "Makefile").read_text()
        for target in (
            "phase92-build:",
            "phase92-test:",
            "phase92-migration:",
            "phase92-certify:",
            "phase92-clean:",
        ):
            self.assertIn(target, makefile)
        result = subprocess.run(
            ["python3", "scripts/phase92_primary_certification.py", "--help"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--rust-image", result.stdout)


if __name__ == "__main__":
    unittest.main()
