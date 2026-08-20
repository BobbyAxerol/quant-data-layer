from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Phase93CertificationContractTest(unittest.TestCase):
    def test_certification_is_fail_closed_and_never_claims_production(self):
        source = (
            ROOT / "scripts/phase93_hold_close_certification.py"
        ).read_text()
        for token in (
            "NO_GO_EXTERNAL",
            "PREREQUISITE_DECISION_NOT_GO",
            "TEST_CONTROL_PLANE_FIXTURE",
            "accelerated_time_is_production_evidence",
            "production_hold_started",
            "production_rollback_window_closed",
            "production_expansions_authorized",
            "production_mutations",
            "v1_topology",
            "v1_health",
        ):
            self.assertIn(token, source)
        self.assertNotIn("qdl_transition_authority", source)
        self.assertNotIn("qdl_close_authority_window", source)

    def test_frozen_evidence_reports_only_control_plane_completion(self):
        evidence = json.loads(
            (
                ROOT
                / "upgrade/evidence/phase93-hold-close-expand-certification.json"
            ).read_text()
        )
        self.assertEqual(
            evidence["status"],
            "COMPLETE_CONTROL_PLANE / PRODUCTION_HOLD_NOT_STARTED",
        )
        self.assertFalse(evidence["production_authorized"])
        self.assertFalse(evidence["production_hold_started"])
        self.assertFalse(evidence["production_rollback_window_closed"])
        self.assertEqual(evidence["production_expansions_authorized"], 0)
        self.assertEqual(evidence["production_mutations"], 0)
        self.assertEqual(
            evidence["control_plane_fixture"]["provenance"],
            "TEST_CONTROL_PLANE_FIXTURE",
        )
        self.assertFalse(
            evidence["control_plane_fixture"][
                "accelerated_time_is_production_evidence"
            ]
        )

    def test_runbook_and_make_targets_are_reproducible(self):
        runbook = (
            ROOT / "docs/runbooks/phase93-hold-close-expand.md"
        ).read_text()
        makefile = (ROOT / "Makefile").read_text()
        for token in (
            "Starting A Real Hold",
            "Closing The Rollback Window",
            "Expansion",
            "Python Runtime Decommission",
            "Incident And Rollback",
            "TEST_CONTROL_PLANE_FIXTURE",
        ):
            self.assertIn(token, runbook)
        for target in (
            "phase93-test:",
            "phase93-migration:",
            "phase93-certify:",
            "phase93-clean:",
        ):
            self.assertIn(target, makefile)
        result = subprocess.run(
            ["python3", "scripts/phase93_hold_close_certification.py", "--help"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_control_plane_import_does_not_eagerly_require_alpha_sdk(self):
        package = (ROOT / "qdl/canary/__init__.py").read_text()
        self.assertNotIn("from qdl.canary.consumer import", package)
        self.assertIn("def __getattr__", package)
        self.assertIn("_CONSUMER_EXPORTS", package)


if __name__ == "__main__":
    unittest.main()
