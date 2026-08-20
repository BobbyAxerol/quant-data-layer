from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Phase90CMigrationContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = (
            ROOT / "migrations/postgres/0006_phase9_authority_prerequisites.sql"
        ).read_text()

    def test_additive_authority_and_evidence_schema_exists(self):
        for name in (
            "qdl_production_prerequisite_bundles",
            "qdl_authority_slices",
            "qdl_authority_transition_audit",
            "qdl_transition_authority",
        ):
            self.assertIn(name, self.sql)
        self.assertNotIn("DROP TABLE", self.sql.upper())
        self.assertNotIn("TRUNCATE", self.sql.upper())

    def test_cas_and_go_bundle_are_database_enforced(self):
        self.assertIn("authority compare-and-swap precondition failed", self.sql)
        self.assertIn("p_expected_partition_plan_epoch", self.sql)
        self.assertIn("p_new_state IN ('RUST_CANARY', 'RUST_PRIMARY')", self.sql)
        self.assertIn("bundle_row.decision <> 'GO'", self.sql)
        self.assertIn("bundle_row.candidate_digest <> current_row.candidate_digest", self.sql)
        self.assertIn("bundle_row.expires_at <= clock_timestamp()", self.sql)
        self.assertIn("bundle_row.expires_at < p_hold_until", self.sql)
        self.assertIn("canary/primary requires a non-negative terminal watermark", self.sql)
        self.assertIn("canary/primary requires a future approval hold window", self.sql)
        self.assertIn("valid only for canary/primary", self.sql)
        self.assertIn("THEN p_prerequisite_bundle_id ELSE NULL", self.sql)

    def test_authority_record_binds_release_provenance_and_hold_window(self):
        for field in (
            "signature_identity", "normalizer_version", "adapter_version",
            "config_revision", "instrument_catalog_revision",
            "source_policy_revision", "hold_until",
        ):
            self.assertIn(field, self.sql)
        self.assertIn("p_hold_until", self.sql)

    def test_audit_is_append_only_and_new_owner_needs_new_epoch(self):
        self.assertIn("qdl_authority_audit_immutable", self.sql)
        self.assertIn("qdl_authority_transition_audit is append-only", self.sql)
        self.assertIn("new owner requires a strictly newer lease epoch", self.sql)

    def test_migration_smoke_keeps_heredoc_stdin_open(self):
        harness = (ROOT / "scripts/phase90c_migration_smoke.sh").read_text()
        self.assertIn('docker exec -i "${CONTAINER}" psql', harness)


if __name__ == "__main__":
    unittest.main()
