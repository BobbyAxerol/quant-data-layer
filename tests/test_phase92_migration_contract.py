from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Phase92MigrationContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = (
            ROOT / "migrations/postgres/0007_phase92_terminal_handoff.sql"
        ).read_text()

    def test_migration_is_additive_and_evidence_is_immutable(self):
        for token in (
            "qdl_terminal_owner_checkpoints",
            "qdl_authority_handoffs",
            "qdl_transition_authority_v2",
            "qdl_authority_primary_handoff_guard",
            "Phase 9.2 checkpoint/handoff evidence is append-only",
        ):
            self.assertIn(token, self.sql)
        self.assertNotIn("DROP TABLE", self.sql.upper())
        self.assertNotIn("TRUNCATE", self.sql.upper())

    def test_terminal_boundary_and_clean_reconciliation_are_enforced(self):
        for token in (
            "first_new_watermark = terminal_watermark + 1",
            "semantic_mismatches = 0",
            "open_gaps = 0",
            "old_event_count = new_event_count",
            "new_authority_revision = expected_authority_revision + 1",
            "new_lease_epoch > expected_lease_epoch",
        ):
            self.assertIn(token, self.sql)

    def test_direct_primary_and_rollback_bypass_are_trigger_fenced(self):
        self.assertIn("NEW.state = 'RUST_PRIMARY'", self.sql)
        self.assertIn("NEW.state = 'PYTHON_PRIMARY'", self.sql)
        self.assertIn("matching_handoffs <> 1", self.sql)
        self.assertIn("accepted exact Phase 9.2 handoff is required", self.sql)

    def test_v2_transition_rechecks_exact_cas_and_expiry(self):
        for token in (
            "handoff.expected_state <> p_expected_state",
            "handoff.expected_authority_revision <> p_expected_revision",
            "handoff.expected_lease_epoch <> p_expected_lease_epoch",
            "handoff.partition_plan_epoch <> p_expected_partition_plan_epoch",
            "handoff.expires_at <= clock_timestamp()",
            "Phase 9.2 prerequisite bundle mismatch",
        ):
            self.assertIn(token, self.sql)


if __name__ == "__main__":
    unittest.main()
