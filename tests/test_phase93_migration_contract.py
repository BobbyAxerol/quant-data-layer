from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Phase93MigrationContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = (
            ROOT / "migrations/postgres/0008_phase93_hold_close_expand.sql"
        ).read_text()

    def test_migration_is_additive_and_all_evidence_is_append_only(self):
        for token in (
            "qdl_primary_holds",
            "qdl_primary_hold_observations",
            "qdl_primary_hold_decisions",
            "qdl_consumer_registry_snapshots",
            "qdl_authority_registry_snapshots",
            "qdl_rollback_rehearsals",
            "qdl_closure_approvals",
            "qdl_authority_closures",
            "qdl_expansion_candidates",
            "qdl_runtime_decommission_decisions",
            "Phase 9.3 hold/closure/expansion evidence is append-only",
        ):
            self.assertIn(token, self.sql)
        self.assertNotIn("DROP TABLE", self.sql.upper())
        self.assertNotIn("TRUNCATE", self.sql.upper())

    def test_hold_observation_and_passing_decision_are_evidence_backed(self):
        for token in (
            "hold observation identity mismatch",
            "hold observation sequence is not contiguous",
            "hold observation time is not monotonic",
            "hold observation gap exceeds policy",
            "hold watermark regressed",
            "passing hold decision is not supported by evidence",
            "semantic_mismatches <> 0",
            "consumer_checkpoint_regressions <> 0",
            "checkpoint_watermark < last_watermark",
        ):
            self.assertIn(token, self.sql)

    def test_closure_locks_authority_and_frozen_registries_without_update(self):
        start = self.sql.index("CREATE OR REPLACE FUNCTION qdl_close_authority_window")
        end = self.sql.index("CREATE TABLE IF NOT EXISTS qdl_expansion_candidates", start)
        function = self.sql[start:end]
        for token in (
            "FOR UPDATE",
            "current_row.state <> 'RUST_PRIMARY'",
            "authority closure CAS mismatch",
            "consumer registry snapshot is invalid",
            "authority registry snapshot is invalid",
            "rollback rehearsal is invalid",
            "closure approval is invalid or expired",
            "INSERT INTO qdl_authority_closures",
        ):
            self.assertIn(token, function)
        self.assertNotIn("UPDATE qdl_authority_slices", function)
        self.assertNotIn("DELETE FROM", function)

    def test_expansions_are_independent_and_cannot_write(self):
        for token in (
            "INDEPENDENT_CERTIFICATION_REQUIRED",
            "NOT transitive_evidence_allowed",
            "NOT public_write_allowed",
            "NOT legacy_write_allowed",
            "expansion requires independent capability gates",
            "instrument expansion requires a new partition epoch",
            "snapshot_delta_sequence",
            "final_revision_lineage",
            "provider_semantics",
        ):
            self.assertIn(token, self.sql)

    def test_decommission_allowance_requires_zero_dependency(self):
        for token in (
            "owned_slice_count = 0",
            "rollback_reference_count = 0",
            "consumer_dependency_count = 0",
            "all_replacement_windows_closed",
            "repository_cleanup_approved",
            "shared_knowledge_retained",
        ):
            self.assertIn(token, self.sql)


if __name__ == "__main__":
    unittest.main()
