from __future__ import annotations

import copy
import json
import time
import unittest
import uuid
from pathlib import Path

from qdl.canary.phase92 import (
    AcceptedHandoff,
    HandoffDirection,
    PrimaryAuthorizationMode,
    ProductionPrimaryAuthorizer,
    TerminalOwnerCheckpoint,
)
from qdl.certification.prerequisites import CandidateSlice


ROOT = Path(__file__).resolve().parents[1]


class Phase92PrimaryAuthorizationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.candidate = CandidateSlice.load(ROOT / "config/phase9/candidate-slice.yaml")
        cls.no_go = json.loads(
            (ROOT / "upgrade/evidence/phase90c-production-prerequisites.json").read_text()
        )

    def _go(self, now_ns: int) -> dict:
        payload = copy.deepcopy(self.no_go)
        payload.update({
            "decision": "GO",
            "passed": len(payload["gates"]),
            "blocked": 0,
            "issued_at_ns": now_ns - 1_000_000,
            "authority_state": "RUST_SHADOW",
            "v1_unchanged": True,
            "production_mutations": 0,
        })
        for gate in payload["gates"]:
            gate.update({"passed": True, "reason": "PASS"})
        return payload

    def _canary(self, now_ns: int) -> dict:
        return {
            "schema": "qdl.phase91.rust-canary-certification.v1",
            "status": "PRODUCTION_CANARY_HOLD_PASSED",
            "production_authorized": True,
            "slice_id": self.candidate.payload["slice_id"],
            "candidate_digest": self.candidate.digest,
            "prerequisite_bundle_id": self.no_go["bundle_id"],
            "python_v1_public_authority_unchanged": True,
            "production_mutations": 0,
            "parity": {"semantic_mismatches": 0},
            "broker": {
                "final_authority": "RUST_CANARY",
                "public_writes": 0,
                "legacy_writes": 0,
            },
            "hold_completed_at_ns": now_ns - 1,
            "rollback_manifest_digest": "1" * 64,
        }

    def _approval(self, now_ns: int) -> dict:
        return {
            "schema": "qdl.primary-slice-approval.v1",
            "decision": "APPROVE",
            "slice_id": self.candidate.payload["slice_id"],
            "candidate_digest": self.candidate.digest,
            "prerequisite_bundle_id": self.no_go["bundle_id"],
            "operator": "phase92-test-operator",
            "change_ticket": "QDL-92",
            "max_partitions": 1,
            "allow_disable_exact_python_slice": True,
            "expires_at_ns": now_ns + 60_000_000_000,
        }

    def test_current_no_go_cannot_authorize_primary(self):
        now_ns = self.no_go["issued_at_ns"] + 1
        result = ProductionPrimaryAuthorizer().authorize(
            candidate=self.candidate,
            prerequisite_decision=self.no_go,
            canary_evidence={},
            approval={},
            expected_bundle_id=self.no_go["bundle_id"],
            now_ns=now_ns,
        )
        self.assertFalse(result.allowed)
        self.assertEqual(result.reason, "PREREQUISITE_DECISION_NOT_GO")

    def test_rehearsal_is_explicitly_non_production(self):
        result = ProductionPrimaryAuthorizer.authorize_isolated_rehearsal(
            candidate=self.candidate
        )
        self.assertTrue(result.allowed)
        self.assertFalse(result.production_authorized)
        self.assertEqual(result.mode, PrimaryAuthorizationMode.ISOLATED_REHEARSAL)

    def test_exact_go_canary_hold_and_approval_authorize(self):
        now_ns = time.time_ns()
        result = ProductionPrimaryAuthorizer().authorize(
            candidate=self.candidate,
            prerequisite_decision=self._go(now_ns),
            canary_evidence=self._canary(now_ns),
            approval=self._approval(now_ns),
            expected_bundle_id=self.no_go["bundle_id"],
            now_ns=now_ns,
        )
        self.assertTrue(result.allowed)
        self.assertTrue(result.production_authorized)
        self.assertEqual(result.reason, "AUTHORIZED")

    def test_canary_and_approval_fail_closed(self):
        now_ns = time.time_ns()
        cases = (
            ("canary", {"status": "COMPLETE_IMPLEMENTATION / CANARY_NOT_AUTHORIZED"}, "CANARY_HOLD_NOT_PASSED"),
            ("canary", {"production_authorized": False}, "CANARY_NOT_PRODUCTION_AUTHORIZED"),
            ("canary", {"hold_completed_at_ns": now_ns + 1}, "CANARY_HOLD_TIME_INVALID"),
            ("canary", {"rollback_manifest_digest": "bad"}, "CANARY_ROLLBACK_MANIFEST_INVALID"),
            ("approval", {"max_partitions": 2}, "PRIMARY_BLAST_RADIUS_INVALID"),
            ("approval", {"allow_disable_exact_python_slice": False}, "PRIMARY_PYTHON_HANDOFF_NOT_APPROVED"),
            ("approval", {"expires_at_ns": now_ns}, "PRIMARY_APPROVAL_EXPIRED"),
        )
        for target, update, expected in cases:
            with self.subTest(expected=expected):
                canary = self._canary(now_ns)
                approval = self._approval(now_ns)
                (canary if target == "canary" else approval).update(update)
                result = ProductionPrimaryAuthorizer().authorize(
                    candidate=self.candidate,
                    prerequisite_decision=self._go(now_ns),
                    canary_evidence=canary,
                    approval=approval,
                    expected_bundle_id=self.no_go["bundle_id"],
                    now_ns=now_ns,
                )
                self.assertFalse(result.allowed)
                self.assertEqual(result.reason, expected)


class Phase92HandoffContractTest(unittest.TestCase):
    def checkpoint(self) -> TerminalOwnerCheckpoint:
        return TerminalOwnerCheckpoint(
            schema="qdl.terminal-owner-checkpoint.v1",
            checkpoint_id=str(uuid.uuid4()),
            slice_id="production/binance/usdm/perpetual/trade/plan-1/btcusdt",
            owner_id="python-primary",
            authority_revision=7,
            lease_epoch=11,
            partition_plan_epoch=1,
            source_session_id="session-1",
            connection_generation=2,
            terminal_watermark=100,
            terminal_event_id="event-100",
            terminal_payload_sha256="1" * 64,
            candidate_digest="2" * 64,
            committed_at_ns=1,
        )

    def handoff(self, checkpoint: TerminalOwnerCheckpoint) -> AcceptedHandoff:
        return AcceptedHandoff(
            schema="qdl.accepted-authority-handoff.v1",
            handoff_id=str(uuid.uuid4()),
            direction=HandoffDirection.PYTHON_TO_RUST,
            checkpoint_digest=checkpoint.digest,
            slice_id=checkpoint.slice_id,
            old_owner_id=checkpoint.owner_id,
            new_owner_id="rust-primary",
            expected_state="RUST_CANARY",
            new_state="RUST_PRIMARY",
            expected_authority_revision=checkpoint.authority_revision,
            new_authority_revision=checkpoint.authority_revision + 1,
            expected_lease_epoch=checkpoint.lease_epoch,
            new_lease_epoch=checkpoint.lease_epoch + 1,
            partition_plan_epoch=checkpoint.partition_plan_epoch,
            terminal_watermark=checkpoint.terminal_watermark,
            first_new_watermark=checkpoint.terminal_watermark + 1,
            overlap_start_watermark=90,
            overlap_end_watermark=checkpoint.terminal_watermark,
            old_event_count=11,
            new_event_count=11,
            semantic_mismatches=0,
            open_gaps=0,
            candidate_digest=checkpoint.candidate_digest,
            prerequisite_bundle_id=str(uuid.uuid4()),
            approved_by="phase92-test",
            approved_at_ns=1,
            expires_at_ns=2,
        )

    def test_checkpoint_and_handoff_are_deterministic(self):
        checkpoint = self.checkpoint()
        self.assertEqual(len(checkpoint.digest), 64)
        handoff = self.handoff(checkpoint)
        self.assertEqual(len(handoff.digest), 64)
        self.assertEqual(handoff.first_new_watermark, 101)

    def test_off_by_one_dirty_parity_and_stale_epoch_are_rejected(self):
        checkpoint = self.checkpoint()
        base = self.handoff(checkpoint)
        payload = {
            field: getattr(base, field)
            for field in base.__dataclass_fields__
        }
        for update in (
            {"first_new_watermark": 102},
            {"semantic_mismatches": 1},
            {"open_gaps": 1},
            {"new_event_count": 10},
            {"new_lease_epoch": base.expected_lease_epoch},
            {"new_authority_revision": base.expected_authority_revision + 2},
        ):
            with self.subTest(update=update):
                candidate = dict(payload)
                candidate.update(update)
                with self.assertRaises(ValueError):
                    AcceptedHandoff(**candidate)


if __name__ == "__main__":
    unittest.main()
