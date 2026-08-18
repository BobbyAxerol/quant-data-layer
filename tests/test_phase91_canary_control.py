from __future__ import annotations

import copy
import json
import time
import unittest
from pathlib import Path

from qdl.canary.phase9 import (
    CanaryAuthorizationMode,
    CanaryGuardrailEngine,
    CanaryGuardrailPolicy,
    CanaryObservation,
    ProductionCanaryAuthorizer,
)
from qdl.certification.prerequisites import CandidateSlice


ROOT = Path(__file__).resolve().parents[1]


class Phase91CanaryAuthorizationTest(unittest.TestCase):
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

    def test_current_external_no_go_cannot_authorize_production(self):
        result = ProductionCanaryAuthorizer().authorize(
            candidate=self.candidate,
            decision=self.no_go,
            expected_bundle_id=self.no_go["bundle_id"],
            now_ns=self.no_go["issued_at_ns"] + 1,
        )
        self.assertFalse(result.allowed)
        self.assertFalse(result.production_authorized)
        self.assertEqual(result.reason, "PREREQUISITE_DECISION_NOT_GO")

    def test_rehearsal_is_explicitly_non_production(self):
        result = ProductionCanaryAuthorizer.authorize_isolated_rehearsal(
            candidate=self.candidate
        )
        self.assertTrue(result.allowed)
        self.assertFalse(result.production_authorized)
        self.assertEqual(result.mode, CanaryAuthorizationMode.ISOLATED_REHEARSAL)
        self.assertEqual(result.reason, "ISOLATED_REHEARSAL_ONLY")

    def test_exact_fresh_go_authorizes_without_mutating(self):
        now_ns = time.time_ns()
        decision = self._go(now_ns)
        snapshot = copy.deepcopy(decision)
        result = ProductionCanaryAuthorizer().authorize(
            candidate=self.candidate,
            decision=decision,
            expected_bundle_id=decision["bundle_id"],
            now_ns=now_ns,
        )
        self.assertTrue(result.allowed)
        self.assertTrue(result.production_authorized)
        self.assertEqual(result.reason, "AUTHORIZED")
        self.assertEqual(decision, snapshot)

    def test_identity_freshness_gate_and_v1_preconditions_fail_closed(self):
        now_ns = time.time_ns()
        cases = (
            ({"candidate_digest": "0" * 64}, "CANDIDATE_DIGEST_MISMATCH"),
            ({"slice_id": "production/other"}, "SLICE_MISMATCH"),
            ({"bundle_id": "11111111-1111-4111-8111-111111111111"}, "PREREQUISITE_BUNDLE_MISMATCH"),
            ({"issued_at_ns": now_ns - 901_000_000_000}, "DECISION_EXPIRED"),
            ({"authority_state": "RUST_CANARY"}, "AUTHORITY_PRECONDITION_INVALID"),
            ({"v1_unchanged": False}, "V1_PRECONDITION_INVALID"),
            ({"production_mutations": 1}, "PRODUCTION_MUTATION_DETECTED"),
        )
        for update, expected in cases:
            with self.subTest(expected=expected):
                decision = self._go(now_ns)
                expected_bundle = decision["bundle_id"]
                decision.update(update)
                result = ProductionCanaryAuthorizer().authorize(
                    candidate=self.candidate,
                    decision=decision,
                    expected_bundle_id=expected_bundle,
                    now_ns=now_ns,
                )
                self.assertFalse(result.allowed)
                self.assertEqual(result.reason, expected)

    def test_incomplete_duplicate_or_failed_gates_are_rejected(self):
        now_ns = time.time_ns()
        decision = self._go(now_ns)
        expected_bundle = decision["bundle_id"]
        decision["gates"].pop()
        decision["passed"] -= 1
        result = ProductionCanaryAuthorizer().authorize(
            candidate=self.candidate, decision=decision,
            expected_bundle_id=expected_bundle, now_ns=now_ns,
        )
        self.assertEqual(result.reason, "GATE_RESULTS_INVALID")

        decision = self._go(now_ns)
        decision["gates"][0]["passed"] = False
        decision["gates"][0]["reason"] = "FAILED"
        result = ProductionCanaryAuthorizer().authorize(
            candidate=self.candidate, decision=decision,
            expected_bundle_id=expected_bundle, now_ns=now_ns,
        )
        self.assertEqual(result.reason, "GATE_NOT_PASSED")


class Phase91GuardrailTest(unittest.TestCase):
    def setUp(self):
        self.policy = CanaryGuardrailPolicy(
            max_lag_ms=500,
            max_freshness_ms=1_000,
            max_cpu_percent=80.0,
            max_rss_mb=512.0,
            max_queue_depth=1_000,
            hold_down_seconds=30,
        )

    def test_clean_observation_passes(self):
        result = CanaryGuardrailEngine(self.policy).evaluate(
            CanaryObservation(observed_at_ns=1)
        )
        self.assertTrue(result.allowed)
        self.assertEqual(result.reason, "PASS")

    def test_each_correctness_and_capacity_guardrail_blocks(self):
        cases = (
            ({"semantic_mismatches": 1}, "SEMANTIC_MISMATCH"),
            ({"open_gaps": 1}, "OPEN_GAP"),
            ({"duplicate_external_writes": 1}, "DUPLICATE_EXTERNAL_WRITE"),
            ({"stale_writer_attempts": 1}, "STALE_WRITER_ATTEMPT"),
            ({"authority_ambiguities": 1}, "AUTHORITY_AMBIGUITY"),
            ({"durable_ack_failures": 1}, "DURABLE_ACK_FAILURE"),
            ({"lag_ms": 501}, "LAG_THRESHOLD_EXCEEDED"),
            ({"freshness_ms": 1_001}, "FRESHNESS_THRESHOLD_EXCEEDED"),
            ({"cpu_percent": 80.1}, "CPU_THRESHOLD_EXCEEDED"),
            ({"rss_mb": 512.1}, "RSS_THRESHOLD_EXCEEDED"),
            ({"queue_depth": 1_001}, "QUEUE_THRESHOLD_EXCEEDED"),
        )
        for update, expected in cases:
            with self.subTest(expected=expected):
                result = CanaryGuardrailEngine(self.policy).evaluate(
                    CanaryObservation(observed_at_ns=1, **update)
                )
                self.assertFalse(result.allowed)
                self.assertEqual(result.reason, expected)

    def test_first_failure_is_sticky_and_reentry_is_explicit_after_hold(self):
        engine = CanaryGuardrailEngine(self.policy)
        blocked = engine.evaluate(
            CanaryObservation(observed_at_ns=1_000, semantic_mismatches=1)
        )
        self.assertEqual(blocked.reason, "SEMANTIC_MISMATCH")
        self.assertEqual(blocked.hold_until_ns, 30_000_001_000)
        self.assertEqual(
            engine.evaluate(CanaryObservation(observed_at_ns=2_000)).reason,
            "EXPLICIT_RESET_REQUIRED",
        )
        self.assertEqual(
            engine.reset_after_hold(CanaryObservation(observed_at_ns=30_000_000_999)).reason,
            "HOLD_DOWN_ACTIVE",
        )
        reset = engine.reset_after_hold(
            CanaryObservation(observed_at_ns=30_000_001_000)
        )
        self.assertTrue(reset.allowed)
        self.assertEqual(reset.reason, "RESET_CONFIRMED")

    def test_invalid_thresholds_and_observations_are_rejected(self):
        with self.assertRaises(ValueError):
            CanaryGuardrailPolicy(0, 1, 1, 1, 1, 1)
        with self.assertRaises(ValueError):
            CanaryObservation(observed_at_ns=0)
        with self.assertRaises(ValueError):
            CanaryObservation(observed_at_ns=1, open_gaps=-1)


if __name__ == "__main__":
    unittest.main()
