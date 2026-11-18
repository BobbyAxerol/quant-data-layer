"""DL v2.4.0 S9: the shared100 acceptance profile, its scorer and the capacity candidate (offline, TEST_ONLY)."""

from __future__ import annotations

import copy
import hashlib
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from qdl.certification.phase3_consumer_load import evaluate_shared100_gates, load_shared100_profile
from scripts.p184_capacity100 import build_capacity100_candidate
from tests.test_p184_capacity100 import inputs

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "config/v2/p184-shared100-profile.json"
A6 = ROOT / "config/v2/p184-stage100-a6-shared100-preregistered.json"


def _passing_receipt() -> dict:
    return {
        "restart_storm": {
            "session_start_offsets_s": [i * 0.09 for i in range(100)],
            "expected_warmups": 100,
            "warmups": [{"session": f"s{i:03d}", "status": "OK"} for i in range(100)],
            "refusals": 0,
            "ts_lane_ms": [120.0] * 40,
        },
        "noisy_neighbour": {
            "noisy_offered_rps": 50.0, "noisy_rate_limited": 1200,
            "other_sessions": [{"session": f"s{i:03d}", "refusals": 0} for i in range(99)],
        },
        "g14": {"IDLE": {"dl_vcpu": 3.0}, "WRITER_ONLY": {"dl_vcpu": 5.9}, "WRITER_PLUS_ALPHAS": {"dl_vcpu": 6.1}},
    }


class Shared100ProfileTests(unittest.TestCase):
    def setUp(self):
        self.profile = json.loads(PROFILE.read_text())

    def test_the_profile_loads_and_a6_pins_its_digest(self):
        load_shared100_profile(self.profile)
        a6 = json.loads(A6.read_text())
        self.assertEqual(a6["label"], "PRE_REGISTERED")
        self.assertFalse(a6["post_hoc"])
        self.assertEqual(a6["profile"]["sha256"], hashlib.sha256(PROFILE.read_bytes()).hexdigest())
        self.assertFalse(self.profile["quota_bump"])

    def test_a_weakened_profile_is_refused(self):
        for path, value in ((("restart_storm", "max_refusals"), 3), (("restart_storm", "sessions"), 50),
                            (("noisy_neighbour", "noisy_session_rps"), 10)):
            broken = copy.deepcopy(self.profile)
            broken["gates"][path[0]][path[1]] = value
            with self.subTest(path=path), self.assertRaises(ValueError):
                load_shared100_profile(broken)

    def test_a_passing_run_passes_every_gate(self):
        result = evaluate_shared100_gates(profile=self.profile, receipt=_passing_receipt())
        self.assertEqual(result["status"], "PASS", result["failed_gates"])
        self.assertEqual(len(result["gates"]), 7)
        g14 = result["gates"][-1]
        self.assertAlmostEqual(g14["alpha_delta_vcpu"], 0.2)
        self.assertAlmostEqual(g14["writer_delta_vcpu"], 2.9)

    def test_each_failure_is_named_and_missing_evidence_fails(self):
        cases = {
            "shared100:restart_storm:zero_refusals": lambda r: r["restart_storm"].update(refusals=1),
            "shared100:restart_storm:cold_start_window": lambda r: r["restart_storm"].update(
                session_start_offsets_s=[0.0] * 99 + [12.0]),
            "shared100:restart_storm:every_warmup_served": lambda r: r["restart_storm"]["warmups"][5].update(
                status="RATE_LIMITED"),
            "shared100:restart_storm:ts_lane_p95_within_g5": lambda r: r["restart_storm"].update(
                ts_lane_ms=[9_000.0] * 10),
            "shared100:noisy_neighbour:noisy_session_throttled": lambda r: r["noisy_neighbour"].update(
                noisy_rate_limited=0),
            "shared100:noisy_neighbour:others_unaffected": lambda r: r["noisy_neighbour"]["other_sessions"][3].update(
                refusals=2),
            "shared100:g14:baseline_subtracted_alpha_delta": lambda r: r["g14"].pop("IDLE"),
        }
        for gate, mutate in cases.items():
            receipt = _passing_receipt()
            mutate(receipt)
            with self.subTest(gate=gate):
                result = evaluate_shared100_gates(profile=self.profile, receipt=receipt)
                self.assertEqual(result["failed_gates"], [gate])
        self.assertEqual(evaluate_shared100_gates(profile=self.profile, receipt={})["status"], "FAIL")


class Shared100CandidateTests(unittest.TestCase):
    def setUp(self):
        self.network = patch("socket.socket", side_effect=AssertionError("offline only"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def test_shared100_keeps_streams_turns_hot_polls_on_demand_and_needs_no_bump(self):
        realistic, _ = build_capacity100_candidate(**inputs())
        plan, report = build_capacity100_candidate(**(inputs() | {"profile": "shared100"}))
        self.assertEqual(plan.stream_count, realistic.stream_count)
        self.assertEqual(plan.hot_requests_per_second, 0)
        self.assertTrue(all(poll.period_seconds >= 60 for s in plan.sessions for poll in s.polls))
        shared = report["shared100"]
        self.assertEqual(len(shared["session_ids"]), 100)
        self.assertEqual(shared["session_header"], "X-QDL-Session")
        self.assertFalse(shared["quota_bump_required"], shared["quota_model"])
        self.assertEqual(report["status"], "CANDIDATE_NOT_CERTIFIED")


if __name__ == "__main__":
    unittest.main()
