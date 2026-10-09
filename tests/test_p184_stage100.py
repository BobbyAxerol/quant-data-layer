"""P18.4 phase D stage-100 ramp (D3): planner, frozen budget and G5 shape gates. TEST_ONLY fixtures, no traffic."""
from __future__ import annotations

import unittest

from qdl.certification.phase3_consumer_load import (
    P184_STAGE_MIX,
    P184_STAGE_SECONDS,
    TARGET_STAGE_MIX,
    evaluate_target_acceptance,
    load_p184_stage100_budget,
    load_target_budget,
)
from tests.test_phase3_target_workload import _plan


def _budget(**overrides):
    raw = {
        "schema": "qdl.p184.stage100-budget.v1", "frozen_before_candidate": True,
        "stages": {str(k): {"mix": list(P184_STAGE_MIX[k]), "seconds": P184_STAGE_SECONDS[k]} for k in P184_STAGE_MIX},
        "g5_shape_limits_ms": {"COLD_2500": {"p95": 4000.0}, "COLD_5000": {"p95": 6000.0}},
        "trading_system": {"demanded_routes": 64},
    }
    raw.update(overrides)
    return raw


class Stage100PlanTests(unittest.TestCase):
    def test_stage_100_keeps_the_four_class_proportions_and_the_v211_table_is_unchanged(self):
        self.assertEqual(P184_STAGE_MIX[100], (40, 30, 20, 10))
        self.assertEqual(set(TARGET_STAGE_MIX), {5, 20, 35, 50})  # frozen v2.1.1 table untouched
        self.assertEqual(P184_STAGE_SECONDS[100], 1800)  # owner: >= 30 min steady at stage 100

    def test_stage_100_plans_100_sessions_and_needs_more_than_todays_alpha_quota(self):
        with self.assertRaisesRegex(ValueError, "does not fit sealed quota"):
            _plan(100, rpm=2400, streams=60)  # the deployed alpha manifests cannot carry stage 100
        plan = _plan(100, rpm=4950, streams=300)  # the capacity100 candidate quota
        self.assertEqual(len(plan.sessions), 100)
        self.assertTrue(all(d.fits for d in plan.demands))
        self.assertEqual(round(plan.hot_requests_per_second), 100)


class Stage100BudgetTests(unittest.TestCase):
    def test_a_frozen_budget_loads(self):
        self.assertEqual(load_p184_stage100_budget(_budget())["trading_system"]["demanded_routes"], 64)

    def test_unfrozen_or_incomplete_budgets_are_refused(self):
        for bad in (_budget(frozen_before_candidate=False), _budget(g5_shape_limits_ms={}),
                    _budget(g5_shape_limits_ms={"COLD_2500": {"p95": None}}), _budget(trading_system={}),
                    _budget(schema="qdl.v211.target-acceptance-budget.v1")):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                load_p184_stage100_budget(bad)

    def test_the_v211_loader_still_refuses_a_stage_100_budget(self):
        with self.assertRaises(ValueError):
            load_target_budget({**_budget(), "schema": "qdl.v211.target-acceptance-budget.v1"})


class Stage100G5GateTests(unittest.TestCase):
    def _evaluate(self, cold):
        budget = {**_budget(), "latency_ms": {}, "sample_rule": {"p99_min_samples": 100, "p95_min_samples": 20},
                  "requests": {"max_missed_ticks": 0, "max_failed_steady": 0, "client_validity_scheduler_lag_p99_ms": 250},
                  "streams": {"max_stream_errors": 0, "first_event_required_feeds": [], "min_final_bars_per_bar_stream": 0},
                  "cold": {"rows": [2500, 5000]}, "startup": {"max_setup_seconds": 60},
                  "trading_system": {"demanded_routes": 64, "max_fallback": 0, "max_v2_error_increase": 0,
                                     "forbidden_disconnect_codes": [], "max_disconnect_rate_over_baseline_per_minute": 1}}
        receipt = {"latency_series": {"QUOTE_SNAPSHOT|BINANCE|STEADY": [10.0]}, "poll_ledgers": [],
                   "scheduler_lag_ms": {"p99": 1}, "streams": [], "cold": cold,
                   "setup": {"failures": 0, "seconds": 1, "retries": 0}, "leaked_tasks": 0}
        result = evaluate_target_acceptance(budget=budget, stage=100, final=False, receipt=receipt,
                                            trading_system={"samples": []})
        return {g["gate"]: g for g in result["gates"]}

    def test_cold_shapes_are_gated_against_their_frozen_limit(self):
        gates = self._evaluate([{"venue": "BINANCE", "rows": 2500, "ms": 3500.0},
                                {"venue": "BINANCE", "rows": 5000, "ms": 6500.0}])
        self.assertEqual(gates["g5:COLD_2500:BINANCE"]["status"], "PASS")
        self.assertEqual(gates["g5:COLD_5000:BINANCE"]["status"], "FAIL")

    def test_a_missing_or_failed_cold_read_fails_its_shape_gate(self):
        gates = self._evaluate([{"venue": "BINANCE", "rows": 2500, "ms": 100.0, "error": {"type": "X"}}])
        self.assertEqual(gates["g5:COLD_2500:BINANCE"]["status"], "FAIL")
        self.assertEqual(gates["g5:COLD_5000:BINANCE"]["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
