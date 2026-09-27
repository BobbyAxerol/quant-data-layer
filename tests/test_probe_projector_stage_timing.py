"""Synthetic unit evidence only, not production latency evidence."""
import unittest

from scripts.probe_projector_stage_timing import classify_stage, counter_rate, reference_metadata


class StageTimingTests(unittest.TestCase):
    def test_reference_preserves_problem_components_without_price_or_secrets(self):
        import json
        response = {"partial": True, "results": [{"status": "ERROR", "problem": {"code": "DATA_STALE"}},
            {"status": "OK", "data": {"received_at_ns": 5, "observations": [{"observed_at_ns": 4,
                "fields": [{"name": "mark_price", "value": "SECRET_PRICE"}],
                "labels": {"component_mark_received_at_ns": "3", "recency_mode": "COMPONENT_SESSION_LIVE",
                           "cursor": "SECRET_CURSOR"}}]}}]}
        result = reference_metadata(response)
        self.assertEqual(result["results"][0]["problem"], {"code": "DATA_STALE"})
        self.assertEqual(result["results"][1]["data"]["observations"][0]["labels"]["component_mark_received_at_ns"], "3")
        self.assertNotIn("SECRET", json.dumps(result))

    def test_missing_state_never_localizes_stage(self):
        self.assertEqual(classify_stage(10, None, 30),
                         "CANONICAL_TO_REDIS_ONLY_STATE_UNAVAILABLE")

    def test_observer_order_is_not_negative_pipeline_latency(self):
        self.assertEqual(classify_stage(20, 10, 30),
                         "OBSERVER_ORDER_INVERSION_NOT_NEGATIVE_LATENCY")
        self.assertEqual(classify_stage(10, 30, 20),
                         "OBSERVER_ORDER_INVERSION_NOT_NEGATIVE_LATENCY")

    def test_ordered_observations_are_not_broker_commit_times(self):
        self.assertEqual(classify_stage(10, 20, 30),
                         "OBSERVED_STAGE_INTERVALS_NOT_BROKER_COMMIT_TIMES")

    def test_counter_rates_reject_restarts_and_resets(self):
        first = dict(at_ms=1000, started_ms=1, stage_a=dict(transactions=10))
        last = dict(at_ms=6000, started_ms=1, stage_a=dict(transactions=35))
        self.assertEqual(counter_rate(first, last, "transactions"), 5)
        last["started_ms"] = 2
        self.assertIsNone(counter_rate(first, last, "transactions"))
        last["started_ms"] = 1
        last["stage_a"]["transactions"] = 9
        self.assertIsNone(counter_rate(first, last, "transactions"))


if __name__ == "__main__":
    unittest.main()
