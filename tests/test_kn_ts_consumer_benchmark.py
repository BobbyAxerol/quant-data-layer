"""Benchmark semantics: no dependency on TS or live credentials at import."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("kn_ts_bench", Path(__file__).resolve().parents[1] / "scripts/benchmark_kn_ts_consumer.py")
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)

class BenchmarkTests(unittest.TestCase):
    def test_sparse_samples_do_not_claim_p99(self):
        self.assertIsNone(bench._dist(range(99))["p99"])
        self.assertEqual(bench._dist(range(100))["p99"], 99)
        self.assertEqual(bench._dist([]), {"n": 0})

    def test_bar_event_age_uses_close_not_open(self):
        item = type("Bar", (), {"timestamp_ms": 60000})()
        self.assertEqual(bench._event_ms(item, "1m"), (120000, "bar_close"))

    def test_execution_event_age_is_not_call_duration(self):
        item = type("Exec", (), {"observed_at_ms": 1234})()
        self.assertEqual(bench._event_ms(item), (1234, "observed_at_ms"))

    def test_typed_failure_keeps_diagnostics(self):
        error = RuntimeError("expired")
        error.code = "DATA_STALE"
        error.diagnostics = {"state": "LIVE", "execution_eligible": False}
        self.assertEqual(bench._error(error)["diagnostics"], error.diagnostics)
