import unittest
from scripts.kn5_review_evidence import distribution, qualify_summary, http_outcome, build_report
from scripts.kn_native_slice_probe import _dist

class EvidenceHonestyTests(unittest.TestCase):
    def test_percentiles_need_sample_count_max_is_always_named_max(self):
        for n in (0, 4, 6, 19, 20, 35, 36, 74, 99, 100, 1260):
            with self.subTest(n=n):
                value = distribution(range(n))
                self.assertEqual(value["n"], n)
                self.assertEqual(value["p99_ms"] is None, n < 100)
                self.assertEqual(value["p95_ms"] is None, n < 20)
                if n:
                    self.assertEqual(value["max_ms"], n - 1)
                    self.assertEqual(_dist(range(n))["p99"] is None, n < 100)
        for v in (-1, float("nan"), float("inf")):
            with self.assertRaises(ValueError): distribution([v])

    def test_partial_diagnostic_is_safe_but_not_complete_or_pass(self):
        for status in (206, 409, 503):
            result = http_outcome({"operation": "GET /v2/data-quality/gaps", "http": status,
                                   "code": "PARTIAL_RESULT", "status": "PASS", "ms": 5000})
            self.assertEqual(result["status"], "PARTIAL_NOT_COMPLETE")
            self.assertFalse(result["scan_complete"])
            self.assertTrue(result["bounded_fail_closed"])
        self.assertTrue(http_outcome({"operation": "GET /v2/data-quality/gaps", "http": 200})["scan_complete"])

    def test_live_harness_does_not_certify_partial_or_malformed_gap_responses(self):
        from scripts.phase3_consumer_load_acceptance import _kn4_http_result
        path = "/v2/data-quality/gaps"
        for code in (206, 409, 503):
            outcome = _kn4_http_result(path, code, {"code": "PARTIAL_RESULT"})
            self.assertEqual(outcome["status"], "FAIL")
            self.assertFalse(outcome["scan_complete"])
        self.assertEqual(_kn4_http_result(path, 200, {})["status"], "FAIL")
        self.assertTrue(_kn4_http_result(path, 200, {"schema": "qdl.data-quality.gaps.v2", "items": []})["scan_complete"])

    def test_refusals_are_not_removed_from_usable_denominator(self):
        row = {"n": 74, "p99": 585, "max": 585}
        sample = {"phase": "A_snapshots_per_replica", "reads": 100, "failed": 26, "products": 60,
                  "products_ok_on_both_replicas": 59, "timing_basis": "caller queue + SDK", "per_binding_replica": {},
                  "by_feed": {"TRADE": {"reads": 100, "failed": 26, "call_to_usable_ms": row}}}
        report = build_report({"sections": {"http": []}}, {"latency_series": {}}, {"results": [sample]})
        trade = report["consumer_snapshot"]["feeds"]["TRADE"]
        self.assertEqual((trade["usable"], trade["refused"], trade["usable_ratio"]), (74, 26, .74))
        self.assertIsNone(trade["timings"]["call_to_usable_ms"]["p99"])
        self.assertEqual(row["p99"], 585)  # original receipt is immutable

if __name__ == "__main__": unittest.main()
