import copy
import unittest
from scripts.kn_retire_expired_l2 import retire


class ExpiredResearchBookTests(unittest.TestCase):
    def docs(self):
        c = {"catalog_revision": 10, "instruments": [
            {"instrument_uid": "expired", "native_symbol": "FUTURE", "expiry_time_ns": 100},
            {"instrument_uid": "live", "native_symbol": "PERP"}], "bindings": [
            {"binding_id": "old", "instrument_uid": "expired", "feed": "BOOK_DELTA"},
            {"binding_id": "other", "instrument_uid": "expired", "feed": "BAR"},
            {"binding_id": "live", "instrument_uid": "live", "feed": "BOOK_DELTA"}]}
        a = {"revision": 3, "bindings": copy.deepcopy(c["bindings"])}
        s = {"revision": 2, "binding_ids": ["old", "other", "live"]}
        m = {"research": {"metadata": {"revision": 5}, "spec": {"requirements": [
            {"instrument_uid": "expired", "feed": "BOOK_DELTA", "consumer_grade": "RESEARCH"},
            {"instrument_uid": "live", "feed": "BOOK_DELTA", "consumer_grade": "RESEARCH"}]}}}
        return c,a,s,m

    def test_expiry_boundary_metadata_and_unrelated_demand_preserved(self):
        docs = self.docs(); saved = copy.deepcopy(docs)
        self.assertEqual(retire(*docs, as_of_ns=99)[:4], docs)
        c,a,s,m,r = retire(*docs, as_of_ns=100)
        self.assertEqual(r["retired_binding_ids"], ["old"])
        self.assertEqual(c["instruments"], docs[0]["instruments"])
        self.assertEqual([b["binding_id"] for b in c["bindings"]], ["other", "live"])
        self.assertEqual((c["catalog_revision"],a["revision"],s["revision"],m["research"]["metadata"]["revision"]), (11,4,3,6))
        self.assertEqual(docs, saved)
        self.assertEqual(retire(c,a,s,m,as_of_ns=101)[:4], (c,a,s,m))

    def test_never_silently_retires_execution_or_alpha_entitlement(self):
        for grade in ("ALPHA", "EXECUTION"):
            with self.subTest(grade=grade):
                c,a,s,m = self.docs()
                m["research"]["spec"]["requirements"][0]["consumer_grade"] = grade
                with self.assertRaisesRegex(ValueError, "separate migration"):
                    retire(c,a,s,m,as_of_ns=100)

    def test_invalid_clock(self):
        for now in (0, -1, True, "100"):
            with self.assertRaises(ValueError):
                retire(*self.docs(), as_of_ns=now)


class ProviderHistoryClassificationTests(unittest.TestCase):
    def test_actual_provider_overlap_is_not_a_missing_bar_to_synthesize(self):
        from qdl.adapters.intervals import BarHistoryGapError
        for previous, current in ((1569110400000,1569283200000),
                                  (1691971200000,1692144000000),
                                  (1692057600000,1692144000000)):
            error = BarHistoryGapError("BINANCE", "TEST_ONLY", "3d", previous, current)
            self.assertEqual(error.kind, "OVERLAPPING_PROVIDER_WINDOWS")
        error = BarHistoryGapError("OKX", "TEST_ONLY", "1m", 60000, 180000)
        self.assertEqual(error.kind, "MISSING_PROVIDER_WINDOW")


class ServingCpuAccountingTests(unittest.TestCase):
    def test_missing_roles_never_pass_and_counter_reset_rejected(self):
        from scripts.kn_serving_cpu_receipt import summarize, SHARED, KN
        rows = [{"t":i*1_000_000_000, "c":{role:{"cpu":f"usage_usec={i*1000}", "mem":100}
                 for role in SHARED+KN}} for i in (1,2)]
        self.assertEqual(summarize(rows)["full_stack_gate"], "INCOMPLETE")
        rows[-1]["c"][KN[0]]["cpu"]="usage_usec=0"
        with self.assertRaisesRegex(ValueError, "counter reset"):
            summarize(rows)

    def test_present_coordination_redis_is_included_in_cpu_gate(self):
        from scripts.kn_serving_cpu_receipt import summarize, SHARED, KN, MISSING
        rows = [{"t":i*1_000_000_000, "c":{role:{"cpu":f"usage_usec={i*1000}", "mem":100}
                 for role in SHARED+KN+MISSING}} for i in (1,2)]
        rows[-1]["c"][MISSING[0]]["cpu"]="usage_usec=10000000"
        result = summarize(rows)
        self.assertEqual(result["full_stack_gate"], "FAIL")
        self.assertGreater(result["measured_subtotal_mean_cores"], 5)
