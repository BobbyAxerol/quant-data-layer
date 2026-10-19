import copy
import sys
import unittest

from scripts.p184_dr05_dl_fault import schedule
from scripts.p184_dr05_verdict import evaluate


def observation(venue="OKX", fault="GAP_REFUSAL"):
    rows = []
    for step in schedule(venue, fault):
        phase = step["phase"]
        negative = phase in {"gap", "new_generation_unanchored"}
        matches = [] if phase == "old_generation" else [{
            "kind": "quarantine" if negative else "canonical", "venue": venue,
            "capture_id": phase, "generation": step["generation"],
            "reason": "QUARANTINE_REASON_SEQUENCE_GAP"}]
        rows.append({"phase": phase, "publication": {"offset": 1, "capture_id": phase},
            "canonical": {"raw_committed_next": 2, "matched": matches},
            "views": {feed: {"GetSnapshot": {"code": "FAILED_PRECONDITION" if negative else "OK",
                "cursor_valid": True, "detail": "DATA_GAP" if negative else ""},
                "GetFeedStatus": {"status": {"execution_eligible": not negative, "gap_open": negative}}}
                for feed in ("BOOK_SNAPSHOT", "BOOK_DELTA")},
            "ts_live": {"live": True, "results": [{"feed": "BOOK_SNAPSHOT", "accepted": not negative,
                "outcome": "ENDPOINT_REFUSED" if negative else "APPLIED"}]}})
    return {"venue": venue, "fault": fault, "observations": rows}


class VerdictTests(unittest.TestCase):
    def test_both_venues_and_faults(self):
        for venue in ("BINANCE", "OKX"):
            for fault in ("GAP_REFUSAL", "GENERATION_CHANGE_FENCED"):
                self.assertTrue(evaluate(observation(venue, fault))["ok"])

    def test_missing_or_duplicate_phase(self):
        value = observation()
        value["observations"].pop()
        self.assertFalse(evaluate(value)["ok"])
        value = observation()
        value["observations"].append(copy.deepcopy(value["observations"][-1]))
        self.assertFalse(evaluate(value)["ok"])

    def test_old_book_still_usable_is_failure(self):
        value = observation()
        row = next(r for r in value["observations"] if r["phase"] == "gap")
        row["views"]["BOOK_SNAPSHOT"]["GetSnapshot"]["code"] = "OK"
        self.assertFalse(evaluate(value)["ok"])

    def test_waiting_for_staleness_does_not_prove_gap(self):
        value = observation()
        row = next(r for r in value["observations"] if r["phase"] == "gap")
        for view in row["views"].values():
            view["GetFeedStatus"]["status"]["gap_open"] = False
            view["GetSnapshot"]["detail"] = "DATA_STALE"
        self.assertFalse(evaluate(value)["ok"])

    def test_unrelated_quarantine_not_evidence(self):
        value = observation()
        row = next(r for r in value["observations"] if r["phase"] == "gap")
        row["canonical"]["matched"][0]["capture_id"] = "other"
        self.assertFalse(evaluate(value)["ok"])

    def test_offline_ts_does_not_count_as_live(self):
        value = observation()
        value["observations"][1]["ts_live"]["live"] = False
        self.assertFalse(evaluate(value)["ok"])

    def test_old_generation_canonical_is_failure(self):
        value = observation()
        row = next(r for r in value["observations"] if r["phase"] == "old_generation")
        row["canonical"]["matched"] = [{"kind": "canonical", "generation": 7}]
        self.assertFalse(evaluate(value)["ok"])

    def test_uncommitted_filter_does_not_prove_fencing(self):
        value = observation()
        row = next(r for r in value["observations"] if r["phase"] == "old_generation")
        row["canonical"]["raw_committed_next"] = 1
        self.assertFalse(evaluate(value)["ok"])

    def test_no_write_is_not_a_refusal(self):
        value = observation()
        row = next(r for r in value["observations"] if r["phase"] == "gap")
        row["ts_live"]["results"][0]["outcome"] = "NO_WRITE"
        self.assertFalse(evaluate(value)["ok"])

    def test_binance_recovery_continues_buffered_generation_delta(self):
        for venue, fault, expected in [
            ("BINANCE", "GENERATION_CHANGE_FENCED", 201),
            ("OKX", "GENERATION_CHANGE_FENCED", 200),
            ("BINANCE", "GAP_REFUSAL", 200),
        ]:
            step = next(s for s in schedule(venue, fault) if s["phase"] == "return_control")
            self.assertEqual(step["previous"], expected)

    def test_post_fence_generation_is_derived_from_the_run_not_a_constant(self):
        """OF-G13-2: the expected generation is the one this run's schedule published (re-anchor), not 8."""
        from unittest.mock import patch
        import scripts.p184_dr05_dl_fault as dl
        original = dl.schedule

        def shifted(venue, fault):  # the same cell published with generations 12 / 13 (e.g. after a core rewind)
            return [dict(step, generation=step["generation"] + 5) for step in original(venue, fault)]
        with patch.object(dl, "schedule", shifted), patch.object(sys.modules[__name__], "schedule", shifted):
            value = observation()
            self.assertTrue(evaluate(value)["ok"])  # observations carry the shifted generations
            stale = observation()
            for row in stale["observations"]:
                for match in row["canonical"]["matched"]:
                    match["generation"] = 8  # the old hard-coded value is no longer accepted
            result = evaluate(stale)
            self.assertFalse(result["checks"]["post_fence_generation"])
            self.assertFalse(result["ok"])

    def test_post_fence_control_must_be_identity_matched_in_the_reanchored_generation(self):
        value = observation()
        row = next(r for r in value["observations"] if r["phase"] == "post_fence_control")
        row["canonical"]["matched"][0]["capture_id"] = "someone-else"  # a generation-8 row of another frame
        self.assertFalse(evaluate(value)["checks"]["post_fence_generation"])
        value = observation()
        row = next(r for r in value["observations"] if r["phase"] == "post_fence_control")
        row["canonical"]["matched"][0]["generation"] = 7  # canonical in the fenced generation
        result = evaluate(value)
        self.assertFalse(result["checks"]["post_fence_generation"])
        self.assertFalse(result["checks"]["post_fence_control.canonical_match"])

    def test_control_canonical_generation_must_match_the_published_frame(self):
        for phase in ("positive_control", "return_control"):
            with self.subTest(phase=phase):
                value = observation()
                row = next(r for r in value["observations"] if r["phase"] == phase)
                row["canonical"]["matched"][0]["generation"] += 1
                self.assertFalse(evaluate(value)["checks"][phase + ".canonical_match"])
                self.assertFalse(evaluate(value)["ok"])
