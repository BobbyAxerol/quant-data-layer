"""P18.4 stage-100 amendment A4 (POST_HOC_OWNER_DECISION 2026-10-18 ~14:45Z): G4 OKX_INVERSE rows on the pooled OKX
row p95 limits (max report-only, outliers retained) and G2 MARK_INDEX_PRICE|BINANCE max <= 1200 ms.

Negatives first: an inverse p95 over the pooled limit, a straddled bucket, thin windows, missing / overflowing /
incomplete retention, MARK BINANCE max 1200.1 ms or p95 500.1 ms, another failing row, a changed A4 file and a run or
scorecard A4 does not name. Parity: the frozen scorer file is the one A3 pins; on 053803Z / 135719Z / 074624Z /
092255Z / 123222Z the frozen scorer reproduces the stored gates without A4, and A4 changes nothing outside its four
rows. Host-runnable (stdlib only)."""
from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts import p184_a4_post_hoc as a4
from scripts import p184_gate_campaign as gc
from tests.p184_receipt_support import mirror

STATE = Path.home() / ".local/state/qdl-v2"
COUNTED = STATE / "p184-final-campaign-20261018T123222Z"
RECEIPTS = [STATE / name for name in ("p184-final-campaign-20261018T053803Z", "p184-final-campaign-20261017T135719Z",
                                      "p184-final-campaign-20261018T074624Z", "p184-final-campaign-20261018T092255Z",
                                      "p184-final-campaign-20261018T123222Z")]
TABLE = {"book_delta|OKX|": {"candidate_p95_limit_ms": 289.1}, "book_snapshot|OKX|": {"candidate_p95_limit_ms": 270.1},
         "quote|OKX|": {"candidate_p95_limit_ms": 283.3}}
ADDENDUM = {"rows": {"book_delta|OKX_INVERSE|": {"status": "GATED", "candidate_max_limit_ms": 282.5},
                     "book_snapshot|OKX_INVERSE|": {"status": "GATED", "candidate_max_limit_ms": 300.7},
                     "quote|OKX_INVERSE|": {"status": "GATED", "candidate_max_limit_ms": 322.6}}}


def bucket(n, p95, mx, lower=None):
    return {"n": n, "p95": p95, "p95_bucket_lower": p95 - 1 if lower is None else lower, "max": mx,
            "percentile_basis": "BUCKET_UPPER_BOUND"}


def window(inverse=None, pooled=None):
    pooled = pooled or {key: bucket(1000, 200.0, 300.0) for key in TABLE}
    inverse = inverse or {}
    rows = {key: inverse.get(key, bucket(120, 180.0, 250.0)) for key in a4.G4_ROWS}
    return {"summary": {key: {"visibility_ms": v} for key, v in pooled.items()}, "inverse_rows": rows}


def event(vis, offset=1):
    return {"topic": "md.canonical.v2", "partition": 1, "offset": offset, "visibility_ms": vis,
            "source_event_ns": 1, "received_at_ns": 2, "producer_timestamp_ms": 3, "observer_receipt_wall_ns": 4}


def retention(n, events, *, threshold=282.5, overflow=0, top=None):
    top = top if top is not None else max(events, key=lambda e: e["visibility_ms"]) if events else None
    return {"threshold_ms": threshold, "cap": 256, "overflow": overflow, "admitted_seen": n,
            "max_ms": top["visibility_ms"] if top else None, "max_event": top, "events": events}


def g4(windows, held=None, table=TABLE, addendum=ADDENDUM):
    held = held if held is not None else [{key: None for key in a4.G4_ROWS} for _ in windows]
    return a4.a4_score_g4(gc.score_g4, held)(windows, table, addendum)


def mark(binance=(9000, 418.6, 1082.9), okx=(21516, 353.2, 743.9)):
    venues = {}
    for venue, (n, p95, mx) in (("BINANCE", binance), ("OKX", okx)):
        if n is None:
            continue
        inside = mx < 1000 and p95 <= 500
        venues[venue] = {"new_event_transit_ms": {"n": n, "p50": 1.0, "p95": p95, "p99": p95, "max": mx},
                         "g2_verdict": "INSIDE" if inside else "OUTSIDE"}
    return {"venues": venues}


def amendment(**over):
    out = {"schema": a4.A4_SCHEMA, "amendment_id": "A4", "label": a4.A4_LABEL, "post_hoc": True,
           "decision_owner": "Bobby (owner)", "frozen_at_unix": 1.0,
           "applies_to": {"campaign_id": COUNTED.name, "scorecard_sha256": "s" * 64, "scorer_sha256": "t" * 64},
           "rules": {"G2": {"row": a4.G2_ROW, "max_le_ms": 1200.0, "p95_le_ms": 500.0},
                     "G4": {"min_samples_per_window": 20, "max": "REPORT_ONLY",
                            "rows": {k: {"pooled_row": p, "p95_limit_ms": lim, "replaced_addendum_max_limit_ms": r}
                                     for k, (p, lim, r) in a4.G4_ROWS.items()}}}}
    out.update(over)
    return out


class G4RowTests(unittest.TestCase):
    def test_inside_pooled_p95_passes_with_max_report_only_and_outliers_retained(self):
        windows = [window({"quote|OKX_INVERSE|": bucket(167, 213.0, 531.9)})]
        held = [{"quote|OKX_INVERSE|": retention(167, [event(531.9)], threshold=322.6),
                 "book_delta|OKX_INVERSE|": None, "book_snapshot|OKX_INVERSE|": None}]
        state, rows, why = g4(windows, held)
        self.assertEqual((state, why), ("PASS", []))
        row = {r["row"]: r for r in rows}["quote|OKX_INVERSE|"]
        self.assertEqual((row["verdict"], row["max_report_only"], row["limit_p95_ms"]), ("PASS", True, 283.3))
        self.assertEqual(row["frozen_rule_row"]["verdict"], "FAIL")  # the frozen addendum max rule
        self.assertEqual([e["visibility_ms"] for e in row["retained_over_replaced_limit"]], [531.9])

    def test_inverse_p95_over_the_pooled_limit_fails(self):
        windows = [window({"book_delta|OKX_INVERSE|": bucket(120, 291.0, 300.0, lower=290.0)})]
        held = [{"book_delta|OKX_INVERSE|": retention(120, [event(300.0)]), "book_snapshot|OKX_INVERSE|": None,
                 "quote|OKX_INVERSE|": None}]
        state, rows, why = g4(windows, held)
        self.assertEqual(state, "FAIL")
        self.assertEqual(why, ["over limit: book_delta|OKX_INVERSE|"])

    def test_straddled_bucket_and_thin_windows_are_not_measured(self):
        straddle = [window({"book_snapshot|OKX_INVERSE|": bucket(120, 271.0, 280.0, lower=270.0)})]
        self.assertEqual(g4(straddle)[0], "NOT_MEASURED")
        thin = [window({"quote|OKX_INVERSE|": bucket(19, 100.0, 120.0)})]
        state, rows, why = g4(thin)
        self.assertEqual(state, "NOT_MEASURED")
        self.assertIn("A4 rows not measured: quote|OKX_INVERSE|", why)

    def test_retention_must_hold_every_event_over_the_replaced_limit(self):
        windows = [window({"book_delta|OKX_INVERSE|": bucket(169, 170.0, 387.1)})]
        cases = {"no retention (observer < v7)": None,
                 "overflow": retention(169, [event(387.1)], overflow=1),
                 "threshold above the limit": retention(169, [event(387.1)], threshold=300.0),
                 "admitted count differs": retention(168, [event(387.1)]),
                 "max not retained": retention(169, [event(300.0)], top=event(387.1, offset=9)),
                 "max differs": retention(169, [event(380.0)])}
        for name, held in cases.items():
            state, rows, _ = g4(windows, [{"book_delta|OKX_INVERSE|": held, "book_snapshot|OKX_INVERSE|": None,
                                           "quote|OKX_INVERSE|": None}])
            row = {r["row"]: r for r in rows}["book_delta|OKX_INVERSE|"]
            self.assertEqual((state, row["verdict"]), ("FAIL", "FAIL"), name)
            self.assertTrue(row["problems"], name)
        good = [{"book_delta|OKX_INVERSE|": retention(169, [event(387.1)]), "book_snapshot|OKX_INVERSE|": None,
                 "quote|OKX_INVERSE|": None}]
        self.assertEqual(g4(windows, good)[0], "PASS")

    def test_another_failing_or_unbaselined_row_keeps_the_gate_blocked(self):
        failing_pooled = [window(pooled={**{k: bucket(1000, 200.0, 300.0) for k in TABLE},
                                         "quote|OKX|": bucket(1000, 300.0, 400.0, lower=299.0)})]
        state, _, why = g4(failing_pooled)
        self.assertEqual((state, why), ("FAIL", ["over limit: quote|OKX|"]))
        unbaselined = [window()]
        unbaselined[0]["summary"]["bar|OKX|7m"] = {"visibility_ms": bucket(5, 1.0, 2.0)}
        state, _, why = g4(unbaselined)
        self.assertEqual(state, "NOT_MEASURED")
        self.assertIn("observed rows without a frozen baseline: bar|OKX|7m", why)

    def test_a_changed_pooled_limit_is_not_measured_never_pass(self):
        table = {**TABLE, "quote|OKX|": {"candidate_p95_limit_ms": 300.0}}
        state, rows, _ = g4([window()], table=table)
        self.assertEqual(state, "NOT_MEASURED")
        self.assertEqual({r["row"]: r for r in rows}["quote|OKX_INVERSE|"]["verdict"], "NOT_MEASURED")

    def test_rows_outside_a4_are_byte_identical(self):
        windows = [window({"quote|OKX_INVERSE|": bucket(167, 213.0, 531.9)})]
        _, frozen, _ = gc.score_g4(windows, TABLE, ADDENDUM)
        _, amended, _ = g4(windows)
        keep = lambda rows: [r for r in rows if r["row"] not in a4.G4_ROWS]  # noqa: E731
        self.assertEqual(json.dumps(keep(frozen), sort_keys=True), json.dumps(keep(amended), sort_keys=True))


class G2RowTests(unittest.TestCase):
    def score(self, **kw):
        return a4.a4_score_mark(gc.score_mark)(mark(**kw))

    def test_max_limit_is_1200_inclusive(self):
        self.assertEqual(self.score(binance=(9000, 418.6, 1082.9))[0], "PASS")
        self.assertEqual(self.score(binance=(9000, 418.6, 1200.0))[0], "PASS")
        state, rows, why = self.score(binance=(9000, 418.6, 1200.1))
        self.assertEqual((state, why), ("FAIL", ["outside: MARK|BINANCE"]))
        self.assertEqual(rows[0]["a4"]["observed_frozen_rule"], "OUTSIDE")

    def test_p95_unchanged_and_other_rows_unchanged(self):
        self.assertEqual(self.score(binance=(9000, 500.1, 900.0))[0], "FAIL")
        state, rows, why = self.score(okx=(100, 300.0, 1000.0))  # MARK|OKX keeps max < 1000
        self.assertEqual((state, why), ("FAIL", ["outside: MARK|OKX"]))
        self.assertEqual(json.dumps(rows[1], sort_keys=True), json.dumps(gc.score_mark(mark(okx=(100, 300.0, 1000.0)))[1][1],
                                                                       sort_keys=True))
        self.assertEqual(self.score(okx=(None, None, None))[0], "NOT_MEASURED")


class ValidationTests(unittest.TestCase):
    def test_owner_decision_exactly(self):
        self.assertEqual(a4.validate_a4(amendment()), [])
        rules = amendment()["rules"]
        bad = [{"schema": "x"}, {"post_hoc": False}, {"label": "PRE_REGISTERED"}, {"decision_owner": None},
               {"rules": {**rules, "G2": {**rules["G2"], "max_le_ms": 1300.0}}},
               {"rules": {**rules, "G2": {**rules["G2"], "p95_le_ms": 600.0}}},
               {"rules": {**rules, "G4": {**rules["G4"], "max": "GATED"}}},
               {"rules": {**rules, "G4": {**rules["G4"], "min_samples_per_window": 5}}},
               {"rules": {**rules, "G4": {**rules["G4"], "rows": {**rules["G4"]["rows"], "quote|OKX_INVERSE|": {
                   "pooled_row": "quote|OKX|", "p95_limit_ms": 400.0, "replaced_addendum_max_limit_ms": 322.6}}}}},
               {"applies_to": {}}]
        for over in bad:
            self.assertTrue(a4.validate_a4(amendment(**over)), over)

    def test_frozen_file_states_the_owner_decision(self):
        self.assertTrue(a4.A4_PATH.exists())
        frozen = json.loads(a4.A4_PATH.read_text())
        self.assertEqual(a4.validate_a4(frozen), [])
        self.assertEqual(frozen["applies_to"]["campaign_id"], COUNTED.name)
        self.assertEqual(frozen["applies_to"]["scorer_sha256"], gc.sha256_file(gc.ROOT / "scripts/p184_gate_campaign.py"))
        self.assertEqual(frozen["pinned"]["rescore_tool"]["sha256"], gc.sha256_file(Path(a4.__file__)))


class FrozenScorerTests(unittest.TestCase):
    def test_the_scorer_file_is_the_one_a3_pins(self):
        pins = json.loads(gc.A3_PATH.read_text())["pinned"]
        self.assertEqual(gc.sha256_file(gc.ROOT / "scripts/p184_gate_campaign.py"),
                         pins["gate_campaign_tool"]["new_sha256"])

    def test_wrappers_are_removed_after_use(self):
        before = (gc.score_mark, gc.score_g4)
        with a4.patched(score_mark=lambda m: None, score_g4=lambda *a, **k: None):
            self.assertNotEqual((gc.score_mark, gc.score_g4), before)
        self.assertEqual((gc.score_mark, gc.score_g4), before)


def strip_paths(value):
    if isinstance(value, dict):
        return {k: strip_paths(v) for k, v in value.items() if k != "path"}
    if isinstance(value, list):
        return [strip_paths(v) for v in value]
    return value


@unittest.skipUnless(all((d / "scorecard.json").exists() for d in RECEIPTS), "existing receipts not on this host")
class ReceiptParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.dirs = [mirror(d, Path(cls.tmp.name)) for d in RECEIPTS]
        cls.amendment_path = Path(cls.tmp.name) / "a4.json"
        frozen_a4 = json.loads(a4.A4_PATH.read_text())
        cls.amendment_path.write_text(json.dumps(frozen_a4))
        cls.results = {d.name: a4.rescore(d, cls.amendment_path, force_report_only=True) for d in cls.dirs}

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_frozen_scorer_reproduces_every_stored_gate_without_a4(self):
        for d in RECEIPTS:
            stored = json.loads((d / "scorecard.json").read_text())
            frozen = self.results[d.name]["frozen"]
            self.assertEqual(json.dumps(strip_paths(frozen["gates"]), sort_keys=True),
                             json.dumps(strip_paths(stored["gates"]), sort_keys=True), d.name)

    def test_counted_receipt_is_reproduced_byte_for_byte_in_place(self):
        stored = json.loads((COUNTED / "scorecard.json").read_text())
        self.assertEqual(hashlib.sha256((COUNTED / "scorecard.json").read_bytes()).hexdigest(),
                         json.loads(a4.A4_PATH.read_text())["applies_to"]["scorecard_sha256"])
        self.assertEqual(a4.canonical(json.loads(json.dumps(gc.build_scorecard(COUNTED), default=str))),
                         a4.canonical(stored))

    def test_a4_changes_nothing_outside_its_four_rows(self):
        for name, result in self.results.items():
            frozen = {g["id"]: g for g in result["frozen"]["gates"]}
            card = {g["id"]: g for g in result["card"]["gates"]}
            self.assertTrue(result["card"]["a4"]["report_only"] or name == COUNTED.name)
            for gid in frozen:
                if gid not in a4.SCOPED_GATES:
                    self.assertEqual(a4.canonical(frozen[gid]), a4.canonical(card[gid]), (name, gid))
            g2f = [r for r in frozen["G2"]["measured"] if r["feed_venue"] != a4.G2_ROW]
            g2a = [r for r in card["G2"]["measured"] if r["feed_venue"] != a4.G2_ROW]
            self.assertEqual(a4.canonical(g2f), a4.canonical(g2a), name)
            for key in ("measured", "report_only_rows"):
                g4f = [r for r in frozen["G4"].get(key, []) if r["row"] not in a4.G4_ROWS]
                g4a = [r for r in card["G4"].get(key, []) if r["row"] not in a4.G4_ROWS]
                self.assertEqual(a4.canonical(g4f), a4.canonical(g4a), (name, key))
            for key in set(frozen["G4"]) - {"measured", "rule", "verdict", "reason", "a4"}:
                self.assertEqual(a4.canonical(frozen["G4"][key]), a4.canonical(card["G4"][key]), (name, key))

    def test_a4_applies_only_to_the_named_run_and_scorecard(self):
        for d in self.dirs:
            result = a4.rescore(d, self.amendment_path)
            if d.name == COUNTED.name:
                continue  # the mirror rewrites no path here, but its stored scorecard is the pinned one
            self.assertFalse(result["applied"], d.name)
            self.assertIsNone(result["card"], d.name)
            self.assertIn(f"A4 names {COUNTED.name}, not {d.name}", result["problems"])
        changed = json.loads(self.amendment_path.read_text())
        changed["applies_to"]["scorecard_sha256"] = "0" * 64
        path = Path(self.tmp.name) / "a4-changed.json"
        path.write_text(json.dumps(changed))
        result = a4.rescore(COUNTED, path)
        self.assertFalse(result["applied"])
        self.assertIn("stored scorecard.json differs from the one A4 pins", result["problems"])

    def test_counted_run_verdicts_under_a4(self):
        result = a4.rescore(COUNTED, a4.A4_PATH)
        self.assertTrue(result["applied"], result["problems"])
        gates = {g["id"]: g for g in result["card"]["gates"]}
        self.assertEqual((gates["G2"]["verdict"], gates["G2"]["a4"]["verdict_without_a4"]), ("PASS", "FAIL"))
        self.assertEqual((gates["G4"]["verdict"], gates["G4"]["a4"]["verdict_without_a4"]), ("PASS", "FAIL"))
        rows = {r["row"]: r for r in gates["G4"]["measured"] if r["row"] in a4.G4_ROWS}
        self.assertEqual({k: r["verdict"] for k, r in rows.items()}, dict.fromkeys(a4.G4_ROWS, "PASS"))
        self.assertEqual(sum(len(r["retained_over_replaced_limit"]) for r in rows.values()), 8)
        self.assertEqual(result["card"]["summary"].get("FAIL"), None)
        self.assertEqual(result["card"]["a4"]["summary_without_a4"]["FAIL"], ["G2", "G4"])


if __name__ == "__main__":
    unittest.main()
