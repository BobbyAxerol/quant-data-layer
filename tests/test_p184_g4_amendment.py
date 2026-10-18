"""P18.4 G4 amendment 1 (mark_index_price|OKX| scored on the same-event internal split): observer v6 paired split,
amended-row scoring, applicability, parity of every other row, baseline recompute and the venue probe (no Kafka)."""
from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from scripts import p184_freeze_g4_amendment as freeze
from scripts import p184_g4_observer as observer
from scripts import p184_g4_okx_mark_index_probe as probe
from scripts import p184_gate_campaign as campaign
from scripts.p184_resp_readonly import read_jsonl
from tests.p184_receipt_support import as_recorded, mirror

S = 1_000_000_000
MS = 1_000_000
W0 = 1_792_072_000 * S
ROW = campaign.G4_AMENDMENT_ROW
STATE = Path.home() / ".local/state/qdl-v2"
RECEIPTS = [STATE / "p184-final-campaign-20261018T053803Z", STATE / "p184-final-campaign-20261017T135719Z"]
D3_BASELINE = STATE / "p184-d3-baseline-20261009"
BUDGET = Path("config/v2/p184-stage100-budget.json")


def seq(mark_ts_ms, index_ts_ms, mark_rx_ns, index_rx_ns):
    return f"{mark_ts_ms}:{index_ts_ms}:{mark_rx_ns}:{index_rx_ns}:aa:bb"


def paired_event(i, *, gap_ms=1200, lag_ms=40, internal_ms=80):
    """One OKX paired event: the index is gap_ms older than the mark; CreateTime internal_ms after the newest."""
    mark_ts = (W0 // MS) + i * 1000
    index_ts = mark_ts - gap_ms
    mark_rx = mark_ts * MS + lag_ms * MS
    index_rx = index_ts * MS + 2 * lag_ms * MS
    produced_ms = (max(mark_rx, index_rx) + internal_ms * MS) // MS
    return mark_ts, index_ts, mark_rx, index_rx, produced_ms


class ObserverV6PairedSplitTests(unittest.TestCase):
    def test_paired_split_is_create_time_minus_the_newest_receipt(self):
        mark_ts, index_ts, mark_rx, index_rx, produced = paired_event(1)
        split = observer.paired_split(seq(mark_ts, index_ts, mark_rx, index_rx), produced, mark_rx)
        self.assertEqual(split["internal_newest_receipt_to_producer_ts_ms"], 80.0)
        self.assertEqual(split["component_ts_gap_ms"], 1200.0)
        self.assertEqual(split["older_component"], "index")
        self.assertEqual((split["mark_venue_to_dl_ms"], split["index_venue_to_dl_ms"]), (40.0, 80.0))
        self.assertFalse(split["published_mismatch"])
        self.assertTrue(observer.paired_split(seq(mark_ts, index_ts, mark_rx, index_rx), produced,
                                              index_rx)["published_mismatch"])
        self.assertIsNone(observer.paired_split("345089622445", produced))  # a quote sequence is not a pair
        self.assertIsNone(observer.paired_split("1:2:x:4", produced))
        self.assertIsNone(observer.paired_split(seq(mark_ts, index_ts, mark_rx, index_rx), None)
                          ["internal_newest_receipt_to_producer_ts_ms"])

    def _window(self, paired_seq=True):
        window = observer.WindowAggregator(120)
        window.begin(W0)
        for i in range(20):
            mark_ts, index_ts, mark_rx, index_rx, produced = paired_event(i, internal_ms=60 + i)
            seen = produced * MS + 20 * MS
            window.observe("mark_index_price", "OKX", None, "BTC-USDT-SWAP", index_ts * MS, index_rx, produced, seen,
                           True, lambda: {"kind": "canonical_observation", "identity": {"native_symbol": "BTC"}},
                           source_sequence=seq(mark_ts, index_ts, mark_rx, index_rx) if paired_seq else "77",
                           published_ns=max(mark_rx, index_rx))
            window.observe("trade", "OKX", None, "BTC-USDT-SWAP", W0 + i * S, W0 + i * S + 5 * MS, None,
                           W0 + i * S + 30 * MS, True, lambda: {"kind": "canonical_observation"},
                           source_sequence="123", published_ns=None)
        return window

    def test_only_the_paired_row_carries_the_split_and_other_rows_keep_the_v5_shape(self):
        snapshot = json.loads(json.dumps(self._window().snapshot(W0 + 120 * S, W0 + 120 * S, final=True)))
        self.assertEqual(set(snapshot["rows"]["trade|OKX|"]), {"admitted", "missing_clock", "all_events_visibility_ms",
                                                               "all_events_missing_clock", "raw_samples", "outliers"})
        self.assertIn("paired_split", snapshot["rows"][ROW])
        summary = observer.summarize_aggregate(snapshot)
        self.assertNotIn("paired_split", summary["trade|OKX|"])
        internal = summary[ROW]["paired_split"][campaign.G4_AMENDMENT_QUANTITY]
        self.assertEqual((internal["n"], internal["max"]), (20, 79.0))
        self.assertEqual(summary[ROW]["paired_split"]["component_ts_gap_ms"]["max"], 1200.0)
        self.assertEqual(summary[ROW]["paired_split"]["older_component"], {"index": 20})
        self.assertEqual(len(summary[ROW]["paired_split"]["internal_outliers"]), observer.OUTLIER_K)
        self.assertEqual(summary[ROW]["paired_split"]["internal_outliers"][0]["paired_split"]
                         [campaign.G4_AMENDMENT_QUANTITY], 79.0)
        # The composite (frozen-table quantity) is unchanged by v6.
        self.assertEqual(summary[ROW]["visibility_ms"]["n"], 20)

    def test_unparsable_sequences_are_counted_never_guessed(self):
        summary = observer.summarize_aggregate(json.loads(json.dumps(
            self._window(paired_seq=False).snapshot(W0, W0, final=True))))
        self.assertEqual(summary[ROW]["paired_split"]["paired_missing"], 20)
        self.assertEqual(summary[ROW]["paired_split"][campaign.G4_AMENDMENT_QUANTITY]["n"], 0)


def amendment(**over):
    out = {"schema": campaign.G4_AMENDMENT_SCHEMA, "amendment_id": "G4-A1", "row": ROW,
           "frozen_before_candidate": True, "frozen_at_unix": 100.0, "decision_owner": "Bobby (owner)",
           "base_budget_sha256": "b" * 64, "base_addendum_sha256": "a" * 64,
           "pinned": {"gate_campaign_tool": {"new_sha256": "t" * 64}},
           "rule": {"quantity": campaign.G4_AMENDMENT_QUANTITY, "factor": campaign.G4_AMENDMENT_FACTOR,
                    "max_min_samples_per_window": campaign.G4_AMENDMENT_MIN_SAMPLES},
           "limits": {"candidate_p95_limit_ms": 200.9, "candidate_max_limit_ms": 993.4}}
    out.update(over)
    return out


def stats(n, p95, mx, lower=None):
    return {"n": n, "p95": p95, "p95_bucket_lower": p95 - 1 if lower is None else lower, "max": mx,
            "percentile_basis": "BUCKET_UPPER_BOUND"}


def amended_window(*, composite=(900, 1632.0, 2177.0), internal=(900, 90.0, 160.0), internal_lower=None,
                   probe_covered=True, split=True, paired_missing=0, other_p95=100.0):
    block = {"visibility_ms": stats(composite[0], composite[1], composite[2])}
    if split:
        block["paired_split"] = {campaign.G4_AMENDMENT_QUANTITY: stats(*internal, lower=internal_lower),
                                 "component_ts_gap_ms": stats(900, 1591.0, 1932.0), "paired_missing": paired_missing,
                                 "published_mismatch": 0, "missing": {}, "internal_outliers": []}
    window = {"summary": {ROW: block, "trade|OKX|": {"visibility_ms": stats(500, other_p95, other_p95 + 50)}},
              "inverse_rows": {}}
    window["okx_probe"] = {"covered": probe_covered, "coverage_problems": [] if probe_covered else ["x"]}
    return window


TABLE = {ROW: {"candidate_p95_limit_ms": 1586.6}, "trade|OKX|": {"candidate_p95_limit_ms": 257.4}}


class AmendedRowScoringTests(unittest.TestCase):
    def score(self, windows, amend=None):
        return campaign.score_g4(windows, TABLE, None, amend or amendment())

    def row(self, windows):
        return {r["row"]: r for r in self.score(windows)[1]}[ROW]

    def test_composite_over_the_frozen_limit_passes_on_the_internal_split_and_is_reported(self):
        state, rows, why = self.score([amended_window(), amended_window(composite=(990, 1232.0, 2052.0))])
        self.assertEqual(state, "PASS", why)
        row = {r["row"]: r for r in rows}[ROW]
        self.assertEqual((row["verdict"], row["basis"], row["p95_ms_per_window"]), ("PASS", "amendment", [90.0, 90.0]))
        self.assertEqual(row["report_only"]["composite_over_frozen_limit_per_window"], [True, False])
        self.assertEqual(row["report_only"]["composite_visibility_ms_per_window"][0]["p95"], 1632.0)
        self.assertEqual(row["report_only"]["component_ts_gap_ms_per_window"][0]["max"], 1932.0)
        self.assertEqual(row["attribution_per_window"], ["EXTERNAL_VENUE_COMPONENT_CADENCE"] * 2)

    def test_negative_internal_split_over_the_p95_limit_fails(self):
        row = self.row([amended_window(), amended_window(internal=(900, 232.0, 400.0), internal_lower=230.0)])
        self.assertEqual(row["verdict"], "FAIL")
        self.assertEqual(row["attribution_per_window"][1], "INTERNAL_OVER_OR_UNRESOLVED")
        self.assertEqual(self.score([amended_window(internal=(900, 232.0, 400.0), internal_lower=230.0)])[0], "FAIL")

    def test_negative_internal_max_over_the_limit_fails_on_a_window_with_five_samples(self):
        self.assertEqual(self.row([amended_window(internal=(900, 90.0, 1200.0))])["verdict"], "FAIL")
        thin = self.row([amended_window(internal=(4, 90.0, 1200.0))])
        self.assertEqual((thin["verdict"], thin["max_gated_windows"]), ("PASS", [False]))

    def test_straddled_p95_bucket_is_unresolved(self):
        row = self.row([amended_window(internal=(900, 201.5, 300.0), internal_lower=200.0)])
        self.assertEqual((row["verdict"], row["unresolved_windows"]), ("NOT_MEASURED", [True]))

    def test_unknown_attribution_is_not_external(self):
        state, rows, why = self.score([amended_window(), amended_window(probe_covered=False)])
        row = {r["row"]: r for r in rows}[ROW]
        self.assertEqual((state, row["verdict"]), ("NOT_MEASURED", "NOT_MEASURED"))
        self.assertEqual(row["attribution_per_window"][1], "UNKNOWN")
        self.assertTrue(any("UNKNOWN (not EXTERNAL)" in w for w in why))
        no_probe = amended_window()
        del no_probe["okx_probe"]
        self.assertEqual(self.row([no_probe])["verdict"], "NOT_MEASURED")

    def test_a_v5_window_without_the_split_or_with_unparsed_events_is_not_measured(self):
        self.assertEqual(self.row([amended_window(split=False)])["verdict"], "NOT_MEASURED")
        self.assertEqual(self.row([amended_window(paired_missing=3)])["verdict"], "NOT_MEASURED")

    def test_every_other_row_is_byte_identical_with_and_without_the_amendment(self):
        for windows in ([amended_window()], [amended_window(other_p95=300.0)]):
            with_amend = self.score(windows)[1]
            without = campaign.score_g4(windows, TABLE, None)[1]
            self.assertEqual(json.dumps([r for r in with_amend if r["row"] != ROW], sort_keys=True),
                             json.dumps([r for r in without if r["row"] != ROW], sort_keys=True))
        self.assertEqual(campaign.score_g4([amended_window()], TABLE), campaign.score_g4([amended_window()], TABLE,
                                                                                          None, None))

    def test_validation(self):
        self.assertEqual(campaign.validate_g4_amendment(amendment(), TABLE), [])
        self.assertTrue(campaign.validate_g4_amendment(amendment(row="quote|OKX|"), TABLE))
        self.assertTrue(campaign.validate_g4_amendment(amendment(rule={"quantity": "visibility_ms", "factor": 1.2,
                                                                       "max_min_samples_per_window": 5}), TABLE))
        self.assertTrue(campaign.validate_g4_amendment(amendment(limits={}), TABLE))
        self.assertTrue(campaign.validate_g4_amendment(amendment(decision_owner=None), TABLE))


class ApplicabilityTests(unittest.TestCase):
    def _campaign(self, tmp, body, **over):
        path = Path(tmp) / "amendment.json"
        path.write_text(json.dumps(body))
        record = {"g4_amendment_path": str(path), "g4_amendment_sha256": campaign.sha256_file(path),
                  "budget_sha256": "b" * 64, "g4_addendum_sha256": "a" * 64, "tool_sha256": "t" * 64,
                  "started_unix": 200.0}
        record.update(over)
        return record

    def test_applies_only_when_recorded_unchanged_frozen_before_and_pinned(self):
        self.assertEqual(campaign.applicable_g4_amendment({}, TABLE), (None, [], None))  # older campaigns
        with tempfile.TemporaryDirectory() as tmp:
            applied, notes, src = campaign.applicable_g4_amendment(self._campaign(tmp, amendment()), TABLE)
            self.assertEqual((applied["amendment_id"], notes), ("G4-A1", []))
            self.assertEqual(len(src["sha256"]), 64)
            for over, needle in (({"tool_sha256": "x" * 64}, "campaign tool"),
                                 ({"g4_addendum_sha256": "x" * 64}, "different addendum"),
                                 ({"budget_sha256": "x" * 64}, "different budget"),
                                 ({"started_unix": 50.0}, "not frozen before"),
                                 ({"g4_amendment_sha256": "x" * 64}, "changed after")):
                applied, notes, _ = campaign.applicable_g4_amendment(self._campaign(tmp, amendment(), **over), TABLE)
                self.assertIsNone(applied)
                self.assertIn(needle, notes[0])
            gone = self._campaign(tmp, amendment())
            Path(gone["g4_amendment_path"]).unlink()
            self.assertIn("missing", campaign.applicable_g4_amendment(gone, TABLE)[1][0])

    def test_command_plan_runs_one_probe_per_window_under_the_guard_prefix(self):
        import argparse
        args = argparse.Namespace(state_tools="/tools", load_timeout=2400, budget="b.json", writer="w1", g11=False,
                                  g11_wait=900, baseline=False, sessions=100, okx_probe=True)
        plan = {"binding": None, "market_redis": {"container": "r", "db": 1},
                "heartbeat_redis": {"container": "h", "db": 0}}
        d = Path("/state/p184-final-campaign-X")
        windows = campaign.command_plan(args, plan, d, 0.0)["g4_windows"]
        self.assertTrue(windows)
        for w in windows:
            self.assertTrue(w["okx_probe_container"].startswith(f"qdl-p184-g4obs-{d.name}-"))  # guard: campaign
            command = w["okx_probe"]
            self.assertEqual(command[command.index("--output") + 1], str(d / f"g4-{w['index']}-okx-probe"))
            self.assertEqual(int(command[command.index("--seconds") + 1]), int(w["seconds"])
                             + campaign.G4_OKX_PROBE_LEAD_SECONDS + campaign.G4_OKX_PROBE_TAIL_SECONDS)
        args.baseline = True
        self.assertTrue(all("okx_probe" not in w for w in campaign.command_plan(args, plan, d, 0.0)["g4_windows"]))


@unittest.skipUnless(all((d / "scorecard.json").exists() for d in RECEIPTS), "existing receipts not on this host")
class ExistingReceiptParityTests(unittest.TestCase):
    """053803Z / 135719Z: nothing recorded -> the scorer output is the current scorer's; with the amendment forced,
    every other row is byte-identical and the amended row is NOT_MEASURED (v5 windows carry no paired split)."""

    @classmethod
    def setUpClass(cls):
        # Mirrored receipts: the run worktrees named in their campaign.json were removed (p184_receipt_support).
        cls.tmp = tempfile.TemporaryDirectory()
        cls.dirs = [mirror(d, Path(cls.tmp.name)) for d in RECEIPTS]

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_g4_gate_identical_to_the_stored_scorecard(self):
        for d in self.dirs:
            stored = {g["id"]: g for g in json.loads((d / "scorecard.json").read_text())["gates"]}["G4"]
            fresh = {g["id"]: g for g in json.loads(json.dumps(campaign.build_scorecard(d)["gates"],
                                                                default=str))}["G4"]
            self.assertEqual(as_recorded(json.dumps(fresh, sort_keys=True), d), json.dumps(stored, sort_keys=True),
                             d.name)

    def test_forced_amendment_changes_only_the_amended_row(self):
        for d in self.dirs:
            c = json.loads((d / "campaign.json").read_text())
            budget = json.loads(Path(c["budget_path"]).read_text())
            table = budget["g4_delivery_visibility"]["table"]
            addendum, _, _ = campaign.applicable_g4_addendum(c, table)
            windows = []
            for item in c["g4_windows"]:
                obs = d / f"g4-{item['index']}" / "observations.jsonl"
                windows.append({"summary": json.loads((d / f"g4-{item['index']}-summary.json").read_text()),
                                "inverse_rows": campaign.g4_inverse_rows(read_jsonl(obs)), "okx_probe": None})
            base_state, base_rows, _ = campaign.score_g4(windows, table, addendum)
            state, rows, _ = campaign.score_g4(windows, table, addendum, amendment())
            self.assertEqual(json.dumps([r for r in rows if r["row"] != ROW], sort_keys=True),
                             json.dumps([r for r in base_rows if r["row"] != ROW], sort_keys=True), d.name)
            self.assertEqual({r["row"]: r for r in rows}[ROW]["verdict"], "NOT_MEASURED")


def write_baseline(tmp, records_per_window, *, break_one=False):
    """A synthetic frozen baseline: summaries hashed into the budget, composite p95 recomputed into its table."""
    d = Path(tmp) / "baseline"
    p95s, sources = [], []
    for index, internals in enumerate(records_per_window, start=1):
        (d / f"g4-{index}").mkdir(parents=True)
        lines = []
        for i, internal in enumerate(internals):
            mark_ts, index_ts, mark_rx, index_rx, produced = paired_event(i, internal_ms=internal)
            sequence = seq(mark_ts, index_ts, mark_rx, index_rx)
            if break_one and index == 2 and i == 0:
                sequence = "not-a-pair"
            lines.append(json.dumps({"kind": "canonical_observation", "feed": "mark_index_price",
                                     "identity": {"venue": "OKX", "source_sequence": sequence},
                                     "source_event_ns": index_ts * MS, "received_at_ns": index_rx,
                                     "published_at_ns": max(mark_rx, index_rx), "producer_timestamp_ms": produced,
                                     "observer_receipt_wall_ns": produced * MS + 20 * MS, "interval": None}))
        (d / f"g4-{index}" / "observations.jsonl").write_text("\n".join(lines) + "\n")
        summary = observer._summarize_records(lines)
        (d / f"g4-{index}-summary.json").write_text(json.dumps(summary))
        p95s.append(summary[ROW]["visibility_ms"]["p95"])
        sources.append(freeze.sha256(d / f"g4-{index}-summary.json"))
    budget = {"frozen_before_candidate": True, "sources": {"g4": sources},
              "g4_delivery_visibility": {"table": {ROW: {"baseline_p95_ms_windows": p95s,
                                                         "candidate_p95_limit_ms": round(max(p95s) * 1.2, 1)}}}}
    return d, budget


class BaselineRecomputeTests(unittest.TestCase):
    def test_limits_come_from_the_same_records_split(self):
        with tempfile.TemporaryDirectory() as tmp:
            d, budget = write_baseline(tmp, [[50 + i for i in range(40)], [60 + i for i in range(40)] + [500]])
            base = freeze.recompute_baseline(d, budget)
        self.assertEqual(base["p95s"], [88.0, 98.0])  # index int(0.95 n) of the sorted split, as the frozen table
        self.assertEqual(base["maxes"], [89.0, 500.0])
        self.assertEqual(base["limits"], {"candidate_p95_limit_ms": 117.6, "candidate_max_limit_ms": 600.0})

    def test_insufficient_or_foreign_baseline_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            d, budget = write_baseline(tmp, [[50] * 10, [60] * 10], break_one=True)
            with self.assertRaisesRegex(ValueError, "insufficient per-event baseline"):
                freeze.recompute_baseline(d, budget)
        with tempfile.TemporaryDirectory() as tmp:
            d, budget = write_baseline(tmp, [[50] * 10])
            budget["sources"]["g4"] = ["0" * 64]
            with self.assertRaisesRegex(ValueError, "not the budget's frozen baseline"):
                freeze.recompute_baseline(d, budget)
        with tempfile.TemporaryDirectory() as tmp:
            d, budget = write_baseline(tmp, [[50] * 10])
            budget["g4_delivery_visibility"]["table"][ROW]["baseline_p95_ms_windows"] = [1.0]
            with self.assertRaisesRegex(ValueError, "not the gated population"):
                freeze.recompute_baseline(d, budget)
        with tempfile.TemporaryDirectory() as tmp:
            d, budget = write_baseline(tmp, [[50] * 3])
            with self.assertRaisesRegex(ValueError, "< 5"):
                freeze.recompute_baseline(d, budget)

    @unittest.skipUnless((D3_BASELINE / "g4-1" / "observations.jsonl").exists(), "frozen baseline not on this host")
    def test_frozen_baseline_recompute(self):
        base = freeze.recompute_baseline(D3_BASELINE, json.loads(BUDGET.read_text()))
        self.assertEqual([w["composite_visibility_ms"]["p95"] for w in base["windows"]], [1322.2, 1289.4, 1296.2, 1195.2])
        self.assertEqual(base["p95s"], [167.4, 143.8, 139.1, 138.2])
        self.assertEqual(base["maxes"], [292.1, 245.8, 182.5, 827.8])
        self.assertEqual(base["limits"], {"candidate_p95_limit_ms": 200.9, "candidate_max_limit_ms": 993.4})


class VenueProbeTests(unittest.TestCase):
    CHRONY = ("Reference ID    : A9FEA97B (169.254.169.123)\nSystem time     : 0.000000330 seconds slow of NTP time\n"
              "Root delay      : 0.000235456 seconds\nRoot dispersion : 0.000174014 seconds\n")

    def test_chrony_bound(self):
        clock = probe.parse_chrony(self.CHRONY)
        self.assertEqual(clock["offset_ms"], -0.00033)
        self.assertAlmostEqual(clock["bound_ms"], 0.00033 + 0.174014 + 0.235456 / 2, places=6)
        self.assertIsNone(probe.parse_chrony("nothing"))

    def rows(self, start, end, skip_index=None):
        out = []
        t = start - 2 * S
        while t <= end + 2 * S:
            for mark in probe.MARK:
                out.append(json.dumps({"r": t, "c": "mark-price", "i": mark, "ts": t // MS - 40}))
                if (t - start) % (1 * S) == 0 and probe.INDEX_OF[mark] != skip_index:
                    out.append(json.dumps({"r": t, "c": "index-tickers", "i": probe.INDEX_OF[mark],
                                           "ts": t // MS - 700}))
            t += 200 * MS
        return out + [json.dumps({"kind": "done", "at_ns": end + 3 * S})]

    def test_analyze_cadence_gap_and_coverage(self):
        start, end = W0, W0 + 120 * S
        out = probe.analyze(self.rows(start, end), start, end)
        self.assertTrue(out["covered"], out["coverage_problems"])
        self.assertEqual(out["mark-price"]["ts_interval_ms"]["p50"], 200)
        self.assertEqual(out["index-tickers"]["ts_interval_ms"]["p50"], 1000)
        self.assertEqual(out["mark-price"]["recv_minus_venue_ts_ms"]["max"], 40.0)
        self.assertGreater(out["pair_ts_gap_at_push_ms"]["max"], 600)
        missing = probe.analyze(self.rows(start, end, skip_index="BTC-USD"), start, end)
        self.assertFalse(missing["covered"])
        late = probe.analyze(self.rows(start + 30 * S, end), start, end)
        self.assertFalse(late["covered"])
        dropped = probe.analyze(self.rows(start, end) + [json.dumps({"kind": "disconnect", "at_ns": start + S})],
                                start, end)
        self.assertFalse(dropped["covered"])

    def test_probe_window_needs_a_clock_bound(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "probe.jsonl").write_text("\n".join(self.rows(W0, W0 + 120 * S)) + "\n")
            (d / "receipt.json").write_text(json.dumps({"exit": 0, "host_clock_bound_ms_max": 0.3}))
            agg = [{"kind": "aggregate", "final": True, "observed_start_ns": W0, "observed_end_ns": W0 + 120 * S}]
            self.assertTrue(campaign.okx_probe_window(d, copy.deepcopy(agg))["covered"])
            (d / "receipt.json").write_text(json.dumps({"exit": 0, "host_clock_bound_ms_max": None}))
            self.assertFalse(campaign.okx_probe_window(d, copy.deepcopy(agg))["covered"])
            self.assertIsNone(campaign.okx_probe_window(d / "absent", agg))


if __name__ == "__main__":
    unittest.main()
