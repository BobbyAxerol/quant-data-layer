"""P18.4 stage-100 amendment A5: the G11 first-valid-apply clock of a TRADE slice starts at max(fault cleared, DL Kafka
time of its first canonical TRADE event after the fault - 3 s guard), with canonical-topic evidence.

Negatives first: an earlier post-fault TRADE event the writer did not apply in time -> FAIL; an event inside the
pre-fault guard -> frozen clock; missing / incomplete / non-matching canonical evidence -> A5 not applied to the slice;
caught-up, rewind, cursor and non-TRADE slices keep the frozen oracle; a changed A5 file, an unnamed run that started
before the freeze, changed pinned receipts or evidence. Parity: on every stored G11 summary A5 without evidence gives the
frozen verdict and recovery; on stored receipts the rescore changes no gate and G11 keeps its verdict; on 040311Z only
G11 and only OKX|BNB-USDT-SWAP|TRADE change. Host-runnable (stdlib only)."""
from __future__ import annotations

import copy
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from scripts import p184_a5_g11_trade_clock as a5
from scripts import p184_g11_writer_restart_probe as g11probe
from scripts import p184_gate_campaign as gc
from scripts.p184_resp_readonly import quantiles
from tests.p184_receipt_support import mirror

STATE = Path.home() / ".local/state/qdl-v2"
COUNTED = STATE / "p184-final-campaign-20261019T040311Z"
SLICE = "OKX|BNB-USDT-SWAP|TRADE|None"
OTHER_TRADE = "OKX|BTC-USDT-SWAP|TRADE|None"
BOOK = "OKX|BNB-USDT-SWAP|BOOK_SNAPSHOT|None"
OUTAGE, CLEARED = 1_000_000, 1_000_113


def report(first: dict, caught: dict | None = None, *, rewinds: int = 0, ahead: bool = False) -> dict:
    caught = caught or first
    per = {n: {"first_valid_apply_s": first[n], "caught_up_s": caught[n], "rewinds": [], "future_events": 0,
               "invalid_post": {}, "body_reads": 0} for n in first}
    return {"schema": g11probe.SCHEMA, "observe_seconds": 180.0, "unresolved_slices": [],
            "restart": {"outage_start_ms": OUTAGE, "fault_cleared_ms": CLEARED},
            "recovery": {"first_valid_apply_s": quantiles(list(first.values())),
                         "caught_up_s": quantiles(list(caught.values())), "slices_without_first_apply": [],
                         "slices_not_caught_up": [], "per_slice": per, "rewinds_total": rewinds,
                         "future_events_total": 0},
            "cursor_samples": [{"compared": 2, "unmatched": 0, "ahead": [{"slice": SLICE}] if ahead else [],
                                "missing_expected": []}]}


def progress(name: str, first_s: float, event_ms: int) -> dict:
    at = CLEARED + round(first_s * 1000)
    return {"kind": "slice_progress", "slice": name, "at_ms": at, "first_apply_ms": at, "caught_up_ms": at,
            "event_ms": event_ms}


def trade(ts_ms: int, trade_id: int, offset: int, partition: int = 2) -> dict:
    return {"kind": "trade", "partition": partition, "offset": offset, "kafka_ts_ms": ts_ms,
            "src_ns": (ts_ms - 50) * 1_000_000, "recv_ns": (ts_ms - 25) * 1_000_000, "pub_ns": (ts_ms - 25) * 1_000_000,
            "trade_id": str(trade_id), "seq": str(trade_id), "conn_gen": 210, "flags": []}


def quote(ts_ms: int, offset: int, partition: int = 2) -> dict:
    return {"kind": "quote", "partition": partition, "offset": offset, "kafka_ts_ms": ts_ms,
            "src_ns": ts_ms * 1_000_000, "recv_ns": ts_ms * 1_000_000, "pub_ns": ts_ms * 1_000_000}


def read(records: list[dict], *, done=(0, 1, 2), errors=0, counts=True) -> dict:
    lines = [{"kind": "assignment", "partition": p, "start": 1, "low": 0, "high": 9} for p in (0, 1, 2)] + records
    if errors:
        lines += [{"kind": "error", "code": -1}] * errors
    if counts:
        lines.append({"kind": "counts", "records_scanned": 99, "per_symbol_feed": {}, "done_partitions": list(done)})
    out = {"launch": None, "assignments": {}, "trades": [], "records": [], "counts": None, "errors": []}
    for item in lines:
        if item["kind"] == "assignment":
            out["assignments"][item["partition"]] = item
        elif item["kind"] in ("trade", "quote"):
            out["records"].append(item)
            if item["kind"] == "trade":
                out["trades"].append(item)
        elif item["kind"] == "counts":
            out["counts"] = item
        else:
            out["errors"].append(item)
    return out


def quiet_records(extra=()):
    """Last pre-fault trade 6 s before the outage, then silence until cleared + 36.0 s, quotes throughout."""
    first_post = CLEARED + 36_000
    return [quote(OUTAGE - 10_000, 1), trade(OUTAGE - 6_000, 100, 2), *extra, trade(first_post, 101, 50),
            quote(CLEARED + 40_000, 60)]


def judge(rep, obs, evidence):
    amended, records = a5.amend_report(rep, obs, evidence)
    return g11probe.verdict(amended), {r["slice"]: r for r in records}, amended


QUIET = report({SLICE: 36.415, OTHER_TRADE: 2.4, BOOK: 3.9})
QUIET_OBS = [progress(SLICE, 36.415, CLEARED + 36_000 - 50), progress(OTHER_TRADE, 2.4, CLEARED + 2_000),
             progress(BOOK, 3.9, CLEARED + 3_000)]


class SliceRuleTests(unittest.TestCase):
    def test_frozen_oracle_fails_the_quiet_slice(self):
        self.assertEqual(g11probe.verdict(QUIET)[0], "FAIL")

    def test_quiet_slice_is_timed_from_its_first_post_fault_event(self):
        (state, why), records, amended = judge(QUIET, QUIET_OBS, {SLICE: (read(quiet_records()), [])})
        self.assertEqual((state, why), ("PASS", []))
        r = records[SLICE]
        self.assertTrue(r["applied"])
        self.assertEqual((r["clock_start_basis"], r["clock_start_ms"]), ("first_trade_event", CLEARED + 36_000))
        self.assertEqual(r["first_valid_apply_s"], 0.415)
        self.assertEqual(r["frozen_first_valid_apply_s"], 36.415)
        self.assertTrue(r["venue_trade_ids_adjacent_report_only"])
        self.assertEqual(amended["recovery"]["first_valid_apply_s"]["max"], 3.9)
        self.assertFalse(records[OTHER_TRADE]["applied"])  # no evidence: frozen clock kept
        self.assertEqual(amended["recovery"]["per_slice"][OTHER_TRADE]["first_valid_apply_s"], 2.4)

    def test_an_earlier_unapplied_post_fault_event_fails(self):
        records = quiet_records(extra=[trade(CLEARED + 2_000, 99, 10)])  # printed 2 s after the restart, not applied
        (state, why), slices, _ = judge(QUIET, QUIET_OBS, {SLICE: (read(records), [])})
        self.assertEqual(state, "FAIL")
        self.assertTrue(slices[SLICE]["applied"])
        self.assertEqual(slices[SLICE]["clock_start_ms"], CLEARED + 2_000)
        self.assertEqual(slices[SLICE]["first_valid_apply_s"], 34.415)
        self.assertIn("first valid apply max 34.415 s > 30 s", why)

    def test_an_event_committed_during_the_outage_starts_the_clock_at_fault_cleared(self):
        records = quiet_records(extra=[trade(OUTAGE + 50, 99, 10)])
        (state, _), slices, _ = judge(QUIET, QUIET_OBS, {SLICE: (read(records), [])})
        self.assertEqual((state, slices[SLICE]["clock_start_basis"]), ("FAIL", "fault_cleared"))
        self.assertEqual(slices[SLICE]["first_valid_apply_s"], 36.415)

    def test_an_event_inside_the_pre_fault_guard_starts_the_clock_at_fault_cleared(self):
        records = quiet_records(extra=[trade(OUTAGE - 1_000, 99, 10)])  # may not have been applied before the stop
        (state, _), slices, _ = judge(QUIET, QUIET_OBS, {SLICE: (read(records), [])})
        self.assertEqual((state, slices[SLICE]["clock_start_basis"]), ("FAIL", "fault_cleared"))

    def test_missing_or_incomplete_evidence_is_not_applied(self):
        cases = {"no evidence": None,
                 "no counts record": read(quiet_records(), counts=False),
                 "partition not done": read(quiet_records(), done=(0, 1)),
                 "read error": read(quiet_records(), errors=1),
                 "coverage starts inside the guard": read([r for r in quiet_records()
                                                           if r["kafka_ts_ms"] > OUTAGE - 3_000]),
                 "coverage ends before the first apply": read([r for r in quiet_records()
                                                               if r["kafka_ts_ms"] <= CLEARED + 36_000]),
                 "no post-fault trade": read([r for r in quiet_records() if r.get("trade_id") != "101"])}
        for name, evidence in cases.items():
            (state, _), slices, amended = judge(QUIET, QUIET_OBS, {SLICE: (evidence, [])} if evidence else {})
            self.assertEqual(state, "FAIL", name)
            self.assertFalse(slices[SLICE]["applied"], name)
            self.assertTrue(slices[SLICE]["problems"], name)
            self.assertEqual(amended["recovery"]["per_slice"][SLICE]["first_valid_apply_s"], 36.415, name)
        pinned = judge(QUIET, QUIET_OBS, {SLICE: (read(quiet_records()), ["canonical evidence differs"])})
        self.assertEqual((pinned[0][0], pinned[1][SLICE]["applied"]), ("FAIL", False))

    def test_the_applied_event_must_be_a_canonical_post_fault_event(self):
        obs = [progress(SLICE, 36.415, CLEARED + 35_000), *QUIET_OBS[1:]]  # an event the canonical read does not hold
        (state, _), slices, _ = judge(QUIET, obs, {SLICE: (read(quiet_records()), [])})
        self.assertEqual((state, slices[SLICE]["applied"]), ("FAIL", False))
        self.assertIn("is not a canonical post-fault TRADE event", " ".join(slices[SLICE]["problems"]))

    def test_other_checks_keep_the_frozen_oracle(self):
        evidence = {SLICE: (read(quiet_records()), [])}
        late = report({SLICE: 36.415, OTHER_TRADE: 2.4, BOOK: 3.9}, {SLICE: 61.0, OTHER_TRADE: 2.4, BOOK: 3.9})
        self.assertIn("caught up max 61.0 s > 60 s", judge(late, QUIET_OBS, evidence)[0][1])
        self.assertIn("2 rewinds", judge(report({SLICE: 36.415, BOOK: 3.9}, rewinds=2), QUIET_OBS, evidence)[0][1])
        self.assertIn("committed cursor ahead of applied state",
                      judge(report({SLICE: 36.415, BOOK: 3.9}, ahead=True), QUIET_OBS, evidence)[0][1])
        book_late = report({SLICE: 36.415, BOOK: 31.0})
        self.assertIn("first valid apply max 31.0 s > 30 s", judge(book_late, QUIET_OBS, evidence)[0][1])
        self.assertEqual(a5.amend_report({"restart": None, "schema": g11probe.SCHEMA}, [], evidence)[1], [])


def amendment(**over):
    frozen = json.loads(a5.A5_PATH.read_text())
    frozen.update(over)
    return frozen


class ValidationTests(unittest.TestCase):
    def test_frozen_file_states_the_rule_and_pins_the_working_code(self):
        frozen = json.loads(a5.A5_PATH.read_text())
        self.assertEqual(a5.validate_a5(frozen), [])
        for name, path in a5.CODE_PINS.items():
            self.assertEqual(frozen["pinned"][name]["sha256"], gc.sha256_file(gc.ROOT / path), name)
        self.assertEqual(frozen["pinned"]["scorer"]["sha256"],
                         json.loads(gc.A3_PATH.read_text())["pinned"]["gate_campaign_tool"]["new_sha256"])
        self.assertEqual([p["campaign_id"] for p in frozen["post_hoc_applications"]], [COUNTED.name])

    def test_changed_files_are_refused(self):
        rule = amendment()["rule"]
        bad = [{"schema": "x"}, {"version": 2}, {"decision_owner": None}, {"frozen_at_unix": None},
               {"rule": {**rule, "pre_fault_guard_ms": 0}}, {"rule": {**rule, "first_valid_apply_le_s": 40.0}},
               {"rule": {**rule, "feeds": ["TRADE", "QUOTE"]}}, {"rule": {**rule, "unchanged": ["rewinds"]}},
               {"pinned": {}}, {"post_hoc_applications": [{"campaign_id": COUNTED.name, "label": "PRE_REGISTERED",
                                                           "post_hoc": True}]},
               {"pre_registered": {"label": "POST_HOC_OWNER_DECISION", "post_hoc": False,
                                   "evidence_dir_suffix": ".g11-canonical"}}]
        for over in bad:
            self.assertTrue(a5.validate_a5(amendment(**over)), over)

    def test_wrapper_is_removed_after_use(self):
        before = g11probe.verdict
        with a5.patched_verdict(lambda r: ("PASS", [])):
            self.assertIsNot(g11probe.verdict, before)
        self.assertIs(g11probe.verdict, before)


SUMMARIES = sorted(STATE.glob("p184-final-campaign-*/g11/g11-summary.json"))


@unittest.skipUnless(SUMMARIES, "stored G11 summaries not on this host")
class StoredSummaryParityTests(unittest.TestCase):
    def test_without_evidence_a5_reproduces_every_frozen_g11_verdict(self):
        for path in SUMMARIES:
            stored = json.loads(path.read_text())
            obs_path = path.parent / "g11-observations.jsonl"
            obs = [json.loads(line) for line in obs_path.read_text().splitlines() if line.strip()] \
                if obs_path.exists() else []
            amended, records = a5.amend_report(stored, obs, {})
            self.assertEqual(g11probe.verdict(amended), g11probe.verdict(stored), path)
            self.assertFalse(any(r["applied"] for r in records), path)
            if stored.get("schema") == g11probe.SCHEMA and stored.get("restart"):
                self.assertEqual(amended["recovery"]["first_valid_apply_s"], stored["recovery"]["first_valid_apply_s"],
                                 path)


RECEIPTS = [STATE / n for n in ("p184-final-campaign-20261018T053803Z", "p184-final-campaign-20261017T135719Z",
                                "p184-final-campaign-20261018T074624Z", "p184-final-campaign-20261018T092255Z",
                                "p184-final-campaign-20261018T123222Z", "p184-final-campaign-20261019T021044Z")]


def strip_paths(value):
    if isinstance(value, dict):
        return {k: strip_paths(v) for k, v in value.items() if k != "path"}
    if isinstance(value, list):
        return [strip_paths(v) for v in value]
    return value


@unittest.skipUnless(all((d / "scorecard.json").exists() for d in RECEIPTS), "stored receipts not on this host")
class ReceiptParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.dirs = [mirror(d, Path(cls.tmp.name)) for d in RECEIPTS]
        cls.results = {d.name: a5.rescore(d, force_report_only=True) for d in cls.dirs}

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_frozen_gates_reproduced_and_a5_changes_no_verdict_without_evidence(self):
        for d in RECEIPTS:
            stored = json.loads((d / "scorecard.json").read_text())
            result = self.results[d.name]
            self.assertEqual(json.dumps(strip_paths(result["frozen"]["gates"]), sort_keys=True),
                             json.dumps(strip_paths(stored["gates"]), sort_keys=True), d.name)
            frozen = {g["id"]: g for g in result["frozen"]["gates"]}
            card = {g["id"]: g for g in result["card"]["gates"]}
            for gid in frozen:
                if gid != "G11":
                    self.assertEqual(a5.canonical(frozen[gid]), a5.canonical(card[gid]), (d.name, gid))
            self.assertEqual((card["G11"]["verdict"], card["G11"]["reason"]),
                             (frozen["G11"]["verdict"], frozen["G11"]["reason"]), d.name)
            self.assertEqual(result["card"]["summary"], result["frozen"]["summary"], d.name)
            self.assertTrue(result["report_only"], d.name)

    def test_a5_is_not_applied_to_unnamed_runs_from_before_the_freeze(self):
        for d in self.dirs:
            result = a5.rescore(d)
            self.assertFalse(result["applied"], d.name)
            self.assertIsNone(result["card"], d.name)
            self.assertTrue(any("did not start after the A5 freeze" in p for p in result["problems"]), d.name)


@unittest.skipUnless((COUNTED / "scorecard.json").exists()
                     and Path(str(COUNTED) + ".g11-forensic").exists(), "040311Z receipts not on this host")
class CountedRunTests(unittest.TestCase):
    def test_040311z_g11_under_a5(self):
        result = a5.rescore(COUNTED)
        self.assertTrue(result["applied"], result["problems"])
        card, frozen = result["card"], result["frozen"]
        gates = {g["id"]: g for g in card["gates"]}
        frozen_gates = {g["id"]: g for g in frozen["gates"]}
        self.assertEqual(a5.canonical(frozen), a5.canonical(json.loads((COUNTED / "scorecard.json").read_text())))
        self.assertEqual((gates["G11"]["verdict"], gates["G11"]["a5"]["verdict_without_a5"]), ("PASS", "FAIL"))
        self.assertEqual(gates["G11"]["a5"]["reason_without_a5"], "first valid apply max 36.415 s > 30 s")
        self.assertEqual((card["a5"]["label"], card["a5"]["post_hoc"]), ("POST_HOC_OWNER_DECISION", True))
        applied = {r["slice"]: r for r in result["slices"] if r["applied"]}
        self.assertEqual(list(applied), [SLICE])
        r = applied[SLICE]
        self.assertEqual((r["frozen_first_valid_apply_s"], r["first_valid_apply_s"]), (36.415, 0.413))
        self.assertEqual((r["last_pre_fault_event"]["trade_id"], r["first_post_fault_event"]["trade_id"]),
                         ("241971262", "241971263"))
        self.assertEqual((r["post_fault_events_before_first_apply"], r["venue_trade_ids_adjacent_report_only"]),
                         (1, True))
        self.assertEqual(gates["G11"]["a5"]["first_valid_apply_s_with_a5"]["max"], 4.928)
        for gid in frozen_gates:
            if gid != "G11":
                self.assertEqual(a5.canonical(frozen_gates[gid]), a5.canonical(gates[gid]), gid)
        self.assertEqual(card["summary"].get("FAIL"), None)
        self.assertEqual(card["a5"]["summary_without_a5"]["FAIL"], ["G11"])

    def test_changed_or_missing_evidence_is_not_applied(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp) / COUNTED.name
            d.mkdir()
            for item in COUNTED.iterdir():
                (d / item.name).symlink_to(item)
            evidence = Path(str(d) + ".g11-forensic")
            shutil.copytree(Path(str(COUNTED) + ".g11-forensic"), evidence)
            lines = (evidence / "canonical.jsonl").read_text().splitlines()
            (evidence / "canonical.jsonl").write_text("\n".join(lines[:-1]) + "\n")  # drop the counts record
            result = a5.rescore(d)
            self.assertFalse(result["applied"])
            self.assertIn(f"{SLICE}: canonical evidence differs from the one A5 pins", result["problems"])
            forced = a5.rescore(d, force_report_only=True)
            gates = {g["id"]: g for g in forced["card"]["gates"]}
            self.assertEqual(gates["G11"]["verdict"], "FAIL")
            self.assertFalse(any(r["applied"] for r in forced["slices"]))
            shutil.rmtree(evidence)
            result = a5.rescore(d)
            self.assertFalse(result["applied"])
            self.assertIn("the canonical reader differs from the one A5 pins", result["problems"])


if __name__ == "__main__":
    unittest.main()
