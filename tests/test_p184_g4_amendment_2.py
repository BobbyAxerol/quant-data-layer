"""P18.4 G4 amendment 2 (owner B1 2026-10-18: bar >= 8h without a frozen baseline is REPORT_ONLY), its re-pin of
amendment 1, parity of every other row on the 053803Z / 135719Z / 074624Z receipts, and the report-only OKX books
fresh-connection probe + ingestor netns TCP sampler (no Kafka, no container)."""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts import p184_freeze_g4_amendment_2 as freeze2
from scripts import p184_g4_okx_books_probe as books
from scripts import p184_gate_campaign as campaign
from scripts.p184_resp_readonly import read_jsonl
from tests.test_p184_g4_amendment import TABLE, amendment, stats

STATE = Path.home() / ".local/state/qdl-v2"
RECEIPTS = [STATE / "p184-final-campaign-20261018T053803Z", STATE / "p184-final-campaign-20261017T135719Z",
            STATE / "p184-final-campaign-20261018T074624Z"]
S = 1_000_000_000
MS = 1_000_000


def amendment2(**over):
    out = {"schema": campaign.G4_AMENDMENT_2_SCHEMA, "amendment_id": campaign.G4_AMENDMENT_2_ID,
           "frozen_before_candidate": True, "decision_owner": "owner", "frozen_at_unix": 100.0,
           "base_budget_sha256": "b" * 64, "base_addendum_sha256": "a" * 64,
           "inherits": {"g4_amendment_1_sha256": None},
           "pinned": {"gate_campaign_tool": {"new_sha256": "n" * 64}},
           "rule": {"feed": "bar", "report_only_min_interval_ms": campaign.G4_B1_MIN_INTERVAL_MS,
                    "applies_to": "rows observed without a frozen baseline (not in the table, not in the addendum)",
                    "threshold_changes": []}}
    out.update(over)
    return out


def window(**rows):
    return {"summary": {key: {"visibility_ms": value} for key, value in rows.items()}, "inverse_rows": {}}


class LongBarRowTests(unittest.TestCase):
    def test_intervals(self):
        for key in ("bar|BINANCE|8h", "bar|OKX|12h", "bar|OKX|1d", "bar|OKX|2d", "bar|OKX|3d", "bar|BINANCE|1w"):
            self.assertTrue(campaign.g4_long_bar_row(key), key)
        for key in ("bar|BINANCE|6h", "bar|OKX|4h", "bar|OKX|1m", "bar|OKX|1M", "bar|OKX|", "trade|OKX|8h",
                    "bar|OKX|7h59m", "book_delta|OKX_INVERSE|"):
            self.assertFalse(campaign.g4_long_bar_row(key), key)


class AmendmentTwoScoringTests(unittest.TestCase):
    TABLE = {"trade|OKX|": {"candidate_p95_limit_ms": 257.4}}

    def test_long_bar_rows_become_report_only_and_nothing_else_moves(self):
        windows = [window(**{"trade|OKX|": stats(500, 100.0, 150.0), "bar|BINANCE|8h": stats(5, 11000.0, 11698.0),
                             "bar|OKX|7m": stats(5, 900.0, 950.0)})]
        state, rows, why = campaign.score_g4(windows, self.TABLE)
        self.assertEqual(state, "NOT_MEASURED")
        self.assertIn("observed rows without a frozen baseline: bar|BINANCE|8h, bar|OKX|7m", why)
        state2, rows2, why2 = campaign.score_g4(windows, self.TABLE, None, None, amendment2())
        self.assertEqual(state2, "NOT_MEASURED")  # bar|OKX|7m is still unbaselined: B1 covers >= 8h only
        self.assertIn("observed rows without a frozen baseline: bar|OKX|7m", why2)
        b1 = {r["row"]: r for r in rows2}["bar|BINANCE|8h"]
        self.assertEqual((b1["verdict"], b1["basis"], b1["max_ms_per_window"], b1["n_per_window"]),
                         ("REPORT_ONLY", "amendment_2", [11698.0], [5]))
        self.assertEqual(json.dumps(rows, sort_keys=True), json.dumps(rows2[:len(rows)], sort_keys=True))
        only_long = [window(**{"trade|OKX|": stats(500, 100.0, 150.0), "bar|BINANCE|8h": stats(5, 1.0, 2.0)})]
        self.assertEqual(campaign.score_g4(only_long, self.TABLE, None, None, amendment2())[0], "PASS")
        failing = [window(**{"trade|OKX|": stats(500, 300.0, 350.0, lower=299.0), "bar|BINANCE|8h": stats(5, 1, 2)})]
        self.assertEqual(campaign.score_g4(failing, self.TABLE, None, None, amendment2())[0], "FAIL")

    def test_validation(self):
        self.assertEqual(campaign.validate_g4_amendment_2(amendment2(inherits={"g4_amendment_1_sha256": "x"})), [])
        for over in ({"schema": "x"}, {"rule": {**amendment2()["rule"], "report_only_min_interval_ms": 3_600_000}},
                     {"rule": {**amendment2()["rule"], "threshold_changes": ["x"]}}, {"decision_owner": None},
                     {"inherits": {}}):
            self.assertTrue(campaign.validate_g4_amendment_2(amendment2(**over)), over)


class AmendmentTwoApplicabilityTests(unittest.TestCase):
    def campaign_record(self, tmp, a1_over=None, a2_over=None, **over):
        a1 = Path(tmp) / "a1.json"
        a1.write_text(json.dumps(amendment(**(a1_over or {}))))
        a1_sha = campaign.sha256_file(a1)
        a2 = Path(tmp) / "a2.json"
        a2.write_text(json.dumps(amendment2(inherits={"g4_amendment_1_sha256": a1_sha}, **(a2_over or {}))))
        record = {"g4_amendment_path": str(a1), "g4_amendment_sha256": a1_sha,
                  "g4_amendment_2_path": str(a2), "g4_amendment_2_sha256": campaign.sha256_file(a2),
                  "budget_sha256": "b" * 64, "g4_addendum_sha256": "a" * 64, "tool_sha256": "n" * 64,
                  "started_unix": 200.0}
        record.update(over)
        return record

    def test_amendment_1_applies_through_the_re_pin_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            record = self.campaign_record(tmp)
            a2, notes, _ = campaign.applicable_g4_amendment_2(record)
            self.assertEqual((a2["amendment_id"], notes), ("G4-A2", []))
            a1, notes, _ = campaign.applicable_g4_amendment(record, TABLE)  # A1 pins "t"*64, campaign tool is "n"*64
            self.assertEqual((a1["amendment_id"], notes), ("G4-A1", []))
            without = dict(record)
            for key in ("g4_amendment_2_path", "g4_amendment_2_sha256"):
                without.pop(key)
            self.assertIsNone(campaign.applicable_g4_amendment(without, TABLE)[0])
            for over, needle in (({"tool_sha256": "x" * 64}, "campaign tool"),
                                 ({"g4_addendum_sha256": "x" * 64}, "different addendum"),
                                 ({"budget_sha256": "x" * 64}, "different budget"),
                                 ({"started_unix": 50.0}, "not frozen before"),
                                 ({"g4_amendment_2_sha256": "x" * 64}, "changed after")):
                bad = self.campaign_record(tmp, **over)
                a2, notes, _ = campaign.applicable_g4_amendment_2(bad)
                self.assertIsNone(a2)
                self.assertIn(needle, notes[0])
                if "tool_sha256" not in over and "budget_sha256" not in over and "g4_addendum_sha256" not in over \
                        and "started_unix" not in over:
                    self.assertIsNone(campaign.applicable_g4_amendment(bad, TABLE)[0])  # re-pin needs a valid A2
            other_a1 = self.campaign_record(tmp)
            other_a1["g4_amendment_sha256"] = "y" * 64
            self.assertIn("different amendment 1", campaign.applicable_g4_amendment_2(other_a1)[1][0])
            self.assertEqual(campaign.applicable_g4_amendment_2({}), (None, [], None))


def _git_head_available() -> bool:
    """The freeze tool reads the base ref's file hashes through git; CI images carry no git binary."""
    return shutil.which("git") is not None and subprocess.run(
        ["git", "-C", str(campaign.ROOT), "cat-file", "-e", "HEAD^{commit}"], capture_output=True).returncode == 0


@unittest.skipUnless(_git_head_available(), "git binary or checkout unavailable: freeze() reads the base ref via git")
class AmendmentTwoFreezeTests(unittest.TestCase):
    def test_freeze_pins_the_working_tool_and_inherits_the_unchanged_amendment_1(self):
        out = freeze2.freeze(base_ref="HEAD", now=123.0)
        self.assertEqual(campaign.validate_g4_amendment_2(out), [])
        self.assertEqual(out["pinned"]["gate_campaign_tool"]["new_sha256"],
                         campaign.sha256_file(campaign.ROOT / "scripts/p184_gate_campaign.py"))
        self.assertEqual(out["inherits"]["g4_amendment_1_sha256"], campaign.sha256_file(campaign.G4_AMENDMENT_PATH))
        self.assertEqual(out["frozen_at"], "1970-01-01T00:02:03Z")


@unittest.skipUnless(all((d / "scorecard.json").exists() for d in RECEIPTS), "existing receipts not on this host")
class ExistingReceiptParityTests(unittest.TestCase):
    """Nothing recorded -> every gate of the stored scorecard is reproduced byte-identically by this tool; with
    amendment 2 forced every row except the B1 rows is byte-identical (074624Z gains only bar|BINANCE|8h)."""

    def test_every_gate_identical_to_the_stored_scorecard(self):
        for d in RECEIPTS:
            stored = json.loads((d / "scorecard.json").read_text())["gates"]
            fresh = json.loads(json.dumps(campaign.build_scorecard(d)["gates"], default=str))
            self.assertEqual(json.dumps(fresh, sort_keys=True), json.dumps(stored, sort_keys=True), d.name)

    def test_forced_amendment_2_changes_only_long_bar_rows(self):
        added = {}
        for d in RECEIPTS:
            c = json.loads((d / "campaign.json").read_text())
            table = json.loads(Path(c["budget_path"]).read_text())["g4_delivery_visibility"]["table"]
            addendum, _, _ = campaign.applicable_g4_addendum(c, table)
            a1, _, _ = campaign.applicable_g4_amendment(c, table)
            windows = []
            for item in c["g4_windows"]:
                obs = d / f"g4-{item['index']}" / "observations.jsonl"
                windows.append({"summary": json.loads((d / f"g4-{item['index']}-summary.json").read_text()),
                                "inverse_rows": campaign.g4_inverse_rows(read_jsonl(obs))})
                if a1 is not None:
                    windows[-1]["okx_probe"] = campaign.okx_probe_window(d / f"g4-{item['index']}-okx-probe",
                                                                         read_jsonl(obs))
            base_state, base_rows, base_why = campaign.score_g4(windows, table, addendum, a1)
            state, rows, why = campaign.score_g4(windows, table, addendum, a1, amendment2())
            b1 = [r for r in rows if r.get("basis") == "amendment_2"]
            self.assertEqual(json.dumps([r for r in rows if r.get("basis") != "amendment_2"], sort_keys=True),
                             json.dumps(base_rows, sort_keys=True), d.name)
            self.assertEqual(state, base_state, d.name)
            added[d.name] = [r["row"] for r in b1]
        self.assertEqual(added, {RECEIPTS[0].name: [], RECEIPTS[1].name: [], RECEIPTS[2].name: ["bar|BINANCE|8h"]})


class CommandPlanTests(unittest.TestCase):
    def test_one_books_probe_per_window_under_the_guard_prefix(self):
        args = argparse.Namespace(state_tools="/tools", load_timeout=2400, budget="b.json", writer="w1", g11=False,
                                  g11_wait=900, baseline=False, sessions=100, okx_probe=True)
        plan = {"binding": None, "market_redis": {"container": "r", "db": 1},
                "heartbeat_redis": {"container": "h", "db": 0}}
        d = Path("/state/p184-final-campaign-X")
        windows = campaign.command_plan(args, plan, d, 0.0)["g4_windows"]
        for w in windows:
            self.assertTrue(w["okx_books_probe_container"].startswith(f"qdl-p184-g4obs-{d.name}-"))
            command = w["okx_books_probe"]
            self.assertIn("scripts/p184_g4_okx_books_probe.py", command)
            self.assertEqual(command[command.index("--output") + 1], str(d / f"g4-{w['index']}-okx-books"))
            self.assertEqual(int(command[command.index("--seconds") + 1]), int(w["seconds"])
                             + campaign.G4_OKX_PROBE_LEAD_SECONDS + campaign.G4_OKX_PROBE_TAIL_SECONDS)
        args.baseline = True
        self.assertTrue(all("okx_books_probe" not in w for w in campaign.command_plan(args, plan, d, 0.0)["g4_windows"]))


NETSTAT = ("TcpExt: TCPOFOQueue TCPRcvQDrop TCPTimeouts\nTcpExt: 3 0 1\n"
           "IpExt: InOctets OutOctets\nIpExt: 100 200\n")
SNMP = "Ip: Forwarding\nIp: 1\nTcp: RetransSegs InSegs\nTcp: 38295 1000\n"
TCP = ("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
       "   0: 0100007F:9C40 0A000001:01BB 01 00000000:00000010 00:00000000 00000000 10001 0 1\n"
       "   1: 0100007F:9C41 0A000001:20FB 01 00000000:00000000 00:00000000 00000000 10001 0 2\n"
       "   2: 0100007F:9C42 0A000001:01BB 0A 00000000:00000000 00:00000000 00000000 10001 0 3\n")


class BooksProbeTests(unittest.TestCase):
    def frame(self, inst, ts, seq, prev, action="update"):
        return (f'{{"arg":{{"channel":"books","instId":"{inst}"}},"action":"{action}","data":[{{"asks":'
                f'[["70000.1","5","0","2"]],"bids":[],"ts":"{ts}","checksum":-1,"prevSeqId":{prev},"seqId":{seq}}}]}}'
                ).encode()

    def test_frame_keeps_no_payload_values(self):
        row = books.parse_frame(self.frame("BTC-USD-SWAP", 1792310000123, 11, 10), 1792310000150 * MS)
        self.assertEqual(row, {"r": 1792310000150 * MS, "i": "BTC-USD-SWAP", "a": "update", "ts": 1792310000123,
                               "seq": 11, "prev": 10, "b": len(self.frame("BTC-USD-SWAP", 1792310000123, 11, 10))})
        self.assertNotIn("70000", json.dumps(row))
        self.assertIsNone(books.parse_frame(b'{"event":"subscribe"}', 1))

    def rows(self, start_ms, seconds, *, lag_ms=30, late=None, break_at=None):
        lines = []
        for inst in books.INSTRUMENTS:
            seq = 1
            for k in range(seconds * 10):
                ts = start_ms + k * 100
                lag = lag_ms if not late or not late[0] <= ts < late[1] else 400
                prev = seq - (2 if break_at == k else 1)
                lines.append(json.dumps({"r": (ts + lag) * MS, "i": inst, "a": "update", "ts": ts, "seq": seq,
                                         "prev": prev, "b": 900}))
                seq += 1
        return lines

    def test_analyze_coverage_lag_and_seq_breaks(self):
        start = 1792310000000
        lines = self.rows(start, 30, break_at=50)
        out = books.analyze(lines, start * MS, (start + 29_000) * MS)
        self.assertTrue(out["covered"], out["coverage_problems"])
        block = out["instruments"]["BTC-USD-SWAP"]
        self.assertEqual((block["recv_minus_venue_ts_ms"]["max"], block["seq_breaks"]), (30.0, 1))
        short = books.analyze(lines + [json.dumps({"kind": "disconnect", "at_ns": (start + 1000) * MS})],
                              start * MS, (start + 29_000) * MS)
        self.assertFalse(short["covered"])
        late_edge = books.analyze(lines, start * MS, (start + 60_000) * MS)
        self.assertFalse(late_edge["covered"])

    def test_proc_parsers_and_tcp_analysis(self):
        counters = {**books.parse_proc_counters(NETSTAT), **books.parse_proc_counters(SNMP)}
        self.assertEqual(counters["TcpExt:TCPOFOQueue"], 3)
        self.assertEqual(counters["Tcp:RetransSegs"], 38295)
        self.assertNotIn("Ip:Forwarding", counters)
        sockets = books.parse_proc_tcp(TCP)
        self.assertEqual(sockets, [{"local_port": 0x9C40, "tx_queue": 0, "rx_queue": 16}])
        self.assertEqual(books.counter_delta({"a": 1, "b": 2}, {"a": 1, "b": 5, "c": 9}), {"b": 3})
        start = 1792310000 * S
        lines = [json.dumps({"kind": "tcp", "at_ns": start + k * S, "delta": {"TcpExt:TCPOFOQueue": 1} if k == 3 else {},
                             "sockets": 5, "rx_queue_max": 16 if k in (4, 6) else 0, "tx_queue_max": 0,
                             "rx_persist_samples_max": 3 if k == 4 else 1 if k == 6 else 0,
                             "rx_queued_ports": []}) for k in range(31)]
        out = books.analyze_tcp(lines, start, start + 30 * S)
        self.assertTrue(out["covered"])
        self.assertEqual(out["counter_totals"]["TcpExt:TCPOFOQueue"], 1)
        self.assertEqual(out["rx_queued_seconds"], [start + 4 * S])  # k == 6: one sample only, not a stall
        self.assertEqual(out["rx_nonempty_seconds"], 2)
        self.assertFalse(books.analyze_tcp(lines[:5], start, start + 30 * S)["covered"])

    def test_sampler_reads_a_netns_through_proc(self):
        with tempfile.TemporaryDirectory() as tmp:
            net = Path(tmp) / "42" / "net"
            net.mkdir(parents=True)
            (net / "netstat").write_text(NETSTAT)
            (net / "snmp").write_text(SNMP)
            (net / "tcp").write_text(TCP)
            import threading
            lines, stop = [], threading.Event()
            sampler = books.TcpSampler(42, lambda d: (lines.append(d), stop.set() if d.get("kind") == "tcp" else None),
                                       stop, proc_root=tmp)
            self.assertEqual(sampler.counters()["TcpExt:TCPTimeouts"], 1)
            self.assertEqual(sampler.sockets()[0]["rx_queue"], 16)
            sampler.run()  # one second of 10 Hz samples of a socket that keeps 16 unread bytes
            tcp = [d for d in lines if d["kind"] == "tcp"][0]
            self.assertEqual((tcp["rx_queue_max"], tcp["sockets"], tcp["delta"]), (16, 1, {}))
            self.assertGreaterEqual(tcp["rx_persist_samples_max"], books.RX_PERSIST_SAMPLES)
            self.assertEqual(lines[-1]["kind"], "tcp_done")

    def test_tail_attribution_classes(self):
        start = 1792310000000
        healthy = books.read_rows(self.rows(start, 30))[0]
        late = books.read_rows(self.rows(start, 30, late=(start + 10_000, start + 11_000)))[0]
        sample = {"source_event_ns": (start + 10_400) * MS, "received_at_ns": (start + 10_760) * MS,
                  "feed": "book_delta", "identity": {"native_symbol": "BTC-USD-SWAP",
                                                     "source_session_id": "okx-public-001-67018-1"}}
        tcp_clean = [json.dumps({"kind": "tcp", "at_ns": (start + 10_000) * MS + k * S, "delta": {},
                                 "rx_queue_max": 0}) for k in range(3)]
        tcp_queued = [json.dumps({"kind": "tcp", "at_ns": (start + 10_000) * MS + k * S, "delta": {},
                                  "rx_queue_max": 4096 if k == 1 else 0,
                                  "rx_persist_samples_max": 4 if k == 1 else 0}) for k in range(3)]
        tcp_blip = [json.dumps({"kind": "tcp", "at_ns": (start + 10_000) * MS + k * S, "delta": {},
                                "rx_queue_max": 195, "rx_persist_samples_max": 1}) for k in range(3)]
        cases = ((healthy, tcp_clean, "HEALTHY", "CLEAN", "EXTERNAL_SESSION_UPSTREAM"),
                 (late, tcp_clean, "LATE", "CLEAN", "COMMON_VENUE_DELAY"),
                 (healthy, tcp_queued, "HEALTHY", "RX_QUEUED", "INTERNAL_RECEIVE_QUEUE"),
                 (healthy, tcp_blip, "HEALTHY", "CLEAN", "EXTERNAL_SESSION_UPSTREAM"),
                 (healthy, [], "HEALTHY", "NO_COVERAGE", "UNKNOWN"),
                 ([], tcp_clean, "NO_COVERAGE", "CLEAN", "UNKNOWN"))
        for rows, tcp, probe_state, host, attribution in cases:
            out = books.tail_attribution(sample, rows, tcp)
            self.assertEqual((out["probe_state"], out["host_receive_queue"], out["attribution"]),
                             (probe_state, host, attribution))
        self.assertEqual(books.tail_attribution(sample, healthy, tcp_clean)["lane_venue_to_dl_ms"], 360.0)

    def test_window_report_attributes_okx_book_outliers_only(self):
        start = 1792310000000
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "books.jsonl").write_text("\n".join(self.rows(start, 30)) + "\n")
            (d / "tcp.jsonl").write_text("\n".join(json.dumps({"kind": "tcp", "at_ns": start * MS + k * S, "delta": {},
                                                               "rx_queue_max": 0, "sockets": 5})
                                                   for k in range(31)) + "\n")
            tail = {"source_event_ns": (start + 10_400) * MS, "received_at_ns": (start + 10_760) * MS,
                    "identity": {"native_symbol": "BTC-USD-SWAP"}, "feed": "book_delta"}
            fast = {"source_event_ns": (start + 10_400) * MS, "received_at_ns": (start + 10_430) * MS,
                    "identity": {"native_symbol": "BTC-USD-SWAP"}, "feed": "book_delta"}
            aggregate = {"observed_start_ns": start * MS, "observed_end_ns": (start + 29_000) * MS,
                         "rows": {"book_delta|OKX|": {"outliers": [tail, fast]}, "trade|OKX|": {"outliers": [tail]}}}
            out = books.window_report(d, aggregate)
            self.assertEqual(len(out["book_tails"]), 1)
            self.assertEqual(out["book_tails"][0]["attribution"], "EXTERNAL_SESSION_UPSTREAM")
            self.assertTrue(out["books"]["covered"] and out["tcp"]["covered"])
            self.assertIsNone(books.window_report(d / "absent", aggregate))


if __name__ == "__main__":
    unittest.main()
