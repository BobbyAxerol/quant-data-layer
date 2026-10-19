"""P18.4 A3 report-only Binance USD-M fresh-connection probe: frame regexes, per-second aggregation, depth
continuity, read_aggregates merge, analyze coverage, probe_state classes, window_report and run_plan argv invariants
(no network, no container)."""
from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from scripts import p184_g4_binance_probe as probe
from scripts import p184_g4_okx_books_probe as books

S = 1_000_000_000
MS = 1_000_000
BASE_S = 1_792_300_000  # an arbitrary venue second

BOOK_TICKER = (b'{"e":"bookTicker","u":8822354685013,"s":"BTCUSDT","b":"104512.30","B":"3.512","a":"104512.40",'
               b'"A":"7.021","T":1792300000120,"E":1792300000123}')
TRADE = (b'{"e":"trade","E":1792300000250,"T":1792300000248,"s":"ETHUSDT","t":6011223344,"p":"3812.51",'
         b'"q":"0.250","X":"MARKET","m":true}')
DEPTH = (b'{"e":"depthUpdate","E":1792300000400,"T":1792300000398,"s":"BTCUSDT","U":8822354685100,'
         b'"u":8822354685120,"pu":8822354685099,"b":[["104512.30","3.512"],["104512.20","0.010"]],'
         b'"a":[["104512.40","7.021"]]}')
MARK = (b'{"e":"markPriceUpdate","E":1792300001000,"s":"SOLUSDT","p":"187.12000000","ap":"187.10000000",'
        b'"P":"187.25000000","i":"187.14000000","r":"0.00010000","T":1792310400000}')
ACK = b'{"result":null,"id":1}'
ERROR = b'{"error":{"code":2,"msg":"Invalid request: unknown stream"},"id":2}'


def frame(channel, sym, e_ms, t_ms=None, **ids):
    return {"c": channel, "i": sym, "E": e_ms, "T": t_ms, "U": ids.get("U"), "u": ids.get("u"),
            "pu": ids.get("pu")}


def agg(c, i, s, lag=20.0, n=5, lag_t=None, seq_breaks=0):
    return {"k": "agg", "conn": probe.CONN_OF[c], "c": c, "i": i, "s": s, "n": n, "lag_e_max_ms": lag,
            "lag_e_min_ms": lag / 2, "lag_t_max_ms": lag_t, "recv_first_ns": s * S + 10 * MS,
            "recv_last_ns": s * S + 900 * MS, "seq_breaks": seq_breaks}


def subscribed(at_ns):
    return [{"kind": "subscribed", "conn": conn, "at_ns": at_ns} for conn in probe.SUBSCRIPTIONS]


def full_window(first_s, last_s, lag=20.0):
    rows = []
    for conn, channels in probe.SUBSCRIPTIONS.items():
        for channel, syms in channels.items():
            for sym in syms:
                rows.extend(agg(channel, sym, s, lag=lag) for s in range(first_s, last_s + 1))
    return rows


def lines(*items):
    return [json.dumps(x) for x in items]


class FrameTests(unittest.TestCase):
    def test_event_ids_match_the_canonical_source_sequence_and_read_events_keeps_first_receipt(self):
        self.assertEqual(probe.parse_frame(DEPTH)["id"], 8822354685120)
        self.assertEqual(probe.parse_frame(MARK)["id"], 1792300001000)
        self.assertIsInstance(probe.parse_frame(TRADE)["id"], int)
        ev = lines({"k": "ev", "c": "depth", "i": "BTCUSDT", "id": 5, "E": 1, "T": 1, "r": 20},
                   {"k": "ev", "c": "depth", "i": "BTCUSDT", "id": 5, "E": 1, "T": 1, "r": 10},
                   {"k": "agg", "c": "depth", "i": "BTCUSDT", "s": 1, "n": 1, "seq_breaks": 0})
        self.assertEqual(probe.read_events(ev), {("depth", "BTCUSDT", 5): {"r": 10, "E": 1, "T": 1}})
        rows, events = probe.read_aggregates(ev)
        self.assertEqual((len(rows), events), (1, []))  # per-event lines are neither aggregates nor events

    def test_four_event_types_keep_ids_and_times_only(self):
        bt = probe.parse_frame(BOOK_TICKER)
        self.assertEqual(bt, {"c": "bookTicker", "i": "BTCUSDT", "E": 1792300000123, "T": 1792300000120,
                              "U": None, "u": 8822354685013, "pu": None, "id": 8822354685013})
        # bookTicker's `u` is DL's quote source_sequence (A3 same-event join), not a depth continuity id
        self.assertEqual(probe.parse_frame(TRADE)["c"], "trade")
        self.assertEqual(probe.parse_frame(TRADE)["T"], 1792300000248)
        depth = probe.parse_frame(DEPTH)
        self.assertEqual((depth["c"], depth["i"], depth["U"], depth["u"], depth["pu"]),
                         ("depth", "BTCUSDT", 8822354685100, 8822354685120, 8822354685099))
        mark = probe.parse_frame(MARK)
        self.assertEqual((mark["c"], mark["i"], mark["E"], mark["T"]), ("markPrice", "SOLUSDT", 1792300001000, None))
        for parsed in (bt, depth, mark):
            self.assertFalse({"p", "q", "b", "a", "r"} & set(parsed))
            self.assertNotIn("104512", json.dumps(parsed))

    def test_ack_and_error_are_not_frames(self):
        self.assertIsNone(probe.parse_frame(ACK))
        self.assertIsNone(probe.error_code(ACK))
        self.assertIsNone(probe.parse_frame(ERROR))
        self.assertEqual(probe.error_code(ERROR), "2")
        self.assertEqual(probe.error_code(b'{"code": -1121, "msg": "Invalid symbol.", "id": 3}'), "-1121")
        self.assertIsNone(probe.error_code(TRADE))
        self.assertIsNone(probe.parse_frame(b'{"e":"aggTrade","E":1792300000250,"s":"ETHUSDT"}'))

    def test_streams_use_the_ingestor_native_spelling(self):
        self.assertEqual(probe.streams("market"), [f"{s.lower()}@markPrice@1s" for s in probe.SYMBOLS])
        public = probe.streams("public")
        self.assertIn("btcusdt@depth@100ms", public)
        self.assertIn("dogeusdt@bookTicker", public)
        self.assertIn("bnbusdt@trade", public)
        self.assertNotIn("solusdt@depth@100ms", public)
        self.assertEqual(len(public), 12)
        self.assertEqual(probe.URLS, {"public": "wss://fstream.binance.com/public/ws",
                                      "market": "wss://fstream.binance.com/market/ws"})


class AggregatorTests(unittest.TestCase):
    def test_per_second_aggregation_and_flush(self):
        emitted = []
        a = probe.Aggregator(emitted.append)
        e0 = BASE_S * 1000
        a.add("public", frame("bookTicker", "BTCUSDT", e0 + 100, e0 + 98), (BASE_S * S) + 130 * MS)
        a.add("public", frame("bookTicker", "BTCUSDT", e0 + 500, e0 + 499), (BASE_S * S) + 560 * MS)
        a.add("public", frame("bookTicker", "BTCUSDT", e0 + 1200, e0 + 1199), (BASE_S * S) + 1210 * MS)
        a.add("market", frame("markPrice", "BTCUSDT", e0 + 1000), (BASE_S * S) + 1005 * MS)
        self.assertEqual(a.flush((BASE_S + 4) * S + 999 * MS), 0)
        self.assertEqual(a.flush((BASE_S + 5) * S), 1)  # second BASE_S + 5 s
        row = emitted[0]
        self.assertEqual((row["k"], row["conn"], row["c"], row["i"], row["s"], row["n"]),
                         ("agg", "public", "bookTicker", "BTCUSDT", BASE_S, 2))
        self.assertAlmostEqual(row["lag_e_max_ms"], 60.0)
        self.assertAlmostEqual(row["lag_e_min_ms"], 30.0)
        self.assertAlmostEqual(row["lag_t_max_ms"], 61.0)
        self.assertEqual((row["recv_first_ns"], row["recv_last_ns"]), (BASE_S * S + 130 * MS, BASE_S * S + 560 * MS))
        # a late frame for the flushed second becomes another row for the same key
        a.add("public", frame("bookTicker", "BTCUSDT", e0 + 900, e0 + 899), (BASE_S + 5) * S + 100 * MS)
        self.assertEqual(a.flush(0, everything=True), 3)
        mark = [r for r in emitted if r["c"] == "markPrice"][0]
        self.assertIsNone(mark["lag_t_max_ms"])
        rows, _ = probe.read_aggregates(lines(*emitted))
        first = [r for r in rows if r["c"] == "bookTicker" and r["s"] == BASE_S][0]
        self.assertEqual(first["n"], 3)
        self.assertAlmostEqual(first["lag_e_max_ms"], 4200.0)  # recv 5.1 s - E 0.9 s
        self.assertAlmostEqual(first["lag_e_min_ms"], 30.0)
        self.assertEqual(first["recv_last_ns"], (BASE_S + 5) * S + 100 * MS)

    def test_depth_continuity_breaks(self):
        emitted = []
        a = probe.Aggregator(emitted.append)
        e0 = BASE_S * 1000
        recv = BASE_S * S
        a.add("public", frame("depth", "BTCUSDT", e0 + 100, U=101, u=110, pu=99), recv)       # first: no check
        a.add("public", frame("depth", "BTCUSDT", e0 + 200, U=111, u=120, pu=110), recv)      # continuous
        a.add("public", frame("depth", "BTCUSDT", e0 + 300, U=131, u=140, pu=130), recv)      # break (130 != 120)
        a.add("public", frame("depth", "ETHUSDT", e0 + 300, U=1, u=5, pu=0), recv)            # other symbol
        a.add("public", frame("depth", "BTCUSDT", e0 + 1100, U=141, u=150, pu=140), recv)     # next second, ok
        a.add("public", frame("depth", "BTCUSDT", e0 + 1200, U=161, u=170, pu=155), recv)     # break
        a.flush(0, everything=True)
        by = {(r["i"], r["s"]): r["seq_breaks"] for r in emitted}
        self.assertEqual(by, {("BTCUSDT", BASE_S): 1, ("ETHUSDT", BASE_S): 0, ("BTCUSDT", BASE_S + 1): 1})

    def test_read_aggregates_merges_rows_and_keeps_events(self):
        first = agg("depth", "BTCUSDT", BASE_S, lag=40.0, n=4, lag_t=38.0, seq_breaks=1)
        late = {**agg("depth", "BTCUSDT", BASE_S, lag=7000.0, n=1, lag_t=None, seq_breaks=2),
                "lag_e_min_ms": 6999.0, "recv_first_ns": BASE_S * S + 5, "recv_last_ns": (BASE_S + 7) * S}
        other = agg("depth", "ETHUSDT", BASE_S)
        rows, events = probe.read_aggregates(lines({"kind": "done", "at_ns": 9}, first, other, late,
                                                   {"kind": "subscribed", "conn": "public", "at_ns": 1}) + [""])
        self.assertEqual([e["kind"] for e in events], ["subscribed", "done"])
        self.assertEqual(len(rows), 2)
        merged = rows[0]
        self.assertEqual((merged["i"], merged["n"], merged["seq_breaks"]), ("BTCUSDT", 5, 3))
        self.assertEqual((merged["lag_e_max_ms"], merged["lag_e_min_ms"], merged["lag_t_max_ms"]), (7000.0, 20.0, 38.0))
        self.assertEqual((merged["recv_first_ns"], merged["recv_last_ns"]), (BASE_S * S + 5, (BASE_S + 7) * S))


class AnalyzeTests(unittest.TestCase):
    start, end = BASE_S * S + 200 * MS, (BASE_S + 60) * S + 300 * MS

    def test_covered(self):
        out = probe.analyze(lines(*subscribed(BASE_S * S - S), *full_window(BASE_S - 2, BASE_S + 62),
                                  {"kind": "done", "at_ns": (BASE_S + 63) * S}), self.start, self.end)
        self.assertTrue(out["report_only"])
        self.assertTrue(out["covered"], out["coverage_problems"])
        block = out["channels"]["bookTicker"]["BTCUSDT"]
        self.assertEqual((block["conn"], block["seconds"], block["n"], block["interior_seconds_missing"]),
                         ("public", 61, 305, 0))
        self.assertEqual(block["lag_e_ms"]["max"], 20.0)
        self.assertIsNone(out["channels"]["trade"]["ETHUSDT"]["interior_seconds_missing"])
        self.assertEqual(set(out["channels"]), {"bookTicker", "trade", "depth", "markPrice"})
        self.assertEqual(set(out["channels"]["depth"]), {"BTCUSDT", "ETHUSDT"})

    def test_missing_edge(self):
        rows = [r for r in full_window(BASE_S - 2, BASE_S + 62)
                if not (r["c"] == "markPrice" and r["i"] == "DOGEUSDT" and r["s"] > BASE_S + 50)]
        out = probe.analyze(lines(*subscribed(0), *rows), self.start, self.end)
        self.assertFalse(out["covered"])
        self.assertEqual(out["coverage_problems"], ["markPrice DOGEUSDT: rows do not reach both window edges"])
        self.assertEqual(out["channels"]["markPrice"]["DOGEUSDT"]["interior_seconds_missing"], 9)

    def test_disconnect_before_end(self):
        out = probe.analyze(lines(*subscribed(0), *full_window(BASE_S - 2, BASE_S + 62),
                                  {"kind": "disconnect", "conn": "market", "at_ns": (BASE_S + 30) * S,
                                   "error_type": "ConnectionClosedError"}), self.start, self.end)
        self.assertFalse(out["covered"])
        self.assertIn("disconnect (market)", out["coverage_problems"])
        after = probe.analyze(lines(*subscribed(0), *full_window(BASE_S - 2, BASE_S + 62),
                                    {"kind": "disconnect", "conn": "market", "at_ns": self.end + S}),
                              self.start, self.end)
        self.assertTrue(after["covered"])

    def test_quiet_trade_needs_a_row_not_the_edges(self):
        rows = [r for r in full_window(BASE_S - 2, BASE_S + 62)
                if not (r["c"] == "trade" and r["i"] == "DOGEUSDT" and r["s"] != BASE_S + 30)]
        out = probe.analyze(lines(*rows), self.start, self.end)
        self.assertTrue(out["covered"], out["coverage_problems"])
        self.assertEqual(out["channels"]["trade"]["DOGEUSDT"]["seconds"], 1)

    def test_no_rows_for_a_symbol(self):
        rows = [r for r in full_window(BASE_S - 2, BASE_S + 62) if not (r["c"] == "trade" and r["i"] == "SOLUSDT")]
        out = probe.analyze(lines(*rows), self.start, self.end)
        self.assertEqual(out["coverage_problems"], ["no row inside the window for trade SOLUSDT"])


class ProbeStateTests(unittest.TestCase):
    def setUp(self):
        self.events = subscribed((BASE_S - 10) * S)
        self.rows = full_window(BASE_S - 5, BASE_S + 20)

    def state(self, rows=None, events=None, channel="bookTicker", symbol="BTCUSDT", lo=BASE_S + 3, hi=BASE_S + 5):
        return probe.probe_state(self.rows if rows is None else rows, self.events if events is None else events,
                                 channel=channel, symbol=symbol, from_ns=lo * S + 400 * MS, to_ns=hi * S + 10 * MS,
                                 healthy_max_ms=150.0, late_min_ms=300.0)

    def test_healthy(self):
        out = self.state()
        self.assertEqual((out["state"], out["seconds"], out["lag_max_ms"], out["n"]), ("HEALTHY", 3, 20.0, 15))

    def test_late_uses_the_larger_of_e_and_t_lag(self):
        rows = [dict(r, lag_t_max_ms=450.0) if (r["c"], r["i"], r["s"]) == ("bookTicker", "BTCUSDT", BASE_S + 4)
                else r for r in self.rows]
        out = self.state(rows)
        self.assertEqual((out["state"], out["lag_max_ms"]), ("LATE", 450.0))
        # any symbol of the channel
        self.assertEqual(self.state(rows, symbol=None)["state"], "LATE")
        self.assertEqual(self.state(rows, symbol="ETHUSDT")["state"], "HEALTHY")

    def test_indeterminate(self):
        rows = [dict(r, lag_e_max_ms=200.0) if r["c"] == "markPrice" else r for r in self.rows]
        out = self.state(rows, channel="markPrice", symbol="SOLUSDT")
        self.assertEqual((out["state"], out["lag_max_ms"]), ("INDETERMINATE", 200.0))

    def test_missing_second_for_bookticker(self):
        rows = [r for r in self.rows if (r["c"], r["i"], r["s"]) != ("bookTicker", "BTCUSDT", BASE_S + 4)]
        out = self.state(rows)
        self.assertEqual((out["state"], out["seconds"]), ("NO_COVERAGE", 2))
        self.assertEqual(out["reason"], "1 second(s) without a row")
        self.assertEqual(self.state(rows, symbol=None)["state"], "HEALTHY")  # other symbols fill that second

    def test_disconnect_inside_range(self):
        events = self.events + [{"kind": "disconnect", "conn": "public", "at_ns": (BASE_S + 5) * S + 500 * MS,
                                 "error_type": "ConnectionClosedError"}]
        self.assertEqual(self.state(events=events)["state"], "NO_COVERAGE")
        self.assertEqual(self.state(events=events, channel="trade", symbol="ETHUSDT")["state"], "NO_COVERAGE")
        # the other connection is not affected
        self.assertEqual(self.state(events=events, channel="markPrice", symbol="BTCUSDT")["state"], "HEALTHY")
        error = self.events + [{"kind": "error", "conn": "market", "at_ns": (BASE_S + 4) * S, "code": "2"}]
        self.assertEqual(self.state(events=error, channel="markPrice", symbol="BTCUSDT")["state"], "NO_COVERAGE")
        done = self.events + [{"kind": "done", "at_ns": (BASE_S + 5) * S}]
        self.assertEqual(self.state(events=done)["state"], "NO_COVERAGE")
        late_sub = subscribed((BASE_S + 4) * S)
        self.assertEqual(self.state(events=late_sub)["reason"], "connection not subscribed before the range")

    def test_trade_quiet_and_zero_rows(self):
        quiet = [r for r in self.rows if not (r["c"] == "trade" and r["i"] == "DOGEUSDT" and r["s"] != BASE_S + 4)]
        self.assertEqual(self.state(quiet, channel="trade", symbol="DOGEUSDT")["state"], "HEALTHY")
        none = [r for r in self.rows if not (r["c"] == "trade" and r["i"] == "DOGEUSDT")]
        out = self.state(none, channel="trade", symbol="DOGEUSDT")
        self.assertEqual((out["state"], out["n"], out["reason"]), ("NO_COVERAGE", 0, "no row in the range"))


class WindowReportTests(unittest.TestCase):
    def test_none_without_frames_and_report_with(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            self.assertIsNone(probe.window_report(d, 0, 1))
            start, end = BASE_S * S, (BASE_S + 30) * S
            (d / "frames.jsonl").write_text("\n".join(lines(*subscribed(0), *full_window(BASE_S - 1, BASE_S + 31)))
                                            + "\n")
            (d / "tcp.jsonl").write_text("\n".join(lines(*[
                {"kind": "tcp", "at_ns": start + i * S, "delta": {"Tcp:InSegs": 3}, "sockets": 2, "rx_queue_max": 0,
                 "tx_queue_max": 0, "rx_persist_samples_max": 0, "rx_queued_ports": []} for i in range(31)])) + "\n")
            (d / "receipt.json").write_text(json.dumps({"exit": 0, "ingestor_pid_stable": True,
                                                        "host_clock_bound_ms_max": 0.42}))
            out = probe.window_report(d, start, end)
            self.assertTrue(out["frames"]["covered"], out["frames"]["coverage_problems"])
            self.assertTrue(out["tcp"]["covered"])
            self.assertEqual(out["tcp"]["counter_totals"]["Tcp:InSegs"], 93)
            self.assertEqual((out["probe_exit"], out["ingestor_pid_stable"], out["host_clock_bound_ms_max"]),
                             (0, True, 0.42))
            self.assertEqual(len(out["frames_sha256"]), 64)
            self.assertEqual(len(out["receipt_sha256"]), 64)

    def test_tcp_helpers_are_the_books_probe_ones(self):
        self.assertIs(probe.TcpSampler, books.TcpSampler)
        self.assertIs(probe.analyze_tcp, books.analyze_tcp)
        self.assertEqual(probe.RX_PERSIST_SAMPLES, books.RX_PERSIST_SAMPLES)


class RunPlanTests(unittest.TestCase):
    def test_argv_invariants(self):
        inspect = {"Image": "sha256:" + "e" * 64, "Config": {"User": "10001:10001"}}
        argv, provenance = probe.run_plan(inspect, "qdl-p184-g4obs-x-binance", 120)
        self.assertEqual(argv[:4], ["docker", "run", "--rm", "-i"])
        for flag in ("--rm", "--init", "--read-only"):
            self.assertIn(flag, argv)
        pairs = dict(zip(argv, argv[1:]))
        self.assertEqual(pairs["--name"], "qdl-p184-g4obs-x-binance")
        self.assertEqual(pairs["--label"], "qdl.p184.role=g4-binance-probe")
        self.assertEqual(pairs["--cap-drop"], "ALL")
        self.assertEqual(pairs["--security-opt"], "no-new-privileges")
        self.assertEqual(pairs["--network"], "bridge")
        self.assertEqual((pairs["--cpus"], pairs["--memory"], pairs["--memory-swap"], pairs["--pids-limit"]),
                         ("0.5", "192m", "192m", "32"))
        self.assertEqual(pairs["--user"], "10001:10001")
        self.assertEqual(argv[-3:], ["--entrypoint", "python", "sha256:" + "e" * 64])
        self.assertNotIn("--privileged", argv)
        self.assertFalse(any(a.startswith("-v") or a == "--volume" for a in argv))
        self.assertEqual(provenance["image_source_container"], "qdl_v2_stable_candidate-query_kn_1-1")
        self.assertEqual(provenance["streams"]["market"][0], "btcusdt@markPrice@1s")
        no_user, _ = probe.run_plan({"Image": "img"}, "n", 1)
        self.assertNotIn("--user", no_user)

    def test_constants(self):
        self.assertEqual(probe.PROBE_VERSION, "qdl.p184.g4-binance-probe.v1")
        self.assertEqual(probe.INGESTOR, "qdl_v2_stable_candidate-ingestor_binance_usdm-1")
        self.assertEqual((probe.MAX_SECONDS, probe.MAX_OUTPUT_BYTES, probe.EDGE_TOLERANCE_NS),
                         (600, 32 * 1024 * 1024, 5 * S))
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            probe.main(["--launch", "--seconds", "601", "--output", "x", "--name", "n"])

    def test_worker_text_runs_without_sibling_imports(self):
        namespace = {"__name__": "probe_worker"}
        exec(compile(Path(probe.__file__).read_text(), "<string>", "exec"), namespace)  # no __file__, like -c
        self.assertNotIn("TcpSampler", namespace)
        self.assertEqual(namespace["parse_frame"](TRADE)["i"], "ETHUSDT")


if __name__ == "__main__":
    unittest.main()
