"""Offline tests for the P18.4 final gate-campaign probes and scorer (synthetic inputs, test-only provenance).

No Redis, Docker or network: the RESP client is exercised on bytes, the probes' analysis parts on synthetic samples.
"""
from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path

from scripts import p184_g3_bar_cache_probe as g3
from scripts import p184_g8_redis_sampler as g8
from scripts import p184_g9_g10_writer_counters as g910
from scripts import p184_g11_writer_restart_probe as g11
from scripts import p184_g14_resource_table as g14
from scripts import p184_gate_campaign as campaign
from scripts import p184_resp_readonly as resp

S = 1_000_000_000


class RespTests(unittest.TestCase):
    def test_only_read_commands_are_allowed(self):
        for args in (("PING",), ("GET", "k"), ("MGET", "a", "b"), ("INFO", "commandstats"), ("SLOWLOG", "GET", 128),
                     ("CONFIG", "GET", "x"), ("CLIENT", "LIST"), ("scan", "0")):
            self.assertTrue(resp.allowed(args), args)
        for args in (("MONITOR",), ("SET", "k", "v"), ("FLUSHALL",), ("SLOWLOG", "RESET"), ("CONFIG", "SET", "a", "b"),
                     ("CLIENT", "KILL", "x"), ("EVAL", "x", "0"), ()):
            self.assertFalse(resp.allowed(args), args)

    def test_refused_command_never_touches_the_socket(self):
        client = resp.ReadOnlyRedis("203.0.113.1")  # TEST-NET, never connected
        with self.assertRaises(PermissionError):
            client.call("MONITOR")
        with self.assertRaises(PermissionError):
            client.pipeline([("PING",), ("FLUSHDB",)])
        self.assertIsNone(client._sock)

    def test_encode_and_parse_roundtrip(self):
        self.assertEqual(resp.encode(("GET", "k")), b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        reader = io.BytesIO(b"*3\r\n$3\r\nabc\r\n$-1\r\n:42\r\n+OK\r\n-ERR bad\r\n")
        self.assertEqual(resp.parse(reader), [b"abc", None, 42])
        self.assertEqual(resp.parse(reader), "OK")
        with self.assertRaises(resp.RespError):
            resp.parse(reader)

    def test_redis_url_and_quantiles(self):
        self.assertEqual(resp.parse_redis_url("redis://redis_marketdata:6379/1"), ("redis_marketdata", 6379, 1))
        with self.assertRaises(ValueError):
            resp.parse_redis_url("redis://user:pw@host:6379/0")
        q = resp.quantiles(list(range(1, 101)))
        self.assertEqual((q["p50"], q["p95"], q["p99"], q["max"]), (51, 96, 100, 100))
        self.assertIsNone(resp.quantiles([1, 2])["p99"])

    def test_bounded_jsonl_stops_at_cap(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = resp.BoundedJsonl(Path(tmp) / "e.jsonl", 60)
            self.assertTrue(out.write({"a": 1}))
            self.assertFalse(out.write({"b": "x" * 100}))
            out.close()
            rows = list(resp.read_jsonl(Path(tmp) / "e.jsonl"))
            self.assertEqual(rows[-1]["kind"], "BYTE_CAP_REACHED")


def bar(open_ms, closed=True):
    return json.dumps({"timestamp_ms": open_ms, "is_closed": closed}).encode()


class G3Tests(unittest.TestCase):
    def test_first_closed_bar_after_start_is_measured_from_close(self):
        name = "OKX|BTC-USDT-SWAP|1m"
        tracker = g3.BarTracker([name], {name: 60_000})
        self.assertIsNone(tracker.observe(name, bar(0), 59_000))  # present at start: not measured
        self.assertIsNone(tracker.observe(name, bar(60_000, closed=False), 100_000))
        record = tracker.observe(name, bar(60_000), 121_500)
        self.assertEqual(record["transit_ms"], 1_500)
        self.assertIsNone(tracker.observe(name, bar(60_000), 122_000))  # revision of the same bar: first only
        summary = tracker.finish(start_ms=59_000, end_ms=200_000)
        self.assertEqual(summary["per_slice"][name]["n"], 1)
        self.assertEqual(summary["unseen_total"], 0)
        self.assertEqual(g3.verdict(summary, unresolved=0), "PASS")

    def test_unseen_expected_close_fails_with_its_lower_bound(self):
        name = "BINANCE|BTCUSDT|1m"
        tracker = g3.BarTracker([name], {name: 60_000})
        tracker.observe(name, bar(0), 59_000)
        summary = tracker.finish(start_ms=59_000, end_ms=200_000)  # closes at 120 s expected, never seen
        self.assertEqual(summary["unseen_total"], 1)
        self.assertEqual(summary["unseen"][0]["lower_bound_ms"], 80_000)
        self.assertEqual(g3.verdict(summary, unresolved=0), "FAIL")

    def test_slow_p95_fails_and_unresolved_slice_blocks_pass(self):
        name = "OKX|ETH-USDT-SWAP|1m"
        tracker = g3.BarTracker([name], {name: 60_000})
        tracker.observe(name, bar(0), 59_000)
        tracker.observe(name, bar(60_000), 127_000)
        summary = tracker.finish(start_ms=59_000, end_ms=200_000)
        self.assertEqual(g3.verdict(summary, unresolved=0), "FAIL")
        tracker = g3.BarTracker([name], {name: 60_000})
        tracker.observe(name, bar(0), 59_000)
        tracker.observe(name, bar(60_000), 121_000)
        self.assertEqual(g3.verdict(tracker.finish(start_ms=59_000, end_ms=200_000), unresolved=1), "NOT_MEASURED")

    def test_slices_and_keys_come_from_the_binding_and_cache(self):
        binding = {"products": [{"feed": "BAR", "interval": "1m", "venue": "OKX", "native_symbol": "BTC-USDT-SWAP"},
                                {"feed": "TRADE", "interval": None, "venue": "OKX", "native_symbol": "BTC-USDT-SWAP"}]}
        slices = g3.bar_slices(binding)
        keys = g3.resolve_keys(slices, ["cache:market:v2:last_bar:OKX:SWAP:BTC-USDT-SWAP:1m",
                                        "cache:market:v2:last_bar:OKX:SWAP:ETH-USDT-SWAP:1m"])
        self.assertEqual(keys, {"OKX|BTC-USDT-SWAP|1m": "cache:market:v2:last_bar:OKX:SWAP:BTC-USDT-SWAP:1m"})


def stats(at_s, evalsha_usec, slow=()):
    return {"kind": "stats", "at_ns": at_s * S, "commandstats": {"evalsha": {"calls": 1, "usec": evalsha_usec}},
            "slowlog": list(slow), "slowlog_threshold_us": 10000, "monitor_clients": 0}


class G8Tests(unittest.TestCase):
    def test_parsers_keep_no_arguments(self):
        info = "# Commandstats\r\ncmdstat_evalsha:calls=10,usec=2500,usec_per_call=250.00\r\ncmdstat_ping:calls=1,usec=1\r\n"
        self.assertEqual(g8.parse_commandstats(info)["evalsha"], {"calls": 10, "usec": 2500})
        rows = g8.slowlog_rows([[7, 1000, 12000, [b"EVALSHA", b"secret-arg"], b"1.2.3.4:5", b""]])
        self.assertEqual(rows, [{"id": 7, "start_s": 1000, "duration_us": 12000, "command": "EVALSHA"}])
        self.assertEqual(g8.monitor_clients("id=1 flags=N\nid=2 flags=O\n"), 1)

    def test_window_core_fraction_and_steady_slowlog(self):
        samples = [stats(t, int(t * 0.3e6)) for t in range(0, 1201, 10)]  # 0.3 core
        samples += [{"kind": "ping", "at_ns": t * S, "rtt_ms": 0.5} for t in range(0, 1200)]
        summary = g8.summarize(samples, steady_from_ns=0, steady_to_ns=1200 * S)
        self.assertEqual([w["core_fraction"] for w in summary["windows"]], [0.3, 0.3])
        self.assertEqual(g8.verdict(summary)[0], "PASS")
        hot = [stats(t, int(t * 0.7e6)) for t in range(0, 601, 10)]
        self.assertEqual(g8.verdict(g8.summarize(hot + samples[121:], steady_from_ns=0, steady_to_ns=600 * S))[0],
                         "FAIL")

    def test_new_slowlog_entries_in_steady_state_fail_history_does_not(self):
        old = {"id": 1, "start_s": 1, "duration_us": 20000, "command": "EVAL"}
        new = {"id": 2, "start_s": 300, "duration_us": 15000, "command": "EVALSHA"}
        samples = [stats(0, 0, [old])] + [stats(t, t * 1000, [old, new] if t >= 300 else [old])
                                         for t in range(10, 611, 10)]
        samples += [{"kind": "ping", "at_ns": t * S, "rtt_ms": 0.5} for t in range(0, 600)]
        summary = g8.summarize(samples, steady_from_ns=0, steady_to_ns=610 * S)
        self.assertEqual((summary["slowlog_new_total"], summary["slowlog_new_steady"]), (1, 1))
        self.assertEqual(g8.verdict(summary)[0], "FAIL")

    def test_counter_reset_invalidates_the_window_and_short_run_is_not_measured(self):
        samples = [stats(0, 10**9), stats(10, 5)]
        summary = g8.summarize(samples, steady_from_ns=0, steady_to_ns=10 * S)
        self.assertFalse(summary["windows"][0]["valid"])
        self.assertEqual(g8.verdict(summary)[0], "NOT_MEASURED")


def beat(at_s, *, demanded=64, session=64, not_ready=(), purpose=True, fallback=0, last_seen=None):
    details = {"demanded_v2_slices": demanded, "session_ready_v2_slices": session, "execution_ready_v2_slices": 50,
               "v1_fallback_count": fallback, "reported_unhealthy_slices": []}
    if purpose:
        details.update(purpose_readiness_schema="ts.market-data.purpose-readiness.v1", not_ready=list(not_ready))
    sample = g910.heartbeat_sample({"status": "READY", "details": details,
                                    "last_seen_unix": last_seen if last_seen is not None else at_s}, at_s)
    return {"kind": "heartbeat", "at_ns": at_s * S, "sample": sample}


def row(feed, reason, symbol="BTCUSDT"):
    return {"provider": "BINANCE", "symbol": symbol, "feed": feed, "interval": None, "reason_code": reason}


class G9Tests(unittest.TestCase):
    def test_new_not_ready_rows_are_losses_quiet_trade_is_excluded(self):
        samples = [beat(0), beat(10, not_ready=[row("TRADE", "TRADE_EVENT_STALE")]),
                   beat(20, not_ready=[row("TRADE", "TRADE_EVENT_STALE"), row("QUOTE", "QUOTE_STALE")]),
                   beat(30, not_ready=[row("QUOTE", "QUOTE_STALE")])]
        summary = g910.summarize_g9(samples)
        self.assertEqual(summary["eligibility_losses_data_path"], 1)
        self.assertEqual(summary["eligibility_loss_causes"], {"QUOTE:QUOTE_STALE": 1})
        self.assertEqual(summary["legit_quiet_refusals"], {"TRADE:TRADE_EVENT_STALE": 1})
        self.assertEqual(g910.verdict_g9(summary, expected_demanded=64)[0], "FAIL")

    def test_clean_run_passes_and_old_writer_is_not_measured(self):
        clean = [beat(t, not_ready=[row("TRADE", "TRADE_EVENT_STALE")]) for t in range(0, 100, 10)]
        self.assertEqual(g910.verdict_g9(g910.summarize_g9(clean), expected_demanded=64), ("PASS", []))
        old = [beat(t, purpose=False) for t in range(0, 100, 10)]
        state, reasons = g910.verdict_g9(g910.summarize_g9(old), expected_demanded=64)
        self.assertEqual(state, "NOT_MEASURED")
        self.assertIn("purpose readiness", reasons[0])

    def test_unknown_and_stale_samples_never_count_as_ready(self):
        samples = [beat(0), {"kind": "heartbeat", "at_ns": 10 * S, "sample": {"readable": False}},
                   beat(100, last_seen=0)]
        summary = g910.summarize_g9(samples)
        self.assertEqual((summary["readable_samples"], summary["unknown_samples"]), (1, 2))
        self.assertEqual(g910.verdict_g9(summary, expected_demanded=64)[0], "NOT_MEASURED")

    def test_session_mismatch_and_fallback_fail(self):
        self.assertEqual(g910.verdict_g9(g910.summarize_g9([beat(0, session=63)]), expected_demanded=64)[0], "FAIL")
        self.assertEqual(g910.verdict_g9(g910.summarize_g9([beat(0, fallback=1)]), expected_demanded=64)[0], "FAIL")


LINES = [
    "2026-10-10 15:00:01,000 - market_data_worker-0 - services.market_data.data_layer_bridge - WARNING - Data Layer V2 "
    "slice disconnected venue=OKX symbol=BTC-USDT-SWAP feed=TRADE interval=None reason=SlowConsumerError "
    "code=RATE_LIMITED retryable=True",
    "2026-10-10 15:00:02,000 - market_data_worker-0 - x - WARNING - Data Layer V2 slice disconnected venue=OKX "
    "symbol=BTC-USDT-SWAP feed=TRADE interval=None reason=CursorExpiredError code=CURSOR_EXPIRED",
    "2026-10-10 15:00:03,000 - market_data_worker-1 - x - INFO - data layer slice control code=SNAPSHOT_REPLACED "
    "venue=BINANCE symbol=BTCUSDT feed=TRADE generation=2",
    "2026-10-10 15:00:04,000 - market_data_worker-1 - x - INFO - data layer slice control code=RECONNECTED "
    "venue=BINANCE symbol=BTCUSDT feed=TRADE generation=3",
    "2026-10-10 15:00:05,000 - market_data_worker-1 - x - WARNING - Data Layer V2 slice disconnected venue=OKX "
    "symbol=BTC-USDT-SWAP feed=QUOTE interval=None reason=DataLayerError code=DATA_STALE",
    "2026-10-10 15:00:06,000 - market_data_worker-1 - qdl_sdk.cursor - INFO - qdl_cursor_persistence {}",
]


class G10Tests(unittest.TestCase):
    def test_trade_lines_are_classified_by_typed_signal(self):
        got = [g910.classify_line(line) for line in LINES]
        self.assertEqual([item[1] if item else None for item in got],
                         ["overflow", "replay_expired", "gap", "resumed", "other_feed_disconnect", None])

    def test_log_tail_reads_only_new_complete_lines_and_survives_truncation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "market_data_worker-0.log"
            path.write_text("old line\n")
            tail = g910.LogTail([path])
            with path.open("a") as handle:
                handle.write(LINES[0] + "\npartial")
            self.assertEqual(tail.read_new(), [LINES[0]])
            with path.open("a") as handle:
                handle.write(" end\n")
            self.assertEqual(tail.read_new(), ["partial end"])
            path.write_text("x\n")  # truncated/rotated
            self.assertEqual(tail.read_new(), ["x"])

    def test_summary_windows_by_log_stamp(self):
        samples = [{"kind": "log_poll", "unreadable": 0}]
        for line in LINES:
            item = g910.classify_line(line)
            if item:
                samples.append({"kind": "log_event", "stamp": item[0], "category": item[1], "detail": item[2]})
        summary = g910.summarize_g10(samples, from_s="2026-10-10 15:00:02", to_s="2026-10-10 15:00:09")
        self.assertEqual((summary["trade_overflow"], summary["trade_replay_expired"], summary["trade_gap"],
                          summary["trade_resumed"]), (0, 1, 1, 1))


def obs(event_ms, digest, pttl=100_000, offset=None, generation="g1", closed=None):
    return {"digest": digest, "pttl": pttl, "event_ms": event_ms, "offset": offset, "generation": generation,
            "closed": closed}


class G11Tests(unittest.TestCase):
    def test_first_apply_and_caught_up_after_fault_cleared(self):
        s = g11.SliceRecovery("OKX|BTC-USDT-SWAP|TRADE|None", "TRADE", None, 500)
        s.update(1_000, obs(900, "a", offset=10), phase="PRE")
        s.update(5_000, obs(900, "a", pttl=96_000, offset=10), phase="DOWN")
        s.update(20_000, obs(900, "a", pttl=81_000, offset=10), phase="POST")
        self.assertIsNone(s.first_apply_ms)
        s.update(21_000, obs(15_000, "b", offset=11), phase="POST")  # rewritten, older than live bound
        self.assertEqual((s.first_apply_ms, s.caught_up_ms), (21_000, None))
        s.update(22_000, obs(21_500, "c", offset=12), phase="POST")
        self.assertEqual(s.caught_up_ms, 22_000)
        summary = g11.recovery_summary({s.name: s}, cleared_ms=19_000)
        self.assertEqual(summary["first_valid_apply_s"]["max"], 2.0)
        self.assertEqual(summary["caught_up_s"]["max"], 3.0)
        self.assertEqual(summary["rewinds_total"], 0)

    def test_ttl_jump_counts_as_a_rewrite_and_rewind_is_detected(self):
        s = g11.SliceRecovery("B", "BOOK_SNAPSHOT", None, 500)
        s.update(0, obs(1_000, "a", pttl=60_000), phase="PRE")
        s.update(10_000, obs(1_000, "a", pttl=50_000), phase="POST")
        s.update(10_500, obs(10_400, "a", pttl=119_900), phase="POST")
        self.assertEqual(s.first_apply_ms, 10_500)
        t = g11.SliceRecovery("T", "TRADE", None, 500)
        t.update(0, obs(5_000, "a", offset=100), phase="PRE")
        t.update(1_000, obs(4_000, "b", offset=90), phase="POST")
        self.assertEqual({r["kind"] for r in t.rewinds}, {"event_time", "applied_offset"})
        self.assertIsNone(t.first_apply_ms)  # an older event is never a valid first apply

    def test_bar_caught_up_by_close_age(self):
        s = g11.SliceRecovery("BAR", "BAR", "1m", 500)
        s.update(0, obs(60_000, "a", closed=True), phase="PRE")
        s.update(130_000, obs(120_000, "b", closed=True), phase="POST")
        self.assertEqual((s.first_apply_ms, s.caught_up_ms), (130_000, 130_000))

    def test_cursor_check_and_helpers(self):
        result = g11.cursor_check({"c|uid1|TRADE||p": 10, "c|uid1|QUOTE||p": 30, "c|uid1|BAR|1m|p": 99,
                                   "c|uid9|TRADE||p": 1}, {"T": 12, "Q": 20}, {"uid1|TRADE": "T", "uid1|QUOTE": "Q"})
        self.assertEqual((result["compared"], result["unmatched"]), (2, 1))
        self.assertEqual(result["ahead"], [{"slice": "Q", "cursor": 30, "applied": 20}])
        self.assertEqual(g11.parse_docker_time("2026-10-10T13:30:24.199934182Z") % 1000, 199)
        self.assertIsNone(g11.parse_docker_time("0001-01-01T00:00:00Z"))
        ids = g11.identities_from_keys(["cache:market:v2:execution_trade:OKX:SWAP:BTC-USDT-SWAP",
                                        "cache:market:v2:applied:trade:OKX:SWAP:BTC-USDT-SWAP"])
        self.assertEqual(ids, {("OKX", "BTC-USDT-SWAP"): "OKX:SWAP:BTC-USDT-SWAP"})
        keys = g11.slice_keys({"venue": "OKX", "native": "X", "feed": "BOOK_DELTA", "interval": None}, "OKX:SWAP:X")
        self.assertEqual(keys["value"], "cache:market:v2:execution_book_delta:OKX:SWAP:X:ordering-v1")

    def test_verdict(self):
        s = g11.SliceRecovery("T", "TRADE", None, 500)
        s.update(0, obs(0, "a"), phase="PRE")
        s.update(5_000, obs(4_500, "b"), phase="POST")
        report = {"restart": {"fault_cleared_ms": 1_000}, "observe_seconds": 180,
                  "recovery": g11.recovery_summary({"T": s}, 1_000),
                  "cursor_samples": [{"compared": 2, "ahead": []}]}
        self.assertEqual(g11.verdict(report), ("PASS", []))
        report["cursor_samples"] = [{"compared": 2, "ahead": [{"slice": "T"}]}]
        self.assertEqual(g11.verdict(report)[0], "FAIL")
        self.assertEqual(g11.verdict({"restart": None})[0], "NOT_MEASURED")


def host_json():
    def sample(at_s, usage_s, cpu, mem):
        return {"at_ns": at_s * S, "stats": [
            {"Name": "qdl_v2_stable_candidate-stable_redis-1", "CPUPerc": f"{cpu}%", "MemUsage": f"{mem}MiB / 512MiB"}],
            "cpu_throttle": {"qdl_v2_stable_candidate-stable_redis-1": {
                "usage_usec": usage_s * 10**6, "throttled_usec": 0, "nr_periods": 0, "nr_throttled": 0}}}
    return {"monitored_containers": ["qdl_v2_stable_candidate-stable_redis-1",
                                     "qdl_v2_stable_candidate-binance_bar_edge-1"],
            "runtime_observation": [sample(0, 0, 10, 100), sample(100, 25, 30, 120)]}


class G14Tests(unittest.TestCase):
    def test_roles_mean_from_cgroup_and_missing_roles_listed(self):
        result = g14.table(host_json())
        row = result["roles"]["stable_redis"]
        self.assertEqual((row["cpu_mean_pct"], row["cpu_max_pct"], row["mem_max_mib"], row["mem_limit_mib"]),
                         (25.0, 30.0, 120.0, 512.0))
        self.assertEqual(result["missing_roles"], ["binance_bar_edge"])
        self.assertIn("stable_redis", g14.render(result))
        self.assertAlmostEqual(g14.mib("1.5GiB"), 1536.0)


class CampaignTests(unittest.TestCase):
    def test_g4_schedule_adds_a_boundary_window_without_overlap(self):
        launch = 1_791_649_000.0  # boundary at 1_791_649_800 (UTC :30)
        windows = campaign.g4_schedule(launch)
        boundary = [w for w in windows if w["boundary"]]
        self.assertEqual(len(boundary), 1)
        self.assertEqual(boundary[0]["offset"], 1_791_649_800 - 30 - launch)
        ends = [(w["offset"], w["offset"] + w["seconds"]) for w in windows]
        self.assertTrue(all(a[1] <= b[0] for a, b in zip(ends, ends[1:])))
        late = campaign.g4_schedule(1_791_649_799.0 - 1700)  # next boundary falls after the load
        self.assertTrue(all(w["boundary"] is None or w["offset"] + w["seconds"] <= 1880 for w in late))

    def test_profile_paths_and_env(self):
        profile = campaign.derive_profile({"monitored_containers": ["a"], "image": "i"})
        self.assertEqual(profile["monitored_containers"], ["a", *campaign.EXTRA_MONITORED])
        inspect = {"Mounts": [{"Source": "/h/logs", "Destination": "/app/logs"},
                              {"Source": "/h/b.json", "Destination": "/run/b.json"}],
                   "Config": {"Env": ["A=1", "B=x=y"]}}
        self.assertEqual(campaign.host_path(inspect, "/app/logs/dr07"), "/h/logs/dr07")
        self.assertEqual(campaign.mount_source(inspect, "/run/b.json"), "/h/b.json")
        self.assertEqual(campaign.env_of(inspect), {"A": "1", "B": "x=y"})

    def test_g4_rows_need_baseline_coverage(self):
        table = {"trade|OKX|": {"candidate_p95_limit_ms": 100.0}}
        inside = [{"summary": {"trade|OKX|": {"visibility_ms": {"n": 5, "p95": 90.0}}}, "inverse_rows": {}}]
        self.assertEqual(campaign.score_g4(inside, table)[0], "PASS")
        over = [{"summary": {"trade|OKX|": {"visibility_ms": {"n": 5, "p95": 120.0}}}, "inverse_rows": {}}]
        self.assertEqual(campaign.score_g4(over, table)[0], "FAIL")
        inverse = [{**inside[0], "inverse_rows": {"trade|OKX_INVERSE|": {"n": 3}}}]
        state, _, why = campaign.score_g4(inverse, table)
        self.assertEqual(state, "NOT_MEASURED")
        self.assertIn("OKX_INVERSE", why[0])
        rows = campaign.g4_inverse_rows([{"kind": "canonical_observation", "feed": "trade", "interval": None,
                                          "identity": {"native_symbol": "BTC-USD-SWAP"}, "source_event_ns": 1 * S,
                                          "observer_receipt_wall_ns": 1 * S + 50_000_000}])
        self.assertEqual(rows, {"trade|OKX_INVERSE|": {"n": 1, "p95": 50.0, "max": 50.0}})

    def test_g6_unsafe_fails_missing_binding_not_measured(self):
        full = {key: {"n": 5, "unsafe": 0} for key in campaign.G6_REQUIRED}
        self.assertEqual(campaign.score_g6(full)[0], "PASS")
        self.assertEqual(campaign.score_g6({**full, "TRADE|OKX|": {"n": 5, "unsafe": 1}})[0], "FAIL")
        partial = {k: v for k, v in full.items() if not k.startswith("BOOK_DELTA")}
        self.assertEqual(campaign.score_g6(partial)[0], "NOT_MEASURED")

    def test_g10_needs_trace_for_duplicates(self):
        g10 = {"trade_gap": 0, "trade_replay_expired": 0, "trade_overflow": 0, "log_polls": 10, "log_unreadable": 0}
        self.assertEqual(campaign.score_g10(g10, {}, [])[0], "PASS")
        self.assertEqual(campaign.score_g10(g10, {"TRADE|OKX": 1}, [])[0], "FAIL")
        self.assertEqual(campaign.score_g10(g10, None, ["no trace"])[0], "NOT_MEASURED")
        self.assertEqual(campaign.score_g10({**g10, "trade_overflow": 2}, None, ["no trace"])[0], "FAIL")

    def test_trace_coverage_flags_cap_and_early_expiry(self):
        ok = {"records": 10, "dropped_at_export_total": 0, "stop_reasons": {"ROTATED": 3, "EXPIRY": 4}}
        self.assertEqual(campaign._trace_coverage(ok, [0, 10 * S], 11 * S), [])
        self.assertTrue(campaign._trace_coverage({**ok, "stop_reasons": {"CAP": 1}}, [0, 10 * S], 11 * S))
        self.assertTrue(campaign._trace_coverage(ok, [0, 10 * S], 9 * S))

    def test_scorecard_never_passes_outside_the_protocol(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "stage100").mkdir()
            (d / "campaign.json").write_text(json.dumps({"trace": {"until_unix_ns": 10**19}, "g4_windows": []}))
            (d / "stage100" / "receipt.json").write_text(json.dumps({
                "stage": 100, "final": True, "load_window_unix_s": [1000.0, 2000.0],  # 1000 s < 1800 s
                "fault_windows": {"BURST": [720, 730]}, "g6_view_quality": {
                    key: {"n": 5, "unsafe": 0} for key in campaign.G6_REQUIRED}}))
            (d / "stage100" / "gates.json").write_text(json.dumps({"status": "PASS", "gates": [
                {"gate": "latency:BAR_LATEST:OKX", "status": "PASS"}]}))
            card = campaign.build_scorecard(d)
            by_id = {g["id"]: g for g in card["gates"]}
            self.assertFalse(card["protocol_met"])
            self.assertEqual(by_id["G5"]["verdict"], "NOT_MEASURED")
            self.assertEqual(by_id["G6"]["verdict"], "NOT_MEASURED")
            self.assertIn("outside the protocol", by_id["G6"]["reason"])
            self.assertEqual(by_id["G1"]["verdict"], "NOT_MEASURED")
            self.assertEqual({g["id"] for g in card["gates"]}, {f"G{i}" for i in range(1, 15)})


if __name__ == "__main__":
    unittest.main()
