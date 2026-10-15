"""Offline tests for the P18.4 final gate-campaign probes and scorer (synthetic inputs, test-only provenance).

No Redis, Docker or network: the RESP client is exercised on bytes, the probes' analysis parts on synthetic samples.
"""
from __future__ import annotations

import io
import json
import tempfile
import unittest
import unittest.mock
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
        self.assertEqual(rows, [{"id": 7, "start_s": 1000, "duration_us": 12000, "command": "EVALSHA",
                                 "client_addr": "1.2.3.4:5", "client_name": "", "script_sha1": None,
                                 "script_sha1_basis": "EVALSHA_ARGUMENT"}])  # not a sha -> not kept
        self.assertEqual(g8.monitor_clients("id=1 flags=N\nid=2 flags=O\n"), 1)

    def test_slowlog_attribution_keeps_client_and_script_digest_never_keys_or_args(self):
        import hashlib
        sha = "a" * 40
        body = b"return redis.call('GET', KEYS[1])"
        long_body = b"x" * 128 + b"... (372 more bytes)"
        rows = g8.slowlog_rows([
            [1, 10, 15000, [b"EVALSHA", sha.upper().encode(), b"1", b"secret:key", b"pii-arg"], b"10.0.0.5:4242",
             b"ts-writer"],
            [2, 11, 16000, [b"EVAL", body, b"1", b"secret:key"], b"10.0.0.6:1", b"alpha"],
            [3, 12, 17000, [b"EVAL", long_body, b"0"], b"10.0.0.6:2", b"alpha"],
            [4, 13, 18000, [b"HGETALL", b"secret:key"], b"10.0.0.7:3", b"x"],
            [5, 14, 19000, [b"GET", b"k"]]])  # Redis < 4 rows have no client fields
        self.assertEqual((rows[0]["script_sha1"], rows[0]["script_sha1_basis"], rows[0]["client_name"],
                          rows[0]["client_addr"]), (sha, "EVALSHA_ARGUMENT", "ts-writer", "10.0.0.5:4242"))
        self.assertEqual((rows[1]["script_sha1"], rows[1]["script_body_truncated"]),
                         (hashlib.sha1(body).hexdigest(), False))
        self.assertEqual((rows[2]["script_sha1"], rows[2]["script_sha1_basis"], rows[2]["script_body_truncated"]),
                         (hashlib.sha1(b"x" * 128).hexdigest(), "SHA1_OF_VISIBLE_PREFIX", True))
        self.assertNotIn("script_sha1", rows[3])
        self.assertEqual((rows[4]["client_addr"], rows[4]["client_name"]), (None, None))
        text = json.dumps(rows)
        for secret in ("secret", "pii", "redis.call", "xxxx"):
            self.assertNotIn(secret, text)
        groups = g8.slowlog_attribution(rows)
        alpha = [g for g in groups if g["client_name"] == "alpha"]
        self.assertEqual(sorted(g["count"] for g in alpha), [1, 1])  # two scripts -> two groups
        self.assertEqual(alpha[0]["client_addrs"][0][:9], "10.0.0.6:")

    def test_attribution_is_report_only(self):
        new = {"id": 2, "start_s": 300, "duration_us": 15000, "command": "EVALSHA", "client_name": "w",
               "script_sha1": "b" * 40}
        samples = [stats(0, 0, [])] + [stats(t, t * 1000, [new] if t >= 300 else []) for t in range(10, 611, 10)]
        samples += [{"kind": "ping", "at_ns": t * S, "rtt_ms": 0.5} for t in range(0, 600)]
        summary = g8.summarize(samples, steady_from_ns=0, steady_to_ns=610 * S)
        self.assertEqual(summary["slowlog_steady_attribution"][0]["count"], 1)
        bare = {**summary, "slowlog_steady_attribution": [], "slowlog_new_attribution": []}
        self.assertEqual(g8.verdict(summary), g8.verdict(bare))

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

    D3 = "D3_EXECUTION"

    def _d3(self, **extra):
        return {key: {"n": 5, "unsafe": 0, "policy_basis": self.D3, **extra} for key in campaign.G6_REQUIRED}

    def _alpha(self, unsafe=0):
        return {f"{key}@REQUIREMENT_ALPHA": {"n": 50, "unsafe": unsafe, "policy_basis": "REQUIREMENT_ALPHA"}
                for key in campaign.G6_REQUIRED}

    def test_g6_only_alpha_rows_is_not_measured(self):
        state, _, why = campaign.score_g6(self._alpha())
        self.assertEqual(state, "NOT_MEASURED")
        self.assertIn("no EXECUTION (D3) view", why[0])
        self.assertIn("REQUIREMENT_ALPHA", why[0])

    def test_g6_clean_d3_rows_pass(self):
        self.assertEqual(campaign.score_g6({**self._d3(), **self._alpha()})[0], "PASS")
        # Probe rows (keyed apart with the probe suffix) count for their base binding.
        probe = campaign.g6_ts_probe_rows({"rows": {key: {"n": 3, "unsafe": 0, "policy_basis": self.D3}
                                                    for key in campaign.G6_REQUIRED}})
        self.assertEqual(campaign.score_g6({**probe, **self._alpha()})[0], "PASS")

    def test_g6_d3_unsafe_fails(self):
        quality = {**self._d3(), "TRADE|OKX|": {"n": 5, "unsafe": 1, "policy_basis": self.D3}}
        state, _, why = campaign.score_g6(quality)
        self.assertEqual(state, "FAIL")
        self.assertIn("D3 execution policy", why[0])

    def test_g6_alpha_unsafe_under_its_own_requirement_fails(self):
        state, _, why = campaign.score_g6({**self._d3(), **self._alpha(unsafe=1)})
        self.assertEqual(state, "FAIL")
        self.assertIn("own requirement", why[0])
        self.assertEqual(campaign.score_g6(self._alpha(unsafe=1))[0], "FAIL")

    def test_g6_missing_d3_binding_and_legacy_rows(self):
        partial = {k: v for k, v in self._d3().items() if not k.startswith("MARK_INDEX_PRICE|OKX")}
        self.assertEqual(campaign.score_g6(partial)[0], "NOT_MEASURED")
        # Rows without policy_basis (older receipts) are reported, not scored: never PASS, never FAIL by themselves.
        legacy = {key: {"n": 5, "unsafe": 3} for key in campaign.G6_REQUIRED}
        state, _, why = campaign.score_g6(legacy)
        self.assertEqual(state, "NOT_MEASURED")
        self.assertTrue(any("without policy_basis" in item for item in why))

    def test_g6_probe_window_and_last_line(self):
        load = [1000.0, 2800.0]
        self.assertEqual(campaign.g6_ts_probe_coverage({"kind": "final", "window_unix_s": [990.0, 2810.0]}, load), [])
        self.assertTrue(campaign.g6_ts_probe_coverage({"kind": "final", "window_unix_s": [990.0, 2500.0]}, load))
        self.assertTrue(campaign.g6_ts_probe_coverage({"kind": "partial", "window_unix_s": [990.0, 2810.0]}, load))
        self.assertTrue(campaign.g6_ts_probe_coverage(None, load))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "probe.jsonl"
            path.write_text('{"kind": "start", "pid": 7}\n{"kind": "partial", "reads": 1}\n{"kind": "final", '
                            '"reads": 2}\n{"kind": "fin')
            self.assertEqual(campaign.read_probe_lines(path), {"kind": "final", "reads": 2})

    def test_g6_probe_is_planned_as_a_read_only_docker_exec_into_the_writer(self):
        import argparse
        args = argparse.Namespace(state_tools="/tools", load_timeout=2400, budget="b.json", writer="w1", g11=False,
                                  g11_wait=900)
        plan = {"binding": "/b.json", "market_redis": {"container": "r", "db": 1},
                "heartbeat_redis": {"container": "h", "db": 0}}
        commands = campaign.command_plan(args, plan, Path("/c"), 0.0)
        self.assertEqual(commands["g6_ts"][:7], ["docker", "exec", "-i", "w1", "python", "-B", "-"])
        self.assertEqual(commands["g6_ts"][commands["g6_ts"].index("--duration") + 1], "2460")
        self.assertTrue(commands["g6_ts_stdin"].endswith("scripts/p184_g6_ts_execution_probe.py"))

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
                    f"{key}@REQUIREMENT_ALPHA": {"n": 5, "unsafe": 0, "policy_basis": "REQUIREMENT_ALPHA"}
                    for key in campaign.G6_REQUIRED}}))
            (d / "g6-ts").mkdir()
            (d / "g6-ts" / "summary.json").write_text(json.dumps({
                "kind": "final", "window_unix_s": [990.0, 2010.0], "rows": {
                    key: {"n": 5, "unsafe": 0, "policy_basis": "D3_EXECUTION"} for key in campaign.G6_REQUIRED}}))
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


# What a window can see (G4 schedule v2): the observer is READY <= 1 s after its start (READY - g4_start in the
# 2026-10-11/13 runs), a closed bar reaches md.canonical.v2 +1..+8 s after its close (RCA 2026-10-14), and the 180 s
# boundary window hit the 10000-record cap after 132 s on 2026-10-13. A close counts as seen only with margins:
# start + 5 s <= close and close + 15 s <= start + min(seconds, 120 s).
READY_S, ARRIVAL_S, HORIZON_S = 5, 15, 120
BAR_PERIODS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800}
GRID = 1_791_648_000  # a UTC :00 (multiple of 1800 s)


def closes_seen(launch, window, period):
    lo = launch + window["offset"] + READY_S
    hi = launch + window["offset"] + min(window["seconds"], HORIZON_S) - ARRIVAL_S
    return int(hi // period) - int(-(-lo // period)) + 1


class G4ScheduleV2Tests(unittest.TestCase):
    """Every launch second of the 1800 s :00/:30 cycle (and the half seconds between), as run() launches."""

    def sweep(self):
        for tenths in range(0, 18000, 5):
            earliest = GRID + tenths / 10
            launch = campaign.g4_launch_unix(earliest)
            yield earliest, launch, campaign.g4_schedule(launch)

    def test_every_launch_phase_has_a_boundary_window_inside_the_load(self):
        holds = []
        for earliest, launch, windows in self.sweep():
            holds.append(launch - earliest)
            boundary = [w for w in windows if w["boundary"] is not None]
            self.assertEqual(len(boundary), 1, earliest)
            close = boundary[0]["boundary"]
            self.assertEqual(close % 1800, 0)
            self.assertAlmostEqual(boundary[0]["offset"], close - 30 - launch, places=3)
            self.assertEqual(boundary[0]["seconds"], campaign.G4_BOUNDARY_SECONDS)
            for name, period in BAR_PERIODS.items():
                self.assertGreaterEqual(closes_seen(launch, boundary[0], period), 1, (earliest, name))
        self.assertLessEqual(max(holds), 220)  # 219 s on whole seconds, 220 s with a fractional launch
        self.assertEqual(min(holds), 0)

    def test_schedule_fits_the_load_with_the_existing_count_and_lengths(self):
        for earliest, launch, windows in self.sweep():
            regular = [w for w in windows if w["boundary"] is None]
            self.assertEqual([w["seconds"] for w in regular], [120.0] * 4)
            self.assertEqual(len(windows), 5)
            self.assertGreaterEqual(windows[0]["offset"], 120)
            self.assertLessEqual(windows[-1]["offset"] + windows[-1]["seconds"], campaign.LOAD_SECONDS + 80)
            for a, b in zip(windows, windows[1:]):
                self.assertGreaterEqual(b["offset"] - a["offset"] - a["seconds"], campaign.G4_WINDOW_GAP, earliest)

    def test_regular_windows_take_different_phases_of_the_closes(self):
        for earliest, launch, windows in self.sweep():
            starts = [w["offset"] for w in windows if w["boundary"] is None]
            self.assertEqual(len({s % 60 for s in starts}), 4, earliest)   # four second-of-minute phases
            self.assertEqual(len({s % 180 for s in starts}), 4, earliest)
            for a, b in zip(starts, starts[1:]):
                self.assertNotEqual((b - a) % 60, 0)
                self.assertNotEqual((b - a) % 180, 0)

    def test_guaranteed_close_coverage_per_bar_interval(self):
        """Guarantee (every launch phase): every window sees a 1m close; >= 3 windows a 3m close (>= 2 regular);
        >= 2 windows a 5m close (>= 1 regular); >= 1 window (the boundary window) a 15m and a 30m close."""
        floor_all = {"1m": 5, "3m": 3, "5m": 2, "15m": 1, "30m": 1}
        floor_regular = {"1m": 4, "3m": 2, "5m": 1}
        worst_all, worst_regular = {k: 99 for k in floor_all}, {k: 99 for k in floor_regular}
        for earliest, launch, windows in self.sweep():
            for name, period in BAR_PERIODS.items():
                seen = [closes_seen(launch, w, period) >= 1 for w in windows]
                worst_all[name] = min(worst_all[name], sum(seen))
                if name in floor_regular:
                    worst_regular[name] = min(worst_regular[name], sum(
                        s for s, w in zip(seen, windows) if w["boundary"] is None))
        self.assertEqual(worst_all, floor_all)
        self.assertEqual(worst_regular, floor_regular)

    def test_v1_spacing_shared_one_3m_phase_and_the_hold_is_needed(self):
        v1 = (120, 480, 840, 1200)
        self.assertEqual({o % 180 for o in v1}, {120})  # the RCA: one phase against every 3m/15m/30m close
        no_boundary = sum(1 for phase in range(1800) if campaign.g4_boundary(GRID + phase) is None)
        self.assertEqual(no_boundary, 219)  # closes admissible at offsets [150, 1730] only: 1581 of 1800 phases

    def test_apparatus_and_cap_receipt(self):
        apparatus = campaign.g4_apparatus(12.0)
        self.assertEqual(apparatus["schedule_version"], campaign.G4_SCHEDULE_VERSION)
        self.assertTrue(apparatus["observer_version"].endswith(".v3"))
        self.assertEqual(apparatus["observer_launch_mode"], "docker_run_isolated")
        self.assertEqual(len(apparatus["observer_sha256"]), 64)
        self.assertEqual(apparatus["launch_hold_seconds"], 12.0)
        items = [{"kind": "scope", "observer_version": "qdl.p184.g4-observer.v2"},
                 *[{"kind": "canonical_observation"}] * 3,
                 {"kind": "canonical_record_cap_reached", "records": 3, "after_seconds": 132.3}]
        receipt = campaign.g4_window_receipt(items)
        self.assertEqual({k: receipt[k] for k in ("records", "record_cap_reached", "cap_reached_after_seconds",
                                                  "observer_version")},
                         {"records": 3, "record_cap_reached": True, "cap_reached_after_seconds": 132.3,
                          "observer_version": "qdl.p184.g4-observer.v2"})
        self.assertTrue(receipt["launch"]["launch_mode"].startswith("DOCKER_EXEC_IN_QUERY"))  # pre-v3 window
        v3 = campaign.g4_window_receipt([{"kind": "launch", "launch_mode": "docker_run_isolated", "cpus": 0.5,
                                          "image_id": "sha256:" + "1" * 64}, *items])
        self.assertEqual((v3["launch"]["launch_mode"], v3["launch"]["cpus"]), ("docker_run_isolated", 0.5))
        self.assertIn("coverage", v3)
        self.assertIn("produce_split", v3)
        old = campaign.g4_window_receipt([{"kind": "scope"}, {"kind": "canonical_observation"}, {"kind": "DONE"}])
        self.assertEqual((old["record_cap_reached"], old["observer_version"]), (False, "v1 (unversioned)"))

    def test_scorecard_shows_the_apparatus_and_capped_windows(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "g4-1").mkdir()
            (d / "g4-1" / "observations.jsonl").write_text("\n".join(json.dumps(x) for x in [
                {"kind": "scope", "observer_version": "qdl.p184.g4-observer.v2"}, {"kind": "canonical_observation"},
                {"kind": "canonical_record_cap_reached", "records": 1, "after_seconds": 1.0}]) + "\n")
            body = {"g4_windows": [{"index": 1, "offset": 120.0, "seconds": 120.0, "boundary": None}],
                    "g4_apparatus": campaign.g4_apparatus(0.0)}
            (d / "campaign.json").write_text(json.dumps(body))
            g4 = {g["id"]: g for g in campaign.build_scorecard(d)["gates"]}["G4"]
            self.assertEqual(g4["g4_apparatus"]["schedule_version"], campaign.G4_SCHEDULE_VERSION)
            self.assertTrue(g4["g4_window_receipts"][0]["record_cap_reached"])
            self.assertEqual(g4["verdict"], "NOT_MEASURED")  # provenance never changes the verdict
            (d / "campaign.json").write_text(json.dumps({"g4_windows": []}))
            g4 = {g["id"]: g for g in campaign.build_scorecard(d)["gates"]}["G4"]
            self.assertEqual(g4["g4_apparatus"], {"schedule_version": "v1 (unversioned)"})


# stage100/gates.json of p184-final-campaign-20261014T150646Z (gate, status; the G5 verdict reads nothing else).
GATES_150646Z = [
    ("latency:BAR_LATEST:BINANCE", "PASS"), ("latency:BAR_LATEST:OKX", "PASS"), ("latency:L2_SNAPSHOT:BINANCE", "PASS"),
    ("latency:L2_SNAPSHOT:OKX", "PASS"), ("latency:MARK_INDEX_REFERENCE:BINANCE", "PASS"),
    ("latency:MARK_INDEX_REFERENCE:OKX", "PASS"), ("latency:QUOTE_SNAPSHOT:BINANCE", "PASS"),
    ("latency:QUOTE_SNAPSHOT:OKX", "PASS"), ("latency:TRADE_SNAPSHOT:BINANCE", "PASS"),
    ("latency:TRADE_SNAPSHOT:OKX", "FAIL"), ("requests:offered_equals_sent_plus_missed", "PASS"),
    ("requests:no_missed_ticks", "PASS"), ("requests:no_failures", "FAIL"), ("requests:no_starved_session", "PASS"),
    ("client:scheduler_lag_valid", "PASS"), ("streams:no_errors", "PASS"),
    ("streams:every_live_stream_delivered", "PASS"), ("streams:final_bar_every_bar_stream", "PASS"),
    ("cold:2500_5000_overlap_per_venue", "PASS"), ("g5:COLD_2500:BINANCE", "FAIL"), ("g5:COLD_2500:OKX", "FAIL"),
    ("g5:COLD_5000:BINANCE", "PASS"), ("g5:COLD_5000:OKX", "PASS"), ("startup:bounded", "PASS"),
    ("teardown:no_leaked_work", "PASS"), ("ts:ready_60_every_sample", "PASS"), ("ts:v2_error_not_increased", "PASS"),
    ("ts:no_auth_or_manifest_disconnect", "PASS"), ("ts:disconnects_within_baseline", "PASS"),
    ("final:fault_windows_ran", "PASS"), ("final:burst_window_measured", "PASS"),
    ("final:reconnect_window_measured", "PASS")]


def g5_before(gates):
    """The G5 scoring expression as it was at 58fb7cd (verdict, reason, measured)."""
    latency = [g for g in (gates or {}).get("gates", []) if str(g.get("gate", "")).startswith(("latency:", "cold"))]
    failed = [g["gate"] for g in latency if g.get("status") != "PASS"]
    v = "FAIL" if failed else "PASS" if latency else "NOT_MEASURED"
    return v, (f"failed: {failed}" if failed else f"{len(latency)} latency/cold gates" if latency else
               "no driver gates"), latency


class G5RowsTests(unittest.TestCase):
    def _g5(self, gates):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "stage100").mkdir()
            (d / "campaign.json").write_text(json.dumps({"g4_windows": []}))
            (d / "stage100" / "gates.json").write_text(json.dumps(gates))
            return {g["id"]: g for g in campaign.build_scorecard(d)["gates"]}["G5"]

    def test_150646z_verdict_identical_only_the_displayed_rows_differ(self):
        gates = {"status": "FAIL", "gates": [{"gate": g, "status": st} for g, st in GATES_150646Z]}
        verdict, reason, before_rows = g5_before(gates)
        g5 = self._g5(gates)
        self.assertEqual((g5["verdict"], g5["reason"]), (verdict, reason))
        self.assertEqual((g5["verdict"], g5["reason"]), ("FAIL", "failed: ['latency:TRADE_SNAPSHOT:OKX']"))  # scorecard
        shown = [m["gate"] for m in g5["measured"]]
        self.assertEqual(shown[:len(before_rows)], [m["gate"] for m in before_rows])
        self.assertEqual(shown[len(before_rows):], ["g5:COLD_2500:BINANCE", "g5:COLD_2500:OKX",
                                                    "g5:COLD_5000:BINANCE", "g5:COLD_5000:OKX"])

    def test_g5_shape_rows_never_change_the_verdict(self):
        for statuses in (("PASS", "PASS"), ("FAIL", "PASS"), ("PASS", "FAIL"), ("FAIL", "FAIL")):
            gates = {"gates": [{"gate": "latency:BAR_LATEST:OKX", "status": statuses[0]},
                               {"gate": "g5:COLD_2500:OKX", "status": statuses[1]}]}
            verdict, reason, _ = g5_before(gates)
            self.assertEqual(campaign.score_g5(gates)[0], verdict)
            # this synthetic dir has no stage-100 receipt: outside the protocol a PASS shows as NOT_MEASURED, as before
            g5 = self._g5(gates)
            self.assertEqual(g5["verdict"], verdict if verdict != "PASS" else "NOT_MEASURED")
            if verdict != "PASS":
                self.assertEqual(g5["reason"], reason)
        self.assertEqual(campaign.score_g5({"gates": [{"gate": "g5:COLD_2500:OKX", "status": "FAIL"}]})[0],
                         "NOT_MEASURED")  # as before: no latency/cold row -> NOT_MEASURED


BUDGET = Path("config/v2/p184-stage100-budget.json")


def vis(n, mx):
    return {"visibility_ms": {"n": n, "p95": mx, "max": mx}}


def addendum_for(table, rows=None, **extra):
    """A valid addendum: every owner row not in the table, REPORT_ONLY unless overridden by `rows`."""
    out = {key: {"status": "REPORT_ONLY", "reason": "test"} for key in campaign.G4_ADDENDUM_OWNER_ROWS
           if key not in table}
    out.update(rows or {})
    return {"schema": campaign.G4_ADDENDUM_SCHEMA, "frozen_before_candidate": True, "frozen_at_unix": 100.0,
            "base_budget_sha256": "b" * 64, "baseline": {"role": "baseline"}, "rows": out,
            "rule": {"statistic": "max", "factor": campaign.G4_ADDENDUM_FACTOR,
                     "min_samples_per_window": campaign.G4_ADDENDUM_MIN_SAMPLES,
                     "report_only_intervals": list(campaign.G4_REPORT_ONLY_INTERVALS)}, **extra}


class G4AddendumScoringTests(unittest.TestCase):
    TABLE = {"trade|OKX|": {"candidate_p95_limit_ms": 100.0}}
    GATED = {"bar|OKX|5m": {"status": "GATED", "candidate_max_limit_ms": 360.0},
             "quote|OKX_INVERSE|": {"status": "GATED", "candidate_max_limit_ms": 200.0}}

    def windows(self, bar5m=(6, 300.0), quote_inv=(60, 150.0), extra=None):
        summary = {"trade|OKX|": vis(5, 90.0), "bar|OKX|5m": vis(*bar5m), "bar|OKX|1h": vis(1, 9e9)}
        summary.update(extra or {})
        return [{"summary": summary, "inverse_rows": {"quote|OKX_INVERSE|": {"n": quote_inv[0], "p95": 1.0,
                                                                            "max": quote_inv[1]}}}]

    def score(self, windows):
        return campaign.score_g4(windows, self.TABLE, addendum_for(self.TABLE, self.GATED))

    def test_gated_rows_pass_and_report_only_never_blocks(self):
        state, rows, why = self.score(self.windows())
        self.assertEqual(state, "PASS", why)
        by_row = {r["row"]: r for r in rows}
        self.assertEqual(by_row["trade|OKX|"], {"row": "trade|OKX|", "p95_ms_per_window": [90.0], "n_per_window": [5],
                                                "limit_p95_ms": 100.0, "verdict": "PASS"})
        self.assertEqual(by_row["bar|OKX|5m"]["verdict"], "PASS")
        self.assertEqual(by_row["bar|OKX|1h"]["verdict"], "REPORT_ONLY")  # 9e9 ms, still never fails
        self.assertEqual(by_row["bar|OKX|1h"]["max_ms_per_window"], [9e9])

    def test_gated_row_over_its_max_limit_fails(self):
        state, _, why = self.score(self.windows(quote_inv=(60, 250.0)))
        self.assertEqual(state, "FAIL")
        self.assertIn("quote|OKX_INVERSE|", why[0])

    def test_small_windows_are_reported_not_scored(self):
        # 4 samples over the limit: not scored; no window with >= 5 samples -> the gated row is NOT_MEASURED.
        state, rows, why = self.score(self.windows(bar5m=(4, 9999.0)))
        self.assertEqual(state, "NOT_MEASURED")
        self.assertEqual({r["row"]: r for r in rows}["bar|OKX|5m"]["gated_windows"], [False])
        self.assertTrue(any(">= 5 samples" in item for item in why))

    def test_row_in_neither_table_still_blocks(self):
        state, _, why = self.score(self.windows(extra={"bar|OKX|7m": vis(9, 1.0)}))
        self.assertEqual(state, "NOT_MEASURED")
        self.assertIn("without a frozen baseline: bar|OKX|7m", why[-1])

    def test_without_addendum_scoring_is_unchanged(self):
        windows = self.windows()
        self.assertEqual(campaign.score_g4(windows, self.TABLE), campaign.score_g4(windows, self.TABLE, None))
        state, rows, why = campaign.score_g4(windows, self.TABLE)
        self.assertEqual(state, "NOT_MEASURED")
        self.assertEqual([r["row"] for r in rows], ["trade|OKX|"])
        self.assertIn("bar|OKX|1h, bar|OKX|5m, quote|OKX_INVERSE|", why[0])

    def test_validation_refuses_overlap_rule_drift_and_gated_report_only_interval(self):
        table = {"bar|OKX|5m": {"candidate_p95_limit_ms": 1.0}}
        self.assertEqual(campaign.validate_g4_addendum(addendum_for(table), table), [])
        overlapping = addendum_for(table, {"bar|OKX|5m": {"status": "REPORT_ONLY"}})
        self.assertTrue(any("overlap" in p for p in campaign.validate_g4_addendum(overlapping, table)))
        drift = addendum_for(table)
        drift["rule"]["statistic"] = "p95"
        self.assertTrue(campaign.validate_g4_addendum(drift, table))
        gated_1h = addendum_for(table, {"bar|OKX|1h": {"status": "GATED", "candidate_max_limit_ms": 5.0}})
        self.assertTrue(any("report-only interval" in p for p in campaign.validate_g4_addendum(gated_1h, table)))

    def _campaign_dir(self, tmp, addendum_path, started=200.0):
        d = Path(tmp) / "candidate"
        (d / "g4-1").mkdir(parents=True)
        budget = json.loads(BUDGET.read_text())
        table = budget["g4_delivery_visibility"]["table"]
        summary = {key: vis(5, 1.0) for key in table}
        summary["bar|BINANCE|5m"] = vis(6, 50.0)
        (d / "g4-1-summary.json").write_text(json.dumps(summary))
        (d / "campaign.json").write_text(json.dumps({
            "started_unix": started, "budget_path": str(BUDGET.resolve()), "g4_addendum_path": str(addendum_path),
            "budget_sha256": campaign.sha256_file(BUDGET), "g4_windows": [{"index": 1}]}))
        return d, table

    def test_scorecard_applies_only_an_addendum_frozen_before_the_campaign(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "addendum.json"
            d, table = self._campaign_dir(tmp, path)
            g4 = {g["id"]: g for g in campaign.build_scorecard(d)["gates"]}["G4"]
            self.assertNotIn("report_only_rows", g4)  # no file: the original G4 entry
            self.assertIn("bar|BINANCE|5m", g4["reason"])
            add = addendum_for(table, {"bar|BINANCE|5m": {"status": "GATED", "candidate_max_limit_ms": 60.0}},
                               base_budget_sha256=campaign.sha256_file(BUDGET))
            path.write_text(json.dumps(add))
            g4 = {g["id"]: g for g in campaign.build_scorecard(d)["gates"]}["G4"]
            measured = {r["row"]: r["verdict"] for r in g4["measured"]}
            self.assertEqual(measured["bar|BINANCE|5m"], "PASS")
            self.assertEqual(len([r for r in g4["measured"] if r.get("basis") != "addendum"]), len(table))
            self.assertTrue(all(r["verdict"] == "REPORT_ONLY" for r in g4["report_only_rows"]))
            self.assertEqual(g4["g4_addendum"]["sha256"], campaign.sha256_file(path))
            path.write_text(json.dumps({**add, "frozen_at_unix": 300.0}))  # frozen after the campaign started
            g4 = {g["id"]: g for g in campaign.build_scorecard(d)["gates"]}["G4"]
            self.assertIn("not frozen before this campaign started", g4["reason"])
            self.assertNotEqual(g4["verdict"], "PASS")

    def test_baseline_plan_runs_driver_and_g4_only(self):
        import argparse
        args = argparse.Namespace(state_tools="/tools", load_timeout=2400, budget="b.json", writer="w1", g11=False,
                                  g11_wait=900, baseline=True, sessions=50)
        plan = {"binding": None, "market_redis": {"container": "r", "db": 1},
                "heartbeat_redis": {"container": "h", "db": 0}}
        commands = campaign.command_plan(args, plan, Path("/c"), 0.0)
        self.assertTrue(all(commands[name] is None for name in campaign.BASELINE_SKIPPED))
        driver = commands["driver"]
        self.assertEqual(driver[driver.index("--sessions") + 1], "50")
        self.assertEqual(driver[driver.index("--output") + 1], "/c/stage50")
        self.assertTrue(commands["g4_windows"])

    def test_score_refuses_a_baseline_campaign(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "campaign.json").write_text(json.dumps({"role": "baseline"}))
            with self.assertRaises(SystemExit):
                campaign.main(["score", "--campaign-dir", tmp])


class G4AddendumFreezeTests(unittest.TestCase):
    def _baseline(self, tmp, role="baseline", marker=True, budget_sha=None):
        from scripts import p184_freeze_g4_addendum as freeze
        d = Path(tmp) / "run"
        (d / "g4-1").mkdir(parents=True)
        (d / "g4-2").mkdir()
        (d / "g4-1-summary.json").write_text(json.dumps({"bar|BINANCE|5m": vis(5, 100.0), "bar|OKX|5m": vis(2, 1.0),
                                                         "bar|BINANCE|1h": vis(50, 9.0)}))
        (d / "g4-2-summary.json").write_text(json.dumps({"bar|BINANCE|5m": vis(7, 150.0)}))
        obs = [{"kind": "canonical_observation", "feed": "quote", "identity": {"native_symbol": "BTC-USD-SWAP"},
                "source_event_ns": i * S, "observer_receipt_wall_ns": i * S + (100 + i) * 1_000_000}
               for i in range(1, 7)]
        (d / "g4-2" / "observations.jsonl").write_text("\n".join(json.dumps(o) for o in obs) + "\n")
        body = {"g4_windows": [{"index": 1, "boundary": None}, {"index": 2, "boundary": 1800}],
                "budget_sha256": budget_sha or campaign.sha256_file(BUDGET), "started_unix": 1.0}
        if role:
            body["role"] = role
        (d / "campaign.json").write_text(json.dumps(body))
        if marker:
            (d / "baseline.json").write_text(json.dumps({"role": "baseline"}))
        return freeze, d

    def test_freeze_derives_gated_and_report_only_rows_from_the_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            freeze, d = self._baseline(tmp)
            out = freeze.freeze(d, BUDGET, now=50.0)
            table = json.loads(BUDGET.read_text())["g4_delivery_visibility"]["table"]
            self.assertEqual(campaign.validate_g4_addendum(out, table), [])
            rows = out["rows"]
            self.assertFalse(set(rows) & set(table))
            self.assertEqual(sorted(out["excluded_already_in_frozen_table"]),
                             ["bar|BINANCE|15m", "bar|BINANCE|30m", "bar|OKX|30m"])
            self.assertEqual(rows["bar|BINANCE|5m"]["status"], "GATED")
            self.assertEqual(rows["bar|BINANCE|5m"]["candidate_max_limit_ms"], 180.0)  # max 150 x 1.2, not p95
            self.assertEqual(rows["bar|BINANCE|1h"]["status"], "REPORT_ONLY")  # 50 samples, interval report-only
            self.assertEqual(rows["bar|OKX|5m"]["status"], "REPORT_ONLY")  # 2 samples < 5
            self.assertEqual(rows["quote|OKX_INVERSE|"]["status"], "GATED")  # from observations.jsonl
            self.assertEqual(rows["quote|OKX_INVERSE|"]["candidate_max_limit_ms"], round(106.0 * 1.2, 1))
            self.assertEqual(rows["book_delta|OKX_INVERSE|"]["status"], "REPORT_ONLY")  # never observed
            self.assertEqual(out["base_budget_sha256"], campaign.sha256_file(BUDGET))
            self.assertEqual(out["baseline"]["role"], "baseline")

    def test_freeze_refuses_candidate_unfinished_and_other_budget_dirs(self):
        for kwargs, message in ((dict(role=None), "not a baseline"), (dict(role="candidate"), "not a baseline"),
                                (dict(marker=False), "not complete"), (dict(budget_sha="0" * 64), "different budget")):
            with self.subTest(kwargs=kwargs), tempfile.TemporaryDirectory() as tmp:
                freeze, d = self._baseline(tmp, **kwargs)
                with self.assertRaisesRegex(ValueError, message):
                    freeze.freeze(d, BUDGET)

    def test_freeze_cli_writes_once_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            freeze, d = self._baseline(tmp)
            output = Path(tmp) / "addendum.json"
            argv = ["--baseline-campaign", str(d), "--budget", str(BUDGET), "--output", str(output)]
            with unittest.mock.patch("sys.stdout", new_callable=io.StringIO):
                self.assertEqual(freeze.main(argv), 0)
            first = output.read_bytes()
            with self.assertRaises(SystemExit):
                freeze.main(argv)
            self.assertEqual(output.read_bytes(), first)


def _manifest(directory: Path, name: str, consumer: str, revision: int) -> Path:
    path = directory / name
    path.write_text(f"apiVersion: qdl/v2\nkind: DataRequirement\nmetadata:\n  id: {consumer}\n  owner: t\n"
                    f"  revision: {revision}\nspec: {{}}\n")
    return path


def _query_inspect(realms: Path, manifests: str) -> dict:
    return {"Config": {"Env": [f"QDL_STABLE_CONSUMER_MANIFESTS={manifests}", "OTHER=x"]},
            "Mounts": [{"Source": str(realms), "Destination": "/consumer-realms"},
                       {"Source": "/elsewhere", "Destination": "/runtime"}]}


class ManifestRevisionPreflightTests(unittest.TestCase):
    """Driver token revision vs the live Query manifest (2026-10-15T075842Z: stale r15 override vs live r16)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.realms = self.root / "realms"
        self.realms.mkdir()
        _manifest(self.realms, "alpha.binance.paper.stable-r16.yaml", "alpha.binance.paper.stable", 16)
        _manifest(self.realms, "alpha.okx.paper.stable-r15.yaml", "alpha.okx.paper.stable", 15)
        self.live = ("/app/consumers/stable/monitoring-multivenue.yaml:/consumer-realms/alpha.binance.paper.stable-"
                     "r16.yaml:/consumer-realms/alpha.okx.paper.stable-r15.yaml")
        self.okx = str(_manifest(self.root, "okx-r15.yaml", "alpha.okx.paper.stable", 15))

    def tearDown(self):
        self.tmp.cleanup()

    def profile(self, binance_revision: int) -> dict:
        binance = _manifest(self.root, f"bn-r{binance_revision}.yaml", "alpha.binance.paper.stable", binance_revision)
        return {"identities": [{"id": "alpha.binance.paper.stable"}, {"id": "alpha.okx.paper.stable"}],
                "manifest_overrides": {"alpha.binance.paper.stable": str(binance),
                                       "alpha.okx.paper.stable": self.okx}}

    def check(self, profile, inspects):
        return campaign.manifest_revision_check(profile, inspects, repo_root=self.root)

    def test_match_has_no_problem(self):
        inspect = _query_inspect(self.realms, self.live)
        rows, problems = self.check(self.profile(16), {"q1": inspect, "q2": inspect})
        self.assertEqual(problems, [])
        self.assertEqual({row["consumer"]: row["driver_revision"] for row in rows},
                         {"alpha.binance.paper.stable": 16, "alpha.okx.paper.stable": 15})
        self.assertEqual(rows[0]["live"]["q1"]["host_path"], str(self.realms / "alpha.binance.paper.stable-r16.yaml"))

    def test_stale_override_is_a_problem(self):
        _, problems = self.check(self.profile(15), {"q1": _query_inspect(self.realms, self.live)})
        self.assertEqual(len(problems), 1)
        self.assertIn("alpha.binance.paper.stable driver revision 15", problems[0])
        self.assertIn("live revision 16", problems[0])

    def test_unresolvable_fails_closed(self):
        profile = self.profile(16)
        # Query container not inspectable.
        self.assertTrue(any("cannot inspect" in p for p in self.check(profile, {"q1": None})[1]))
        # No Query container at all.
        self.assertTrue(self.check(profile, {})[1])
        # Live manifest path not under any bind mount (image-baked copy): unresolvable.
        baked = "/app/consumers/stable/alpha-binance-paper.yaml:/consumer-realms/alpha.okx.paper.stable-r15.yaml"
        problems = self.check(profile, {"q1": _query_inspect(self.realms, baked)})[1]
        self.assertEqual(len(problems), 1)
        self.assertIn("alpha.binance.paper.stable live manifest on q1 unresolvable (lives in the image", problems[0])
        # Consumer not loaded by Query at all.
        only_okx = "/consumer-realms/alpha.okx.paper.stable-r15.yaml"
        self.assertIn("(not loaded)", self.check(profile, {"q1": _query_inspect(self.realms, only_okx)})[1][0])
        # Driver manifest unreadable.
        profile["manifest_overrides"]["alpha.binance.paper.stable"] = str(self.root / "missing.yaml")
        problems = self.check(profile, {"q1": _query_inspect(self.realms, self.live)})[1]
        self.assertTrue(any("unreadable or for another consumer" in p for p in problems))

    def test_identity_without_override_uses_repo_default(self):
        (self.root / "consumers/stable").mkdir(parents=True)
        _manifest(self.root / "consumers/stable", "alpha-binance-paper.yaml", "alpha.binance.paper.stable", 14)
        profile = {"identities": [{"id": "alpha.binance.paper.stable"}], "manifest_overrides": {}}
        rows, problems = self.check(profile, {"q1": _query_inspect(self.realms, self.live)})
        self.assertEqual(rows[0]["driver_source"], "repo default (client image baked copy)")
        self.assertEqual(len(problems), 1)
        self.assertIn("driver revision 14", problems[0])

    def test_preflight_refuses_on_mismatch(self):
        profile_path = self.root / "profile.json"
        profile = self.profile(15)
        profile["query_containers"] = ["q1"]
        profile_path.write_text(json.dumps(profile))
        args = unittest.mock.Mock(base_profile=str(profile_path), writer="w")
        inspects = {"q1": _query_inspect(self.realms, self.live), "w": None}
        with unittest.mock.patch.object(campaign, "docker_inspect", side_effect=inspects.get) as inspect:
            plan = campaign.preflight(args)
        self.assertIn(unittest.mock.call("q1"), inspect.call_args_list)
        self.assertTrue(any("driver revision 15" in p for p in plan["problems"]))
        self.assertIn("writer not running", plan["problems"])


if __name__ == "__main__":
    unittest.main()


def _block(n, p95, mx, verdict="INSIDE", **extra):
    return {"n": n, "p50": 1.0, "p95": p95, "p99": p95, "max": mx, "verdict": verdict, **extra}


class NativeWriterScoringTests(unittest.TestCase):
    """The production native writer (TS N1 B12): TRADE/QUOTE scored from its summary, Python feeds unchanged."""

    def transit(self, *, native_gaps=(), native_cap=1, python_cap=0, trade_max=40.0):
        g7_ok = {"verdict": {"sdk_to_writer_ms": "INSIDE", "writer_to_redis_ms": "INSIDE"},
                 "sdk_to_writer_ms": {"n": 5, "p95": 1.0}, "writer_to_redis_ms": {"n": 5, "p95": 1.0}}
        summary_rows = {f"{feed}|{venue}": _block(1000, 30.0, trade_max if feed == "TRADE" else 40.0,
                                                  "OUTSIDE" if trade_max >= 1500 and feed == "TRADE" else "INSIDE",
                                                  basis="NATIVE_SUMMARY applied.source_event_to_ack")
                        for feed in ("TRADE", "QUOTE") for venue in campaign.VENUES}
        raw_rows = {f"{feed}|{venue}": _block(10, 20.0, 30.0) for feed in ("TRADE", "QUOTE", "BOOK_DELTA",
                                                                          "BOOK_SNAPSHOT") for venue in campaign.VENUES}
        return {"records": 100, "dropped_at_export_total": 0,
                "stop_reasons": {"CAP": native_cap + python_cap, "EXPIRY": 2},
                "files_by_kind": {"native": {"files": 2, "stop_reasons": {"CAP": native_cap},
                                             "dropped_at_export_total": 0, "first_start_ns": 1000 * S,
                                             "last_start_ns": 1100 * S},
                                  "python": {"files": 4, "stop_reasons": {"CAP": python_cap, "EXPIRY": 2},
                                             "dropped_at_export_total": 0}},
                "transit": raw_rows, "duplicate_applied_event_id": {},
                "g7": {feed: g7_ok for feed in campaign.G7_FEEDS},
                "native_summary": {"coverage_gaps": list(native_gaps), "transit": summary_rows,
                                   "g7": {"TRADE": g7_ok, "QUOTE": g7_ok},
                                   "window_scope": {"pre_load_seconds": 300.0, "post_load_seconds": 60.0}}}

    def card(self, transit, native_feeds=("TRADE", "QUOTE")):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "stage100").mkdir()
            (d / "trace").mkdir()
            trace = {"until_unix_ns": 10**19}
            if native_feeds is not None:
                trace["native_feeds"] = list(native_feeds)
            (d / "campaign.json").write_text(json.dumps({"trace": trace, "g4_windows": []}))
            (d / "stage100" / "receipt.json").write_text(json.dumps({
                "stage": 100, "final": True, "load_window_unix_s": [1000.0, 2900.0], "fault_windows": {"BURST": [1, 2]}}))
            (d / "stage100" / "gates.json").write_text(json.dumps({"status": "PASS", "gates": []}))
            (d / "trace" / "transit-load.json").write_text(json.dumps(transit))
            mark = {"venues": {v: {"new_event_transit_ms": {"n": 5, "p95": 1.0, "max": 2.0}, "g2_verdict": "INSIDE"}
                               for v in campaign.VENUES}}
            (d / "trace" / "mark-load.json").write_text(json.dumps(mark))
            return {g["id"]: g for g in campaign.build_scorecard(d)["gates"]}

    def test_native_raw_cap_is_a_sample_not_a_gap_and_rows_come_from_the_summary(self):
        gates = self.card(self.transit())
        self.assertEqual(gates["G1"]["verdict"], "PASS")
        self.assertEqual(gates["G2"]["verdict"], "PASS")
        self.assertEqual(gates["G7"]["verdict"], "PASS")
        trade = [r for r in gates["G2"]["measured"] if r["feed_venue"] == "TRADE|OKX"][0]
        self.assertEqual((trade["n"], trade["basis"]), (1000, "NATIVE_SUMMARY applied.source_event_to_ack"))
        self.assertEqual(trade["raw_sample"]["n"], 10)
        self.assertEqual(gates["G2"]["window_scope"]["pre_load_seconds"], 300.0)
        # The same files without the native writer keep the old rule: a CAP stop is a coverage gap.
        legacy = self.transit()
        legacy.pop("native_summary")
        gates = self.card(legacy, native_feeds=None)
        self.assertEqual(gates["G2"]["verdict"], "NOT_MEASURED")
        self.assertIn("file cap", gates["G2"]["reason"])

    def test_python_cap_and_native_gaps_still_block(self):
        gates = self.card(self.transit(python_cap=1))
        self.assertEqual(gates["G1"]["verdict"], "NOT_MEASURED")
        self.assertIn("python writer", gates["G1"]["reason"])
        gates = self.card(self.transit(native_gaps=["native activation x/1 does not span the load window"]))
        self.assertEqual(gates["G1"]["verdict"], "PASS")
        self.assertEqual(gates["G2"]["verdict"], "NOT_MEASURED")
        self.assertEqual(gates["G7"]["verdict"], "NOT_MEASURED")
        missing = self.transit()
        missing.pop("native_summary")
        gates = self.card(missing)
        self.assertEqual(gates["G2"]["verdict"], "NOT_MEASURED")
        self.assertIn("no native writer summary", gates["G2"]["reason"])

    def test_thresholds_are_unchanged_for_native_rows(self):
        gates = self.card(self.transit(trade_max=1600.0))
        self.assertEqual(gates["G2"]["verdict"], "FAIL")
        self.assertIn("TRADE|OKX", gates["G2"]["reason"])

    def test_g10_native_trade_sample_can_fail_never_pass(self):
        gates = self.card(self.transit())
        self.assertEqual(gates["G10"]["verdict"], "NOT_MEASURED")
        self.assertIn("sample", gates["G10"]["reason"])
        self.assertIn("native raw sample spans 100 s", gates["G10"]["measured"]["duplicates_applied_scope"])
        g10 = {"trade_gap": 0, "trade_replay_expired": 0, "trade_overflow": 0, "log_polls": 10, "log_unreadable": 0}
        self.assertEqual(campaign.score_g10(g10, {"TRADE|OKX": 1}, [], sample_scope="s")[0], "FAIL")
        self.assertEqual(campaign.score_g10(g10, {}, [], sample_scope="s")[0], "NOT_MEASURED")
        self.assertEqual(campaign.score_g10(g10, {}, [])[0], "PASS")

    def test_trace_coverage_by_writer_kind(self):
        t = self.transit()
        self.assertTrue(campaign._trace_coverage(t, [0, 10 * S], 11 * S))
        self.assertEqual(campaign._trace_coverage(t, [0, 10 * S], 11 * S, raw_kinds=("python",)), [])
        self.assertEqual(campaign.native_feeds_of({}, t), ("QUOTE", "TRADE"))
        self.assertEqual(campaign.native_feeds_of({"trace": {"native_feeds": []}}, t), ())

    def test_preflight_native_checks(self):
        now = 1_792_000_000 * S
        env = {"MARKET_DATA_NATIVE_FEEDS": "TRADE,QUOTE", "TS_DELIVERY_TRACE_MAX_FILES": "1"}
        started = "2026-10-15T13:00:00.5Z"
        start_ns = campaign.docker_time_ns(started)
        problems, warnings = campaign.native_trace_checks(env, start_ns + 3000 * S, start_ns + 60 * S, started,
                                                          g11=True)
        self.assertEqual(problems, [])
        self.assertTrue(any("caps each Python worker" in w for w in warnings))
        problems, _ = campaign.native_trace_checks({}, now, now, started, g11=True)
        self.assertIn("not in native mode", problems[0])
        problems, _ = campaign.native_trace_checks({**env, "TS_DELIVERY_TRACE_MAX_FILES": "96"}, start_ns + 3000 * S,
                                                   start_ns, started, g11=True)
        self.assertTrue(any("MAX_FILES == 1" in p for p in problems))
        problems, warnings = campaign.native_trace_checks({**env, "TS_DELIVERY_TRACE_MAX_FILES": "96"},
                                                         start_ns + 3000 * S, start_ns + 60 * S, started, g11=True,
                                                         writer_running_clean=True)
        self.assertFalse([p for p in problems if "MAX_FILES" in p])
        self.assertTrue(any("running native writer" in w for w in warnings))
        problems, warnings = campaign.native_trace_checks(
            {**env, "TS_DELIVERY_TRACE_MAX_FILES": "96", "TS_DELIVERY_TRACE_NATIVE_MAX_FILES": "1"},
            start_ns + 3000 * S, start_ns, started, g11=False)
        self.assertEqual(problems, [])
        self.assertTrue(any("native-only key" in w for w in warnings))
        self.assertTrue(any("--g11" in w for w in warnings))
        problems, _ = campaign.native_trace_checks(env, start_ns + 4000 * S, start_ns, started, g11=True)
        self.assertEqual(len(problems), 2)  # > 3600 s from now and from the writer start
        problems, _ = campaign.native_trace_checks({**env, "TS_DELIVERY_TRACE_NATIVE_MAX_FILES": "2"},
                                                   start_ns + 3000 * S, start_ns, started, g11=True)
        self.assertTrue(any("exactly 1 file" in p for p in problems))

    def test_trace_fit_shortens_the_automatic_g11_wait(self):
        now = 1_792_000_000 * S
        wait, problems, detail = campaign.trace_fit(now + 3500 * S, now, launch_hold=100.0, load_timeout=2400,
                                                    g11=True, g11_wait=None)
        self.assertEqual(problems, [])
        self.assertEqual(wait, 3500 - (5 + 100 + 2400 + 60 + 45))
        self.assertLessEqual(detail["planned_seconds"], 3500)
        wait, problems, _ = campaign.trace_fit(now + 3500 * S, now, launch_hold=100.0, load_timeout=2400,
                                               g11=True, g11_wait=900)
        self.assertTrue(problems)
        wait, problems, _ = campaign.trace_fit(now + 2700 * S, now, launch_hold=100.0, load_timeout=2400,
                                               g11=True, g11_wait=None)
        self.assertTrue(any("G11 wait" in p for p in problems))
        self.assertEqual(campaign.trace_fit(now - 10 * S, now, launch_hold=0.0, load_timeout=2400, g11=True,
                                            g11_wait=None)[0], 0)
        wait, problems, _ = campaign.trace_fit(now + 2400 * S, now, launch_hold=0.0, load_timeout=2400,
                                               g11=False, g11_wait=None)
        self.assertTrue(any("exceeds the trace window" in p for p in problems))
        self.assertEqual(campaign.trace_fit(now + 3600 * S, now, launch_hold=0.0, load_timeout=2400, g11=False,
                                            g11_wait=None)[1], [])

    def test_trace_analysis_passes_the_native_summary_glob(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "stage100").mkdir()
            (d / "stage100" / "receipt.json").write_text(json.dumps({"load_window_unix_s": [1.0, 2.0]}))
            calls = []

            def fake(cmd, **_):
                calls.append(cmd)
                return unittest.mock.Mock(stdout="{}", returncode=0, stderr="")
            with unittest.mock.patch.object(campaign.subprocess, "run", fake):
                campaign.run_trace_analysis({"trace": {"dir_on_host": "/x", "until_unix_ns": 5,
                                                       "native_feeds": ["TRADE"]}}, d, image="img")
        self.assertIn("/trace/summary-native-*-5.json", calls[0])
        self.assertNotIn("--native-summary", calls[1])


class G11NativeCursorTests(unittest.TestCase):
    def test_cursor_dump_reads_python_and_native_cursor_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "fixed-workers-v1"
            for sub, key, offset in (("count-2-worker-0", "c|u1|BOOK_DELTA||p", 3), ("native-0", "c|u2|TRADE||p", 7),
                                     ("native-1", "c|u3|QUOTE||p", 9), ("other", "c|u4|TRADE||p", 1)):
                (root / sub).mkdir(parents=True)
                (root / sub / "c.json").write_text(json.dumps({"schema": "qdl.sdk-cursors.v2", "items": {
                    key: {"offset": offset, "token": "TEST-not-printed"}}}))
            code = g11.CURSOR_DUMP.replace("/app/state/data-layer-v2/cursors", tmp)
            out = io.StringIO()
            with unittest.mock.patch("sys.stdout", out):
                exec(compile(code, "<cursor-dump>", "exec"), {})
        self.assertEqual(json.loads(out.getvalue()), {"c|u1|BOOK_DELTA||p": 3, "c|u2|TRADE||p": 7,
                                                      "c|u3|QUOTE||p": 9})
        self.assertNotIn("TEST-not-printed", out.getvalue())
