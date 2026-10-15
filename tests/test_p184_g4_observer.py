"""P18.4 G4 observer: filter and offline summary (no Kafka)."""
import json
import subprocess
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from scripts import p184_g4_observer as observer
from scripts.p184_g4_observer import ALL_FEEDS, OBSERVER_VERSION, Sampler, accepted, sampling_key, summarize


def obs(feed, venue, source, received, seen, interval=None):
    return json.dumps({"kind": "canonical_observation", "feed": feed, "identity": {"venue": venue},
                       "source_event_ns": source, "received_at_ns": received, "observer_receipt_wall_ns": seen,
                       "interval": interval})


class ObserverTests(unittest.TestCase):
    def test_default_filter_takes_every_symbol_and_feed_of_both_venues(self):
        feeds = set(ALL_FEEDS.split(","))
        self.assertTrue(accepted("OKX", "DOGE-USDT-SWAP", "book_delta", None, feeds))
        self.assertTrue(accepted("BINANCE", "BNBUSDT", "bar", None, feeds))
        self.assertFalse(accepted("KRAKEN", "XBTUSD", "trade", None, feeds))
        self.assertFalse(accepted("OKX", "BTC-USDT-SWAP", "trade", {"ETH-USDT-SWAP"}, feeds))

    def test_summary_groups_by_feed_venue_interval_and_counts_missing_and_negative(self):
        lines = [obs("trade", "OKX", 1_000_000_000, 1_030_000_000, 1_150_000_000),
                 obs("trade", "OKX", 2_000_000_000, 2_020_000_000, 1_990_000_000),
                 obs("trade", "OKX", None, 1, 1),
                 obs("bar", "BINANCE", 3_000_000_000, 3_010_000_000, 3_200_000_000, interval="1m"),
                 json.dumps({"kind": "lag"})]
        out = summarize(lines)
        trade = out["trade|OKX|"]
        self.assertEqual(trade["visibility_ms"]["n"], 2)
        self.assertEqual(trade["visibility_ms"]["max"], 150.0)
        self.assertEqual(trade["visibility_ms"]["negative"], 1)  # skewed sample kept and counted, not clamped
        self.assertEqual(trade["missing_clock"], 1)
        self.assertEqual(out["bar|BINANCE|1m"]["venue_to_dl_ms"]["p50"], 10.0)
        self.assertIsNone(trade["visibility_ms"]["p99"])  # p99 only with n >= 100


S = 1_000_000_000


class SamplingKeyTests(unittest.TestCase):
    """v2 sampling: BAR keyed per interval, every other feed unchanged."""

    def test_simultaneous_closes_of_different_intervals_are_all_kept(self):
        sampler = Sampler(1000)
        closes = ["1m", "3m", "5m", "15m", "30m"]
        kept = [sampler.admit("OKX", "BTC-USDT-SWAP", "bar", interval, 10 * S + i) for i, interval in enumerate(closes)]
        self.assertEqual(kept, [True] * 5)
        # the same interval inside the sample period is still rate limited
        self.assertFalse(sampler.admit("OKX", "BTC-USDT-SWAP", "bar", "3m", 10 * S + 500_000_000))
        self.assertTrue(sampler.admit("OKX", "BTC-USDT-SWAP", "bar", "3m", 11 * S + 1))

    def test_v1_key_collapsed_the_same_closes_to_one(self):
        keys = {sampling_key("BINANCE", "BTCUSDT", "bar", i) for i in ("3m", "5m", "15m", "30m")}
        self.assertEqual(len(keys), 4)
        self.assertEqual(len({k[:3] for k in keys}), 1)  # the v1 key (venue, symbol, feed) had one slot

    def test_non_bar_feeds_keep_the_v1_key_and_rate(self):
        for feed in ("trade", "quote", "mark_index_price", "book_delta", "book_snapshot"):
            self.assertEqual(sampling_key("OKX", "ETH-USDT-SWAP", feed, "1m"), ("OKX", "ETH-USDT-SWAP", feed))
        sampler = Sampler(1000)
        self.assertTrue(sampler.admit("OKX", "ETH-USDT-SWAP", "trade", None, 5 * S))
        self.assertFalse(sampler.admit("OKX", "ETH-USDT-SWAP", "trade", None, 5 * S + 999_999_999))
        self.assertTrue(sampler.admit("OKX", "ETH-USDT-SWAP", "trade", None, 6 * S))
        self.assertTrue(sampler.admit("OKX", "ETH-USDT-SWAP", "book_snapshot", None, 6 * S))
        self.assertTrue(sampler.admit("OKX", "ETH-USDT-SWAP", "book_snapshot", None, 6 * S))  # unsampled, as before
        self.assertTrue(OBSERVER_VERSION.endswith(".v5"))  # v3 launch, v4 cap, v5 own-cgroup evidence; not this sampling


QUERY_INSPECT = {
    "Image": "sha256:" + "7d" * 32, "Config": {"Image": "qdl-v2-python:rc", "User": "10001:10001"},
    "NetworkSettings": {"Networks": {"executor_network": {}, "qdl_v2_stable_candidate_stable_internal": {}}},
    "Mounts": [
        {"Type": "bind", "Source": "/host/releases/v2", "Destination": "/kn", "RW": False},
        {"Type": "bind", "Source": "/host/state/query-1", "Destination": "/var/lib/qdl-kn", "RW": True},
        {"Type": "volume", "Name": "stable_tls", "Source": "/var/lib/docker/volumes/stable_tls/_data",
         "Destination": "/stable-certs", "RW": False},
        {"Type": "bind", "Source": "/host/realms", "Destination": "/consumer-realms", "RW": False}]}
CONFIG = {"projector": {"ssl.ca.location": "/kn/tls/broker-ca.crt", "ssl.certificate.location": "/kn/tls/p.crt",
                        "ssl.key.location": "/kn/tls/p.key", "bootstrap.servers": "kafka1:9092"},
          "core": {"ssl.ca.location": "/stable-certs/core/ca.crt", "ssl.key.location": "/stable-certs/core/c.key"}}


class IsolatedLaunchTests(unittest.TestCase):
    """v3: own `docker run --rm` container of the query image, read-only TLS mounts only, bounded, removed by name."""

    def test_run_plan_reuses_image_and_networks_and_mounts_only_tls_read_only(self):
        name = observer.container_name("p184-final-campaign-20261015T000000Z", Path("/x/g4-3"))
        self.assertEqual(name, "qdl-p184-g4obs-p184-final-campaign-20261015T000000Z-g4-3")
        argv, prov = observer.isolated_run_plan(QUERY_INSPECT, observer.tls_paths(CONFIG), name, 0.5, "512m")
        self.assertEqual(argv[:3], ["docker", "run", "--rm"])
        self.assertEqual(argv[-1], QUERY_INSPECT["Image"])  # the image id query_kn_1 runs, not a tag
        mounts = [argv[i + 1] for i, a in enumerate(argv) if a == "--mount"]
        self.assertEqual(mounts, ["type=bind,src=/host/releases/v2,dst=/kn,readonly",
                                  "type=volume,src=stable_tls,dst=/stable-certs,readonly,volume-nocopy"])
        self.assertNotIn("/var/lib/qdl-kn", " ".join(argv))  # the rw state mount is never taken
        self.assertEqual([argv[i + 1] for i, a in enumerate(argv) if a == "--network"],
                         sorted(QUERY_INSPECT["NetworkSettings"]["Networks"]))
        for flag, value in (("--cpus", "0.5"), ("--memory", "512m"), ("--memory-swap", "512m"),
                            ("--name", name), ("--user", "10001:10001"), ("--entrypoint", "python")):
            self.assertEqual(argv[argv.index(flag) + 1], value)
        self.assertIn("--read-only", argv)
        self.assertEqual((prov["launch_mode"], prov["image_id"], prov["cpus"]),
                         ("docker_run_isolated", QUERY_INSPECT["Image"], 0.5))
        self.assertTrue(all(m["readonly"] for m in prov["mounts"]))

    def test_a_path_no_query_mount_holds_is_refused(self):
        with self.assertRaises(RuntimeError):
            observer.isolated_run_plan(QUERY_INSPECT, ["/elsewhere/ca.crt"], "n", 0.5, "512m")
        with self.assertRaises(RuntimeError):  # prefix match is per path component
            observer.isolated_run_plan(QUERY_INSPECT, ["/knx/ca.crt"], "n", 0.5, "512m")

    def _launch(self, out, lines):
        args = observer.argparse.Namespace(
            launch=True, output=out, campaign_id="camp", cpus=0.5, memory="512m", mode="canonical", seconds=120,
            partitions=[0], tail_records=0, max_records=10, sample_ms=1000, include_quote=False, symbols="",
            feeds=ALL_FEEDS, include_top=False, max_output_bytes=4000)
        projector = {"Config": {"Env": ["QDL_KN_KAFKA_BOOTSTRAP=kafka1:9092", "QDL_KN_KAFKA_CA=/kn/tls/ca.crt",
                                        "QDL_KN_KAFKA_CERT=/kn/tls/p.crt", "QDL_KN_KAFKA_KEY=/kn/tls/p.key"]}}
        inspect = {"docker inspect " + observer.PROJECTOR: [projector], "docker inspect " + observer.QUERY: [QUERY_INSPECT],
                   "docker image inspect " + QUERY_INSPECT["Image"]: [{"RepoDigests": ["qdl-v2-python@sha256:" + "7d" * 32]}]}
        process = unittest.mock.MagicMock()
        process.stdout = iter(lines)
        process.poll.return_value = None
        process.wait.return_value = 0
        calls = []

        def run(cmd, **kw):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        with unittest.mock.patch.object(observer.subprocess, "check_output",
                                        side_effect=lambda cmd, text: json.dumps(inspect[" ".join(cmd)])), \
                unittest.mock.patch.object(observer.subprocess, "Popen", return_value=process) as popen, \
                unittest.mock.patch.object(observer.subprocess, "run", side_effect=run):
            try:
                observer.launch(args)
            finally:
                command = popen.call_args[0][0]
        return command, calls, process

    def test_launch_runs_its_own_container_records_it_and_removes_it(self):
        ok = [json.dumps({"kind": "scope", "observer_version": OBSERVER_VERSION, "seconds": 120}) + "\n",
              json.dumps({"kind": "DONE", "records": 0}) + "\n"]
        with tempfile.TemporaryDirectory() as tmp:
            command, calls, _ = self._launch(Path(tmp) / "g4-1", ok)
            self.assertEqual(command[:3], ["docker", "run", "--rm"])
            self.assertIn("--worker", command)
            self.assertNotIn("exec", command)
            self.assertEqual(calls[0], ["docker", "rm", "-f", "qdl-p184-g4obs-camp-g4-1"])
            receipt = json.loads((Path(tmp) / "g4-1" / "receipt.json").read_text())
            self.assertEqual(receipt["observer_version"], OBSERVER_VERSION)
            self.assertEqual((receipt["launch"]["launch_mode"], receipt["launch"]["cpus"]), ("docker_run_isolated", 0.5))
            self.assertEqual(receipt["launch"]["image_repo_digests"], ["qdl-v2-python@sha256:" + "7d" * 32])
            self.assertTrue(receipt["container_absent_after_exit"])
            first = json.loads((Path(tmp) / "g4-1" / "observations.jsonl").read_text().splitlines()[0])
            self.assertEqual((first["kind"], first["image_id"]), ("launch", QUERY_INSPECT["Image"]))

    def test_byte_cap_path_still_removes_the_container(self):
        big = [json.dumps({"kind": "canonical_observation", "pad": "x" * 1500}) + "\n"] * 3
        removed = []
        real = observer.remove_container
        with tempfile.TemporaryDirectory() as tmp, \
                unittest.mock.patch.object(observer, "remove_container", side_effect=lambda n: removed.append(n) or True):
            with self.assertRaises(RuntimeError):
                self._launch(Path(tmp) / "g4-2", big)
        self.assertEqual(removed, ["qdl-p184-g4obs-camp-g4-2"])
        self.assertIs(observer.remove_container, real)


def ob(feed, venue, interval=None, source=None, received=None, produced_ms=None, seen=None):
    return {"kind": "canonical_observation", "feed": feed, "identity": {"venue": venue}, "interval": interval,
            "source_event_ns": source, "received_at_ns": received, "producer_timestamp_ms": produced_ms,
            "observer_receipt_wall_ns": seen}


class ReportOnlyCoverageTests(unittest.TestCase):
    def test_coverage_counts_per_interval_and_dates_a_cap_before_the_window_end(self):
        items = [{"kind": "scope", "seconds": 180}, {"kind": "READY", "at_ns": 100 * S},
                 ob("bar", "OKX", "1m"), ob("bar", "OKX", "1m"), ob("bar", "OKX", "5m"), ob("trade", "BINANCE"),
                 {"kind": "canonical_record_cap_reached", "records": 4, "at_ns": 232 * S, "after_seconds": 132.0},
                 {"kind": "DONE", "record_cap_reached": True}]
        cov = observer.window_coverage(items)
        self.assertEqual(cov["per_key_records"], {"bar|OKX|1m": 2, "bar|OKX|5m": 1, "trade|BINANCE|": 1})
        self.assertTrue(cov["truncated_before_end"])
        self.assertEqual((cov["truncated_at_ns"], cov["truncated_after_seconds"], cov["unobserved_tail_seconds"]),
                         (232 * S, 132.0, 48.0))
        full = observer.window_coverage(items[:6] + [{"kind": "DONE", "record_cap_reached": False}])
        self.assertEqual((full["record_cap_reached"], full["truncated_before_end"], full["truncated_at_ns"]),
                         (False, False, None))

    def test_produce_split_three_stages_and_missing_clock_per_stage(self):
        items = [ob("trade", "OKX", source=1_000_000_000, received=1_030_000_000, produced_ms=1_045, seen=1_200_000_000),
                 ob("trade", "OKX", source=2_000_000_000, received=2_010_000_000, produced_ms=None, seen=2_100_000_000)]
        out = observer.produce_split(items)["trade|OKX|"]
        self.assertEqual(out["venue_to_ingest_ms"]["n"], 2)
        self.assertEqual(out["venue_to_ingest_ms"]["max"], 30.0)
        self.assertEqual((out["ingest_to_producer_ts_ms"]["n"], out["ingest_to_producer_ts_ms"]["max"]), (1, 15.0))
        self.assertEqual(out["producer_ts_to_receipt_ms"]["max"], 155.0)
        self.assertEqual(out["missing_clock"], {"ingest_to_producer_ts_ms": 1, "producer_ts_to_receipt_ms": 1})

    def test_summarize_ignores_the_launch_line(self):
        lines = [json.dumps({"kind": "launch", "launch_mode": "docker_run_isolated"}),
                 obs("trade", "OKX", 1_000_000_000, 1_030_000_000, 1_150_000_000)]
        self.assertEqual(list(summarize(lines)), ["trade|OKX|"])


# ---- v4: fixed-memory full-window aggregates (Codex review S100-4, 2026-10-15) ----
from scripts import p184_gate_campaign as campaign  # noqa: E402

W0 = 1_792_072_000 * S


def run_window(events, *, seconds=180, raw_cap=10000, final=True, end_ns=None, **kw):
    """Feed (feed, venue, symbol, source_ns, seen_ns, admitted) tuples; return observation items as the worker
    emits them (scope, READY, raw samples, final/partial aggregate)."""
    window = observer.WindowAggregator(seconds, raw_cap=raw_cap, **kw)
    window.begin(W0)
    items = [{"kind": "scope", "observer_version": OBSERVER_VERSION, "seconds": seconds},
             {"kind": "READY", "at_ns": W0, "planned_end_ns": window.planned_end_ns}]
    for i, (feed, venue, symbol, source, seen, admitted) in enumerate(events):
        record = window.observe(feed, venue, None, symbol, source, source + 5_000_000, None, seen, admitted,
                                lambda i=i, source=source, seen=seen, symbol=symbol: {
                                    "kind": "canonical_observation", "feed": feed, "offset": i,
                                    "identity": {"venue": venue, "native_symbol": symbol}, "interval": None,
                                    "source_event_ns": source, "received_at_ns": source + 5_000_000,
                                    "producer_timestamp_ms": None, "observer_receipt_wall_ns": seen})
        if record is not None:
            items.append(record)
    end = end_ns if end_ns is not None else window.planned_end_ns
    items.append(json.loads(json.dumps(window.snapshot(end, end, final=final))))
    return items, window


def snapshots(n, *, seconds=180, symbol="BTC-USDT-SWAP"):
    """n BOOK_SNAPSHOT events evenly over the window, visibility 100 + (i % 200) ms, one 4.5 s outlier."""
    out = []
    for i in range(n):
        seen = W0 + int(i * seconds * S / n)
        latency_ms = 4_500 if i == n // 2 else 100 + i % 200
        out.append(("book_snapshot", "OKX", symbol, seen - latency_ms * 1_000_000, seen, True))
    return out


class V4AggregateTests(unittest.TestCase):
    def test_bucket_scheme_brackets_every_value_with_bounded_width(self):
        for ms in (0.0, 0.4, 1.0, 127.99, 128.0, 129.7, 257.6, 1586.6, 73_817.2, 2_160_788.8, 9e9):
            lower, upper = observer.bucket_bounds(observer.bucket_index(ms))
            self.assertTrue(lower <= ms < upper, (ms, lower, upper))
            self.assertLessEqual(upper - lower, max(1.0, ms / observer.SUB_BUCKETS))
        self.assertEqual(observer.bucket_index(-0.1), -1)
        self.assertEqual(observer.bucket_bounds(-1), (None, 0.0))

    def test_histogram_is_conservative_against_the_v3_exact_nearest_rank(self):
        values = [100 + (i * 7919) % 400 + (i % 10) / 10 for i in range(5000)] + [-3.0, 6_749.2]
        h = observer.Histogram()
        for v in values:
            h.add(v)
        stats = h.stats()
        exact = observer._quantiles(values)
        self.assertEqual((stats["n"], stats["negative"], stats["max"], stats["min"]), (5002, 1, 6749.2, -3.0))
        for name in ("p50", "p95"):
            self.assertLessEqual(stats[f"{name}_bucket_lower"], exact[name] + 0.05)
            self.assertGreaterEqual(stats[name], exact[name] - 0.05)
            self.assertLessEqual(stats[name] - stats[f"{name}_bucket_lower"], 4.0)
        self.assertEqual(stats["percentile_basis"], "BUCKET_UPPER_BOUND")
        self.assertEqual(observer.Histogram.from_json(json.loads(json.dumps(h.to_json()))).stats(), stats)
        small = observer.Histogram()
        small.add(10.2)
        self.assertIsNone(small.stats()["p99"])
        self.assertEqual(small.stats()["p95"], 10.2)  # an upper bound above the exact max is clamped to it
        with self.assertRaises(ValueError):
            observer.Histogram.from_json({"n": 3, "buckets": [[1, 1]]})

    def test_the_reader_never_stops_at_a_record_cap_and_coverage_is_the_whole_window(self):
        # The 2026-10-15 failure: 10,000 records reached after ~90 s of a 180 s window. v4 keeps every admitted
        # record in the aggregate; only the raw stdout sample is bounded.
        items, window = run_window(snapshots(30_000), raw_cap=10_000)
        aggregate = observer.last_aggregate(items)
        self.assertEqual(aggregate["events_admitted"], 30_000)
        raw = [i for i in items if i["kind"] == "canonical_observation"]
        self.assertLessEqual(len(raw), observer.RAW_SLICES * observer.RAW_PER_SLICE)
        self.assertEqual(len({(r["observer_receipt_wall_ns"] - W0) * observer.RAW_SLICES // (180 * S) for r in raw}),
                         observer.RAW_SLICES)  # stratified over the whole window, not the first seconds
        summary = observer.summarize([json.dumps(i) for i in items])["book_snapshot|OKX|"]
        self.assertEqual(summary["visibility_ms"]["n"], 30_000)
        self.assertEqual(summary["visibility_ms"]["max"], 4_500.0)  # exact, however late in the window
        self.assertEqual(summary["outliers"][0]["visibility_ms"], 4_500.0)
        self.assertEqual(len(summary["outliers"]), observer.OUTLIER_K)
        self.assertEqual(summary["venue_to_dl_ms"]["max"], 5.0)
        coverage = observer.window_coverage(items)
        self.assertEqual((coverage["coverage_basis"], coverage["truncated_before_end"], coverage["record_cap_reached"]),
                         ("FULL_WINDOW_AGGREGATE", False, False))
        self.assertEqual(coverage["observed_seconds"], 180.0)
        receipt = campaign.g4_window_receipt(items)
        self.assertFalse(receipt["record_cap_reached"])
        self.assertEqual(receipt["events_admitted"], 30_000)
        self.assertIn("frozen G4 table and addendum unchanged", receipt["baseline_comparability"])

    def test_raw_cap_bounds_samples_only(self):
        items, window = run_window(snapshots(2_000), raw_cap=10)
        self.assertEqual(sum(1 for i in items if i["kind"] == "canonical_observation"), 10)
        self.assertTrue(window.raw_cap_reached)
        self.assertEqual(observer.last_aggregate(items)["events_admitted"], 2_000)

    def test_gated_population_is_the_v3_admission_rule(self):
        # Same admitted events: the v4 aggregate and the v3 raw summary agree on n and the exact max.
        events = snapshots(3_000) + [("trade", "OKX", "ETH-USDT-SWAP", W0 + i * S - 30_000_000, W0 + i * S, i % 2 == 0)
                                     for i in range(100)]
        items, _ = run_window(events, raw_cap=10**6, raw_per_slice=10**6)
        v4 = observer.summarize([json.dumps(i) for i in items])
        v3 = observer._summarize_records([json.dumps(i) for i in items if i["kind"] == "canonical_observation"])
        for key in ("book_snapshot|OKX|", "trade|OKX|"):
            self.assertEqual(v4[key]["visibility_ms"]["n"], v3[key]["visibility_ms"]["n"])
            self.assertAlmostEqual(v4[key]["visibility_ms"]["max"], v3[key]["visibility_ms"]["max"], places=1)
            self.assertGreaterEqual(v4[key]["visibility_ms"]["p95"] + 0.05, v3[key]["visibility_ms"]["p95"])
        self.assertEqual(v4["trade|OKX|"]["visibility_ms"]["n"], 50)  # non-admitted events are not gated
        self.assertEqual(v4["trade|OKX|"]["all_events_visibility_ms"]["n"], 100)  # report-only

    def test_partial_aggregate_is_a_truncated_window(self):
        items, _ = run_window(snapshots(100), final=False, end_ns=W0 + 120 * S)
        coverage = observer.window_coverage(items)
        self.assertTrue(coverage["truncated_before_end"])
        self.assertEqual(coverage["unobserved_tail_seconds"], 60.0)

    def test_inverse_rows_come_from_the_aggregate(self):
        events = [("quote", "OKX", "BTC-USD-SWAP", W0 + i * S - 50_000_000, W0 + i * S, True) for i in range(10)]
        items, _ = run_window(events)
        rows = campaign.g4_inverse_rows(items)
        self.assertEqual(rows["quote|OKX_INVERSE|"]["n"], 10)
        self.assertEqual(rows["quote|OKX_INVERSE|"]["max"], 50.0)
        self.assertEqual(rows["quote|OKX_INVERSE|"]["percentile_basis"], "BUCKET_UPPER_BOUND")


def g4_window(p95, lower=None, n=500, basis="BUCKET_UPPER_BOUND"):
    stats = {"n": n, "p95": p95, "max": p95 + 10}
    if basis:
        stats.update(percentile_basis=basis, p95_bucket_lower=lower)
    return {"summary": {"trade|OKX|": {"visibility_ms": stats}}, "inverse_rows": {}}


class V4ScoringTests(unittest.TestCase):
    TABLE = {"trade|OKX|": {"candidate_p95_limit_ms": 257.4}}

    def test_bucket_straddling_the_limit_is_unresolved_not_fail_or_pass(self):
        v, rows, why = campaign.score_g4([g4_window(258.0, 256.0)], self.TABLE)
        self.assertEqual(v, "NOT_MEASURED")
        self.assertEqual(rows[0]["unresolved_windows"], [True])
        self.assertTrue(any("straddles" in w for w in why))
        self.assertEqual(campaign.score_g4([g4_window(260.0, 258.0)], self.TABLE)[0], "FAIL")
        self.assertEqual(campaign.score_g4([g4_window(256.0, 254.0)], self.TABLE)[0], "PASS")
        self.assertEqual(campaign.score_g4([g4_window(258.0, basis=None)], self.TABLE)[0], "FAIL")  # v1-v3 exact
        mixed = campaign.score_g4([g4_window(258.0, 256.0), g4_window(300.0, 298.0)], self.TABLE)
        self.assertEqual(mixed[0], "FAIL")  # a decisive window fails even next to an unresolved one

    def test_a_truncated_window_cannot_pass_g4(self):
        items, _ = run_window(snapshots(50), final=False, end_ns=W0 + 100 * S)
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            budget = d / "budget.json"
            budget.write_text(json.dumps({"g4_delivery_visibility": {"table": {
                "book_snapshot|OKX|": {"candidate_p95_limit_ms": 10_000.0}}}}))
            (d / "campaign.json").write_text(json.dumps({"budget_path": str(budget), "g4_addendum_path":
                                                          str(d / "absent.json"), "g4_windows": [{"index": 1}]}))
            (d / "g4-1").mkdir()
            (d / "g4-1" / "observations.jsonl").write_text("".join(json.dumps(i) + "\n" for i in items))
            (d / "g4-1-summary.json").write_text(json.dumps(observer.summarize(
                (d / "g4-1" / "observations.jsonl").read_text().splitlines())))
            gate = next(g for g in campaign.build_scorecard(d)["gates"] if g["id"] == "G4")
        self.assertEqual(gate["verdict"], "NOT_MEASURED")
        self.assertIn("observed only part of their planned window", gate["reason"])


if __name__ == "__main__":
    unittest.main()


# ---- v5: the observer's own cgroup CPU evidence (report-only, G4 common-mode tail RCA 2026-10-15) ----
def cg(at_s, usage_s, periods, throttled, throttled_ms):
    return {"kind": "observer_cgroup", "at_ns": int(at_s * S), "label": "checkpoint",
            "source": "OBSERVER_OWN_CGROUP_V2_CPU_STAT",
            "cpu": {"usage_usec": int(usage_s * 1e6), "nr_periods": periods, "nr_throttled": throttled,
                    "throttled_usec": int(throttled_ms * 1000), "cpu_max_cores": 0.5}}


class ObserverCgroupTests(unittest.TestCase):
    def test_reads_own_cpu_stat_and_bound_and_none_when_unreadable(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "cpu.stat").write_text("usage_usec 39711\nuser_usec 23419\nsystem_usec 16292\nnice_usec 0\n"
                                        "core_sched.force_idle_usec 0\nnr_periods 2\nnr_throttled 1\n"
                                        "throttled_usec 4000\nnr_bursts 0\nburst_usec 0\n")
            (d / "cpu.max").write_text("50000 100000\n")
            got = observer.read_cgroup_cpu(d)
            self.assertEqual(got, {"usage_usec": 39711, "user_usec": 23419, "system_usec": 16292, "nr_periods": 2,
                                   "nr_throttled": 1, "throttled_usec": 4000, "cpu_max_cores": 0.5})
            (d / "cpu.max").write_text("max 100000\n")
            self.assertIsNone(observer.read_cgroup_cpu(d)["cpu_max_cores"])
            line = observer.cgroup_line(7, "ready", d)
            self.assertEqual((line["kind"], line["label"], line["cpu"]["nr_throttled"]), ("observer_cgroup", "ready", 1))
            self.assertIsNone(observer.read_cgroup_cpu(d / "absent"))
            (d / "cpu.stat").write_text("usage_usec 1\n")  # cgroup v1 or a partial file: not guessed
            self.assertIsNone(observer.read_cgroup_cpu(d))

    def test_summary_window_and_per_interval_throttling(self):
        items = [{"kind": "READY", "at_ns": 0}, cg(100, 10.0, 1000, 0, 0), {"kind": "aggregate", "final": False},
                 cg(130, 22.0, 1300, 60, 900.0), cg(160, 34.0, 1600, 60, 900.0),
                 {"kind": "observer_cgroup", "at_ns": 170 * S, "label": "final", "cpu": None}]
        out = observer.cgroup_summary(items)
        self.assertTrue(out["available"] and out["report_only"])
        self.assertEqual((out["samples"], out["unreadable"], out["cpu_max_cores"]), (3, 1, 0.5))
        self.assertEqual(out["window"], {"start_ns": 100 * S, "end_ns": 160 * S, "seconds": 60.0, "usage_cores": 0.4,
                                         "nr_periods": 600, "nr_throttled": 60, "throttled_ratio": 0.1,
                                         "throttled_ms": 900.0})
        self.assertEqual([i["throttled_ratio"] for i in out["intervals"]], [0.2, 0.0])
        self.assertEqual((out["max_interval_throttled_ratio"], out["max_interval_usage_cores"]), (0.2, 0.4))

    def test_pre_v5_and_single_sample_windows_say_so(self):
        self.assertFalse(observer.cgroup_summary([{"kind": "READY"}])["available"])
        one = observer.cgroup_summary([cg(1, 1.0, 10, 0, 0)])
        self.assertEqual((one["available"], one["reason"]), (False, "FEWER_THAN_TWO_READABLE_SAMPLES"))

    def test_launch_receipt_carries_the_cgroup_summary_report_only(self):
        lines = [json.dumps({"kind": "scope", "observer_version": OBSERVER_VERSION, "seconds": 60}) + "\n",
                 json.dumps(cg(0, 0.0, 0, 0, 0)) + "\n", json.dumps(cg(60, 30.0, 600, 120, 2400.0)) + "\n",
                 json.dumps({"kind": "DONE", "records": 0}) + "\n"]
        with tempfile.TemporaryDirectory() as tmp:
            IsolatedLaunchTests._launch(self, Path(tmp) / "g4-3", lines)
            receipt = json.loads((Path(tmp) / "g4-3" / "receipt.json").read_text())
        self.assertIn("observer_cgroup", receipt["report_only"])
        self.assertEqual((receipt["observer_cgroup"]["window"]["throttled_ratio"],
                          receipt["observer_cgroup"]["window"]["usage_cores"]), (0.2, 0.5))
