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
        self.assertTrue(OBSERVER_VERSION.endswith(".v3"))  # v3 changed the launch only, not this sampling


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


if __name__ == "__main__":
    unittest.main()
