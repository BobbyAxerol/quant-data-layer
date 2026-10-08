"""Offline runner guards, not runtime fault qualification."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("dr08", ROOT / "scripts/p184_dr08_isolated.py")
R = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(R)


def packet(development=True):
    return {"namespace": "p184-dr08-test-12345678", "development": development,
        "bootstrap": "localhost:9092", "slice": {"target": "localhost:8210"},
        "consumer_id": "slow", "fast_consumer_id": "fast", "raw_topic": "test-raw",
        "metrics_path": "/isolated/metrics", "ts_source_sha": "1234567890" * 4,
        "ts_image_digest": "sha256:" + "12345678" * 8,
        "images": {role: "sha256:" + "abcdef01" * 8 for role in ("core", "stream", "kafka", "python")}}


class Guards(unittest.TestCase):
    def setUp(self):
        self.p = packet()
        env = patch.dict(os.environ, {"P184_DR08_ISOLATED_NAMESPACE": self.p["namespace"]})
        env.start()
        self.addCleanup(env.stop)

    def test_valid_modes(self):
        R.validate_packet(self.p, development=True)
        R.validate_packet(packet(False), development=False)

    def test_shared_broker_refused(self):
        for host in ("kafka1:9092", "localhost:9092,shared:9092", "127.0.0.1:9092"):
            with self.subTest(host=host):
                self.p["bootstrap"] = host
                with self.assertRaises(ValueError):
                    R.validate_packet(self.p, development=True)

    def test_shared_stream_refused(self):
        self.p["slice"]["target"] = "qdl-v2-stream-a:8210"
        with self.assertRaises(ValueError):
            R.validate_packet(self.p, development=True)

    def test_parent_attestation_required(self):
        with patch.dict(os.environ, {"P184_DR08_ISOLATED_NAMESPACE": ""}):
            with self.assertRaises(ValueError):
                R.validate_packet(self.p, development=True)

    def test_distinct_consumers(self):
        self.p["fast_consumer_id"] = self.p["consumer_id"]
        with self.assertRaises(ValueError):
            R.validate_packet(self.p, development=True)

    def test_immutable_images(self):
        for role in self.p["images"]:
            p = packet()
            p["images"][role] = "latest"
            with self.subTest(role=role), self.assertRaises(ValueError):
                R.validate_packet(p, development=True)

    def test_development_cannot_be_final(self):
        with self.assertRaisesRegex(ValueError, "fresh packet"):
            R.validate_packet(self.p, development=False)

    def test_dummy_final_provenance_refused(self):
        for field, value in (("ts_source_sha", "dummy"), ("ts_source_sha", "0" * 40),
                             ("ts_image_digest", "sha256:" + "a" * 64)):
            p = packet(False)
            p[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                R.validate_packet(p, development=False)


class Evidence(unittest.TestCase):
    def test_exact_cells(self):
        self.assertEqual(set(R.CELLS), {
            "DR08.SLOW_CONSUMER_BOUNDED_QUEUE@ANY",
            "DR08.CONSUMER_RESTART_RECOVERY_MEASURED@ANY",
            "DR08.NO_CROSS_CONSUMER_STARVATION@ANY",
            "DR08.ALERT_PATH_OBSERVABLE@ANY"})

    def test_missing_facts_fail_closed(self):
        rows = R.evidence_rows(packet(False), {}, development=False, run_id="new")
        self.assertEqual(rows[0]["provenance"]["schema"], "p184.surface-evidence-provenance.v1")
        self.assertEqual(len(rows), 5)
        self.assertTrue(all(not row["ok"] and row["outcome"] == "NOT_OBSERVED" for row in rows[1:]))

    def test_relabelling_header_cannot_qualify_development(self):
        facts = {cell: {"observed": True, "passed": True} for cell in R.CELLS}
        rows = R.evidence_rows(packet(), facts, development=True, run_id="dev")
        rows[0] = R.evidence_rows(packet(False), {}, development=False, run_id="final")[0]
        self.assertTrue(all(not row["ok"] and row["outcome"] == "NOT_OBSERVED" for row in rows[1:]))
        self.assertTrue(all(row["development_pass"] and row["mode"] == "DEVELOPMENT" for row in rows[1:]))

    def test_observation_and_pass_required(self):
        for fact, expected in (({"observed": True, "passed": True}, True),
                               ({"observed": False, "passed": True}, False),
                               ({"observed": True, "passed": False}, False),
                               ({"observed": 1, "passed": True}, False)):
            rows = R.evidence_rows(packet(False), {R.CELLS[0]: fact}, development=False, run_id="new")
            self.assertIs(rows[1]["ok"], expected)

    def test_alert_deltas_and_bounds(self):
        before = dict(overflow=2, expired=3, queue_bytes=0, rss_bytes=100, cpu_seconds=1)
        after = dict(overflow=3, expired=4, queue_bytes=100, rss_bytes=200, cpu_seconds=2)
        self.assertTrue(R.metric_verdict(before, after))
        for key, value in (("overflow", 2), ("expired", 3), ("queue_bytes", R.QUEUE_BYTES + 1), ("cpu_seconds", 0)):
            self.assertFalse(R.metric_verdict(before, {**after, key: value}))
        self.assertFalse(R.metric_verdict({}, after))

    def test_partial_metrics_and_no_samples(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "metrics"
            path.write_text('startup\n{"event":"qdl_kn_gateway_metrics","overflow":1}\n{')
            self.assertEqual(len(R.read_metrics(path, since_ns=1)), 1)
            path.write_text("no samples")
            with self.assertRaises(ValueError):
                R.read_metrics(path, since_ns=1)

    def test_metrics_size_bound(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "metrics"
            with path.open("wb") as f:
                f.truncate(R.MAX_METRICS_BYTES + 1)
            with self.assertRaisesRegex(ValueError, "8MiB"):
                R.read_metrics(path, since_ns=1)

    def test_startup_failure_is_not_observed(self):
        with patch.object(R, "wait_metrics", side_effect=ValueError("missing")):
            facts = R.run_faults(packet())
        self.assertEqual(set(facts), set(R.CELLS))
        self.assertTrue(all(not f["observed"] and not f["passed"] for f in facts.values()))

    def test_output_exclusive_before_traffic(self):
        with tempfile.TemporaryDirectory() as d:
            config, output = Path(d) / "packet", Path(d) / "evidence"
            p = packet()
            config.write_text(json.dumps(p))
            output.write_text("existing evidence")
            with patch.dict(os.environ, {"P184_DR08_ISOLATED_NAMESPACE": p["namespace"]}), patch.object(R, "run_faults") as run:
                with self.assertRaises(FileExistsError):
                    R.main(["--packet", str(config), "--output", str(output), "--development"])
                run.assert_not_called()
            self.assertEqual(output.read_text(), "existing evidence")

    def test_fresh_run_writes_header_and_four_rows(self):
        with tempfile.TemporaryDirectory() as d:
            config, output = Path(d) / "packet", Path(d) / "evidence"
            p = packet()
            config.write_text(json.dumps(p))
            facts = {cell: {"observed": True, "passed": True} for cell in R.CELLS}
            with patch.dict(os.environ, {"P184_DR08_ISOLATED_NAMESPACE": p["namespace"]}), patch.object(R, "run_faults", return_value=facts):
                self.assertEqual(R.main(["--packet", str(config), "--output", str(output), "--development"]), 0)
            rows = [json.loads(line) for line in output.read_text().splitlines()]
            self.assertEqual(len(rows), 5)
            self.assertEqual(rows[0]["provenance"]["data_provenance"], "SYNTHETIC_TEST_ONLY")
            self.assertTrue(all(row["ok"] is False for row in rows[1:]))

    def test_metrics_baseline_wait_is_bounded(self):
        with patch.object(R, "read_metrics", side_effect=FileNotFoundError), \
             patch.object(R.time, "monotonic", side_effect=[0, 21]):
            with self.assertRaises(TimeoutError):
                R.wait_metrics("absent", since_ns=1, timeout=20)

    def test_metrics_baseline_retries_empty_capture(self):
        row = {"event": "qdl_kn_gateway_metrics"}
        with patch.object(R, "read_metrics", side_effect=[ValueError("empty"), [row]]), \
             patch.object(R.time, "sleep") as sleep:
            self.assertEqual(R.wait_metrics("capture", since_ns=1), [row])
            sleep.assert_called_once_with(.1)

    def test_tls_producer_settings(self):
        p = packet()
        p["client_tls"] = {"ca": "/work/ca.crt", "cert": "/work/client.crt", "key": "/work/client.key"}
        settings = R.kafka_security(p)
        self.assertEqual(settings["security.protocol"], "SSL")
        self.assertEqual(settings["ssl.key.location"], "/work/client.key")
        self.assertEqual(settings["bootstrap.servers"], "localhost:9092")

    def test_invalid_provenance_rejected_before_setup(self):
        with tempfile.TemporaryDirectory() as d:
            output = Path(d) / "absent"
            with self.assertRaises(ValueError):
                R.orchestrate(output, development=True, source_sha="DEVELOPMENT", image_digest="DEVELOPMENT")
            self.assertFalse(output.exists())


    def test_queue_observation_is_measured_not_configured(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "metrics"
            row = {"event": "qdl_kn_subscription_closed", "consumer_id": "slow",
                   "outcome": "backpressure", "subscription_id": 1, "queued": 1000, "queued_bytes": 10000}
            path.write_text(json.dumps(row))
            self.assertEqual(R.slow_queue_observation(path, "slow", subscription_id=1, wire_code="RESOURCE_EXHAUSTED")["queued"], 1000)
            self.assertIsNone(R.slow_queue_observation(path, "other", subscription_id=1, wire_code="RESOURCE_EXHAUSTED"))
            row["queued"] = 999
            path.write_text(json.dumps(row))
            self.assertIsNone(R.slow_queue_observation(path, "slow", subscription_id=1, wire_code="RESOURCE_EXHAUSTED"))


    def test_actual_rust_close_description_requires_wire_and_subscription(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "exclusive-stream.log"
            row = {"event": "qdl_kn_subscription_closed", "consumer_id": "isolated.slow",
                   "outcome": "Some resource has been exhausted", "subscription_id": 1,
                   "queued": 1000, "queued_bytes": 4719110}
            path.write_text(json.dumps(row))
            self.assertEqual(R.slow_queue_observation(path, "isolated.slow", subscription_id=1,
                wire_code="RESOURCE_EXHAUSTED")["queued_bytes"], 4719110)
            for consumer, subscription, code in (("other.slow", 1, "RESOURCE_EXHAUSTED"),
                    ("isolated.slow", 2, "RESOURCE_EXHAUSTED"), ("isolated.slow", 1, "OK")):
                self.assertIsNone(R.slow_queue_observation(path, consumer,
                    subscription_id=subscription, wire_code=code))
            row["outcome"] = "ended"
            path.write_text(json.dumps(row))
            self.assertIsNone(R.slow_queue_observation(path, "isolated.slow", subscription_id=1,
                wire_code="RESOURCE_EXHAUSTED"))

    def test_foreground_timeout_stops_pre_registered_owned_container(self):
        import subprocess
        from contextlib import redirect_stdout
        import io
        calls, owned = [], {}
        cfg = {"core": {"bindings": [{"venue": "BINANCE", "market": "USDM",
               "native_symbol": "BTCUSDT", "native_channel": "btcusdt@trade"}]}, "authority": {}}

        def run(args, **kwargs):
            calls.append(args)
            out = ""
            if args[:3] == ["docker", "image", "inspect"]:
                out = args[3]
            elif args[:3] == ["docker", "inspect", "qdl_v2_stable_candidate-rust_core-1"]:
                out = json.dumps([{"Image": R.IMAGES["core"], "Id": "shared-core"}])
            elif args[:3] == ["docker", "exec", "shared-core"]:
                out = json.dumps(cfg)
            elif args[:2] == ["docker", "run"]:
                name = args[args.index("--name") + 1]
                namespace = args[args.index("--label") + 1].split("=", 1)[1]
                owned.update(name=name, namespace=namespace)
                raise subprocess.TimeoutExpired(args, 100)
            elif args[:2] == ["docker", "inspect"]:
                self.assertEqual(args[2], owned["name"])
                out = json.dumps([{"Id": "owned-immutable-id", "Config": {
                    "Labels": {"p184.dr08": owned["namespace"]}}}])
            return subprocess.CompletedProcess(args, 0, stdout=out, stderr="")

        with tempfile.TemporaryDirectory() as d, patch("subprocess.run", side_effect=run), redirect_stdout(io.StringIO()):
            target = Path(d) / "attempt"
            with self.assertRaises(subprocess.TimeoutExpired):
                R.orchestrate(target, development=True, source_sha="0" * 40, image_digest="sha256:" + "0" * 64)
            cleanup = json.loads((target / "cleanup.json").read_text())
            self.assertEqual(cleanup["cleanup_failures"], [])
            self.assertTrue(cleanup["shared_runtime_unchanged"])
            self.assertIn(["docker", "stop", "--time", "5", "owned-immutable-id"], calls)
            self.assertFalse(any(a[:2] == ["docker", "stop"] and "shared-core" in a for a in calls))



if __name__ == "__main__":
    unittest.main()
