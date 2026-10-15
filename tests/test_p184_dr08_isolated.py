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
        "query_provenance": {"image_ref": R.QUERY_IMAGE_REF, "actual_image_digest": R.QUERY_IMAGE_DIGEST},
        "images": {"query": R.QUERY_IMAGE_DIGEST, **{
            role: "sha256:" + "abcdef01" * 8 for role in ("core", "stream", "kafka", "python")}}}


class ReplayBoundary(unittest.TestCase):
    def test_reaches_live_then_receives_new_marker(self):
        from unittest.mock import Mock
        child, producer, start = Mock(), Mock(), Mock()
        start.return_value = child
        def wait(kind, *args):
            if kind == "live":
                producer.publish.assert_not_called()
                return {"count": 10000}
            producer.publish.assert_called_once_with(1)
            return {"count": 10001, "last": 10003, "code": "OK", "controls": ["REPLAYING", "LIVE"]}
        child.wait.side_effect = wait
        result = R.replay_boundary(start, "slow", "old", producer, 2)
        self.assertEqual(result["replay_count_at_live"], 10000)
        start.assert_called_once_with("slow", "old", stop_after=10001)
        child.close.assert_called_once()

    def test_receiving_limit_without_live_is_not_success(self):
        from unittest.mock import Mock
        child, producer = Mock(), Mock()
        child.wait.side_effect = RuntimeError("done before live: OUT_OF_RANGE")
        with self.assertRaises(RuntimeError):
            R.replay_boundary(Mock(return_value=child), "slow", "old", producer, 2)
        producer.publish.assert_not_called()
        child.close.assert_called_once()

    def test_wrong_replay_count_and_bad_live_marker_fail_closed(self):
        from unittest.mock import Mock
        for live, end in ((9999, {}), (10000, {"count": 10001, "last": 10004, "code": "OK", "controls": ["LIVE"]}),
                          (10000, {"count": 10001, "last": 10003, "code": "OK", "controls": []})):
            with self.subTest(live=live, end=end):
                child, producer = Mock(), Mock()
                child.wait.side_effect = [{"count": live}, end]
                with self.assertRaises(ValueError):
                    R.replay_boundary(Mock(return_value=child), "slow", "old", producer, 2)
                child.close.assert_called_once()


class CandidateBinary(unittest.TestCase):
    def test_overlay_cannot_be_relabelled_as_final(self):
        p = packet(False)
        p["stream_binary_override"] = {"sha256": "a" * 64}
        with self.assertRaisesRegex(ValueError, "qualification"):
            R.evidence_rows(p, {}, development=False, run_id="forged")

    def test_inner_driver_checks_actual_binary(self):
        p = packet()
        p["stream_binary_override"] = {"sha256": "a" * 64}
        with patch.dict(os.environ, {"P184_DR08_ISOLATED_NAMESPACE": p["namespace"]}), \
                patch.object(R, "candidate_stream_provenance", return_value={"sha256": "b" * 64}):
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                R.validate_packet(p, development=True)

    def test_overlay_is_development_only_and_hashed(self):
        import hashlib
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory) / "candidate"
            p.write_bytes(b"test-only-binary")
            result = R.candidate_stream_provenance(p, development=True)
            self.assertEqual(result["sha256"], hashlib.sha256(p.read_bytes()).hexdigest())
            self.assertEqual(result["bytes"], 16)
            with self.assertRaises(ValueError):
                R.candidate_stream_provenance(p, development=False)
            p.write_bytes(b"")
            with self.assertRaises(ValueError):
                R.candidate_stream_provenance(p, development=True)
        self.assertIsNone(R.candidate_stream_provenance(None, development=False))


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


class RestartMeasurements(unittest.TestCase):
    @staticmethod
    def samples(n=5):
        return [{"started_ns": (i + 1) * 100_000_000,
                 "first_event_ns": (i + 1) * 100_000_000 + (i + 1) * 1_000_000,
                 "trade_id": i + 1, "code": "OK", "count": 1,
                 "restart_to_first_event_ms": float(i + 1)} for i in range(n)]

    def test_count_defaults_and_bounds(self):
        self.assertEqual(R.restart_count(packet()), 1)
        self.assertEqual(R.restart_count(packet(False)), 5)
        for development, counts in ((True, (0, 11, True, 1.0, "5")),
                                    (False, (0, 1, 4, 11, False, 5.0))):
            for count in counts:
                with self.subTest(development=development, count=count), self.assertRaises(ValueError):
                    R.restart_count({**packet(development), "restart_samples": count})
        self.assertEqual(R.restart_count({**packet(False), "restart_samples": 10}), 10)

    def test_summary_uses_measured_samples(self):
        summary = R.recovery_summary(self.samples())
        self.assertEqual(summary["measurement_n"], 5)
        self.assertEqual(summary["restart_to_first_event_ms_samples"], [1, 2, 3, 4, 5])
        self.assertEqual(summary["restart_to_first_event_ms_p50"], 3)
        self.assertEqual(summary["restart_to_first_event_ms_max"], 5)
        self.assertEqual(R.recovery_summary(self.samples(6))["restart_to_first_event_ms_p50"], 3.5)

    def test_invalid_or_duplicated_measurements_rejected(self):
        bad_lists = [None, [], self.samples(11), [self.samples(1)[0]] * 5]
        for key, value in (("started_ns", 0), ("first_event_ns", 0),
                           ("trade_id", 0), ("code", "OUT_OF_RANGE"), ("count", 2),
                           ("restart_to_first_event_ms", float("nan")),
                           ("restart_to_first_event_ms", 60000)):
            samples = self.samples()
            samples[2][key] = value
            bad_lists.append(samples)
        for samples in bad_lists:
            with self.subTest(samples=samples), self.assertRaises(ValueError):
                R.recovery_summary(samples)

    def test_old_n1_or_forged_n5_cannot_qualify(self):
        for extra in ({"measurement_n": 1, "restart_to_first_event_ms": 5241.271},
                      {"measurement_n": 5}, {"restart_samples": self.samples(1)},
                      {"restart_samples": [self.samples(1)[0]] * 5}):
            fact = {"observed": True, "passed": True, **extra}
            row = R.evidence_rows(packet(False), {R.CELLS[1]: fact}, development=False, run_id="fresh")[2]
            self.assertFalse(row["ok"])
            self.assertEqual(row["outcome"], "NOT_OBSERVED")

    def test_five_qualifies_but_development_never_does(self):
        fact = {"observed": True, "passed": True, "restart_samples": self.samples()}
        final = R.evidence_rows(packet(False), {R.CELLS[1]: fact}, development=False, run_id="fresh")[2]
        self.assertTrue(final["ok"])
        self.assertEqual(final["measurement"]["restart_to_first_event_ms_p50"], 3)
        for n in (1, 5):
            fact["restart_samples"] = self.samples(n)
            dev = R.evidence_rows(packet(), {R.CELLS[1]: fact}, development=True, run_id="dev")[2]
            self.assertFalse(dev["ok"])
            self.assertTrue(dev["development_pass"])

    def test_independent_consumers_started_and_closed(self):
        from unittest.mock import AsyncMock, Mock
        for development, n in ((True, 1), (False, 5)):
            with self.subTest(development=development):
                producer = Mock(next_id=11)
                producer.publish.side_effect = lambda count: setattr(producer, "next_id", producer.next_id + count)
                children = []
                def start(consumer, token, **kwargs):
                    self.assertEqual(consumer, "slow")
                    self.assertEqual(kwargs, {"stop_after": 1})
                    if children:
                        children[-1].close.assert_called_once()
                    child = Mock(started_ns=(len(children) + 1) * 100_000_000)
                    def wait(kind):
                        return {} if kind == "live" else {
                            "first_ns": child.started_ns + 1_000_000,
                            "last": producer.next_id - 1, "code": "OK", "count": 1}
                    child.wait.side_effect = wait
                    children.append(child)
                    return child
                with patch.object(R, "snapshot", new_callable=AsyncMock, return_value=("fresh", 20)) as snapshot:
                    summary = R.measure_restarts(packet(development), producer, start, "initial", 10)
                self.assertEqual(len(children), n)
                self.assertEqual(snapshot.await_count, n - 1)
                self.assertEqual(summary["measurement_n"], n)
                self.assertEqual(summary["restart_to_first_event_ms_samples"], [1] * n)
                for child in children:
                    child.close.assert_called_once()

    def test_failed_restart_is_closed_and_not_summarized(self):
        from unittest.mock import Mock
        child = Mock(started_ns=1)
        child.wait.side_effect = TimeoutError("test only")
        with self.assertRaises(TimeoutError):
            R.measure_restarts(packet(), Mock(), Mock(return_value=child), "token", 10)
        child.close.assert_called_once()

    def test_incomplete_requested_sample_count_cannot_qualify(self):
        p = {**packet(False), "restart_samples": 6}
        fact = {"observed": True, "passed": True, "restart_samples": self.samples(5)}
        row = R.evidence_rows(p, {R.CELLS[1]: fact}, development=False, run_id="fresh")[2]
        self.assertFalse(row["ok"])

    def test_wrong_query_provenance_cannot_emit_evidence(self):
        p = packet(False)
        p["query_provenance"]["actual_image_digest"] = "sha256:" + "1" * 64
        with self.assertRaises(ValueError):
            R.evidence_rows(p, {}, development=False, run_id="fresh")

    def test_final_exit_succeeds_with_five_measured_restarts(self):
        with tempfile.TemporaryDirectory() as d:
            config, output = Path(d) / "packet", Path(d) / "evidence"
            p = packet(False)
            config.write_text(json.dumps(p))
            facts = {cell: {"observed": True, "passed": True} for cell in R.CELLS}
            facts[R.CELLS[1]]["restart_samples"] = self.samples()
            with patch.dict(os.environ, {"P184_DR08_ISOLATED_NAMESPACE": p["namespace"]}), \
                 patch.object(R, "run_faults", return_value=facts):
                self.assertEqual(R.main(["--packet", str(config), "--output", str(output)]), 0)
            row = json.loads(output.read_text().splitlines()[2])
            self.assertTrue(row["ok"])
            self.assertEqual(row["measurement"]["measurement_n"], 5)

    def test_final_exit_fails_for_old_single_sample(self):
        with tempfile.TemporaryDirectory() as d:
            config, output = Path(d) / "packet", Path(d) / "evidence"
            p = packet(False)
            config.write_text(json.dumps(p))
            facts = {cell: {"observed": True, "passed": True} for cell in R.CELLS}
            facts[R.CELLS[1]]["measurement_n"] = 1
            with patch.dict(os.environ, {"P184_DR08_ISOLATED_NAMESPACE": p["namespace"]}), \
                 patch.object(R, "run_faults", return_value=facts):
                self.assertEqual(R.main(["--packet", str(config), "--output", str(output)]), 1)
            self.assertFalse(json.loads(output.read_text().splitlines()[2])["ok"])

    def test_query_candidate_digest_and_label_required(self):
        p = packet()
        R.validate_query_provenance(p)
        for field, value in (("actual_image_digest", "sha256:" + "1" * 64),
                             ("image_ref", "qdl-v2-python:2.2.2")):
            bad = packet()
            bad["query_provenance"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                R.validate_query_provenance(bad)
        p["images"]["query"] = "sha256:" + "1" * 64
        with self.assertRaises(ValueError):
            R.validate_query_provenance(p)
        p = packet()
        del p["query_provenance"]
        with self.assertRaises(ValueError):
            R.validate_query_provenance(p)

    def test_query_candidate_named_in_evidence(self):
        provenance = R.evidence_rows(packet(), {}, development=True, run_id="dev")[0]["provenance"]
        self.assertEqual(provenance["query_provenance"]["image_ref"], "qdl-v2-python:2.2.3-rc2-43bbcf0")
        self.assertEqual(provenance["query_provenance"]["actual_image_digest"], R.QUERY_IMAGE_DIGEST)

    def test_final_one_run_refused_before_setup(self):
        with tempfile.TemporaryDirectory() as d, patch("subprocess.run") as run:
            target = Path(d) / "absent"
            with self.assertRaises(ValueError):
                R.orchestrate(target, development=False, source_sha=packet(False)["ts_source_sha"],
                              image_digest=packet(False)["ts_image_digest"], restart_samples=1)
            self.assertFalse(target.exists())
            run.assert_not_called()

    def test_actual_local_image_mismatch_fails_before_launch(self):
        import subprocess
        from contextlib import redirect_stdout
        import io
        calls = []
        def run(args, **kwargs):
            calls.append(args)
            out = "sha256:" + "1" * 64 if args[:3] == ["docker", "image", "inspect"] else ""
            return subprocess.CompletedProcess(args, 0, stdout=out, stderr="")
        with tempfile.TemporaryDirectory() as d, patch("subprocess.run", side_effect=run), redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, "image digest mismatch"):
                R.orchestrate(Path(d) / "attempt", development=True,
                              source_sha="0" * 40, image_digest="sha256:" + "0" * 64)
        self.assertFalse(any(args[:2] == ["docker", "run"] for args in calls))


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



FINAL_PY, FINAL_RUST = "sha256:" + "a1" * 32, "sha256:" + "b2" * 32
FINAL_REF = "qdl-v2-python:2.2.3-abcdef1"
OVERRIDES = {"python": FINAL_PY, "query": FINAL_PY, "core": FINAL_RUST, "stream": FINAL_RUST}


class ImageParameters(unittest.TestCase):
    """The image set is a parameter; the pinned constants are defaults; containers check the attested values."""

    def test_parse_and_resolve(self):
        flags = [f"{k}={v}" for k, v in OVERRIDES.items()]
        self.assertEqual(R.parse_image_overrides(flags), OVERRIDES)
        self.assertEqual(R.resolve_images(), R.IMAGES)
        resolved = R.resolve_images(OVERRIDES)
        self.assertEqual({k: resolved[k] for k in OVERRIDES}, OVERRIDES)
        self.assertEqual(resolved["kafka"], R.IMAGES["kafka"])
        for bad in (["core"], ["core=latest"], ["nope=" + FINAL_RUST], ["core=" + FINAL_RUST, "core=" + FINAL_RUST]):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                R.parse_image_overrides(bad)

    def test_query_ref_and_digest_change_together(self):
        self.assertEqual(R.resolve_query_ref(R.IMAGES), R.QUERY_IMAGE_REF)
        self.assertEqual(R.resolve_query_ref(R.resolve_images(OVERRIDES), FINAL_REF), FINAL_REF)
        for images, ref in ((R.resolve_images(OVERRIDES), None), (R.IMAGES, FINAL_REF),
                            (R.resolve_images(OVERRIDES), "final")):
            with self.subTest(ref=ref), self.assertRaises(ValueError):
                R.resolve_query_ref(images, ref)

    def _final_packet(self):
        p = packet()
        p["images"] = R.resolve_images(OVERRIDES)
        p["query_provenance"] = {"image_ref": FINAL_REF, "actual_image_digest": FINAL_PY}
        return p

    def test_in_container_check_uses_the_attested_env(self):
        env = {R.ENV_IMAGES: json.dumps(R.resolve_images(OVERRIDES)), R.ENV_QUERY_IMAGE_REF: FINAL_REF}
        final = self._final_packet()
        with patch.dict(os.environ, env):
            R.validate_query_provenance(final)
            with self.assertRaises(ValueError):
                R.validate_query_provenance(packet())  # the rc2 packet is not what the parent attested
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(R.ENV_IMAGES, None)
            os.environ.pop(R.ENV_QUERY_IMAGE_REF, None)
            R.validate_query_provenance(packet())  # no attestation: the pinned defaults, as before
            with self.assertRaises(ValueError):
                R.validate_query_provenance(final)

    def test_packet_images_must_equal_the_attested_set(self):
        p = self._final_packet()
        env = {"P184_DR08_ISOLATED_NAMESPACE": p["namespace"], R.ENV_IMAGES: json.dumps(p["images"]),
               R.ENV_QUERY_IMAGE_REF: FINAL_REF}
        with patch.dict(os.environ, env):
            R.validate_packet(p, development=True)
            p["images"] = {**p["images"], "stream": "sha256:" + "c" * 64}
            with self.assertRaisesRegex(ValueError, "parent-attested"):
                R.validate_packet(p, development=True)

    def test_flags_only_with_orchestrate(self):
        for argv in (["--image", "core=" + FINAL_RUST], ["--query-image-ref", FINAL_REF]):
            with self.subTest(argv=argv), self.assertRaises(SystemExit), patch("sys.stderr"):
                R.main(["--packet", "p", "--output", "o", *argv])

    def test_orchestrate_launches_and_attests_the_passed_images(self):
        import subprocess
        from contextlib import redirect_stdout
        import io
        calls = []
        cfg = {"core": {"bindings": [{"venue": "BINANCE", "market": "USDM",
               "native_symbol": "BTCUSDT", "native_channel": "btcusdt@trade"}]}, "authority": {}}
        final_images = R.resolve_images(OVERRIDES)

        def run(args, **kwargs):
            calls.append(args)
            out = ""
            if args[:3] == ["docker", "image", "inspect"]:
                out = args[3]
            elif args[:3] == ["docker", "inspect", "qdl_v2_stable_candidate-rust_core-1"]:
                out = json.dumps([{"Image": FINAL_RUST, "Id": "shared-core"}])
            elif args[:3] == ["docker", "exec", "shared-core"]:
                out = json.dumps(cfg)
            elif args[:2] == ["docker", "run"]:
                raise subprocess.TimeoutExpired(args, 100)  # stop at the first launch
            elif args[:2] == ["docker", "inspect"]:
                raise subprocess.CalledProcessError(1, args)
            return subprocess.CompletedProcess(args, 0, stdout=out, stderr="")

        with tempfile.TemporaryDirectory() as d, patch("subprocess.run", side_effect=run), \
                redirect_stdout(io.StringIO()):
            target = Path(d) / "attempt"
            with self.assertRaises(subprocess.TimeoutExpired):
                R.orchestrate(target, development=True, source_sha="0" * 40, image_digest="sha256:" + "0" * 64,
                              images=OVERRIDES, query_image_ref=FINAL_REF)
            written = json.loads((target / "packet.json").read_text())
        inspected = {a[3] for a in calls if a[:3] == ["docker", "image", "inspect"]}
        self.assertEqual(inspected, set(final_images.values()))
        self.assertEqual(written["images"], final_images)
        self.assertEqual(written["query_provenance"]["image_ref"], FINAL_REF)
        launch = next(a for a in calls if a[:2] == ["docker", "run"])
        self.assertIn(FINAL_PY, launch)  # the prepare role runs the final python image
        self.assertIn(R.ENV_IMAGES + "=" + json.dumps(final_images, sort_keys=True), launch)
        self.assertIn(R.ENV_QUERY_IMAGE_REF + "=" + FINAL_REF, launch)

    def test_orchestrate_refuses_a_deployed_core_other_than_the_passed_one(self):
        import subprocess
        from contextlib import redirect_stdout
        import io

        def run(args, **kwargs):
            out = ""
            if args[:3] == ["docker", "image", "inspect"]:
                out = args[3]
            elif args[:3] == ["docker", "inspect", "qdl_v2_stable_candidate-rust_core-1"]:
                out = json.dumps([{"Image": R.IMAGES["core"], "Id": "shared-core"}])
            self.assertNotEqual(args[:2], ["docker", "run"])
            return subprocess.CompletedProcess(args, 0, stdout=out, stderr="")

        with tempfile.TemporaryDirectory() as d, patch("subprocess.run", side_effect=run), \
                redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, "deployed core image drift"):
                R.orchestrate(Path(d) / "attempt", development=True, source_sha="0" * 40,
                              image_digest="sha256:" + "0" * 64, images=OVERRIDES, query_image_ref=FINAL_REF)


def deployed(symbol, channel, uid="u"):
    return {"venue": "BINANCE", "market": "USDM", "native_symbol": symbol, "native_channel": channel,
            "instrument_uid": uid + symbol, "source_id": symbol.lower() + "-" + channel}


def deployed_set():
    rows = []
    for symbol in R.ISOLATION_DEEP + R.ISOLATION_IDLE:
        lower = symbol.lower()
        rows += [deployed(symbol, lower + "@trade"), deployed(symbol, lower + "@bookTicker"),
                 deployed(symbol, lower + "@markPrice@1s"), deployed(symbol, lower + "@depth@100ms")]
    rows.append({**deployed("BTC-USDT-SWAP", "trades"), "venue": "OKX", "market": "SWAP"})
    return rows


def outcome(**overrides):
    base = {"observed": True, "idle_readers": 32, "idle_client_terminals": 0,
            "idle_stream_abnormal_closures": 0, "idle_not_progressed": 0,
            "deep_stream_abnormal_closures": 3, "other_abnormal_closures": 0,
            "reserve_exhausted": 0, "over_floor_refused": 5, "reserve_admitted": 9,
            "deep_resume_lossless": True, "deep_resumed": 3,
            "peak_queue_bytes": 30 << 20, "peak_reserve_bytes_used": 1 << 20, "trade_rate_per_s": 5000.0}
    base.update(overrides)
    return base


class LiveBudgetIsolation(unittest.TestCase):
    def test_selects_one_deployed_binding_per_route_in_route_order(self):
        rows = R.isolation_bindings(deployed_set())
        self.assertEqual([(r["native_symbol"], R.isolation_feed(r["native_channel"])) for r in rows],
                         R.isolation_routes())
        self.assertEqual(len(rows), 3 + 2 * 2)

    def test_refuses_missing_or_ambiguous_routes(self):
        rows = [r for r in deployed_set() if r["native_channel"] != "dogeusdt@markPrice@1s"]
        with self.assertRaisesRegex(ValueError, "lacks isolation routes"):
            R.isolation_bindings(rows)
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            R.isolation_bindings(deployed_set() + [deployed("BTCUSDT", "btcusdt@trade")])

    def test_frames_carry_the_fields_the_core_normalizer_requires(self):
        trade = R.isolation_frame("TRADE", "BTCUSDT", 7, 1_000_000_000)
        self.assertEqual((trade["e"], trade["s"], trade["t"]), ("trade", "BTCUSDT", 7))
        quote = R.isolation_frame("QUOTE", "BNBUSDT", 8, 1_000_000_000)
        self.assertEqual(quote["e"], "bookTicker")
        self.assertTrue(all(field in quote for field in ("u", "b", "B", "a", "A", "T")))
        mark = R.isolation_frame("MARK_INDEX_PRICE", "DOGEUSDT", 9, 1_000_000_000)
        self.assertEqual((mark["e"], mark["E"]), ("markPriceUpdate", 1000))
        self.assertNotEqual(mark["p"], R.isolation_frame("MARK_INDEX_PRICE", "DOGEUSDT", 10, 1)["p"])
        with self.assertRaises(ValueError):
            R.isolation_frame("BAR", "BTCUSDT", 1, 1)

    def test_manifests_split_deep_trade_and_idle_quote_mark(self):
        routes = [{"feed": feed, "instrument_uid": sym, "source_policy_id": "p"}
                  for sym, feed in R.isolation_routes()]
        def mapping():
            return {"spec": {"quotas": {"max_streams": 10, "max_buffer_events": 1000},
                             "requirements": [{"feed": "TRADE", "instrument_uid": "x", "warmup_limit": 1}]}}
        deep = R.isolation_manifest(mapping(), "deep", routes)
        idle = R.isolation_manifest(mapping(), "idle", routes)
        self.assertEqual({r["feed"] for r in deep["spec"]["requirements"]}, {"TRADE"})
        self.assertEqual(len(deep["spec"]["requirements"]), 3)
        self.assertEqual(deep["spec"]["quotas"]["max_buffer_events"], R.DEEP_BUFFER)
        self.assertEqual({r["feed"] for r in idle["spec"]["requirements"]}, set(R.ISOLATION_IDLE_FEEDS))
        self.assertGreaterEqual(idle["spec"]["quotas"]["max_streams"], 4 * R.IDLE_READERS_PER_ROUTE)
        self.assertEqual(idle["spec"]["requirements"][0]["warmup_limit"], 1)

    def test_verdict_passes_only_a_discriminating_isolating_pair(self):
        baseline = outcome(idle_stream_abnormal_closures=20, idle_client_terminals=20,
                           reserve_exhausted=None, over_floor_refused=None)
        self.assertEqual(R.isolation_verdict(outcome(), baseline)["verdict"], "PASS")
        for broken in (outcome(idle_client_terminals=1), outcome(idle_stream_abnormal_closures=1),
                       outcome(idle_not_progressed=1), outcome(reserve_exhausted=1),
                       outcome(reserve_exhausted=None), outcome(over_floor_refused=0),
                       outcome(other_abnormal_closures=1), outcome(deep_stream_abnormal_closures=0),
                       outcome(deep_resume_lossless=False), {"observed": False}):
            self.assertEqual(R.isolation_verdict(broken, baseline)["verdict"], "FAIL", broken)
        # A baseline without idle overflow cannot discriminate.
        self.assertEqual(R.isolation_verdict(outcome(), outcome())["verdict"], "FAIL")

    def test_outcome_counts_abnormal_closures_per_identity(self):
        facts = {"observed": True, "deep_identity": "d", "idle_identity": "i",
                 "idle": [{"terminal": None, "progressed": True}, {"terminal": "RESOURCE_EXHAUSTED",
                          "progressed": False}],
                 "closures": {"d": {"ended": 1, "Some resource has been exhausted": 3},
                              "i": {"ended": 30, "Some resource has been exhausted": 1}},
                 "resume": {"BTCUSDT/TRADE": {"needed": True, "lossless": True}},
                 "metrics": {"queue_isolation": {"reserve_exhausted": 0, "over_floor_refused": 4,
                                                 "reserve_admitted": 2},
                             "peak_queue_bytes": 33554000, "peak_reserve_bytes_used": 4193616},
                 "burst": {"trade_rate_per_s": 1.0}}
        result = R.isolation_outcome(facts)
        self.assertEqual((result["idle_client_terminals"], result["idle_stream_abnormal_closures"],
                          result["deep_stream_abnormal_closures"], result["idle_not_progressed"]), (1, 1, 3, 1))
        self.assertEqual(R.isolation_outcome({"observed": False}), {"observed": False})

    def test_metric_summary_reports_reserve_use_above_the_shared_part(self):
        shared = R.QUEUE_BYTES - R.LIVE_RESERVE_BYTES
        summary = R.isolation_metric_summary([{"queue_bytes": shared - 5}, {"queue_bytes": shared + 1234,
                                               "queue_isolation": {"reserve_exhausted": 0}}])
        self.assertEqual(summary["peak_reserve_bytes_used"], 1234)
        self.assertEqual(R.isolation_metric_summary([{"queue_bytes": 10}])["peak_reserve_bytes_used"], 0)

    def test_closures_are_parsed_from_the_stream_log(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stream.log"
            path.write_text("text\n" + "\n".join(json.dumps(row) for row in (
                {"event": "qdl_kn_subscription_closed", "consumer_id": "a", "outcome": "ended"},
                {"event": "qdl_kn_subscription_closed", "consumer_id": "a", "outcome": "x"},
                {"event": "qdl_kn_gateway_metrics", "queue_bytes": 1})) + "\n")
            self.assertEqual(R.isolation_closures(path), {"a": {"ended": 1, "x": 1}})

    def test_preregistration_fixes_the_ab_variable_and_acceptance(self):
        a, b = "sha256:" + "a" * 64, "sha256:" + "b" * 64
        prereg = R.isolation_prereg(candidate=a, baseline=b, images={"core": "sha256:" + "c" * 64},
            query_image_ref="q", source_sha="1" * 40, now_utc="t")
        self.assertEqual(prereg["acceptance"], R.ISOLATION_ACCEPTANCE)
        self.assertEqual(prereg["scenario"]["idle_readers"], 32)
        self.assertEqual(prereg["scenario"]["live_budget_bytes"], 32 << 20)
        self.assertEqual((prereg["scenario"]["idle_hold_seconds"], prereg["scenario"]["idle_hold_rate_hz_per_route"]),
                         (R.IDLE_HOLD_SECONDS, R.IDLE_HOLD_RATE_HZ))
        for kwargs in ({"candidate": a, "baseline": a}, {"candidate": "latest", "baseline": b}):
            with self.assertRaises(ValueError):
                R.isolation_prereg(images={}, query_image_ref="q", source_sha="1" * 40, now_utc="t", **kwargs)
        with self.assertRaisesRegex(ValueError, "A/B variable"):
            R.isolation_prereg(candidate=a, baseline=b, images={"stream": a}, query_image_ref="q",
                               source_sha="1" * 40, now_utc="t")


if __name__ == "__main__":
    unittest.main()
