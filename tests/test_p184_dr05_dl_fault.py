"""Offline contracts only; no Docker daemon or provider traffic."""
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from scripts import p184_dr05_dl_fault as dr

NS = "p184-dr05-dl-0123456789abcdef"


class PreparationTests(unittest.TestCase):
    def test_exact_nonqualifying_cells(self):
        rows = dr.pending_rows()
        self.assertEqual({r["cell"] for r in rows}, {
            "DR05.GAP_REFUSAL@BINANCE", "DR05.GAP_REFUSAL@OKX",
            "DR05.GENERATION_CHANGE_FENCED@BINANCE", "DR05.GENERATION_CHANGE_FENCED@OKX"})
        self.assertTrue(all(r["ok"] is False and r["outcome"] == "PREPARED_NOT_RUN"
                            and r["level"] == "ISOLATED_FAULT" for r in rows))

    def test_namespace_rejects_shared_and_injection(self):
        for value in ("production", NS + ";sh", "", "p184-dr08-0123456789abcdef"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                dr.namespace(value)

    def test_topology_is_bounded_and_disconnected(self):
        commands = dr.topology(NS, "/tmp/dr05-test")
        self.assertEqual(set(commands), set(dr.ROLES))
        for role, argv in commands.items():
            with self.subTest(role=role):
                for required in ("--rm", "--pull=never", "--read-only", "--memory", "--cpus", "--pids-limit", "--log-opt"):
                    self.assertIn(required, argv)
                self.assertIn(dr.LABEL + "=" + NS, argv)
                self.assertEqual(argv[argv.index("--network") + 1],
                                 "none" if role == "kafka" else "container:" + NS + "-kafka")
                self.assertFalse(set(argv) & {"-p", "--publish", "--privileged", "--network=host"})
                self.assertNotIn("/run/secrets", " ".join(argv))
                self.assertTrue(any(a.startswith("sha256:") for a in argv))
        self.assertIn("scripts/p184_dr05_observer.py", commands["query"])
        self.assertIn("--query", commands["query"])

    def test_external_directory_required(self):
        for work in (dr.ROOT, dr.ROOT / "runtime", "/tmp/a:rw", "/tmp/a,b"):
            with self.subTest(work=work), self.assertRaises(ValueError):
                dr.topology(NS, work)

    def test_prepare_exclusive_no_subprocess(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(dr, "sh") as command:
            work = Path(tmp) / "case"
            packet = dr.prepare(work)
            self.assertFalse(packet["acceptance"])
            self.assertFalse(packet["adapter_ready"])
            self.assertEqual(len(packet["schedules"]), 4)
            self.assertEqual(work.stat().st_mode & 0o777, 0o700)
            self.assertEqual(len((work / "pending-cells.jsonl").read_text().splitlines()), 4)
            with self.assertRaises(FileExistsError):
                dr.prepare(work)
            command.assert_not_called()

    def test_provider_frames(self):
        for venue in dr.VENUES:
            snap = dr.frame(venue, True, 100, 0, 7, "1", 123000000)
            delta = dr.frame(venue, False, 110, 109, 7, "3", 124000000)
            self.assertTrue(snap["test_provenance"])
            self.assertEqual(snap["provenance"], "TEST_ONLY")
            self.assertEqual(delta["transport"], "WEBSOCKET")
            if venue == "BINANCE":
                self.assertEqual(snap["transport"], "HTTP")
                self.assertEqual(delta["payload"]["pu"], 109)
                self.assertEqual(snap["payload"]["lastUpdateId"], 100)
            else:
                self.assertEqual(snap["payload"]["data"][0]["prevSeqId"], "-1")
                self.assertEqual(delta["payload"]["data"][0]["seqId"], "110")

    def test_schedules_positive_gap_return_and_stale(self):
        for venue in dr.VENUES:
            for fault in dr.FAULTS:
                steps = dr.schedule(venue, fault)
                self.assertEqual(steps[1]["phase"], "positive_control")
                self.assertEqual(steps[-1]["phase"], "post_fence_control")
                stale = next(s for s in steps if s["phase"] == "old_generation")
                self.assertEqual((stale["generation"], stale["quantity"]), (7, "99"))
                self.assertEqual(steps[-1]["generation"], 8)
                self.assertEqual(steps[2]["previous"], 109 if fault == "GAP_REFUSAL" else 200)

    def test_unknown_cells_rejected(self):
        with self.assertRaises(ValueError):
            dr.schedule("BINANCE", "STALE_TYPED")
        with self.assertRaises(ValueError):
            dr.frame("OTHER", True, 1, 0, 1, "1", 123)

    def test_adapter_status_cannot_fabricate_pass(self):
        class Adapter:
            def fresh_case(self, venue, fault, timeout):
                self.offset = 0

            def publish(self, raw, timeout):
                self.offset += 1
                return self.offset

            def observe(self, offset, timeout):
                return {"ok": True, "status": "FAKE_SUCCESS"}

        for venue in dr.VENUES:
            for fault in dr.FAULTS:
                row = dr.drive(Adapter(), venue, fault)
                self.assertFalse(row["ok"])
                self.assertEqual(row["outcome"], "OWNER_REVIEW_REQUIRED")
                self.assertEqual(len(row["observations"]), len(dr.schedule(venue, fault)))

    def test_adapter_failure_propagates(self):
        class Adapter:
            def fresh_case(self, *args, **kwargs):
                raise TimeoutError("no positive control")
        with self.assertRaises(TimeoutError):
            dr.drive(Adapter(), "OKX", "GAP_REFUSAL")


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.rows = {}
        self.calls = []
        for role in dr.ROLES:
            self.rows[role] = {"Name": "/" + NS + "-" + role, "Id": role + "-id",
                "Config": {"Labels": {dr.LABEL: NS}}, "State": {"Running": True},
                "Image": dr.IMAGES[{"quota": "redis"}.get(role, role)],
                "HostConfig": {"AutoRemove": True, "PortBindings": {},
                    "NetworkMode": "none" if role == "kafka" else "container:kafka-id"}}

    def call(self, argv):
        self.calls.append(argv)
        if argv[1] == "ps":
            if argv[-1].startswith("id="):
                identity = argv[-1].removeprefix("id=")
                return "\n".join(row["Id"] for row in self.rows.values() if row["Id"] == identity)
            role = argv[-1].removesuffix("$").split(NS + "-")[1]
            return self.rows[role]["Id"] if role in self.rows else ""
        role = argv[-1].removesuffix("-id")
        if argv[1] == "inspect":
            return json.dumps([self.rows[role]])
        if argv[1] == "stop":
            del self.rows[role]
            return ""
        raise AssertionError(argv)

    def test_process_ready_is_not_acceptance(self):
        result = dr.readiness(NS, self.call)
        self.assertTrue(result["process_isolation_ready"])
        self.assertFalse(result["functional_ready"])
        self.assertFalse(result["acceptance"])

    def test_readiness_rejects_bad_image_network_ports_and_state(self):
        original = copy.deepcopy(self.rows)
        for section, key, value in ((None, "Image", "bad"), ("HostConfig", "NetworkMode", "host"),
                                    ("HostConfig", "PortBindings", {"8210": [{}]}),
                                    ("HostConfig", "AutoRemove", False), ("State", "Running", False)):
            self.rows = copy.deepcopy(original)
            target = self.rows["stream"] if section is None else self.rows["stream"][section]
            target[key] = value
            self.assertFalse(dr.readiness(NS, self.call)["process_isolation_ready"])

    def test_missing_role_not_ready(self):
        del self.rows["query"]
        self.assertFalse(dr.readiness(NS, self.call)["process_isolation_ready"])

    def test_cleanup_reverse_order_and_idempotent(self):
        self.assertEqual(dr.cleanup(NS, self.call)["removed"], list(reversed(dr.ROLES)))
        self.assertEqual(dr.cleanup(NS, self.call)["removed"], [])
        self.assertFalse(any("rm" in cmd or "prune" in cmd for cmd in self.calls))

    def test_cleanup_ownership_preflight_stops_nothing(self):
        self.rows["kafka"]["Config"]["Labels"][dr.LABEL] = "other"
        with self.assertRaises(ValueError):
            dr.cleanup(NS, self.call)
        self.assertFalse(any(cmd[1] == "stop" for cmd in self.calls))

    def test_cleanup_rejects_retained_container(self):
        self.rows["kafka"]["HostConfig"]["AutoRemove"] = False
        with self.assertRaises(ValueError):
            dr.cleanup(NS, self.call)
        self.assertFalse(any(cmd[1] == "stop" for cmd in self.calls))

    def test_cleanup_failure_is_not_hidden(self):
        def no_stop(argv):
            return "" if argv[1] == "stop" else self.call(argv)
        with self.assertRaises(RuntimeError):
            dr.cleanup(NS, no_stop)

    def test_cleanup_stop_race_requires_exact_inspected_absence(self):
        original = copy.deepcopy(self.rows["query"])
        cases = ("absent", "present", "replacement", "foreign_replacement",
                 "unrelated_error", "wrong_id_error", "extra_error", "list_error", "name_list_error")
        for case in cases:
            with self.subTest(case=case):
                self.rows, self.calls = {"query": copy.deepcopy(original)}, []
                stopped = False
                def call(argv):
                    nonlocal stopped
                    if argv[1] == "stop":
                        self.calls.append(argv)
                        stopped = True
                        if case != "present":
                            del self.rows["query"]
                        if case in ("replacement", "foreign_replacement"):
                            self.rows["query"] = copy.deepcopy(original)
                            self.rows["query"]["Id"] = "replacement-id"
                            if case == "foreign_replacement":
                                self.rows["query"]["Config"]["Labels"][dr.LABEL] = "foreign"
                        message = "Error response from daemon: No such container: query-id"
                        if case == "unrelated_error":
                            message = "permission denied"
                        elif case == "wrong_id_error":
                            message = "Error response from daemon: No such container: other-id"
                        elif case == "extra_error":
                            message += "\nadditional failure"
                        raise subprocess.CalledProcessError(1, argv, stderr=message + "\n")
                    if stopped and argv[1] == "ps":
                        if case == "list_error" and argv[-1] == "id=query-id":
                            raise subprocess.CalledProcessError(1, argv, stderr="daemon unavailable")
                        if case == "name_list_error" and argv[-1].startswith("name="):
                            raise subprocess.CalledProcessError(1, argv, stderr="daemon unavailable")
                    return self.call(argv)
                if case == "absent":
                    self.assertEqual(dr.cleanup(NS, call)["removed"], ["query"])
                    self.assertIn(["docker", "ps", "-aq", "--no-trunc", "--filter", "id=query-id"], self.calls)
                else:
                    with self.assertRaisesRegex(RuntimeError, "cleanup incomplete"):
                        dr.cleanup(NS, call)
                self.assertEqual([cmd[-1] for cmd in self.calls if cmd[1] == "stop"], ["query-id"])
                if case in ("replacement", "foreign_replacement"):
                    self.assertEqual(self.rows["query"]["Id"], "replacement-id")

    def test_cleanup_successful_stop_rejects_replacement_without_waiting(self):
        self.rows = {"query": self.rows["query"]}
        def call(argv):
            if argv[1] == "stop":
                self.calls.append(argv)
                self.rows["query"]["Id"] = "replacement-id"
                self.rows["query"]["State"]["Running"] = False
                return ""
            if argv[1] == "inspect" and argv[-1] == "replacement-id":
                self.calls.append(argv)
                return json.dumps([self.rows["query"]])
            return self.call(argv)
        with patch.object(dr.time, "sleep", side_effect=AssertionError("replacement must not be polled")) as sleep:
            with self.assertRaisesRegex(RuntimeError, "cleanup incomplete"):
                dr.cleanup(NS, call)
            sleep.assert_not_called()
        self.assertEqual([cmd[-1] for cmd in self.calls if cmd[1] == "stop"], ["query-id"])

    def test_cleanup_successful_stop_requires_pinned_id_absence_not_just_name(self):
        self.rows = {"query": self.rows["query"]}
        stopped = False
        def call(argv):
            nonlocal stopped
            if argv[1] == "stop":
                self.calls.append(argv)
                stopped = True
                return ""
            if stopped and argv[1] == "ps" and argv[-1].startswith("name="):
                return ""
            return self.call(argv)
        with self.assertRaisesRegex(RuntimeError, "cleanup incomplete"):
            dr.cleanup(NS, call)
        self.assertEqual(self.rows["query"]["Id"], "query-id")
        self.assertEqual([cmd[-1] for cmd in self.calls if cmd[1] == "stop"], ["query-id"])

    def test_cleanup_inspection_errors_never_become_absence(self):
        original = copy.deepcopy(self.rows["query"])
        for after_stop in (False, True):
            with self.subTest(after_stop=after_stop):
                self.rows, self.calls = {"query": copy.deepcopy(original)}, []
                stopped = False
                def call(argv):
                    nonlocal stopped
                    if argv[1] == "stop":
                        self.calls.append(argv)
                        stopped = True
                        return ""
                    if argv[1] == "inspect" and (stopped or not after_stop):
                        del self.rows["query"]
                        raise subprocess.CalledProcessError(1, argv, stderr="inspection failed")
                    return self.call(argv)
                expected = RuntimeError if after_stop else subprocess.CalledProcessError
                with self.assertRaises(expected):
                    dr.cleanup(NS, call)
                if not after_stop:
                    self.assertFalse(any(cmd[1] == "stop" for cmd in self.calls))

    def test_cleanup_waits_for_async_auto_remove(self):
        original = self.call
        pending = {}
        def call(argv):
            if argv[1] == "stop":
                role = argv[-1].removesuffix("-id")
                self.rows[role]["State"]["Running"] = False
                pending[role] = 2
                return ""
            if argv[1] == "ps" and argv[-1].startswith("name="):
                role = argv[-1].removesuffix("$").split(NS + "-")[1]
                if role in pending:
                    pending[role] -= 1
                    if pending[role] == 0:
                        del self.rows[role]
            return original(argv)
        with patch.object(dr.time, "sleep") as sleep:
            self.assertEqual(dr.cleanup(NS, call)["removed"], list(reversed(dr.ROLES)))
            self.assertEqual(sleep.call_count, 5)

    def test_disappeared_between_list_and_inspect(self):
        import subprocess
        original = self.call
        def call(argv):
            if argv[1] == "inspect":
                del self.rows["query"]
                raise subprocess.CalledProcessError(1, argv)
            return original(argv)
        self.assertIsNone(dr.owned(NS, "query", call))


class TimeoutTests(unittest.TestCase):
    def test_helper_timeout_cleans_container_and_private_files(self):
        import subprocess
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "case"
            def fail(argv, timeout=120):
                for name in dr.PRIVATE_FILES:
                    (work / name).write_text("test-only")
                raise subprocess.TimeoutExpired(argv, timeout)
            with patch.object(dr, "bounded", side_effect=fail), patch.object(dr, "cleanup") as cleanup:
                with self.assertRaises(subprocess.TimeoutExpired):
                    dr.init(work, Path(tmp) / "export.json")
                cleanup.assert_called_once()
                self.assertTrue(all(not (work / n).exists() for n in dr.PRIVATE_FILES))
                self.assertFalse(json.loads((work / "handoff.json").read_text())["material_ready"])

    def test_private_cleanup_does_not_touch_other_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "case"
            dr.prepare(work)
            (work / "unrelated.key").write_text("retained")
            for name in dr.PRIVATE_FILES:
                (work / name).write_text("test-only")
            dr.erase_private(work)
            dr.erase_private(work)
            self.assertEqual((work / "unrelated.key").read_text(), "retained")
            self.assertTrue(all(not (work / n).exists() for n in dr.PRIVATE_FILES))

    def test_timed_out_helper_is_owned_and_removed(self):
        calls = []
        live = True
        def call(argv):
            nonlocal live
            calls.append(argv)
            if argv[1] == "ps":
                return "helper-id" if live and argv[-1].endswith("-helper$") else ""
            if argv[1] == "inspect":
                return json.dumps([{"Name": "/" + NS + "-helper", "Id": "helper-id",
                    "Config": {"Labels": {dr.LABEL: NS}}, "HostConfig": {"AutoRemove": True}}])
            if argv[1] == "stop":
                live = False
                return ""
            raise AssertionError(argv)
        self.assertEqual(dr.cleanup(NS, call)["removed"], ["helper"])
        self.assertFalse(live)


class MaterialTests(unittest.TestCase):
    @staticmethod
    def exported():
        import yaml
        catalog = yaml.safe_load((dr.ROOT / "config/v2/stable-source-bindings.yaml").read_text())
        bindings = []
        for venue, market, symbol in (("BINANCE", "USDM", "ETHUSDT"), ("OKX", "SWAP", "ETH-USDT-SWAP")):
            instrument = next(i for i in catalog["instruments"] if
                              (i["venue"], i["market"], i["native_symbol"]) == (venue, market, symbol))
            source = next(b["source"] for b in catalog["bindings"] if
                          b["instrument_uid"] == instrument["instrument_uid"] and b["feed"] == "BOOK_DELTA")
            bindings.append({"provider": source["provider"], "venue": venue, "market": market,
                "product_type": "PERPETUAL", "native_symbol": symbol,
                "native_channel": "ethusdt@depth@100ms" if venue == "BINANCE" else "books",
                "provider_kind": "binance_usdm_book" if venue == "BINANCE" else "okx_book",
                "instrument_uid": instrument["instrument_uid"], "instrument_id": instrument["instrument_id"],
                "instrument_revision": instrument["metadata_revision"],
                "instrument_catalog_revision": catalog["catalog_revision"], "source_id": source["source_id"],
                "source_role": "PRIMARY", "normalizer_version": source["normalizer_version"],
                "sequence_policy": "CONTIGUOUS", "l2": {
                    "provider_protocol": "BINANCE_DIFF_DEPTH" if venue == "BINANCE" else "OKX_PUBLIC_BOOKS",
                    "depth_per_side": 100, "snapshot_refresh_seconds": 30,
                    "materialized_snapshot_interval_ms": 100}})
        return {"core": {"bindings": bindings}, "password": "MUST_NOT_COPY", "bootstrap": "production"}

    def test_sanitized_configuration(self):
        exported = self.exported()
        cfg = dr.isolated_config(exported, NS)
        self.assertEqual({b["venue"] for b in cfg["core"]["bindings"]}, set(dr.VENUES))
        self.assertTrue(cfg["core"]["allow_test_provenance"])
        self.assertEqual(cfg["raw_topics"], [NS + ".raw"])
        self.assertFalse(cfg["authority"]["public_write_allowed"])
        self.assertNotIn("MUST_NOT_COPY", json.dumps(cfg))
        self.assertNotIn("production", json.dumps(cfg))
        exported["core"]["bindings"].pop()
        with self.assertRaises(ValueError):
            dr.isolated_config(exported, NS)

    def test_duplicate_book_rejected(self):
        exported = self.exported()
        exported["core"]["bindings"].append(exported["core"]["bindings"][0])
        with self.assertRaises(ValueError):
            dr.isolated_config(exported, NS)

    def test_real_offline_material_and_fresh_keys(self):
        from cryptography import x509
        from scripts.kn_gateway_bundle import compile_bundle
        from qdl.consumer.manifest import ConsumerManifestLoader
        from qdl.runtime.stable_catalog import StableSourceCatalog
        with tempfile.TemporaryDirectory() as tmp:
            fingerprints = []
            for index in range(2):
                work = Path(tmp) / str(index)
                packet = dr.prepare(work)
                result = dr.material(work, self.exported())
                self.assertTrue(result["material_ready"])
                self.assertFalse(result["adapter_ready"])
                cfg = json.loads((work / "core.json").read_text())
                self.assertEqual(len(cfg["core"]["bindings"]), 2)
                catalog = StableSourceCatalog.load(work / "catalog.yaml")
                self.assertEqual(len(catalog.bindings), 4)
                manifests = [work / (venue.lower() + ".yaml") for venue in dr.VENUES]
                for path in manifests:
                    manifest = ConsumerManifestLoader.load(path)
                    self.assertTrue(manifest.consumer_id.startswith(packet["namespace"] + "."))
                    self.assertEqual({r.feed.value for r in manifest.requirements}, {"BOOK_DELTA", "BOOK_SNAPSHOT"})
                self.assertEqual(json.loads((work / "bundle.json").read_text()), compile_bundle(
                    environment="paper", catalog_path=work / "catalog.yaml", manifest_paths=manifests))
                for name in ("client.key", "binance.key", "okx.key", "server.p12", "cursor-keys.json", "read-view.secret"):
                    self.assertEqual((work / name).stat().st_mode & 0o777, 0o600)
                cert = x509.load_pem_x509_certificate((work / "client.crt").read_bytes())
                fingerprints.append(cert.serial_number)
                self.assertIn("localhost", cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value.get_values_for_type(x509.DNSName))
                runtime = json.loads((work / "packet.json").read_text())
                self.assertNotIn("topic_id", runtime["slice"])
                self.assertEqual(runtime["bootstrap"], "localhost:9092")
                with self.assertRaises(FileExistsError):
                    dr.material(work, self.exported())
            self.assertNotEqual(*fingerprints)

    def test_correct_quota_prefix_and_helper_network(self):
        stream = dr.topology(NS, "/tmp/dr05-test", "test-topic-id")["stream"]
        self.assertIn("QDL_KN_QUOTA_PREFIX=qdl:stable:v2:paper:" + NS, stream)
        self.assertIn("QDL_KN_TOPIC_ID=test-topic-id", stream)
        with self.assertRaises(ValueError):
            dr.helper_command(NS, "/tmp/dr05-test", [], network="production")


if __name__ == "__main__":
    unittest.main()
