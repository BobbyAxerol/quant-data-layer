import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts import p184_dr05_case as case
from scripts import p184_dr05_dl_fault as dr
from tests import test_p184_dr05_dl_fault as fixtures

NS = "p184-dr05-dl-0123456789abcdef"
WORK = Path("/tmp/dr05-case-test")


def runtime_packet():
    return {"namespace": NS, "provenance": "TEST_ONLY", "bootstrap": "localhost:9092",
            "client_tls": {"ca": "/work/ca.crt", "cert": "/work/client.crt", "key": "/work/client.key"},
            "slice": {"topic_id": "old-topic"}}


def config():
    return {"raw_topics": [NS + ".raw"], "core": {"canonical_stream": NS + ".canonical",
        "quarantine_stream": NS + ".quarantine", "allow_test_provenance": True}}


def owned_rows():
    rows = {}
    for role in dr.ROLES:
        rows[role] = {"Id": role + "-id", "Image": dr.IMAGES[{"quota": "redis"}.get(role, role)],
            "HostConfig": {"AutoRemove": True, "ReadonlyRootfs": True, "PortBindings": {},
                "NetworkMode": "none" if role == "kafka" else "container:kafka-id",
                "Tmpfs": {"/data": "rw"} if role == "kafka" else {}},
            "Mounts": [{"Type": "bind", "Destination": "/work", "Source": str(WORK)},
                       {"Type": "bind", "Destination": "/app", "Source": str(dr.ROOT)}]}
    return rows


class PacketTests(unittest.TestCase):
    def test_material_packet_binding_v3_parity(self):
        from qdl.consumer.manifest import ConsumerManifestLoader
        from qdl.consumer.universal_release import ConsumerRouteBinding
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "case"
            dr.prepare(work)
            dr.material(work, fixtures.MaterialTests.exported())
            for venue in dr.VENUES:
                label = venue.lower()
                spec = json.loads((work / f"ts-consumer.{label}.json").read_text())
                route = json.loads((work / f"{label}.binding.json").read_text())
                binding = ConsumerRouteBinding.from_canonical_mapping(route)
                manifest = ConsumerManifestLoader.load(work / f"{label}.yaml")
                self.assertEqual(spec["schema"], "p184.dr05.ts-consumer.v1")
                self.assertEqual(spec["venue"], venue)
                self.assertEqual(set(spec), {"schema", "venue", "consumer_id", "subject",
                    "query_url", "grpc_target", "tls_ca_file", "tls_cert_file", "tls_key_file",
                    "binding_file", "jwt_private_key_file", "jwt_key_id", "issuer", "audience",
                    "environment", "manifest_revision", "release_manifest_sha256",
                    "execution_book_source_policy_id"})
                self.assertEqual(len(binding.products), 2)
                self.assertEqual(len(binding.request_policies), 2)
                self.assertEqual(route["schema"], "qdl.v2.consumer-route-binding.v3")
                self.assertEqual(spec["consumer_id"], manifest.consumer_id)
                self.assertEqual(spec["manifest_revision"], manifest.manifest_revision)
                self.assertEqual(spec["release_manifest_sha256"], binding.universal_manifest_sha256)
                self.assertEqual(spec["release_manifest_sha256"], hashlib.sha256((work / f"{label}.yaml").read_bytes()).hexdigest())
                self.assertEqual(spec["query_url"], "https://localhost:8200")
                self.assertEqual(spec["grpc_target"], "localhost:8210")
                self.assertEqual(spec["subject"], "spiffe://qdl/paper/" + manifest.consumer_id)
                self.assertNotIn("PRIVATE KEY", json.dumps(spec))
                for key, value in spec.items():
                    if key.endswith("_file"):
                        self.assertTrue(value.startswith("/work/"))
                        self.assertTrue((work / Path(value).name).is_file())
                for product, policy in zip(binding.products, binding.request_policies):
                    req = next(r for r in manifest.requirements if r.feed.value == product.feed)
                    self.assertEqual(product.venue, venue)
                    self.assertEqual(product.fallback, "BLOCKED")
                    self.assertEqual(policy.requirement_id, product.requirement_id)
                    self.assertEqual(policy.event_recency_policy, None if req.event_recency_policy is None else req.event_recency_policy.value)
                    self.assertEqual(policy.max_session_liveness_ms, req.max_session_liveness_ms)
                    self.assertEqual(product.max_freshness_ms, req.max_freshness_ms)

    @patch.object(dr.os, "access", return_value=True)
    def test_core_override_pinned_readonly_and_tampering_refused(self, _access):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "qdl-realtime-core"
            binary.write_bytes(b"test-only-not-a-real-binary")
            binary.chmod(0o700)
            selected = dr.core_override(binary)
            self.assertEqual(selected["sha256"], hashlib.sha256(binary.read_bytes()).hexdigest())
            commands = dr.topology(NS, WORK, core_binary=selected)
            self.assertIn(str(binary) + ":/usr/local/bin/qdl-realtime-core:ro", commands["core"])
            self.assertNotIn(str(binary), json.dumps({k: v for k, v in commands.items() if k != "core"}))
            binary.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "digest changed"):
                dr.topology(NS, WORK, core_binary=selected)

    def test_reserved_ports_absent(self):
        commands = json.dumps(dr.topology(NS, WORK))
        self.assertNotIn("6391", commands)
        self.assertNotIn("5433", commands)

    def test_image_declared_volumes_are_tmpfs(self):
        commands = dr.topology(NS, WORK)
        for role, paths in (("kafka", ("/etc/kafka/secrets", "/mnt/shared/config", "/var/lib/kafka/data", "/data")), ("quota", ("/data",))):
            tmpfs = [commands[role][i + 1].split(":")[0] for i, arg in enumerate(commands[role]) if arg == "--tmpfs"]
            self.assertTrue(set(paths) <= set(tmpfs))

    def test_deadline_rejects_invalid(self):
        for value in (True, 0, -1, 121, float("inf"), float("nan"), "10"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                case._deadline(value)

    def test_packet_rejects_shared_transport_and_topics(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            handoff = {"namespace": NS, "provenance": "TEST_ONLY", "material_ready": True}
            (work / "handoff.json").write_text(json.dumps(handoff))
            for target in ("bootstrap", "tls", "raw", "canonical", "quarantine", "test"):
                packet, cfg = runtime_packet(), config()
                if target == "bootstrap": packet["bootstrap"] = "production:9092"
                if target == "tls": packet["client_tls"]["key"] = "/shared/key"
                if target == "raw": cfg["raw_topics"] = ["shared.raw"]
                if target in ("canonical", "quarantine"): cfg["core"][target + "_stream"] = "shared"
                if target == "test": cfg["core"]["allow_test_provenance"] = False
                (work / "packet.json").write_text(json.dumps(packet))
                (work / "core.json").write_text(json.dumps(cfg))
                with self.subTest(target=target), self.assertRaises(ValueError):
                    case._packet(work)


class ResetTests(unittest.TestCase):
    def preflight(self, rows, outsider=""):
        call = Mock(side_effect=["one", outsider or "kafka-id none"])
        with patch.object(dr, "owned", side_effect=lambda ns, role, call: rows.get(role)):
            return case._reset_preflight(WORK, NS, call)

    def test_exact_disposable_accepted(self):
        self.assertEqual(self.preflight(owned_rows())["Id"], "kafka-id")

    def test_refuses_attached_clone_or_channel(self):
        for mode in ("container:kafka-id", "container:" + NS + "-kafka"):
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, "still attached"):
                self.preflight(owned_rows(), "clone-id " + mode)

    def test_refuses_wrong_image_mount_network_or_persistence(self):
        for field in ("Image", "AutoRemove", "NetworkMode", "ReadonlyRootfs", "PortBindings", "Tmpfs", "mount"):
            rows = owned_rows()
            if field == "Image": rows["kafka"][field] = "wrong-image"
            elif field == "mount": rows["kafka"]["Mounts"].append({"Type": "volume", "Destination": "/data", "Source": "shared"})
            else: rows["kafka"]["HostConfig"][field] = {"AutoRemove": False, "NetworkMode": "host", "ReadonlyRootfs": False, "PortBindings": {"9092": [{}]}, "Tmpfs": {}}[field]
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.preflight(rows)

    def test_helpers_and_orphan_roles_refused(self):
        for role in ("helper", "tsvalidator", "missing-broker"):
            rows = owned_rows()
            if role == "missing-broker": rows.pop("kafka")
            else: rows[role] = {"Id": "helper-id"}
            with self.subTest(role=role), self.assertRaises(ValueError):
                self.preflight(rows)

    @patch.object(dr.os, "access", return_value=True)
    def test_candidate_override_mount_must_match_and_be_readonly(self, _access):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "core"
            binary.write_bytes(b"test-only")
            binary.chmod(0o700)
            selected = dr.core_override(binary)
            rows = owned_rows()
            mount = {"Type": "bind", "Destination": "/usr/local/bin/qdl-realtime-core", "Source": str(binary), "RW": False}
            rows["core"]["Mounts"].append(mount)
            with patch.object(dr, "owned", side_effect=lambda ns, role, call: rows.get(role)):
                self.assertEqual(case._reset_preflight(WORK, NS, Mock(return_value=""), selected)["Id"], "kafka-id")
                mount["RW"] = True
                with self.assertRaisesRegex(ValueError, "read-only"):
                    case._reset_preflight(WORK, NS, Mock(return_value=""), selected)

    def test_reset_delegates_to_broker_preserving_controller(self):
        result = {"broker_id": "same-broker", "topic_id": "new-topic", "acceptance": False}
        with patch("scripts.p184_dr05_reset.reset_preserving_broker", return_value=result) as reset:
            self.assertIs(dr.reset_case(WORK, "BINANCE", 7), result)
            reset.assert_called_once_with(WORK, "BINANCE", 7)

    def test_reset_delegation_preserves_controller_error(self):
        with patch("scripts.p184_dr05_reset.reset_preserving_broker", side_effect=ValueError("foreign")), patch.object(dr, "cleanup") as cleanup, patch.object(dr, "up") as up:
            with self.assertRaisesRegex(ValueError, "foreign"):
                case.reset_case(WORK, "OKX", 30)
            cleanup.assert_not_called()
            up.assert_not_called()

    def test_reset_delegation_preserves_override_receipt(self):
        selected = {"path": "/tmp/candidate", "sha256": "a" * 64}
        result = {"broker_id": "same-broker", "core_binary": selected}
        with patch("scripts.p184_dr05_reset.reset_preserving_broker", return_value=result) as reset:
            self.assertEqual(case.reset_case(WORK, "OKX")["core_binary"], selected)
            reset.assert_called_once_with(WORK, "OKX", 30)


class ObserveTests(unittest.TestCase):
    def setUp(self):
        from qdl.provider.v1.raw_provider_pb2 import RawProviderEnvelope
        self.wire = RawProviderEnvelope(capture_id=b"x" * 16, test_provenance=True).SerializeToString()
        self.receipt = {"topic": NS + ".raw", "partition": 0, "offset": 3, "capture_id": (b"x" * 16).hex(),
                        "raw_sha256": hashlib.sha256(self.wire).hexdigest(), "provenance": "TEST_ONLY"}
        self.packet = patch.object(case, "_packet", return_value=(WORK, NS, runtime_packet(), config()))
        self.env = patch.dict(os.environ, {"P184_DR05_ISOLATED_NAMESPACE": NS})
        self.packet.start(); self.env.start()
        self.addCleanup(self.packet.stop); self.addCleanup(self.env.stop)

    def test_rejects_receipt_before_io(self):
        for key, value in (("topic", "shared"), ("partition", 1), ("partition", False), ("offset", -1), ("offset", True), ("capture_id", "bad"), ("raw_sha256", "bad"), ("provenance", "LIVE")):
            with self.subTest(key=key), patch("confluent_kafka.Consumer") as consumer:
                with self.assertRaises(ValueError):
                    dr.canonical_observe(WORK, {**self.receipt, key: value})
                consumer.assert_not_called()

    def test_attestation_required(self):
        with patch.dict(os.environ, {"P184_DR05_ISOLATED_NAMESPACE": "shared"}), self.assertRaises(ValueError):
            dr.canonical_observe(WORK, self.receipt)

    def observe(self, records=(), mismatch=False, timeout=False, barrier=True, invalid=False):
        message = Mock()
        message.error.return_value = None
        message.topic.return_value = self.receipt["topic"]
        message.partition.return_value = 0
        message.offset.return_value = 4 if mismatch else 3
        message.value.return_value = self.wire
        reader = Mock()
        reader.poll.return_value = None if timeout else message
        observer = Mock()
        observer.topics = [NS + ".canonical", NS + ".quarantine"]
        observer.observe.return_value = {"raw_committed_next": 4 if barrier else 3,
            "read_committed_eof": observer.topics, "matched": list(records)}
        with patch("confluent_kafka.Consumer", return_value=reader) as factory, patch("scripts.p184_dr05_observer.CanonicalObserver", return_value=observer):
            try:
                return dr.canonical_observe(WORK, self.receipt, 2)
            finally:
                self.assertEqual(factory.call_args.args[0]["fetch.wait.max.ms"], 10)
                reader.close.assert_called_once()
                if not mismatch and not timeout and not invalid:
                    observer.close.assert_called_once()

    def test_canonical_and_quarantine_preserve_coordinates_reason(self):
        row = {"kind": "quarantine", "topic": NS + ".quarantine", "partition": 0, "offset": 9, "reason": "SEQUENCE_GAP", "capture_id": self.receipt["capture_id"]}
        result = self.observe([row])
        self.assertTrue(result["observed"])
        self.assertEqual(result["records"], [{**row, "partition": 0}])
        self.assertFalse(result["acceptance"])

    def test_filtered_requires_commit_barrier(self):
        self.assertEqual(self.observe()["outcome"], "FILTERED_AFTER_COMMIT")
        with self.assertRaises(RuntimeError):
            self.observe(barrier=False)

    def test_stale_receipt_refused(self):
        with self.assertRaises(ValueError):
            self.observe(mismatch=True)

    def test_wrong_hash_or_capture_refused(self):
        for key, value in (("raw_sha256", "0" * 64), ("capture_id", "0" * 32)):
            original = self.receipt[key]
            self.receipt[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.observe(invalid=True)
            self.receipt[key] = original

    def test_raw_timeout_refused_and_closed(self):
        with self.assertRaises(TimeoutError):
            self.observe(timeout=True)


if __name__ == "__main__":
    unittest.main()
