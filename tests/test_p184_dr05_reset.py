import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts import p184_dr05_reset as reset
from tests.test_p184_dr05_case import NS, runtime_packet, config


class ResetControllerTests(unittest.TestCase):
    def run_reset(self, *, preflight_error=None, missing=False, changed=False):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            (work / "handoff.json").write_text("{}")
            stopped = False
            started = set()
            calls = []

            def owned(ns, role, call):
                self.assertEqual(ns, NS)
                if role == "kafka":
                    return {"Id": "changed" if changed else "broker"}
                if role == "helper":
                    return {"Id": "channel"}
                if missing and role == "query":
                    return None
                return {"Id": role} if not stopped or role in started else None

            def bounded(argv, timeout):
                nonlocal stopped
                self.assertGreater(timeout, 0)
                calls.append(argv)
                if argv[:2] == ["docker", "stop"]:
                    stopped = True
                    return ""
                if argv[0] == "start":
                    started.add(argv[1])
                    return "id"
                if "scripts/p184_dr05_reset.py" in argv:
                    return json.dumps({"topic_id": "new", "previous_topic_id": "old"})
                return "{}"

            preflight = Mock(side_effect=preflight_error, return_value={"Id": "broker"})
            with patch.object(reset, "_packet", return_value=(work, NS, runtime_packet(), config())), \
                 patch.object(reset, "_reset_preflight", preflight), \
                 patch.object(reset.dr, "owned", side_effect=owned), \
                 patch.object(reset.dr, "bounded", side_effect=bounded), \
                 patch.object(reset.dr, "topology", return_value={r: ["start", r] for r in ("quota", "core", "stream", "query")}):
                try:
                    return reset.reset_preserving_broker(work, "OKX", 10), calls
                finally:
                    self.calls = calls

    def test_only_owned_readers_restarted_broker_untouched(self):
        result, calls = self.run_reset()
        self.assertEqual(result["generation"], 7)
        self.assertEqual(result["broker_id"], "broker")
        self.assertFalse(result["acceptance"])
        stop = next(c for c in calls if c[:2] == ["docker", "stop"])
        self.assertEqual(stop[4:], ["query", "stream", "core", "quota"])
        self.assertNotIn("broker", stop)
        self.assertNotIn("channel", stop)

    def test_foreign_or_missing_stack_refused_before_mutation(self):
        for options in ({"preflight_error": ValueError("foreign")}, {"missing": True}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.run_reset(**options)
            self.assertFalse(self.calls)

    def test_broker_replacement_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "namespace changed"):
            self.run_reset(changed=True)

    def test_invalid_deadline_and_venue_before_docker(self):
        for venue, timeout in (("OKX", float("nan")), ("OKX", 0), ("OTHER", 1)):
            with patch.object(reset.dr, "bounded") as call, self.assertRaises(ValueError):
                reset.reset_preserving_broker(Path("/unused"), venue, timeout)
            call.assert_not_called()


class ResetStageDiagnosticTests(unittest.TestCase):
    """D6 (DR05 r13): a failure after the readers were stopped names its stage and keeps a bounded stderr tail."""

    def test_a_failed_topic_reset_reports_stage_rc_and_stderr(self):
        import subprocess
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            (work / "handoff.json").write_text("{}")
            stopped = []

            def owned(ns, role, call):
                if role == "kafka":
                    return {"Id": "broker"}
                if role == "helper":
                    return {"Id": "channel"}
                return None if stopped else {"Id": role}

            def bounded(argv, timeout):
                if argv[:2] == ["docker", "stop"]:
                    stopped.append(True)
                    return ""
                if "scripts/p184_dr05_reset.py" in argv:
                    raise subprocess.CalledProcessError(1, argv, output="", stderr="Traceback ...\nKafkaException: TOPIC_ALREADY_EXISTS\x07" + "x" * 500)
                return "{}"

            with patch.object(reset, "_packet", return_value=(work, NS, runtime_packet(), config())), \
                 patch.object(reset, "_reset_preflight", Mock(return_value={"Id": "broker"})), \
                 patch.object(reset.dr, "owned", side_effect=owned), \
                 patch.object(reset.dr, "bounded", side_effect=bounded):
                with self.assertRaises(reset.ResetStageError) as caught:
                    reset.reset_preserving_broker(work, "OKX", 10)
        message = str(caught.exception)
        self.assertEqual(caught.exception.stage, "reset_topics")
        self.assertIn("stage=reset_topics", message)
        self.assertIn("readers_stopped=true", message)
        self.assertIn("rc=1", message)
        self.assertLessEqual(len(message), 380)
        self.assertNotIn("\x07", message)


class ResetTopicsIdempotenceTests(unittest.TestCase):
    """G13 r2 (2026-10-16): UNKNOWN_TOPIC_OR_PART on delete means already deleted; the post-state is verified."""

    def fake_admin(self, *, delete_errors=None, describe_misses=0, partitions=1, group_error=None):
        from concurrent.futures import Future
        from confluent_kafka import KafkaError, KafkaException
        state = {"present": {NS + s for s in (".raw", ".canonical", ".quarantine")}, "misses": describe_misses}

        def done(value=None, error=None):
            future = Future()
            future.set_exception(error) if error else future.set_result(value)
            return future

        def kafka(code):
            return KafkaException(KafkaError(code))

        admin = Mock()

        def delete_topics(names, operation_timeout):
            out = {}
            for name in names:
                code = (delete_errors or {}).get(name)
                state["present"].discard(name)
                out[name] = done(error=kafka(code)) if code is not None else done()
            return out

        def describe_topics(collection, request_timeout):
            out = {}
            for name in collection.topic_names:
                if state["misses"]:
                    state["misses"] -= 1
                    out[name] = done(error=kafka(KafkaError.UNKNOWN_TOPIC_OR_PART))
                    continue
                part = Mock(replicas=[1])
                out[name] = done(Mock(partitions=[part] * partitions, topic_id="new-" + name))
            return out

        admin.delete_topics.side_effect = delete_topics
        admin.list_topics.side_effect = lambda timeout: Mock(topics={n: 1 for n in state["present"]})
        admin.delete_consumer_groups.side_effect = lambda groups, request_timeout: {
            g: done(error=kafka(group_error if group_error is not None else KafkaError.GROUP_ID_NOT_FOUND))
            for g in groups}
        admin.create_topics.side_effect = lambda topics, operation_timeout: {t.topic: done() for t in topics}
        admin.describe_topics.side_effect = describe_topics
        return admin

    def run_topics(self, admin):
        import os
        import sys
        import types
        fake_dr08 = types.SimpleNamespace(kafka_security=lambda packet: {})
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            with patch.object(reset, "_packet", return_value=(work, NS, runtime_packet(), config())), \
                 patch.dict(os.environ, {"P184_DR05_ISOLATED_NAMESPACE": NS}), \
                 patch.dict(sys.modules, {"scripts.p184_dr08_isolated": fake_dr08}), \
                 patch("confluent_kafka.admin.AdminClient", return_value=admin):
                result = reset.reset_topics(work, 5)
                packet = json.loads((work / "packet.json").read_text())
        return result, packet

    def test_absent_topic_at_delete_is_already_deleted_and_recreated(self):
        from confluent_kafka import KafkaError
        admin = self.fake_admin(delete_errors={NS + ".quarantine": KafkaError.UNKNOWN_TOPIC_OR_PART})
        result, packet = self.run_topics(admin)
        self.assertEqual(result["already_absent"], [NS + ".quarantine"])
        self.assertEqual(result["topic_id"], "new-" + NS + ".canonical")
        self.assertEqual(packet["slice"]["topic_id"], result["topic_id"])
        created = [t.topic for t in admin.create_topics.call_args.args[0]]
        self.assertEqual(sorted(created), sorted(NS + s for s in (".raw", ".canonical", ".quarantine")))

    def test_other_delete_errors_are_not_swallowed(self):
        from confluent_kafka import KafkaError, KafkaException
        admin = self.fake_admin(delete_errors={NS + ".raw": KafkaError.TOPIC_AUTHORIZATION_FAILED})
        with self.assertRaises(KafkaException):
            self.run_topics(admin)
        admin.create_topics.assert_not_called()

    def test_other_group_errors_are_not_swallowed(self):
        from confluent_kafka import KafkaError, KafkaException
        admin = self.fake_admin(group_error=KafkaError.GROUP_AUTHORIZATION_FAILED)
        with self.assertRaises(KafkaException):
            self.run_topics(admin)
        admin.create_topics.assert_not_called()

    def test_describe_propagation_lag_is_retried_then_verified(self):
        result, _ = self.run_topics(self.fake_admin(describe_misses=2))
        self.assertEqual(result["already_absent"], [])
        self.assertEqual(result["previous_topic_id"], "old-topic")

    def test_recreated_shape_mismatch_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "shape mismatch"):
            self.run_topics(self.fake_admin(partitions=2))
