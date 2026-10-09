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
