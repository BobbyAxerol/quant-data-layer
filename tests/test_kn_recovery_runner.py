"""No Docker mutation: execution-boundary regression with explicit fake CLI."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock

from scripts.kn_recovery_runner import bounded_probe, read_packet, start_exact


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.expected = {"id": "a" * 64, "image": "sha256:" + "b" * 64}
        self.output = "|".join([self.expected["id"], self.expected["image"], "/reader", "exited", "false"])

    def test_default_never_inspects_or_starts(self):
        run = Mock()
        self.assertFalse(start_exact("reader", self.expected, set(), 2, run=run)["executed"])
        run.assert_not_called()

    def test_exact_start_uses_remaining_budget(self):
        run = Mock(side_effect=[subprocess.CompletedProcess([], 0, self.output), subprocess.CompletedProcess([], 0, "")])
        clock = Mock(side_effect=[1, 2.5])
        result = start_exact("reader", self.expected, {self.expected["id"]}, 2, run=run, clock=clock)
        self.assertTrue(result["accepted"])
        self.assertEqual(run.call_args.kwargs["timeout"], .5)
        self.assertEqual(run.call_args.args[0], ["docker", "start", self.expected["id"]])

    def test_expired_budget_never_starts(self):
        run = Mock(return_value=subprocess.CompletedProcess([], 0, self.output))
        with self.assertRaisesRegex(ValueError, "DEADLINE"):
            start_exact("reader", self.expected, {self.expected["id"]}, 2, run=run, clock=Mock(side_effect=[1, 3]))
        self.assertEqual(run.call_count, 1)

    def test_drift_running_and_oom_never_start(self):
        for output in (self.output.replace("/reader", "/other"), self.output.replace("exited", "running"), self.output.replace("false", "true"), self.output.replace("a" * 64, "c" * 64)):
            with self.subTest(output=output):
                run = Mock(return_value=subprocess.CompletedProcess([], 0, output))
                with self.assertRaises(ValueError):
                    start_exact("reader", self.expected, {self.expected["id"]}, 2, run=run)
                self.assertEqual(run.call_count, 1)

    def test_inspect_failure(self):
        run = Mock(return_value=subprocess.CompletedProcess([], 1, ""))
        with self.assertRaisesRegex(ValueError, "INSPECT"):
            start_exact("reader", self.expected, {self.expected["id"]}, 2, run=run)
        self.assertEqual(run.call_count, 1)

    def test_probe_real_subprocess(self):
        self.assertEqual(bounded_probe([sys.executable, "-c", 'print("{}")'], 2), {})

    def test_probe_failure_or_oversize(self):
        for code in ('raise SystemExit(1)', 'print("x" * 300000)'):
            with self.assertRaises(ValueError):
                bounded_probe([sys.executable, "-c", code], 2)

    def test_probe_timeout(self):
        with self.assertRaises(subprocess.TimeoutExpired):
            bounded_probe([sys.executable, "-c", "import time; time.sleep(3)"], .05)

    def test_packet_requires_absolute_executable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "packet.json"
            path.write_text(json.dumps({"schema": "qdl.kn.recovery-packet.v1", "probe_command": ["sh"]}))
            with self.assertRaises(ValueError):
                read_packet(path)


if __name__ == "__main__":
    unittest.main()
