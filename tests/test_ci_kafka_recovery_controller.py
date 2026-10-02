"""Exercise CI fault-controller orchestration without a Docker socket or broker."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import yaml


class KafkaRecoveryControllerTests(unittest.TestCase):
    def run_controller(self, mode):
        workflow = yaml.safe_load((Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml").read_text())
        script = next(step["run"] for step in workflow["jobs"]["kn-native-integration"]["steps"]
                      if step["name"].startswith("Committed reads,"))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            log = root / "calls.jsonl"
            docker = root / "docker"
            docker.write_text("#!" + sys.executable + "\n" + r'''import json, os, pathlib, sys, time
args = sys.argv[1:]
with open(os.environ["FAKE_DOCKER_LOG"], "a") as f:
    f.write(json.dumps(args) + "\n")
mode = os.environ["FAKE_DOCKER_MODE"]
if args[0] == "inspect":
    print("c" * 64 if args[2] == "{{.Id}}" else
          ("wrong-namespace" if mode == "wrong-namespace" else "kn-native-integration"))
elif args[0] == "run":
    if mode == "runner-failure":
        sys.exit(7)
    assert "QDL_RECOVERY_FAULT_DIR=/fault" in args
    fault = pathlib.Path(next(a[:-7] for a in args if a.endswith(":/fault")))
    (fault / "ready").write_text("TEST_ONLY")
    deadline = time.monotonic() + 18
    while not (fault / "restored").exists():
        assert time.monotonic() < deadline, "controller did not restore"
        time.sleep(.02)
    assert (fault / "paused").exists()
    (fault / "receipt.json").write_text('{"scope":"TEST_ONLY_FAKE_DOCKER"}')
elif args[0] in ("pause", "unpause"):
    assert args[1] == "c" * 64, "only the captured test container ID is allowed"
else:
    raise AssertionError(args)
''')
            docker.chmod(0o700)
            result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", script],
                                    env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                                         "TMPDIR": str(root), "FAKE_DOCKER_LOG": str(log),
                                         "FAKE_DOCKER_MODE": mode},
                                    capture_output=True, text=True, timeout=25)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            return result, calls

    def test_runs_required_fault_and_restores_exact_broker(self):
        result, calls = self.run_controller("success")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(sum(call[0] == "pause" for call in calls), 1)
        self.assertGreaterEqual(sum(call[0] == "unpause" for call in calls), 1)
        self.assertIn("TEST_ONLY_FAKE_DOCKER", result.stdout)

    def test_runner_failure_is_not_hidden_and_controller_is_cleaned(self):
        result, calls = self.run_controller("runner-failure")
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertFalse(any(call[0] == "pause" for call in calls))
        self.assertTrue(any(call[0] == "unpause" for call in calls))

    def test_wrong_namespace_is_rejected_before_any_pause_or_runner(self):
        result, calls = self.run_controller("wrong-namespace")
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(all(call[0] == "inspect" for call in calls))


if __name__ == "__main__":
    unittest.main()
