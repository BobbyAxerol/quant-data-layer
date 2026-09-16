from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scripts import v2_stable_boot_recovery as boot
from scripts.rebuild_v2_stable_projection_cache import CACHE_FILES, CONFIRM_TOKEN, STOP_SERVICES


class FakeDocker:
    """Scripted docker: container states, redis keys, spool size, kafka lag; records every command."""

    def __init__(self, *, states=None, keys=0, spool=0, lag_samples=None, projector_ready=True):
        self.states = dict(states or {})
        self.keys = keys
        self.spool = spool
        self.lag_samples = list(lag_samples or [])
        self.projector_ready = projector_ready
        self.commands: list[list[str]] = []

    def _cp(self, out="", rc=0):
        return subprocess.CompletedProcess([], rc, out, "")

    def __call__(self, command):
        self.commands.append(command)
        c = command
        if c[:2] == ["docker", "inspect"]:
            name = c[-1].split(boot.PROJECT_NAME + "-", 1)[-1].rsplit("-1", 1)[0]
            if "{{.Config.Image}}" == c[3]:
                return self._cp("apache/kafka@sha256:abc\n")
            if "Mounts" in c[3]:
                return self._cp("/host/certs\n")
            st = self.states.get(name)
            if st is None:
                return self._cp("", rc=1)
            return self._cp(f"{st['status']}|{st.get('health', '-')}|0|img\n")
        if c[:2] == ["docker", "exec"] and "DBSIZE" in c:
            return self._cp(f"{self.keys}\n")
        if c[:2] == ["docker", "exec"] and "FLUSHDB" in c:
            self.keys = 0
            return self._cp("OK\n")
        if c[:2] == ["docker", "exec"] and "python" in c:
            return self._cp("", rc=0 if self.projector_ready else 1)
        if c[:2] == ["docker", "run"] and "stat" in c[-1]:
            return self._cp(f"{self.spool}\n")
        if c[:2] == ["docker", "run"] and c[-1].startswith("rm -f"):
            self.spool = 0
            return self._cp("")
        if c[:2] == ["docker", "run"] and any("kafka-consumer-groups" in x for x in c):
            if "--reset-offsets" in c:
                return self._cp("")
            total, parts, mx = self.lag_samples.pop(0) if self.lag_samples else (0, 6, 0)
            lines = []
            per = [mx] + [max(0, (total - mx) // max(1, parts - 1))] * (parts - 1)
            for i in range(parts):
                lines.append(f"g {boot.CANONICAL_TOPIC} {i} 10 20 {per[i]}")
            return self._cp("\n".join(lines) + "\n")
        if c[:2] in (["docker", "start"], ["docker", "stop"], ["docker", "restart"]):
            for name in c[2:]:
                if name.startswith(boot.PROJECT_NAME):
                    svc = name.split(boot.PROJECT_NAME + "-", 1)[1].rsplit("-1", 1)[0]
                    if c[1] == "stop":
                        self.states[svc] = {"status": "exited", "health": "-"}
                    else:
                        self.states[svc] = {"status": "running", "health": "healthy"}
                        if svc == "stable_redis" and c[1] == "restart":
                            self.keys = 0
            if c[1] == "start" and any("projector" in n for n in c[2:]):
                self.keys = 600
            return self._cp("")
        raise AssertionError(f"unexpected command {c[:5]}")


def healthy_states():
    st = {s: {"status": "running", "health": "healthy"} for s in boot.INFRA_SERVICES}
    st.update({s: {"status": "running", "health": "healthy"} for s in STOP_SERVICES})
    return st


def post_boot_states():
    st = {s: {"status": "running", "health": "healthy"} for s in boot.INFRA_SERVICES}
    st.update({s: {"status": "exited", "health": "-"} for s in STOP_SERVICES})
    return st


class AssessTests(unittest.TestCase):
    def test_wait_infra_when_kafka_or_redis_not_healthy(self):
        st = post_boot_states(); st["kafka2"] = {"status": "running", "health": "starting"}
        a = boot.assess(FakeDocker(states=st, keys=0, spool=10))
        self.assertEqual(a.decision, "WAIT_INFRA"); self.assertIn("kafka2", a.reasons[0])
        self.assertIsNone(a.redis_keys)

    def test_healthy_when_redis_populated_and_all_users_running(self):
        a = boot.assess(FakeDocker(states=healthy_states(), keys=533, spool=10))
        self.assertEqual(a.decision, "HEALTHY")

    def test_recover_when_redis_empty_next_to_spool(self):
        a = boot.assess(FakeDocker(states=post_boot_states(), keys=0, spool=1_000_000))
        self.assertEqual(a.decision, "RECOVER")
        crash_loop = post_boot_states(); crash_loop["projector_v2"] = {"status": "restarting", "health": "-"}
        self.assertEqual(boot.assess(FakeDocker(states=crash_loop, keys=0, spool=5)).decision, "RECOVER")

    def test_start_only_when_both_empty_and_manual_otherwise(self):
        self.assertEqual(boot.assess(FakeDocker(states=post_boot_states(), keys=0, spool=0)).decision, "START_ONLY")
        partial = boot.assess(FakeDocker(states=post_boot_states(), keys=12, spool=10))
        self.assertEqual(partial.decision, "MANUAL")
        st = post_boot_states(); del st["query_v2_2"]
        absent = boot.assess(FakeDocker(states=st, keys=0, spool=10))
        self.assertEqual(absent.decision, "MANUAL"); self.assertIn("query_v2_2", absent.reasons[0])


class RecoverTests(unittest.TestCase):
    def _recovery(self, fake, **kw):
        receipt = {}
        clock = {"t": 0.0}
        def sleep(s): clock["t"] += s
        return boot.Recovery(fake, receipt=receipt, sleep=sleep, clock=lambda: clock["t"],
                             now=lambda: datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc), **kw), receipt

    def test_recover_runs_the_governed_sequence_in_order(self):
        fake = FakeDocker(states=post_boot_states(), keys=0, spool=1_000_000,
                          lag_samples=[(5000, 6, 3000), (400, 6, 200), (300, 6, 100), (100, 6, 40)])
        rec, receipt = self._recovery(fake)
        result = rec.recover(timeout_seconds=600)
        self.assertEqual(result["status"], "PASS"); self.assertEqual(result["redis_keys"], 600)
        steps = [s["step"] for s in receipt["steps"]]
        self.assertEqual(steps[:4], ["cache_users_stopped", "cache_files_deleted", "redis_flushed", "group_reset"])
        self.assertIn("lag_bounded", steps); self.assertIn("projectors_ready", steps); self.assertEqual(steps[-1], "done")
        kinds = [(c[1], c[2] if len(c) > 2 else "") for c in fake.commands if c[1] in ("start", "stop")]
        starts = [c for c in fake.commands if c[1] == "start"]
        self.assertTrue(starts[0][2].endswith("stream_v2_active-1"))
        self.assertTrue(any("projector_v2-1" in c[2] for c in starts[1:2]))
        self.assertTrue(starts[-1][2].endswith("query_v2_1-1"))
        rm = [c for c in fake.commands if c[1] == "run" and c[-1].startswith("rm -f")]
        self.assertEqual(len(rm), 1); self.assertTrue(all(f in rm[0][-1] for f in CACHE_FILES))
        reset = [c for c in fake.commands if "--reset-offsets" in c][0]
        self.assertIn("2026-09-16T11:45:00.000", reset)  # now - 900 s
        self.assertEqual(receipt["steps"][3]["replay_records"], 5000)
        self.assertTrue(kinds)

    def test_recover_fails_closed_before_start_when_partitions_or_budget_wrong(self):
        fake = FakeDocker(states=post_boot_states(), keys=0, spool=10, lag_samples=[(10, 5, 5)])
        rec, _ = self._recovery(fake)
        with self.assertRaises(RuntimeError):
            rec.recover(timeout_seconds=60)
        self.assertFalse(any(c[1] == "start" for c in fake.commands))
        fake = FakeDocker(states=post_boot_states(), keys=0, spool=10, lag_samples=[(boot.MAX_REPLAY_BOOTSTRAP_RECORDS + 60, 6, 10)])
        with self.assertRaises(RuntimeError):
            self._recovery(fake)[0].recover(timeout_seconds=60)

    def test_recover_times_out_when_lag_never_bounds_and_leaves_queries_down(self):
        fake = FakeDocker(states=post_boot_states(), keys=0, spool=10, lag_samples=[(9000, 6, 4000)] * 50)
        rec, receipt = self._recovery(fake)
        with self.assertRaises(TimeoutError):
            rec.recover(timeout_seconds=30)
        self.assertFalse(any(c[1] == "start" and "query" in c[2] for c in fake.commands))

    def test_simulate_crash_reproduces_post_boot_state(self):
        fake = FakeDocker(states=healthy_states(), keys=533, spool=10)
        rec, receipt = self._recovery(fake)
        rec.simulate_crash()
        self.assertEqual(fake.keys, 0)
        self.assertTrue(all(fake.states[s]["status"] == "exited" for s in STOP_SERVICES))
        self.assertEqual(boot.assess(fake).decision, "RECOVER")


class CliTests(unittest.TestCase):
    def test_apply_and_simulate_require_the_exact_token(self):
        with self.assertRaises(SystemExit):
            boot.main(["--apply"], run=FakeDocker())
        with self.assertRaises(SystemExit):
            boot.main(["--simulate-crash", "--confirm", "nope"], run=FakeDocker())

    def test_check_is_read_only_and_exit_code_names_the_decision(self):
        fake = FakeDocker(states=post_boot_states(), keys=0, spool=10)
        with tempfile.TemporaryDirectory() as d:
            code = boot.main(["--receipt-dir", d], run=fake)
            self.assertEqual(code, 4)
            self.assertFalse(any(c[1] in ("start", "stop", "exec") and "FLUSHDB" in c for c in fake.commands))
            receipt = json.loads(next(Path(d).iterdir()).read_text())
            self.assertEqual(receipt["assessment"]["decision"], "RECOVER"); self.assertFalse(receipt["apply"])

    def test_simulate_crash_refuses_unless_healthy(self):
        with tempfile.TemporaryDirectory() as d:
            code = boot.main(["--simulate-crash", "--confirm", CONFIRM_TOKEN, "--receipt-dir", d], run=FakeDocker(states=post_boot_states(), keys=0, spool=10))
            self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
