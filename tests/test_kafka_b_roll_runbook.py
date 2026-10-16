"""Decision B runbook: plan is pure; roll needs the own lock, keeps the quorum leader last (re-read at run time),
waits for the old controller's automatic election, collects core/load/writer evidence, and maps the roll tool's exit
codes (1 = RESTORED_VERIFIED stop with per-broker undo); elect gates coordinator moves on measured loads."""

from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import kafka_b_roll_runbook as rb

T0 = dt.datetime(2026, 10, 16, 12, 40, tzinfo=dt.timezone.utc)


def progress(t: str, wait: float, commit: float) -> str:
    body = {"event": "qdl_realtime_core_progress", "generation": 7, "batches": 100,
            "commit_ms": {"n": 5, "min": 1.0, "mean": 2.0, "max": commit},
            "commit_wait_ms": {"n": 5, "min": 0.1, "mean": 0.5, "max": wait}}
    return f"{t} {json.dumps(body)}"


GROUP = ("[2026-10-16 12:41:10,123] INFO [GroupCoordinator id=2] Finished loading of metadata from "
         "__consumer_offsets-43 with epoch 9 in 812ms where 1ms was spent in the scheduler. Loaded 120000 records "
         "which total to 9000000 bytes. (org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime)")
TXN = ("[2026-10-16 12:41:09,001] INFO [Transaction State Manager 2]: Finished loading 3 transaction metadata from "
       "__transaction_state-4 in 15 milliseconds, of which 0 milliseconds was spent in the scheduler. "
       "(org.apache.kafka.coordinator.transaction.TransactionStateManager)")


class FakeProc:
    def __init__(self):
        self.signals = []

    def poll(self):
        return None

    def send_signal(self, sig):
        self.signals.append(sig)

    def wait(self, timeout=None):
        return 0

    def kill(self):
        pass


class FakeHost(rb.Host):
    def __init__(self, env=None, leader=3, roll_rc=0, roll_report=None, balanced_after=1):
        self.env = env or {b: None for b in rb.BROKERS}
        self.leader, self.roll_rc, self.balanced_after = leader, roll_rc, balanced_after
        self.roll_report = roll_report or {"ok": True, "healthy_after_s": 40, "isr_restored_after_s": 30,
                                           "broker_config": "false"}
        self.calls, self.clock, self.observes, self.procs = [], T0, 0, []

    def run(self, argv, timeout=3600):
        self.calls.append(list(argv))
        if "--check" in argv:
            return 0, json.dumps({"ok": True, "quorum_leader": self.leader, "env": self.env, "drift": {},
                                  "recommended_order": ["kafka1", "kafka2", "kafka3"]}), ""
        if "--roll" in argv:
            self.clock += dt.timedelta(seconds=90)
            return self.roll_rc, json.dumps(self.roll_report), ""
        if "--observe" in argv and any(a.endswith("kafka_preferred_leader_election.py") for a in argv):
            self.observes += 1
            n = 0 if self.observes >= self.balanced_after else 5
            return 0, json.dumps({"mode": "observed", "summary": {"coordinator": n, "data": n, "blocked": 0}}), ""
        if "--apply" in argv and any(a.endswith("kafka_preferred_leader_election.py") for a in argv):
            return 0, json.dumps({"mode": "elected", "before": {"data": 4}, "after": {"data": 0},
                                  "coordinator_moves": []}), ""
        if "--parity-snapshot" in argv:
            Path(argv[-1]).write_text(json.dumps({"kafka": {"auto.leader.rebalance.enable": False}}))
            return 0, "", ""
        if "--observe" in argv:
            return 0, json.dumps({"temp_removal": {"remove_recommended": False}, "topics": {}}), ""
        raise AssertionError(argv)

    def inspect(self, name):
        mounts = [{"Source": "/w/logs", "Destination": "/app/logs"}] if name == rb.WRITER else []
        return {"RestartCount": 0, "State": {"StartedAt": "x", "Running": True}, "Mounts": mounts}

    def logs(self, name, since, until):
        if "rust_core" in name:
            return "\n".join([progress("2026-10-16T12:39:00.000000000Z", 3.0, 20.0),
                              progress("2026-10-16T12:41:11.000000000Z", 1890.0, 1900.0)])
        if name.endswith("kafka2-1"):
            return TXN + "\n" + GROUP + "\n"
        return ""

    def popen(self, argv, log):
        self.calls.append(list(argv))
        proc = FakeProc()
        self.procs.append(proc)
        return proc

    def now(self):
        return self.clock

    def sleep(self, seconds):
        self.clock += dt.timedelta(seconds=seconds)


class Runbook(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.lock = Path(self.tmp.name) / "lock"
        patcher = mock.patch.object(rb, "LOCK", self.lock)
        patcher.start()
        self.addCleanup(patcher.stop)
        # require_own_lock binds LOCK as a default argument at definition time
        patcher = mock.patch.object(rb.require_own_lock, "__defaults__", (self.lock,))
        patcher.start()
        self.addCleanup(patcher.stop)

    def own_lock(self, owner=rb.OWNER):
        until = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.lock.write_text(json.dumps({"owner": owner, "until_utc": until}))

    def main(self, host, argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = rb.main(["--receipt", str(Path(self.tmp.name) / "r"), *argv], host=host)
        return code, out.getvalue(), err.getvalue()

    def test_plan_is_pure_and_names_the_own_lock(self):
        host = FakeHost()
        code, out, _ = self.main(host, ["plan"])
        self.assertEqual(code, 0)
        self.assertEqual(host.calls, [])
        self.assertIn(f"acquire --owner {rb.OWNER}", out)
        self.assertIn("--rollback", out)
        self.assertIn("elect --scope data", out)

    def test_roll_refuses_without_the_own_lock(self):
        for owner in (None, "claude-burst-b16-a1parity"):
            if owner:
                self.own_lock(owner)
            host = FakeHost()
            code, _, err = self.main(host, ["roll", "--broker", "kafka1"])
            self.assertEqual(code, 4, err)
            self.assertFalse(any("--roll" in c for c in host.calls))

    def test_quorum_leader_is_rolled_last_rechecked_at_run_time(self):
        self.own_lock()
        host = FakeHost(env={"kafka1": "false", "kafka2": None, "kafka3": None}, leader=2)
        code, _, err = self.main(host, ["roll", "--broker", "kafka2"])
        self.assertEqual(code, 4)
        self.assertIn("quorum leader", err)
        self.assertFalse(any("--roll" in c for c in host.calls))
        host = FakeHost(env={"kafka1": "false", "kafka2": None, "kafka3": None}, leader=2)
        code, out, err = self.main(host, ["roll", "--broker", "kafka3"])
        self.assertEqual(code, 0, err)
        code, _, err = self.main(FakeHost(env={"kafka1": "false"}), ["roll", "--broker", "kafka1"])
        self.assertEqual(code, 4)
        self.assertIn("already", err)

    def test_roll_waits_for_the_auto_election_and_collects_evidence(self):
        self.own_lock()
        host = FakeHost(balanced_after=3)
        code, out, err = self.main(host, ["roll", "--broker", "kafka1"])
        self.assertEqual(code, 0, err)
        body = json.loads(out)
        roll = next(c for c in host.calls if "--roll" in c)
        self.assertEqual(roll[roll.index("--lock-owner") + 1], rb.OWNER)
        self.assertIn("--apply", roll)
        self.assertTrue(body["auto_rebalance_wait"]["balanced"])
        self.assertEqual(host.observes, 3)
        core = body["core"][rb.CORES[0]]
        self.assertEqual((core["baseline_commit_wait_max_ms"], core["window_commit_wait_max_ms"]), (3.0, 1890.0))
        self.assertEqual(core["window_stall_lines"], 1)
        self.assertEqual(body["watch_partition_loads"], [{"broker": "kafka2", "ts": "2026-10-16 12:41:10,123",
                                                          "ms": 812, "records": 120000, "bytes": 9000000}])
        self.assertEqual(body["max_load_ms"], 812)
        self.assertEqual(body["restarted"], [])
        sampler = next(c for c in host.calls if any(a.endswith("p184_g9_g10_writer_counters.py") for a in c))
        self.assertEqual(sampler[sampler.index("--log-dir") + 1], "/w/logs")
        self.assertTrue(host.procs[0].signals)  # sampler stopped (SIGTERM -> it writes its summary)
        receipt = Path(body["receipt"])
        for name in ("roll.json", "summary.json", "core-progress.jsonl", "coordinator-loads.json",
                     "check-before.json", "auto-rebalance-wait.json"):
            self.assertTrue((receipt / name).exists(), name)

    def test_last_roll_skips_the_wait_controller_already_false(self):
        self.own_lock()
        host = FakeHost(env={"kafka1": "false", "kafka2": "false", "kafka3": None}, leader=3)
        code, out, _ = self.main(host, ["roll", "--broker", "kafka3"])
        self.assertEqual(code, 0)
        self.assertIsNone(json.loads(out)["auto_rebalance_wait"])
        self.assertEqual(host.observes, 0)

    def test_restored_verified_stops_with_the_undo_of_rolled_brokers(self):
        self.own_lock()
        host = FakeHost(env={"kafka1": "false", "kafka2": None, "kafka3": None}, leader=3, roll_rc=1,
                        roll_report={"ok": False, "auto_rollback": "RESTORED_VERIFIED"})
        code, out, _ = self.main(host, ["roll", "--broker", "kafka2"])
        self.assertEqual(code, 1)
        body = json.loads(out)
        self.assertEqual(body["auto_rollback"], "RESTORED_VERIFIED")
        self.assertIn("STOP", body["next"])
        self.assertIn("--rollback", body["next"])
        self.assertIn("--broker kafka1", body["next"])
        self.assertNotIn("--broker kafka3", body["next"])
        self.assertEqual(host.observes, 0)  # no settle wait after a failed roll

    def test_hard_stop(self):
        self.own_lock()
        host = FakeHost(roll_rc=3, roll_report={"ok": False, "auto_rollback": "ROLLBACK_FAILED"})
        code, out, _ = self.main(host, ["roll", "--broker", "kafka1"])
        self.assertEqual(code, 3)
        self.assertIn("HARD STOP", json.loads(out)["next"])

    def test_elect_coordinators_needs_measured_loads_within_budget(self):
        self.own_lock()
        code, _, err = self.main(FakeHost(), ["elect", "--scope", "coordinators"])
        self.assertEqual(code, 4)
        self.assertIn("--scope data", err)
        code, out, err = self.main(FakeHost(), ["elect", "--scope", "data"])
        self.assertEqual(code, 0, err)
        host = FakeHost()
        self.main(host, ["roll", "--broker", "kafka1"])  # measures an 812 ms load of partition 43
        code, _, err = self.main(FakeHost(), ["elect", "--scope", "coordinators", "--coordinator-load-budget-ms",
                                              "500"])
        self.assertEqual(code, 4, err)
        host = FakeHost()
        code, _, err = self.main(host, ["elect", "--scope", "coordinators"])
        self.assertEqual(code, 0, err)
        elect = next(c for c in host.calls if "--apply" in c)
        self.assertEqual(elect[elect.index("--scope") + 1], "coordinators")
        self.assertIn("--window-out", elect)

    def test_final_check_needs_false_everywhere(self):
        code, out, _ = self.main(FakeHost(env={b: "false" for b in rb.BROKERS}), ["final-check"])
        self.assertEqual(code, 0)
        self.assertIs(json.loads(out)["parity_rebalance"], False)
        code, _, _ = self.main(FakeHost(env={"kafka1": "false", "kafka2": "false", "kafka3": None}), ["final-check"])
        self.assertEqual(code, 3)


if __name__ == "__main__":
    unittest.main()
