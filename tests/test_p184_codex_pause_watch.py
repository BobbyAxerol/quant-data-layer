"""Exercise the Codex measurement-window pause watcher: tree rule, ordering, pid-reuse guard, real SIGSTOP/SIGCONT.

The real-process tests pause only a python tree spawned by the test itself (``--also-pid`` + ``--no-ide-match``), so
no real Codex process is ever signalled. Fixture-table CLI tests use ``--proc-root`` (dry run: nothing is signalled).
"""
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "p184_codex_pause_watch.py"
_spec = importlib.util.spec_from_file_location("p184_codex_pause_watch", SCRIPT)
watch = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(watch)

UID = 1000
CODEX = "/home/u/.vscode-server/extensions/openai.chatgpt-26.5/bin/linux-x86_64/codex"
HAS_PROC = Path("/proc/self/stat").exists()


def stat_line(pid, comm, state, ppid, starttime):
    # fields 3..22: state ppid pgrp session tty tpgid flags minflt cminflt majflt cmajflt utime stime cutime cstime
    # priority nice num_threads itrealvalue starttime
    rest = [state, ppid, ppid, 1, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 1, 0, starttime, 1000, 10]
    return f"{pid} ({comm}) " + " ".join(str(x) for x in rest) + "\n"


def P(ppid, argv0="x", uid=UID, state="S", starttime=100):
    return watch.Proc(ppid, uid, state, starttime, argv0)


def deadline_in(seconds):
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_log(path):
    try:
        return [json.loads(line) for line in Path(path).read_text().splitlines()]
    except OSError:
        return []


class StatParsingTests(unittest.TestCase):
    def test_comm_with_parentheses_and_spaces(self):
        line = stat_line(4242, "evil) (S 1 2 3) x", "S", 777, 123456)
        self.assertEqual(watch.parse_stat(line), ("S", 777, 123456))

    def test_read_process_table_from_fixture(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for pid, comm, state, ppid, start, cmd in [
                    (10, "a) (b c", "S", 1, 500, CODEX.encode() + b"\0app-server\0--secret=x\0"),
                    (11, "z", "Z", 10, 600, b"")]:
                (root / str(pid)).mkdir()
                (root / str(pid) / "stat").write_text(stat_line(pid, comm, state, ppid, start))
                (root / str(pid) / "cmdline").write_bytes(cmd)
            (root / "self").mkdir()
            (root / "99").mkdir()  # exited mid-scan: no stat
            table = watch.read_process_table(root)
            self.assertEqual(set(table), {10, 11})
            self.assertEqual(table[10], watch.Proc(1, os.getuid(), "S", 500, CODEX))
            self.assertEqual(table[11].state, "Z")
            self.assertEqual(watch.ide_codex_roots(table, os.getuid()), {10})
            self.assertEqual(watch.read_starttime(root, 10), 500)
            self.assertIsNone(watch.read_starttime(root, 12))

    def test_argv0_rule_unchanged(self):
        self.assertTrue(watch.is_ide_codex(CODEX))
        self.assertFalse(watch.is_ide_codex("/bin/bash"))
        self.assertFalse(watch.is_ide_codex("/home/u/.vscode-server/extensions/openai.chatgpt-26.5/bin/x/node"))
        self.assertFalse(watch.is_ide_codex("/usr/bin/codex"))


class TreeRuleTests(unittest.TestCase):
    def table(self):
        return {
            1: P(0, "init", uid=0),
            2: P(0, "kthreadd", uid=0),
            3: P(2, "kworker", uid=0),
            50: P(1, "sshd", uid=0),
            60: P(50, "vscode-server"),          # session leader of the VS Code server
            100: P(60, CODEX),                   # root
            101: P(100, "python3"),              # launch_sgp.py
            102: P(101, "ssh"),
            103: P(102, "ssh-helper"),
            104: P(100, "sudo", uid=0),          # foreign uid: excluded and not descended
            105: P(104, "child-of-sudo"),
            106: P(101, "defunct", state="Z"),   # zombie
            200: P(60, "claude"),                # same session, not under a root
            201: P(200, "bash"),
        }

    def test_multilevel_descendants_bfs_order(self):
        targets = watch.pause_targets(self.table(), {100}, set(), UID)
        self.assertEqual([t.pid for t in targets], [100, 101, 102, 103])
        self.assertEqual([t.depth for t in targets], [0, 1, 2, 3])
        self.assertEqual({t.root for t in targets}, {100})
        self.assertEqual([t.ppid for t in targets], [60, 100, 101, 102])

    def test_bfs_parent_before_child_wide_tree(self):
        table = {1: P(0, uid=0), 10: P(1), 11: P(10), 12: P(10), 13: P(11), 14: P(12), 15: P(13), 20: P(1), 21: P(20)}
        order = [t.pid for t in watch.pause_targets(table, {10, 20}, set(), UID)]
        self.assertEqual(order, [10, 20, 11, 12, 21, 13, 14, 15])
        position = {pid: i for i, pid in enumerate(order)}
        for pid in order:
            if table[pid].ppid in position:
                self.assertLess(position[table[pid].ppid], position[pid])

    def test_foreign_uid_zombie_and_session_neighbours_excluded(self):
        pids = {t.pid for t in watch.pause_targets(self.table(), {100}, set(), UID)}
        self.assertNotIn(104, pids)
        self.assertNotIn(105, pids)
        self.assertNotIn(106, pids)
        self.assertTrue(pids.isdisjoint({1, 2, 3, 50, 60, 200, 201}))

    def test_kernel_threads_and_init_never_targets(self):
        pids = {t.pid for t in watch.pause_targets(self.table(), {1, 2, 3}, set(), UID)}
        self.assertEqual(pids, set())

    def test_own_pid_and_ancestors_excluded_even_under_root(self):
        table = self.table()
        table[300] = P(101, "bash")          # watcher's parent shell, under the root
        table[301] = P(300, "watcher")
        table[302] = P(301, "watcher-child")
        exclude = watch.self_chain(table, 301)
        self.assertEqual(exclude, {301, 300, 101, 100, 60, 50, 1})
        self.assertEqual(watch.pause_targets(table, {100}, exclude, UID), [])
        exclude = watch.self_chain(table, 302)
        self.assertIn(301, exclude)
        self.assertNotIn(302, {t.pid for t in watch.pause_targets(table, {300, 302}, exclude, UID)})

    def test_self_chain_survives_cycles_and_gaps(self):
        self.assertEqual(watch.self_chain({5: P(6), 6: P(5)}, 5), {5, 6})
        self.assertEqual(watch.self_chain({5: P(9)}, 5), {5, 9})


class PassAndResumeTests(unittest.TestCase):
    def run_pass(self, scans, roots=frozenset({100}), own_pid=999, ledger=None):
        ledger = ledger or watch.Ledger()
        calls, notes = [], []
        sequence = iter(scans)
        current = {}

        def read_table():
            current.clear()
            current.update(next(sequence, scans[-1]))
            return dict(current)

        used = watch.pause_pass(read_table, lambda table: set(roots), own_pid, UID, ledger, calls.append,
                                lambda pid: current[pid].starttime if pid in current else None, notes.append)
        return ledger, calls, notes, used

    def test_child_forked_during_pass_stopped_in_same_poll(self):
        first = {1: P(0, uid=0), 100: P(1, CODEX), 101: P(100)}
        second = {**first, 102: P(101, "ssh", starttime=900)}
        ledger, calls, notes, used = self.run_pass([first, second, second])
        self.assertEqual(calls, [100, 101, 102])
        self.assertEqual(used, 3)
        stops = [n for n in notes if n["event"] == "sigstop"]
        self.assertEqual([(n["pid"], n["depth"], n["root"]) for n in stops], [(100, 0, 100), (101, 1, 100), (102, 2, 100)])
        self.assertTrue(all(set(n) == {"event", "pid", "ppid", "argv0", "root", "depth", "prior_state"} for n in stops))

    def test_already_paused_not_restopped_next_poll(self):
        table = {1: P(0, uid=0), 100: P(1, CODEX), 101: P(100)}
        ledger, calls, _, _ = self.run_pass([table])
        _, calls2, _, used = self.run_pass([table], ledger=ledger)
        self.assertEqual(calls, [100, 101])
        self.assertEqual(calls2, [])
        self.assertEqual(used, 1)

    def test_rescan_bounded(self):
        scans = [{1: P(0, uid=0), 100: P(1), **{200 + i: P(100, starttime=i) for i in range(n)}} for n in range(30)]
        _, calls, notes, used = self.run_pass(scans)
        self.assertEqual(used, watch.MAX_RESCANS)
        self.assertEqual(notes[-1], {"event": "rescan_limit", "scans": watch.MAX_RESCANS})

    def test_stop_failure_logged_and_not_retried_forever(self):
        ledger, notes = watch.Ledger(), []
        table = {1: P(0, uid=0), 100: P(1), 101: P(100)}

        def stop(pid):
            if pid == 101:
                raise ProcessLookupError(3, "No such process")

        used = watch.pause_pass(lambda: table, lambda t: {100}, 999, UID, ledger, stop,
                                lambda pid: table[pid].starttime, notes.append)
        self.assertEqual(used, 2)
        self.assertEqual([n["event"] for n in notes], ["sigstop", "sigstop_failed"])
        self.assertEqual([p.pid for p in ledger.paused], [100])

    def test_reused_pid_between_scan_and_stop_not_signalled(self):
        ledger, calls, notes = watch.Ledger(), [], []
        table = {1: P(0, uid=0), 100: P(1, starttime=5)}
        watch.pause_pass(lambda: table, lambda t: {100}, 999, UID, ledger, calls.append, lambda pid: 6, notes.append)
        self.assertEqual(calls, [])
        self.assertEqual(notes[0]["error"], "gone_or_reused")

    def test_resume_reverse_order_with_pid_reuse_guard(self):
        first = {1: P(0, uid=0), 100: P(1, CODEX), 101: P(100), 102: P(101), 103: P(102)}
        ledger, _, _, _ = self.run_pass([first])
        alive = {100: 100, 101: 100, 102: 777}       # 102 reused, 103 gone
        calls, notes = [], []
        resumed, skipped = watch.resume_all(ledger, calls.append, alive.get, notes.append)
        self.assertEqual((resumed, skipped), (2, 2))
        self.assertEqual(calls, [101, 100])
        self.assertEqual([(n["event"], n["pid"], n.get("reason")) for n in notes],
                         [("sigcont_skipped", 103, "gone"), ("sigcont_skipped", 102, "pid_reused"),
                          ("sigcont", 101, None), ("sigcont", 100, None)])


class FixtureCliTests(unittest.TestCase):
    def test_dry_run_from_fixture_until_deadline(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "proc"
            for pid, ppid, cmd in [(4100001, 1, CODEX.encode() + b"\0--token=hunter2\0"),
                                   (4100002, 4100001, b"python3\0launch_sgp.py\0--password=x\0")]:
                (root / str(pid)).mkdir(parents=True)
                (root / str(pid) / "stat").write_text(stat_line(pid, "c", "S", ppid, 42))
                (root / str(pid) / "cmdline").write_bytes(cmd)
            log = Path(temporary) / "log.jsonl"
            result = subprocess.run(
                [sys.executable, "-B", str(SCRIPT), "--done-file", str(Path(temporary) / "none"), "--done-marker",
                 "DONE", "--deadline-utc", deadline_in(3), "--log", str(log), "--proc-root", str(root),
                 "--poll-seconds", "0.05"], timeout=30, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            events = read_log(log)
            self.assertTrue(events[0]["dry_run"])
            stops = [e for e in events if e["event"] == "sigstop"]
            self.assertEqual([(e["pid"], e["depth"], e["dry_run"]) for e in stops],
                             [(4100001, 0, True), (4100002, 1, True)])
            self.assertEqual([e["pid"] for e in events if e["event"] == "sigcont"], [4100002, 4100001])
            self.assertEqual(events[-1]["reason"], "deadline")
            self.assertEqual((events[-1]["stopped"], events[-1]["resumed"]), (2, 2))
            text = log.read_text()
            for secret in ("hunter2", "--password", "launch_sgp.py", "--token"):
                self.assertNotIn(secret, text)


PARENT = r"""
import subprocess, sys, time
GRANDCHILD = "import time; time.sleep(120)"
CHILD = "import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', %r]); time.sleep(120)" % GRANDCHILD
subprocess.Popen([sys.executable, "-c", CHILD])
time.sleep(120)
"""


@unittest.skipUnless(HAS_PROC, "/proc unavailable")
class RealProcessTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.dir = Path(self.temporary.name)
        self.watcher = None
        self.parent = subprocess.Popen([sys.executable, "-B", "-c", PARENT], start_new_session=True)
        self.tree = {}  # pid -> starttime, our own spawned processes only
        self.addCleanup(self.cleanup)  # runs even when setUp fails below
        until = time.time() + 15
        while time.time() < until:
            table = watch.read_process_table()
            targets = watch.pause_targets(table, {self.parent.pid}, set(), os.getuid())
            if len(targets) == 3:
                self.tree = {t.pid: table[t.pid].starttime for t in targets}
                self.order = [t.pid for t in targets]
                break
            time.sleep(0.05)
        self.assertEqual(len(self.tree), 3, "test tree did not come up")

    def cleanup(self):
        table = watch.read_process_table()
        for target in watch.pause_targets(table, {self.parent.pid}, set(), os.getuid()):
            self.tree.setdefault(target.pid, table[target.pid].starttime)
        if self.watcher is not None and self.watcher.poll() is None:
            self.watcher.terminate()
            try:
                self.watcher.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.watcher.kill()
                self.watcher.wait()
        for pid, start in self.tree.items():
            if watch.read_starttime("/proc", pid) == start:  # still our process
                for sig in (signal.SIGCONT, signal.SIGKILL):
                    try:
                        os.kill(pid, sig)
                    except OSError:
                        pass
        self.parent.wait(timeout=10)
        self.temporary.cleanup()

    def state(self, pid):
        return watch.parse_stat(Path(f"/proc/{pid}/stat").read_text())[0]

    def wait_for(self, predicate, seconds=15):
        until = time.time() + seconds
        while time.time() < until:
            if predicate():
                return True
            time.sleep(0.05)
        return False

    def start_watcher(self, deadline_seconds=120):
        self.log, self.done = self.dir / "log.jsonl", self.dir / "done"
        self.watcher = subprocess.Popen(
            [sys.executable, "-B", str(SCRIPT), "--done-file", str(self.done), "--done-marker", "DONE",
             "--deadline-utc", deadline_in(deadline_seconds), "--log", str(self.log), "--also-pid",
             str(self.parent.pid), "--no-ide-match", "--poll-seconds", "0.05"])
        self.assertTrue(self.wait_for(lambda: all(self.state(pid) == "T" for pid in self.tree)),
                        {pid: self.state(pid) for pid in self.tree})

    def assert_resumed_alive(self, reason):
        for pid, start in self.tree.items():
            self.assertEqual(watch.read_starttime("/proc", pid), start, f"{pid} gone")
            self.assertNotIn(self.state(pid), ("T", "Z"))
        events = read_log(self.log)
        stops = [e for e in events if e["event"] == "sigstop"]
        self.assertEqual([e["pid"] for e in stops], self.order)
        self.assertEqual([e["depth"] for e in stops], [0, 1, 2])
        self.assertEqual([e["pid"] for e in events if e["event"] == "sigcont"], self.order[::-1])
        self.assertEqual(events[-1]["event"], "end")
        self.assertEqual(events[-1]["reason"], reason)
        self.assertEqual((events[-1]["stopped"], events[-1]["resumed"], events[-1]["resume_skipped"]), (3, 3, 0))
        also = [e for e in events if e["event"] == "also_pid"]
        self.assertEqual(also[0]["pid"], self.parent.pid)
        self.assertTrue(also[0]["eligible"])
        self.assertFalse(events[0]["ide_match"])

    def test_tree_paused_then_resumed_on_done_marker(self):
        self.start_watcher()
        time.sleep(0.3)
        self.assertTrue(all(self.state(pid) == "T" for pid in self.tree))
        self.done.write_text("DONE\n")
        self.assertEqual(self.watcher.wait(timeout=15), 0)
        self.assert_resumed_alive("done")

    def test_sigterm_to_watcher_resumes_tree(self):
        self.start_watcher()
        self.watcher.send_signal(signal.SIGTERM)
        self.assertEqual(self.watcher.wait(timeout=15), 0)
        self.assert_resumed_alive("signal:SIGTERM")


if __name__ == "__main__":
    unittest.main()
