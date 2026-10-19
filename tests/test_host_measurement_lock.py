"""Offline contracts of the shared-host measurement lock wrapper (AGENTS.md 34a); no Docker, no real lock file."""
import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest

from scripts import host_measurement_lock as hml

T0 = dt.datetime(2026, 10, 16, 12, 0, tzinfo=dt.timezone.utc)


class LockStateTests(unittest.TestCase):
    def test_states(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "lock"
            self.assertEqual(hml.lock_state(path, T0), ("ABSENT", None))
            path.write_text("{")
            self.assertEqual(hml.lock_state(path, T0)[0], "UNREADABLE")
            path.write_text(json.dumps(hml.new_lock("a", "p", 10, T0)))
            self.assertEqual(hml.lock_state(path, T0)[0], "VALID")
            self.assertEqual(hml.lock_state(path, T0 + dt.timedelta(minutes=11))[0], "EXPIRED")
            path.write_text(json.dumps({"owner": "x", "until_utc": "2026-10-16T12:05:00+00:00"}))  # offset form
            self.assertEqual(hml.lock_state(path, T0)[0], "VALID")
            self.assertEqual(hml.lock_state(path, T0 + dt.timedelta(minutes=6))[0], "EXPIRED")
            path.write_text(json.dumps({"owner": "x", "until_utc": "soon"}))
            self.assertEqual(hml.lock_state(path, T0)[0], "UNREADABLE")

    def test_deadline_is_bounded_to_three_hours(self):
        self.assertEqual(hml.new_lock("a", "p", 180, T0)["until_utc"], "2026-10-16T15:00:00Z")
        for minutes in (0, 181):
            with self.subTest(minutes=minutes), self.assertRaises(ValueError):
                hml.new_lock("a", "p", minutes, T0)
        with self.assertRaises(ValueError):
            hml.new_lock("", "p", 10, T0)

    def test_release_only_own_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "lock"
            self.assertEqual(hml.release("me", path), "ABSENT")
            path.write_text(json.dumps(hml.new_lock("other", "p", 10)))
            self.assertEqual(hml.release("me", path), "NOT_OWNER")
            self.assertTrue(path.exists())
            path.write_text(json.dumps(hml.new_lock("me", "p", 10)))
            self.assertEqual(hml.release("me", path), "RELEASED")
            self.assertFalse(path.exists())


class AcquireTests(unittest.TestCase):
    def run_acquire(self, path, foreign_seq, **kw):
        logs, clock = [], [0.0]
        seq = iter(foreign_seq)

        def sleep(s):
            clock[0] += s
        code = hml.acquire("me", "G12", 60, wait_s=kw.pop("wait_s", 3600), poll_s=60, path=path,
                           foreign=lambda: next(seq, []), clock=lambda: clock[0], sleep=sleep,
                           log=lambda *x: logs.append(x[0]), **kw)
        return code, logs

    def test_waits_for_a_valid_foreign_lock_and_never_touches_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "lock"
            path.write_text(json.dumps(hml.new_lock("burst", "p", 180)))
            code, logs = self.run_acquire(path, [], wait_s=150)
            self.assertEqual(code, 4)
            self.assertEqual(set(logs), {"WAIT_LOCK", "WINDOW_NOT_SCHEDULED: valid lock or foreign job until the "
                                                      "wait deadline; nothing changed"})
            self.assertEqual(json.loads(path.read_text())["owner"], "burst")

    def test_removes_expired_lock_then_takes_its_own(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "lock"
            path.write_text(json.dumps({"owner": "old", "until_utc": "2026-01-01T00:00:00Z"}))
            code, logs = self.run_acquire(path, [[], []])
            self.assertEqual(code, 0)
            self.assertEqual(logs, ["REMOVED_EXPIRED_LOCK", "LOCK_TAKEN"])
            self.assertEqual(json.loads(path.read_text())["owner"], "me")

    def test_foreign_job_in_the_gap_releases_own_lock_and_waits(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "lock"
            code, logs = self.run_acquire(path, [[], ["p184-dr08-x"], ["p184-dr08-x"], [], []])
            self.assertEqual(code, 0)
            self.assertEqual(logs, ["RELEASED_OWN_LOCK_FOREIGN", "WAIT_FOREIGN", "LOCK_TAKEN"])


if __name__ == "__main__":
    unittest.main()
