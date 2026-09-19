"""R1.31: a role that is alive and idle must be distinguishable from one that stopped.

On 2026-09-19 two of three projectors hung inside the supervisor's recovery path
and stayed `Up` for eleven minutes. Docker restarts a process that exits and has
no opinion about one that is alive and doing nothing, and neither the projector
nor the bar edge wrote anything a healthcheck could read - the ingestors did,
through their session-liveness files, which is why only they could be checked.

These tests pin the heartbeat and, more importantly, the rule that it must never
break the role it watches.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from qdl.runtime.heartbeat import write_heartbeat


class HeartbeatTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = Path(self._tmp.name) / "hb" / "role.json"

    def test_it_writes_a_readable_heartbeat(self) -> None:
        write_heartbeat(self.path, role="stable_projector", detail="md.canonical.v2")
        payload = json.loads(self.path.read_text())
        self.assertEqual(payload["schema"], "qdl.role-heartbeat.v1")
        self.assertEqual(payload["role"], "stable_projector")
        self.assertEqual(payload["detail"], "md.canonical.v2")
        self.assertEqual(payload["pid"], os.getpid())
        self.assertGreater(payload["updated_at_ns"], 0)

    def test_it_creates_the_directory(self) -> None:
        self.assertFalse(self.path.parent.exists())
        write_heartbeat(self.path, role="stable_bar_edge")
        self.assertTrue(self.path.exists())

    def test_a_later_write_advances_the_timestamp(self) -> None:
        write_heartbeat(self.path, role="r")
        first = json.loads(self.path.read_text())["updated_at_ns"]
        write_heartbeat(self.path, role="r")
        self.assertGreater(json.loads(self.path.read_text())["updated_at_ns"], first)

    def test_an_unwritable_path_does_not_raise(self) -> None:
        """A heartbeat that kills its own role is worse than no heartbeat."""
        write_heartbeat("/proc/cannot/write/here.json", role="r")

    def test_it_leaves_no_temporary_file_behind(self) -> None:
        write_heartbeat(self.path, role="r")
        leftovers = [p.name for p in self.path.parent.iterdir() if p.name.startswith(".hb-")]
        self.assertEqual(leftovers, [])

    def test_a_reader_never_sees_a_half_written_file(self) -> None:
        """os.replace is atomic; a healthcheck must never parse a partial write."""
        source = (Path(__file__).resolve().parents[1]
                  / "qdl/runtime/heartbeat.py").read_text(encoding="utf-8")
        self.assertIn("os.replace(temporary, target)", source)


class RolesWriteHeartbeatTests(unittest.TestCase):
    """Both roles that had no readable signal now have one, on their own loop."""

    @staticmethod
    def _source(name: str) -> str:
        return (Path(__file__).resolve().parents[1] / name).read_text(encoding="utf-8")

    def test_the_projector_beats_on_every_loop_turn(self) -> None:
        source = self._source("qdl/runtime/stable_projector.py")
        self.assertIn("async def run_once", source)
        head = source.split("async def run_once", 1)[1][:600]
        self.assertIn("write_heartbeat(self.heartbeat_path", head)

    def test_the_bar_edge_beats_on_every_loop_turn(self) -> None:
        source = self._source("qdl/runtime/stable_bar_edge.py")
        body = source.split("while not self._stopped.is_set():", 1)[1][:900]
        self.assertIn("write_heartbeat(heartbeat_path", body)

    def test_both_stay_silent_when_no_path_is_configured(self) -> None:
        """An unset variable must leave the roles exactly as they were."""
        for name, attr in (("qdl/runtime/stable_projector.py", "self.heartbeat_path"),
                           ("qdl/runtime/stable_bar_edge.py", "heartbeat_path")):
            with self.subTest(role=name):
                self.assertIn(f"if {attr} is not None:", self._source(name))

    def test_the_bar_edge_tolerates_a_double_without_the_attribute(self) -> None:
        """The scheduling tests drive run_forever on an object with no __init__."""
        source = self._source("qdl/runtime/stable_bar_edge.py")
        self.assertIn('getattr(self, "_heartbeat_path", None)', source)


if __name__ == "__main__":
    unittest.main()
