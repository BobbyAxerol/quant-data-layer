"""Unit tests for the stage-100 foreign-activity guard sidecar (classification and log parsing only)."""
from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import p184_foreign_activity_guard as guard  # noqa: E402


def info(comm, cmdline):
    return {"comm": comm, "cmdline": cmdline}


class ClassifyTool(unittest.TestCase):
    def test_docker_cli_any_subcommand(self):
        for cmd in ("docker image ls", "docker builder du", "docker system df", "docker inspect x",
                    "/usr/bin/docker exec -i w python", "docker run --rm alpine", "docker build .", "docker cp a b"):
            self.assertEqual(guard.classify_tool(info("docker", cmd)), "docker_cli", cmd)
        self.assertEqual(guard.classify_tool(info("docker-compose", "docker-compose up")), "docker_cli")

    def test_plumbing_is_not_cli(self):
        for comm in ("dockerd", "containerd-shim-runc-v2", "runc", "docker-proxy"):
            self.assertIsNone(guard.classify_tool(info(comm, f"/usr/bin/{comm} -x")), comm)

    def test_tools_and_python(self):
        self.assertEqual(guard.classify_tool(info("ssh", "ssh -p 22 host")), "tool:ssh")
        self.assertEqual(guard.classify_tool(info("tar", "tar czf a.tgz src")), "tool:tar")
        self.assertEqual(guard.classify_tool(info("python3", "python3 collect_inventory.py")), "tool:python")
        self.assertEqual(guard.classify_tool(info("python3.12", "/usr/bin/python3.12 -m pytest")), "tool:python")
        self.assertEqual(guard.classify_tool(info("cargo", "cargo build")), "tool:cargo")
        self.assertEqual(guard.classify_tool(info("bwrap", "bwrap --as-pid-1 ls")), "agent_exec:bwrap")
        self.assertEqual(guard.classify_tool(info("codex-linux-sandbox", "codex-linux-sandbox --x")),
                         "agent_exec:codex-linux-sandbox")
        self.assertIsNone(guard.classify_tool(info("sleep", "sleep 20")))
        self.assertIsNone(guard.classify_tool(info("git", "git status")))


class Parsers(unittest.TestCase):
    def test_parse_stat_with_spaces_in_comm(self):
        fields = ["S", "42"] + ["0"] * 9 + ["7", "5"] + ["0"] * 6 + ["1234"] + ["0"] * 10
        stat = guard.parse_stat("99 (a b) c) " + " ".join(fields))
        self.assertEqual(stat["comm"], "a b) c")
        self.assertEqual(stat["ppid"], 42)
        self.assertEqual(stat["cpu_ticks"], 12)
        self.assertEqual(stat["start_ticks"], 1234)

    def test_parse_stat_self(self):
        stat = guard.parse_stat(Path(f"/proc/{os.getpid()}/stat").read_text())
        self.assertEqual(stat["ppid"], os.getppid())

    def test_cleaner_phases(self):
        lines = {
            "[2026-10-17 10:59:41,001] INFO Cleaner 0: Beginning cleaning of log md.latest.v2-3. (x)": "begin",
            "[2026-10-17 10:59:41,002] INFO Cleaner 0: Building offset map for md.latest.v2-3... (x)": "offset_map",
            "[2026-10-17 11:02:21,003] INFO Cleaner 0: Offset map for log md.latest.v2-3 complete. (x)":
                "offset_map_done",
            "[2026-10-17 11:02:21,004] INFO Cleaner 0: Cleaning log md.latest.v2-3 (cleaning prior to Sat) (x)":
                "cleaning",
        }
        for line, phase in lines.items():
            parsed = guard.cleaner_phase(line)
            self.assertEqual(parsed["phase"], phase, line)
            self.assertEqual(parsed["log"], "md.latest.v2-3", line)
        swap = ("[2026-10-17 11:53:59,199] INFO Cleaner 0: Swapping in cleaned segment LogSegment(baseOffset=1) for "
                "segment(s) [x] in log Log(dir=/var/lib/kafka/data/md.latest.v2-1, topicId=L, topic=md.latest.v2)")
        self.assertEqual(guard.cleaner_phase(swap), {"ts": "2026-10-17 11:53:59,199", "phase": "swap",
                                                     "log": "md.latest.v2-1"})
        self.assertIsNone(guard.cleaner_phase("\tBuffer utilization: 0.1%"))

    def test_core_progress(self):
        line = json.dumps({"event": "qdl_realtime_core_progress", "batches": 5,
                           "commit_ms": {"max": 154.0, "mean": 15.0, "min": 10.0, "n": 100},
                           "commit_wait_ms": {"max": 20.0, "mean": 6.0, "min": 0.0, "n": 100}})
        parsed = guard.core_progress(line)
        self.assertEqual(parsed["commit_ms"], {"max": 154.0, "mean": 15.0, "n": 100})
        self.assertEqual(parsed["commit_wait_ms"]["max"], 20.0)
        self.assertIsNone(guard.core_progress('{"event": "other"}'))
        self.assertIsNone(guard.core_progress("not json"))


if __name__ == "__main__":
    unittest.main()
