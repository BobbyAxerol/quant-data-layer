"""Container-local healthcheck and last-resort recovery of a stuck native call.

Runs outside the projector interpreter, so a native call holding its GIL cannot
disable recovery. It can terminate only the verified projector child of
Docker's init, never a host process or a PID supplied by a heartbeat file.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from pathlib import Path


def heartbeat_age(
    payload: dict, *, pid: int, started_ns: int, now_ns: int
) -> float:
    if payload.get("role") != "stable_projector" or payload.get("pid") != pid:
        return (now_ns - started_ns) / 1e9
    stamp = payload.get("monotonic_ns")
    if not isinstance(stamp, int) or isinstance(stamp, bool) or not started_ns <= stamp <= now_ns:
        return (now_ns - started_ns) / 1e9
    return (now_ns - stamp) / 1e9


def check_projector(
    heartbeat: Path, *, proc: Path = Path("/proc"), now_ns: int | None = None,
    healthy_seconds: float = 30, restart_seconds: float = 120,
    terminate=None,
) -> bool:
    if not 0 < healthy_seconds < restart_seconds:
        raise ValueError("projector watchdog deadlines are invalid")
    # Fail without signalling when invoked outside the exact Docker role.
    init_command = (proc / "1/cmdline").read_bytes().split(b"\0")
    if Path(os.fsdecode(init_command[0])).name not in {"docker-init", "tini"}:
        return False
    children = (proc / "1/task/1/children").read_text().split()
    if len(children) != 1:
        return False
    pid = int(children[0])
    if pid <= 1:
        return False
    command = (proc / f"{pid}/cmdline").read_bytes().split(b"\0")
    if len(command) < 3 or command[1:3] != [b"-m", b"app.entrypoints.projector_v2_stable"]:
        return False
    stat = (proc / f"{pid}/stat").read_text().rsplit(")", 1)[1].split()
    if int(stat[1]) != 1:
        return False
    started_ns = int(stat[19]) * 1_000_000_000 // os.sysconf("SC_CLK_TCK")
    now_ns = time.monotonic_ns() if now_ns is None else now_ns
    if now_ns < started_ns:
        return False
    try:
        payload = json.loads(heartbeat.read_text())
        if not isinstance(payload, dict):
            payload = {}
    except (OSError, ValueError):
        payload = {}
    age = heartbeat_age(payload, pid=pid, started_ns=started_ns, now_ns=now_ns)
    if age > restart_seconds:
        print(f"projector watchdog: no loop progress for {age:.1f}s; restarting own projector PID {pid}", flush=True)
        (terminate or os.kill)(pid, signal.SIGKILL)
    stamp = payload.get("monotonic_ns")
    valid = (
        payload.get("role") == "stable_projector" and payload.get("pid") == pid
        and isinstance(stamp, int) and not isinstance(stamp, bool)
        and started_ns <= stamp <= now_ns
    )
    return valid and age <= healthy_seconds


def main() -> int:
    path = os.environ.get("QDL_STABLE_HEARTBEAT_PATH")
    if not path:
        return 1
    try:
        return 0 if check_projector(Path(path)) else 1
    except (OSError, ValueError, IndexError):
        return 1


if __name__ == "__main__":
    sys.exit(main())
