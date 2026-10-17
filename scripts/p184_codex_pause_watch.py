#!/usr/bin/env python3
"""Pause the VS Code Codex extension server for one measurement window, then resume it (owner-approved 2026-10-17).

Purpose: a final stage-100 needs ~80 min of host without another agent's jobs. Each VS Code reload starts a fresh
Codex app-server that may resume its work. This watcher SIGSTOPs every process whose argv0 is the extension's own
Codex binary (``~/.vscode-server/extensions/openai.chatgpt-*/bin/*/codex*``) as soon as it appears, and SIGCONTs every
process it paused when ``--done-file`` contains ``--done-marker`` or at ``--deadline-utc``, whichever comes first.

Boundaries: matches argv0 only (never ``pgrep -f``, which also matches shells mentioning the path); never kills; only
pid owned by the invoking user; writes one JSON line per action to ``--log``.

usage: p184_codex_pause_watch.py --done-file FILE --done-marker TEXT --deadline-utc 2026-10-17T16:30:00Z --log FILE
       [--also-pid PID ...]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import signal
import time
from pathlib import Path

PREFIX = "/.vscode-server/extensions/openai.chatgpt-"


def utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"


def argv0(pid: int) -> str:
    try:
        return (Path("/proc") / str(pid) / "cmdline").read_bytes().split(b"\0", 1)[0].decode("utf-8", "replace")
    except OSError:
        return ""


def ide_codex_pids(uid: int) -> set[int]:
    found = set()
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        pid = int(name)
        try:
            if (Path("/proc") / name).stat().st_uid != uid:
                continue
        except OSError:
            continue
        head = argv0(pid)
        if PREFIX in head and "/bin/" in head and os.path.basename(head).startswith("codex"):
            found.add(pid)
    return found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--done-file", required=True)
    parser.add_argument("--done-marker", required=True)
    parser.add_argument("--deadline-utc", required=True)
    parser.add_argument("--log", required=True)
    parser.add_argument("--also-pid", type=int, action="append", default=[])
    parser.add_argument("--poll-seconds", type=float, default=0.2)
    args = parser.parse_args()
    deadline = dt.datetime.strptime(args.deadline_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp()
    uid, paused = os.getuid(), {}
    log = Path(args.log)

    def note(item: dict) -> None:
        with log.open("a") as handle:
            handle.write(json.dumps({"at": utc(), **item}, sort_keys=True) + "\n")

    def done() -> bool:
        try:
            return args.done_marker in Path(args.done_file).read_text()
        except OSError:
            return False

    note({"event": "start", "deadline": args.deadline_utc})
    for pid in args.also_pid:
        paused[pid] = argv0(pid)
    while time.time() < deadline and not done():
        for pid in ide_codex_pids(uid) - paused.keys():
            try:
                os.kill(pid, signal.SIGSTOP)
                paused[pid] = argv0(pid)
                note({"event": "sigstop", "pid": pid, "argv0": paused[pid]})
            except OSError as error:
                note({"event": "sigstop_failed", "pid": pid, "error": str(error)})
        time.sleep(args.poll_seconds)
    for pid in sorted(paused):
        try:
            os.kill(pid, signal.SIGCONT)
            note({"event": "sigcont", "pid": pid})
        except OSError as error:
            note({"event": "sigcont_skipped", "pid": pid, "error": str(error)})
    note({"event": "end", "reason": "done" if done() else "deadline"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
