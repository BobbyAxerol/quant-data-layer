#!/usr/bin/env python3
"""Shared-host measurement lock (AGENTS.md 34a): check, wait-then-acquire, release.

Purpose: one reusable wrapper for the lock every container/build/benchmark job must respect, so gate runs (G12, G13
isolated, G13 runtime window) do not each carry an inline loop. Lock file ``~/.local/state/host-measurement-window.lock``
= JSON {owner, purpose, started_utc, until_utc}, deadline at most 3 h.

Modes (stdlib only, host python, no container):
  check                 read-only: prints the lock state (VALID / EXPIRED / ABSENT / UNREADABLE) and foreign jobs;
                        exit 0 when the host is free for a new job (absent or expired lock, no foreign job), else 3.
  acquire --owner O --purpose P --minutes M [--wait-hours H]
                        waits (polls every --poll-s) while a valid lock of ANY owner exists or a foreign job runs;
                        removes an expired lock (34a allows); creates the lock with O_EXCL (a concurrent creator wins
                        and we wait again); re-checks foreign jobs after creation and releases its own lock if one
                        appeared in the gap. Exit 0 LOCK_TAKEN, 4 WINDOW_NOT_SCHEDULED (deadline, nothing changed).
  release --owner O     deletes the lock only when its owner is O (never another agent's lock); exit 0 also when
                        the lock is already absent; 4 when it belongs to someone else.
Foreign jobs: running containers whose name starts with a test prefix (FOREIGN_PREFIXES, plus --foreign-prefix) and
processes matching FOREIGN_PROCESSES that are not this process's ancestors. Prints owner/until and names only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import sys
import time

LOCK = Path.home() / ".local/state/host-measurement-window.lock"
MAX_MINUTES = 180
FOREIGN_PREFIXES = ("p184-g12-dl-", "p184-g13-", "p184-dr05-", "p184-dr08-", "p184-n1-", "ts-p184", "idr094-",
                    "qdl-phase3-target-", "pma11-", "ts-edc1-codex-", "arch")
FOREIGN_PROCESSES = r"p184_gate_campaign|p184_n1_capacity_run|cargo (build|test|bench)|pytest|docker build"


def fmt(t: dt.datetime) -> str:
    return t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def lock_state(path: Path = LOCK, at: dt.datetime | None = None) -> tuple[str, dict | None]:
    """(VALID|EXPIRED|ABSENT|UNREADABLE, document). UNREADABLE (being written / damaged) counts as valid."""
    try:
        doc = json.loads(Path(path).read_text())
    except FileNotFoundError:
        return "ABSENT", None
    except (ValueError, OSError):
        return "UNREADABLE", None
    if not isinstance(doc, dict) or not isinstance(doc.get("until_utc"), str):
        return "UNREADABLE", doc if isinstance(doc, dict) else None
    try:  # owners write "...Z" or "...+00:00"; compare instants, not strings
        until = dt.datetime.fromisoformat(doc["until_utc"].replace("Z", "+00:00"))
    except ValueError:
        return "UNREADABLE", doc
    if until.tzinfo is None:
        return "UNREADABLE", doc
    return ("VALID" if until > (at or now()) else "EXPIRED"), doc


def new_lock(owner: str, purpose: str, minutes: int, at: dt.datetime | None = None) -> dict:
    if not owner or not purpose:
        raise ValueError("owner and purpose required")
    if not 1 <= int(minutes) <= MAX_MINUTES:
        raise ValueError(f"minutes must be 1..{MAX_MINUTES} (34a: deadline at most 3 h)")
    start = at or now()
    return {"owner": owner, "purpose": purpose, "started_utc": fmt(start),
            "until_utc": fmt(start + dt.timedelta(minutes=int(minutes)))}


def create_exclusive(path: Path, doc: dict) -> bool:
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w") as out:
        out.write(json.dumps(doc, indent=1) + "\n")
    return True


def release(owner: str, path: Path = LOCK) -> str:
    state, doc = lock_state(path)
    if state == "ABSENT":
        return "ABSENT"
    if not isinstance(doc, dict) or doc.get("owner") != owner:
        return "NOT_OWNER"
    Path(path).unlink()
    return "RELEASED"


def ancestors() -> set[str]:
    pids, pid = set(), os.getpid()
    while pid > 1:
        pids.add(str(pid))
        try:
            pid = int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            break
    return pids


def foreign_jobs(prefixes=FOREIGN_PREFIXES) -> list[str]:
    names = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True,
                           check=True, timeout=30).stdout.split()
    jobs = subprocess.run(["pgrep", "-af", FOREIGN_PROCESSES], capture_output=True, text=True,
                          timeout=30).stdout.splitlines()
    own = ancestors()
    return [n for n in names if n.startswith(tuple(prefixes))] + [
        j.split(" ", 1)[0] + " <process>" for j in jobs if j.split(" ", 1)[0] not in own]


def acquire(owner, purpose, minutes, *, wait_s, poll_s=60.0, path=LOCK, foreign=foreign_jobs,
            clock=time.monotonic, sleep=time.sleep, log=print) -> int:
    new_lock(owner, purpose, minutes)  # validate before waiting
    deadline = clock() + wait_s
    while True:
        if clock() > deadline:
            log("WINDOW_NOT_SCHEDULED: valid lock or foreign job until the wait deadline; nothing changed")
            return 4
        state, doc = lock_state(path)
        if state in ("VALID", "UNREADABLE"):
            log("WAIT_LOCK", (doc or {}).get("owner"), (doc or {}).get("until_utc"))
            sleep(poll_s)
            continue
        if state == "EXPIRED":
            Path(path).unlink(missing_ok=True)
            log("REMOVED_EXPIRED_LOCK", doc.get("owner"), doc.get("until_utc"))
        busy = foreign()
        if busy:
            log("WAIT_FOREIGN", busy)
            sleep(poll_s)
            continue
        mine = new_lock(owner, purpose, minutes)
        if not create_exclusive(path, mine):
            continue  # someone else took it first: wait for theirs
        sleep(min(5.0, poll_s))
        busy = foreign()
        if busy:  # a job started between the check and the create
            release(owner, path)
            log("RELEASED_OWN_LOCK_FOREIGN", busy)
            sleep(poll_s)
            continue
        log("LOCK_TAKEN", json.dumps(mine, sort_keys=True))
        return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="mode", required=True)
    c = sub.add_parser("check")
    c.add_argument("--foreign-prefix", action="append", default=[])
    a = sub.add_parser("acquire")
    a.add_argument("--owner", required=True)
    a.add_argument("--purpose", required=True)
    a.add_argument("--minutes", type=int, required=True)
    a.add_argument("--wait-hours", type=float, default=6.0)
    a.add_argument("--poll-s", type=float, default=60.0)
    a.add_argument("--foreign-prefix", action="append", default=[])
    r = sub.add_parser("release")
    r.add_argument("--owner", required=True)
    args = p.parse_args(argv)
    if args.mode == "release":
        result = release(args.owner)
        print(result)
        return 4 if result == "NOT_OWNER" else 0
    prefixes = FOREIGN_PREFIXES + tuple(args.foreign_prefix)
    if args.mode == "check":
        state, doc = lock_state()
        busy = foreign_jobs(prefixes)
        print(json.dumps({"lock": state, "owner": (doc or {}).get("owner"), "until_utc": (doc or {}).get("until_utc"),
                          "foreign": busy}))
        return 0 if state in ("ABSENT", "EXPIRED") and not busy else 3
    return acquire(args.owner, args.purpose, args.minutes, wait_s=args.wait_hours * 3600, poll_s=args.poll_s,
                   foreign=lambda: foreign_jobs(prefixes), log=lambda *x: print(*x, flush=True))


if __name__ == "__main__":
    sys.exit(main())
