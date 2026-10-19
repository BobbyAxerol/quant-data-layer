#!/usr/bin/env python3
"""Pause the VS Code Codex extension server and its whole process tree for one measurement window, then resume it.

Purpose (owner-approved 2026-10-17: pause Codex for measurement windows, NEVER kill): a final stage-100 needs ~80 min
of host without another agent's jobs. Each VS Code reload starts a fresh Codex app-server that may resume its work.
This watcher SIGSTOPs every root process -- argv0 is the extension's own Codex binary
(``~/.vscode-server/extensions/openai.chatgpt-*/bin/*/codex*``), plus every ``--also-pid`` -- together with every
descendant of a root, as soon as it appears. When ``--done-file`` contains ``--done-marker``, at ``--deadline-utc``
(whichever comes first), or when the watcher itself receives SIGTERM/SIGINT, it SIGCONTs everything it paused.

Why the tree (2026-10-18): a Codex IDE app-server had a child python launcher (``launch_sgp.py``) that spawned
``ssh ...`` every 30 s. SIGSTOP of only the app-server did not stop its children, and the ssh traffic tainted a
measurement run. Stopping a parent does not stop its children, so the tree must be stopped explicitly.

Tree rule: a process is a target iff it is a root or its parent chain (``/proc/<pid>/stat`` field 4) reaches a root
through eligible processes only. Eligible = owned by the invoking uid (``st_uid`` of ``/proc/<pid>``), not a zombie,
not pid 1/2, not a kernel thread (ppid 2), not the watcher itself and not one of the watcher's own ancestors. An
ineligible process is neither paused nor descended into. There is NO session-id/pgid widening: the VS Code server
session also contains other agents, and only the parent chain decides. Each poll stops targets parents-before-children
(BFS from the roots, so a parent cannot fork new children while its existing ones are being stopped) and rescans
``/proc`` until a fixpoint (at most ``MAX_RESCANS`` scans), so a child forked during the pass is stopped in the same
poll. Resume runs in the reverse order of stopping (children first) and only for a pid whose starttime (stat field 22)
still matches the one recorded at stop time (pid-reuse guard); otherwise ``sigcont_skipped`` with reason
``pid_reused``/``gone``.

Boundaries: matches argv0 only (never ``pgrep -f``, which also matches shells mentioning the path); never kills; never
logs a full command line (argv0 only -- command lines can hold secrets); writes one JSON line per action to ``--log``
(start, also_pid, sigstop, sigstop_failed, rescan_limit, sigcont, sigcont_skipped, end). ``--also-pid`` is a real root:
it is SIGSTOPped together with its tree (before 2026-10-18 it was only recorded and SIGCONTed). ``--proc-root`` other
than ``/proc`` is a test fixture: the table is read from it and every signal is logged with ``"dry_run": true`` but
never sent. ``--no-ide-match`` disables the argv0 rule so tests can pause only their own ``--also-pid`` tree.

usage: p184_codex_pause_watch.py --done-file FILE --done-marker TEXT --deadline-utc 2026-10-17T16:30:00Z --log FILE
       [--also-pid PID ...] [--poll-seconds 0.2] [--proc-root /proc] [--no-ide-match]
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import os
import signal
import time
from pathlib import Path
from typing import Callable, Iterable, NamedTuple

PREFIX = "/.vscode-server/extensions/openai.chatgpt-"
MAX_RESCANS = 10


class Proc(NamedTuple):
    ppid: int
    uid: int
    state: str
    starttime: int
    argv0: str


class Target(NamedTuple):
    pid: int
    ppid: int
    root: int
    depth: int


class Paused(NamedTuple):
    pid: int
    ppid: int
    starttime: int
    argv0: str
    root: int
    depth: int


def utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"


def parse_stat(text: str) -> tuple[str, int, int]:
    """Return (state, ppid, starttime) from a /proc/<pid>/stat line; comm may contain spaces and parentheses."""
    fields = text[text.rindex(")") + 1:].split()
    # fields[0] is stat field 3 (state), so stat field N is fields[N - 3].
    return fields[0], int(fields[1]), int(fields[19])


def read_argv0(proc_root: str | Path, pid: int) -> str:
    try:
        return (Path(proc_root) / str(pid) / "cmdline").read_bytes().split(b"\0", 1)[0].decode("utf-8", "replace")
    except OSError:
        return ""


def argv0(pid: int) -> str:
    return read_argv0("/proc", pid)


def read_starttime(proc_root: str | Path, pid: int) -> int | None:
    try:
        return parse_stat((Path(proc_root) / str(pid) / "stat").read_text())[2]
    except (OSError, ValueError, IndexError):
        return None


def read_process_table(proc_root: str | Path = "/proc") -> dict[int, Proc]:
    """One scan of ``proc_root``: {pid: Proc}. Processes of every uid are kept (ancestor walks need them)."""
    table = {}
    root = Path(proc_root)
    try:
        names = os.listdir(root)
    except OSError:
        return table
    for name in names:
        if not name.isdigit():
            continue
        entry = root / name
        try:
            uid = entry.stat().st_uid
            state, ppid, starttime = parse_stat((entry / "stat").read_text())
        except (OSError, ValueError, IndexError):
            continue  # exited during the scan, or unreadable
        table[int(name)] = Proc(ppid, uid, state, starttime, read_argv0(root, int(name)))
    return table


def is_ide_codex(head: str) -> bool:
    return PREFIX in head and "/bin/" in head and os.path.basename(head).startswith("codex")


def ide_codex_roots(table: dict[int, Proc], uid: int) -> set[int]:
    return {pid for pid, proc in table.items() if proc.uid == uid and is_ide_codex(proc.argv0)}


def ide_codex_pids(uid: int, proc_root: str | Path = "/proc") -> set[int]:
    return ide_codex_roots(read_process_table(proc_root), uid)


def self_chain(table: dict[int, Proc], pid: int) -> set[int]:
    """``pid`` and all of its ancestors up to pid 1 (cycle- and gap-safe)."""
    chain = set()
    while pid > 0 and pid not in chain:
        chain.add(pid)
        proc = table.get(pid)
        if proc is None:
            break
        pid = proc.ppid
    return chain


def eligible(table: dict[int, Proc], pid: int, exclude: set[int], uid: int) -> bool:
    proc = table.get(pid)
    return (proc is not None and pid not in (0, 1, 2) and pid not in exclude and proc.ppid != 2
            and proc.uid == uid and proc.state != "Z")


def pause_targets(table: dict[int, Proc], roots: Iterable[int], exclude: set[int], uid: int) -> list[Target]:
    """Roots and their eligible descendants in BFS order (every parent precedes its children)."""
    children = collections.defaultdict(list)
    for pid, proc in table.items():
        children[proc.ppid].append(pid)
    ordered, seen = [], set()
    queue = collections.deque()
    for root in sorted(set(roots)):
        if root not in seen and eligible(table, root, exclude, uid):
            seen.add(root)
            queue.append(Target(root, table[root].ppid, root, 0))
    while queue:
        target = queue.popleft()
        ordered.append(target)
        for child in sorted(children.get(target.pid, ())):
            if child not in seen and eligible(table, child, exclude, uid):
                seen.add(child)
                queue.append(Target(child, target.pid, target.root, target.depth + 1))
    return ordered


class Ledger:
    """Paused processes in stop order, keyed by (pid, starttime) so a reused pid is a different process."""

    def __init__(self) -> None:
        self.paused: list[Paused] = []
        self.keys: set[tuple[int, int]] = set()
        self.failed: set[tuple[int, int]] = set()


def pause_pass(read_table: Callable[[], dict[int, Proc]], roots_of: Callable[[dict[int, Proc]], set[int]],
               own_pid: int, uid: int, ledger: Ledger, stop: Callable[[int], None],
               starttime_of: Callable[[int], int | None], note: Callable[[dict], None],
               max_rescans: int = MAX_RESCANS) -> int:
    """Stop every new target, rescanning until no new target appears. Returns the number of scans used."""
    for scan in range(1, max_rescans + 1):
        table = read_table()
        targets = pause_targets(table, roots_of(table), self_chain(table, own_pid), uid)
        known = ledger.keys | ledger.failed
        new = [t for t in targets if (t.pid, table[t.pid].starttime) not in known]
        if not new:
            return scan
        for target in new:
            proc = table[target.pid]
            key = (target.pid, proc.starttime)
            if starttime_of(target.pid) != proc.starttime:
                ledger.failed.add(key)  # exited (or pid reused) since the scan: never signal a stranger
                note({"event": "sigstop_failed", "pid": target.pid, "argv0": proc.argv0, "error": "gone_or_reused"})
                continue
            try:
                stop(target.pid)
            except OSError as error:
                ledger.failed.add(key)
                note({"event": "sigstop_failed", "pid": target.pid, "argv0": proc.argv0, "error": str(error)})
                continue
            ledger.keys.add(key)
            ledger.paused.append(Paused(target.pid, target.ppid, proc.starttime, proc.argv0, target.root, target.depth))
            note({"event": "sigstop", "pid": target.pid, "ppid": target.ppid, "argv0": proc.argv0,
                  "root": target.root, "depth": target.depth, "prior_state": proc.state})
    note({"event": "rescan_limit", "scans": max_rescans})
    return max_rescans


def resume_all(ledger: Ledger, cont: Callable[[int], None], starttime_of: Callable[[int], int | None],
               note: Callable[[dict], None]) -> tuple[int, int]:
    """SIGCONT in reverse stop order (children first), only when the pid still has its recorded starttime."""
    resumed = skipped = 0
    for item in reversed(ledger.paused):
        current = starttime_of(item.pid)
        if current is None or current != item.starttime:
            skipped += 1
            note({"event": "sigcont_skipped", "pid": item.pid, "argv0": item.argv0,
                  "reason": "gone" if current is None else "pid_reused"})
            continue
        try:
            cont(item.pid)
        except OSError as error:
            skipped += 1
            note({"event": "sigcont_skipped", "pid": item.pid, "argv0": item.argv0, "reason": "error",
                  "error": str(error)})
            continue
        resumed += 1
        note({"event": "sigcont", "pid": item.pid, "argv0": item.argv0, "depth": item.depth})
    return resumed, skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--done-file", required=True)
    parser.add_argument("--done-marker", required=True)
    parser.add_argument("--deadline-utc", required=True)
    parser.add_argument("--log", required=True)
    parser.add_argument("--also-pid", type=int, action="append", default=[],
                        help="extra root: SIGSTOPped with its whole descendant tree, resumed at the end")
    parser.add_argument("--poll-seconds", type=float, default=0.2)
    parser.add_argument("--proc-root", default="/proc", help="process table source; non-/proc = fixture, dry run")
    parser.add_argument("--no-ide-match", action="store_true", help="only --also-pid roots (tests)")
    args = parser.parse_args(argv)
    deadline = dt.datetime.strptime(args.deadline_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp()
    uid, own_pid, log = os.getuid(), os.getpid(), Path(args.log)
    dry_run = os.path.realpath(args.proc_root) != "/proc"
    also = set(args.also_pid)
    received: list[str] = []

    def note(item: dict) -> None:
        if dry_run and item["event"] in ("sigstop", "sigcont"):
            item = {**item, "dry_run": True}
        with log.open("a") as handle:
            handle.write(json.dumps({"at": utc(), **item}, sort_keys=True) + "\n")

    def done() -> bool:
        try:
            return args.done_marker in Path(args.done_file).read_text()
        except OSError:
            return False

    def send(sig: int) -> Callable[[int], None]:
        return (lambda pid: None) if dry_run else (lambda pid: os.kill(pid, sig))

    def on_signal(signum: int, _frame) -> None:
        received.append(signal.Signals(signum).name)

    def roots_of(table: dict[int, Proc]) -> set[int]:
        return (set() if args.no_ide_match else ide_codex_roots(table, uid)) | also

    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, on_signal)
    starttime_of = lambda pid: read_starttime(args.proc_root, pid)  # noqa: E731
    ledger = Ledger()
    note({"event": "start", "deadline": args.deadline_utc, "poll_seconds": args.poll_seconds,
          "ide_match": not args.no_ide_match, "also_pid": sorted(also), "dry_run": dry_run, "watcher_pid": own_pid})
    table = read_process_table(args.proc_root)
    exclude = self_chain(table, own_pid)
    for pid in sorted(also):
        note({"event": "also_pid", "pid": pid, "argv0": table[pid].argv0 if pid in table else "",
              "present": pid in table, "eligible": eligible(table, pid, exclude, uid)})
    try:
        while not received and time.time() < deadline and not done():
            pause_pass(lambda: read_process_table(args.proc_root), roots_of, own_pid, uid, ledger,
                       send(signal.SIGSTOP), starttime_of, note)
            if received:
                break
            time.sleep(args.poll_seconds)
    finally:
        resumed, skipped = resume_all(ledger, send(signal.SIGCONT), starttime_of, note)
        reason = f"signal:{received[0]}" if received else ("done" if done() else "deadline")
        note({"event": "end", "reason": reason, "stopped": len(ledger.paused), "stop_failed": len(ledger.failed),
              "resumed": resumed, "resume_skipped": skipped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
