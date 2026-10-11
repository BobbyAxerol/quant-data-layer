"""Run a native-client probe in a supervised child process (IDR-087).

Purpose: librdkafka 2.15.0 (confluent-kafka 2.15.0) segfaulted inside
``AdminClient.list_consumer_group_offsets`` during a broker stop (G12 isolated
harness, 2026-10-11: monitor child SIGSEGV; earlier runs exit 139). A native
crash cannot be caught in-process, so a role that only needs a read-only
metadata answer runs the client in a child: a crash, a hang past the hard
timeout or a dead pipe loses only that answer, comes back to the caller as
``ProbeUnavailable`` (typed reason + exit code), and the next call starts a
fresh child.

Start method: ``spawn`` (fresh interpreter via fork+exec). The parent may
already hold librdkafka clients and their threads (the BAR publisher); a plain
``fork`` would copy their locks in an arbitrary state into the child. With
spawn the child loads the native library itself and inherits nothing.

Boundary: one long-lived child per probe; requests are serialised; the
parent never imports the probed client. No retries here - the caller decides
what an unknown answer means.
"""
from __future__ import annotations

import multiprocessing
import signal
import threading
import time
from typing import Any, Callable


class ProbeUnavailable(RuntimeError):
    """The probe produced no answer: the child crashed, hung, or failed."""

    def __init__(self, reason: str, *, exitcode: int | None = None, detail: str = "") -> None:
        self.reason, self.exitcode, self.detail = reason, exitcode, detail
        super().__init__(f"{reason} exitcode={exitcode}{': ' + detail if detail else ''}")


def exit_reason(exitcode: int | None) -> str:
    """SIGSEGV for -11, EXIT_<n> for a plain exit, NO_EXIT if still unknown."""

    if exitcode is None:
        return "NO_EXIT"
    if exitcode < 0:
        try:
            return signal.Signals(-exitcode).name
        except ValueError:
            return f"SIGNAL_{-exitcode}"
    return f"EXIT_{exitcode}"


def _probe_child(conn: Any, factory: Callable[..., Callable[[], Any]], args: tuple) -> None:
    """Child loop: build the probe once, answer each request until the pipe closes."""

    probe = None
    while True:
        try:
            conn.recv()
        except (EOFError, OSError):
            return  # the parent is gone
        try:
            if probe is None:
                probe = factory(*args)
            conn.send(("ok", probe()))
        except Exception as error:  # noqa: BLE001 - reported to the parent as a typed failure
            conn.send(("error", f"{type(error).__name__}: {error}"[:300]))


class SupervisedProbe:
    """``probe()`` returns ``factory(*args)()`` evaluated in a spawned child.

    ``factory`` and ``args`` must be picklable (a module-level function). Raises
    ``ProbeUnavailable`` on crash (reason = signal name), timeout (``TIMEOUT``),
    or an exception inside the child (``CHILD_ERROR``).
    """

    def __init__(self, factory: Callable[..., Callable[[], Any]], args: tuple = (), *,
                 timeout_s: float, context: Any = None) -> None:
        if timeout_s <= 0:
            raise ValueError("supervised probe needs a positive timeout")
        self.factory, self.args, self.timeout_s = factory, tuple(args), float(timeout_s)
        self.context = context or multiprocessing.get_context("spawn")
        self.proc = self.conn = None
        self.restarts = 0
        self._started = False
        self._lock = threading.Lock()

    def _start(self) -> None:
        parent, child = self.context.Pipe()
        self.proc = self.context.Process(target=_probe_child, args=(child, self.factory, self.args),
                                         daemon=True, name="qdl-supervised-probe")
        self.proc.start()
        child.close()
        self.conn = parent
        if self._started:
            self.restarts += 1
        self._started = True

    def _stop(self, *, kill: bool) -> int | None:
        proc, conn, self.proc, self.conn = self.proc, self.conn, None, None
        if conn is not None:
            conn.close()
        if proc is None:
            return None
        if kill and proc.is_alive():
            proc.kill()
        proc.join(5)
        return proc.exitcode

    def close(self) -> None:
        with self._lock:
            self._stop(kill=True)

    def __call__(self) -> Any:
        with self._lock:
            if self.proc is not None and not self.proc.is_alive():
                self._stop(kill=True)
            if self.proc is None:
                self._start()
            deadline = time.monotonic() + self.timeout_s
            try:
                self.conn.send("probe")
                while True:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        code = self._stop(kill=True)
                        raise ProbeUnavailable("TIMEOUT", exitcode=code)
                    if self.conn.poll(min(left, 0.05)):
                        kind, value = self.conn.recv()
                        if kind == "ok":
                            return value
                        raise ProbeUnavailable("CHILD_ERROR", detail=str(value))
                    if not self.proc.is_alive():
                        # A last answer may still sit in the pipe after a clean exit.
                        if self.conn.poll(0):
                            continue
                        code = self._stop(kill=True)
                        raise ProbeUnavailable(exit_reason(code), exitcode=code)
            except (EOFError, BrokenPipeError, ConnectionResetError, OSError) as error:
                code = self._stop(kill=True)
                raise ProbeUnavailable(exit_reason(code), exitcode=code,
                                       detail=type(error).__name__) from None
