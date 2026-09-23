"""Cooperative duty cycle for CPU-bound cold reads inside one Query process.

A 5,000-row warmup keeps a worker thread CPU-bound for seconds. Python's GIL
then hands the event loop at most one slice per switch interval, and a hot HTTP
request needs many: measured live on 2026-09-23 (v2.1.1 Phase-3), QUOTE
snapshots on the same replica went from p50 9.5 ms to p50 156 ms / max 1.44 s
during one 4.1 s cold warmup, with no disk read and no CPU throttling.

A thread that marks itself cold sleeps briefly after every slice of work, which
actually releases the GIL for that window. Hot threads never mark themselves,
so they never pause; a cold read that finishes within one slice never pauses.
"""

from __future__ import annotations

from contextlib import contextmanager
import threading
import time

COLD_SLICE_SECONDS = 0.004
COLD_PAUSE_SECONDS = 0.002

_state = threading.local()


@contextmanager
def cold_work():
    """Mark the current thread as doing cold work for the duration."""

    previous = getattr(_state, "since", None)
    _state.since = time.perf_counter()
    try:
        yield
    finally:
        _state.since = previous


def cold_yield() -> None:
    """Pause briefly if this cold thread has worked a whole slice; else no-op."""

    since = getattr(_state, "since", None)
    if since is None:
        return
    now = time.perf_counter()
    if now - since >= COLD_SLICE_SECONDS:
        time.sleep(COLD_PAUSE_SECONDS)
        _state.since = time.perf_counter()


def run_cold(work, /, *args, **kwargs):
    """Run ``work`` with this thread marked cold (for executor submission)."""

    with cold_work():
        return work(*args, **kwargs)
