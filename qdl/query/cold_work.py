"""Cooperative duty cycle for CPU-bound cold reads inside one Query process.

A 5,000-row warmup keeps a worker thread CPU-bound for seconds. Python's GIL
then hands the event loop at most one slice per switch interval, and a hot HTTP
request needs many: measured live on 2026-09-23 (v2.1.1 Phase-3), QUOTE
snapshots on the same replica went from p50 9.5 ms to p50 156 ms / max 1.44 s
during one 4.1 s cold warmup, with no disk read and no CPU throttling.

A thread that marks itself cold sleeps briefly after every slice of work, which
actually releases the GIL for that window. Hot threads never mark themselves,
so they never pause; a cold read that finishes within one slice never pauses.

Cancellation (KN-4 K4.2/K4-T06): a thread cannot be interrupted, so a caller
whose request was cancelled or timed out must not release its admission
permit while the worker still runs. ``await_in_thread`` keeps the awaiting
task until the worker has returned, and asks a cold worker to stop at its
next ``cold_yield`` (``ColdWorkCancelled``) so the wait is one slice long.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from functools import partial
import threading
import time

COLD_SLICE_SECONDS = 0.004
COLD_PAUSE_SECONDS = 0.002

_state = threading.local()


class ColdWorkCancelled(BaseException):
    """The request that owns this cold work was cancelled; stop now.

    A ``BaseException`` like ``asyncio.CancelledError``: per-item ``except
    Exception`` outcomes inside a batch must not turn it into an item error
    and keep working."""


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
    cancelled = getattr(_state, "cancelled", None)
    if cancelled is not None and cancelled.is_set():
        raise ColdWorkCancelled("the request that owns this cold work was cancelled")
    now = time.perf_counter()
    if now - since >= COLD_SLICE_SECONDS:
        time.sleep(COLD_PAUSE_SECONDS)
        _state.since = time.perf_counter()


def run_cold(work, /, *args, **kwargs):
    """Run ``work`` with this thread marked cold (for executor submission)."""

    with cold_work():
        return work(*args, **kwargs)


def _run_cancellable(cancelled: threading.Event, cold: bool, work, /, *args, **kwargs):
    previous = getattr(_state, "cancelled", None)
    _state.cancelled = cancelled
    try:
        if cold:
            with cold_work():
                return work(*args, **kwargs)
        return work(*args, **kwargs)
    finally:
        _state.cancelled = previous


async def await_in_thread(executor, work, /, *args, cold: bool = False, **kwargs):
    """Run ``work`` on ``executor`` and hold the caller until it has returned.

    On cancellation the worker is asked to stop (a cold worker raises at its
    next ``cold_yield``); the caller waits for the thread to finish before
    the cancellation propagates, so every permit or lease the caller holds
    stays held while the thread runs. A second cancellation during that wait
    does not abandon the thread either.
    """

    loop = asyncio.get_running_loop()
    cancelled = threading.Event()
    future = loop.run_in_executor(
        executor, partial(_run_cancellable, cancelled, cold, work, *args, **kwargs)
    )
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        cancelled.set()
        while not future.done():
            try:
                await asyncio.wait({future})
            except asyncio.CancelledError:
                continue
        if not future.cancelled():
            future.exception()  # retrieved: the request is gone, its error with it
        raise
