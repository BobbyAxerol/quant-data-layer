"""Replica stall telemetry for the latency-first Python readers (P18.4 IDR-114).

Purpose: make a per-replica stall attributable on the replica itself. The final stage-100 run (2026-10-17 135719Z)
showed 0.3-1.1 s Query stalls every ~76 s per replica; the client sees only the outcome. Three cheap recorders feed
one bounded buffer, and one background writer turns the records into ``qdl.stall`` stderr lines:

    qdl.stall {"kind": "gc", "generation": 0, "duration_ms": 201.4, "collected": 37107, ...}
    qdl.stall {"kind": "loop_lag", "lag_ms": 412.0, ...}
    qdl.stall {"kind": "slow_request", "duration_ms": 655.1, "method": "POST", "path": "/v2/...", ...}

* ``gc``: a ``gc.callbacks`` hook for collections >= ``GC_PAUSE_LOG_THRESHOLD_SECONDS`` (generation, duration,
  collected, uncollectable, ``gc.get_count()`` after it, per-generation collection counts seen by the hook, RSS).
* ``loop_lag``: the asyncio loop's lateness on a 100 ms timer, >= ``LOOP_LAG_THRESHOLD_SECONDS``.
* ``slow_request``: an outermost ASGI wrapper (it sees the bounded-request admission wait too) for HTTP requests
  >= ``SLOW_REQUEST_THRESHOLD_SECONDS``: start wall time, response-start and end offsets, method, path, status.
  Time spent before the app is called (accept, TLS, a stalled loop) is client-minus-server, plus ``loop_lag``.

Every record carries ``wall_ms`` (epoch ms at the event's end) so it lines up with client latency outliers.

Boundary: observation only. Recorders never change thresholds, trigger or skip a collection, alter a request or
raise into their caller; they take two clock reads and, past a threshold, append one tuple to a bounded deque
(GIL-atomic, no lock). Formatting and the ``os.write`` to fd 2 happen on the writer thread, outside the collector
and the event loop. When the buffer is full the oldest record is dropped and counted (``dropped`` on the next line).
"""
from __future__ import annotations

import asyncio
from collections import deque
import gc
import json
import os
import threading
import time
from typing import Callable

GC_PAUSE_LOG_THRESHOLD_SECONDS = 0.010
LOOP_LAG_INTERVAL_SECONDS = 0.100
LOOP_LAG_THRESHOLD_SECONDS = 0.050
SLOW_REQUEST_THRESHOLD_SECONDS = 0.250
STALL_LOG_PREFIX = "qdl.stall"
STALL_BUFFER_RECORDS = 4096
STALL_FLUSH_SECONDS = 0.5

_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


def _rss_mib() -> float | None:
    try:
        with open("/proc/self/statm", "rb") as handle:
            resident_pages = int(handle.read().split()[1])
    except (OSError, ValueError, IndexError):
        return None
    return round(resident_pages * _PAGE_SIZE / 1048576, 1)


def _thread_count() -> int | None:
    try:
        return len(os.listdir("/proc/self/task"))
    except OSError:
        return None


def _stderr_line(line: str) -> None:
    os.write(2, line.encode("utf-8"))


class StallRecorder:
    """Bounded record buffer plus the writer that formats it off the hot path."""

    def __init__(self, *, capacity: int = STALL_BUFFER_RECORDS, write: Callable[[str], None] = _stderr_line) -> None:
        self._records: deque[dict] = deque(maxlen=capacity)
        self._write = write
        self._appended = 0
        self._flushed = 0
        self._writer: threading.Thread | None = None
        self._writer_guard = threading.Lock()

    def record(self, entry: dict) -> None:
        self._appended += 1
        self._records.append(entry)

    def flush(self) -> int:
        """Write every buffered record; returns the number written."""

        written = 0
        while True:
            try:
                entry = self._records.popleft()
            except IndexError:
                break
            written += 1
            self._flushed += 1
            dropped = self._appended - self._flushed - len(self._records)
            if dropped > 0:
                entry = {**entry, "dropped": dropped}
            try:
                self._write(f"{STALL_LOG_PREFIX} {json.dumps(entry, separators=(',', ':'))}\n")
            except Exception:  # telemetry never fails its process
                pass
        return written

    def start_writer(self, interval_seconds: float = STALL_FLUSH_SECONDS) -> None:
        with self._writer_guard:
            if self._writer is not None:
                return

            def run() -> None:
                while True:
                    time.sleep(interval_seconds)
                    self.flush()

            self._writer = threading.Thread(target=run, name="qdl-stall-telemetry", daemon=True)
            self._writer.start()


class GcPauseTelemetry:
    """A ``gc.callbacks`` entry: time every collection, record the long ones.

    CPython runs one collection at a time under the GIL, so one start timestamp per process is enough."""

    __slots__ = ("threshold_seconds", "_recorder", "_clock", "_wall_ms", "_rss", "_started", "_collections")

    def __init__(
        self,
        recorder: StallRecorder,
        *,
        threshold_seconds: float = GC_PAUSE_LOG_THRESHOLD_SECONDS,
        clock: Callable[[], float] = time.perf_counter,
        wall_ms: Callable[[], int] = _wall_ms,
        rss: Callable[[], float | None] = _rss_mib,
    ) -> None:
        if threshold_seconds < 0:
            raise ValueError("GC pause threshold cannot be negative")
        self.threshold_seconds = threshold_seconds
        self._recorder = recorder
        self._clock = clock
        self._wall_ms = wall_ms
        self._rss = rss
        self._started: float | None = None
        self._collections = [0, 0, 0]

    def __call__(self, phase: str, info: dict) -> None:
        if phase == "start":
            self._started = self._clock()
            return
        started, self._started = self._started, None
        if started is None:
            return
        duration = self._clock() - started
        generation = info.get("generation")
        try:
            self._collections[generation] += 1
        except (IndexError, TypeError):
            pass
        if duration < self.threshold_seconds:
            return
        try:
            self._recorder.record({
                "kind": "gc", "generation": generation, "duration_ms": round(duration * 1000.0, 3),
                "collected": info.get("collected"), "uncollectable": info.get("uncollectable"),
                "count": gc.get_count(), "collections": tuple(self._collections), "rss_mib": self._rss(),
                "wall_ms": self._wall_ms(),
            })
        except Exception:  # telemetry never breaks a collection
            pass


async def watch_loop_lag(
    recorder: StallRecorder,
    *,
    interval_seconds: float = LOOP_LAG_INTERVAL_SECONDS,
    threshold_seconds: float = LOOP_LAG_THRESHOLD_SECONDS,
    clock: Callable[[], float] = time.perf_counter,
) -> None:
    """Record the loop's lateness on a fixed timer; runs until cancelled."""

    while True:
        started = clock()
        await asyncio.sleep(interval_seconds)
        lag = clock() - started - interval_seconds
        if lag >= threshold_seconds:
            recorder.record({"kind": "loop_lag", "lag_ms": round(lag * 1000.0, 3), "threads": _thread_count(),
                             "rss_mib": _rss_mib(), "wall_ms": _wall_ms()})


class SlowRequestTelemetry:
    """Outermost pure-ASGI wrapper; records HTTP requests that took >= the threshold inside the app."""

    def __init__(self, app, *, recorder: StallRecorder, threshold_seconds: float = SLOW_REQUEST_THRESHOLD_SECONDS,
                 clock: Callable[[], float] = time.perf_counter, wall_ms: Callable[[], int] = _wall_ms) -> None:
        self.app = app
        self._recorder = recorder
        self._threshold = threshold_seconds
        self._clock = clock
        self._wall_ms = wall_ms

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        started = self._clock()
        start_wall_ms = self._wall_ms()
        response = [None, None]  # status, response-start offset

        async def observed_send(message):
            if message.get("type") == "http.response.start" and response[0] is None:
                response[0] = message.get("status")
                response[1] = self._clock() - started
            await send(message)

        try:
            await self.app(scope, receive, observed_send)
        finally:
            duration = self._clock() - started
            if duration >= self._threshold:
                try:
                    self._recorder.record({
                        "kind": "slow_request", "duration_ms": round(duration * 1000.0, 3),
                        "response_start_ms": None if response[1] is None else round(response[1] * 1000.0, 3),
                        "status": response[0], "method": scope.get("method"), "path": scope.get("path"),
                        "start_wall_ms": start_wall_ms, "wall_ms": self._wall_ms(),
                    })
                except Exception:
                    pass


_PROCESS_RECORDER: StallRecorder | None = None
_PROCESS_GUARD = threading.Lock()


def process_stall_recorder() -> StallRecorder:
    """The process's single recorder, with its writer thread started."""

    global _PROCESS_RECORDER
    with _PROCESS_GUARD:
        if _PROCESS_RECORDER is None:
            _PROCESS_RECORDER = StallRecorder()
        _PROCESS_RECORDER.start_writer()
        return _PROCESS_RECORDER


def install_gc_pause_telemetry(threshold_seconds: float = GC_PAUSE_LOG_THRESHOLD_SECONDS) -> GcPauseTelemetry:
    """Register the GC hook once per process; a second call returns the first."""

    for callback in gc.callbacks:
        if isinstance(callback, GcPauseTelemetry):
            return callback
    telemetry = GcPauseTelemetry(process_stall_recorder(), threshold_seconds=threshold_seconds)
    gc.callbacks.append(telemetry)
    return telemetry


def install_request_stall_telemetry(app) -> None:
    """Query HTTP app: the slow-request wrapper (outermost user middleware) and the loop-lag watcher."""

    recorder = process_stall_recorder()
    app.add_middleware(SlowRequestTelemetry, recorder=recorder)
    tasks: list[asyncio.Task] = []

    @app.on_event("startup")
    async def start_loop_lag_watch() -> None:
        tasks.append(asyncio.get_running_loop().create_task(watch_loop_lag(recorder), name="qdl-loop-lag"))

    @app.on_event("shutdown")
    async def stop_loop_lag_watch() -> None:
        for task in tasks:
            task.cancel()
