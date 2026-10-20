"""IDR-129: bounded memory maintenance for a Query replica (full collection + ``malloc_trim``, idle only).

Purpose: give back memory that the Query's own settings otherwise keep until restart. Measured 2026-10-19 on an
isolated replica of the production image under repeated 64-warmup bursts (``scripts/idr129_query_rss_isolated.py``):

* Cyclic garbage outlives the bursts. ``QUERY_GC_THRESHOLDS = (100_000, 50, 100)`` (``qdl/runtime/stable.py``,
  chosen to stop GC pauses) runs a generation-1 collection only after 50 generation-0 collections and a full one
  after 100 of those: production made 16 generation-0 and 0 generation-1 collections in 32 h. Closed client
  connections (uvicorn protocol, asyncio TLS transport and their buffers) are such cycles.
* glibc keeps freed arena memory: after the garbage was freed, ``malloc_trim(0)`` returned 150-350 MiB of RSS.

Policy (one variable, the target is RSS): when the replica's RSS is above a soft limit, the large-warmup lane has
been idle for ``idle_seconds`` and the last maintenance is at least ``min_interval_seconds`` old, run one
``gc.collect()`` and one ``malloc_trim(0)`` on the event-loop thread and record one ``qdl.stall`` line
(``kind=memory_maintenance``). Below the soft limit nothing runs, so a replica that does not accumulate pays no
pause. The thresholds and ``gc.freeze`` of start-up are unchanged.

Boundary: no I/O besides ``/proc/self/statm`` and the existing stall recorder; never raises into the loop.
"""
from __future__ import annotations

import asyncio
import ctypes
import gc
import logging
import os
import time
from typing import Callable

logger = logging.getLogger(__name__)

SOFT_LIMIT_ENV = "QDL_QUERY_MEMORY_SOFT_LIMIT_MIB"
DEFAULT_SOFT_LIMIT_MIB = 448
DEFAULT_MIN_INTERVAL_SECONDS = 120.0
DEFAULT_IDLE_SECONDS = 3.0
DEFAULT_TICK_SECONDS = 1.0


def _rss_mib() -> float | None:
    try:
        with open("/proc/self/statm", encoding="ascii") as handle:
            return int(handle.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 1048576.0
    except (OSError, ValueError, IndexError):
        return None


def _glibc_malloc_trim() -> Callable[[], int] | None:
    try:
        libc = ctypes.CDLL("libc.so.6")
        trim = libc.malloc_trim
    except (OSError, AttributeError):
        return None
    trim.argtypes = [ctypes.c_size_t]
    trim.restype = ctypes.c_int
    return lambda: int(trim(0))


def soft_limit_from_environment(environ=os.environ) -> float:
    raw = environ.get(SOFT_LIMIT_ENV, "").strip()
    if not raw:
        return float(DEFAULT_SOFT_LIMIT_MIB)
    value = float(raw)
    if value < 0:
        raise ValueError(f"{SOFT_LIMIT_ENV} must be >= 0 (0 disables maintenance)")
    return value


class MemoryMaintenance:
    """Decides and runs one maintenance step per ``tick`` (see the module docstring)."""

    def __init__(
        self,
        *,
        busy: Callable[[], bool],
        soft_limit_mib: float = DEFAULT_SOFT_LIMIT_MIB,
        min_interval_seconds: float = DEFAULT_MIN_INTERVAL_SECONDS,
        idle_seconds: float = DEFAULT_IDLE_SECONDS,
        record: Callable[[dict], None] | None = None,
        rss: Callable[[], float | None] = _rss_mib,
        collect: Callable[[], int] = gc.collect,
        trim: Callable[[], int] | None = None,
        clock: Callable[[], float] = time.monotonic,
        wall_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        if soft_limit_mib < 0 or min_interval_seconds < 0 or idle_seconds < 0:
            raise ValueError("memory maintenance bounds must be >= 0")
        self.soft_limit_mib = soft_limit_mib
        self.min_interval_seconds = min_interval_seconds
        self.idle_seconds = idle_seconds
        self._busy = busy
        self._record = record
        self._rss = rss
        self._collect = collect
        self._trim = trim if trim is not None else (_glibc_malloc_trim() or (lambda: 0))
        self._clock = clock
        self._wall_ms = wall_ms
        self._last_busy = clock()
        self._last_run: float | None = None
        self.runs = 0

    def tick(self) -> dict | None:
        """One decision; returns the record of a maintenance step or ``None``."""

        if self.soft_limit_mib == 0:
            return None
        now = self._clock()
        if self._busy():
            self._last_busy = now
            return None
        if now - self._last_busy < self.idle_seconds:
            return None
        if self._last_run is not None and now - self._last_run < self.min_interval_seconds:
            return None
        before = self._rss()
        if before is None or before < self.soft_limit_mib:
            return None
        self._last_run = now
        started = time.perf_counter()
        collected = self._collect()
        collected_at = time.perf_counter()
        self._trim()
        finished = time.perf_counter()
        self.runs += 1
        record = {
            "kind": "memory_maintenance", "rss_before_mib": round(before, 1),
            "rss_after_mib": None if (after := self._rss()) is None else round(after, 1),
            "collected": collected, "gc_ms": round((collected_at - started) * 1000.0, 3),
            "trim_ms": round((finished - collected_at) * 1000.0, 3),
            "soft_limit_mib": self.soft_limit_mib, "wall_ms": self._wall_ms(),
        }
        if self._record is not None:
            try:
                self._record(record)
            except Exception:  # telemetry never breaks maintenance
                pass
        return record


async def run_memory_maintenance(maintenance: MemoryMaintenance, *, tick_seconds: float = DEFAULT_TICK_SECONDS):
    """Tick on the event loop until cancelled (a collection holds the GIL on any thread)."""

    while True:
        await asyncio.sleep(tick_seconds)
        try:
            maintenance.tick()
        except Exception:  # pragma: no cover - defensive: maintenance never stops the replica
            logger.exception("query memory maintenance tick failed")


def install_query_memory_maintenance(app, service, *, recorder=None) -> MemoryMaintenance | None:
    """Start the maintenance task with the app; ``None`` when disabled (soft limit 0)."""

    soft_limit = soft_limit_from_environment()
    if soft_limit == 0:
        return None
    busy = getattr(service, "large_warmup_lane_busy", None)
    maintenance = MemoryMaintenance(
        busy=busy if callable(busy) else (lambda: False),
        soft_limit_mib=soft_limit,
        record=recorder.record if recorder is not None else None,
    )
    tasks: list[asyncio.Task] = []

    @app.on_event("startup")
    async def start_memory_maintenance() -> None:
        tasks.append(asyncio.get_running_loop().create_task(
            run_memory_maintenance(maintenance), name="qdl-memory-maintenance"))

    @app.on_event("shutdown")
    async def stop_memory_maintenance() -> None:
        for task in tasks:
            task.cancel()

    app.state.memory_maintenance = maintenance
    return maintenance
