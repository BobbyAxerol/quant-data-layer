"""Bounded per-consumer Query request counts, one structured line per window (DL-BL-2026-11-25-D).

Purpose: the DL load gate (plan journal, "DL release decision 2026-11-25") must see request growth per alpha
when more alphas go on Paper. Query had no per-consumer counter: the only per-consumer figure was the hashed
Redis quota key. Each authenticated data-plane request now increments a (consumer_id, route template) counter;
at the first request after a window ends, one JSON line ``qdl_consumer_requests`` reports the finished window.

Boundaries:
* observation only: no refusal, status, quota or fairness decision changes;
* payload-free: the consumer id (already in the signed manifest and the quota log), the route *template*
  (``/v2/market-data/{instrument_uid}/snapshot``, never the uid) and counts;
* bounded memory and log volume: at most ``max_consumers`` x ``max_routes`` keys per window (excess goes to the
  ``__overflow__`` consumer/route), and one line per window per replica (default 60 s);
* WARNING level on logger ``qdl.consumer_requests``, like ``qdl.refusal`` (no handler is installed by the Query
  role, so the line reaches stderr through the logging last-resort handler).
"""
from __future__ import annotations

import json
import logging
import socket
import threading
import time
from typing import Callable

logger = logging.getLogger("qdl.consumer_requests")
OVERFLOW = "__overflow__"


class ConsumerRequestLog:
    def __init__(self, *, window_seconds: float = 60.0, max_consumers: int = 64, max_routes: int = 24,
                 clock: Callable[[], float] = time.time, emit: Callable[[str], None] | None = None) -> None:
        if window_seconds <= 0 or max_consumers < 1 or max_routes < 1:
            raise ValueError("consumer request log bounds must be positive")
        self._window = float(window_seconds)
        self._max_consumers, self._max_routes = max_consumers, max_routes
        self._clock = clock
        self._emit = emit or (lambda line: logger.warning(line))
        self._lock = threading.Lock()
        self._window_start = self._floor(clock())
        self._counts: dict[str, dict[str, int]] = {}
        self._replica = socket.gethostname()

    def _floor(self, now: float) -> float:
        return now - (now % self._window)

    def record(self, consumer_id: str, route: str) -> None:
        now = self._clock()
        line = None
        with self._lock:
            if now - self._window_start >= self._window:
                line = self._render_locked()
                self._counts = {}
                self._window_start = self._floor(now)
            consumer = consumer_id if (consumer_id in self._counts or len(self._counts) < self._max_consumers) \
                else OVERFLOW
            routes = self._counts.setdefault(consumer, {})
            key = route if (route in routes or len(routes) < self._max_routes) else OVERFLOW
            routes[key] = routes.get(key, 0) + 1
        if line is not None:
            self._emit(line)

    def flush(self) -> str | None:
        """Render (and clear) the current window, for shutdown and tests."""

        with self._lock:
            line = self._render_locked()
            self._counts = {}
            self._window_start = self._floor(self._clock())
        return line

    def _render_locked(self) -> str | None:
        if not self._counts:
            return None
        return json.dumps({
            "event": "qdl_consumer_requests", "replica": self._replica,
            "window_start_s": int(self._window_start), "window_s": int(self._window),
            "total": sum(sum(routes.values()) for routes in self._counts.values()),
            "consumers": {consumer: dict(sorted(routes.items())) for consumer, routes in sorted(self._counts.items())},
        }, sort_keys=True, separators=(",", ":"))


_DEFAULT = ConsumerRequestLog()


def record_consumer_request(consumer_id: str, route: str) -> None:
    """Process-wide entry point used by the V2 router (never raises)."""

    try:
        _DEFAULT.record(consumer_id, route)
    except Exception:  # observation must never fail a request
        pass
