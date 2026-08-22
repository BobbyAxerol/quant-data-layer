"""Share a fetched closed-bar window between consumers asking for the same one.

Without this, every consumer request reaches the venue, and a wide universe
cannot stay inside a rate limit. V1 amortises the same way, which is why it can
serve hundreds of symbols today.

Correctness rests on one fact: a closed bar is immutable. The cache is therefore
keyed on the identity of the window, including the closed-bar boundary it was
fetched for, so a window can never be served into a later bar period. When the
boundary moves, every entry for that series is unreachable by construction
rather than by expiry.

**Bounded limitation.** A provider correction to an already closed bar is not
seen until the next period, because the window is not re-fetched within one. A
consumer that must observe corrections immediately needs the materialised path,
which carries revisions as append-only events.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any

_DEFAULT_MAX_ENTRIES = 512


class ClosedBarWindowCache:
    def __init__(self, *, max_entries: int = _DEFAULT_MAX_ENTRIES) -> None:
        if max_entries < 1:
            raise ValueError("closed-bar cache must hold at least one window")
        self.max_entries = max_entries
        self._entries: OrderedDict[tuple[str, str, int], tuple[Any, ...]] = OrderedDict()
        self._lock = threading.RLock()
        self.hits = 0
        self.misses = 0

    @staticmethod
    def _key(instrument_uid: str, interval: str, boundary_ms: int):
        return (instrument_uid, interval, int(boundary_ms))

    def get(
        self, instrument_uid: str, interval: str, boundary_ms: int, limit: int
    ) -> tuple[Any, ...] | None:
        """Return the newest `limit` bars of a cached window, or None.

        A cached window shorter than the request is a miss: serving fewer rows
        than asked for would be a silently short answer.
        """
        if limit < 1:
            return None
        key = self._key(instrument_uid, interval, boundary_ms)
        with self._lock:
            window = self._entries.get(key)
            if window is None or len(window) < limit:
                self.misses += 1
                return None
            self._entries.move_to_end(key)
            self.hits += 1
            return tuple(window[-limit:])

    def put(
        self,
        instrument_uid: str,
        interval: str,
        boundary_ms: int,
        window: tuple[Any, ...],
    ) -> None:
        if not window:
            return
        key = self._key(instrument_uid, interval, boundary_ms)
        with self._lock:
            existing = self._entries.get(key)
            # Keep the longest window seen for this boundary so a large request
            # also satisfies the small ones that follow it.
            if existing is not None and len(existing) >= len(window):
                self._entries.move_to_end(key)
                return
            self._entries[key] = tuple(window)
            self._entries.move_to_end(key)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "entries": len(self._entries),
                "max_entries": self.max_entries,
                "hits": self.hits,
                "misses": self.misses,
            }
