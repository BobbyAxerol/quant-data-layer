"""Bounded, thread-safe LRU for values derived from immutable rows (KN-4 D30).

Purpose: a Query replica derives the same projection of a market-cache row
(decoded envelope, lineage verdict, item fields, validated public views) for
every consumer that reads it. Those derivations depend only on the row's
content and the process's catalog, so they are cached here under a content
key (binding + canonical SHA-256). Anything that depends on time or on the
request - quality, freshness, eligibility, cursor, watermark - is never
stored: the callers rebuild it per request.

Boundary: entries are bounded by count (``max_entries``); the oldest are
evicted first; counters are exported for measurement. No I/O.
"""
from __future__ import annotations

from collections import OrderedDict
import threading
from typing import Any, Hashable

ROW_CACHE_ENTRIES_ENV = "QDL_KN_ROW_CACHE_ENTRIES"


class BoundedRowCache:
    def __init__(self, max_entries: int) -> None:
        if max_entries < 0:
            raise ValueError("row cache size cannot be negative")
        self.max_entries = max_entries
        self._entries: OrderedDict[Hashable, Any] = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def get(self, key: Hashable) -> Any | None:
        if self.max_entries == 0:
            return None
        with self._lock:
            value = self._entries.get(key)
            if value is None:
                self.misses += 1
                return None
            self._entries.move_to_end(key)
            self.hits += 1
            return value

    def put(self, key: Hashable, value: Any) -> None:
        if self.max_entries == 0:
            return
        with self._lock:
            self._entries[key] = value
            self._entries.move_to_end(key)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)
                self.evictions += 1

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "entries": len(self._entries),
                "max_entries": self.max_entries,
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
            }
