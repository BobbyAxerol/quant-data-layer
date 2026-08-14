from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Generic, TypeVar

from qdl.ingestion.contracts import DeliveryPolicy


T = TypeVar("T")


@dataclass(frozen=True)
class QueueStats:
    size: int
    capacity: int
    high_watermark: int
    enqueued: int
    dequeued: int
    coalesced: int
    rejected: int
    enqueue_wait_ns: int


class FeedQueue(Generic[T]):
    """Bounded feed queue: lossless producers block; latest-state queues coalesce by key."""

    def __init__(self, *, capacity: int, policy: DeliveryPolicy):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._policy = policy
        self._queue: asyncio.Queue[tuple[str, T, DeliveryPolicy]] = asyncio.Queue(
            maxsize=capacity
        )
        self._latest: dict[str, T] = {}
        self._pending_keys: set[str] = set()
        self._high_watermark = 0
        self._enqueued = 0
        self._dequeued = 0
        self._coalesced = 0
        self._rejected = 0
        self._enqueue_wait_ns = 0

    async def put(
        self,
        key: str,
        value: T,
        *,
        policy: DeliveryPolicy | None = None,
    ) -> None:
        if not key.strip():
            raise ValueError("queue key is required")
        started = time.perf_counter_ns()
        effective_policy = policy or self._policy
        coalescing = effective_policy in {
            DeliveryPolicy.LATEST_STATE,
            DeliveryPolicy.LIFECYCLE_COALESCE,
        }
        if coalescing and key in self._pending_keys:
            self._latest[key] = value
            self._coalesced += 1
            return
        try:
            await self._queue.put((key, value, effective_policy))
        except asyncio.CancelledError:
            self._rejected += 1
            raise
        finally:
            self._enqueue_wait_ns += time.perf_counter_ns() - started
        if coalescing:
            self._pending_keys.add(key)
            self._latest[key] = value
        self._enqueued += 1
        self._high_watermark = max(self._high_watermark, self._queue.qsize())

    async def get(self) -> T:
        key, value, policy = await self._queue.get()
        if policy in {DeliveryPolicy.LATEST_STATE, DeliveryPolicy.LIFECYCLE_COALESCE}:
            value = self._latest.pop(key)
            self._pending_keys.remove(key)
        self._dequeued += 1
        return value

    def task_done(self) -> None:
        self._queue.task_done()

    def stats(self) -> QueueStats:
        return QueueStats(
            size=self._queue.qsize(),
            capacity=self._queue.maxsize,
            high_watermark=self._high_watermark,
            enqueued=self._enqueued,
            dequeued=self._dequeued,
            coalesced=self._coalesced,
            rejected=self._rejected,
            enqueue_wait_ns=self._enqueue_wait_ns,
        )
