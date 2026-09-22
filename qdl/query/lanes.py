"""Bounded, fair request admission for Query read-plane work.

The data plane has materially different work classes: a latest snapshot is a
small cache read, while a warmup can retain and decode thousands of bars.  A
single process-wide executor makes those classes contend invisibly.  This
module keeps the admission boundary explicit without changing any public V2
endpoint, provider quota, or quality decision.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, TypeVar


T = TypeVar("T")


class ReadLaneRejected(RuntimeError):
    """A finite in-process read lane cannot accept more work."""


@dataclass(frozen=True, slots=True)
class ReadLanePolicy:
    """One bounded fair lane with an optional reserved consumer share."""

    max_active: int
    max_pending: int
    max_pending_bytes: int
    max_active_per_consumer: int = 1
    max_pending_per_consumer: int = 2
    reserved_consumer_id: str | None = None
    reserved_slots: int = 0

    def __post_init__(self) -> None:
        if not 1 <= self.max_active <= 64:
            raise ValueError("read lane max_active must be between 1 and 64")
        if not self.max_active <= self.max_pending <= 8_192:
            raise ValueError("read lane max_pending must include active work")
        if self.max_pending_bytes < 1:
            raise ValueError("read lane max_pending_bytes must be positive")
        if not 1 <= self.max_active_per_consumer <= self.max_active:
            raise ValueError("read lane per-consumer active bound is invalid")
        if not self.max_active_per_consumer <= self.max_pending_per_consumer <= self.max_pending:
            raise ValueError("read lane per-consumer pending bound is invalid")
        if not 0 <= self.reserved_slots < self.max_active:
            raise ValueError("read lane reserved slots must leave one general slot")
        if self.reserved_slots and not self.reserved_consumer_id:
            raise ValueError("read lane reserved slots require a consumer identity")


@dataclass(slots=True)
class _Entry:
    consumer_id: str
    reserved_bytes: int
    sequence: int


class BoundedReadLane:
    """FIFO-by-consumer admission with finite request and byte reservations.

    A waiting Trading System request receives its reserved share before another
    alpha can consume the final general slot.  The reservation is demand-driven:
    idle capacity remains available to ordinary consumers.  Within a consumer,
    only its oldest queued task may run; a single identity cannot fill every
    active slot while another identity waits.
    """

    def __init__(self, policy: ReadLanePolicy) -> None:
        self.policy = policy
        self._condition = asyncio.Condition()
        self._waiting: list[_Entry] = []
        self._active = 0
        self._active_by_consumer: dict[str, int] = {}
        self._pending_by_consumer: dict[str, int] = {}
        self._pending_bytes = 0
        self._sequence = 0
        self._admitted = 0
        self._rejected = 0
        self._rejected_bytes = 0
        self._queue_wait_timeouts = 0

    async def run(
        self,
        work: Callable[[], Awaitable[T]],
        *,
        consumer_id: str,
        reserved_bytes: int,
        wait_timeout_ms: int | None = None,
    ) -> T:
        if not consumer_id.strip():
            raise ValueError("read lane consumer_id is required")
        if reserved_bytes < 1:
            raise ValueError("read lane reserved_bytes must be positive")
        if wait_timeout_ms is not None and wait_timeout_ms < 1:
            raise ValueError("read lane wait timeout must be positive")

        loop = asyncio.get_running_loop()
        entry: _Entry | None = None
        active = False
        async with self._condition:
            self._reject_if_unadmittable(consumer_id, reserved_bytes)
            entry = _Entry(consumer_id, reserved_bytes, self._sequence)
            self._sequence += 1
            self._waiting.append(entry)
            self._pending_by_consumer[consumer_id] = (
                self._pending_by_consumer.get(consumer_id, 0) + 1
            )
            self._pending_bytes += reserved_bytes
            self._condition.notify_all()
            deadline = (
                loop.time() + wait_timeout_ms / 1_000
                if wait_timeout_ms is not None
                else None
            )
            try:
                while not self._can_start(entry):
                    if deadline is None:
                        await self._condition.wait()
                        continue
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        self._queue_wait_timeouts += 1
                        raise ReadLaneRejected(
                            "read lane admission wait exceeded the declared deadline"
                        )
                    try:
                        await asyncio.wait_for(self._condition.wait(), timeout=remaining)
                    except asyncio.TimeoutError as error:
                        self._queue_wait_timeouts += 1
                        raise ReadLaneRejected(
                            "read lane admission wait exceeded the declared deadline"
                        ) from error
                self._waiting.remove(entry)
                self._active += 1
                self._active_by_consumer[consumer_id] = (
                    self._active_by_consumer.get(consumer_id, 0) + 1
                )
                self._admitted += 1
                active = True
                self._condition.notify_all()
            except BaseException:
                if entry in self._waiting:
                    self._withdraw(entry)
                    self._condition.notify_all()
                raise

        try:
            return await work()
        finally:
            if active:
                async with self._condition:
                    self._active -= 1
                    active_for_consumer = self._active_by_consumer[consumer_id] - 1
                    if active_for_consumer:
                        self._active_by_consumer[consumer_id] = active_for_consumer
                    else:
                        self._active_by_consumer.pop(consumer_id, None)
                    self._withdraw(entry)
                    self._condition.notify_all()

    def stats(self) -> dict[str, int]:
        reserved = self.policy.reserved_consumer_id
        return {
            "active": self._active,
            "pending": sum(self._pending_by_consumer.values()),
            "pending_bytes": self._pending_bytes,
            "active_reserved": self._active_by_consumer.get(reserved or "", 0),
            "pending_reserved": self._pending_by_consumer.get(reserved or "", 0),
            "admitted": self._admitted,
            "rejected": self._rejected,
            "rejected_bytes": self._rejected_bytes,
            "queue_wait_timeouts": self._queue_wait_timeouts,
        }

    def _reject_if_unadmittable(self, consumer_id: str, reserved_bytes: int) -> None:
        pending = sum(self._pending_by_consumer.values())
        if reserved_bytes > self.policy.max_pending_bytes:
            self._rejected += 1
            self._rejected_bytes += 1
            raise ReadLaneRejected("read lane request exceeds its byte reservation budget")
        if pending >= self.policy.max_pending:
            self._rejected += 1
            raise ReadLaneRejected("read lane is at request capacity")
        if self._pending_bytes + reserved_bytes > self.policy.max_pending_bytes:
            self._rejected += 1
            self._rejected_bytes += 1
            raise ReadLaneRejected("read lane is at byte reservation capacity")
        if (
            self._pending_by_consumer.get(consumer_id, 0)
            >= self.policy.max_pending_per_consumer
        ):
            self._rejected += 1
            raise ReadLaneRejected("read lane consumer is at its finite pending bound")

    def _withdraw(self, entry: _Entry) -> None:
        current = self._pending_by_consumer[entry.consumer_id] - 1
        if current:
            self._pending_by_consumer[entry.consumer_id] = current
        else:
            self._pending_by_consumer.pop(entry.consumer_id, None)
        self._pending_bytes -= entry.reserved_bytes

    def _can_start(self, entry: _Entry) -> bool:
        if self._active >= self.policy.max_active:
            return False
        heads: dict[str, _Entry] = {}
        for queued in self._waiting:
            heads.setdefault(queued.consumer_id, queued)
        if heads.get(entry.consumer_id) is not entry:
            return False
        if (
            self._active_by_consumer.get(entry.consumer_id, 0)
            >= self.policy.max_active_per_consumer
        ):
            return False

        reserved = self.policy.reserved_consumer_id
        reserved_head = heads.get(reserved) if reserved else None
        reserved_can_start = (
            reserved_head is not None and self._can_start_without_reservation(reserved_head)
        )
        if (
            entry.consumer_id != reserved
            and reserved_can_start
            and self._non_reserved_active() >= self.policy.max_active - self.policy.reserved_slots
        ):
            return False

        if reserved_can_start:
            return entry is reserved_head

        eligible = [
            head
            for head in heads.values()
            if self._can_start_without_reservation(head)
        ]
        if not eligible:
            return False
        return entry is min(eligible, key=lambda candidate: candidate.sequence)

    def _can_start_without_reservation(self, entry: _Entry) -> bool:
        return (
            self._active < self.policy.max_active
            and self._active_by_consumer.get(entry.consumer_id, 0)
            < self.policy.max_active_per_consumer
        )

    def _non_reserved_active(self) -> int:
        reserved = self.policy.reserved_consumer_id
        return self._active - self._active_by_consumer.get(reserved or "", 0)
