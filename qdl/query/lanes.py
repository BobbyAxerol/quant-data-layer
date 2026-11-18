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


class ReadLaneAtCapacity(ReadLaneRejected):
    """The lane's finite queue cannot hold this request; nothing was queued.

    Raised at admission, before any wait: a count, byte or class reserve
    bound is full now but frees as queued work drains. A request larger than
    the whole byte budget, or whose deadline expired while queued (it has
    already waited), stays a plain ``ReadLaneRejected``.
    """


class ReadLaneBusy(ReadLaneRejected):
    """The lane would queue this request; the caller asked not to wait here.

    Raised only when ``run(..., refuse_if_queued=True)``: the request could
    not start at once (another job holds the active slot, an earlier waiter is
    ahead, or the busy lane's queue is full). Nothing was reserved or queued.
    """


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
    reserved_max_active_per_consumer: int | None = None
    reserved_max_pending_per_consumer: int | None = None
    reserved_pending_slots: int = 0
    non_reserved_pending_slots: int = 0

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
        if self.reserved_max_active_per_consumer is not None:
            if not self.reserved_consumer_id:
                raise ValueError("reserved consumer active bound requires a consumer identity")
            if not self.max_active_per_consumer <= self.reserved_max_active_per_consumer <= self.max_active:
                raise ValueError("reserved consumer active bound is invalid")
        if self.reserved_max_pending_per_consumer is not None:
            if not self.reserved_consumer_id:
                raise ValueError("reserved consumer pending bound requires a consumer identity")
            if not self.max_active <= self.reserved_max_pending_per_consumer <= self.max_pending:
                raise ValueError("reserved consumer pending bound is invalid")
        if not 0 <= self.reserved_pending_slots < self.max_pending:
            raise ValueError("reserved consumer pending reserve is invalid")
        if not 0 <= self.non_reserved_pending_slots < self.max_pending:
            raise ValueError("non-reserved consumer pending reserve is invalid")
        if (
            self.reserved_pending_slots or self.non_reserved_pending_slots
        ) and not self.reserved_consumer_id:
            raise ValueError("pending reserves require a consumer identity")


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
        self._busy_refusals = 0

    async def run(
        self,
        work: Callable[[], Awaitable[T]],
        *,
        consumer_id: str,
        reserved_bytes: int,
        wait_timeout_ms: int | None = None,
        refuse_if_queued: bool = False,
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
            if (
                refuse_if_queued
                and (self._active or self._waiting)
                and reserved_bytes <= self.policy.max_pending_bytes
                and not self._admittable(consumer_id, reserved_bytes)
            ):
                # A full queue on a busy lane is also "busy here": the request
                # could run on another replica, so say that instead of a plain
                # capacity refusal the client must not redirect.
                self._busy_refusals += 1
                raise ReadLaneBusy("read lane is busy; its queue is full")
            self._reject_if_unadmittable(consumer_id, reserved_bytes)
            entry = _Entry(consumer_id, reserved_bytes, self._sequence)
            self._sequence += 1
            self._waiting.append(entry)
            self._pending_by_consumer[consumer_id] = (
                self._pending_by_consumer.get(consumer_id, 0) + 1
            )
            self._pending_bytes += reserved_bytes
            if refuse_if_queued and not self._can_start(entry):
                # Admissible, but it would wait behind active or earlier work.
                # The caller can run it on another replica instead; withdraw
                # before any waiter is notified, so nothing observes it.
                self._waiting.remove(entry)
                self._withdraw(entry)
                self._busy_refusals += 1
                raise ReadLaneBusy("read lane is busy; the request would queue")
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
                    self._waiting.remove(entry)
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
            "busy_refusals": self._busy_refusals,
        }

    def _admittable(self, consumer_id: str, reserved_bytes: int) -> bool:
        """``_reject_if_unadmittable`` as a predicate, without counting."""

        pending = sum(self._pending_by_consumer.values())
        return (
            reserved_bytes <= self.policy.max_pending_bytes
            and pending < self._class_pending_limit(consumer_id)
            and self._pending_bytes + reserved_bytes <= self.policy.max_pending_bytes
            and self._pending_by_consumer.get(consumer_id, 0)
            < self._consumer_pending_limit(consumer_id)
        )

    def _reject_if_unadmittable(self, consumer_id: str, reserved_bytes: int) -> None:
        pending = sum(self._pending_by_consumer.values())
        if reserved_bytes > self.policy.max_pending_bytes:
            self._rejected += 1
            self._rejected_bytes += 1
            raise ReadLaneRejected("read lane request exceeds its byte reservation budget")
        if pending >= self._class_pending_limit(consumer_id):
            self._rejected += 1
            raise ReadLaneAtCapacity("read lane retains finite capacity for another consumer class")
        if self._pending_bytes + reserved_bytes > self.policy.max_pending_bytes:
            self._rejected += 1
            self._rejected_bytes += 1
            raise ReadLaneAtCapacity("read lane is at byte reservation capacity")
        if (
            self._pending_by_consumer.get(consumer_id, 0)
            >= self._consumer_pending_limit(consumer_id)
        ):
            self._rejected += 1
            raise ReadLaneAtCapacity("read lane consumer is at its finite pending bound")

    def _consumer_pending_limit(self, consumer_id: str) -> int:
        if (
            consumer_id == self.policy.reserved_consumer_id
            and self.policy.reserved_max_pending_per_consumer is not None
        ):
            return self.policy.reserved_max_pending_per_consumer
        return self.policy.max_pending_per_consumer

    def _class_pending_limit(self, consumer_id: str) -> int:
        """Keep one bounded queue slot for the absent consumer class.

        A critical identity can use idle capacity until an ordinary identity is
        present, but it cannot fill the whole finite queue and make a new alpha
        request unadmittable. The reciprocal reserve gives a later TS request a
        bounded entry point when only alphas have work. Once both classes have
        queued work, all declared capacity is usable and FIFO fairness decides
        service order.
        """

        reserved = self.policy.reserved_consumer_id
        if reserved is None:
            return self.policy.max_pending
        has_reserved = bool(self._pending_by_consumer.get(reserved, 0))
        has_non_reserved = any(
            identity != reserved and pending
            for identity, pending in self._pending_by_consumer.items()
        )
        if consumer_id == reserved and not has_non_reserved:
            return self.policy.max_pending - self.policy.non_reserved_pending_slots
        if consumer_id != reserved and not has_reserved:
            return self.policy.max_pending - self.policy.reserved_pending_slots
        return self.policy.max_pending

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
        reserved = self.policy.reserved_consumer_id
        reserved_head = heads.get(reserved) if reserved else None
        reserved_can_start = (
            reserved_head is not None
            and self._can_start_without_reservation(reserved_head, heads)
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
            if self._can_start_without_reservation(head, heads)
        ]
        if not eligible:
            return False
        return entry is min(eligible, key=lambda candidate: candidate.sequence)

    def _can_start_without_reservation(
        self, entry: _Entry, heads: dict[str, _Entry]
    ) -> bool:
        if self._active >= self.policy.max_active:
            return False
        active_for_consumer = self._active_by_consumer.get(entry.consumer_id, 0)
        if active_for_consumer < self.policy.max_active_per_consumer:
            return True
        # Only the explicitly reserved critical identity may borrow an idle
        # worker. Generic identities retain the hard fair-share cap even when
        # they arrive one event-loop turn before a peer, avoiding a race where
        # a bursty alpha could acquire every active worker ahead of another.
        borrowed_limit = self.policy.reserved_max_active_per_consumer
        return (
            entry.consumer_id == self.policy.reserved_consumer_id
            and borrowed_limit is not None
            and len(heads) == 1
            and active_for_consumer < borrowed_limit
        )

    def _non_reserved_active(self) -> int:
        reserved = self.policy.reserved_consumer_id
        return self._active - self._active_by_consumer.get(reserved or "", 0)


@dataclass(frozen=True, slots=True)
class FairLanePolicy:
    """A session-fair warmup queue (DL v2.4.0 S6).

    ``max_active`` bounds concurrent materializations (the memory bound: the
    large-warmup lane keeps 1 per replica). Waiting costs a coroutine, not
    rows, so the queue is bounded by count only; one request may still not
    reserve more than ``max_request_bytes``.
    """

    max_active: int
    max_pending: int
    max_pending_per_session: int
    max_pending_per_consumer: int
    max_request_bytes: int
    reserved_consumer_id: str | None = None
    reserved_pending_slots: int = 0
    reserved_max_active: int = 1
    initial_service_ms: int = 500
    retry_after_min_ms: int = 250
    retry_after_max_ms: int = 2_000
    abandon_poll_ms: int = 500

    def __post_init__(self) -> None:
        if not 1 <= self.max_active <= 64:
            raise ValueError("fair lane max_active must be between 1 and 64")
        if not self.max_active <= self.max_pending <= 8_192:
            raise ValueError("fair lane max_pending must include active work")
        if not 1 <= self.max_pending_per_session <= self.max_pending_per_consumer <= self.max_pending:
            raise ValueError("fair lane per-session/per-consumer bounds are invalid")
        if self.max_request_bytes < 1:
            raise ValueError("fair lane max_request_bytes must be positive")
        if not 0 <= self.reserved_pending_slots < self.max_pending:
            raise ValueError("fair lane reserved pending slots are invalid")
        if self.reserved_pending_slots and not self.reserved_consumer_id:
            raise ValueError("fair lane pending reserve requires a consumer identity")
        if not 1 <= self.reserved_max_active <= self.max_active:
            raise ValueError("fair lane reserved active bound is invalid")
        if not 0 < self.retry_after_min_ms <= self.retry_after_max_ms <= 2_000:
            raise ValueError("fair lane retry hint must stay within the SDK's 0..2000 ms")
        if not 50 <= self.abandon_poll_ms <= 10_000 or self.initial_service_ms < 1:
            raise ValueError("fair lane timing bounds are invalid")


@dataclass(slots=True)
class _FairEntry:
    consumer_id: str
    key: str
    explicit_session: bool
    reserved_bytes: int
    sequence: int
    round: int
    future: asyncio.Future
    started: bool = False
    started_at: float = 0.0


class FairWarmupLane:
    """Session-fair FIFO admission that waits up to each request's deadline (DL v2.4.0 S6).

    Ordering: a waiting Trading System request (the reserved identity) goes
    first when a slot frees; otherwise the entry with the lowest per-key round
    wins, then the oldest. A key's n-th queued request has round n, so a
    session's second warmup runs after every other session's first (session-
    fair FIFO). The key is the request's fairness key (``X-QDL-Session`` within
    its identity) or the identity itself. At most one request per key runs at
    a time (the reserved identity up to ``reserved_max_active``).

    Admission: refused at once only when a count bound is full (``ReadLaneAt
    Capacity``) or a request exceeds the byte budget; a waiter is refused when
    its own deadline passes, or withdrawn when ``abandoned()`` says its client
    went away. Every refusal carries ``retry_after_ms`` derived from the queue
    position and the measured service time. Dispatch is explicit (one O(n)
    scan per completion), never a broadcast to every waiter.
    """

    def __init__(self, policy: FairLanePolicy) -> None:
        self.policy = policy
        self._waiting: list[_FairEntry] = []
        self._active = 0
        self._active_by_key: dict[str, int] = {}
        self._pending_by_key: dict[str, int] = {}
        self._pending_by_consumer: dict[str, int] = {}
        self._rounds: dict[str, int] = {}
        self._pending_bytes = 0
        self._sequence = 0
        self._service_ms = float(policy.initial_service_ms)
        self._admitted = 0
        self._rejected = 0
        self._rejected_bytes = 0
        self._queue_wait_timeouts = 0
        self._busy_refusals = 0
        self._abandoned = 0
        self._max_waited_ms = 0.0

    # ------------------------------------------------------------ public

    async def run(
        self,
        work: Callable[[], Awaitable[T]],
        *,
        consumer_id: str,
        reserved_bytes: int,
        fairness_key: str | None = None,
        wait_timeout_ms: int | None = None,
        refuse_if_queued: bool = False,
        abandoned: Callable[[], Awaitable[bool]] | None = None,
    ) -> T:
        if not consumer_id.strip():
            raise ValueError("read lane consumer_id is required")
        if reserved_bytes < 1:
            raise ValueError("read lane reserved_bytes must be positive")
        if wait_timeout_ms is not None and wait_timeout_ms < 1:
            raise ValueError("read lane wait timeout must be positive")
        reserved = consumer_id == self.policy.reserved_consumer_id
        explicit = bool(fairness_key) and fairness_key != consumer_id and not reserved
        key = fairness_key if explicit else consumer_id
        loop = asyncio.get_running_loop()
        if reserved_bytes > self.policy.max_request_bytes:
            self._rejected += 1
            self._rejected_bytes += 1
            raise ReadLaneRejected("read lane request exceeds its byte reservation budget")
        if refuse_if_queued and not self._startable_now(key, reserved):
            self._busy_refusals += 1
            raise ReadLaneBusy("read lane is busy; the request would queue")
        self._reject_if_full(consumer_id, key, explicit, reserved)
        round_ = self._rounds.get(key, 0) + 1
        self._rounds[key] = round_
        entry = _FairEntry(
            consumer_id, key, explicit, reserved_bytes, self._sequence, round_, loop.create_future(),
        )
        self._sequence += 1
        self._waiting.append(entry)
        self._pending_by_key[key] = self._pending_by_key.get(key, 0) + 1
        self._pending_by_consumer[consumer_id] = self._pending_by_consumer.get(consumer_id, 0) + 1
        self._pending_bytes += reserved_bytes
        enqueued_at = loop.time()
        self._dispatch(loop)
        try:
            if not entry.started:
                await self._wait(entry, loop, wait_timeout_ms, abandoned)
        except BaseException:
            if entry.started:
                self._finish(entry, loop)
            else:
                self._withdraw_waiting(entry)
                self._dispatch(loop)
            raise
        self._max_waited_ms = max(self._max_waited_ms, (entry.started_at - enqueued_at) * 1_000)
        try:
            return await work()
        finally:
            self._finish(entry, loop)

    def stats(self) -> dict[str, int]:
        reserved = self.policy.reserved_consumer_id or ""
        return {
            "active": self._active,
            "pending": len(self._waiting) + self._active,
            "waiting": len(self._waiting),
            "pending_bytes": self._pending_bytes,
            "active_reserved": self._active_by_key.get(reserved, 0),
            "pending_reserved": self._pending_by_consumer.get(reserved, 0),
            "admitted": self._admitted,
            "rejected": self._rejected,
            "rejected_bytes": self._rejected_bytes,
            "queue_wait_timeouts": self._queue_wait_timeouts,
            "busy_refusals": self._busy_refusals,
            "abandoned": self._abandoned,
            "service_ms": int(self._service_ms),
            "max_waited_ms": int(self._max_waited_ms),
        }

    def retry_after_ms(self, position: int | None = None) -> int:
        ahead = len(self._waiting) if position is None else position
        estimate = (ahead // self.policy.max_active + 1) * self._service_ms
        return int(min(self.policy.retry_after_max_ms, max(self.policy.retry_after_min_ms, estimate)))

    # ------------------------------------------------------------ internals

    def _refusal(self, kind, message: str, position: int | None = None):
        error = kind(message)
        error.retry_after_ms = self.retry_after_ms(position)
        return error

    def _reject_if_full(self, consumer_id: str, key: str, explicit: bool, reserved: bool) -> None:
        total = len(self._waiting) + self._active
        limit = self.policy.max_pending - (0 if reserved else self.policy.reserved_pending_slots)
        if total >= limit:
            self._rejected += 1
            raise self._refusal(ReadLaneAtCapacity, "read lane queue is full")
        if not reserved and self._pending_by_consumer.get(consumer_id, 0) >= self.policy.max_pending_per_consumer:
            self._rejected += 1
            raise self._refusal(ReadLaneAtCapacity, "read lane consumer is at its finite pending bound")
        if explicit and self._pending_by_key.get(key, 0) >= self.policy.max_pending_per_session:
            self._rejected += 1
            raise self._refusal(ReadLaneAtCapacity, "read lane session is at its finite pending bound")

    def _key_limit(self, key: str) -> int:
        return self.policy.reserved_max_active if key == self.policy.reserved_consumer_id else 1

    def _startable_now(self, key: str, reserved: bool) -> bool:
        if self._active >= self.policy.max_active:
            return False
        if self._active_by_key.get(key, 0) >= self._key_limit(key):
            return False
        if reserved:
            return not any(item.key == key for item in self._waiting)
        # Anything already waiting that could run would be served first.
        return not any(
            self._active_by_key.get(item.key, 0) < self._key_limit(item.key) for item in self._waiting
        )

    def _dispatch(self, loop) -> None:
        reserved = self.policy.reserved_consumer_id
        while self._active < self.policy.max_active and self._waiting:
            candidate = None
            if reserved is not None and self._active_by_key.get(reserved, 0) < self.policy.reserved_max_active:
                candidate = next((item for item in self._waiting if item.key == reserved), None)
            if candidate is None:
                best = None
                for item in self._waiting:
                    if self._active_by_key.get(item.key, 0) >= self._key_limit(item.key):
                        continue
                    rank = (item.round, item.sequence)
                    if best is None or rank < best[0]:
                        best = (rank, item)
                candidate = None if best is None else best[1]
            if candidate is None:
                return
            self._waiting.remove(candidate)
            candidate.started = True
            candidate.started_at = loop.time()
            self._active += 1
            self._active_by_key[candidate.key] = self._active_by_key.get(candidate.key, 0) + 1
            self._admitted += 1
            if not candidate.future.done():
                candidate.future.set_result(None)

    def _position(self, entry: _FairEntry) -> int:
        rank = (entry.round, entry.sequence)
        return sum(1 for item in self._waiting if (item.round, item.sequence) < rank)

    async def _wait(self, entry, loop, wait_timeout_ms, abandoned) -> None:
        deadline = None if wait_timeout_ms is None else loop.time() + wait_timeout_ms / 1_000
        poll = self.policy.abandon_poll_ms / 1_000
        while not entry.started:
            timeout = None if deadline is None else deadline - loop.time()
            if timeout is not None and timeout <= 0:
                self._queue_wait_timeouts += 1
                position = self._position(entry)
                self._withdraw_waiting(entry)
                self._dispatch(loop)
                # Same detail text as BoundedReadLane (refusal-log detail
                # digests stay comparable); the position is in the hint.
                raise self._refusal(
                    ReadLaneRejected,
                    "read lane admission wait exceeded the declared deadline",
                    position,
                )
            if abandoned is not None:
                timeout = poll if timeout is None else min(timeout, poll)
            await asyncio.wait({entry.future}, timeout=timeout)
            if not entry.started and abandoned is not None and await abandoned():
                self._abandoned += 1
                self._withdraw_waiting(entry)
                self._dispatch(loop)
                raise ReadLaneRejected("read lane waiter withdrawn: the client disconnected")

    def _withdraw_waiting(self, entry: _FairEntry) -> None:
        if entry in self._waiting:
            self._waiting.remove(entry)
            self._release_counts(entry)

    def _release_counts(self, entry: _FairEntry) -> None:
        for counts, name in ((self._pending_by_key, entry.key), (self._pending_by_consumer, entry.consumer_id)):
            left = counts.get(name, 0) - 1
            if left > 0:
                counts[name] = left
            else:
                counts.pop(name, None)
        if entry.key not in self._pending_by_key:
            self._rounds.pop(entry.key, None)
        self._pending_bytes -= entry.reserved_bytes

    def _finish(self, entry: _FairEntry, loop) -> None:
        if not entry.started:
            return
        entry.started = False  # finish once
        self._active -= 1
        left = self._active_by_key.get(entry.key, 0) - 1
        if left > 0:
            self._active_by_key[entry.key] = left
        else:
            self._active_by_key.pop(entry.key, None)
        elapsed_ms = max(1.0, (loop.time() - entry.started_at) * 1_000)
        self._service_ms = 0.8 * self._service_ms + 0.2 * elapsed_ms
        self._release_counts(entry)
        self._dispatch(loop)
