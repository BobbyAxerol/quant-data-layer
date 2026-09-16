from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Callable, Protocol

from qdl.replay import GapFreeHandoff
from qdl.transport import (
    BatchEventSink,
    CursorExpired,
    DurableEvent,
    EventSink,
    StoredEvent,
)


# DL-V2 R1.8. How many records a latest-state subscription may hold. A deep
# buffer is not just unnecessary for a feed whose contract is "the newest
# value", it is actively harmful: a thousand queued quotes are seconds of
# waiting, and a consumer that also demands a two second event age will reject
# every one of them on the way out and receive nothing. Eight records is tens
# of milliseconds on the busiest instrument, so a record cannot age out while
# it waits. Lossless feeds are untouched and keep the buffer they asked for.
LATEST_STATE_BUFFER_EVENTS = 8


class SlowConsumer(RuntimeError):
    """The consumer must reconnect and replay from its last confirmed token."""


class StreamCapacityExceeded(RuntimeError):
    """The gateway cannot admit another bounded subscriber."""


class StreamAuthority(Protocol):
    @property
    def current_epoch(self) -> int | None: ...

    def assert_active(self, expected_epoch: int | None = None) -> int: ...


@dataclass(frozen=True)
class StreamRecord:
    stored: StoredEvent
    resume_token: str


class StreamSubscription:
    def __init__(
        self,
        *,
        gateway: "DurableStreamGateway",
        subscription_id: int,
        consumer_id: str,
        token: str,
        initial: tuple[StoredEvent, ...],
        max_buffer_events: int,
        lease_epoch: int | None,
        accepts: Callable[[StoredEvent], bool] | None,
        coalesce: bool = False,
    ) -> None:
        self._gateway = gateway
        self.subscription_id = subscription_id
        self.consumer_id = consumer_id
        self.token = token
        self.initial = initial
        depth = (
            min(max_buffer_events, LATEST_STATE_BUFFER_EVENTS)
            if coalesce
            else max_buffer_events
        )
        self.queue: asyncio.Queue[StoredEvent] = asyncio.Queue(maxsize=depth)
        self.lease_epoch = lease_epoch
        self._accepts = accepts or (lambda _stored: True)
        # DL-V2 R1.8. A latest-state feed keeps the newest record when the
        # bounded buffer is full, instead of keeping the oldest and declaring
        # the consumer slow. See `push` for why that matters.
        self._coalesce = bool(coalesce)
        self.overflowed = False
        self.closed = False
        self._in_flight = 0
        self.coalesced = 0

    def push(self, stored: StoredEvent) -> None:
        if self.closed or self.overflowed or not self._accepts(stored):
            return
        if self.queue.qsize() + self._in_flight >= self.queue.maxsize:
            if not self._coalesce:
                self.overflowed = True
                return
            # DL-V2 R1.8. A latest-state feed, a bounded FIFO buffer and a
            # strict event-age predicate combine into a livelock that starves
            # exactly the busiest instruments: the queue fills with records
            # that are still fresh when queued, every one of them ages past the
            # consumer's own freshness bound while it waits, and the reader
            # then rejects all of them and receives nothing. Observed on
            # BTCUSDT and ETH-USDT-SWAP quotes, which went 449 s and 1,648 s
            # without a delivered batch while a quiet DOGE quote was fine.
            #
            # `qdl/ingestion/contracts.py:delivery_policy` already says a
            # non-lossless feed is LATEST_STATE, so keeping the newest record
            # is the declared contract, not a relaxation of it. Trades, book
            # deltas and final bars are lossless and never take this path.
            try:
                self.queue.get_nowait()
                self.queue.task_done()
                self.coalesced += 1
            except asyncio.QueueEmpty:
                # Every slot is in flight rather than queued; there is nothing
                # to drop, so fall back to the bounded-buffer signal.
                self.overflowed = True
                return
        try:
            self.queue.put_nowait(stored)
        except asyncio.QueueFull:
            self.overflowed = True

    def accepts(self, stored: StoredEvent) -> bool:
        """Return whether a physical-partition event belongs to this consumer."""

        return self._accepts(stored)

    async def record(self, stored: StoredEvent) -> StreamRecord:
        self._gateway.assert_active(self.lease_epoch)
        grant = await self._gateway.advance_token(
            token=self.token,
            consumer_id=self.consumer_id,
            cursor=stored.cursor,
        )
        self.token = grant.token
        return StreamRecord(stored, grant.token)

    async def next_live(self) -> StreamRecord:
        self._gateway.assert_active(self.lease_epoch)
        if self.overflowed:
            raise SlowConsumer("bounded outbound buffer exhausted; replay is required")
        if self.closed:
            raise StopAsyncIteration
        while True:
            stored = await self.queue.get()
            if self.overflowed:
                raise SlowConsumer("bounded outbound buffer exhausted; replay is required")
            self._in_flight += 1
            record = await self.record(stored)
            # A strict predicate can depend on time. A record that was fresh
            # when queued may age out while this bounded subscriber waits.
            # Advance its signed cursor as a filtered physical record, then
            # wait for the next eligible record instead of leaking stale data.
            if not self._accepts(stored):
                self.mark_delivered()
                continue
            return record

    def mark_delivered(self) -> None:
        if self._in_flight > 0:
            self._in_flight -= 1

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            await self._gateway.close(self.subscription_id)


class DurableStreamGateway:
    """Durable-first fan-out; one slow consumer never blocks ingestion or peers."""

    def __init__(
        self,
        *,
        handoff: GapFreeHandoff,
        sink: EventSink,
        max_subscribers: int = 10_000,
        max_buffer_events: int = 1_000,
        max_replay_events: int = 10_000,
        cursor_ttl_seconds: int = 3_600,
        authority: StreamAuthority | None = None,
    ) -> None:
        if max_subscribers <= 0:
            raise ValueError("max_subscribers must be positive")
        if not 1 <= max_buffer_events <= 10_000:
            raise ValueError("max_buffer_events must be between 1 and 10000")
        if not 1 <= max_replay_events <= 10_000:
            raise ValueError("max_replay_events must be between 1 and 10000")
        if cursor_ttl_seconds <= 0:
            raise ValueError("cursor_ttl_seconds must be positive")
        self.handoff = handoff
        self._sink = sink
        self.max_subscribers = max_subscribers
        self.max_buffer_events = max_buffer_events
        self.max_replay_events = max_replay_events
        self.cursor_ttl_seconds = cursor_ttl_seconds
        self.authority = authority
        self._subscriptions_lock = asyncio.Lock()
        self._partition_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._next_id = 1
        self._subscriptions: dict[int, tuple[str, str, StreamSubscription]] = {}
        # Durable high watermark per partition, learned from committed appends
        # in this process only, tagged with the writer-lease epoch that produced
        # it. Delivering a live record then costs no second read of the durable
        # store, which is the lock the ingest append holds through its fsync.
        # An entry from another epoch is never used: after a fence this process
        # is no longer the writer, so its knowledge is stale by definition and
        # the store is read instead.
        self._watermarks: dict[tuple[str, str], tuple[int | None, int]] = {}

    async def open(
        self,
        *,
        consumer_id: str,
        stream: str,
        partition_key: str,
        token: str,
        max_buffer_events: int | None = None,
        max_consumer_streams: int | None = None,
        replay_limit: int = 10_000,
        accepts: Callable[[StoredEvent], bool] | None = None,
        coalesce: bool = False,
    ) -> StreamSubscription:
        lease_epoch = self.assert_active()
        if not 1 <= replay_limit <= self.max_replay_events:
            raise ValueError("requested replay limit exceeds the server bound")
        buffer_size = max_buffer_events or self.max_buffer_events
        if not 1 <= buffer_size <= self.max_buffer_events:
            raise ValueError("requested stream buffer exceeds the server bound")
        consumer_limit = max_consumer_streams or self.max_subscribers
        if not 1 <= consumer_limit <= self.max_subscribers:
            raise ValueError("requested consumer stream limit exceeds the server bound")
        if accepts is not None and not callable(accepts):
            raise ValueError("subscription event matcher must be callable")
        partition_lock = self._partition_lock(stream, partition_key)
        async with partition_lock:
            self.assert_active(lease_epoch)
            initial = tuple(await asyncio.to_thread(
                self.handoff.replay,
                token=token,
                consumer_id=consumer_id,
                stream=stream,
                partition_key=partition_key,
                limit=replay_limit,
            ))
            if len(initial) == replay_limit:
                high_watermark = await asyncio.to_thread(
                    self.handoff.capture_watermark,
                    stream=stream,
                    partition_key=partition_key,
                )
                if initial[-1].cursor.offset < high_watermark.offset:
                    raise CursorExpired(
                        "replay backlog exceeds the bounded gateway window; "
                        "a fresh snapshot is required"
                    )
            self.assert_active(lease_epoch)
            # Register behind the same partition barrier as replay. A publish
            # cannot land between the replay watermark and live fan-out.
            async with self._subscriptions_lock:
                if len(self._subscriptions) >= self.max_subscribers:
                    raise StreamCapacityExceeded("stream subscriber capacity exhausted")
                consumer_streams = sum(
                    subscription.consumer_id == consumer_id
                    for _, _, subscription in self._subscriptions.values()
                )
                if consumer_streams >= consumer_limit:
                    raise StreamCapacityExceeded(
                        "consumer concurrent stream quota exhausted"
                    )
                subscription_id = self._next_id
                self._next_id += 1
                subscription = StreamSubscription(
                    gateway=self,
                    subscription_id=subscription_id,
                    consumer_id=consumer_id,
                    token=token,
                    initial=initial,
                    max_buffer_events=buffer_size,
                    lease_epoch=lease_epoch,
                    accepts=accepts,
                    coalesce=coalesce,
                )
                self._subscriptions[subscription_id] = (
                    stream, partition_key, subscription
                )
                return subscription

    async def publish(self, event: DurableEvent) -> StoredEvent | None:
        """Commit before delivery; duplicate durable events are not re-delivered."""

        return (await self.publish_many((event,)))[0]

    async def publish_many(
        self, events: tuple[DurableEvent, ...] | list[DurableEvent]
    ) -> tuple[StoredEvent | None, ...]:
        """Durably append one bounded batch before ordered live fan-out."""

        values = tuple(events)
        if not values:
            return ()
        lease_epoch = self.assert_active()
        partitions = sorted({(event.stream, event.partition_key) for event in values})
        async with AsyncExitStack() as stack:
            for stream, partition_key in partitions:
                await stack.enter_async_context(self._partition_lock(stream, partition_key))
            self.assert_active(lease_epoch)
            if isinstance(self._sink, BatchEventSink):
                results = await asyncio.to_thread(self._sink.append_many, list(values))
            else:
                results = [
                    await asyncio.to_thread(self._sink.append, event) for event in values
                ]
            self.assert_active(lease_epoch)
            if len(results) != len(values):
                raise RuntimeError("durable batch sink returned an invalid result count")
            stored_values = tuple(
                None
                if result.duplicate
                else StoredEvent(
                    event,
                    result.cursor,
                    result.committed_at_ns,
                    result.payload_sha256,
                )
                for event, result in zip(values, results, strict=True)
            )
            # The store committed before returning these offsets, so each one is
            # durable. Only ever move a partition's watermark forward: a
            # duplicate append reports the offset of the record that already
            # existed, which can be behind the current watermark.
            for result in results:
                key = (result.cursor.stream, result.cursor.partition_key)
                epoch, offset = self._watermarks.get(key, (lease_epoch, -1))
                if epoch != lease_epoch:
                    offset = -1
                self._watermarks[key] = (lease_epoch, max(offset, result.cursor.offset))
            async with self._subscriptions_lock:
                subscriptions = tuple(self._subscriptions.values())
            for event, stored in zip(values, stored_values, strict=True):
                if stored is None:
                    continue
                for stream, partition_key, subscription in subscriptions:
                    if stream == event.stream and partition_key == event.partition_key:
                        subscription.push(stored)
            return stored_values

    async def close(self, subscription_id: int) -> None:
        async with self._subscriptions_lock:
            self._subscriptions.pop(subscription_id, None)

    async def fence_all(self) -> None:
        # Losing the lease means another process may now append, so every
        # remembered watermark is stale. Dropping them returns the gateway to
        # reading the durable store.
        self._watermarks.clear()
        async with self._subscriptions_lock:
            subscriptions = tuple(self._subscriptions.values())
            self._subscriptions.clear()
        for _, _, subscription in subscriptions:
            subscription.closed = True

    def assert_active(self, expected_epoch: int | None = None) -> int | None:
        if self.authority is None:
            return None
        return self.authority.assert_active(expected_epoch)

    def _partition_lock(self, stream: str, partition_key: str) -> asyncio.Lock:
        key = (stream, partition_key)
        lock = self._partition_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._partition_locks[key] = lock
        return lock

    async def capture_watermark(self, *, stream: str, partition_key: str):
        lease_epoch = self.assert_active()
        cursor = await asyncio.to_thread(
            self.handoff.capture_watermark,
            stream=stream,
            partition_key=partition_key,
        )
        self.assert_active(lease_epoch)
        return cursor

    async def replay(
        self,
        *,
        token: str,
        consumer_id: str,
        stream: str,
        partition_key: str,
        limit: int,
    ) -> tuple[StoredEvent, ...]:
        if not 1 <= limit <= self.max_replay_events:
            raise ValueError("requested replay limit exceeds the server bound")
        lease_epoch = self.assert_active()
        records = tuple(await asyncio.to_thread(
            self.handoff.replay,
            token=token,
            consumer_id=consumer_id,
            stream=stream,
            partition_key=partition_key,
            limit=limit,
        ))
        self.assert_active(lease_epoch)
        return records

    def known_high_watermark(self, stream: str, partition_key: str, lease_epoch: int | None) -> int | None:
        """Return this writer's committed watermark, or None if it cannot know.

        None is the safe answer and means the durable store is read. An entry
        recorded under a different lease epoch is discarded rather than trusted,
        because after a fence another process owns the writer role.
        """

        entry = self._watermarks.get((stream, partition_key))
        if entry is None:
            return None
        epoch, offset = entry
        if epoch != lease_epoch or offset < 0:
            return None
        return offset

    async def advance_token(self, *, token: str, consumer_id: str, cursor):
        lease_epoch = self.assert_active()
        known = self.known_high_watermark(
            cursor.stream, cursor.partition_key, lease_epoch
        )
        if known is not None and cursor.offset > known:
            # The caller is acknowledging a record this process did not commit,
            # so its knowledge is incomplete. Fall back to the durable read
            # rather than sign a cursor the store may not back.
            known = None
        grant = await asyncio.to_thread(
            self.handoff.advance_token,
            token=token,
            consumer_id=consumer_id,
            cursor=cursor,
            ttl_seconds=self.cursor_ttl_seconds,
            known_high_watermark=known,
        )
        self.assert_active(lease_epoch)
        return grant

    @property
    def subscriber_count(self) -> int:
        return len(self._subscriptions)
