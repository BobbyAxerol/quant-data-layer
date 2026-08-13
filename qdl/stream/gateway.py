from __future__ import annotations

import asyncio
from dataclasses import dataclass

from qdl.replay import GapFreeHandoff
from qdl.transport import DurableEvent, EventSink, StoredEvent


class SlowConsumer(RuntimeError):
    """The consumer must reconnect and replay from its last confirmed token."""


class StreamCapacityExceeded(RuntimeError):
    """The gateway cannot admit another bounded subscriber."""


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
    ) -> None:
        self._gateway = gateway
        self.subscription_id = subscription_id
        self.consumer_id = consumer_id
        self.token = token
        self.initial = initial
        self.queue: asyncio.Queue[StoredEvent] = asyncio.Queue(maxsize=max_buffer_events)
        self.overflowed = False
        self.closed = False

    def push(self, stored: StoredEvent) -> None:
        if self.closed or self.overflowed:
            return
        try:
            self.queue.put_nowait(stored)
        except asyncio.QueueFull:
            self.overflowed = True

    def record(self, stored: StoredEvent) -> StreamRecord:
        grant = self._gateway.handoff.advance_token(
            token=self.token,
            consumer_id=self.consumer_id,
            cursor=stored.cursor,
            ttl_seconds=self._gateway.cursor_ttl_seconds,
        )
        self.token = grant.token
        return StreamRecord(stored, grant.token)

    async def next_live(self) -> StreamRecord:
        if self.overflowed:
            raise SlowConsumer("bounded outbound buffer exhausted; replay is required")
        if self.closed:
            raise StopAsyncIteration
        stored = await self.queue.get()
        if self.overflowed:
            raise SlowConsumer("bounded outbound buffer exhausted; replay is required")
        return self.record(stored)

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
        cursor_ttl_seconds: int = 3_600,
    ) -> None:
        if max_subscribers <= 0:
            raise ValueError("max_subscribers must be positive")
        if not 1 <= max_buffer_events <= 10_000:
            raise ValueError("max_buffer_events must be between 1 and 10000")
        if cursor_ttl_seconds <= 0:
            raise ValueError("cursor_ttl_seconds must be positive")
        self.handoff = handoff
        self._sink = sink
        self.max_subscribers = max_subscribers
        self.max_buffer_events = max_buffer_events
        self.cursor_ttl_seconds = cursor_ttl_seconds
        self._lock = asyncio.Lock()
        self._next_id = 1
        self._subscriptions: dict[int, tuple[str, str, StreamSubscription]] = {}

    async def open(
        self,
        *,
        consumer_id: str,
        stream: str,
        partition_key: str,
        token: str,
        max_buffer_events: int | None = None,
        replay_limit: int = 10_000,
    ) -> StreamSubscription:
        buffer_size = max_buffer_events or self.max_buffer_events
        if not 1 <= buffer_size <= self.max_buffer_events:
            raise ValueError("requested stream buffer exceeds the server bound")
        async with self._lock:
            if len(self._subscriptions) >= self.max_subscribers:
                raise StreamCapacityExceeded("stream subscriber capacity exhausted")
            initial = tuple(self.handoff.replay(
                token=token,
                consumer_id=consumer_id,
                stream=stream,
                partition_key=partition_key,
                limit=replay_limit,
            ))
            subscription_id = self._next_id
            self._next_id += 1
            subscription = StreamSubscription(
                gateway=self,
                subscription_id=subscription_id,
                consumer_id=consumer_id,
                token=token,
                initial=initial,
                max_buffer_events=buffer_size,
            )
            self._subscriptions[subscription_id] = (stream, partition_key, subscription)
            return subscription

    async def publish(self, event: DurableEvent) -> StoredEvent | None:
        """Commit before delivery; duplicate durable events are not re-delivered."""

        async with self._lock:
            result = self._sink.append(event)
            if result.duplicate:
                return None
            stored = StoredEvent(event, result.cursor, result.committed_at_ns, result.payload_sha256)
            for stream, partition_key, subscription in self._subscriptions.values():
                if stream == event.stream and partition_key == event.partition_key:
                    subscription.push(stored)
            return stored

    async def close(self, subscription_id: int) -> None:
        async with self._lock:
            self._subscriptions.pop(subscription_id, None)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscriptions)
