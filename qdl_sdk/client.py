from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Protocol

from qdl_sdk.cursor import CursorCheckpoint, CursorStore, MemoryCursorStore
from qdl_sdk.errors import ContinuityError, CursorExpiredError, DataLayerError
from qdl_sdk.models import ControlEvent, DataRequirement, StreamEvent


class QueryTransport(Protocol):
    async def warmup(self, requirement: DataRequirement, *, consumer_id: str) -> dict: ...
    async def snapshot(self, requirement: DataRequirement, *, consumer_id: str) -> dict: ...
    async def close(self) -> None: ...


class StreamTransport(Protocol):
    def subscribe(
        self,
        requirement: DataRequirement,
        *,
        consumer_id: str,
        stream: str,
        partition_key: str,
        cursor_token: str,
        max_buffer_events: int,
    ): ...
    async def close(self) -> None: ...


class TelemetryRecorder(Protocol):
    def record(
        self, *, consumer_id: str, sdk_major: int, contract: str, cursor_offset: int
    ) -> None: ...


def _validate_query_payload(
    requirement: DataRequirement, payload: dict, *, warmup: bool
) -> None:
    rows = payload.get("data") if warmup else [payload.get("data")]
    if not isinstance(rows, list) or not rows or any(not isinstance(row, dict) for row in rows):
        raise ContinuityError("DATA_NOT_READY", "query response has no typed market-data rows")
    if warmup:
        if int(payload.get("count", -1)) != len(rows):
            raise ContinuityError("PARTIAL_RESULT", "warmup count does not match returned rows")
        if requirement.require_full_coverage and payload.get("coverage") != "FULL":
            raise ContinuityError("PARTIAL_RESULT", "warmup response is not full coverage")
    for row in rows:
        if row.get("instrument_uid") != requirement.instrument_uid:
            raise ContinuityError("CONFLICT", "query response instrument does not match requirement")
        if str(row.get("feed", "")).upper() != requirement.feed:
            raise ContinuityError("CONFLICT", "query response feed does not match requirement")
        if requirement.interval is not None and row.get("interval") != requirement.interval:
            raise ContinuityError("CONFLICT", "query response interval does not match requirement")
        quality = row.get("quality")
        if not isinstance(quality, dict):
            raise ContinuityError("DATA_NOT_READY", "query response has no quality metadata")
        state = str(quality.get("state", "")).upper()
        if quality.get("gap_open") and requirement.gap_policy in {"BLOCK", "PAUSE"}:
            raise ContinuityError("OPEN_SEQUENCE_GAP", "query response has an open gap")
        if state in {"STALE", "OFFLINE", "UNAVAILABLE"} and requirement.stale_policy in {
            "BLOCK", "PAUSE",
        }:
            raise ContinuityError("DATA_STALE", f"query response quality state is {state}")
        if requirement.consumer_grade == "EXECUTION" and not quality.get(
            "execution_eligible", False
        ):
            raise ContinuityError(
                "SOURCE_NON_AUTHORITATIVE",
                "execution-grade response is not execution eligible",
            )
        if requirement.feed == "BAR" and requirement.require_final_bars:
            market_payload = row.get("payload")
            if not isinstance(market_payload, dict) or not market_payload.get("is_final", False):
                raise ContinuityError("DATA_NOT_READY", "bar response is not final")


class WarmupStreamSession:
    def __init__(
        self,
        *,
        consumer_id: str,
        requirement: DataRequirement,
        warmup: dict,
        events,
        cursor_store: CursorStore,
        cursor_key: str,
        starting_offset: int,
        query_transport: QueryTransport,
        stream_transport: StreamTransport,
        stream: str,
        partition_key: str,
        max_buffer_events: int,
        max_reconnect_attempts: int,
        telemetry: TelemetryRecorder | None,
        state_restored: bool,
    ) -> None:
        self.consumer_id = consumer_id
        self.requirement = requirement
        self.warmup = warmup
        self._events = events
        self._cursor_store = cursor_store
        self._cursor_key = cursor_key
        self._last_seen_offset = starting_offset
        self._query_transport = query_transport
        self._stream_transport = stream_transport
        self._stream = stream
        self._partition_key = partition_key
        self._max_buffer_events = max_buffer_events
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_attempts = 0
        self._telemetry = telemetry
        self.state_restored = state_restored

    def __aiter__(self):
        return self

    async def __anext__(self) -> StreamEvent | ControlEvent:
        try:
            event = await self._events.__anext__()
        except CursorExpiredError:
            self.warmup = await self._fresh_snapshot()
            self._last_seen_offset = int(self.warmup.get("watermark_offset", 0))
            self._events = self._subscribe(
                str(self.warmup["stream_cursor"])
            )
            self._reconnect_attempts = 0
            return ControlEvent(
                "SNAPSHOT_REPLACED",
                "cursor retention expired; local state must be rebuilt from the attached snapshot",
                self.warmup,
            )
        except DataLayerError as error:
            if not error.retryable or self._reconnect_attempts >= self._max_reconnect_attempts:
                raise
            self._reconnect_attempts += 1
            await asyncio.sleep(min(0.1 * 2 ** (self._reconnect_attempts - 1), 2.0))
            checkpoint = self._cursor_store.load(self._cursor_key)
            if checkpoint is None:
                token = str(self.warmup["stream_cursor"])
                self._last_seen_offset = int(self.warmup.get("watermark_offset", 0))
            else:
                token = checkpoint.token
                self._last_seen_offset = checkpoint.offset
            self._events = self._subscribe(token)
            return ControlEvent(
                "RECONNECTED",
                "stream transport reconnected from the last confirmed cursor",
            )
        if event.logical_offset <= self._last_seen_offset:
            raise ContinuityError(
                "OPEN_SEQUENCE_GAP",
                f"non-monotonic stream offset {event.logical_offset} after {self._last_seen_offset}",
            )
        if event.logical_offset != self._last_seen_offset + 1:
            raise ContinuityError(
                "OPEN_SEQUENCE_GAP",
                f"expected offset {self._last_seen_offset + 1}, observed {event.logical_offset}",
            )
        self._last_seen_offset = event.logical_offset
        self._reconnect_attempts = 0
        return event

    def acknowledge(self, event: StreamEvent) -> None:
        if event.logical_offset > self._last_seen_offset:
            raise ValueError("cannot acknowledge an event that was not observed")
        self._cursor_store.save(
            self._cursor_key,
            CursorCheckpoint(event.resume_token, event.logical_offset),
        )
        if self._telemetry is not None:
            self._telemetry.record(
                consumer_id=self.consumer_id,
                sdk_major=2,
                contract="grpc:Subscribe",
                cursor_offset=event.logical_offset,
            )

    def _subscribe(self, token: str):
        return self._stream_transport.subscribe(
            self.requirement,
            consumer_id=self.consumer_id,
            stream=self._stream,
            partition_key=self._partition_key,
            cursor_token=token,
            max_buffer_events=self._max_buffer_events,
        ).__aiter__()

    async def _fresh_snapshot(self) -> dict:
        if self.requirement.warmup_limit > 0:
            return await self._query_transport.warmup(
                self.requirement, consumer_id=self.consumer_id
            )
        snapshot = await self._query_transport.snapshot(
            self.requirement, consumer_id=self.consumer_id
        )
        data = snapshot["data"]
        return {
            "schema": "qdl.marketdata.warmup.v2",
            "request_id": snapshot["request_id"],
            "snapshot_id": data.get("snapshot_id") or "latest-snapshot",
            "stream_cursor": data.get("cursor"),
            "watermark_offset": data.get("watermark_offset", 0),
            "coverage": "FULL",
            "count": 1,
            "data": [data],
        }


class AsyncDataLayerClient:
    def __init__(
        self,
        *,
        query_transport: QueryTransport,
        stream_transport: StreamTransport,
        consumer_id: str,
        cursor_store: CursorStore | None = None,
        max_buffer_events: int = 1000,
        max_reconnect_attempts: int = 5,
        telemetry: TelemetryRecorder | None = None,
    ) -> None:
        if not consumer_id.strip():
            raise ValueError("consumer_id is required")
        if not 1 <= max_buffer_events <= 10_000:
            raise ValueError("max_buffer_events must be between 1 and 10000")
        self.query_transport = query_transport
        self.stream_transport = stream_transport
        self.consumer_id = consumer_id
        self.cursor_store = cursor_store or MemoryCursorStore()
        self.max_buffer_events = max_buffer_events
        if max_reconnect_attempts < 0 or max_reconnect_attempts > 20:
            raise ValueError("max_reconnect_attempts must be between 0 and 20")
        self.max_reconnect_attempts = max_reconnect_attempts
        self.telemetry = telemetry

    @asynccontextmanager
    async def warmup_then_stream(
        self,
        requirement: DataRequirement,
        *,
        stream: str,
        partition_key: str,
        resume_restored_state: bool = False,
    ):
        cursor_key = self._cursor_key(requirement, stream, partition_key)
        checkpoint = self.cursor_store.load(cursor_key)
        if requirement.warmup_limit > 0:
            warmup = await self.query_transport.warmup(
                requirement, consumer_id=self.consumer_id
            )
        else:
            snapshot = await self.query_transport.snapshot(
                requirement, consumer_id=self.consumer_id
            )
            data = snapshot["data"]
            warmup = {
                "schema": "qdl.marketdata.warmup.v2",
                "request_id": snapshot["request_id"],
                "snapshot_id": data.get("snapshot_id") or "latest-snapshot",
                "stream_cursor": data.get("cursor"),
                "watermark_offset": data.get("watermark_offset", 0),
                "coverage": "FULL",
                "count": 1,
                "data": [data],
            }
        _validate_query_payload(requirement, warmup, warmup=True)
        snapshot_token = warmup.get("stream_cursor")
        snapshot_offset = int(warmup.get("watermark_offset", 0))
        if not snapshot_token:
            raise ContinuityError("CURSOR_INVALID", "warmup response has no signed stream cursor")
        if resume_restored_state and checkpoint is None:
            raise ContinuityError(
                "CURSOR_INVALID",
                "resume_restored_state requires an existing durable cursor checkpoint",
            )
        token = checkpoint.token if resume_restored_state else snapshot_token
        starting_offset = checkpoint.offset if resume_restored_state else snapshot_offset
        if self.telemetry is not None:
            self.telemetry.record(
                consumer_id=self.consumer_id,
                sdk_major=2,
                contract="/v2/market-data/warmup",
                cursor_offset=starting_offset,
            )
        events = self.stream_transport.subscribe(
            requirement,
            consumer_id=self.consumer_id,
            stream=stream,
            partition_key=partition_key,
            cursor_token=token,
            max_buffer_events=self.max_buffer_events,
        ).__aiter__()
        session = WarmupStreamSession(
            consumer_id=self.consumer_id,
            requirement=requirement,
            warmup=warmup,
            events=events,
            cursor_store=self.cursor_store,
            cursor_key=cursor_key,
            starting_offset=starting_offset,
            query_transport=self.query_transport,
            stream_transport=self.stream_transport,
            stream=stream,
            partition_key=partition_key,
            max_buffer_events=self.max_buffer_events,
            max_reconnect_attempts=self.max_reconnect_attempts,
            telemetry=self.telemetry,
            state_restored=resume_restored_state,
        )
        try:
            yield session
        finally:
            close = getattr(events, "aclose", None)
            if close is not None:
                await close()

    async def close(self) -> None:
        await self.stream_transport.close()
        await self.query_transport.close()

    def _cursor_key(self, requirement: DataRequirement, stream: str, partition_key: str) -> str:
        return "|".join((self.consumer_id, requirement.instrument_uid, requirement.feed, stream, partition_key))


class DataLayerClientV2:
    """Sync facade for scripts; async applications must use AsyncDataLayerClient."""

    def __init__(self, async_client: AsyncDataLayerClient):
        self.async_client = async_client

    def warmup(self, requirement: DataRequirement) -> dict:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.async_client.query_transport.warmup(
                    requirement, consumer_id=self.async_client.consumer_id
                )
            )
        raise RuntimeError("sync SDK cannot run inside an active event loop")
