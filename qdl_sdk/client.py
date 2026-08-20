from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Protocol

from pydantic import ValidationError

from qdl_sdk.cursor import CursorCheckpoint, CursorStore, MemoryCursorStore
from qdl_sdk.errors import ContinuityError, CursorExpiredError, DataLayerError
from qdl_sdk.models import (
    ControlEvent,
    DataRequirement,
    Feed,
    Grade,
    InstrumentPageResponse,
    InstrumentResponse,
    InstrumentView,
    SnapshotResponse,
    StreamEvent,
    WarmupResponse,
)


class QueryTransport(Protocol):
    async def warmup(self, requirement: DataRequirement, *, consumer_id: str) -> dict: ...
    async def snapshot(self, requirement: DataRequirement, *, consumer_id: str) -> dict: ...
    async def instruments(
        self,
        *,
        consumer_id: str,
        consumer_grade: Grade,
        cursor: str | None,
        limit: int,
    ) -> dict: ...
    async def instrument(
        self, identity: str, *, consumer_id: str, consumer_grade: Grade
    ) -> dict: ...
    async def close(self) -> None: ...


class StreamTransport(Protocol):
    def subscribe(
        self,
        requirement: DataRequirement,
        *,
        consumer_id: str,
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
) -> SnapshotResponse | WarmupResponse:
    try:
        response = (
            WarmupResponse.model_validate(payload)
            if warmup
            else SnapshotResponse.model_validate(payload)
        )
    except ValidationError as error:
        raise ContinuityError(
            "SCHEMA_NOT_SUPPORTED", "query response violates the typed V2 contract"
        ) from error
    rows = response.data if isinstance(response, WarmupResponse) else [response.data]
    if not rows:
        raise ContinuityError("DATA_NOT_READY", "query response has no market-data rows")
    if warmup:
        assert isinstance(response, WarmupResponse)
        if response.count != len(rows):
            raise ContinuityError("PARTIAL_RESULT", "warmup count does not match returned rows")
        if requirement.require_full_coverage and response.coverage != "FULL":
            raise ContinuityError("PARTIAL_RESULT", "warmup response is not full coverage")
    for index, row in enumerate(rows):
        is_tail = index == len(rows) - 1
        if row.instrument_uid != requirement.instrument_uid:
            raise ContinuityError("CONFLICT", "query response instrument does not match requirement")
        if row.feed.value != requirement.feed.value:
            raise ContinuityError("CONFLICT", "query response feed does not match requirement")
        if requirement.interval is not None and row.interval != requirement.interval:
            raise ContinuityError("CONFLICT", "query response interval does not match requirement")
        quality = row.quality
        if quality.policy_id != requirement.source_policy_id:
            raise ContinuityError(
                "CONFLICT", "query response source policy does not match requirement"
            )
        state = quality.state.upper()
        freshness_ms = quality.freshness_ms
        if (
            is_tail
            and state != "MARKET_CLOSED"
            and requirement.max_freshness_ms is not None
            and freshness_ms > requirement.max_freshness_ms
            and requirement.stale_policy.value in {"BLOCK", "PAUSE"}
        ):
            raise ContinuityError("DATA_STALE", "query response exceeds freshness policy")
        if quality.gap_open and requirement.gap_policy.value in {"BLOCK", "PAUSE"}:
            raise ContinuityError("OPEN_SEQUENCE_GAP", "query response has an open gap")
        if is_tail and state in {
            "STALE", "OFFLINE", "UNAVAILABLE"
        } and requirement.stale_policy.value in {
            "BLOCK", "PAUSE",
        }:
            raise ContinuityError("DATA_STALE", f"query response quality state is {state}")
        if (
            is_tail
            and requirement.consumer_grade is Grade.EXECUTION
            and not quality.execution_eligible
        ):
            raise ContinuityError(
                "SOURCE_NON_AUTHORITATIVE",
                "execution-grade response is not execution eligible",
            )
        if requirement.require_full_coverage and not quality.complete:
            raise ContinuityError("PARTIAL_RESULT", "query response quality is incomplete")
        if requirement.feed is Feed.BAR and requirement.require_final_bars:
            lifecycle = str(getattr(row.payload, "lifecycle", "")).upper()
            if lifecycle not in {"FINAL", "REVISED"}:
                raise ContinuityError("DATA_NOT_READY", "bar response is not final")
    return response


class WarmupStreamSession:
    def __init__(
        self,
        *,
        consumer_id: str,
        requirement: DataRequirement,
        warmup: WarmupResponse,
        events,
        cursor_store: CursorStore,
        cursor_key: str,
        starting_offset: int,
        query_transport: QueryTransport,
        stream_transport: StreamTransport,
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
            self._last_seen_offset = self.warmup.watermark_offset
            self._events = self._subscribe(
                self.warmup.stream_cursor
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
                token = self.warmup.stream_cursor
                self._last_seen_offset = self.warmup.watermark_offset
            else:
                token = checkpoint.token
                self._last_seen_offset = checkpoint.offset
            self._events = self._subscribe(token)
            return ControlEvent(
                "RECONNECTED",
                "stream transport reconnected from the last confirmed cursor",
            )
        if isinstance(event, ControlEvent):
            return event
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
            cursor_token=token,
            max_buffer_events=self._max_buffer_events,
        ).__aiter__()

    async def _fresh_snapshot(self) -> WarmupResponse:
        if self.requirement.warmup_limit > 0:
            payload = await self._query_transport.warmup(
                self.requirement, consumer_id=self.consumer_id
            )
            result = _validate_query_payload(self.requirement, payload, warmup=True)
            assert isinstance(result, WarmupResponse)
            return result
        snapshot_payload = await self._query_transport.snapshot(
            self.requirement, consumer_id=self.consumer_id
        )
        snapshot = _validate_query_payload(
            self.requirement, snapshot_payload, warmup=False
        )
        assert isinstance(snapshot, SnapshotResponse)
        if not snapshot.data.snapshot_id or not snapshot.data.cursor:
            raise ContinuityError(
                "CURSOR_INVALID", "snapshot response has no server-issued handoff state"
            )
        return WarmupResponse.model_validate({
            "schema": "qdl.marketdata.warmup.v2",
            "request_id": snapshot.request_id,
            "snapshot_id": snapshot.data.snapshot_id,
            "stream_cursor": snapshot.data.cursor,
            "watermark_offset": snapshot.data.watermark_offset,
            "data_as_of_ns": snapshot.data.observed_at_ns,
            "coverage": "FULL",
            "count": 1,
            "data": [snapshot.data.model_dump(mode="json")],
        })


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

    async def snapshot(self, requirement: DataRequirement) -> SnapshotResponse:
        payload = await self.query_transport.snapshot(
            requirement, consumer_id=self.consumer_id
        )
        response = _validate_query_payload(requirement, payload, warmup=False)
        assert isinstance(response, SnapshotResponse)
        self._record_query("/v2/market-data/snapshot", response)
        return response

    async def warmup(self, requirement: DataRequirement) -> WarmupResponse:
        payload = await self.query_transport.warmup(
            requirement, consumer_id=self.consumer_id
        )
        response = _validate_query_payload(requirement, payload, warmup=True)
        assert isinstance(response, WarmupResponse)
        self._record_query("/v2/market-data/warmup", response)
        return response

    async def instrument(
        self, identity: str, *, consumer_grade: Grade
    ) -> InstrumentResponse:
        payload = await self.query_transport.instrument(
            identity,
            consumer_id=self.consumer_id,
            consumer_grade=consumer_grade,
        )
        try:
            return InstrumentResponse.model_validate(payload)
        except ValidationError as error:
            raise ContinuityError(
                "SCHEMA_NOT_SUPPORTED",
                "instrument response violates the typed V2 contract",
            ) from error

    async def resolve_instrument(
        self,
        *,
        venue: str,
        product_type: str,
        native_symbol: str,
        consumer_grade: Grade,
        market: str | None = None,
        page_limit: int = 500,
        max_pages: int = 100,
    ) -> InstrumentView:
        if not isinstance(consumer_grade, Grade):
            raise TypeError("consumer_grade must use the typed SDK enum")
        if not 1 <= page_limit <= 500 or not 1 <= max_pages <= 100:
            raise ValueError("instrument resolver page bounds are invalid")
        expected = {
            "venue": venue.strip().upper(),
            "product_type": product_type.strip().upper(),
            "native_symbol": native_symbol.strip().upper(),
            "market": market.strip().upper() if market is not None else None,
        }
        if not all(expected[key] for key in ("venue", "product_type", "native_symbol")):
            raise ValueError("venue, product_type and native_symbol are required")
        cursor: str | None = None
        matches: list[InstrumentView] = []
        seen_cursors: set[str] = set()
        for _ in range(max_pages):
            payload = await self.query_transport.instruments(
                consumer_id=self.consumer_id,
                consumer_grade=consumer_grade,
                cursor=cursor,
                limit=page_limit,
            )
            try:
                page = InstrumentPageResponse.model_validate(payload)
            except ValidationError as error:
                raise ContinuityError(
                    "SCHEMA_NOT_SUPPORTED",
                    "instrument page violates the typed V2 contract",
                ) from error
            for item in page.items:
                if (
                    item.venue.upper() == expected["venue"]
                    and item.product_type.upper() == expected["product_type"]
                    and item.native_symbol.upper() == expected["native_symbol"]
                    and (
                        expected["market"] is None
                        or item.market.upper() == expected["market"]
                    )
                ):
                    matches.append(item)
            if page.next_cursor is None:
                break
            if page.next_cursor in seen_cursors:
                raise ContinuityError(
                    "CONFLICT", "instrument catalog returned a cursor cycle"
                )
            seen_cursors.add(page.next_cursor)
            cursor = page.next_cursor
        else:
            raise ContinuityError(
                "PARTIAL_RESULT", "instrument catalog exceeded bounded pagination"
            )
        active = [item for item in matches if item.status.upper() == "ACTIVE"]
        if len(active) == 1:
            return active[0]
        if not active:
            raise DataLayerError(
                "INSTRUMENT_NOT_FOUND",
                "no active V2 instrument matches the venue/product/native symbol",
                retryable=False,
            )
        raise ContinuityError(
            "CONFLICT", "instrument identity is ambiguous; specify market"
        )

    @asynccontextmanager
    async def warmup_then_stream(
        self,
        requirement: DataRequirement,
        *,
        resume_restored_state: bool = False,
    ):
        cursor_key = self._cursor_key(requirement)
        checkpoint = self.cursor_store.load(cursor_key)
        if requirement.warmup_limit > 0:
            raw_warmup = await self.query_transport.warmup(
                requirement, consumer_id=self.consumer_id
            )
            warmup = _validate_query_payload(requirement, raw_warmup, warmup=True)
            assert isinstance(warmup, WarmupResponse)
        else:
            raw_snapshot = await self.query_transport.snapshot(
                requirement, consumer_id=self.consumer_id
            )
            snapshot = _validate_query_payload(requirement, raw_snapshot, warmup=False)
            assert isinstance(snapshot, SnapshotResponse)
            data = snapshot.data
            if not data.snapshot_id or not data.cursor:
                raise ContinuityError(
                    "CURSOR_INVALID", "snapshot response has no server-issued handoff state"
                )
            warmup = WarmupResponse.model_validate({
                "schema": "qdl.marketdata.warmup.v2",
                "request_id": snapshot.request_id,
                "snapshot_id": data.snapshot_id,
                "stream_cursor": data.cursor,
                "watermark_offset": data.watermark_offset,
                "data_as_of_ns": data.observed_at_ns,
                "coverage": "FULL",
                "count": 1,
                "data": [data.model_dump(mode="json")],
            })
        snapshot_token = warmup.stream_cursor
        snapshot_offset = warmup.watermark_offset
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

    def _cursor_key(self, requirement: DataRequirement) -> str:
        return "|".join((
            self.consumer_id,
            requirement.instrument_uid,
            requirement.feed.value,
            requirement.interval or "",
            requirement.source_policy_id,
        ))

    def _record_query(
        self, contract: str, payload: SnapshotResponse | WarmupResponse
    ) -> None:
        if self.telemetry is not None:
            watermark = (
                payload.data.watermark_offset
                if isinstance(payload, SnapshotResponse)
                else payload.watermark_offset
            )
            self.telemetry.record(
                consumer_id=self.consumer_id,
                sdk_major=2,
                contract=contract,
                cursor_offset=int(watermark),
            )


class DataLayerClientV2:
    """Sync facade for scripts; async applications must use AsyncDataLayerClient."""

    def __init__(self, async_client: AsyncDataLayerClient):
        self.async_client = async_client

    def warmup(self, requirement: DataRequirement) -> WarmupResponse:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.async_client.warmup(requirement))
        raise RuntimeError("sync SDK cannot run inside an active event loop")

    def snapshot(self, requirement: DataRequirement) -> SnapshotResponse:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.async_client.snapshot(requirement))
        raise RuntimeError("sync SDK cannot run inside an active event loop")
