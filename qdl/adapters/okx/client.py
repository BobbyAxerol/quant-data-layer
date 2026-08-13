from __future__ import annotations

import asyncio
import json
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

import requests


OKX_REST_BASE = "https://www.okx.com"
OKX_PUBLIC_WS = "wss://ws.okx.com:8443/ws/v5/public"
OKX_BUSINESS_WS = "wss://ws.okx.com:8443/ws/v5/business"


class BookState(str, Enum):
    SYNCING = "SYNCING"
    LIVE = "LIVE"
    GAPPED = "GAPPED"
    INVALID = "INVALID"


class AsyncTokenBucket:
    """Endpoint-scoped monotonic token bucket shared by concurrent REST calls."""

    def __init__(self, *, capacity: int, refill_per_second: float, clock=time.monotonic):
        if capacity <= 0 or refill_per_second <= 0:
            raise ValueError("rate-limit capacity and refill must be positive")
        self._capacity = float(capacity)
        self._tokens = float(capacity)
        self._refill = refill_per_second
        self._clock = clock
        self._updated = clock()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = self._clock()
                self._tokens = min(
                    self._capacity,
                    self._tokens + max(0.0, now - self._updated) * self._refill,
                )
                self._updated = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                wait = (1 - self._tokens) / self._refill
            await asyncio.sleep(wait)


class OkxRestClient:
    """Real OKX V5 public REST wrapper with endpoint-bucket budgets and retries."""

    def __init__(self, *, base_url: str = OKX_REST_BASE, timeout_seconds: float = 10.0):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._buckets = {
            "instruments": AsyncTokenBucket(capacity=10, refill_per_second=5),
            "market": AsyncTokenBucket(capacity=10, refill_per_second=5),
        }

    async def get(
        self, path: str, *, params: Mapping[str, str], bucket: str, attempts: int = 3
    ) -> list[dict[str, Any]]:
        if bucket not in self._buckets:
            raise ValueError(f"undeclared OKX endpoint bucket: {bucket}")
        last_error: BaseException | None = None
        for attempt in range(attempts):
            await self._buckets[bucket].acquire()
            try:
                response = await asyncio.to_thread(
                    requests.get,
                    f"{self._base_url}{path}",
                    params=dict(params),
                    timeout=self._timeout,
                )
                response.raise_for_status()
                payload = response.json()
                if str(payload.get("code")) != "0" or not isinstance(payload.get("data"), list):
                    raise ValueError(f"OKX V5 error code={payload.get('code')} msg={payload.get('msg')}")
                return payload["data"]
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt + 1 < attempts:
                    delay = min(4.0, 0.5 * 2**attempt)
                    await asyncio.sleep(delay + random.random() * delay * 0.2)
        raise RuntimeError(f"OKX V5 request exhausted retries: {path}") from last_error

    async def instruments(self, inst_type: str) -> list[dict[str, Any]]:
        return await self.get(
            "/api/v5/public/instruments",
            params={"instType": inst_type.upper()},
            bucket="instruments",
        )

    async def trades(self, inst_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        if limit < 1 or limit > 500:
            raise ValueError("OKX trades limit must be between 1 and 500")
        return await self.get(
            "/api/v5/market/trades",
            params={"instId": inst_id, "limit": str(limit)},
            bucket="market",
        )

    async def book_snapshot(self, inst_id: str, *, depth: int = 100) -> list[dict[str, Any]]:
        if depth < 1 or depth > 400:
            raise ValueError("OKX book depth must be between 1 and 400")
        return await self.get(
            "/api/v5/market/books",
            params={"instId": inst_id, "sz": str(depth)},
            bucket="market",
        )


class OkxOrderBook:
    """Executable WS book. Any sequence/checksum gap invalidates all local levels."""

    def __init__(self, inst_id: str):
        self.inst_id = inst_id
        self.state = BookState.SYNCING
        self.generation = 0
        self.sequence: int | None = None
        self.bids: dict[str, str] = {}
        self.asks: dict[str, str] = {}

    def reconnect(self, generation: int) -> None:
        if generation <= self.generation:
            raise ValueError("connection generation must increase")
        self.generation = generation
        self._invalidate(BookState.SYNCING)

    def apply_ws(self, frame: Mapping[str, Any], *, generation: int) -> bool:
        if generation != self.generation:
            return False
        argument = frame.get("arg") or {}
        if argument.get("instId") != self.inst_id:
            raise ValueError("OKX book frame instrument mismatch")
        action = str(frame.get("action") or "")
        rows = frame.get("data")
        if action not in {"snapshot", "update"} or not isinstance(rows, list) or len(rows) != 1:
            raise ValueError("OKX book frame requires one snapshot/update data row")
        row = rows[0]
        sequence = int(row["seqId"])
        previous = int(row.get("prevSeqId", -1))
        if action == "snapshot":
            self.bids.clear()
            self.asks.clear()
        elif self.state is not BookState.LIVE or self.sequence != previous:
            self._invalidate(BookState.GAPPED)
            return False
        self._apply_levels(self.bids, row.get("bids", []))
        self._apply_levels(self.asks, row.get("asks", []))
        self.sequence = sequence
        # OKX V5 deprecated this field and currently fixes it to 0. Integrity
        # is sequence continuity; treating checksum=0 as CRC would invalidate
        # every real public book snapshot.
        self.state = BookState.LIVE
        return True

    def apply_rest_snapshot(self, _: Mapping[str, Any]) -> None:
        raise RuntimeError("REST /books cannot establish executable WS delta continuity")

    def _apply_levels(self, side: dict[str, str], levels: list[list[str]]) -> None:
        for level in levels:
            if len(level) < 2:
                raise ValueError("OKX book level requires price and size")
            price, quantity = str(level[0]), str(level[1])
            float(price)
            float(quantity)
            if quantity == "0" or float(quantity) == 0:
                side.pop(price, None)
            else:
                side[price] = quantity

    def _invalidate(self, state: BookState) -> None:
        self.state = state
        self.sequence = None
        self.bids.clear()
        self.asks.clear()


@dataclass(frozen=True)
class OkxSubscription:
    channel: str
    inst_id: str
    business: bool = False


class OkxWebSocketSupervisor:
    """Real OKX public/business WS reader with ack correlation and resubscription."""

    def __init__(
        self,
        *,
        on_frame: Callable[[Mapping[str, Any], int], Awaitable[None]],
        heartbeat_seconds: float = 20.0,
        max_backoff_seconds: float = 30.0,
    ) -> None:
        self._on_frame = on_frame
        self._heartbeat = heartbeat_seconds
        self._max_backoff = max_backoff_seconds

    async def run(
        self,
        subscriptions: tuple[OkxSubscription, ...],
        *,
        stop: asyncio.Event,
        max_events: int | None = None,
    ) -> int:
        if not subscriptions:
            return 0
        business_values = {item.business for item in subscriptions}
        if len(business_values) != 1:
            raise ValueError("public and business OKX subscriptions require separate sockets")
        from websockets.asyncio.client import connect

        endpoint = OKX_BUSINESS_WS if next(iter(business_values)) else OKX_PUBLIC_WS
        generation = 0
        received = 0
        failures = 0
        arguments = [{"channel": item.channel, "instId": item.inst_id} for item in subscriptions]
        while not stop.is_set() and (max_events is None or received < max_events):
            generation += 1
            try:
                async with connect(
                    endpoint, ping_interval=None, open_timeout=10, max_queue=16
                ) as socket:
                    await socket.send(json.dumps({"id": str(generation), "op": "subscribe", "args": arguments}))
                    pending = {(item.channel, item.inst_id) for item in subscriptions}
                    deadline = time.monotonic() + 10
                    while pending:
                        payload = json.loads(await asyncio.wait_for(socket.recv(), timeout=max(0.1, deadline - time.monotonic())))
                        if payload.get("event") == "error":
                            raise RuntimeError(f"OKX subscription rejected: {payload.get('code')} {payload.get('msg')}")
                        if payload.get("event") == "subscribe":
                            arg = payload.get("arg", {})
                            pending.discard((arg.get("channel"), arg.get("instId")))
                    failures = 0
                    while not stop.is_set() and (max_events is None or received < max_events):
                        try:
                            message = await asyncio.wait_for(socket.recv(), timeout=self._heartbeat)
                        except TimeoutError:
                            await socket.send("ping")
                            pong = await asyncio.wait_for(socket.recv(), timeout=self._heartbeat)
                            if pong != "pong":
                                raise RuntimeError("OKX heartbeat pong missing")
                            continue
                        payload = json.loads(message)
                        if payload.get("event"):
                            continue
                        await self._on_frame(payload, generation)
                        received += 1
            except asyncio.CancelledError:
                raise
            except (OSError, TimeoutError, ValueError, RuntimeError, json.JSONDecodeError):
                failures += 1
                if stop.is_set():
                    break
                delay = min(self._max_backoff, 0.5 * 2 ** min(failures, 8))
                await asyncio.sleep(delay + random.random() * delay * 0.2)
        return received
