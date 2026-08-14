from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Protocol

from redis.asyncio import Redis

from qdl.runtime.readiness import ComponentReadiness, ComponentState


@dataclass(frozen=True, slots=True)
class GatewayLease:
    shard_id: str
    owner_id: str
    epoch: int
    expires_at_ns: int

    def __post_init__(self) -> None:
        if not self.shard_id.strip() or not self.owner_id.strip() or self.epoch < 1:
            raise ValueError("gateway lease identity and epoch are required")


class AsyncGatewayLeaseStore(Protocol):
    async def acquire(self, shard_id: str, owner_id: str, ttl_seconds: int) -> GatewayLease | None: ...

    async def renew(self, lease: GatewayLease, ttl_seconds: int) -> GatewayLease | None: ...

    async def release(self, lease: GatewayLease) -> bool: ...


_ACQUIRE = """
local current_owner = redis.call('HGET', KEYS[1], 'owner')
if current_owner and current_owner ~= ARGV[1] then
  return nil
end
local epoch = redis.call('HGET', KEYS[1], 'epoch')
if not epoch then
  epoch = redis.call('INCR', KEYS[2])
end
redis.call('HSET', KEYS[1], 'owner', ARGV[1], 'epoch', epoch)
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return epoch
"""

_RENEW = """
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then
  return 0
end
if redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[2] then
  return 0
end
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
"""

_RELEASE = """
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then
  return 0
end
if redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[2] then
  return 0
end
redis.call('DEL', KEYS[1])
return 1
"""


class RedisGatewayLeaseStore:
    """Redis lease with a persistent monotonic fencing epoch in a beta namespace."""

    def __init__(self, redis: Redis, *, prefix: str) -> None:
        normalized = prefix.strip(": ")
        if not normalized or "legacy" in normalized.lower():
            raise ValueError("a dedicated non-legacy lease prefix is required")
        self.redis = redis
        self.prefix = normalized

    def _keys(self, shard_id: str) -> tuple[str, str]:
        if not shard_id.strip():
            raise ValueError("gateway shard ID is required")
        return (
            f"{self.prefix}:gateway:{shard_id}:lease",
            f"{self.prefix}:gateway:{shard_id}:epoch",
        )

    async def acquire(
        self, shard_id: str, owner_id: str, ttl_seconds: int
    ) -> GatewayLease | None:
        if not owner_id.strip() or not 5 <= ttl_seconds <= 300:
            raise ValueError("gateway owner and lease TTL 5..300 seconds are required")
        lease_key, epoch_key = self._keys(shard_id)
        epoch = await self.redis.eval(
            _ACQUIRE, 2, lease_key, epoch_key, owner_id, ttl_seconds * 1000
        )
        if epoch is None:
            return None
        return GatewayLease(
            shard_id, owner_id, int(epoch), time.time_ns() + ttl_seconds * 1_000_000_000
        )

    async def renew(self, lease: GatewayLease, ttl_seconds: int) -> GatewayLease | None:
        if not 5 <= ttl_seconds <= 300:
            raise ValueError("gateway lease TTL must be 5..300 seconds")
        lease_key, _ = self._keys(lease.shard_id)
        renewed = await self.redis.eval(
            _RENEW,
            1,
            lease_key,
            lease.owner_id,
            str(lease.epoch),
            ttl_seconds * 1000,
        )
        if int(renewed or 0) != 1:
            return None
        return GatewayLease(
            lease.shard_id,
            lease.owner_id,
            lease.epoch,
            time.time_ns() + ttl_seconds * 1_000_000_000,
        )

    async def release(self, lease: GatewayLease) -> bool:
        lease_key, _ = self._keys(lease.shard_id)
        released = await self.redis.eval(
            _RELEASE, 1, lease_key, lease.owner_id, str(lease.epoch)
        )
        return int(released or 0) == 1


class InMemoryAsyncGatewayLeaseStore:
    """Deterministic distributed-lease oracle used only by unit tests."""

    def __init__(self, *, clock_ns=time.time_ns) -> None:
        self._clock_ns = clock_ns
        self._leases: dict[str, GatewayLease] = {}
        self._epochs: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def acquire(
        self, shard_id: str, owner_id: str, ttl_seconds: int
    ) -> GatewayLease | None:
        async with self._lock:
            now = self._clock_ns()
            current = self._leases.get(shard_id)
            if current and current.expires_at_ns > now and current.owner_id != owner_id:
                return None
            if current is None or current.expires_at_ns <= now:
                self._epochs[shard_id] = self._epochs.get(shard_id, 0) + 1
            lease = GatewayLease(
                shard_id,
                owner_id,
                self._epochs[shard_id],
                now + ttl_seconds * 1_000_000_000,
            )
            self._leases[shard_id] = lease
            return lease

    async def renew(self, lease: GatewayLease, ttl_seconds: int) -> GatewayLease | None:
        async with self._lock:
            now = self._clock_ns()
            if self._leases.get(lease.shard_id) != lease or lease.expires_at_ns <= now:
                return None
            renewed = GatewayLease(
                lease.shard_id,
                lease.owner_id,
                lease.epoch,
                now + ttl_seconds * 1_000_000_000,
            )
            self._leases[lease.shard_id] = renewed
            return renewed

    async def release(self, lease: GatewayLease) -> bool:
        async with self._lock:
            if self._leases.get(lease.shard_id) != lease:
                return False
            self._leases.pop(lease.shard_id, None)
            return True


class GatewayFenced(RuntimeError):
    """The gateway is passive or its ownership epoch is stale."""


class ActivePassiveGatewayLease:
    def __init__(
        self,
        store: AsyncGatewayLeaseStore,
        *,
        shard_id: str,
        owner_id: str,
        ttl_seconds: int = 15,
        renew_interval_seconds: float = 5.0,
        on_fenced: Callable[[], Awaitable[None]] | None = None,
        clock_ns=time.time_ns,
    ) -> None:
        if not shard_id.strip() or not owner_id.strip():
            raise ValueError("gateway shard and owner are required")
        if not 5 <= ttl_seconds <= 300:
            raise ValueError("gateway lease TTL must be 5..300 seconds")
        if not 0 < renew_interval_seconds < ttl_seconds:
            raise ValueError("gateway renew interval must be below TTL")
        self.store = store
        self.shard_id = shard_id
        self.owner_id = owner_id
        self.ttl_seconds = ttl_seconds
        self.renew_interval_seconds = renew_interval_seconds
        self.on_fenced = on_fenced
        self._clock_ns = clock_ns
        self.lease: GatewayLease | None = None
        self._task: asyncio.Task | None = None
        self._stopping = False
        self.last_error: str | None = None

    @property
    def current_epoch(self) -> int | None:
        return self.lease.epoch if self.lease is not None else None

    @property
    def active(self) -> bool:
        return bool(self.lease and self.lease.expires_at_ns > self._clock_ns())

    def assert_active(self, expected_epoch: int | None = None) -> int:
        if not self.active or self.lease is None:
            raise GatewayFenced("stream gateway is passive or lease expired")
        if expected_epoch is not None and expected_epoch != self.lease.epoch:
            raise GatewayFenced("stream gateway lease epoch changed")
        return self.lease.epoch

    async def acquire_once(self) -> bool:
        try:
            lease = await self.store.acquire(
                self.shard_id, self.owner_id, self.ttl_seconds
            )
            self.last_error = None
        except Exception as error:
            self.last_error = f"{type(error).__name__}: {error}"
            lease = None
        self.lease = lease
        return lease is not None

    async def _lose_lease(self) -> None:
        had_lease = self.lease is not None
        self.lease = None
        if had_lease and self.on_fenced is not None:
            await self.on_fenced()

    async def _run(self) -> None:
        while not self._stopping:
            if self.lease is None:
                await self.acquire_once()
            else:
                try:
                    renewed = await self.store.renew(self.lease, self.ttl_seconds)
                    self.last_error = None
                except Exception as error:
                    self.last_error = f"{type(error).__name__}: {error}"
                    renewed = None
                if renewed is None:
                    await self._lose_lease()
                else:
                    self.lease = renewed
            await asyncio.sleep(self.renew_interval_seconds)

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stopping = False
        await self.acquire_once()
        self._task = asyncio.create_task(
            self._run(), name=f"qdl-gateway-lease-{self.owner_id}"
        )

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        lease = self.lease
        self.lease = None
        if lease is not None:
            try:
                await self.store.release(lease)
            except Exception as error:
                self.last_error = f"{type(error).__name__}: {error}"

    async def readiness(self) -> ComponentReadiness:
        state = ComponentState.READY if self.active else ComponentState.STANDBY
        return ComponentReadiness(
            "gateway_lease",
            state,
            required=True,
            detail=(
                f"active owner={self.owner_id} epoch={self.current_epoch}"
                if self.active
                else f"standby owner={self.owner_id}; {self.last_error or 'lease held by peer'}"
            ),
            revision=str(self.current_epoch) if self.current_epoch else None,
            checked_at_ns=time.time_ns(),
        )
