from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from collections import defaultdict
from typing import Any, Callable


class PreloadTopupBackoff(RuntimeError):
    pass


class PreloadTopupTimeout(RuntimeError):
    pass


class PreloadTopupCoordinator:
    """Local singleflight plus Redis fencing for VN read-through top-up."""

    _RELEASE_SCRIPT = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
      return redis.call('del', KEYS[1])
    end
    return 0
    """

    def __init__(
        self,
        redis_client,
        topup: Callable[..., dict[str, Any]],
        *,
        lock_ttl_seconds: int = 180,
        wait_timeout_seconds: float = 20.0,
        failure_backoff_seconds: int = 30,
    ) -> None:
        self.redis = redis_client
        self.topup = topup
        self.lock_ttl_seconds = max(30, int(lock_ttl_seconds))
        self.wait_timeout_seconds = max(0.1, float(wait_timeout_seconds))
        self.failure_backoff_seconds = max(1, int(failure_backoff_seconds))
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._recent_results: dict[str, tuple[float, dict[str, Any]]] = {}
        self.provider_fetch_count = 0
        self.waiter_count = 0
        self.failure_count = 0

    @staticmethod
    def _scope(symbol: str, interval: str) -> str:
        return f"{symbol.upper()}:{interval}"

    @staticmethod
    def _digest(scope: str) -> str:
        return hashlib.sha256(scope.encode("utf-8")).hexdigest()[:24]

    def _keys(self, scope: str) -> tuple[str, str]:
        digest = self._digest(scope)
        return f"preload:topup:lock:{digest}", f"preload:topup:backoff:{digest}"

    async def run(
        self,
        symbol: str,
        *,
        interval: str,
        max_lag_minutes: int,
    ) -> dict[str, Any]:
        scope = self._scope(symbol, interval)
        async with self._locks[scope]:
            recent = self._recent_results.get(scope)
            if recent and time.monotonic() - recent[0] <= 2.0:
                return {**recent[1], "singleflight": "local_waiter_completed"}
            lock_key, backoff_key = self._keys(scope)
            if await self.redis.exists(backoff_key):
                raise PreloadTopupBackoff(f"provider backoff active for {scope}")
            token = uuid.uuid4().hex
            acquired = await self.redis.set(lock_key, token, nx=True, ex=self.lock_ttl_seconds)
            if not acquired:
                self.waiter_count += 1
                return await self._wait_for_owner(lock_key, backoff_key, scope)
            try:
                self.provider_fetch_count += 1
                result = await asyncio.to_thread(
                    self.topup,
                    symbol.upper(),
                    interval=interval,
                    max_lag_minutes=max_lag_minutes,
                )
                owner_result = {**result, "singleflight": "owner"}
                self._recent_results[scope] = (time.monotonic(), owner_result)
                return owner_result
            except Exception as exc:
                self.failure_count += 1
                await self.redis.set(backoff_key, type(exc).__name__, ex=self.failure_backoff_seconds)
                raise
            finally:
                await self.redis.eval(self._RELEASE_SCRIPT, 1, lock_key, token)

    async def _wait_for_owner(self, lock_key: str, backoff_key: str, scope: str) -> dict[str, Any]:
        deadline = time.monotonic() + self.wait_timeout_seconds
        while time.monotonic() < deadline:
            if await self.redis.exists(backoff_key):
                raise PreloadTopupBackoff(f"provider backoff active for {scope}")
            if not await self.redis.exists(lock_key):
                return {"needed": False, "singleflight": "waiter_completed"}
            await asyncio.sleep(0.1)
        raise PreloadTopupTimeout(f"top-up owner did not finish within timeout for {scope}")

    def snapshot(self) -> dict[str, Any]:
        return {
            "provider_fetch_count": self.provider_fetch_count,
            "waiter_count": self.waiter_count,
            "failure_count": self.failure_count,
            "active_local_scopes": sum(1 for lock in self._locks.values() if lock.locked()),
        }
