from __future__ import annotations

import asyncio
from dataclasses import dataclass
import random
import time
from typing import Awaitable, Callable, Generic, Hashable, Iterable, TypeVar


T = TypeVar("T")
R = TypeVar("R")


class RetryableWarmupError(RuntimeError):
    def __init__(
        self,
        detail: str,
        *,
        retry_after_ms: int | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(detail)
        self.retry_after_ms = retry_after_ms
        self.cause = cause


@dataclass(frozen=True, slots=True)
class ProviderBudgetPolicy:
    max_concurrency: int = 4
    requests_per_second: float = 5.0
    burst_requests: int = 5
    max_attempts: int = 4
    circuit_failures: int = 5
    circuit_cooldown_ms: int = 30_000

    def __post_init__(self) -> None:
        if not 1 <= self.max_concurrency <= 64:
            raise ValueError("provider concurrency must be between 1 and 64")
        if self.requests_per_second <= 0 or self.burst_requests < 1:
            raise ValueError("provider token budget values must be positive")
        if not 1 <= self.max_attempts <= 10:
            raise ValueError("provider attempts must be between 1 and 10")
        if self.circuit_failures < 1 or self.circuit_cooldown_ms < 1:
            raise ValueError("provider circuit policy values must be positive")


@dataclass(frozen=True, slots=True)
class WarmupExecution(Generic[T, R]):
    item: T
    value: R | None
    error: BaseException | None
    attempts: int
    shared: bool
    elapsed_ms: float

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass(slots=True)
class _Inflight(Generic[R]):
    task: asyncio.Task[tuple[R, int]]
    waiters: int = 0


@dataclass(slots=True)
class _TokenState:
    tokens: float
    updated_at: float


class BoundedWarmupExecutor(Generic[T, R]):
    """Bounded provider-fair execution with cross-request singleflight."""

    DEFAULT_PROVIDER_POLICIES = {
        "BINANCE": ProviderBudgetPolicy(
            max_concurrency=8,
            requests_per_second=8.0,
            burst_requests=8,
        ),
        "OKX": ProviderBudgetPolicy(
            max_concurrency=4,
            requests_per_second=5.0,
            burst_requests=5,
        ),
        "DNSE": ProviderBudgetPolicy(
            max_concurrency=2,
            requests_per_second=2.0,
            burst_requests=2,
        ),
        "HNX": ProviderBudgetPolicy(
            max_concurrency=2,
            requests_per_second=2.0,
            burst_requests=2,
        ),
        "HOSE": ProviderBudgetPolicy(
            max_concurrency=2,
            requests_per_second=2.0,
            burst_requests=2,
        ),
        "VN_MARKETS": ProviderBudgetPolicy(
            max_concurrency=2,
            requests_per_second=2.0,
            burst_requests=2,
        ),
    }

    def __init__(
        self,
        *,
        default_policy: ProviderBudgetPolicy | None = None,
        provider_policies: dict[str, ProviderBudgetPolicy] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        self.default_policy = default_policy or ProviderBudgetPolicy()
        configured = provider_policies
        if configured is None:
            # Venue defaults are a safe production baseline. An explicitly
            # supplied default policy is nevertheless the caller's authority
            # (and is commonly used to impose a stricter test/operator bound),
            # so it must not be shadowed by implicit venue entries.
            configured = self.DEFAULT_PROVIDER_POLICIES if default_policy is None else {}
        self.provider_policies = {
            key.upper(): value for key, value in configured.items()
        }
        self._sleep = sleep
        self._clock = clock
        self._random = random_value
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._inflight: dict[Hashable, _Inflight[R]] = {}
        self._inflight_lock = asyncio.Lock()
        self._rate_locks: dict[str, asyncio.Lock] = {}
        self._tokens: dict[str, _TokenState] = {}
        self._circuit: dict[str, tuple[int, float]] = {}
        self._circuit_lock = asyncio.Lock()
        self.source_calls = 0
        self.singleflight_hits = 0
        self.retry_count = 0
        self.circuit_rejections = 0

    async def execute(
        self,
        items: Iterable[T],
        *,
        work: Callable[[T], Awaitable[R]],
        identity: Callable[[T], Hashable],
        provider: Callable[[T], str],
        deadline_ms: Callable[[T], int],
    ) -> tuple[WarmupExecution[T, R], ...]:
        values = tuple(items)
        tasks = []
        for item in values:
            provider_key = provider(item).upper()
            tasks.append(asyncio.create_task(self._one(
                item,
                work=work,
                key=(provider_key, identity(item)),
                provider_key=provider_key,
                deadline_ms=deadline_ms(item),
            )))
        tasks = tuple(tasks)
        try:
            return tuple(await asyncio.gather(*tasks))
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _one(
        self,
        item: T,
        *,
        work: Callable[[T], Awaitable[R]],
        key: Hashable,
        provider_key: str,
        deadline_ms: int,
    ) -> WarmupExecution[T, R]:
        started = self._clock()
        shared = False
        attempts = 0
        try:
            async with self._inflight_lock:
                inflight = self._inflight.get(key)
                if inflight is None:
                    task = asyncio.create_task(
                        self._run_with_policy(
                            item,
                            work,
                            provider_key,
                            deadline_at=started + deadline_ms / 1000,
                        )
                    )
                    inflight = _Inflight(task=task, waiters=1)
                    self._inflight[key] = inflight
                else:
                    inflight.waiters += 1
                    shared = True
                    self.singleflight_hits += 1
                task = inflight.task
            try:
                value, attempts = await asyncio.wait_for(
                    asyncio.shield(task), timeout=deadline_ms / 1000
                )
                return WarmupExecution(
                    item, value, None, attempts, shared,
                    (self._clock() - started) * 1000,
                )
            except asyncio.TimeoutError as error:
                return WarmupExecution(
                    item,
                    None,
                    RetryableWarmupError(
                        f"warmup deadline exceeded after {deadline_ms}ms"
                    ),
                    attempts,
                    shared,
                    (self._clock() - started) * 1000,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as error:
                attempts = int(getattr(error, "warmup_attempts", attempts))
                return WarmupExecution(
                    item, None, error, attempts, shared,
                    (self._clock() - started) * 1000,
                )
            finally:
                await self._release_waiter(key, inflight)
        except asyncio.CancelledError:
            raise

    async def _release_waiter(self, key: Hashable, inflight: _Inflight[R]) -> None:
        cancelled = False
        async with self._inflight_lock:
            current = self._inflight.get(key)
            if current is not inflight:
                return
            current.waiters -= 1
            if current.waiters > 0:
                return
            self._inflight.pop(key, None)
            if not current.task.done():
                current.task.cancel()
                cancelled = True
        if cancelled:
            await asyncio.gather(current.task, return_exceptions=True)
        elif current.task.done():
            # Retrieve a late exception after the final waiter timed out so the
            # event loop never reports an orphaned task. Awaiters have already
            # observed the same task result through shield().
            try:
                current.task.exception()
            except asyncio.CancelledError:
                pass

    async def _run_with_policy(
        self,
        item: T,
        work: Callable[[T], Awaitable[R]],
        provider: str,
        *,
        deadline_at: float | None = None,
    ) -> tuple[R, int]:
        policy = self.provider_policies.get(provider, self.default_policy)
        semaphore = self._semaphores.setdefault(
            provider, asyncio.Semaphore(policy.max_concurrency)
        )
        last_error: BaseException | None = None
        for attempt in range(1, policy.max_attempts + 1):
            await self._require_closed_circuit(provider)
            try:
                async with semaphore:
                    await self._acquire_provider_token(provider, policy)
                    self.source_calls += 1
                    value = await work(item)
                await self._record_success(provider)
                return value, attempt
            except RetryableWarmupError as error:
                error.warmup_attempts = attempt
                last_error = error
                open_until = await self._record_failure(provider, policy)
                if open_until:
                    break
                if attempt == policy.max_attempts:
                    break
                provider_delay = (error.retry_after_ms or 0) / 1000
                exponential = min(4.0, 0.1 * (2 ** (attempt - 1)))
                delay = max(provider_delay, exponential) + self._random() * 0.05
                if deadline_at is not None and self._clock() + delay >= deadline_at:
                    raise RetryableWarmupError(
                        "provider retry delay exceeds the remaining bounded deadline",
                        retry_after_ms=error.retry_after_ms,
                        cause=error,
                    ) from error
                self.retry_count += 1
                await self._sleep(delay)
        assert last_error is not None
        raise last_error

    async def _require_closed_circuit(self, provider: str) -> None:
        async with self._circuit_lock:
            _, open_until = self._circuit.get(provider, (0, 0.0))
            now = self._clock()
            if open_until <= now:
                return
            self.circuit_rejections += 1
            retry_after_ms = max(1, int((open_until - now) * 1000))
        raise RetryableWarmupError(
            f"provider circuit is open for {provider}",
            retry_after_ms=retry_after_ms,
        )

    async def _record_success(self, provider: str) -> None:
        async with self._circuit_lock:
            self._circuit[provider] = (0, 0.0)

    async def _record_failure(
        self,
        provider: str,
        policy: ProviderBudgetPolicy,
    ) -> float:
        async with self._circuit_lock:
            failures, open_until = self._circuit.get(provider, (0, 0.0))
            if open_until > self._clock():
                return open_until
            failures += 1
            open_until = 0.0
            if failures >= policy.circuit_failures:
                open_until = self._clock() + policy.circuit_cooldown_ms / 1000
            self._circuit[provider] = (failures, open_until)
            return open_until

    async def _acquire_provider_token(
        self,
        provider: str,
        policy: ProviderBudgetPolicy,
    ) -> None:
        lock = self._rate_locks.setdefault(provider, asyncio.Lock())
        async with lock:
            state = self._tokens.get(provider)
            now = self._clock()
            if state is None:
                state = _TokenState(float(policy.burst_requests), now)
            elapsed = max(0.0, now - state.updated_at)
            tokens = min(
                float(policy.burst_requests),
                state.tokens + elapsed * policy.requests_per_second,
            )
            if tokens < 1.0:
                delay = (1.0 - tokens) / policy.requests_per_second
                await self._sleep(delay)
                now = self._clock()
                elapsed = max(0.0, now - state.updated_at)
                tokens = min(
                    float(policy.burst_requests),
                    state.tokens + elapsed * policy.requests_per_second,
                )
            # A custom test clock may not advance with sleep; consume the
            # granted token without allowing the bucket to become negative.
            self._tokens[provider] = _TokenState(max(0.0, tokens - 1.0), now)

    def stats(self) -> dict[str, int]:
        return {
            "source_calls": self.source_calls,
            "singleflight_hits": self.singleflight_hits,
            "retry_count": self.retry_count,
            "circuit_rejections": self.circuit_rejections,
        }
