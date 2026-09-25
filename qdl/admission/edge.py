"""Blocking facade over the Rust provider admission for the thread-based BAR edge.

Purpose (KN-4 D47-2): the stable BAR edge calls venue REST from threads, while
the Rust admission relay (``RustHttpProviderAdmission``) is async. This module
owns one private event-loop thread for the relay and gives the edge a lease:
admit (waiting out a DEFERRED decision by the Rust ``retry_after_ms`` hint,
bounded by a deadline), run the provider call, complete - also when the call
fails. A provider rate-limit reply is relayed to Rust (which opens the shared
cooldown) and raised as ``ProviderRateLimited``.

Boundary: no budget, queue, retry or cooldown policy lives here - Rust decides
(``rust/qdl-core/src/provider_admission.rs``); this only waits for what Rust
says and never widens a grant.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
import logging
import threading
import time
import uuid
from typing import Any, Callable, Iterator

from qdl.admission.contracts import (
    AdmissionDecision,
    AdmissionDisposition,
    AdmissionPriority,
    AdmissionRequest,
    ProviderAdmissionRuntime,
    ProviderLane,
)

logger = logging.getLogger(__name__)

_CALL_TIMEOUT_S = 5.0


class AdmissionDeadlineExceeded(RuntimeError):
    """Rust kept deferring past the caller's deadline (typed, never a venue call)."""

    def __init__(self, lane: ProviderLane, decision: AdmissionDecision) -> None:
        self.lane = lane
        self.decision = decision
        reason = decision.defer_reason.value if decision.defer_reason else "DEFERRED"
        super().__init__(f"provider admission deferred {lane.provider}/{lane.market}/"
                         f"{lane.endpoint_family}: {reason}")


class ProviderRateLimited(RuntimeError):
    """The venue answered with a rate-limit signal; Rust has opened the cooldown."""

    def __init__(self, lane: ProviderLane, *, http_status: int | None, provider_code: int | None,
                 retry_after_ms: int | None) -> None:
        self.lane = lane
        self.http_status = http_status
        self.provider_code = provider_code
        self.retry_after_ms = retry_after_ms
        self.banned = http_status == 418
        super().__init__(f"{lane.provider} rate limited http={http_status} code={provider_code} "
                         f"retry_after_ms={retry_after_ms}")


def request_id(prefix: str) -> str:
    """A unique admission request id (Rust: 1..128 of [A-Za-z0-9._:-])."""

    safe = "".join(ch if ch.isalnum() or ch in "._:-" else "-" for ch in prefix)[:90]
    return f"{safe}:{uuid.uuid4().hex}"


class BlockingProviderAdmission:
    def __init__(self, runtime_factory: Callable[[], ProviderAdmissionRuntime], *,
                 max_wait_s: float = 30.0, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if max_wait_s <= 0:
            raise ValueError("admission max wait must be positive")
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="qdl-provider-admission",
                                        daemon=True)
        self._thread.start()
        self._max_wait_s = max_wait_s
        self._sleep = sleep
        self._clock = clock

        async def create() -> ProviderAdmissionRuntime:
            return runtime_factory()

        self._runtime = self._call(create())

    def _call(self, coroutine) -> Any:
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result(_CALL_TIMEOUT_S)

    @contextmanager
    def lease(self, lane: ProviderLane, request_prefix: str, *, priority: AdmissionPriority, token_cost: int,
              max_wait_s: float | None = None) -> Iterator[AdmissionDecision]:
        """Hold a Rust grant for one provider call; completed on every exit."""

        identifier = request_id(request_prefix)
        request = AdmissionRequest(lane=lane, request_id=identifier, priority=priority, token_cost=token_cost)
        deadline = self._clock() + (self._max_wait_s if max_wait_s is None else max_wait_s)
        while True:
            decision = self._call(self._runtime.admit(request))
            if decision.disposition is AdmissionDisposition.GRANTED:
                break
            wait_s = max(0.01, (decision.retry_after_ms or 100) / 1000.0)
            if self._clock() + wait_s > deadline:
                raise AdmissionDeadlineExceeded(lane, decision)
            self._sleep(wait_s)
        try:
            yield decision
        finally:
            try:
                self._call(self._runtime.complete(lane, identifier))
            except Exception:  # noqa: BLE001 - the lease expires in Rust (max_lease_ns)
                logger.warning("provider admission completion failed lane=%s request=%s", lane, identifier,
                               exc_info=True)

    def rate_limited(self, lane: ProviderLane, *, http_status: int | None, provider_code: int | None,
                     retry_after_ms: int | None) -> ProviderRateLimited:
        """Relay a venue rate-limit reply to Rust; return the typed error to raise."""

        try:
            self._call(self._runtime.record_rate_limit(
                lane, None, http_status=http_status, provider_code=provider_code,
                retry_after_ms=retry_after_ms if retry_after_ms and retry_after_ms > 0 else None))
        except Exception:  # noqa: BLE001 - still raised to the caller, typed
            logger.warning("provider rate-limit relay failed lane=%s", lane, exc_info=True)
        return ProviderRateLimited(lane, http_status=http_status, provider_code=provider_code,
                                   retry_after_ms=retry_after_ms)

    def close(self) -> None:
        close = getattr(self._runtime, "aclose", None) or getattr(self._runtime, "close", None)
        if close is not None:
            try:
                result = close()
                if asyncio.iscoroutine(result):
                    self._call(result)
            except Exception:  # noqa: BLE001
                logger.debug("provider admission close failed", exc_info=True)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
