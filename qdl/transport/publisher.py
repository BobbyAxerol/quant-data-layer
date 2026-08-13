from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable

from qdl.transport.contracts import (
    AppendResult,
    DurableEvent,
    EventSink,
    RetryClass,
    RetryDecision,
    classify_transport_error,
)


class PublisherState(str, Enum):
    LIVE = "LIVE"
    DEGRADED = "DEGRADED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class PublisherStatus:
    state: PublisherState
    reason: str
    consecutive_failures: int


class DurablePublisher:
    """Acknowledge only after durable append; never silently drop on failure."""

    def __init__(
        self,
        sink: EventSink,
        *,
        max_attempts: int = 3,
        classifier: Callable[[BaseException], RetryDecision] = classify_transport_error,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        self._sink = sink
        self._max_attempts = max_attempts
        self._classifier = classifier
        self._sleep = sleep
        self._status = PublisherStatus(PublisherState.LIVE, "ready", 0)

    @property
    def status(self) -> PublisherStatus:
        return self._status

    def publish(self, event: DurableEvent) -> AppendResult:
        last_error: BaseException | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                result = self._sink.append(event)
                self._status = PublisherStatus(PublisherState.LIVE, "durable_commit", 0)
                return result
            except BaseException as exc:
                last_error = exc
                decision = self._classifier(exc)
                if decision.classification is RetryClass.CAPACITY:
                    self._status = PublisherStatus(
                        PublisherState.BLOCKED, decision.reason, attempt
                    )
                    raise
                if decision.classification is RetryClass.NON_RETRYABLE:
                    self._status = PublisherStatus(
                        PublisherState.BLOCKED, decision.reason, attempt
                    )
                    raise
                self._status = PublisherStatus(
                    PublisherState.DEGRADED, decision.reason, attempt
                )
                if attempt < self._max_attempts:
                    self._sleep(decision.retry_after_seconds * attempt)
        assert last_error is not None
        raise last_error
