from __future__ import annotations

from collections import deque
import inspect
from typing import Awaitable, Callable, Iterable

from qdl_sdk.models import BarLifecycle, BarPayload, Feed, MarketDataView, WarmupResponse


def _interval_ns(interval: str) -> int:
    units = {
        "s": 1_000_000_000,
        "m": 60 * 1_000_000_000,
        "h": 60 * 60 * 1_000_000_000,
        "d": 24 * 60 * 60 * 1_000_000_000,
        "w": 7 * 24 * 60 * 60 * 1_000_000_000,
    }
    value = str(interval or "")
    if len(value) < 2 or value[-1] not in units or value != value.lower():
        raise ValueError("handoff interval must be a canonical fixed duration")
    try:
        count = int(value[:-1])
    except ValueError as error:
        raise ValueError("handoff interval count must be an integer") from error
    if count < 1:
        raise ValueError("handoff interval must be positive")
    return count * units[value[-1]]


class ClosedBarHandoff:
    """Bounded alpha-facing FIFO with exactly-once closed-bar release."""

    def __init__(
        self,
        *,
        interval: str,
        maxlen: int,
        next_open_ns: Callable[[int], int] | None = None,
    ) -> None:
        if maxlen < 1:
            raise ValueError("closed-bar handoff maxlen must be positive")
        self.interval = interval
        self.interval_ns = _interval_ns(interval)
        self._next_open_ns = next_open_ns
        self._bars: deque[MarketDataView] = deque(maxlen=maxlen)

    @property
    def bars(self) -> tuple[MarketDataView, ...]:
        return tuple(self._bars)

    @classmethod
    def from_warmup(
        cls,
        response: WarmupResponse,
        *,
        interval: str,
        maxlen: int,
        next_open_ns: Callable[[int], int] | None = None,
    ) -> "ClosedBarHandoff":
        handoff = cls(
            interval=interval,
            maxlen=maxlen,
            next_open_ns=next_open_ns,
        )
        handoff.seed(response.data)
        return handoff

    def seed(self, values: Iterable[MarketDataView]) -> None:
        self._bars.clear()
        for item in values:
            self._append(item, seeding=True)

    async def append_closed(
        self,
        item: MarketDataView,
        *,
        release: Callable[
            [tuple[MarketDataView, ...]], object | Awaitable[object]
        ],
    ) -> bool:
        appended = self._append(item, seeding=False)
        if not appended:
            return False
        result = release(tuple(self._bars))
        if inspect.isawaitable(result):
            await result
        return True

    def _append(self, item: MarketDataView, *, seeding: bool) -> bool:
        if item.feed is not Feed.BAR or item.interval != self.interval:
            raise ValueError("handoff accepts only BAR rows for its interval")
        if not isinstance(item.payload, BarPayload) or item.payload.lifecycle not in {
            BarLifecycle.FINAL,
            BarLifecycle.REVISED,
        }:
            raise ValueError("handoff accepts only final or revised bars")
        open_ns = int(item.payload.open_time_ns)
        if self._bars:
            previous = self._bars[-1]
            assert isinstance(previous.payload, BarPayload)
            previous_open = int(previous.payload.open_time_ns)
            if open_ns == previous_open:
                if item.revision > previous.revision:
                    self._bars[-1] = item
                return False
            expected = (
                self._next_open_ns(previous_open)
                if self._next_open_ns is not None
                else previous_open + self.interval_ns
            )
            if open_ns != expected:
                raise ValueError("closed-bar handoff detected a gap or late bar")
        self._bars.append(item)
        return not seeding
