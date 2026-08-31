from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace
from decimal import Decimal
import hashlib
import json
from typing import Callable, Iterable

from qdl.adapters.intervals import canonical_interval_ms
from qdl.query.lifecycle import BarLifecycle
from qdl.query.results import MarketDataItem


@dataclass(frozen=True, slots=True)
class ResampleLineage:
    base_interval: str
    constituent_count: int
    first_watermark: int
    last_watermark: int
    constituent_sha256: str


def _sum_optional(items: tuple[MarketDataItem, ...], field: str) -> str | None:
    values = [item.payload.get(field) for item in items]
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise ValueError(f"resample constituent {field} coverage is incomplete")
    return format(sum((Decimal(str(value)) for value in values), Decimal(0)), "f")


def resample_final_bars(
    bars: Iterable[MarketDataItem],
    *,
    target_interval: str,
    allowed_discontinuity: Callable[[int, int], bool] | None = None,
) -> tuple[MarketDataItem, ...]:
    source = tuple(bars)
    if not source:
        return ()
    base_interval = source[0].interval or ""
    base_ns = canonical_interval_ms(base_interval) * 1_000_000
    target_ns = canonical_interval_ms(target_interval) * 1_000_000
    if target_ns <= base_ns or target_ns % base_ns:
        raise ValueError("target interval must be an exact multiple of the base interval")
    factor = target_ns // base_ns
    identity = (source[0].instrument_uid, source[0].source.source_id)
    for previous, current in zip(source, source[1:]):
        previous_open = int(previous.payload["open_time_ns"])
        current_open = int(current.payload["open_time_ns"])
        if current_open - previous_open != base_ns and not (
            allowed_discontinuity is not None
            and allowed_discontinuity(previous_open, current_open)
        ):
            raise ValueError("resample input contains a constituent gap")
    for item in source:
        if (item.instrument_uid, item.source.source_id) != identity:
            raise ValueError("resample input mixes instrument or source identity")
        if item.interval != base_interval or item.bar_lifecycle not in {
            BarLifecycle.FINAL,
            BarLifecycle.REVISED,
        } or item.payload.get("is_final") is not True:
            raise ValueError("resample input requires final bars from one base interval")
        if item.payload.get("volume_unit") != source[0].payload.get("volume_unit"):
            raise ValueError("resample input mixes volume units")
    groups: dict[int, list[MarketDataItem]] = {}
    for item in source:
        open_ns = int(item.payload["open_time_ns"])
        groups.setdefault(open_ns // target_ns * target_ns, []).append(item)
    result = []
    for target_open, raw_group in sorted(groups.items()):
        group = tuple(raw_group)
        expected = tuple(target_open + index * base_ns for index in range(factor))
        observed = tuple(int(item.payload["open_time_ns"]) for item in group)
        if observed != expected:
            continue
        first, last = group[0], group[-1]
        payload = {
            **last.payload,
            "open_time_ns": target_open,
            # Preserve the canonical source timestamp precision. Binance bars
            # close on the final source millisecond while other providers may
            # encode the exclusive boundary; the final constituent is the
            # authoritative close timestamp for the aggregate.
            "close_time_ns": last.payload["close_time_ns"],
            "open": first.payload["open"],
            "high": format(max(Decimal(str(item.payload["high"])) for item in group), "f"),
            "low": format(min(Decimal(str(item.payload["low"])) for item in group), "f"),
            "close": last.payload["close"],
            "volume": format(sum((Decimal(str(item.payload["volume"])) for item in group), Decimal(0)), "f"),
            "base_volume": _sum_optional(group, "base_volume"),
            "quote_volume": _sum_optional(group, "quote_volume"),
            "contract_volume": _sum_optional(group, "contract_volume"),
            "trade_count": sum(int(item.payload.get("trade_count", 0)) for item in group),
            "origin": "AGGREGATED",
            "is_final": True,
        }
        constituent_sha256 = hashlib.sha256(
            json.dumps(
                [
                    {
                        "correlation_id": item.contract.correlation_id,
                        "open_time_ns": item.payload["open_time_ns"],
                        "close_time_ns": item.payload["close_time_ns"],
                        "revision": item.revision,
                        "watermark": item.watermark_offset,
                    }
                    for item in group
                ],
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        result.append(
            replace(
                last,
                interval=target_interval,
                payload=payload,
                observed_at_ns=target_open + target_ns - 1,
                received_at_ns=max(
                    item.received_at_ns or item.observed_at_ns for item in group
                ),
                revision=max(item.revision for item in group),
                watermark_offset=max(item.watermark_offset for item in group),
                bar_lifecycle=BarLifecycle.FINAL,
                supersedes_event_id=None,
                contract=replace(
                    last.contract,
                    correlation_id=f"resample:{constituent_sha256}",
                ),
                resample_lineage=ResampleLineage(
                    base_interval=base_interval,
                    constituent_count=len(group),
                    first_watermark=group[0].watermark_offset,
                    last_watermark=group[-1].watermark_offset,
                    constituent_sha256=constituent_sha256,
                ),
            )
        )
    return tuple(result)


class ClosedBarFifo:
    """Keep the newest N final bars and release one callback per new bar."""

    def __init__(
        self,
        *,
        interval: str,
        maxlen: int,
        next_open_ns: Callable[[int], int] | None = None,
    ) -> None:
        if maxlen < 1:
            raise ValueError("closed-bar FIFO maxlen must be positive")
        self.interval = interval
        self.interval_ns = canonical_interval_ms(interval) * 1_000_000
        self._next_open_ns = next_open_ns
        self._bars: deque[MarketDataItem] = deque(maxlen=maxlen)

    @property
    def bars(self) -> tuple[MarketDataItem, ...]:
        return tuple(self._bars)

    def seed(self, values: Iterable[MarketDataItem]) -> None:
        rows = tuple(values)
        self._bars.clear()
        for item in rows:
            self._append(item, release=None, seeding=True)

    def append_closed(
        self,
        item: MarketDataItem,
        *,
        release: Callable[[tuple[MarketDataItem, ...]], None],
    ) -> bool:
        return self._append(item, release=release, seeding=False)

    def _append(self, item, *, release, seeding: bool) -> bool:
        if item.interval != self.interval or item.payload.get("is_final") is not True:
            raise ValueError("FIFO accepts only final bars for its declared interval")
        open_ns = int(item.payload["open_time_ns"])
        if self._bars:
            previous_open = int(self._bars[-1].payload["open_time_ns"])
            if open_ns == previous_open:
                if item.revision > self._bars[-1].revision:
                    self._bars[-1] = item
                return False
            expected_open = (
                self._next_open_ns(previous_open)
                if self._next_open_ns is not None
                else previous_open + self.interval_ns
            )
            if open_ns != expected_open:
                raise ValueError("closed-bar FIFO detected a gap or late bar")
        self._bars.append(item)
        if not seeding:
            release(tuple(self._bars))
        return True
