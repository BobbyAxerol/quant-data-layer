from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.providers.binance import rest as binance_rest
from app.stream.demand_registry import parse_feed_key


logger = logging.getLogger(__name__)


def _fixed_interval_ms(interval: str) -> int | None:
    value = int(interval[:-1])
    unit = interval[-1]
    multiplier = {
        "s": 1_000,
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
    }.get(unit)
    return value * multiplier if multiplier else None


def _next_close_ms(now_ms: int, interval: str, settle_ms: int) -> int:
    fixed = _fixed_interval_ms(interval)
    if fixed:
        return ((now_ms // fixed) + 1) * fixed + settle_ms

    now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    if interval.endswith("w"):
        weeks = int(interval[:-1])
        monday = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        boundary = monday + timedelta(weeks=weeks)
    elif interval.endswith("M"):
        months = int(interval[:-1])
        year = now.year
        month = now.month + months
        year += (month - 1) // 12
        month = ((month - 1) % 12) + 1
        boundary = datetime(year, month, 1, tzinfo=timezone.utc)
    else:
        raise ValueError(f"Unsupported recovery interval: {interval}")
    return int(boundary.timestamp() * 1000) + settle_ms


def _payload_marker(payload: dict[str, Any] | None) -> tuple[int | None, bool]:
    if not isinstance(payload, dict):
        return None, False
    kline = payload.get("k") if isinstance(payload.get("k"), dict) else payload
    try:
        open_time = int(kline.get("t"))
    except (TypeError, ValueError):
        return None, False
    return open_time, bool(kline.get("x", False))


def _closed_rows(rows: list[Any], now_ms: int) -> list[list[Any]]:
    closed: list[list[Any]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) <= 6:
            continue
        try:
            open_time = int(row[0])
            close_time = int(row[6])
        except (TypeError, ValueError):
            continue
        if close_time > now_ms or any(row[index] is None for index in range(1, 6)):
            continue
        if open_time >= close_time:
            continue
        closed.append(row)
    return sorted(closed, key=lambda row: int(row[0]))


def _recovery_event(symbol: str, interval: str, row: list[Any]) -> dict[str, Any]:
    return {
        "e": "kline_recovery",
        "E": int(time.time() * 1000),
        "s": symbol,
        "k": {
            "t": row[0],
            "T": row[6],
            "s": symbol,
            "i": interval,
            "o": row[1],
            "c": row[4],
            "h": row[2],
            "l": row[3],
            "v": row[5],
            "x": True,
        },
        "recovery_source": "BINANCE_REST_GAP_FILL",
        "provider": "binance",
        "market": "binance_usdm",
        "authoritative": True,
    }


@dataclass(frozen=True)
class KlineRecoveryConfig:
    enabled: bool = True
    poll_seconds: float = 2.0
    settle_seconds: float = 1.0
    concurrency: int = 4
    max_limit: int = 1000
    max_backoff_seconds: float = 300.0
    queue_put_timeout_seconds: float = 2.0


class DemandKlineRecovery:
    """Demand-only Binance REST recovery for closed bars.

    This is a recovery projector, not a replacement WebSocket health signal.
    Provider rows retain their native values and only fully closed rows enter
    the existing V1 kline projection.
    """

    def __init__(
        self,
        *,
        queue: asyncio.Queue,
        redis_cache: Any,
        demand_registry: Any,
        config: KlineRecoveryConfig,
        fetcher: Callable[..., dict[str, Any]] = binance_rest.fetch_klines,
    ) -> None:
        self.queue = queue
        self.redis_cache = redis_cache
        self.demand_registry = demand_registry
        self.config = config
        self.fetcher = fetcher
        self.running = False
        self.last_poll_at: float | None = None
        self.last_success_at: float | None = None
        self.last_error: str | None = None
        self.provider_fetch_count = 0
        self.emitted_count = 0
        self.deduplicated_count = 0
        self.rejected_open_or_invalid_count = 0
        self.failure_count = 0
        self.active_demand_count = 0
        self._next_due_ms: dict[str, int] = {}
        self._failures: dict[str, int] = {}
        self._semaphore = asyncio.Semaphore(max(1, min(config.concurrency, 16)))

    def snapshot(self) -> dict[str, Any]:
        return {
            "enabled": self.config.enabled,
            "running": self.running,
            "status": (
                "disabled"
                if not self.config.enabled
                else "degraded"
                if self.last_error
                else "ready"
                if self.running
                else "stopped"
            ),
            "active_demand_count": self.active_demand_count,
            "provider_fetch_count": self.provider_fetch_count,
            "emitted_count": self.emitted_count,
            "deduplicated_count": self.deduplicated_count,
            "rejected_open_or_invalid_count": self.rejected_open_or_invalid_count,
            "failure_count": self.failure_count,
            "last_poll_at": self.last_poll_at,
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
        }

    async def run(self) -> None:
        if not self.config.enabled:
            return
        self.running = True
        try:
            while True:
                try:
                    await self.poll_once()
                except Exception as exc:
                    self.failure_count += 1
                    self.last_error = f"{type(exc).__name__}: {exc}"
                    logger.warning("Binance kline recovery poll failed: %s", exc)
                await asyncio.sleep(max(0.25, self.config.poll_seconds))
        except asyncio.CancelledError:
            raise
        finally:
            self.running = False

    async def poll_once(self, *, now_ms: int | None = None) -> dict[str, int]:
        now_ms = now_ms or int(time.time() * 1000)
        self.last_poll_at = time.time()
        demands = await self.demand_registry.snapshot()
        selected: list[dict[str, str | None]] = []
        invalid_demand_count = 0
        active_keys: set[str] = set()
        for key in demands.get("feed_keys", []):
            parsed = parse_feed_key(key)
            if parsed["source"] != "binance_usdm" or parsed["feed"] != "kline":
                continue
            if not parsed["symbol"] or not parsed["interval"]:
                continue
            try:
                normalized_interval = binance_rest.normalize_interval(str(parsed["interval"]))
            except ValueError as exc:
                invalid_demand_count += 1
                self.failure_count += 1
                self.last_error = f"invalid demand {key}: {exc}"
                continue
            parsed["interval"] = normalized_interval
            selected.append(parsed)
            active_keys.add(
                f"kline:binance_usdm:{normalized_interval}:{parsed['symbol']}"
            )

        self.active_demand_count = len(selected)
        for key in set(self._next_due_ms) - active_keys:
            self._next_due_ms.pop(key, None)
            self._failures.pop(key, None)

        due = [
            item
            for item in selected
            if now_ms >= self._next_due_ms.get(
                f"kline:binance_usdm:{item['interval']}:{item['symbol']}", 0
            )
        ]
        if not due:
            if not selected and invalid_demand_count == 0:
                self.last_error = None
            return {"due": 0, "emitted": 0, "failed": invalid_demand_count}

        outcomes = await asyncio.gather(
            *(self._recover_one(item, now_ms=now_ms) for item in due),
            return_exceptions=True,
        )
        emitted = sum(value for value in outcomes if isinstance(value, int))
        failed = sum(1 for value in outcomes if isinstance(value, BaseException))
        if failed == 0:
            self.last_error = None
        return {"due": len(due), "emitted": emitted, "failed": failed}

    async def _recover_one(self, item: dict[str, str | None], *, now_ms: int) -> int:
        symbol = str(item["symbol"])
        interval = str(item["interval"])
        feed_key = f"kline:binance_usdm:{interval}:{symbol}"
        try:
            async with self._semaphore:
                existing = await self.redis_cache.get_binance_kline_last(symbol, interval)
                existing_open, existing_final = _payload_marker(existing)
                interval_ms = _fixed_interval_ms(interval)
                if existing_open is None or interval_ms is None:
                    limit = 3
                else:
                    missing = math.ceil(max(0, now_ms - existing_open) / interval_ms) + 2
                    limit = min(self.config.max_limit, max(3, missing))
                self.provider_fetch_count += 1
                payload = await asyncio.to_thread(
                    self.fetcher,
                    symbol,
                    interval,
                    limit,
                    None,
                    None,
                    "usdm",
                )

            raw_rows = payload.get("data") or []
            rows = _closed_rows(raw_rows, now_ms)
            self.rejected_open_or_invalid_count += max(0, len(raw_rows) - len(rows))
            if existing_open is None and rows:
                rows = rows[-1:]

            emitted = 0
            current_open = existing_open
            current_final = existing_final
            for row in rows:
                open_time = int(row[0])
                if current_open is not None and (
                    open_time < current_open or (open_time == current_open and current_final)
                ):
                    self.deduplicated_count += 1
                    continue
                event = _recovery_event(symbol, interval, row)
                await asyncio.wait_for(
                    self.queue.put(("binance_futures_kline", event)),
                    timeout=self.config.queue_put_timeout_seconds,
                )
                current_open = open_time
                current_final = True
                emitted += 1

            self.emitted_count += emitted
            self.last_success_at = time.time()
            self._failures[feed_key] = 0
            self._next_due_ms[feed_key] = _next_close_ms(
                now_ms,
                interval,
                int(self.config.settle_seconds * 1000),
            )
            return emitted
        except Exception as exc:
            self.failure_count += 1
            self.last_error = f"{type(exc).__name__}: {exc}"
            failures = self._failures.get(feed_key, 0) + 1
            self._failures[feed_key] = failures
            backoff = min(
                self.config.max_backoff_seconds,
                max(self.config.poll_seconds, self.config.poll_seconds * (2 ** min(failures, 8))),
            )
            self._next_due_ms[feed_key] = now_ms + int(backoff * 1000)
            logger.warning(
                "Binance demanded kline recovery failed symbol=%s interval=%s backoff=%.1fs error=%s",
                symbol,
                interval,
                backoff,
                exc,
            )
            raise
