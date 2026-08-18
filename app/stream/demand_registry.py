from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from typing import Any


def feed_source_scope(source: str) -> str:
    value = str(source or "unknown").lower()
    if value.startswith("binance_futures") or value in {"binance", "binance_auto", "binance_usdm"}:
        return "binance_usdm"
    if value.startswith("binance_spot"):
        return "binance_spot"
    return value


def feed_key_for(source: str, feed: str, symbol: str, interval: str | None = None) -> str:
    prefix = f"{str(feed).lower()}:{feed_source_scope(source)}"
    if interval:
        return f"{prefix}:{interval}:{str(symbol).upper()}"
    return f"{prefix}:{str(symbol).upper()}"


def parse_feed_key(value: str) -> dict[str, str | None]:
    parts = str(value).split(":")
    if len(parts) == 4:
        return {"feed": parts[0], "source": parts[1], "interval": parts[2], "symbol": parts[3]}
    if len(parts) == 3:
        return {"feed": parts[0], "source": parts[1], "interval": None, "symbol": parts[2]}
    return {"feed": parts[0] if parts else "unknown", "source": "unknown", "interval": None, "symbol": parts[-1] if parts else ""}


@dataclass(frozen=True)
class FeedDemand:
    source: str
    feed: str
    symbol: str
    interval: str | None = None
    reason: str = "runtime_request"

    @property
    def feed_key(self) -> str:
        return feed_key_for(self.source, self.feed, self.symbol, self.interval)

    def normalized(self) -> "FeedDemand":
        return FeedDemand(
            source=str(self.source or "unknown").lower(),
            feed=str(self.feed).lower(),
            symbol=str(self.symbol).upper(),
            interval=str(self.interval) if self.interval else None,
            reason=str(self.reason or "runtime_request"),
        )


class FeedDemandRegistry:
    """TTL-backed runtime demand leases; broad configured feeds are not leases."""

    def __init__(self, redis_client, *, prefix: str = "feed:demand:lease") -> None:
        self.redis = redis_client
        self.prefix = prefix.rstrip(":")

    @staticmethod
    def _digest(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()[:20]

    def _key(self, owner_id: str, feed_key: str) -> str:
        return f"{self.prefix}:{self._digest(owner_id)}:{self._digest(feed_key)}"

    async def upsert(
        self,
        owner_id: str,
        demands: list[FeedDemand],
        *,
        ttl_seconds: int,
    ) -> dict[str, Any]:
        owner = str(owner_id).strip()
        if not owner:
            raise ValueError("owner_id is required")
        ttl = max(30, min(int(ttl_seconds), 3600))
        now = time.time()
        normalized = {item.normalized().feed_key: item.normalized() for item in demands}
        if not normalized:
            return {"owner_id": owner, "lease_count": 0, "ttl_seconds": ttl}
        async with self.redis.pipeline(transaction=False) as pipe:
            for feed_key, demand in normalized.items():
                payload = {
                    **asdict(demand),
                    "owner_id": owner,
                    "feed_key": feed_key,
                    "renewed_at_unix": now,
                    "expires_at_unix": now + ttl,
                }
                pipe.set(self._key(owner, feed_key), json.dumps(payload, separators=(",", ":")), ex=ttl)
            await pipe.execute()
        return {"owner_id": owner, "lease_count": len(normalized), "ttl_seconds": ttl}

    async def touch_request(
        self,
        *,
        owner_id: str,
        source: str,
        feed: str,
        symbol: str,
        interval: str | None = None,
        ttl_seconds: int = 180,
    ) -> bool:
        try:
            await self.upsert(
                owner_id,
                [FeedDemand(source, feed, symbol, interval, "api_request")],
                ttl_seconds=ttl_seconds,
            )
            return True
        except Exception:
            # Demand telemetry must not turn an otherwise serviceable read into a 5xx.
            return False

    async def release_owner(self, owner_id: str) -> int:
        pattern = f"{self.prefix}:{self._digest(str(owner_id).strip())}:*"
        keys = [key async for key in self.redis.scan_iter(match=pattern, count=100)]
        return int(await self.redis.delete(*keys)) if keys else 0

    async def snapshot(self) -> dict[str, Any]:
        keys = [key async for key in self.redis.scan_iter(match=f"{self.prefix}:*", count=200)]
        raw_records = await self.redis.mget(keys) if keys else []
        records: list[dict[str, Any]] = []
        for raw in raw_records:
            if not raw:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                records.append(json.loads(raw))
            except (TypeError, ValueError):
                continue

        aggregate: dict[str, dict[str, Any]] = {}
        for item in records:
            feed_key = str(item.get("feed_key") or "")
            if not feed_key:
                continue
            entry = aggregate.setdefault(
                feed_key,
                {
                    "feed_key": feed_key,
                    "source": item.get("source"),
                    "feed": item.get("feed"),
                    "symbol": item.get("symbol"),
                    "interval": item.get("interval"),
                    "refcount": 0,
                    "owners": [],
                    "reasons": [],
                    "expires_at_unix": 0.0,
                },
            )
            entry["refcount"] += 1
            entry["owners"].append(item.get("owner_id"))
            entry["reasons"].append(item.get("reason"))
            entry["expires_at_unix"] = max(
                float(entry["expires_at_unix"] or 0),
                float(item.get("expires_at_unix") or 0),
            )
        items = sorted(aggregate.values(), key=lambda item: item["feed_key"])
        owners = sorted({str(item.get("owner_id")) for item in records if item.get("owner_id")})
        return {
            "lease_count": len(records),
            "owner_count": len(owners),
            "owners": owners,
            "demanded_feed_count": len(items),
            "feed_keys": [item["feed_key"] for item in items],
            "items": items,
        }
