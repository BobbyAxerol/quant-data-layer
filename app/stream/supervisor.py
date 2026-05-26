import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def _now() -> float:
    return time.time()


def _iso(ts: Optional[float]) -> Optional[str]:
    if not ts:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


@dataclass
class ShardState:
    shard_id: str
    source: str
    url_preview: str
    status: str = "created"
    reconnect_count: int = 0
    message_count: int = 0
    parse_error_count: int = 0
    queue_drop_count: int = 0
    last_connected_at: Optional[float] = None
    last_message_at: Optional[float] = None
    last_error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "source": self.source,
            "status": self.status,
            "reconnect_count": self.reconnect_count,
            "message_count": self.message_count,
            "parse_error_count": self.parse_error_count,
            "queue_drop_count": self.queue_drop_count,
            "last_connected_at": _iso(self.last_connected_at),
            "last_message_at": _iso(self.last_message_at),
            "last_error": self.last_error,
            "url_preview": self.url_preview,
        }


@dataclass
class FeedState:
    feed_key: str
    source: str
    feed: str
    symbol: str
    interval: Optional[str] = None
    expected: bool = True
    publish_count: int = 0
    last_event_at: Optional[float] = None
    last_published_at: Optional[float] = None
    last_key: Optional[str] = None

    def age_seconds(self, now: Optional[float] = None) -> Optional[float]:
        if not self.last_published_at:
            return None
        return max(0.0, (now or _now()) - self.last_published_at)

    def to_dict(self, now: Optional[float] = None) -> Dict[str, Any]:
        age = self.age_seconds(now)
        return {
            "feed_key": self.feed_key,
            "source": self.source,
            "feed": self.feed,
            "symbol": self.symbol,
            "interval": self.interval,
            "expected": self.expected,
            "publish_count": self.publish_count,
            "last_event_at": _iso(self.last_event_at),
            "last_published_at": _iso(self.last_published_at),
            "age_seconds": round(age, 3) if age is not None else None,
            "last_key": self.last_key,
        }


class StreamSupervisor:
    """
    In-memory runtime supervisor for websocket ingestion.

    This intentionally has no network or Redis dependency. Websocket tasks and
    publisher tasks call it as they progress, while health endpoints read a
    compact snapshot from it.
    """

    def __init__(
        self,
        stale_after_seconds: float = 180.0,
        sample_limit: int = 10,
        startup_grace_seconds: float = 180.0,
        strict_feed_health: bool = False,
    ):
        self.stale_after_seconds = stale_after_seconds
        self.startup_grace_seconds = startup_grace_seconds
        self.strict_feed_health = strict_feed_health
        self.sample_limit = sample_limit
        self.started_at = _now()
        self.shards: Dict[str, ShardState] = {}
        self.feeds: Dict[str, FeedState] = {}
        self.queue_size = 0
        self.queue_maxsize = 0
        self.queue_drop_count = 0
        self.redis_error_count = 0
        self.last_redis_error: Optional[str] = None
        self.publisher_batch_count = 0
        self.publisher_item_count = 0
        self.last_publisher_at: Optional[float] = None

    @staticmethod
    def feed_key(feed: str, symbol: str, interval: Optional[str] = None) -> str:
        normalized_symbol = symbol.upper()
        if interval:
            return f"{feed}:{interval}:{normalized_symbol}"
        return f"{feed}:{normalized_symbol}"

    def register_shard(self, source: str, url: str, shard_id: Optional[str] = None) -> str:
        shard_id = shard_id or f"{source}:{len(self.shards)}"
        self.shards[shard_id] = ShardState(
            shard_id=shard_id,
            source=source,
            url_preview=url[:160],
        )
        return shard_id

    def expect_feed(
        self,
        source: str,
        feed: str,
        symbol: str,
        interval: Optional[str] = None,
    ) -> None:
        key = self.feed_key(feed, symbol, interval)
        self.feeds.setdefault(
            key,
            FeedState(
                feed_key=key,
                source=source,
                feed=feed,
                symbol=symbol.upper(),
                interval=interval,
                expected=True,
            ),
        )

    def mark_connecting(self, shard_id: str) -> None:
        shard = self.shards.get(shard_id)
        if shard:
            shard.status = "connecting"

    def mark_connected(self, shard_id: str) -> None:
        shard = self.shards.get(shard_id)
        if shard:
            shard.status = "connected"
            shard.last_connected_at = _now()
            shard.last_error = None

    def mark_message(self, shard_id: str) -> None:
        shard = self.shards.get(shard_id)
        if shard:
            shard.message_count += 1
            shard.last_message_at = _now()

    def mark_parse_error(self, shard_id: str, error: Exception) -> None:
        shard = self.shards.get(shard_id)
        if shard:
            shard.parse_error_count += 1
            shard.last_error = str(error)

    def mark_reconnect(self, shard_id: str, error: Exception | str) -> None:
        shard = self.shards.get(shard_id)
        if shard:
            shard.status = "reconnecting"
            shard.reconnect_count += 1
            shard.last_error = str(error)

    def record_queue_size(self, size: int, maxsize: int) -> None:
        self.queue_size = int(size)
        self.queue_maxsize = int(maxsize)

    def record_queue_drop(self, source: str, shard_id: Optional[str] = None) -> None:
        self.queue_drop_count += 1
        if shard_id and shard_id in self.shards:
            self.shards[shard_id].queue_drop_count += 1

    def record_redis_error(self, error: Exception) -> None:
        self.redis_error_count += 1
        self.last_redis_error = str(error)

    def record_publish(self, item: Dict[str, Any]) -> None:
        key = str(item.get("key") or "")
        data = item.get("data") or {}
        source = str(data.get("source") or "")
        feed = None
        symbol = None
        interval = None

        if key.startswith("trade:price:"):
            feed = "trade"
            symbol = key.split(":")[-1]
        elif key.startswith("kline:"):
            _, interval, symbol = key.split(":", 2)
            feed = "kline"
        elif key.startswith("vn:quote:") and not key.startswith("vn:quote:last:"):
            feed = "vn_quote"
            symbol = key.split(":")[-1]

        if not feed or not symbol:
            return

        event_ts = data.get("event_time") or data.get("trade_time") or data.get("timestamp")
        event_at = None
        if isinstance(event_ts, (int, float)) and event_ts:
            event_at = float(event_ts) / 1000.0 if event_ts > 10_000_000_000 else float(event_ts)

        feed_key = self.feed_key(feed, symbol, interval)
        state = self.feeds.setdefault(
            feed_key,
            FeedState(
                feed_key=feed_key,
                source=source,
                feed=feed,
                symbol=symbol.upper(),
                interval=interval,
                expected=False,
            ),
        )
        if source:
            state.source = source
        state.publish_count += 1
        state.last_event_at = event_at
        state.last_published_at = _now()
        state.last_key = key

        self.publisher_item_count += 1
        self.last_publisher_at = state.last_published_at

    def record_batch_published(self, size: int) -> None:
        self.publisher_batch_count += 1
        self.last_publisher_at = _now()

    def snapshot(self, now: Optional[float] = None) -> Dict[str, Any]:
        now = now or _now()
        uptime_seconds = max(0.0, now - self.started_at)
        expected_feeds = [feed for feed in self.feeds.values() if feed.expected]
        missing = [feed for feed in expected_feeds if not feed.last_published_at]
        health_missing = [
            feed
            for feed in missing
            if feed.feed != "trade" and uptime_seconds > self.startup_grace_seconds
        ]
        stale = [
            feed
            for feed in expected_feeds
            if feed.last_published_at and feed.age_seconds(now) > self.stale_after_seconds
        ]
        reconnect_count = sum(shard.reconnect_count for shard in self.shards.values())
        connected_shards = [shard for shard in self.shards.values() if shard.status == "connected"]

        health_warnings = []
        if self.queue_drop_count:
            health_warnings.append("queue_drop_observed")
        if health_missing:
            health_warnings.append("missing_expected_feeds")
        if stale:
            health_warnings.append("stale_expected_feeds")

        if self.redis_error_count:
            status = "degraded"
        elif not self.shards:
            status = "not_started"
        elif not connected_shards:
            status = "starting"
        elif self.strict_feed_health and health_warnings:
            status = "degraded"
        else:
            status = "ok"

        return {
            "status": status,
            "started_at": _iso(self.started_at),
            "uptime_seconds": round(uptime_seconds, 3),
            "stale_after_seconds": self.stale_after_seconds,
            "startup_grace_seconds": self.startup_grace_seconds,
            "strict_feed_health": self.strict_feed_health,
            "health_warnings": health_warnings,
            "queue": {
                "size": self.queue_size,
                "maxsize": self.queue_maxsize,
                "drop_count": self.queue_drop_count,
            },
            "publisher": {
                "batch_count": self.publisher_batch_count,
                "item_count": self.publisher_item_count,
                "last_publisher_at": _iso(self.last_publisher_at),
                "redis_error_count": self.redis_error_count,
                "last_redis_error": self.last_redis_error,
            },
            "shards": {
                "count": len(self.shards),
                "connected_count": len(connected_shards),
                "reconnect_count": reconnect_count,
                "items": [s.to_dict() for s in list(self.shards.values())[: self.sample_limit]],
            },
            "feeds": {
                "expected_count": len(expected_feeds),
                "observed_count": len([feed for feed in expected_feeds if feed.last_published_at]),
                "missing_count": len(missing),
                "health_missing_count": len(health_missing),
                "stale_count": len(stale),
                "missing_samples": [feed.to_dict(now) for feed in missing[: self.sample_limit]],
                "health_missing_samples": [feed.to_dict(now) for feed in health_missing[: self.sample_limit]],
                "stale_samples": [feed.to_dict(now) for feed in stale[: self.sample_limit]],
            },
        }
