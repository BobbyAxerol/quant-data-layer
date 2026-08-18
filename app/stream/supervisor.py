import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Optional

from app.stream.demand_registry import feed_key_for, parse_feed_key


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
    data_timeout_count: int = 0
    last_data_timeout_at: Optional[float] = None
    outage_started_at: Optional[float] = None
    last_disconnected_at: Optional[float] = None
    last_recovered_at: Optional[float] = None
    last_outage_seconds: Optional[float] = None
    max_outage_seconds: float = 0.0
    reconnect_success_count: int = 0
    gap_detected_count: int = 0
    gap_fill_success_count: int = 0
    gap_fill_failure_count: int = 0
    last_gap_fill_at: Optional[float] = None

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
            "data_timeout_count": self.data_timeout_count,
            "last_data_timeout_at": _iso(self.last_data_timeout_at),
            "outage_started_at": _iso(self.outage_started_at),
            "last_disconnected_at": _iso(self.last_disconnected_at),
            "last_recovered_at": _iso(self.last_recovered_at),
            "last_outage_seconds": round(self.last_outage_seconds, 3) if self.last_outage_seconds is not None else None,
            "max_outage_seconds": round(self.max_outage_seconds, 3),
            "reconnect_success_count": self.reconnect_success_count,
            "gap_detected_count": self.gap_detected_count,
            "gap_fill_success_count": self.gap_fill_success_count,
            "gap_fill_failure_count": self.gap_fill_failure_count,
            "last_gap_fill_at": _iso(self.last_gap_fill_at),
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
        first_frame_timeout_seconds: float = 15.0,
    ):
        self.stale_after_seconds = stale_after_seconds
        self.startup_grace_seconds = startup_grace_seconds
        self.first_frame_timeout_seconds = first_frame_timeout_seconds
        self.strict_feed_health = strict_feed_health
        self.sample_limit = sample_limit
        self.started_at = _now()
        self.shards: Dict[str, ShardState] = {}
        self.feeds: Dict[str, FeedState] = {}
        self.queue_size = 0
        self.queue_maxsize = 0
        self.queue_drop_count = 0
        self.queue_drop_window_seconds = 300.0
        self._queue_drop_times: deque[float] = deque(maxlen=100_000)
        self.queue_pressure_count = 0
        self.last_queue_pressure_at: Optional[float] = None
        self.redis_error_count = 0
        self.last_redis_error: Optional[str] = None
        self.publisher_batch_count = 0
        self.publisher_item_count = 0
        self.last_publisher_at: Optional[float] = None

    @staticmethod
    def feed_key(
        feed: str,
        symbol: str,
        interval: Optional[str] = None,
        *,
        source: str = "unknown",
    ) -> str:
        return feed_key_for(source, feed, symbol, interval)

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
        key = self.feed_key(feed, symbol, interval, source=source)
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

    def mark_connected(self, shard_id: str) -> bool:
        shard = self.shards.get(shard_id)
        if shard:
            recovered = shard.outage_started_at is not None
            now = _now()
            shard.status = "connected"
            shard.last_connected_at = now
            shard.last_error = None
            if recovered:
                duration = max(0.0, now - float(shard.outage_started_at))
                shard.last_outage_seconds = duration
                shard.max_outage_seconds = max(shard.max_outage_seconds, duration)
                shard.last_recovered_at = now
                shard.reconnect_success_count += 1
                shard.outage_started_at = None
            return recovered
        return False

    def mark_message(self, shard_id: str) -> None:
        shard = self.shards.get(shard_id)
        if shard:
            shard.message_count += 1
            shard.last_message_at = _now()
            if shard.last_error and shard.last_error.startswith("data_timeout:"):
                shard.last_error = None

    def mark_data_timeout(self, shard_id: str, reason: str) -> None:
        shard = self.shards.get(shard_id)
        if shard:
            now = _now()
            shard.data_timeout_count += 1
            shard.last_data_timeout_at = now
            shard.last_error = f"data_timeout:{reason}"

    def mark_parse_error(self, shard_id: str, error: Exception) -> None:
        shard = self.shards.get(shard_id)
        if shard:
            shard.parse_error_count += 1
            shard.last_error = str(error)

    def mark_reconnect(self, shard_id: str, error: Exception | str) -> None:
        shard = self.shards.get(shard_id)
        if shard:
            now = _now()
            shard.status = "reconnecting"
            shard.reconnect_count += 1
            shard.last_error = str(error)
            shard.last_disconnected_at = now
            if shard.outage_started_at is None:
                shard.outage_started_at = now

    def record_gap_detected(self, shard_id: str) -> None:
        shard = self.shards.get(shard_id)
        if shard:
            shard.gap_detected_count += 1

    def record_gap_fill(self, shard_id: str, *, success: bool) -> None:
        shard = self.shards.get(shard_id)
        if shard:
            if success:
                shard.gap_fill_success_count += 1
            else:
                shard.gap_fill_failure_count += 1
            shard.last_gap_fill_at = _now()

    def record_queue_size(self, size: int, maxsize: int) -> None:
        self.queue_size = int(size)
        self.queue_maxsize = int(maxsize)

    def record_queue_drop(self, source: str, shard_id: Optional[str] = None) -> None:
        self.queue_drop_count += 1
        self._queue_drop_times.append(_now())
        if shard_id and shard_id in self.shards:
            self.shards[shard_id].queue_drop_count += 1

    def record_queue_pressure(self) -> None:
        self.queue_pressure_count += 1
        self.last_queue_pressure_at = _now()

    def record_redis_error(self, error: Exception) -> None:
        self.redis_error_count += 1
        self.last_redis_error = str(error)

    def record_publish(self, item: Dict[str, Any]) -> None:
        key = str(item.get("key") or "")
        data = item.get("data") or {}
        source = str(item.get("source") or data.get("source") or "")
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

        feed_key = self.feed_key(feed, symbol, interval, source=source)
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

    def _source_states(self, now: float) -> Dict[str, Dict[str, Any]]:
        states: Dict[str, Dict[str, Any]] = {}
        for source in sorted({shard.source for shard in self.shards.values()}):
            shards = [shard for shard in self.shards.values() if shard.source == source]
            connected = [shard for shard in shards if shard.status == "connected"]
            producing = []
            waiting = []
            stale = []
            unavailable = []
            for shard in shards:
                session_has_frame = bool(
                    shard.status == "connected"
                    and shard.last_message_at
                    and shard.last_connected_at
                    and shard.last_message_at >= shard.last_connected_at
                )
                if session_has_frame:
                    age = max(0.0, now - float(shard.last_message_at))
                    if age <= self.stale_after_seconds:
                        producing.append(shard)
                    else:
                        stale.append(shard)
                elif shard.status == "connected" and shard.last_connected_at:
                    age = max(0.0, now - float(shard.last_connected_at))
                    if age <= self.first_frame_timeout_seconds:
                        waiting.append(shard)
                    else:
                        unavailable.append(shard)
                else:
                    unavailable.append(shard)

            if len(producing) == len(shards) and shards:
                status = "ready"
            elif producing:
                status = "degraded"
            elif waiting and len(waiting) == len(shards):
                status = "starting"
            else:
                status = "unavailable"
            states[source] = {
                "feed": "trade" if source.endswith("_trade") else "kline",
                "status": status,
                "transport_ready": len(connected) == len(shards) and bool(shards),
                "data_ready": status == "ready",
                "shard_count": len(shards),
                "connected_count": len(connected),
                "producing_count": len(producing),
                "waiting_first_frame_count": len(waiting),
                "stale_count": len(stale),
                "unavailable_count": len(unavailable),
                "first_frame_timeout_seconds": self.first_frame_timeout_seconds,
                "stale_after_seconds": self.stale_after_seconds,
            }
        return states

    def snapshot(
        self,
        now: Optional[float] = None,
        *,
        demanded_feed_keys: set[str] | None = None,
    ) -> Dict[str, Any]:
        now = now or _now()
        demanded_feed_keys = demanded_feed_keys or set()
        uptime_seconds = max(0.0, now - self.started_at)
        expected_feeds = [feed for feed in self.feeds.values() if feed.expected]
        missing = [feed for feed in expected_feeds if not feed.last_published_at]
        broad_health_missing = [
            feed
            for feed in missing
            if feed.feed != "trade" and uptime_seconds > self.startup_grace_seconds
        ]
        stale = [
            feed
            for feed in expected_feeds
            if feed.last_published_at and feed.age_seconds(now) > self.stale_after_seconds
        ]
        by_key = {feed.feed_key: feed for feed in self.feeds.values()}
        demanded_states = []
        for key in sorted(demanded_feed_keys):
            parsed = parse_feed_key(key)
            demanded_states.append(
                by_key.get(key)
                or FeedState(
                    feed_key=key,
                    source=str(parsed["source"]),
                    feed=str(parsed["feed"]),
                    symbol=str(parsed["symbol"]),
                    interval=parsed["interval"],
                    expected=True,
                )
            )
        demanded_missing = [feed for feed in demanded_states if not feed.last_published_at]
        demanded_stale = [
            feed
            for feed in demanded_states
            if feed.last_published_at and feed.age_seconds(now) > self.stale_after_seconds
        ]
        reconnect_count = sum(shard.reconnect_count for shard in self.shards.values())
        connected_shards = [shard for shard in self.shards.values() if shard.status == "connected"]
        source_states = self._source_states(now)
        unavailable_sources = [
            source
            for source, state in source_states.items()
            if state["status"] in {"degraded", "unavailable"}
        ]
        starting_sources = [
            source for source, state in source_states.items() if state["status"] == "starting"
        ]

        health_warnings = []
        cutoff = now - self.queue_drop_window_seconds
        while self._queue_drop_times and self._queue_drop_times[0] < cutoff:
            self._queue_drop_times.popleft()
        recent_queue_drops = len(self._queue_drop_times)
        if recent_queue_drops:
            health_warnings.append("queue_drop_observed")
            health_warnings.append("recent_queue_drop_observed")
        if demanded_missing and uptime_seconds > self.startup_grace_seconds:
            health_warnings.append("missing_demanded_feeds")
        if demanded_stale:
            health_warnings.append("stale_demanded_feeds")
        if broad_health_missing:
            health_warnings.append("missing_expected_feeds")
        if stale:
            health_warnings.append("stale_expected_feeds")
        if unavailable_sources:
            health_warnings.append("source_data_unavailable")

        if self.redis_error_count:
            status = "degraded"
        elif not self.shards:
            status = "not_started"
        elif unavailable_sources:
            status = "degraded"
        elif not connected_shards:
            status = "starting"
        elif starting_sources:
            status = "starting"
        elif demanded_stale or (demanded_missing and uptime_seconds > self.startup_grace_seconds):
            status = "degraded"
        elif self.strict_feed_health and (recent_queue_drops or broad_health_missing or stale):
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
                "recent_drop_count": recent_queue_drops,
                "pressure_count": self.queue_pressure_count,
                "last_pressure_at": _iso(self.last_queue_pressure_at),
                "window_seconds": self.queue_drop_window_seconds,
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
            "sources": source_states,
            "feeds": {
                "expected_count": len(expected_feeds),
                "observed_count": len([feed for feed in expected_feeds if feed.last_published_at]),
                "missing_count": len(missing),
                "broad_missing_count": len(missing),
                "health_missing_count": len(broad_health_missing),
                "stale_count": len(stale),
                "broad_stale_count": len(stale),
                "demanded_count": len(demanded_states),
                "demanded_missing_count": len(demanded_missing),
                "demanded_stale_count": len(demanded_stale),
                "missing_samples": [feed.to_dict(now) for feed in missing[: self.sample_limit]],
                "health_missing_samples": [
                    feed.to_dict(now) for feed in broad_health_missing[: self.sample_limit]
                ],
                "demanded_missing_samples": [feed.to_dict(now) for feed in demanded_missing[: self.sample_limit]],
                "demanded_stale_samples": [feed.to_dict(now) for feed in demanded_stale[: self.sample_limit]],
                "stale_samples": [feed.to_dict(now) for feed in stale[: self.sample_limit]],
            },
        }
