from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum

from qdl.domain.lifecycle import BarLifecycle


class FeedType(str, Enum):
    TRADE = "trade"
    BBO = "bbo"
    BAR = "bar"
    BOOK = "book"


class DeliveryPolicy(str, Enum):
    LOSSLESS = "LOSSLESS"
    LATEST_STATE = "LATEST_STATE"
    LIFECYCLE_COALESCE = "LIFECYCLE_COALESCE"


LOSSLESS_FEEDS = frozenset({FeedType.TRADE, FeedType.BOOK})


def delivery_policy(
    feed: FeedType,
    *,
    bar_lifecycle: BarLifecycle | None = None,
) -> DeliveryPolicy:
    if feed in LOSSLESS_FEEDS:
        return DeliveryPolicy.LOSSLESS
    if feed is FeedType.BAR:
        if bar_lifecycle in {None, BarLifecycle.UNSPECIFIED}:
            raise ValueError("bar delivery requires an explicit lifecycle")
        if bar_lifecycle is BarLifecycle.IN_PROGRESS:
            return DeliveryPolicy.LIFECYCLE_COALESCE
        return DeliveryPolicy.LOSSLESS
    return DeliveryPolicy.LATEST_STATE


@dataclass(frozen=True, order=True)
class Subscription:
    venue: str
    market: str
    feed: FeedType
    native_symbol: str
    interval: str | None = None
    priority: int = 100

    def __post_init__(self) -> None:
        if not all((self.venue.strip(), self.market.strip(), self.native_symbol.strip())):
            raise ValueError("venue, market and native_symbol are required")
        if self.feed is FeedType.BAR and not self.interval:
            raise ValueError("bar subscriptions require interval")
        if self.feed is not FeedType.BAR and self.interval is not None:
            raise ValueError("interval is only valid for bar subscriptions")

    @property
    def key(self) -> str:
        values = (
            self.venue.upper(),
            self.market.upper(),
            self.feed.value,
            self.interval or "",
            self.native_symbol.upper(),
        )
        return ":".join(values)


@dataclass(frozen=True)
class ConnectionShard:
    shard_id: str
    venue: str
    market: str
    feed: FeedType
    subscriptions: tuple[Subscription, ...]
    config_revision: int


def plan_shards(
    subscriptions: set[Subscription], *, max_streams: int, config_revision: int
) -> tuple[ConnectionShard, ...]:
    if max_streams <= 0:
        raise ValueError("max_streams must be positive")
    grouped: dict[tuple[str, str, FeedType], list[Subscription]] = {}
    for item in sorted(subscriptions, key=lambda value: (value.priority, value.key)):
        grouped.setdefault((item.venue.upper(), item.market.upper(), item.feed), []).append(item)

    shards: list[ConnectionShard] = []
    for (venue, market, feed), items in sorted(grouped.items(), key=lambda item: str(item[0])):
        for offset in range(0, len(items), max_streams):
            batch = tuple(items[offset : offset + max_streams])
            digest = hashlib.sha256("\n".join(item.key for item in batch).encode()).hexdigest()[:12]
            shards.append(
                ConnectionShard(
                    shard_id=f"{venue.lower()}-{market.lower()}-{feed.value}-{digest}",
                    venue=venue,
                    market=market,
                    feed=feed,
                    subscriptions=batch,
                    config_revision=config_revision,
                )
            )
    return tuple(shards)
