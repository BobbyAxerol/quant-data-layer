from __future__ import annotations

from qdl._compat import StrEnum
from qdl.domain.lifecycle import BarLifecycle
from qdl.query.contracts import FeedType


class DeliveryMode(StrEnum):
    LOSSLESS = "LOSSLESS"
    LATEST_STATE = "LATEST_STATE"
    LIFECYCLE_COALESCE = "LIFECYCLE_COALESCE"


def delivery_mode(
    feed: FeedType,
    *,
    bar_lifecycle: BarLifecycle | None = None,
) -> DeliveryMode:
    if feed in {FeedType.TRADE, FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA}:
        return DeliveryMode.LOSSLESS
    if feed is FeedType.BAR:
        if bar_lifecycle is None or bar_lifecycle is BarLifecycle.UNSPECIFIED:
            raise ValueError("bar delivery requires an explicit lifecycle")
        if bar_lifecycle is BarLifecycle.IN_PROGRESS:
            return DeliveryMode.LIFECYCLE_COALESCE
        return DeliveryMode.LOSSLESS
    if feed in {
        FeedType.FUNDING_RATE,
        FeedType.LONG_SHORT_RATIO,
        FeedType.TAKER_FLOW,
        FeedType.BASIS,
        FeedType.CONTRACT_METADATA,
    }:
        return DeliveryMode.LOSSLESS
    if feed in {
        FeedType.QUOTE,
        FeedType.TICKER,
        FeedType.OPEN_INTEREST,
        FeedType.MARK_INDEX_PRICE,
    }:
        return DeliveryMode.LATEST_STATE
    raise ValueError(f"delivery policy is undefined for feed {feed.value}")
