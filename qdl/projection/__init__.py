"""Idempotent latest-state and V1 compatibility projection primitives."""

from qdl.projection.trade import (
    InMemoryProjectionTarget,
    ProjectionRecord,
    TradeProjector,
)
from qdl.projection.market import MarketProjector

try:
    from qdl.projection.redis_target import RedisProjectionTarget
except ImportError:  # Redis remains an optional adapter for the domain package.
    RedisProjectionTarget = None

__all__ = [
    "InMemoryProjectionTarget",
    "MarketProjector",
    "ProjectionRecord",
    "RedisProjectionTarget",
    "TradeProjector",
]
