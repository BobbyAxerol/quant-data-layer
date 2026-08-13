from __future__ import annotations

from app.api.context import DataLayerContext
from app.cache.redis_cache import RedisCache
from app.history.preload_vn import topup_existing_symbol_if_needed
from app.history.topup_coordinator import PreloadTopupCoordinator
from app.stream.demand_registry import FeedDemandRegistry


class PassiveExternalStreamStatus:
    """Read-path status view; it never imports or starts a venue connection."""

    def snapshot(self, *, demanded_feed_keys=None) -> dict[str, object]:
        return {
            "status": "external_owner",
            "running": False,
            "owned_by_this_process": False,
            "demanded_feed_keys": sorted(demanded_feed_keys or []),
        }


def build_passive_context() -> DataLayerContext:
    redis_cache = RedisCache()
    demand_registry = FeedDemandRegistry(redis_cache.r)
    return DataLayerContext(
        redis_cache=redis_cache,
        binance_stream_supervisor=PassiveExternalStreamStatus(),
        get_dnse_stream_manager=lambda: None,
        demand_registry=demand_registry,
        preload_topup_coordinator=PreloadTopupCoordinator(
            redis_cache.r,
            topup_existing_symbol_if_needed,
        ),
    )

