from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.context import DataLayerContext, get_context


router = APIRouter(prefix="/v1", tags=["health"])


def _binance_demand_keys(demands: dict) -> set[str]:
    return {
        str(item["feed_key"])
        for item in demands.get("items", [])
        if str(item.get("source") or "").startswith("binance")
    }


@router.get("/health")
async def health(ctx: DataLayerContext = Depends(get_context)):
    redis_ok = await ctx.redis_cache.health_check()
    demands = await ctx.demand_registry.snapshot()
    binance_stream = ctx.binance_stream_supervisor.snapshot(
        demanded_feed_keys=_binance_demand_keys(demands),
    )
    dnse_manager = ctx.get_dnse_stream_manager()
    dnse_stream = (
        dnse_manager.get_status()
        if dnse_manager
        else {"status": "not_started", "running": False}
    )
    stream_ok = (
        binance_stream.get("status") in {"ok", "starting"}
        and dnse_stream.get("status") in {"OPEN_HEALTHY", "MARKET_CLOSED", "not_started"}
    )
    return {
        "status": "ok" if redis_ok and stream_ok else "degraded",
        "redis": redis_ok,
        "binance_trade_stream": binance_stream.get("status") in {"ok", "starting"},
        "binance_kline_stream": binance_stream.get("status") in {"ok", "starting"},
        "binance_stream": binance_stream,
        "feed_demands": demands,
        "preload_topup": ctx.preload_topup_coordinator.snapshot(),
        "dnse_stream": dnse_stream,
    }


@router.get("/health/streams")
async def health_streams(ctx: DataLayerContext = Depends(get_context)):
    dnse_manager = ctx.get_dnse_stream_manager()
    demands = await ctx.demand_registry.snapshot()
    return {
        "binance_stream": ctx.binance_stream_supervisor.snapshot(
            demanded_feed_keys=_binance_demand_keys(demands),
        ),
        "feed_demands": demands,
        "dnse_stream": (
            dnse_manager.get_status()
            if dnse_manager
            else {"status": "not_started", "running": False}
        ),
    }
