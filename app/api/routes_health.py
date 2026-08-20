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


def _feed_data_ready(stream: dict, feed: str) -> bool:
    states = [
        state
        for state in stream.get("sources", {}).values()
        if state.get("feed") == feed
    ]
    return bool(states) and all(bool(state.get("data_ready")) for state in states)


def _recovery_snapshot(ctx: DataLayerContext) -> dict:
    manager = ctx.get_kline_recovery_manager()
    return manager.snapshot() if manager else {"enabled": False, "status": "not_started"}


@router.get("/health")
async def health(ctx: DataLayerContext = Depends(get_context)):
    redis_ok = await ctx.redis_cache.health_check()
    demands = await ctx.demand_registry.snapshot()
    binance_stream = ctx.binance_stream_supervisor.snapshot(
        demanded_feed_keys=_binance_demand_keys(demands),
    )
    binance_stream["kline_recovery"] = _recovery_snapshot(ctx)
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
        "binance_trade_stream": _feed_data_ready(binance_stream, "trade"),
        "binance_kline_stream": _feed_data_ready(binance_stream, "kline"),
        "binance_stream": binance_stream,
        "feed_demands": demands,
        "preload_topup": ctx.preload_topup_coordinator.snapshot(),
        "dnse_stream": dnse_stream,
    }


@router.get("/health/streams")
async def health_streams(ctx: DataLayerContext = Depends(get_context)):
    dnse_manager = ctx.get_dnse_stream_manager()
    demands = await ctx.demand_registry.snapshot()
    binance_stream = ctx.binance_stream_supervisor.snapshot(
        demanded_feed_keys=_binance_demand_keys(demands),
    )
    binance_stream["kline_recovery"] = _recovery_snapshot(ctx)
    return {
        "binance_stream": binance_stream,
        "feed_demands": demands,
        "dnse_stream": (
            dnse_manager.get_status()
            if dnse_manager
            else {"status": "not_started", "running": False}
        ),
    }
