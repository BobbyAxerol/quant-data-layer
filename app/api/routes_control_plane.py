from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.api.context import DataLayerContext, get_context
from app.market_universe import registry
from app.stream.demand_registry import FeedDemand


router = APIRouter(prefix="/v1/control", tags=["control-plane"])

VN_TZ = timezone(timedelta(hours=7))
VN_SESSIONS = [
    {"name": "morning", "start": "09:00", "end": "11:30"},
    {"name": "afternoon", "start": "13:00", "end": "14:30"},
]


class FeedDemandItem(BaseModel):
    source: str = "runtime"
    feed: str
    symbol: str
    interval: str | None = None
    reason: str = "runtime_execution"


class FeedLeaseRequest(BaseModel):
    owner_id: str = Field(min_length=1, max_length=200)
    ttl_seconds: int = Field(default=180, ge=30, le=3600)
    demands: list[FeedDemandItem] = Field(min_length=1, max_length=1000)


def _is_vn_session_open(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    local_time = now.time()
    return (
        time(9, 0) <= local_time < time(11, 30)
        or time(13, 0) <= local_time < time(14, 30)
    )


@router.get("/runtime-roles")
async def runtime_roles():
    return {
        "current_container_role": "combined_api_ingestion_history",
        "target_roles": ["api", "ingestion", "history", "diagnostics"],
        "separable_in_compose": False,
        "notes": [
            "Phase 3 exposes route/module boundaries first.",
            "Docker role split should happen after route contracts are stable.",
        ],
    }


@router.get("/universe/configured")
async def configured_universe():
    return registry.configured_universe()


@router.get("/universe/active")
async def active_universe():
    return registry.active_universe()


@router.get("/universe/priority")
async def priority_universe():
    return registry.priority_universe()


@router.get("/feed-demands")
async def feed_demands(ctx: DataLayerContext = Depends(get_context)):
    return await ctx.demand_registry.snapshot()


@router.post("/feed-leases")
async def renew_feed_leases(
    request: FeedLeaseRequest,
    ctx: DataLayerContext = Depends(get_context),
):
    try:
        demands = [FeedDemand(**item.model_dump()) for item in request.demands]
        return await ctx.demand_registry.upsert(
            request.owner_id,
            demands,
            ttl_seconds=request.ttl_seconds,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/feed-leases/{owner_id}")
async def release_feed_leases(owner_id: str, ctx: DataLayerContext = Depends(get_context)):
    return {"owner_id": owner_id, "released": await ctx.demand_registry.release_owner(owner_id)}


@router.get("/provider-priority")
async def provider_priority():
    return registry.provider_priority()


@router.get("/session-calendar")
async def session_calendar():
    now = datetime.now(tz=VN_TZ)
    return {
        "vn_stock": {
            "timezone": "Asia/Ho_Chi_Minh",
            "date": now.date().isoformat(),
            "is_open": _is_vn_session_open(now),
            "sessions": VN_SESSIONS,
        },
        "crypto": {
            "timezone": "UTC",
            "is_open": True,
            "sessions": [{"name": "continuous", "start": "00:00", "end": "24:00"}],
        },
    }
