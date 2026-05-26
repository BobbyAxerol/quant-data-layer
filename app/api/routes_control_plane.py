from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

from fastapi import APIRouter

from app.market_universe import registry


router = APIRouter(prefix="/v1/control", tags=["control-plane"])

VN_TZ = timezone(timedelta(hours=7))
VN_SESSIONS = [
    {"name": "morning", "start": "09:00", "end": "11:30"},
    {"name": "afternoon", "start": "13:00", "end": "14:30"},
]


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

