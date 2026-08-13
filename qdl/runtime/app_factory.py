from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, Request

from qdl.runtime.passive_dependencies import build_passive_context
from qdl.runtime.roles import RuntimeRole, RuntimeRoleConfig


@asynccontextmanager
async def _passive_lifespan(app: FastAPI):
    # Constructing a query/control/history replica never opens venue sockets.
    # Redis itself remains lazy and only connects when a route needs it.
    yield
    context = getattr(app.state, "context", None)
    redis_cache = getattr(context, "redis_cache", None)
    if redis_cache is not None:
        await redis_cache.r.aclose()
        redis_cache.sync_r.close()


def _role_health_router() -> APIRouter:
    router = APIRouter(tags=["runtime"])

    @router.get("/health/live")
    async def live(request: Request):
        return {"status": "alive", "runtime": request.app.state.runtime_manifest}

    @router.get("/health/ready")
    async def ready(request: Request):
        manifest = request.app.state.runtime_manifest
        return {
            "status": "ready",
            "runtime": manifest,
            "note": "Phase 1 role is dark; V1 combined runtime remains authoritative.",
        }

    return router


def create_role_app(role: RuntimeRole) -> FastAPI:
    config = RuntimeRoleConfig.for_entrypoint(role)
    app = FastAPI(
        title=f"qdl-{role.value}",
        version="2.0.0-phase1",
        lifespan=_passive_lifespan,
    )
    app.state.runtime_manifest = config.manifest()
    app.state.context = build_passive_context()
    app.include_router(_role_health_router())

    if role is RuntimeRole.API:
        from app.api import routes_fallback, routes_health, routes_latest

        app.include_router(routes_health.router)
        app.include_router(routes_latest.router)
        app.include_router(routes_fallback.router)
    elif role is RuntimeRole.CONTROL:
        from app.api import routes_control_plane

        app.include_router(routes_control_plane.router)
    elif role is RuntimeRole.HISTORY:
        from app.api import routes_binance_derivatives, routes_history, routes_preload

        app.include_router(routes_history.router)
        app.include_router(routes_binance_derivatives.router)
        app.include_router(routes_preload.router)
    else:
        raise ValueError("compatibility combined runtime remains app.main:app")
    return app

