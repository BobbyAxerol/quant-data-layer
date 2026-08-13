from __future__ import annotations

from contextlib import asynccontextmanager
import os
import uuid

from fastapi import APIRouter, Depends, FastAPI, Request

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
        from qdl.security import AuditChain
        from qdl.security.fastapi import ControlPlaneGuard, ControlSecurityConfig

        guard = ControlPlaneGuard(ControlSecurityConfig.from_environment())
        try:
            audit_path = os.environ["QDL_CONTROL_AUDIT_PATH"]
        except KeyError as error:
            raise RuntimeError("QDL_CONTROL_AUDIT_PATH is required for the control role") from error
        audit = AuditChain(audit_path)
        app.state.control_audit = audit

        @app.middleware("http")
        async def audit_control_mutation(request: Request, call_next):
            response = await call_next(request)
            if request.method in {"POST", "PUT", "PATCH", "DELETE"} and request.url.path.startswith("/v1/control/"):
                principal = getattr(request.state, "qdl_principal", None)
                audit.append(
                    actor=principal.subject if principal is not None else "unauthenticated",
                    action=f"{request.method} {request.url.path}",
                    environment=os.environ.get("QDL_ENVIRONMENT", "paper"),
                    request_id=request.headers.get("x-request-id", str(uuid.uuid4())),
                    result=str(response.status_code),
                    details={"path": request.url.path, "method": request.method},
                )
            return response

        app.include_router(routes_control_plane.router, dependencies=[Depends(guard)])
        app.state.runtime_manifest["control_auth"] = "required"
        app.state.runtime_manifest["control_audit"] = "hash_chain_fsync"
    elif role is RuntimeRole.HISTORY:
        from app.api import routes_binance_derivatives, routes_history, routes_preload

        app.include_router(routes_history.router)
        app.include_router(routes_binance_derivatives.router)
        app.include_router(routes_preload.router)
    else:
        raise ValueError("compatibility combined runtime remains app.main:app")
    return app
