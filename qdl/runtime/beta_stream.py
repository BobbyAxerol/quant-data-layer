from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import grpc
import uvicorn
from fastapi import FastAPI
from redis.asyncio import Redis

from qdl.query import CanonicalErrorCode, QueryProblem, QueryServiceError
from qdl.runtime.beta import (
    BetaRuntimeConfig,
    beta_readiness,
    build_beta_handoff,
    build_beta_identity,
    build_beta_spool,
    build_empty_query_service,
    install_beta_health,
    load_beta_manifests,
)
from qdl.runtime.bounds import BoundedRequestMiddleware, RequestBounds
from qdl.runtime.canary_bridge import install_internal_canonical_ingest
from qdl.runtime.canary_source import (
    CanaryGrpcSnapshotLoader,
    CanarySourceCatalog,
    build_canary_query_stack,
)
from qdl.runtime.lease import ActivePassiveGatewayLease, RedisGatewayLeaseStore
from qdl.runtime.readiness import (
    CallableReadinessProbe,
    ComponentReadiness,
    ComponentState,
)
from qdl.security import AuditChain
from qdl.stream import DurableStreamGateway, GrpcMarketDataService, create_grpc_server


class UnavailableSnapshotLoader:
    """Phase 7.1 has no consumer data activation; 7.2 supplies the real catalog view."""

    def load(self, requirement, *, consumer_id):
        del consumer_id
        raise QueryServiceError(
            QueryProblem(
                CanonicalErrorCode.DATA_NOT_READY,
                "beta snapshot catalog is not activated for a consumer",
                True,
            ),
            request_id="phase7-beta-no-consumer",
            instrument_uid=requirement.instrument_uid,
        )


@dataclass(slots=True)
class BetaStreamRuntime:
    config: BetaRuntimeConfig
    redis: Redis
    spool: object
    gateway: DurableStreamGateway
    lease: ActivePassiveGatewayLease
    grpc_server: grpc.aio.Server
    health_app: FastAPI
    audit: AuditChain
    quota: object

    async def start(self) -> None:
        await self.redis.ping()
        await self.lease.start()
        self.grpc_server.add_insecure_port(f"0.0.0.0:{self.config.grpc_port}")
        await self.grpc_server.start()
        self.audit.append(
            actor=self.config.instance_id,
            action="beta.stream_v2.start",
            environment=self.config.environment,
            request_id=self.config.config_revision,
            result="ACTIVE" if self.lease.active else "STANDBY",
            details=self.config.public_manifest() | {
                "lease_epoch": self.lease.current_epoch,
            },
        )

    async def stop(self) -> None:
        await self.grpc_server.stop(grace=2.0)
        await self.lease.stop()
        await self.redis.aclose()
        await asyncio.to_thread(self.quota.close)
        await asyncio.to_thread(self.spool.close)


def create_beta_stream_runtime(
    config: BetaRuntimeConfig | None = None,
) -> BetaStreamRuntime:
    config = config or BetaRuntimeConfig.from_environment("stream_v2")
    config.state_dir.mkdir(parents=True, exist_ok=True)
    manifests = load_beta_manifests(config)
    identity = build_beta_identity(config, manifests)
    spool = build_beta_spool(config)
    handoff = build_beta_handoff(config, spool)
    redis = Redis.from_url(config.redis_url, decode_responses=True)
    lease = ActivePassiveGatewayLease(
        RedisGatewayLeaseStore(redis, prefix=config.redis_prefix),
        shard_id=config.lease_shard_id,
        owner_id=config.instance_id,
        ttl_seconds=config.lease_ttl_seconds,
        renew_interval_seconds=config.lease_renew_seconds,
    )
    gateway = DurableStreamGateway(
        handoff=handoff,
        sink=spool,
        max_subscribers=config.max_streams,
        max_buffer_events=config.max_buffer_events,
        max_replay_events=config.max_replay_events,
        cursor_ttl_seconds=config.cursor_ttl_seconds,
        authority=lease,
    )
    lease.on_fenced = gateway.fence_all
    catalog = None
    if config.source_bindings_path is None:
        query_service = build_empty_query_service()
        snapshot_loader = UnavailableSnapshotLoader()
    else:
        catalog = CanarySourceCatalog.load(config.source_bindings_path)
        query_service, backend, issuer = build_canary_query_stack(
            spool=spool,
            catalog=catalog,
            schema_digest=config.schema_digest,
            handoff=handoff,
            cursor_ttl_seconds=config.cursor_ttl_seconds,
        )
        snapshot_loader = CanaryGrpcSnapshotLoader(
            service=query_service,
            backend=backend,
            issuer=issuer,
        )
    grpc_service = GrpcMarketDataService(
        gateway=gateway,
        query_service=query_service,
        snapshot_loader=snapshot_loader,
    )
    grpc_server = create_grpc_server(
        grpc_service,
        identity_service=identity,
        maximum_concurrent_rpcs=config.max_concurrent_rpcs,
        max_receive_message_bytes=config.max_request_bytes,
    )
    extra_probes = [CallableReadinessProbe("gateway_lease", lease.readiness)]
    if catalog is not None:
        extra_probes.append(CallableReadinessProbe(
            "instrument_catalog",
            lambda: ComponentReadiness(
                "instrument_catalog",
                ComponentState.READY,
                detail=f"bindings={len(catalog.bindings)}",
                revision=str(catalog.catalog_revision),
                checked_at_ns=time.time_ns(),
            ),
        ))
    readiness = beta_readiness(
        config,
        manifests,
        spool,
        extra_probes=tuple(extra_probes),
        quota=identity.quota,
    )
    health_app = FastAPI(
        title="Quant Data Layer V2 Stream Beta Health",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    health_app.add_middleware(
        BoundedRequestMiddleware,
        bounds=RequestBounds(
            max_request_bytes=config.max_request_bytes,
            max_concurrent_requests=config.max_concurrent_requests,
        ),
    )
    install_beta_health(health_app, readiness, config.public_manifest())
    if catalog is not None:
        assert config.internal_ingest_secret is not None
        install_internal_canonical_ingest(
            health_app,
            gateway=gateway,
            catalog=catalog,
            secret=config.internal_ingest_secret,
        )
    audit = AuditChain(config.audit_path)
    return BetaStreamRuntime(
        config, redis, spool, gateway, lease, grpc_server, health_app, audit,
        identity.quota,
    )


async def serve_beta_stream() -> None:
    runtime = create_beta_stream_runtime()
    await runtime.start()
    server = uvicorn.Server(uvicorn.Config(
        runtime.health_app,
        host="0.0.0.0",
        port=runtime.config.http_port,
        log_level="info",
        access_log=False,
    ))
    try:
        await server.serve()
    finally:
        await runtime.stop()
