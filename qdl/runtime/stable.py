from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping

import grpc
import redis
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from redis.asyncio import Redis as AsyncRedis

from qdl.api_v2 import create_v2_app
from qdl.consumer import ConsumerManifestLoader, ConsumerManifestRegistry
from qdl.projection.stable import RedisStableProjectionTarget, StableCompatibilityProjector
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.runtime.bounds import BoundedRequestMiddleware, RequestBounds
from qdl.runtime.lease import ActivePassiveGatewayLease, RedisGatewayLeaseStore
from qdl.runtime.readiness import (
    CallableReadinessProbe,
    ComponentReadiness,
    ComponentState,
    MeasuredRuntimeReadiness,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_ingest import (
    StableHttpCanonicalSink,
    install_stable_canonical_ingest,
)
from qdl.runtime.stable_projector import StableProjectorEngine
from qdl.runtime.stable_source import (
    StableGrpcSnapshotLoader,
    build_stable_query_stack,
)
from qdl.security import (
    AuditChain,
    DataPlaneIdentityService,
    DataPlaneSecurityConfig,
    RedisMinuteQuota,
)
from qdl.stream import DurableStreamGateway, GrpcMarketDataService, create_grpc_server
from qdl.transport import SQLiteDurableSpool, SpoolConfig
from qdl.transport.kafka_projector import (
    ConfluentProjectorBroker,
    KafkaProjectorConfig,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StableRuntimeConfig:
    role: str
    instance_id: str
    environment: str
    config_revision: str
    authority_mode: str
    authority_revision: int
    schema_digest: str
    state_dir: Path
    durable_state_dir: Path
    audit_path: Path
    manifest_paths: tuple[Path, ...]
    source_bindings_path: Path
    internal_ingest_secret: bytes
    redis_url: str
    redis_prefix: str
    consumer_group: str
    cursor_keys: Mapping[str, bytes]
    active_cursor_key_id: str
    cursor_ttl_seconds: int
    http_port: int
    grpc_port: int
    max_request_bytes: int
    max_concurrent_requests: int
    max_concurrent_rpcs: int
    max_streams: int
    max_buffer_events: int
    max_replay_events: int
    lease_shard_id: str
    lease_ttl_seconds: int
    lease_renew_seconds: float
    kafka_bootstrap_servers: str | None = None
    kafka_client_id: str | None = None
    kafka_raw_topics: tuple[str, ...] = ()
    kafka_canonical_topic: str | None = None
    kafka_cert_root: Path | None = None
    stream_ingest_urls: tuple[str, ...] = ()
    max_pending_records: int = 10_000
    max_pending_bytes: int = 256 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.role not in {"query_v2", "stream_v2", "projector_v2"}:
            raise ValueError("stable role is invalid")
        if self.authority_mode != "RUST_SHADOW":
            raise ValueError("Phase B stable runtime must remain RUST_SHADOW")
        if not all((
            self.instance_id, self.environment, self.config_revision,
            self.redis_url, self.redis_prefix, self.consumer_group,
        )):
            raise ValueError("stable runtime identity/isolation fields are required")
        if not self.redis_prefix.startswith("qdl:stable:v2:"):
            raise ValueError("stable Redis prefix must start with qdl:stable:v2:")
        if len(self.schema_digest) != 64 or any(
            value not in "0123456789abcdef" for value in self.schema_digest
        ):
            raise ValueError("stable schema digest must be lowercase SHA-256")
        if self.authority_revision < 1 or not self.manifest_paths:
            raise ValueError("stable authority revision and consumer manifests are required")
        if not self.source_bindings_path.is_file():
            raise ValueError("stable source binding catalog is unavailable")
        if len(self.internal_ingest_secret) < 32:
            raise ValueError("stable internal ingest secret must contain 256 bits")
        if self.active_cursor_key_id not in self.cursor_keys or any(
            len(value) < 32 for value in self.cursor_keys.values()
        ):
            raise ValueError("stable active cursor key is unavailable or weak")
        if not 60 <= self.cursor_ttl_seconds <= 86_400:
            raise ValueError("stable cursor TTL must be 60..86400 seconds")
        if min(
            self.http_port, self.grpc_port, self.max_request_bytes,
            self.max_concurrent_requests, self.max_concurrent_rpcs,
            self.max_streams, self.max_buffer_events, self.max_replay_events,
            self.max_pending_records, self.max_pending_bytes,
        ) <= 0:
            raise ValueError("stable runtime ports/bounds must be positive")
        if self.max_buffer_events > 10_000 or self.max_replay_events > 10_000:
            raise ValueError("stable stream/replay bounds exceed contract maximum")
        if not 5 <= self.lease_ttl_seconds <= 300 or not (
            0 < self.lease_renew_seconds < self.lease_ttl_seconds
        ):
            raise ValueError("stable gateway lease policy is invalid")
        if self.durable_state_dir.resolve() == Path("/app").resolve():
            raise ValueError("stable durable state cannot use application source")
        try:
            self.audit_path.resolve().relative_to(self.state_dir.resolve())
        except ValueError as error:
            raise ValueError("stable audit path must remain inside stable state") from error
        if self.role == "projector_v2":
            if not all((
                self.kafka_bootstrap_servers, self.kafka_client_id,
                self.kafka_canonical_topic, self.kafka_cert_root,
            )) or not self.kafka_raw_topics or not self.stream_ingest_urls:
                raise ValueError("stable projector Kafka/stream dependencies are required")

    @classmethod
    def from_environment(
        cls, role: str, values: Mapping[str, str] | None = None
    ) -> "StableRuntimeConfig":
        env = os.environ if values is None else values
        cursor_raw = json.loads(env["QDL_STABLE_CURSOR_KEYS_JSON"])
        if not isinstance(cursor_raw, dict) or not cursor_raw:
            raise ValueError("QDL_STABLE_CURSOR_KEYS_JSON must be a non-empty object")
        manifests = tuple(
            Path(value) for value in env["QDL_STABLE_CONSUMER_MANIFESTS"].split(":")
            if value
        )
        state_dir = Path(env.get("QDL_STABLE_STATE_DIR", "/var/lib/qdl-stable"))
        cert_root_raw = env.get("QDL_STABLE_KAFKA_CERT_ROOT", "").strip()
        urls_raw = json.loads(env.get("QDL_STABLE_STREAM_INGEST_URLS_JSON", "[]"))
        if not isinstance(urls_raw, list):
            raise ValueError("QDL_STABLE_STREAM_INGEST_URLS_JSON must be an array")
        instance_id = env.get("QDL_STABLE_INSTANCE_ID", f"stable-{role}-local")
        return cls(
            role=role,
            instance_id=instance_id,
            environment=env.get("QDL_ENVIRONMENT", "paper").lower(),
            config_revision=env["QDL_CONFIG_REVISION"],
            authority_mode=env.get("QDL_STABLE_AUTHORITY_MODE", "RUST_SHADOW"),
            authority_revision=int(env["QDL_STABLE_AUTHORITY_REVISION"]),
            schema_digest=env["QDL_STABLE_SCHEMA_DIGEST"],
            state_dir=state_dir,
            durable_state_dir=Path(env.get(
                "QDL_STABLE_DURABLE_STATE_DIR", str(state_dir / "shared")
            )),
            audit_path=Path(env.get(
                "QDL_STABLE_AUDIT_PATH", str(state_dir / f"{instance_id}-audit.jsonl")
            )),
            manifest_paths=manifests,
            source_bindings_path=Path(env["QDL_STABLE_SOURCE_BINDINGS"]),
            internal_ingest_secret=env["QDL_STABLE_INTERNAL_INGEST_SECRET"].encode(),
            redis_url=env["QDL_STABLE_REDIS_URL"],
            redis_prefix=env["QDL_STABLE_REDIS_PREFIX"],
            consumer_group=env["QDL_STABLE_CONSUMER_GROUP"],
            cursor_keys={str(key): str(value).encode() for key, value in cursor_raw.items()},
            active_cursor_key_id=env["QDL_STABLE_CURSOR_ACTIVE_KEY_ID"],
            cursor_ttl_seconds=int(env.get("QDL_STABLE_CURSOR_TTL_SECONDS", "3600")),
            http_port=int(env.get("QDL_STABLE_HTTP_PORT", "18200")),
            grpc_port=int(env.get("QDL_STABLE_GRPC_PORT", "18210")),
            max_request_bytes=int(env.get("QDL_STABLE_MAX_REQUEST_BYTES", "1048576")),
            max_concurrent_requests=int(env.get("QDL_STABLE_MAX_CONCURRENT_REQUESTS", "200")),
            max_concurrent_rpcs=int(env.get("QDL_STABLE_MAX_CONCURRENT_RPCS", "200")),
            max_streams=int(env.get("QDL_STABLE_MAX_STREAMS", "1000")),
            max_buffer_events=int(env.get("QDL_STABLE_MAX_BUFFER_EVENTS", "2000")),
            max_replay_events=int(env.get("QDL_STABLE_MAX_REPLAY_EVENTS", "5000")),
            lease_shard_id=env.get("QDL_STABLE_LEASE_SHARD_ID", "stable-stream-v2"),
            lease_ttl_seconds=int(env.get("QDL_STABLE_LEASE_TTL_SECONDS", "15")),
            lease_renew_seconds=float(env.get("QDL_STABLE_LEASE_RENEW_SECONDS", "5")),
            kafka_bootstrap_servers=env.get("QDL_STABLE_KAFKA_BOOTSTRAP_SERVERS"),
            kafka_client_id=env.get("QDL_STABLE_KAFKA_CLIENT_ID"),
            kafka_raw_topics=tuple(
                value for value in env.get("QDL_STABLE_KAFKA_RAW_TOPICS", "").split(",")
                if value
            ),
            kafka_canonical_topic=env.get("QDL_STABLE_KAFKA_CANONICAL_TOPIC"),
            kafka_cert_root=Path(cert_root_raw) if cert_root_raw else None,
            stream_ingest_urls=tuple(str(value) for value in urls_raw),
            max_pending_records=int(env.get("QDL_STABLE_MAX_PENDING_RECORDS", "10000")),
            max_pending_bytes=int(env.get("QDL_STABLE_MAX_PENDING_BYTES", "268435456")),
        )

    def public_manifest(self) -> dict[str, object]:
        return {
            "contract_version": "2.0.0",
            "role": self.role,
            "instance_id": self.instance_id,
            "environment": self.environment,
            "config_revision": self.config_revision,
            "authority": self.authority_mode,
            "authority_revision": self.authority_revision,
            "schema_digest": self.schema_digest,
            "redis_prefix": self.redis_prefix,
            "consumer_group": self.consumer_group,
            "owns_venue_connections": False,
            "writes_current_v1_redis": False,
            "compatibility_projection": "DEDICATED_REDIS_ONLY",
            "replay_authority": "KAFKA",
            "query_cache_authority": False,
        }


def _ready(name: str, *, detail: str, revision: str | None = None) -> ComponentReadiness:
    return ComponentReadiness(
        name, ComponentState.READY, detail=detail, revision=revision,
        checked_at_ns=time.time_ns(),
    )


def load_stable_manifests(config: StableRuntimeConfig) -> ConsumerManifestRegistry:
    manifests = tuple(ConsumerManifestLoader.load(path) for path in config.manifest_paths)
    if any(item.environment != config.environment for item in manifests):
        raise ValueError("stable consumer manifest environment mismatch")
    return ConsumerManifestRegistry(manifests)


def build_stable_identity(
    config: StableRuntimeConfig, manifests: ConsumerManifestRegistry
) -> DataPlaneIdentityService:
    security = DataPlaneSecurityConfig.from_environment()
    if security.environment != config.environment:
        raise ValueError("stable JWT environment mismatch")
    quota = RedisMinuteQuota.from_url(
        config.redis_url, prefix=f"{config.redis_prefix}:identity"
    )
    return DataPlaneIdentityService(security, manifests, quota=quota)


def build_stable_spool(config: StableRuntimeConfig) -> SQLiteDurableSpool:
    config.durable_state_dir.mkdir(parents=True, exist_ok=True)
    return SQLiteDurableSpool(SpoolConfig(
        path=config.durable_state_dir / "canonical-cache.sqlite3",
        max_records=1_000_000,
        max_payload_bytes=2 * 1024 * 1024 * 1024,
        max_storage_bytes=3 * 1024 * 1024 * 1024,
        max_partitions=100_000,
        max_consumer_checkpoints=100_000,
        min_free_disk_bytes=512 * 1024 * 1024,
        consumer_ttl_seconds=config.cursor_ttl_seconds,
        replay_retention_seconds=24 * 3600,
    ))


def build_stable_handoff(
    config: StableRuntimeConfig, spool: SQLiteDurableSpool
) -> GapFreeHandoff:
    return GapFreeHandoff(
        spool,
        SignedHandoffCursorCodec(
            config.cursor_keys, active_key_id=config.active_cursor_key_id
        ),
        checkpoint_ttl_seconds=config.cursor_ttl_seconds,
    )


def stable_readiness(
    config: StableRuntimeConfig,
    manifests: ConsumerManifestRegistry,
    spool: SQLiteDurableSpool,
    *,
    quota: RedisMinuteQuota | None = None,
    extra_probes=(),
) -> MeasuredRuntimeReadiness:
    async def cache():
        stats = await asyncio.to_thread(spool.stats)
        return _ready(
            "query_cache",
            detail=f"bounded rebuildable cache records={stats.records} utilization={stats.utilization:.6f}",
        )

    async def redis_probe():
        if quota is None or not await asyncio.to_thread(quota.ping):
            return ComponentReadiness(
                "dedicated_redis", ComponentState.NOT_READY,
                detail="stable dedicated Redis is unavailable", checked_at_ns=time.time_ns(),
            )
        return _ready("dedicated_redis", detail="stable isolated Redis reachable")

    probes = (
        CallableReadinessProbe("consumer_manifests", lambda: _ready(
            "consumer_manifests", detail=f"registered={manifests.count}",
            revision=str(max(item[1] for item in manifests.revisions)),
        )),
        CallableReadinessProbe("query_cache", cache),
        CallableReadinessProbe("dedicated_redis", redis_probe),
        CallableReadinessProbe("authority_manifest", lambda: _ready(
            "authority_manifest", detail=config.authority_mode,
            revision=str(config.authority_revision),
        )),
        *tuple(extra_probes),
    )
    return MeasuredRuntimeReadiness(
        role=config.role,
        authority=config.authority_mode,
        config_revision=config.config_revision,
        probes=probes,
    )


def install_stable_health(app, readiness, manifest) -> None:
    @app.get("/health/live", include_in_schema=False)
    async def live():
        return {"status": "alive", "runtime": manifest}

    @app.get("/health/ready", include_in_schema=False)
    async def ready():
        snapshot = await readiness.snapshot()
        return JSONResponse(
            status_code=200 if snapshot.ready else 503,
            content={
                "status": snapshot.status, "ready": snapshot.ready,
                "role": snapshot.role, "authority": snapshot.authority,
                "config_revision": snapshot.config_revision,
            },
        )

    @app.get("/health/dependencies", include_in_schema=False)
    async def dependencies():
        snapshot = await readiness.snapshot()
        return {
            "status": snapshot.status, "ready": snapshot.ready,
            "components": [{**asdict(item), "state": item.state.value} for item in snapshot.components],
        }


def create_stable_query_app(config: StableRuntimeConfig | None = None) -> FastAPI:
    config = config or StableRuntimeConfig.from_environment("query_v2")
    config.state_dir.mkdir(parents=True, exist_ok=True)
    manifests = load_stable_manifests(config)
    identity = build_stable_identity(config, manifests)
    spool = build_stable_spool(config)
    catalog = StableSourceCatalog.load(config.source_bindings_path)
    handoff = build_stable_handoff(config, spool)
    service, _backend, issuer = build_stable_query_stack(
        spool=spool, catalog=catalog, schema_digest=config.schema_digest,
        handoff=handoff, cursor_ttl_seconds=config.cursor_ttl_seconds,
    )
    readiness = stable_readiness(
        config, manifests, spool, quota=identity.quota,
        extra_probes=(CallableReadinessProbe("instrument_catalog", lambda: _ready(
            "instrument_catalog", detail=f"bindings={len(catalog.bindings)}",
            revision=str(catalog.catalog_revision),
        )),),
    )
    app = create_v2_app(
        service, identity_service=identity, readiness_service=readiness,
        cursor_issuer=issuer,
        request_bounds=RequestBounds(
            max_request_bytes=config.max_request_bytes,
            max_concurrent_requests=config.max_concurrent_requests,
        ),
        contract_version="2.0.0", authority="INTERNAL_STABLE",
    )
    app.state.runtime_manifest = config.public_manifest()
    app.state.stable_spool = spool
    app.state.stable_audit = AuditChain(config.audit_path)
    install_stable_health(app, readiness, config.public_manifest())

    @app.on_event("shutdown")
    async def close_stable_query():
        await asyncio.to_thread(spool.close)
        await asyncio.to_thread(identity.quota.close)

    return app


@dataclass(slots=True)
class StableStreamRuntime:
    config: StableRuntimeConfig
    redis: AsyncRedis
    spool: SQLiteDurableSpool
    gateway: DurableStreamGateway
    lease: ActivePassiveGatewayLease
    grpc_server: grpc.aio.Server
    health_app: FastAPI
    quota: RedisMinuteQuota

    async def start(self) -> None:
        await self.redis.ping()
        await self.lease.start()
        self.grpc_server.add_insecure_port(f"0.0.0.0:{self.config.grpc_port}")
        await self.grpc_server.start()

    async def stop(self) -> None:
        await self.grpc_server.stop(grace=2.0)
        await self.lease.stop()
        await self.redis.aclose()
        await asyncio.to_thread(self.quota.close)
        await asyncio.to_thread(self.spool.close)


def create_stable_stream_runtime(
    config: StableRuntimeConfig | None = None,
) -> StableStreamRuntime:
    config = config or StableRuntimeConfig.from_environment("stream_v2")
    config.state_dir.mkdir(parents=True, exist_ok=True)
    manifests = load_stable_manifests(config)
    identity = build_stable_identity(config, manifests)
    spool = build_stable_spool(config)
    handoff = build_stable_handoff(config, spool)
    catalog = StableSourceCatalog.load(config.source_bindings_path)
    async_redis = AsyncRedis.from_url(config.redis_url, decode_responses=True)
    lease = ActivePassiveGatewayLease(
        RedisGatewayLeaseStore(async_redis, prefix=config.redis_prefix),
        shard_id=config.lease_shard_id, owner_id=config.instance_id,
        ttl_seconds=config.lease_ttl_seconds,
        renew_interval_seconds=config.lease_renew_seconds,
    )
    gateway = DurableStreamGateway(
        handoff=handoff, sink=spool, max_subscribers=config.max_streams,
        max_buffer_events=config.max_buffer_events,
        max_replay_events=config.max_replay_events,
        cursor_ttl_seconds=config.cursor_ttl_seconds, authority=lease,
    )
    lease.on_fenced = gateway.fence_all
    query_service, backend, issuer = build_stable_query_stack(
        spool=spool, catalog=catalog, schema_digest=config.schema_digest,
        handoff=handoff, cursor_ttl_seconds=config.cursor_ttl_seconds,
    )
    grpc_service = GrpcMarketDataService(
        gateway=gateway, query_service=query_service,
        snapshot_loader=StableGrpcSnapshotLoader(
            service=query_service, backend=backend, issuer=issuer
        ),
    )
    grpc_server = create_grpc_server(
        grpc_service, identity_service=identity,
        maximum_concurrent_rpcs=config.max_concurrent_rpcs,
        max_receive_message_bytes=config.max_request_bytes,
    )
    readiness = stable_readiness(
        config, manifests, spool, quota=identity.quota,
        extra_probes=(CallableReadinessProbe("gateway_lease", lease.readiness),),
    )
    app = FastAPI(
        title="Quant Data Layer V2 Stable Stream Health", version="2.0.0",
        docs_url=None, redoc_url=None, openapi_url=None,
    )
    app.add_middleware(
        BoundedRequestMiddleware,
        bounds=RequestBounds(
            max_request_bytes=config.max_request_bytes,
            max_concurrent_requests=config.max_concurrent_requests,
        ),
    )
    install_stable_health(app, readiness, config.public_manifest())
    install_stable_canonical_ingest(
        app, gateway=gateway, catalog=catalog, spool=spool,
        secret=config.internal_ingest_secret,
    )
    return StableStreamRuntime(
        config, async_redis, spool, gateway, lease, grpc_server, app, identity.quota
    )


async def serve_stable_stream() -> None:
    runtime = create_stable_stream_runtime()
    await runtime.start()
    server = uvicorn.Server(uvicorn.Config(
        runtime.health_app, host="0.0.0.0", port=runtime.config.http_port,
        log_level="info", access_log=False,
    ))
    try:
        await server.serve()
    finally:
        await runtime.stop()


async def serve_stable_projector() -> None:
    config = StableRuntimeConfig.from_environment("projector_v2")
    config.state_dir.mkdir(parents=True, exist_ok=True)
    spool = build_stable_spool(config)
    catalog = StableSourceCatalog.load(config.source_bindings_path)
    assert config.kafka_cert_root is not None
    assert config.kafka_bootstrap_servers is not None
    assert config.kafka_client_id is not None
    assert config.kafka_canonical_topic is not None
    broker = ConfluentProjectorBroker(KafkaProjectorConfig(
        bootstrap_servers=config.kafka_bootstrap_servers,
        client_id=config.kafka_client_id,
        group_id=config.consumer_group,
        raw_topics=config.kafka_raw_topics,
        canonical_topic=config.kafka_canonical_topic,
        ca_path=config.kafka_cert_root / "ca.crt",
        certificate_path=config.kafka_cert_root / "client.crt",
        key_path=config.kafka_cert_root / "client.key",
    ))
    redis_client = redis.Redis.from_url(config.redis_url)
    sink = StableHttpCanonicalSink(
        config.stream_ingest_urls, config.internal_ingest_secret, spool
    )
    engine = StableProjectorEngine(
        broker=broker, spool=spool, catalog=catalog,
        canonical_topic=config.kafka_canonical_topic,
        raw_topics=config.kafka_raw_topics, sink=sink,
        projector=StableCompatibilityProjector(
            catalog, namespace=config.redis_prefix.rstrip(":")
        ),
        target=RedisStableProjectionTarget(
            redis_client, namespace=config.redis_prefix.rstrip(":"),
            dedicated_database=True,
        ),
        max_pending_records=config.max_pending_records,
        max_pending_bytes=config.max_pending_bytes,
    )
    try:
        while True:
            await engine.run_once(timeout_seconds=1.0)
    finally:
        await sink.close()
        await asyncio.to_thread(broker.close)
        await asyncio.to_thread(redis_client.close)
        await asyncio.to_thread(spool.close)
