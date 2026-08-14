from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from qdl.api_v2 import create_v2_app
from qdl.consumer import ConsumerManifestLoader, ConsumerManifestRegistry
from qdl.domain.instrument import InstrumentRegistry
from qdl.query import EntitlementPolicy, InstrumentQuery, MemoryMarketDataBackend, V2QueryService
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.runtime.bounds import RequestBounds
from qdl.runtime.canary_source import (
    CanarySourceCatalog,
    build_canary_query_stack,
)
from qdl.runtime.readiness import (
    CallableReadinessProbe,
    ComponentReadiness,
    ComponentState,
    MeasuredRuntimeReadiness,
)
from qdl.security import (
    AuditChain,
    DataPlaneIdentityService,
    DataPlaneSecurityConfig,
    RedisMinuteQuota,
)
from qdl.transport import SQLiteDurableSpool, SpoolConfig


@dataclass(frozen=True, slots=True)
class BetaRuntimeConfig:
    role: str
    instance_id: str
    environment: str
    config_revision: str
    authority_revision: int
    schema_digest: str
    state_dir: Path
    durable_state_dir: Path
    audit_path: Path
    manifest_paths: tuple[Path, ...]
    source_bindings_path: Path | None
    internal_ingest_secret: bytes | None
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

    def __post_init__(self) -> None:
        if self.role not in {"query_v2", "stream_v2"}:
            raise ValueError("beta role must be query_v2 or stream_v2")
        if not all((
            self.instance_id.strip(),
            self.environment.strip(),
            self.config_revision.strip(),
            self.redis_url.strip(),
            self.redis_prefix.strip(),
            self.consumer_group.strip(),
        )):
            raise ValueError("beta runtime identity and isolation fields are required")
        if not self.redis_prefix.startswith("qdl:beta:v2:"):
            raise ValueError("beta Redis prefix must start with qdl:beta:v2:")
        if len(self.schema_digest) != 64 or any(
            value not in "0123456789abcdef" for value in self.schema_digest
        ):
            raise ValueError("beta schema digest must be lowercase SHA-256")
        if self.authority_revision < 1 or not self.manifest_paths:
            raise ValueError("authority revision and consumer manifests are required")
        if self.active_cursor_key_id not in self.cursor_keys:
            raise ValueError("active beta cursor key is unavailable")
        if any(len(value) < 32 for value in self.cursor_keys.values()):
            raise ValueError("beta cursor keys must contain at least 256 bits")
        if not 60 <= self.cursor_ttl_seconds <= 86_400:
            raise ValueError("beta cursor TTL must be 60..86400 seconds")
        if min(
            self.http_port,
            self.grpc_port,
            self.max_request_bytes,
            self.max_concurrent_requests,
            self.max_concurrent_rpcs,
            self.max_streams,
            self.max_buffer_events,
            self.max_replay_events,
        ) <= 0:
            raise ValueError("beta runtime ports and bounds must be positive")
        if self.max_buffer_events > 10_000 or self.max_replay_events > 10_000:
            raise ValueError("beta stream/replay bounds exceed contract maximum")
        if not 5 <= self.lease_ttl_seconds <= 300:
            raise ValueError("beta gateway lease TTL must be 5..300 seconds")
        if not 0 < self.lease_renew_seconds < self.lease_ttl_seconds:
            raise ValueError("beta lease renewal must be below TTL")
        state_root = self.state_dir.resolve()
        try:
            self.audit_path.resolve().relative_to(state_root)
        except ValueError as error:
            raise ValueError("beta audit path must stay inside beta state directory") from error
        if self.source_bindings_path is not None:
            if self.internal_ingest_secret is None or len(self.internal_ingest_secret) < 32:
                raise ValueError(
                    "activated beta source bindings require a 256-bit internal ingest secret"
                )
        if self.durable_state_dir.resolve() == Path("/app").resolve():
            raise ValueError("beta durable state cannot use the application source directory")

    @classmethod
    def from_environment(
        cls,
        role: str,
        values: Mapping[str, str] | None = None,
    ) -> "BetaRuntimeConfig":
        env = os.environ if values is None else values
        keys_raw = json.loads(env["QDL_BETA_CURSOR_KEYS_JSON"])
        if not isinstance(keys_raw, dict) or not keys_raw:
            raise ValueError("QDL_BETA_CURSOR_KEYS_JSON must be a non-empty object")
        manifests = tuple(
            Path(value.strip())
            for value in env["QDL_BETA_CONSUMER_MANIFESTS"].split(":")
            if value.strip()
        )
        state_dir = Path(env.get("QDL_BETA_STATE_DIR", "/var/lib/qdl-beta"))
        source_path = env.get("QDL_BETA_SOURCE_BINDINGS", "").strip()
        ingest_secret = env.get("QDL_BETA_INTERNAL_INGEST_SECRET", "")
        instance = env.get("QDL_BETA_INSTANCE_ID", f"{role}-local")
        return cls(
            role=role,
            instance_id=instance,
            environment=env.get("QDL_ENVIRONMENT", "paper").lower(),
            config_revision=env["QDL_CONFIG_REVISION"],
            authority_revision=int(env["QDL_BETA_AUTHORITY_REVISION"]),
            schema_digest=env["QDL_BETA_SCHEMA_DIGEST"],
            state_dir=state_dir,
            durable_state_dir=Path(
                env.get("QDL_BETA_DURABLE_STATE_DIR", str(state_dir))
            ),
            audit_path=Path(
                env.get("QDL_BETA_AUDIT_PATH", str(state_dir / f"{instance}-audit.jsonl"))
            ),
            manifest_paths=manifests,
            source_bindings_path=Path(source_path) if source_path else None,
            internal_ingest_secret=ingest_secret.encode() if ingest_secret else None,
            redis_url=env["QDL_BETA_REDIS_URL"],
            redis_prefix=env["QDL_BETA_REDIS_PREFIX"],
            consumer_group=env["QDL_BETA_CONSUMER_GROUP"],
            cursor_keys={str(key): str(value).encode() for key, value in keys_raw.items()},
            active_cursor_key_id=env["QDL_BETA_CURSOR_ACTIVE_KEY_ID"],
            cursor_ttl_seconds=int(env.get("QDL_BETA_CURSOR_TTL_SECONDS", "3600")),
            http_port=int(env.get("QDL_BETA_HTTP_PORT", "18100")),
            grpc_port=int(env.get("QDL_BETA_GRPC_PORT", "18110")),
            max_request_bytes=int(env.get("QDL_BETA_MAX_REQUEST_BYTES", "1048576")),
            max_concurrent_requests=int(
                env.get("QDL_BETA_MAX_CONCURRENT_REQUESTS", "200")
            ),
            max_concurrent_rpcs=int(env.get("QDL_BETA_MAX_CONCURRENT_RPCS", "200")),
            max_streams=int(env.get("QDL_BETA_MAX_STREAMS", "100")),
            max_buffer_events=int(env.get("QDL_BETA_MAX_BUFFER_EVENTS", "2000")),
            max_replay_events=int(env.get("QDL_BETA_MAX_REPLAY_EVENTS", "5000")),
            lease_shard_id=env.get("QDL_BETA_LEASE_SHARD_ID", "stream-v2-public-beta"),
            lease_ttl_seconds=int(env.get("QDL_BETA_LEASE_TTL_SECONDS", "15")),
            lease_renew_seconds=float(env.get("QDL_BETA_LEASE_RENEW_SECONDS", "5")),
        )

    def public_manifest(self) -> dict[str, object]:
        return {
            "role": self.role,
            "instance_id": self.instance_id,
            "environment": self.environment,
            "config_revision": self.config_revision,
            "authority": "V1_SHADOW_READ_ONLY",
            "authority_revision": self.authority_revision,
            "schema_digest": self.schema_digest,
            "redis_prefix": self.redis_prefix,
            "consumer_group": self.consumer_group,
            "cursor_key_ids": sorted(self.cursor_keys),
            "active_cursor_key_id": self.active_cursor_key_id,
            "cursor_ttl_seconds": self.cursor_ttl_seconds,
            "state_dir": str(self.state_dir),
            "durable_state_dir": str(self.durable_state_dir),
            "source_bindings": bool(self.source_bindings_path),
            "owns_venue_connections": False,
            "writes_legacy_namespaces": False,
        }


def load_beta_manifests(config: BetaRuntimeConfig) -> ConsumerManifestRegistry:
    manifests = tuple(ConsumerManifestLoader.load(path) for path in config.manifest_paths)
    if any(item.environment != config.environment for item in manifests):
        raise ValueError("consumer manifest environment does not match beta runtime")
    return ConsumerManifestRegistry(manifests)


def build_beta_identity(
    config: BetaRuntimeConfig, manifests: ConsumerManifestRegistry
) -> DataPlaneIdentityService:
    security = DataPlaneSecurityConfig.from_environment()
    if security.environment != config.environment:
        raise ValueError("JWT and beta runtime environments do not match")
    quota = RedisMinuteQuota.from_url(
        config.redis_url, prefix=f"{config.redis_prefix}:identity"
    )
    return DataPlaneIdentityService(security, manifests, quota=quota)


def build_empty_query_service() -> V2QueryService:
    return V2QueryService(
        instruments=InstrumentQuery(InstrumentRegistry()),
        backend=MemoryMarketDataBackend(),
        entitlements=EntitlementPolicy(()),
    )


def build_beta_spool(config: BetaRuntimeConfig) -> SQLiteDurableSpool:
    return SQLiteDurableSpool(SpoolConfig(
        path=config.durable_state_dir / "canonical-shadow.sqlite3",
        max_records=100_000,
        max_payload_bytes=256 * 1024 * 1024,
        max_storage_bytes=384 * 1024 * 1024,
        max_consumer_checkpoints=4096,
        min_free_disk_bytes=64 * 1024 * 1024,
        consumer_ttl_seconds=config.cursor_ttl_seconds,
    ))


def build_beta_handoff(
    config: BetaRuntimeConfig, spool: SQLiteDurableSpool
) -> GapFreeHandoff:
    codec = SignedHandoffCursorCodec(
        config.cursor_keys,
        active_key_id=config.active_cursor_key_id,
    )
    return GapFreeHandoff(
        spool, codec, checkpoint_ttl_seconds=config.cursor_ttl_seconds
    )


def _ready(
    name: str,
    *,
    revision: str | None = None,
    detail: str = "available",
) -> ComponentReadiness:
    return ComponentReadiness(
        name,
        ComponentState.READY,
        detail=detail,
        revision=revision,
        checked_at_ns=time.time_ns(),
    )


def beta_readiness(
    config: BetaRuntimeConfig,
    manifests: ConsumerManifestRegistry,
    spool: SQLiteDurableSpool,
    *,
    extra_probes=(),
    quota: RedisMinuteQuota | None = None,
) -> MeasuredRuntimeReadiness:
    async def durable_source():
        stats = await asyncio.to_thread(spool.stats)
        return _ready(
            "durable_source",
            detail=f"bounded shadow spool records={stats.records} utilization={stats.utilization:.6f}",
        )

    async def shared_quota():
        if quota is None or not await asyncio.to_thread(quota.ping):
            return ComponentReadiness(
                "shared_quota",
                ComponentState.NOT_READY,
                detail="isolated beta Redis quota is unavailable",
                checked_at_ns=time.time_ns(),
            )
        return _ready("shared_quota", detail="isolated beta Redis quota reachable")

    probes = (
        CallableReadinessProbe(
            "workload_identity",
            lambda: _ready("workload_identity", revision=config.config_revision),
        ),
        CallableReadinessProbe(
            "consumer_manifests",
            lambda: _ready(
                "consumer_manifests",
                revision=str(max(item[1] for item in manifests.revisions)),
                detail=f"registered={manifests.count}",
            ),
        ),
        CallableReadinessProbe(
            "query_store",
            lambda: _ready("query_store", detail="provider-neutral shadow backend reachable"),
        ),
        CallableReadinessProbe("durable_source", durable_source),
        CallableReadinessProbe(
            "cursor_signer",
            lambda: _ready("cursor_signer", revision=config.active_cursor_key_id),
        ),
        CallableReadinessProbe(
            "authority_manifest",
            lambda: _ready("authority_manifest", revision=str(config.authority_revision)),
        ),
        CallableReadinessProbe("shared_quota", shared_quota),
        *tuple(extra_probes),
    )
    return MeasuredRuntimeReadiness(
        role=config.role,
        authority="V1",
        config_revision=config.config_revision,
        probes=probes,
    )


def install_beta_health(
    app: FastAPI,
    readiness: MeasuredRuntimeReadiness,
    runtime_manifest: Mapping[str, object],
) -> None:
    @app.get("/health/live", include_in_schema=False)
    async def live():
        return {"status": "alive", "runtime": runtime_manifest}

    @app.get("/health/ready", include_in_schema=False)
    async def ready():
        snapshot = await readiness.snapshot()
        return JSONResponse(
            status_code=200 if snapshot.ready else 503,
            content={
                "status": snapshot.status,
                "ready": snapshot.ready,
                "role": snapshot.role,
                "authority": snapshot.authority,
                "config_revision": snapshot.config_revision,
            },
        )

    @app.get("/health/dependencies", include_in_schema=False)
    async def dependencies():
        snapshot = await readiness.snapshot()
        return {
            "status": snapshot.status,
            "ready": snapshot.ready,
            "components": [
                {**asdict(item), "state": item.state.value}
                for item in snapshot.components
            ],
        }


def create_beta_query_app(config: BetaRuntimeConfig | None = None) -> FastAPI:
    config = config or BetaRuntimeConfig.from_environment("query_v2")
    config.state_dir.mkdir(parents=True, exist_ok=True)
    manifests = load_beta_manifests(config)
    identity = build_beta_identity(config, manifests)
    spool = build_beta_spool(config)
    cursor_issuer = None
    extra_probes = ()
    if config.source_bindings_path is None:
        query_service = build_empty_query_service()
    else:
        catalog = CanarySourceCatalog.load(config.source_bindings_path)
        handoff = build_beta_handoff(config, spool)
        query_service, _backend, cursor_issuer = build_canary_query_stack(
            spool=spool,
            catalog=catalog,
            schema_digest=config.schema_digest,
            handoff=handoff,
            cursor_ttl_seconds=config.cursor_ttl_seconds,
        )
        extra_probes = (
            CallableReadinessProbe(
                "instrument_catalog",
                lambda: _ready(
                    "instrument_catalog",
                    revision=str(catalog.catalog_revision),
                    detail=f"bindings={len(catalog.bindings)}",
                ),
            ),
        )
    readiness = beta_readiness(
        config,
        manifests,
        spool,
        extra_probes=extra_probes,
        quota=identity.quota,
    )
    audit = AuditChain(config.audit_path)
    audit.append(
        actor=config.instance_id,
        action="beta.query_v2.start",
        environment=config.environment,
        request_id=config.config_revision,
        result="READY_EVALUATED",
        details=config.public_manifest(),
    )
    app = create_v2_app(
        query_service,
        identity_service=identity,
        readiness_service=readiness,
        cursor_issuer=cursor_issuer,
        request_bounds=RequestBounds(
            max_request_bytes=config.max_request_bytes,
            max_concurrent_requests=config.max_concurrent_requests,
        ),
    )
    app.state.beta_spool = spool
    app.state.beta_audit = audit
    app.state.runtime_manifest = config.public_manifest()
    install_beta_health(app, readiness, config.public_manifest())

    @app.on_event("shutdown")
    async def close_beta_query_state():
        await asyncio.to_thread(spool.close)
        await asyncio.to_thread(identity.quota.close)

    return app
