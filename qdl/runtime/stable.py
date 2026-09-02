from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import ssl
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
from qdl.runtime.stable_deployment import validate_shared_authority_record
from qdl.runtime.stable_ingest import (
    StableHttpCanonicalSink,
    install_stable_canonical_ingest,
)
from qdl.runtime.stable_projector import (
    StableProjectorEngine,
    supervise_stable_projector,
)
from qdl.runtime.stable_source import (
    StableCatalogCursorScopeValidator,
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


_STABLE_SPOOL_RECORD_FLOOR = 1_000_000
_STABLE_SPOOL_PARTITION_WINDOW = 10_000


@dataclass(frozen=True, slots=True)
class StableSpoolCapacity:
    """Catalog-derived hard bounds for the shared canonical replay cache."""

    physical_partitions: int
    max_partition_records: int
    max_records: int


def stable_spool_capacity(catalog: StableSourceCatalog) -> StableSpoolCapacity:
    """Keep global capacity compatible with the catalog's physical windows.

    Snapshot and delta bindings for a single book deliberately share one
    partition. The record bound must therefore count physical keys rather than
    logical product requirements, otherwise a larger manifest can deadlock the
    shared cache before any declared partition window is reached.
    """

    physical_partitions = len({binding.partition_key for binding in catalog.bindings})
    if physical_partitions <= 0:
        raise ValueError("stable spool requires at least one physical partition")
    return StableSpoolCapacity(
        physical_partitions=physical_partitions,
        max_partition_records=_STABLE_SPOOL_PARTITION_WINDOW,
        max_records=max(
            _STABLE_SPOOL_RECORD_FLOOR,
            physical_partitions * _STABLE_SPOOL_PARTITION_WINDOW,
        ),
    )


def _env_flag(
    env: Mapping[str, str], name: str, *, default: bool
) -> bool:
    """Read a boolean deployment flag, refusing anything ambiguous.

    A misspelled value must not quietly select a default: a flag that governs
    whether a data product is served has to fail loudly when its value is not
    understood.
    """
    raw = env.get(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean flag, got {raw!r}")


def _load_runtime_authority(env: Mapping[str, str]) -> Mapping[str, object]:
    """Load the generated authority record and reject env-only authority drift."""
    runtime_dir = env.get("QDL_STABLE_RUNTIME_DIR", "").strip()
    if not runtime_dir:
        raise ValueError("QDL_STABLE_RUNTIME_DIR is required for authority validation")
    path = Path(runtime_dir) / "authority.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("stable runtime authority record is unreadable") from error
    if not isinstance(payload, dict):
        raise ValueError("stable runtime authority record must be an object")
    validate_shared_authority_record(payload)

    configured_mode = env.get("QDL_STABLE_AUTHORITY_MODE")
    if configured_mode is not None and configured_mode != payload["mode"]:
        raise ValueError("stable runtime authority mode differs from generated record")
    configured_revision = env.get("QDL_STABLE_AUTHORITY_REVISION")
    if configured_revision is not None:
        try:
            revision = int(configured_revision)
        except ValueError as error:
            raise ValueError("stable runtime authority revision is invalid") from error
        if revision != payload["revision"]:
            raise ValueError(
                "stable runtime authority revision differs from generated record"
            )
    return payload


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
    tls_ca_path: Path
    tls_certificate_path: Path
    tls_private_key_path: Path
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
    request_deadline_seconds: float
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
    # Off unless a deployment turns it on. Declaring catalog metadata for an
    # instrument must never open the pass-through product by itself.
    pass_through_enabled: bool = False
    # Reference data is a distinct provider-authentic alpha/research product.
    # It remains dark until a reviewed stable-runtime config enables it.
    reference_data_enabled: bool = False
    # Opt-in only: this is the existing internal Rust core, never a public or
    # arbitrary provider endpoint.  When absent, V2 preserves its previous
    # reference adapter construction and does not attempt a coordinator call.
    provider_admission_url: str | None = None
    # Query/stream may trust an additive client-CA bundle while their own
    # client-side trust remains pinned to the server CA. Keeping this optional
    # preserves existing one-CA deployments.
    tls_client_ca_path: Path | None = None

    def __post_init__(self) -> None:
        if self.role not in {"query_v2", "stream_v2", "projector_v2"}:
            raise ValueError("stable role is invalid")
        if self.authority_mode not in {"RUST_SHADOW", "RUST_PRIMARY"}:
            raise ValueError("stable runtime authority mode is unsupported")
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
        missing_tls = [
            path for path in (
                self.tls_ca_path,
                self.tls_certificate_path,
                self.tls_private_key_path,
                self.tls_client_authority_path,
            ) if not path.is_file()
        ]
        if missing_tls:
            raise ValueError("stable workload TLS files are unavailable")
        if len(self.internal_ingest_secret) < 32:
            raise ValueError("stable internal ingest secret must contain 256 bits")
        if self.provider_admission_url not in {None, "http://rust_core:8300"}:
            raise ValueError("stable provider admission URL must be private rust_core")
        if self.active_cursor_key_id not in self.cursor_keys or any(
            len(value) < 32 for value in self.cursor_keys.values()
        ):
            raise ValueError("stable active cursor key is unavailable or weak")
        if not 60 <= self.cursor_ttl_seconds <= 86_400:
            raise ValueError("stable cursor TTL must be 60..86400 seconds")
        if not 1 <= self.request_deadline_seconds <= 120:
            raise ValueError("stable request deadline must be 1..120 seconds")
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
            )) or not self.stream_ingest_urls:
                raise ValueError("stable projector Kafka/stream dependencies are required")

    @property
    def session_liveness_dir(self) -> Path:
        """Shared read-only query view of bounded ingestor session state."""

        # QDL_STABLE_STATE_DIR is the mounted runtime root, not its parent.
        # Native ingestors write ``<state_dir>/session-liveness/<lane>``.
        return self.state_dir / "session-liveness"

    @classmethod
    def from_environment(
        cls, role: str, values: Mapping[str, str] | None = None
    ) -> "StableRuntimeConfig":
        env = os.environ if values is None else values
        authority = _load_runtime_authority(env)
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
            authority_mode=str(authority["mode"]),
            authority_revision=int(authority["revision"]),
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
            tls_ca_path=Path(env["QDL_STABLE_TLS_CA_FILE"]),
            tls_certificate_path=Path(env["QDL_STABLE_TLS_CERT_FILE"]),
            tls_private_key_path=Path(env["QDL_STABLE_TLS_KEY_FILE"]),
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
            request_deadline_seconds=float(
                env.get("QDL_STABLE_REQUEST_DEADLINE_SECONDS", "10")
            ),
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
            pass_through_enabled=_env_flag(
                env, "QDL_STABLE_PASS_THROUGH_ENABLED", default=False
            ),
            reference_data_enabled=_env_flag(
                env, "QDL_STABLE_REFERENCE_DATA_ENABLED", default=False
            ),
            provider_admission_url=env.get("QDL_STABLE_PROVIDER_ADMISSION_URL"),
            tls_client_ca_path=Path(env.get(
                "QDL_STABLE_TLS_CLIENT_CA_FILE", env["QDL_STABLE_TLS_CA_FILE"]
            )),
        )

    @property
    def tls_client_authority_path(self) -> Path:
        """CA bundle used only when this runtime authenticates mTLS clients."""
        return self.tls_client_ca_path or self.tls_ca_path

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
            "reference_data_enabled": self.reference_data_enabled,
            "transport_security": "MTLS_PLUS_JWT",
        }


def _ready(name: str, *, detail: str, revision: str | None = None) -> ComponentReadiness:
    return ComponentReadiness(
        name, ComponentState.READY, detail=detail, revision=revision,
        checked_at_ns=time.time_ns(),
    )


def stable_client_ssl_context(config: StableRuntimeConfig) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=str(config.tls_ca_path))
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(
        certfile=str(config.tls_certificate_path),
        keyfile=str(config.tls_private_key_path),
    )
    return context


def stable_uvicorn_tls(config: StableRuntimeConfig) -> dict[str, object]:
    return {
        "ssl_keyfile": str(config.tls_private_key_path),
        "ssl_certfile": str(config.tls_certificate_path),
        "ssl_ca_certs": str(config.tls_client_authority_path),
        "ssl_cert_reqs": ssl.CERT_REQUIRED,
    }


def stable_grpc_server_credentials(
    config: StableRuntimeConfig,
) -> grpc.ServerCredentials:
    return grpc.ssl_server_credentials(
        ((config.tls_private_key_path.read_bytes(), config.tls_certificate_path.read_bytes()),),
        root_certificates=config.tls_client_authority_path.read_bytes(),
        require_client_auth=True,
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


def build_stable_spool(
    config: StableRuntimeConfig, catalog: StableSourceCatalog
) -> SQLiteDurableSpool:
    config.durable_state_dir.mkdir(parents=True, exist_ok=True)
    capacity = stable_spool_capacity(catalog)
    return SQLiteDurableSpool(SpoolConfig(
        path=config.durable_state_dir / "canonical-cache.sqlite3",
        max_records=capacity.max_records,
        max_payload_bytes=2 * 1024 * 1024 * 1024,
        max_storage_bytes=3 * 1024 * 1024 * 1024,
        max_partitions=100_000,
        max_consumer_checkpoints=100_000,
        min_free_disk_bytes=512 * 1024 * 1024,
        consumer_ttl_seconds=config.cursor_ttl_seconds,
        replay_retention_seconds=24 * 3600,
        max_partition_records=capacity.max_partition_records,
        verify_integrity_on_open=False,
    ))


def build_stable_handoff(
    config: StableRuntimeConfig, spool: SQLiteDurableSpool
) -> GapFreeHandoff:
    return GapFreeHandoff(
        spool,
        SignedHandoffCursorCodec(
            config.cursor_keys,
            active_key_id=config.active_cursor_key_id,
            generation_id=spool.cache_id,
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
        summary = await asyncio.to_thread(spool.readiness_summary)
        return _ready(
            "query_cache",
            detail=(
                "bounded rebuildable cache readable "
                f"records={summary.records} payload_bytes={summary.payload_bytes}"
            ),
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
    catalog = StableSourceCatalog.load(config.source_bindings_path)
    spool = build_stable_spool(config, catalog)
    handoff = build_stable_handoff(config, spool)
    service, _backend, issuer = build_stable_query_stack(
        spool=spool, catalog=catalog, schema_digest=config.schema_digest,
        handoff=handoff, cursor_ttl_seconds=config.cursor_ttl_seconds,
        pass_through_enabled=config.pass_through_enabled,
        reference_data_enabled=config.reference_data_enabled,
        provider_admission_url=config.provider_admission_url,
        provider_admission_secret=config.internal_ingest_secret,
        session_liveness_root=str(config.session_liveness_dir),
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
            request_deadline_seconds=config.request_deadline_seconds,
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
        bound = self.grpc_server.add_secure_port(
            f"0.0.0.0:{self.config.grpc_port}",
            stable_grpc_server_credentials(self.config),
        )
        if bound != self.config.grpc_port:
            raise RuntimeError("stable gRPC mTLS port binding failed")
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
    catalog = StableSourceCatalog.load(config.source_bindings_path)
    spool = build_stable_spool(config, catalog)
    handoff = build_stable_handoff(config, spool)
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
        pass_through_enabled=config.pass_through_enabled,
        reference_data_enabled=config.reference_data_enabled,
        provider_admission_url=config.provider_admission_url,
        provider_admission_secret=config.internal_ingest_secret,
        session_liveness_root=str(config.session_liveness_dir),
    )
    grpc_service = GrpcMarketDataService(
        gateway=gateway, query_service=query_service,
        snapshot_loader=StableGrpcSnapshotLoader(
            service=query_service, backend=backend, issuer=issuer
        ),
        cursor_scope_validator=StableCatalogCursorScopeValidator(catalog),
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


async def serve_stable_query() -> None:
    config = StableRuntimeConfig.from_environment("query_v2")
    app = create_stable_query_app(config)
    server = uvicorn.Server(uvicorn.Config(
        app, host="0.0.0.0", port=config.http_port,
        log_level="info", access_log=False,
        **stable_uvicorn_tls(config),
    ))
    await server.serve()


async def serve_stable_stream() -> None:
    runtime = create_stable_stream_runtime()
    await runtime.start()
    server = uvicorn.Server(uvicorn.Config(
        runtime.health_app, host="0.0.0.0", port=runtime.config.http_port,
        log_level="info", access_log=False,
        **stable_uvicorn_tls(runtime.config),
    ))
    try:
        await server.serve()
    finally:
        await runtime.stop()


async def serve_stable_projector() -> None:
    config = StableRuntimeConfig.from_environment("projector_v2")
    config.state_dir.mkdir(parents=True, exist_ok=True)
    manifests = load_stable_manifests(config)
    catalog = StableSourceCatalog.load(config.source_bindings_path)
    spool = build_stable_spool(config, catalog)
    assert config.kafka_cert_root is not None
    assert config.kafka_bootstrap_servers is not None
    assert config.kafka_client_id is not None
    assert config.kafka_canonical_topic is not None
    broker_config = KafkaProjectorConfig(
        bootstrap_servers=config.kafka_bootstrap_servers,
        client_id=config.kafka_client_id,
        group_id=config.consumer_group,
        raw_topics=(),  # Rust canonical records carry private raw lineage.
        canonical_topic=config.kafka_canonical_topic,
        ca_path=config.kafka_cert_root / "ca.crt",
        certificate_path=config.kafka_cert_root / "client.crt",
        key_path=config.kafka_cert_root / "client.key",
        checkpoint_batch_size=512,
        checkpoint_interval_ms=100,
    )
    redis_client = redis.Redis.from_url(config.redis_url)
    quota = RedisMinuteQuota.from_url(
        config.redis_url, prefix=f"{config.redis_prefix}:projector"
    )
    sink = StableHttpCanonicalSink(
        config.stream_ingest_urls, config.internal_ingest_secret, spool,
        max_request_bytes=config.max_request_bytes,
        ssl_context=stable_client_ssl_context(config),
    )
    projector = StableCompatibilityProjector(
        catalog, namespace=config.redis_prefix.rstrip(":")
    )
    target = RedisStableProjectionTarget(
        redis_client,
        namespace=config.redis_prefix.rstrip(":"),
        dedicated_database=True,
    )
    spool_usage = await asyncio.to_thread(spool.readiness_summary)
    await asyncio.to_thread(
        target.bind_cache,
        spool.cache_id,
        initialize_if_missing=spool_usage.records == 0,
    )
    active_broker: list[ConfluentProjectorBroker | None] = [None]

    def broker_factory():
        broker = ConfluentProjectorBroker(broker_config)
        return broker, StableProjectorEngine(
            broker=broker,
            spool=spool,
            catalog=catalog,
            canonical_topic=config.kafka_canonical_topic,
            raw_topics=(),  # Rust canonical records carry private raw lineage.
            sink=sink,
            projector=projector,
            target=target,
            max_pending_records=config.max_pending_records,
            max_pending_bytes=config.max_pending_bytes,
            max_batch_records=512,
            batch_wait_seconds=0.01,
        )

    def on_broker(broker):
        active_broker[0] = broker

    async def kafka_probe() -> ComponentReadiness:
        broker = active_broker[0]
        try:
            available = broker is not None and await asyncio.to_thread(
                broker.ping, 1.0
            )
        except Exception:  # noqa: BLE001 - readiness must degrade, not crash
            available = False
        if not available:
            return ComponentReadiness(
                "kafka_read_committed",
                ComponentState.NOT_READY,
                detail="stable Kafka metadata unavailable",
                checked_at_ns=time.time_ns(),
            )
        return _ready(
            "kafka_read_committed",
            detail="read_committed/manual-checkpoint consumer reachable",
        )

    async def projection_cache_probe() -> ComponentReadiness:
        bound = await asyncio.to_thread(target.cache_is_bound)
        if not bound:
            return ComponentReadiness(
                "projection_cache_generation",
                ComponentState.NOT_READY,
                detail="Redis/SQLite projection cache identity is missing or mismatched",
                checked_at_ns=time.time_ns(),
            )
        return _ready(
            "projection_cache_generation",
            detail=f"cache_id_sha256={hashlib.sha256(spool.cache_id.encode()).hexdigest()}",
        )

    readiness = stable_readiness(
        config,
        manifests,
        spool,
        quota=quota,
        extra_probes=(
            CallableReadinessProbe("kafka_read_committed", kafka_probe),
            CallableReadinessProbe("projection_cache_generation", projection_cache_probe),
            CallableReadinessProbe("instrument_catalog", lambda: _ready(
                "instrument_catalog",
                detail=f"bindings={len(catalog.bindings)}",
                revision=str(catalog.catalog_revision),
            )),
        ),
    )
    app = FastAPI(
        title="Quant Data Layer V2 Stable Projector Health",
        version="2.0.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    install_stable_health(app, readiness, config.public_manifest())
    server = uvicorn.Server(uvicorn.Config(
        app,
        host="0.0.0.0",
        port=config.http_port,
        log_level="info",
        access_log=False,
    ))
    health_task = asyncio.create_task(server.serve())
    try:
        await supervise_stable_projector(
            broker_factory=broker_factory,
            should_stop=health_task.done,
            on_broker=on_broker,
        )
        await health_task
    finally:
        server.should_exit = True
        if not health_task.done():
            await health_task
        await sink.close()
        await asyncio.to_thread(redis_client.close)
        await asyncio.to_thread(quota.close)
        await asyncio.to_thread(spool.close)
