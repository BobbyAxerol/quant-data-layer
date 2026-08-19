from __future__ import annotations

import json
import hashlib
import os
import threading
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping, Protocol

from redis import Redis
from redis.exceptions import RedisError

from qdl.consumer.manifest import ConsumerManifest, ConsumerManifestRegistry
from qdl.query import AccessPurpose, DataRequirement, FeedType
from qdl.security.policy import Permission, Principal, ServiceTokenVerifier


class DataPlanePermission(StrEnum):
    INSTRUMENTS_READ = "instruments:read"
    SNAPSHOT_READ = "snapshot:read"
    HISTORY_READ = "history:read"
    STATUS_READ = "status:read"
    STREAM_READ = "stream:read"
    QUALITY_READ = "quality:read"


class DataPlaneAccessError(PermissionError):
    def __init__(self, code: str, detail: str, *, status_code: int = 403) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class DataPlaneSecurityConfig:
    environment: str
    issuer: str
    audience: str
    keys_by_id: Mapping[str, str | bytes]
    algorithms: tuple[str, ...]
    max_token_lifetime_seconds: int = 900

    def __post_init__(self) -> None:
        if not all((self.environment.strip(), self.issuer.strip(), self.audience.strip())):
            raise ValueError("data-plane environment, issuer and audience are required")
        if not self.keys_by_id or not self.algorithms:
            raise ValueError("data-plane JWT keys and algorithms are required")

    @classmethod
    def from_environment(cls) -> "DataPlaneSecurityConfig":
        try:
            keys = json.loads(os.environ["QDL_DATA_JWT_KEYS_JSON"])
            issuer = os.environ["QDL_DATA_JWT_ISSUER"]
            audience = os.environ["QDL_DATA_JWT_AUDIENCE"]
        except (KeyError, json.JSONDecodeError) as error:
            raise RuntimeError("data-plane identity configuration is incomplete") from error
        if not isinstance(keys, dict) or not keys:
            raise RuntimeError("QDL_DATA_JWT_KEYS_JSON must be a non-empty object")
        algorithms = tuple(
            value.strip()
            for value in os.environ.get("QDL_DATA_JWT_ALGORITHMS", "RS256,ES256").split(",")
            if value.strip()
        )
        return cls(
            environment=os.environ.get("QDL_ENVIRONMENT", "paper").lower(),
            issuer=issuer,
            audience=audience,
            keys_by_id={str(key): str(value) for key, value in keys.items()},
            algorithms=algorithms,
            max_token_lifetime_seconds=int(
                os.environ.get("QDL_DATA_JWT_MAX_LIFETIME_SECONDS", "900")
            ),
        )


class RequestQuota(Protocol):
    def consume(self, manifest: ConsumerManifest) -> None: ...


class InMemoryMinuteQuota:
    """Bounded beta quota; Phase 7.1 replaces it with the selected shared backend."""

    def __init__(self, *, clock=time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._windows: dict[str, tuple[int, int]] = {}

    def consume(self, manifest: ConsumerManifest) -> None:
        minute = int(self._clock() // 60)
        with self._lock:
            current_minute, count = self._windows.get(manifest.consumer_id, (minute, 0))
            if current_minute != minute:
                current_minute, count = minute, 0
            if count >= manifest.quotas.requests_per_minute:
                raise DataPlaneAccessError(
                    "RATE_LIMITED",
                    "consumer request quota is exhausted",
                    status_code=429,
                )
            self._windows[manifest.consumer_id] = (current_minute, count + 1)


_REDIS_MINUTE_QUOTA = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
if count > tonumber(ARGV[1]) then
  return {0, count}
end
return {1, count}
"""


class RedisMinuteQuota:
    """Shared, atomic beta quota; Redis failure denies access rather than bypassing."""

    def __init__(self, redis: Redis, *, prefix: str) -> None:
        normalized = prefix.strip(": ")
        if not normalized.startswith(("qdl:beta:v2:", "qdl:stable:v2:")):
            raise ValueError(
                "shared quota requires a dedicated beta or stable Redis prefix"
            )
        self.redis = redis
        self.prefix = normalized

    @classmethod
    def from_url(cls, url: str, *, prefix: str) -> "RedisMinuteQuota":
        return cls(
            Redis.from_url(
                url,
                decode_responses=True,
                socket_connect_timeout=0.5,
                socket_timeout=0.5,
                health_check_interval=30,
            ),
            prefix=prefix,
        )

    def _key(self, manifest: ConsumerManifest, minute: int) -> str:
        identity = hashlib.sha256(manifest.consumer_id.encode()).hexdigest()[:24]
        return f"{self.prefix}:quota:minute:{identity}:{minute}"

    def consume(self, manifest: ConsumerManifest) -> None:
        minute = int(time.time() // 60)
        try:
            allowed, _ = self.redis.eval(
                _REDIS_MINUTE_QUOTA,
                1,
                self._key(manifest, minute),
                manifest.quotas.requests_per_minute,
                120_000,
            )
        except RedisError as error:
            raise DataPlaneAccessError(
                "DEPENDENCY_UNAVAILABLE",
                "shared request quota is unavailable",
                status_code=503,
            ) from error
        if int(allowed) != 1:
            raise DataPlaneAccessError(
                "RATE_LIMITED",
                "consumer request quota is exhausted",
                status_code=429,
            )

    def ping(self) -> bool:
        try:
            return bool(self.redis.ping())
        except RedisError:
            return False

    def close(self) -> None:
        self.redis.close()


@dataclass(frozen=True, slots=True)
class DataPlaneAccess:
    principal: Principal
    manifest: ConsumerManifest

    @property
    def consumer_id(self) -> str:
        return self.manifest.consumer_id

    def require_permission(self, permission: DataPlanePermission) -> None:
        principal_permission = {
            DataPlanePermission.INSTRUMENTS_READ: Permission.MARKET_DATA_READ,
            DataPlanePermission.SNAPSHOT_READ: Permission.MARKET_DATA_READ,
            DataPlanePermission.HISTORY_READ: Permission.HISTORY_READ,
            DataPlanePermission.STATUS_READ: Permission.MARKET_DATA_READ,
            DataPlanePermission.STREAM_READ: Permission.STREAM_CONSUME,
            DataPlanePermission.QUALITY_READ: Permission.MARKET_DATA_READ,
        }[permission]
        if not self.principal.has_permission(principal_permission):
            raise DataPlaneAccessError(
                "PERMISSION_DENIED",
                "workload token does not grant the requested data-plane scope",
            )
        if permission.value not in self.manifest.allowed_permissions:
            raise DataPlaneAccessError(
                "PERMISSION_DENIED",
                f"consumer is not entitled to {permission.value}",
            )

    def require_consumer(self, consumer_id: str) -> None:
        if consumer_id != self.manifest.consumer_id:
            raise DataPlaneAccessError(
                "CONSUMER_MISMATCH",
                "authenticated workload is not bound to the requested consumer",
            )

    def require_purpose(self, purpose: AccessPurpose) -> None:
        if not self.manifest.purpose_allowed(purpose):
            raise DataPlaneAccessError(
                "PERMISSION_DENIED",
                "consumer manifest does not allow the requested data purpose",
            )

    def require_requirement(self, requirement: DataRequirement) -> None:
        if not self.manifest.requirement_allowed(requirement):
            raise DataPlaneAccessError(
                "PERMISSION_DENIED",
                "data requirement is outside the registered consumer manifest",
            )
        if requirement.warmup_limit > self.manifest.quotas.max_warmup_rows:
            raise DataPlaneAccessError(
                "QUOTA_EXCEEDED",
                "warmup limit exceeds the registered consumer quota",
                status_code=429,
            )

    def require_batch_size(self, size: int) -> None:
        if size > self.manifest.quotas.max_batch_items:
            raise DataPlaneAccessError(
                "QUOTA_EXCEEDED",
                "batch size exceeds the registered consumer quota",
                status_code=429,
            )

    def require_stream_buffer(self, size: int) -> None:
        if size < 1 or size > self.manifest.quotas.max_buffer_events:
            raise DataPlaneAccessError(
                "QUOTA_EXCEEDED",
                "stream buffer exceeds the registered consumer quota",
                status_code=429,
            )

    def require_feed_scope(self, *, instrument_uid: str, feed: FeedType | str) -> None:
        parsed_feed = feed if isinstance(feed, FeedType) else FeedType(str(feed))
        if not self.manifest.feed_scope_allowed(
            instrument_uid=instrument_uid,
            feed=parsed_feed,
        ):
            raise DataPlaneAccessError(
                "PERMISSION_DENIED",
                "stream cursor scope is outside the registered consumer manifest",
            )


class DataPlaneIdentityService:
    """Application trust boundary shared by REST dependencies and gRPC interceptors."""

    def __init__(
        self,
        config: DataPlaneSecurityConfig,
        manifests: ConsumerManifestRegistry,
        *,
        quota: RequestQuota | None = None,
    ) -> None:
        self.config = config
        self.manifests = manifests
        self.quota = quota or InMemoryMinuteQuota()
        self._verifier = ServiceTokenVerifier(
            issuer=config.issuer,
            audience=config.audience,
            keys_by_id=config.keys_by_id,
            algorithms=config.algorithms,
            max_lifetime_seconds=config.max_token_lifetime_seconds,
        )

    def authenticate(self, bearer_token: str, *, consumer_id: str) -> DataPlaneAccess:
        if not bearer_token.strip():
            raise DataPlaneAccessError(
                "UNAUTHENTICATED", "workload bearer token is required", status_code=401
            )
        try:
            principal = self._verifier.verify(
                bearer_token,
                expected_environment=self.config.environment,
            )
            manifest = self.manifests.by_subject(
                environment=principal.environment,
                subject=principal.subject,
            )
        except (PermissionError, KeyError, ValueError) as error:
            raise DataPlaneAccessError(
                "UNAUTHENTICATED", str(error), status_code=401
            ) from error
        access = DataPlaneAccess(principal, manifest)
        if principal.consumer_manifest_revision != manifest.manifest_revision:
            raise DataPlaneAccessError(
                "UNAUTHENTICATED",
                "workload token is not bound to the active consumer manifest revision",
                status_code=401,
            )
        access.require_consumer(consumer_id)
        self.quota.consume(manifest)
        return access
