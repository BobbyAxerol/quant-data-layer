from __future__ import annotations

import json
import hashlib
import logging
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from qdl._compat import StrEnum
from typing import Mapping, Protocol

from redis import Redis
from redis.exceptions import RedisError

from qdl.consumer.manifest import ConsumerManifest, ConsumerManifestRegistry
from qdl.query import AccessPurpose, DataRequirement, FeedType
from qdl.security.policy import Principal, ServiceTokenVerifier


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
    subjects_by_key_id: Mapping[str, str] | None = None
    environments_by_key_id: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        if not all((self.environment.strip(), self.issuer.strip(), self.audience.strip())):
            raise ValueError("data-plane environment, issuer and audience are required")
        if not self.keys_by_id or not self.algorithms:
            raise ValueError("data-plane JWT keys and algorithms are required")
        if self.subjects_by_key_id is not None:
            if set(self.subjects_by_key_id) != set(self.keys_by_id):
                raise ValueError(
                    "data-plane JWT key-subject bindings must cover exactly the keyring"
                )
            if any(
                not isinstance(subject, str) or not subject.strip()
                for subject in self.subjects_by_key_id.values()
            ):
                raise ValueError("data-plane JWT key-subject bindings are invalid")

        if self.environments_by_key_id is not None:
            if (not isinstance(self.environments_by_key_id, Mapping)
                    or self.subjects_by_key_id is None
                    or set(self.environments_by_key_id) != set(self.keys_by_id)):
                raise ValueError("JWT key-realm bindings require exact key-subject coverage")
            if any(value not in {"paper", "sandbox", "live"} for value in self.environments_by_key_id.values()):
                raise ValueError("JWT consumer realm must be paper, sandbox or live")

    @property
    def consumer_environments(self) -> frozenset[str]:
        if self.environments_by_key_id is None:
            return frozenset({self.environment})
        return frozenset(self.environments_by_key_id.values())

    @classmethod
    def from_environment(cls) -> "DataPlaneSecurityConfig":
        try:
            keys = json.loads(os.environ["QDL_DATA_JWT_KEYS_JSON"])
            key_subjects = json.loads(os.environ["QDL_DATA_JWT_KEY_SUBJECTS_JSON"])
            realms = json.loads(os.environ.get("QDL_DATA_JWT_KEY_ENVIRONMENTS_JSON", "null"))
            issuer = os.environ["QDL_DATA_JWT_ISSUER"]
            audience = os.environ["QDL_DATA_JWT_AUDIENCE"]
        except (KeyError, json.JSONDecodeError) as error:
            raise RuntimeError("data-plane identity configuration is incomplete") from error
        if not isinstance(keys, dict) or not keys:
            raise RuntimeError("QDL_DATA_JWT_KEYS_JSON must be a non-empty object")
        if not isinstance(key_subjects, dict) or not key_subjects:
            raise RuntimeError(
                "QDL_DATA_JWT_KEY_SUBJECTS_JSON must be a non-empty object"
            )
        algorithms = tuple(
            value.strip()
            for value in os.environ.get("QDL_DATA_JWT_ALGORITHMS", "RS256,ES256").split(",")
            if value.strip()
        )
        return cls(
            environment=os.environ.get("QDL_ENVIRONMENT", "paper").lower(),
            environments_by_key_id=realms,
            issuer=issuer,
            audience=audience,
            keys_by_id={str(key): str(value) for key, value in keys.items()},
            algorithms=algorithms,
            max_token_lifetime_seconds=int(
                os.environ.get("QDL_DATA_JWT_MAX_LIFETIME_SECONDS", "900")
            ),
            subjects_by_key_id={
                str(key): str(value) for key, value in key_subjects.items()
            },
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


logger = logging.getLogger(__name__)


class RedisMinuteQuota:
    """Shared, atomic beta quota; Redis failure denies access rather than bypassing.

    Exhaustion is reported once per consumer and minute (IDR-029: the per-identity spend was only visible by
    reading the Redis minute key), so the log is bounded by consumers x minutes, never by request rate.
    """

    def __init__(self, redis: Redis, *, prefix: str) -> None:
        normalized = prefix.strip(": ")
        if not normalized.startswith(("qdl:beta:v2:", "qdl:stable:v2:")):
            raise ValueError(
                "shared quota requires a dedicated beta or stable Redis prefix"
            )
        self.redis = redis
        self.prefix = normalized
        self._reported: dict[str, int] = {}

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
            allowed, count = self.redis.eval(
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
            if self._reported.get(manifest.consumer_id) != minute:
                self._reported[manifest.consumer_id] = minute
                logger.warning(json.dumps({
                    "event": "qdl_consumer_quota_exhausted", "consumer_id": manifest.consumer_id,
                    "minute": minute, "count": int(count), "limit": manifest.quotas.requests_per_minute,
                }, sort_keys=True))
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


class QuotaLease:
    """One request's quota unit(s), settled once the response is known (DL v2.4.0 S3)."""

    __slots__ = ("_quota", "_manifest", "_session", "_settled")

    def __init__(self, quota, manifest: ConsumerManifest, session: str | None) -> None:
        self._quota = quota
        self._manifest = manifest
        self._session = session
        self._settled = False

    @property
    def settled(self) -> bool:
        return self._settled

    def settle(self, *, charged: bool) -> None:
        if self._settled:
            return
        self._settled = True
        if charged:
            commit = getattr(self._quota, "commit", None)
            if callable(commit):
                commit(self._manifest)
        else:
            self._quota.refund(self._manifest, session=self._session)


class _CommittedLease:
    """A legacy quota already charged at authentication; settling does nothing."""

    __slots__ = ()
    settled = True

    def settle(self, *, charged: bool) -> None:
        del charged


_MAX_SESSION_KEY_CHARS = 128


def session_fairness_key(consumer_id: str, session: str | None) -> str | None:
    """A bounded fairness key for ``X-QDL-Session``; None when absent or unusable.

    The header is untrusted and only ever narrows fairness inside an
    authenticated identity: it never grants or widens anything.
    """

    if session is None:
        return None
    value = session.strip()
    if not value or len(value) > _MAX_SESSION_KEY_CHARS or not value.isprintable():
        return None
    return f"{consumer_id}#{value}"


class LocalTokenBucketQuota:
    """Per-replica in-memory token buckets: the coarse guard (DL v2.4.0 S7).

    * Identity bucket: ``requests_per_minute * share / 60`` per second with a
      burst of ``burst_seconds`` of that rate (at least 10). With ``share`` =
      1/replicas the fleet total is the manifest's minute quota.
    * Session bucket (only for requests that name ``X-QDL-Session``): a
      ``session_share`` of the identity rate, so one noisy session of a
      shared tier identity is throttled alone. Requests without the header
      use the identity bucket only (SDK 2.x before 2.8.0 is unchanged).
    * Optional shared layer (``RedisMinuteQuota``), counted when a request
      keeps its unit. A Redis failure is ignored: the local guard already
      admitted the request, so a Redis outage no longer answers 503 to every
      read. A shared "exhausted" answer closes that identity on this replica
      until the minute ends.

    * The Trading System identity (``full_share_prefixes``, default
      ``trading-system.``) gets its FULL manifest rate on every replica, not
      ``share``: its traffic is not balanced across replicas. The ledger's
      v2.3.1 R4 soak (CERTIFICATION_LEDGER.md, "TS writer Rust client stuck on
      kn_2 (86/87 connections)") and plan A.1 (the sequential writer stream
      100 % on one replica) show a peak per-replica share of ~1.0, and the
      writer's modelled steady demand (~6,000/min for 104 products, plan
      2026-11-13 stage A) exceeds rpm/2 = 5,000 of its 10,000. With share 1.0
      the TS can never meet this guard earlier than the pre-v2.4.0 global
      Redis minute counter, which also allowed the full rpm on one replica.

    No network call on the request path unless the shared layer is enabled.
    """

    def __init__(
        self,
        *,
        share: float = 0.5,
        burst_seconds: float = 10.0,
        session_share: float = 0.125,
        max_sessions: int = 10_000,
        full_share_prefixes: tuple[str, ...] = ("trading-system.",),
        shared: "RedisMinuteQuota | None" = None,
        clock=time.monotonic,
        wall_clock=time.time,
    ) -> None:
        if not 0 < share <= 1 or not 1 <= burst_seconds <= 600 or not 0 < session_share <= 1:
            raise ValueError("local quota share, burst and session share are out of bounds")
        if not 1 <= max_sessions <= 1_000_000:
            raise ValueError("local quota session bound is out of bounds")
        self.share = share
        self.burst_seconds = burst_seconds
        self.session_share = session_share
        self.max_sessions = max_sessions
        self.full_share_prefixes = tuple(item for item in full_share_prefixes if item)
        self.shared = shared
        self._clock = clock
        self._wall_clock = wall_clock
        self._lock = threading.Lock()
        self._identities: dict[str, list[float]] = {}
        self._sessions: "OrderedDict[str, list[float]]" = OrderedDict()
        self._shared_closed: dict[str, int] = {}
        self._shared_failures = 0
        self._reported_shared_failure_minute: int | None = None
        self.refused_identity = 0
        self.refused_session = 0

    @classmethod
    def from_environment(cls, environ: Mapping[str, str], *, redis_url: str | None, redis_prefix: str):
        mode = environ.get("QDL_V2_SHARED_QUOTA", "off").strip().lower()
        if mode not in {"off", "redis"}:
            raise ValueError("QDL_V2_SHARED_QUOTA must be off or redis")
        shared = None
        if mode == "redis":
            if not redis_url:
                raise ValueError("QDL_V2_SHARED_QUOTA=redis requires the stable Redis URL")
            shared = RedisMinuteQuota.from_url(redis_url, prefix=redis_prefix)
        return cls(
            share=float(environ.get("QDL_V2_QUOTA_REPLICA_SHARE", "0.5")),
            burst_seconds=float(environ.get("QDL_V2_QUOTA_BURST_SECONDS", "10")),
            session_share=float(environ.get("QDL_V2_SESSION_QUOTA_SHARE", "0.125")),
            full_share_prefixes=tuple(
                item.strip() for item in
                environ.get("QDL_V2_QUOTA_FULL_SHARE_PREFIXES", "trading-system.").split(",")
                if item.strip()
            ),
            shared=shared,
        )

    def share_for(self, consumer_id: str) -> float:
        return 1.0 if consumer_id.startswith(self.full_share_prefixes) else self.share

    def _shape(self, manifest: ConsumerManifest, fraction: float) -> tuple[float, float]:
        rate = (manifest.quotas.requests_per_minute * self.share_for(manifest.consumer_id)
                * fraction / 60.0)
        return rate, max(10.0, rate * self.burst_seconds)

    @staticmethod
    def _refill(bucket: list[float], rate: float, capacity: float, now: float) -> None:
        bucket[0] = min(capacity, bucket[0] + max(0.0, now - bucket[1]) * rate)
        bucket[1] = now

    def acquire(self, manifest: ConsumerManifest, *, session: str | None = None) -> QuotaLease:
        consumer = manifest.consumer_id
        now = self._clock()
        session_key = session_fairness_key(consumer, session)
        with self._lock:
            closed_minute = self._shared_closed.get(consumer)
            if closed_minute is not None:
                if closed_minute == int(self._wall_clock() // 60):
                    self.refused_identity += 1
                    raise DataPlaneAccessError(
                        "RATE_LIMITED", "consumer request quota is exhausted", status_code=429,
                    )
                del self._shared_closed[consumer]
            rate, capacity = self._shape(manifest, 1.0)
            identity = self._identities.setdefault(consumer, [capacity, now])
            self._refill(identity, rate, capacity, now)
            session_bucket = None
            if session_key is not None:
                s_rate, s_capacity = self._shape(manifest, self.session_share)
                session_bucket = self._sessions.get(session_key)
                if session_bucket is None:
                    session_bucket = [s_capacity, now]
                    self._sessions[session_key] = session_bucket
                    while len(self._sessions) > self.max_sessions:
                        self._sessions.popitem(last=False)
                else:
                    self._sessions.move_to_end(session_key)
                self._refill(session_bucket, s_rate, s_capacity, now)
                if session_bucket[0] < 1.0:
                    self.refused_session += 1
                    raise DataPlaneAccessError(
                        "RATE_LIMITED",
                        "session request rate is exhausted (fair share of the consumer quota)",
                        status_code=429,
                    )
            if identity[0] < 1.0:
                self.refused_identity += 1
                raise DataPlaneAccessError(
                    "RATE_LIMITED", "consumer request quota is exhausted", status_code=429,
                )
            identity[0] -= 1.0
            if session_bucket is not None:
                session_bucket[0] -= 1.0
        return QuotaLease(self, manifest, session)

    def refund(self, manifest: ConsumerManifest, *, session: str | None = None) -> None:
        consumer = manifest.consumer_id
        session_key = session_fairness_key(consumer, session)
        with self._lock:
            _rate, capacity = self._shape(manifest, 1.0)
            identity = self._identities.get(consumer)
            if identity is not None:
                identity[0] = min(capacity, identity[0] + 1.0)
            if session_key is not None:
                _s_rate, s_capacity = self._shape(manifest, self.session_share)
                bucket = self._sessions.get(session_key)
                if bucket is not None:
                    bucket[0] = min(s_capacity, bucket[0] + 1.0)

    def commit(self, manifest: ConsumerManifest) -> None:
        """Count a kept unit in the optional shared layer; never raises."""

        if self.shared is None:
            return
        try:
            self.shared.consume(manifest)
        except DataPlaneAccessError as error:
            if error.status_code == 429:
                with self._lock:
                    self._shared_closed[manifest.consumer_id] = int(self._wall_clock() // 60)
                return
            minute = int(self._wall_clock() // 60)
            with self._lock:
                self._shared_failures += 1
                report = self._reported_shared_failure_minute != minute
                self._reported_shared_failure_minute = minute
            if report:
                logger.warning(json.dumps({
                    "event": "qdl_shared_quota_unavailable", "minute": minute,
                    "detail": "shared quota layer failed; the local guard keeps serving",
                }, sort_keys=True))

    def consume(self, manifest: ConsumerManifest) -> None:
        """``RequestQuota``: take and keep one unit (gRPC and legacy callers)."""

        self.acquire(manifest).settle(charged=True)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "identities": len(self._identities),
                "sessions": len(self._sessions),
                "refused_identity": self.refused_identity,
                "refused_session": self.refused_session,
                "shared_failures": self._shared_failures,
                "shared_closed": len(self._shared_closed),
            }

    def ping(self) -> bool:
        return True if self.shared is None else self.shared.ping()

    def close(self) -> None:
        if self.shared is not None:
            self.shared.close()


@dataclass(frozen=True, slots=True)
class DataPlaneAccess:
    principal: Principal
    manifest: ConsumerManifest

    @property
    def consumer_id(self) -> str:
        return self.manifest.consumer_id

    def require_permission(self, permission: DataPlanePermission) -> None:
        """The consumer manifest's permissions are the only gate (DL v2.4.0 S8).

        Token role claims are accepted and ignored: the signing key is bound to
        the manifest subject, so the manifest already states what the workload
        may read. Before v2.4.0 the role claim was a second, redundant check
        (``Principal.has_permission``) that a token minter had to keep in step
        with the manifest.
        """
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
        warmup = requirement.warmup_specification
        requested_rows = requirement.warmup_limit
        if warmup is not None and warmup.rows is not None:
            requested_rows = warmup.rows
        elif warmup is not None and warmup.time_range is not None:
            if not requirement.interval:
                raise DataPlaneAccessError(
                    "QUOTA_EXCEEDED",
                    "time-range warmup requires an interval",
                    status_code=429,
                )
            from qdl.adapters.intervals import canonical_interval_ms

            duration_ns = (
                warmup.time_range.end_time_ns - warmup.time_range.start_time_ns
            )
            interval_ns = canonical_interval_ms(requirement.interval) * 1_000_000
            if duration_ns % interval_ns:
                raise DataPlaneAccessError(
                    "QUOTA_EXCEEDED",
                    "time-range warmup is not aligned to the interval",
                    status_code=429,
                )
            requested_rows = duration_ns // interval_ns
        if requested_rows > self.manifest.quotas.max_warmup_rows:
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


class VerifiedTokenCache:
    """Bounded LRU of verified workload tokens (DL v2.4.0 S1).

    Key: SHA-256 of the exact bearer token, so the cache never holds a token.
    Value: the verified ``Principal``, kept only while ``now < exp`` of that
    token and only while its signing key is still in the verifier's keyring
    (a rotated-out key evicts its entries on their next lookup;
    ``invalidate()`` drops everything). Anything not in the cache goes
    through full signature, issuer, audience, lifetime and realm
    verification, so a hit is exactly a token that already passed all of it.
    The manifest lookup, key-subject binding, revision accept-set and every
    per-request entitlement check still run on every request.
    """

    def __init__(self, *, max_entries: int = 4096, clock=time.time) -> None:
        if not 1 <= max_entries <= 1_000_000:
            raise ValueError("verified-token cache size is out of bounds")
        self._max_entries = max_entries
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: "OrderedDict[bytes, Principal]" = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    @staticmethod
    def key(token: str) -> bytes:
        return hashlib.sha256(token.encode("utf-8", "surrogatepass")).digest()

    def get(self, key: bytes, *, key_is_trusted) -> Principal | None:
        now = self._clock()
        with self._lock:
            principal = self._entries.get(key)
            if principal is None:
                self.misses += 1
                return None
            if (
                principal.expires_at is None
                or now >= principal.expires_at
                or not key_is_trusted(principal.key_id)
            ):
                del self._entries[key]
                self.evictions += 1
                self.misses += 1
                return None
            self._entries.move_to_end(key)
            self.hits += 1
            return principal

    def put(self, key: bytes, principal: Principal) -> None:
        if principal.expires_at is None:
            return
        with self._lock:
            self._entries[key] = principal
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)
                self.evictions += 1

    def invalidate(self) -> None:
        with self._lock:
            self.evictions += len(self._entries)
            self._entries.clear()

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "entries": len(self._entries),
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
            }


class DataPlaneIdentityService:
    """Application trust boundary shared by REST dependencies and gRPC interceptors."""

    def __init__(
        self,
        config: DataPlaneSecurityConfig,
        manifests: ConsumerManifestRegistry,
        *,
        quota: RequestQuota | None = None,
        token_cache: VerifiedTokenCache | None = None,
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
            environments_by_key_id=config.environments_by_key_id,
            enforce_roles=False,
        )
        self.token_cache = token_cache or VerifiedTokenCache()

    def rotate_keys(self, config: DataPlaneSecurityConfig) -> None:
        """Install a new keyring and drop every cached verification."""

        self.config = config
        self._verifier = ServiceTokenVerifier(
            issuer=config.issuer,
            audience=config.audience,
            keys_by_id=config.keys_by_id,
            algorithms=config.algorithms,
            max_lifetime_seconds=config.max_token_lifetime_seconds,
            environments_by_key_id=config.environments_by_key_id,
            enforce_roles=False,
        )
        self.token_cache.invalidate()

    def _verified_principal(self, bearer_token: str) -> Principal:
        key = VerifiedTokenCache.key(bearer_token)
        verifier = self._verifier
        principal = self.token_cache.get(key, key_is_trusted=verifier.has_key)
        if principal is None:
            principal = verifier.verify(
                bearer_token,
                expected_environment=self.config.environment,
            )
            self.token_cache.put(key, principal)
        return principal

    def authenticate(
        self, bearer_token: str, *, consumer_id: str, charge_quota: bool = True,
    ) -> DataPlaneAccess:
        """Verify the workload and bind it to its active manifest.

        ``charge_quota`` keeps the pre-v2.4.0 behaviour (one unit kept at
        authentication) for callers that do not settle a lease; the REST
        router passes False and calls ``admit`` (DL v2.4.0 S3).
        """
        if not bearer_token.strip():
            raise DataPlaneAccessError(
                "UNAUTHENTICATED", "workload bearer token is required", status_code=401
            )
        try:
            principal = self._verified_principal(bearer_token)
            manifest = self.manifests.by_subject(
                environment=principal.environment,
                subject=principal.subject,
            )
        except (PermissionError, KeyError, ValueError) as error:
            raise DataPlaneAccessError(
                "UNAUTHENTICATED", str(error), status_code=401
            ) from error
        if self.config.subjects_by_key_id is not None:
            expected_subject = self.config.subjects_by_key_id.get(principal.key_id or "")
            if expected_subject != principal.subject:
                raise DataPlaneAccessError(
                    "UNAUTHENTICATED",
                    "workload token signing key is not bound to the manifest subject",
                    status_code=401,
                )
        access = DataPlaneAccess(principal, manifest)
        # Exact revision unless the manifest declares a purely additive
        # accept-set (``min_token_revision``, DL v2.3.0); see
        # ``qdl.consumer.manifest_compat``.
        if not manifest.accepts_token_revision(principal.consumer_manifest_revision):
            raise DataPlaneAccessError(
                "UNAUTHENTICATED",
                "workload token is not bound to the active consumer manifest revision",
                status_code=401,
            )
        access.require_consumer(consumer_id)
        if charge_quota:
            self.quota.consume(manifest)
        return access

    def admit(self, access: DataPlaneAccess, *, session: str | None = None):
        """Take the request's quota unit(s); the caller settles the lease.

        Quotas with no refund (``InMemoryMinuteQuota``, ``RedisMinuteQuota``)
        are charged here and return an already-settled lease.
        """

        acquire = getattr(self.quota, "acquire", None)
        if callable(acquire):
            return acquire(access.manifest, session=session)
        self.quota.consume(access.manifest)
        return _CommittedLease()
