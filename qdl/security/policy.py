from __future__ import annotations

import ipaddress
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

import jwt


class Permission(str, Enum):
    MARKET_DATA_READ = "market_data:read"
    HISTORY_READ = "history:read"
    STREAM_CONSUME = "stream:consume"
    CONSUMER_REGISTRY_WRITE = "consumer_registry:write"
    VENUE_OPERATE = "venue:operate"
    SCHEMA_OPERATE = "schema:operate"
    PLATFORM_ADMIN = "platform:admin"
    AUDIT_READ = "audit:read"


_ROLE_PERMISSIONS = {
    "market_data_reader": frozenset({Permission.MARKET_DATA_READ}),
    "historical_reader": frozenset({Permission.HISTORY_READ}),
    "stream_consumer": frozenset({Permission.STREAM_CONSUME}),
    "consumer_registry_writer": frozenset({Permission.CONSUMER_REGISTRY_WRITE}),
    "venue_operator": frozenset({Permission.VENUE_OPERATE}),
    "schema_operator": frozenset({Permission.SCHEMA_OPERATE}),
    "platform_admin": frozenset(Permission),
    "auditor": frozenset({Permission.AUDIT_READ}),
}


@dataclass(frozen=True, slots=True)
class Principal:
    subject: str
    environment: str
    roles: frozenset[str]
    venues: frozenset[str]
    token_id: str
    consumer_manifest_revision: int | None = None

    def has_permission(self, permission: Permission) -> bool:
        return any(
            permission in _ROLE_PERMISSIONS.get(role, ()) for role in self.roles
        )


class ServiceTokenVerifier:
    """Verifies short-lived workload JWTs with pinned key IDs and algorithms."""

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        keys_by_id: Mapping[str, str | bytes],
        algorithms: Sequence[str] = ("RS256", "ES256"),
        max_lifetime_seconds: int = 900,
    ):
        if not issuer or not audience or not keys_by_id:
            raise ValueError("issuer, audience and at least one verification key are required")
        if not algorithms or any(name.lower() == "none" for name in algorithms):
            raise ValueError("an explicit signed JWT algorithm allowlist is required")
        self._issuer = issuer
        self._audience = audience
        self._keys = dict(keys_by_id)
        self._algorithms = tuple(algorithms)
        self._max_lifetime = max_lifetime_seconds

    def verify(self, token: str, *, expected_environment: str) -> Principal:
        try:
            header = jwt.get_unverified_header(token)
            key_id = str(header.get("kid") or "")
            algorithm = str(header.get("alg") or "")
            if algorithm not in self._algorithms or key_id not in self._keys:
                raise PermissionError("untrusted workload token key or algorithm")
            claims = jwt.decode(
                token,
                self._keys[key_id],
                algorithms=[algorithm],
                issuer=self._issuer,
                audience=self._audience,
                options={
                    "require": [
                        "sub", "iss", "aud", "exp", "iat", "jti", "environment"
                    ]
                },
            )
        except jwt.PyJWTError as error:
            raise PermissionError("workload token verification failed") from error
        issued_at = int(claims["iat"])
        expires_at = int(claims["exp"])
        if expires_at <= issued_at or expires_at - issued_at > self._max_lifetime:
            raise PermissionError("workload token lifetime exceeds policy")
        environment = str(claims["environment"])
        if environment != expected_environment:
            raise PermissionError("workload token environment mismatch")
        roles = frozenset(str(role) for role in claims.get("roles", []))
        unknown = roles - _ROLE_PERMISSIONS.keys()
        if not roles or unknown:
            raise PermissionError("workload token contains unknown or empty roles")
        manifest_revision = claims.get("consumer_manifest_revision")
        if manifest_revision is not None:
            try:
                manifest_revision = int(manifest_revision)
            except (TypeError, ValueError) as error:
                raise PermissionError(
                    "workload token manifest revision is invalid"
                ) from error
            if manifest_revision < 1:
                raise PermissionError("workload token manifest revision is invalid")
        return Principal(
            subject=str(claims["sub"]),
            environment=environment,
            roles=roles,
            venues=frozenset(str(item).upper() for item in claims.get("venues", [])),
            token_id=str(claims["jti"]),
            consumer_manifest_revision=manifest_revision,
        )


class RbacAuthorizer:
    def require(
        self,
        principal: Principal,
        permission: Permission,
        *,
        environment: str,
        venue: str | None = None,
    ) -> None:
        if principal.environment != environment:
            raise PermissionError("principal cannot cross environment boundary")
        granted = frozenset(
            item for role in principal.roles for item in _ROLE_PERMISSIONS.get(role, ())
        )
        if permission not in granted:
            raise PermissionError(f"principal lacks permission {permission.value}")
        if venue is not None and principal.venues and venue.upper() not in principal.venues:
            raise PermissionError("principal is not authorized for venue scope")


@dataclass(frozen=True, slots=True)
class RegisteredTarget:
    source_id: str
    schemes: frozenset[str]
    hosts: frozenset[str]
    ports: frozenset[int]
    path_prefixes: tuple[str, ...]


class EgressPolicy:
    def __init__(self, targets: Sequence[RegisteredTarget]):
        self._targets = {target.source_id: target for target in targets}
        if len(self._targets) != len(targets):
            raise ValueError("source IDs must be unique")

    @staticmethod
    def _reject_unsafe_ip(host: str) -> None:
        try:
            address = ipaddress.ip_address(host.strip("[]"))
        except ValueError:
            return
        if not address.is_global:
            raise PermissionError("private, loopback, link-local or reserved egress is forbidden")

    def validate(self, source_id: str, url: str) -> str:
        try:
            target = self._targets[source_id]
        except KeyError as error:
            raise PermissionError("unregistered outbound source") from error
        parsed = urlsplit(url)
        if parsed.username or parsed.password or parsed.fragment:
            raise PermissionError("userinfo and fragments are forbidden in outbound URLs")
        host = (parsed.hostname or "").lower().rstrip(".")
        scheme = parsed.scheme.lower()
        port = parsed.port or (443 if scheme in {"https", "wss"} else 80)
        self._reject_unsafe_ip(host)
        if scheme not in target.schemes or host not in target.hosts or port not in target.ports:
            raise PermissionError("outbound target is outside the registered allowlist")
        if not any(parsed.path.startswith(prefix) for prefix in target.path_prefixes):
            raise PermissionError("outbound path is outside the registered allowlist")
        return url


@dataclass(frozen=True, slots=True)
class PayloadPolicy:
    max_bytes: int
    max_nesting_depth: int = 32
    max_numeric_characters: int = 128
    max_decompression_ratio: float = 100.0

    def validate_json(self, payload: bytes, *, compressed_bytes: int | None = None) -> Any:
        if len(payload) > self.max_bytes:
            raise ValueError("payload exceeds configured byte limit")
        if compressed_bytes is not None:
            if compressed_bytes <= 0 or len(payload) / compressed_bytes > self.max_decompression_ratio:
                raise ValueError("payload exceeds decompression ratio limit")
        value = json.loads(payload)
        self._walk(value, depth=0)
        return value

    def _walk(self, value: Any, *, depth: int) -> None:
        if depth > self.max_nesting_depth:
            raise ValueError("payload exceeds nesting depth limit")
        if isinstance(value, Mapping):
            for key, item in value.items():
                if len(str(key)) > 256:
                    raise ValueError("payload key is too long")
                self._walk(item, depth=depth + 1)
        elif isinstance(value, list):
            for item in value:
                self._walk(item, depth=depth + 1)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            if len(str(value)) > self.max_numeric_characters:
                raise ValueError("numeric field exceeds configured length")
        elif isinstance(value, str) and len(value) > self.max_bytes:
            raise ValueError("string field exceeds configured length")


_SECRET_KEYS = frozenset(
    {"authorization", "api_key", "apikey", "secret", "password", "token", "private_key"}
)


def redact_secrets(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): "[REDACTED]" if str(key).lower() in _SECRET_KEYS else redact_secrets(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_secrets(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_secrets(item) for item in value)
    return value
