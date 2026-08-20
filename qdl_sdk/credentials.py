from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import jwt
from typing import Awaitable, Callable, Protocol


class CredentialProvider(Protocol):
    async def get_token(self) -> str: ...


@dataclass(frozen=True, slots=True)
class StaticBearerCredential:
    """Test/local credential; production should inject a rotating workload provider."""

    token: str

    def __post_init__(self) -> None:
        if not self.token.strip():
            raise ValueError("bearer token cannot be empty")

    async def get_token(self) -> str:
        return self.token


class CallbackCredentialProvider:
    def __init__(self, callback: Callable[[], str | Awaitable[str]]) -> None:
        self._callback = callback

    async def get_token(self) -> str:
        value = self._callback()
        token = await value if inspect.isawaitable(value) else value
        if not isinstance(token, str) or not token.strip():
            raise RuntimeError("workload credential provider returned an empty token")
        return token


class RotatingJwtCredentialProvider:
    """Signs bounded workload JWTs locally and reloads the private key on refresh."""

    def __init__(
        self,
        *,
        private_key_file: str | Path,
        key_id: str,
        algorithm: str,
        issuer: str,
        audience: str,
        subject: str,
        environment: str,
        roles: tuple[str, ...],
        venues: tuple[str, ...] = (),
        consumer_manifest_revision: int = 1,
        lifetime_seconds: int = 600,
        refresh_before_seconds: int = 120,
        clock=time.time,
    ) -> None:
        self.private_key_file = Path(private_key_file).expanduser().resolve()
        self.key_id = key_id.strip()
        self.algorithm = algorithm.strip().upper()
        self.issuer = issuer.strip()
        self.audience = audience.strip()
        self.subject = subject.strip()
        self.environment = environment.strip().lower()
        self.roles = tuple(sorted(set(roles)))
        self.venues = tuple(sorted({value.upper() for value in venues}))
        self.consumer_manifest_revision = int(consumer_manifest_revision)
        self.lifetime_seconds = int(lifetime_seconds)
        self.refresh_before_seconds = int(refresh_before_seconds)
        self._clock = clock
        self._token = ""
        self._expires_at = 0
        self._lock = asyncio.Lock()
        if (
            not self.private_key_file.is_file()
            or not all((
                self.key_id, self.issuer, self.audience,
                self.subject, self.environment,
            ))
            or self.algorithm not in {"RS256", "ES256"}
            or not self.roles
            or self.consumer_manifest_revision < 1
            or not 60 <= self.lifetime_seconds <= 900
            or not 5 <= self.refresh_before_seconds < self.lifetime_seconds
        ):
            raise ValueError("rotating workload JWT configuration is invalid")

    async def get_token(self) -> str:
        now = int(self._clock())
        if self._token and self._expires_at - now > self.refresh_before_seconds:
            return self._token
        async with self._lock:
            now = int(self._clock())
            if self._token and self._expires_at - now > self.refresh_before_seconds:
                return self._token
            expires_at = now + self.lifetime_seconds
            claims = {
                "sub": self.subject,
                "iss": self.issuer,
                "aud": self.audience,
                "iat": now,
                "exp": expires_at,
                "jti": str(uuid.uuid4()),
                "environment": self.environment,
                "roles": list(self.roles),
                "venues": list(self.venues),
                "consumer_manifest_revision": self.consumer_manifest_revision,
            }
            self._token = jwt.encode(
                claims,
                self.private_key_file.read_bytes(),
                algorithm=self.algorithm,
                headers={"kid": self.key_id},
            )
            self._expires_at = expires_at
            return self._token
