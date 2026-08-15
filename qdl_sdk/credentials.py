from __future__ import annotations

import inspect
from dataclasses import dataclass
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
