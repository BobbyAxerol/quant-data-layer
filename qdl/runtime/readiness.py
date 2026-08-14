from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable, Protocol


class ComponentState(str, Enum):
    READY = "READY"
    DEGRADED = "DEGRADED"
    NOT_READY = "NOT_READY"
    STANDBY = "STANDBY"


@dataclass(frozen=True, slots=True)
class ComponentReadiness:
    name: str
    state: ComponentState
    required: bool = True
    detail: str = ""
    revision: str | None = None
    lag_ms: int | None = None
    freshness_ms: int | None = None
    checked_at_ns: int = 0

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("readiness component name is required")
        if self.lag_ms is not None and self.lag_ms < 0:
            raise ValueError("readiness lag cannot be negative")
        if self.freshness_ms is not None and self.freshness_ms < 0:
            raise ValueError("readiness freshness cannot be negative")


class ReadinessProbe(Protocol):
    name: str
    required: bool

    async def check(self) -> ComponentReadiness: ...


class CallableReadinessProbe:
    def __init__(
        self,
        name: str,
        check: Callable[[], ComponentReadiness | Awaitable[ComponentReadiness]],
        *,
        required: bool = True,
    ) -> None:
        if not name.strip():
            raise ValueError("readiness probe name is required")
        self.name = name
        self.required = required
        self._check = check

    async def check(self) -> ComponentReadiness:
        result = self._check()
        if inspect.isawaitable(result):
            result = await result
        if result.name != self.name or result.required != self.required:
            raise ValueError("readiness probe returned inconsistent identity")
        return result


@dataclass(frozen=True, slots=True)
class RuntimeReadinessSnapshot:
    role: str
    status: str
    ready: bool
    authority: str
    config_revision: str
    checked_at_ns: int
    components: tuple[ComponentReadiness, ...]

    def public_summary(self) -> dict[str, str]:
        return {
            "schema": "qdl.system-readiness.v2",
            "status": self.status,
            "authority": self.authority,
            "v2_consumer_activation": "MANIFEST_CONTROLLED",
        }


class MeasuredRuntimeReadiness:
    """Role readiness derived from bounded dependency probes, never a phase note."""

    def __init__(
        self,
        *,
        role: str,
        authority: str,
        config_revision: str,
        probes: tuple[ReadinessProbe, ...],
        timeout_seconds: float = 1.0,
        clock_ns=time.time_ns,
    ) -> None:
        if not role.strip() or not authority.strip() or not config_revision.strip():
            raise ValueError("readiness role, authority and config revision are required")
        if not probes or timeout_seconds <= 0:
            raise ValueError("readiness requires probes and a positive timeout")
        names = [probe.name for probe in probes]
        if len(names) != len(set(names)):
            raise ValueError("readiness probe names must be unique")
        self.role = role
        self.authority = authority
        self.config_revision = config_revision
        self.probes = probes
        self.timeout_seconds = timeout_seconds
        self._clock_ns = clock_ns

    async def _check(self, probe: ReadinessProbe) -> ComponentReadiness:
        try:
            return await asyncio.wait_for(probe.check(), timeout=self.timeout_seconds)
        except TimeoutError:
            detail = "dependency probe deadline exceeded"
        except Exception as error:
            detail = f"{type(error).__name__}: {error}"
        return ComponentReadiness(
            probe.name,
            ComponentState.NOT_READY,
            required=probe.required,
            detail=detail,
            checked_at_ns=self._clock_ns(),
        )

    async def snapshot(self) -> RuntimeReadinessSnapshot:
        components = tuple(await asyncio.gather(*(
            self._check(probe) for probe in self.probes
        )))
        blocking = tuple(
            item
            for item in components
            if item.required and item.state is not ComponentState.READY
        )
        degraded = any(
            item.state is ComponentState.DEGRADED for item in components
        )
        standby = any(
            item.required and item.state is ComponentState.STANDBY
            for item in components
        )
        if blocking:
            status = "STANDBY" if standby and all(
                item.state is ComponentState.STANDBY for item in blocking
            ) else "NOT_READY"
        else:
            status = "DEGRADED" if degraded else "READY"
        return RuntimeReadinessSnapshot(
            role=self.role,
            status=status,
            ready=not blocking,
            authority=self.authority,
            config_revision=self.config_revision,
            checked_at_ns=self._clock_ns(),
            components=components,
        )

    async def public_summary(self) -> dict[str, str]:
        return (await self.snapshot()).public_summary()


class FailClosedReadiness:
    async def public_summary(self) -> dict[str, str]:
        return {
            "schema": "qdl.system-readiness.v2",
            "status": "NOT_READY",
            "authority": "V1",
            "v2_consumer_activation": "MANIFEST_CONTROLLED",
        }
