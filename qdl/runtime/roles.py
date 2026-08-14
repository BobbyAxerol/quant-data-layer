from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from os import environ
from typing import Mapping


class RuntimeRole(str, Enum):
    API = "api"
    CONTROL = "control"
    HISTORY = "history"
    QUERY_V2 = "query_v2"
    STREAM_V2 = "stream_v2"
    COMPAT_COMBINED = "compat_combined"


_ROLE_OWNERSHIP = {
    RuntimeRole.API: frozenset({"query_api"}),
    RuntimeRole.CONTROL: frozenset({"control_api"}),
    RuntimeRole.HISTORY: frozenset({"history_api"}),
    RuntimeRole.QUERY_V2: frozenset({"query_v2", "beta_read_only"}),
    RuntimeRole.STREAM_V2: frozenset({"stream_v2", "beta_read_only", "leased_gateway"}),
    RuntimeRole.COMPAT_COMBINED: frozenset(
        {"query_api", "control_api", "history_api", "live_ingestion", "legacy_projection"}
    ),
}
_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})


def _optional_bool(values: Mapping[str, str], name: str) -> bool | None:
    value = values.get(name)
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in _TRUE:
        return True
    if normalized in _FALSE:
        return False
    raise ValueError(f"{name} must be one of true/false, 1/0, yes/no, on/off")


@dataclass(frozen=True)
class RuntimeRoleConfig:
    role: RuntimeRole
    owned_capabilities: frozenset[str]
    config_revision: str

    @classmethod
    def for_entrypoint(
        cls,
        expected_role: RuntimeRole,
        values: Mapping[str, str] | None = None,
    ) -> "RuntimeRoleConfig":
        env = environ if values is None else values
        configured_role = RuntimeRole(env.get("QDL_RUNTIME_ROLE", expected_role.value).strip().lower())
        if configured_role is not expected_role:
            raise ValueError(
                f"entrypoint role is {expected_role.value}, but QDL_RUNTIME_ROLE={configured_role.value}"
            )
        expected_ingestion = "live_ingestion" in _ROLE_OWNERSHIP[expected_role]
        ingestion_override = _optional_bool(env, "QDL_OWNS_LIVE_INGESTION")
        if ingestion_override is not None and ingestion_override != expected_ingestion:
            raise ValueError(
                f"QDL_OWNS_LIVE_INGESTION={str(ingestion_override).lower()} contradicts "
                f"role={expected_role.value}"
            )
        return cls(
            role=expected_role,
            owned_capabilities=_ROLE_OWNERSHIP[expected_role],
            config_revision=env.get("QDL_CONFIG_REVISION", "phase1-dark-0").strip(),
        )

    @property
    def owns_live_ingestion(self) -> bool:
        return "live_ingestion" in self.owned_capabilities

    def manifest(self) -> dict[str, object]:
        if self.role is RuntimeRole.COMPAT_COMBINED:
            authority = "v1_authoritative"
        elif self.role in {RuntimeRole.QUERY_V2, RuntimeRole.STREAM_V2}:
            authority = "v1_shadow_read_only"
        else:
            authority = "phase1_dark"
        return {
            "role": self.role.value,
            "owned_capabilities": sorted(self.owned_capabilities),
            "owns_live_ingestion": self.owns_live_ingestion,
            "config_revision": self.config_revision,
            "authority": authority,
        }
