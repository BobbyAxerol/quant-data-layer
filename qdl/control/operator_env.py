"""Fail-closed operator environment resolution for authority tools."""

from __future__ import annotations

from collections.abc import Mapping
import os


def require_control_admin_dsn(
    environ: Mapping[str, str] | None = None,
) -> str:
    """Return the one accepted control-plane admin DSN without exposing it."""

    values = os.environ if environ is None else environ
    runtime = str(values.get("QDL_CONTROL_ADMIN_DSN", "")).strip()
    stable = str(values.get("QDL_STABLE_CONTROL_ADMIN_DSN", "")).strip()
    if runtime and stable and runtime != stable:
        raise RuntimeError("control admin DSN aliases disagree")
    value = runtime or stable
    if not value:
        raise RuntimeError(
            "QDL_CONTROL_ADMIN_DSN or QDL_STABLE_CONTROL_ADMIN_DSN is required"
        )
    return value
