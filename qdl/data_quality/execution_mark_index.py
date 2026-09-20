"""Pure validation for the explicit execution MARK/INDEX quiet contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


_VALID_RECENCY_MODES = frozenset({
    "STRICT_EVENT_SESSION_LIVE",
    "COMPONENT_SESSION_LIVE",
})
_MIN_COMPONENT_CADENCE_MS = 250
_MAX_COMPONENT_CADENCE_MS = 120_000


@dataclass(frozen=True, slots=True)
class QuietExecutionMarkIndexEvidence:
    """Bounded session/component ages derived from immutable response labels."""

    session_liveness_ms: int
    session_checked_age_ms: int
    component_mark_age_ms: int
    component_index_age_ms: int
    component_mark_quiet_after_ms: int
    component_index_quiet_after_ms: int


def validate_quiet_execution_mark_index_evidence(
    labels: Mapping[str, str],
    *,
    at_ns: int,
    max_session_liveness_ms: int | None,
) -> QuietExecutionMarkIndexEvidence:
    """Validate the signed live-view proof without rewriting event lineage.

    Only an execution MARK/INDEX request already selected by its caller reaches
    this helper. It therefore validates the current session and paired-component
    cadence that make an old, unchanged source event observable. Missing or
    malformed evidence always fails closed.
    """

    if max_session_liveness_ms is None or max_session_liveness_ms < 1:
        raise ValueError("quiet execution MARK/INDEX contract is incomplete")
    if (
        labels.get("event_recency_policy") != "OBSERVE"
        or labels.get("recency_mode") not in _VALID_RECENCY_MODES
        or labels.get("provider_session_state") != "LIVE"
    ):
        raise ValueError("quiet execution MARK/INDEX session evidence is not live")
    try:
        session_liveness_ms = int(labels["provider_session_liveness_ms"])
        session_checked_at_ns = int(labels["provider_session_checked_at_ns"])
        component_values = tuple(
            (
                name,
                int(labels[f"component_{name.lower()}_received_at_ns"]),
                int(labels[f"component_{name.lower()}_quiet_after_ms"]),
            )
            for name in ("MARK", "INDEX")
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("quiet execution MARK/INDEX evidence is malformed") from error
    if (
        session_liveness_ms < 0
        or session_checked_at_ns <= 0
        or session_checked_at_ns > at_ns
    ):
        raise ValueError("quiet execution MARK/INDEX session clock is invalid")
    session_checked_age_ms = (at_ns - session_checked_at_ns) // 1_000_000
    if session_liveness_ms + session_checked_age_ms > max_session_liveness_ms:
        raise ValueError("quiet execution MARK/INDEX provider session exceeded its SLA")

    component_ages: dict[str, tuple[int, int]] = {}
    for name, receipt_ns, cadence_ms in component_values:
        if (
            receipt_ns <= 0
            or not _MIN_COMPONENT_CADENCE_MS <= cadence_ms <= _MAX_COMPONENT_CADENCE_MS
            or receipt_ns > at_ns
        ):
            raise ValueError("quiet execution MARK/INDEX component evidence is invalid")
        age_ms = (at_ns - receipt_ns) // 1_000_000
        if age_ms > cadence_ms:
            raise ValueError("quiet execution MARK/INDEX component exceeded its cadence")
        component_ages[name] = (age_ms, cadence_ms)
    return QuietExecutionMarkIndexEvidence(
        session_liveness_ms=session_liveness_ms,
        session_checked_age_ms=int(session_checked_age_ms),
        component_mark_age_ms=component_ages["MARK"][0],
        component_index_age_ms=component_ages["INDEX"][0],
        component_mark_quiet_after_ms=component_ages["MARK"][1],
        component_index_quiet_after_ms=component_ages["INDEX"][1],
    )
