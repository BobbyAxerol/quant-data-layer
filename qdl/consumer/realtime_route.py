"""Typed, per-requirement V2 primary and V1 fallback decisions.

This module is deliberately pure: deployment changes a governed consumer
migration manifest, while this evaluator decides whether one requested slice
is safe to serve from the selected data plane at that instant. It never makes
an endpoint, provider, or broker call.
"""

from __future__ import annotations

from dataclasses import dataclass
from qdl._compat import StrEnum

from qdl.consumer.manifest import ConsumerManifest, ConsumerRoute
from qdl.query import (
    CanonicalErrorCode,
    CoverageStatus,
    DataRequirement,
    QueryProblem,
    evaluate_requirement,
)


class RealtimeRoute(StrEnum):
    V1_PRIMARY = "V1_PRIMARY"
    V2_PRIMARY = "V2_PRIMARY"
    V1_FALLBACK = "V1_FALLBACK"
    BLOCKED = "BLOCKED"


_FALLBACK_ELIGIBLE = frozenset({
    CanonicalErrorCode.DATA_NOT_READY,
    CanonicalErrorCode.DATA_STALE,
    CanonicalErrorCode.SOURCE_UNAVAILABLE,
    CanonicalErrorCode.DEPENDENCY_UNAVAILABLE,
    CanonicalErrorCode.OPEN_SEQUENCE_GAP,
    CanonicalErrorCode.PARTIAL_RESULT,
})


@dataclass(frozen=True, slots=True)
class RealtimeSliceObservation:
    """Bounded health facts for one data-plane source and requirement."""

    available: bool
    authoritative: bool
    coverage: CoverageStatus
    source_age_ms: int | None
    receive_age_ms: int | None
    gap_open: bool
    final_bar_available: bool = True
    market_closed: bool = False

    def __post_init__(self) -> None:
        if self.source_age_ms is not None and self.source_age_ms < 0:
            raise ValueError("source_age_ms must be non-negative")
        if self.receive_age_ms is not None and self.receive_age_ms < 0:
            raise ValueError("receive_age_ms must be non-negative")
        if self.market_closed and self.available:
            raise ValueError("market-closed observation cannot claim live availability")


@dataclass(frozen=True, slots=True)
class RealtimeSliceHealth:
    available: bool
    market_closed: bool
    source_age_ms: int | None
    receive_age_ms: int | None
    gap_open: bool
    problem: QueryProblem | None

    @property
    def ready(self) -> bool:
        return not self.market_closed and self.problem is None


@dataclass(frozen=True, slots=True)
class RealtimeRouteDecision:
    consumer_id: str
    requirement_key: str
    route: RealtimeRoute
    reason: str
    v2: RealtimeSliceHealth
    v1: RealtimeSliceHealth | None

    @property
    def fallback_used(self) -> bool:
        return self.route is RealtimeRoute.V1_FALLBACK

    def audit_record(self) -> dict[str, object]:
        """Return bounded observability only; raw provider data is excluded."""
        return {
            "consumer_id": self.consumer_id,
            "requirement_key": self.requirement_key,
            "route": self.route.value,
            "reason": self.reason,
            "fallback_used": self.fallback_used,
            "v2_problem": self.v2.problem.code.value if self.v2.problem else None,
            "v2_source_age_ms": self.v2.source_age_ms,
            "v2_receive_age_ms": self.v2.receive_age_ms,
            "v2_gap_open": self.v2.gap_open,
            "v1_problem": self.v1.problem.code.value if self.v1 and self.v1.problem else None,
            "v1_source_age_ms": self.v1.source_age_ms if self.v1 else None,
            "v1_receive_age_ms": self.v1.receive_age_ms if self.v1 else None,
        }


def requirement_key(requirement: DataRequirement) -> str:
    return ":".join((
        requirement.instrument_uid,
        requirement.feed.value,
        requirement.interval or "",
        requirement.source_policy_id,
    ))


def evaluate_realtime_health(
    requirement: DataRequirement,
    observation: RealtimeSliceObservation,
) -> RealtimeSliceHealth:
    if observation.market_closed:
        return RealtimeSliceHealth(
            available=False,
            market_closed=True,
            source_age_ms=observation.source_age_ms,
            receive_age_ms=observation.receive_age_ms,
            gap_open=observation.gap_open,
            problem=QueryProblem(
                CanonicalErrorCode.DATA_NOT_READY,
                "market session is closed for this demanded slice",
                True,
            ),
        )
    fresh = observation.source_age_ms is not None and (
        requirement.max_freshness_ms is None
        or observation.source_age_ms <= requirement.max_freshness_ms
    )
    available = observation.available and (
        not requirement.require_final_bars or observation.final_bar_available
    )
    problem = evaluate_requirement(
        requirement,
        coverage=observation.coverage,
        entitled=True,
        available=available,
        fresh=fresh,
        authoritative=observation.authoritative,
        gap_open=observation.gap_open,
    )
    return RealtimeSliceHealth(
        available=available,
        market_closed=False,
        source_age_ms=observation.source_age_ms,
        receive_age_ms=observation.receive_age_ms,
        gap_open=observation.gap_open,
        problem=problem,
    )


def select_realtime_route(
    *,
    manifest: ConsumerManifest,
    migration_route: ConsumerRoute,
    requirement: DataRequirement,
    v2_observation: RealtimeSliceObservation,
    v1_observation: RealtimeSliceObservation | None,
    v1_compatible: bool,
) -> RealtimeRouteDecision:
    """Select a route without silently widening compatibility or freshness.

    A V1 fallback is legal only for an ACTIVE V2 consumer that declared `V1`
    rollback, received an eligible V2 operational failure, has a compatible
    V1 binding, and sees V1 pass the same requirement health gate.
    """
    if not manifest.requirement_allowed(requirement):
        raise PermissionError("requested realtime requirement is outside manifest scope")
    v2 = evaluate_realtime_health(requirement, v2_observation)
    v1 = (
        evaluate_realtime_health(requirement, v1_observation)
        if v1_observation is not None
        else None
    )
    key = requirement_key(requirement)

    if migration_route in {ConsumerRoute.V1, ConsumerRoute.V1_WITH_V2_SHADOW}:
        if v1 is None or not v1.ready:
            return RealtimeRouteDecision(
                manifest.consumer_id,
                key,
                RealtimeRoute.BLOCKED,
                "V1_PRIMARY_NOT_READY",
                v2,
                v1,
            )
        return RealtimeRouteDecision(
            manifest.consumer_id,
            key,
            RealtimeRoute.V1_PRIMARY,
            "MANIFEST_ROUTE_V1",
            v2,
            v1,
        )

    if v2.ready:
        return RealtimeRouteDecision(
            manifest.consumer_id,
            key,
            RealtimeRoute.V2_PRIMARY,
            "V2_READY",
            v2,
            v1,
        )
    if v2.market_closed:
        return RealtimeRouteDecision(
            manifest.consumer_id,
            key,
            RealtimeRoute.BLOCKED,
            "MARKET_CLOSED",
            v2,
            v1,
        )
    code = v2.problem.code if v2.problem else CanonicalErrorCode.INTERNAL_ERROR
    if code not in _FALLBACK_ELIGIBLE:
        return RealtimeRouteDecision(
            manifest.consumer_id,
            key,
            RealtimeRoute.BLOCKED,
            f"V2_{code.value}_NOT_FALLBACK_ELIGIBLE",
            v2,
            v1,
        )
    if manifest.rollback_contract != "V1":
        return RealtimeRouteDecision(
            manifest.consumer_id,
            key,
            RealtimeRoute.BLOCKED,
            "V1_ROLLBACK_NOT_DECLARED",
            v2,
            v1,
        )
    if not v1_compatible:
        return RealtimeRouteDecision(
            manifest.consumer_id,
            key,
            RealtimeRoute.BLOCKED,
            "V1_COMPATIBILITY_NOT_APPROVED",
            v2,
            v1,
        )
    if v1 is None or not v1.ready:
        return RealtimeRouteDecision(
            manifest.consumer_id,
            key,
            RealtimeRoute.BLOCKED,
            "V1_FALLBACK_NOT_READY",
            v2,
            v1,
        )
    return RealtimeRouteDecision(
        manifest.consumer_id,
        key,
        RealtimeRoute.V1_FALLBACK,
        f"V2_{code.value}",
        v2,
        v1,
    )
