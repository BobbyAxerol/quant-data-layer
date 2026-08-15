from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class SourceRole(StrEnum):
    PRIMARY = "PRIMARY"
    SECONDARY = "SECONDARY"
    REFERENCE = "REFERENCE"
    BACKFILL = "BACKFILL"


class SourceAuthorityState(StrEnum):
    PRIMARY_HEALTHY = "PRIMARY_HEALTHY"
    PRIMARY_DEGRADED = "PRIMARY_DEGRADED"
    FAILOVER_PENDING = "FAILOVER_PENDING"
    SECONDARY_VALIDATING = "SECONDARY_VALIDATING"
    SECONDARY_ACTIVE = "SECONDARY_ACTIVE"
    PRIMARY_RECOVERING = "PRIMARY_RECOVERING"
    PRIMARY_SHADOW = "PRIMARY_SHADOW"
    PRIMARY_ACTIVE = "PRIMARY_ACTIVE"


@dataclass(frozen=True)
class SourceCandidate:
    source_id: str
    venue: str
    role: SourceRole
    fresh: bool
    quality_valid: bool


@dataclass(frozen=True)
class AuthorityAction:
    state: SourceAuthorityState
    selected_source_id: str | None
    execution_eligible: bool
    reason: str
    switched: bool


class SourceAuthorityController:
    """Explicit failover state with cooldown; cross-venue reference never impersonates primary."""

    def __init__(
        self,
        *,
        primary_source_id: str,
        primary_venue: str,
        secondary_source_id: str | None = None,
        secondary_venue: str | None = None,
        failover_delay_ns: int = 5_000_000_000,
        recovery_delay_ns: int = 10_000_000_000,
    ) -> None:
        if not primary_source_id.strip() or not primary_venue.strip():
            raise ValueError("primary source identity is required")
        if (secondary_source_id is None) != (secondary_venue is None):
            raise ValueError("secondary source and venue must be configured together")
        if min(failover_delay_ns, recovery_delay_ns) < 0:
            raise ValueError("authority delays must be non-negative")
        self.primary_source_id = primary_source_id
        self.primary_venue = primary_venue.upper()
        self.secondary_source_id = secondary_source_id
        self.secondary_venue = secondary_venue.upper() if secondary_venue else None
        self.failover_delay_ns = failover_delay_ns
        self.recovery_delay_ns = recovery_delay_ns
        self.state = SourceAuthorityState.PRIMARY_HEALTHY
        self.selected_source_id: str | None = primary_source_id
        self._state_since_ns = 0

    def evaluate(
        self,
        *,
        primary: SourceCandidate,
        secondary: SourceCandidate | None,
        now_ns: int,
        allow_secondary_execution: bool,
    ) -> AuthorityAction:
        self._validate_candidates(primary, secondary)
        primary_ok = primary.fresh and primary.quality_valid
        secondary_ok = bool(secondary and secondary.fresh and secondary.quality_valid)
        previous = self.selected_source_id

        if primary_ok:
            if self.selected_source_id == self.secondary_source_id:
                if self.state is not SourceAuthorityState.PRIMARY_RECOVERING:
                    self._transition(SourceAuthorityState.PRIMARY_RECOVERING, now_ns)
                elif now_ns - self._state_since_ns >= self.recovery_delay_ns:
                    self.selected_source_id = self.primary_source_id
                    self._transition(SourceAuthorityState.PRIMARY_ACTIVE, now_ns)
            else:
                self.selected_source_id = self.primary_source_id
                self._transition(SourceAuthorityState.PRIMARY_HEALTHY, now_ns)
        else:
            if self.state in {SourceAuthorityState.PRIMARY_HEALTHY, SourceAuthorityState.PRIMARY_ACTIVE}:
                self._transition(SourceAuthorityState.PRIMARY_DEGRADED, now_ns)
            elif self.state is SourceAuthorityState.PRIMARY_DEGRADED:
                self._transition(SourceAuthorityState.FAILOVER_PENDING, now_ns)
            elif (
                self.state is SourceAuthorityState.FAILOVER_PENDING
                and now_ns - self._state_since_ns >= self.failover_delay_ns
            ):
                self._transition(SourceAuthorityState.SECONDARY_VALIDATING, now_ns)
            elif self.state is SourceAuthorityState.SECONDARY_VALIDATING and secondary_ok:
                if secondary and secondary.role is SourceRole.SECONDARY and allow_secondary_execution:
                    self.selected_source_id = secondary.source_id
                    self._transition(SourceAuthorityState.SECONDARY_ACTIVE, now_ns)

        selected_is_primary = self.selected_source_id == self.primary_source_id and primary_ok
        selected_is_secondary = (
            secondary_ok
            and secondary is not None
            and self.selected_source_id == secondary.source_id
            and secondary.role is SourceRole.SECONDARY
            and allow_secondary_execution
        )
        execution_eligible = bool(selected_is_primary or selected_is_secondary)
        if not execution_eligible and not primary_ok:
            self.selected_source_id = None
        return AuthorityAction(
            state=self.state,
            selected_source_id=self.selected_source_id,
            execution_eligible=execution_eligible,
            reason=("PRIMARY_SELECTED" if selected_is_primary else
                    "SECONDARY_SELECTED" if selected_is_secondary else
                    "NO_EXECUTION_ELIGIBLE_SOURCE"),
            switched=previous != self.selected_source_id,
        )

    def _transition(self, state: SourceAuthorityState, now_ns: int) -> None:
        if self.state is not state:
            self.state = state
            self._state_since_ns = now_ns

    def _validate_candidates(
        self, primary: SourceCandidate, secondary: SourceCandidate | None
    ) -> None:
        if primary.source_id != self.primary_source_id or primary.venue.upper() != self.primary_venue:
            raise ValueError("primary candidate identity mismatch")
        if primary.role is not SourceRole.PRIMARY:
            raise ValueError("primary candidate must have PRIMARY role")
        if secondary is None:
            return
        if secondary.source_id != self.secondary_source_id:
            raise ValueError("secondary candidate identity mismatch")
        if secondary.venue.upper() != self.secondary_venue:
            raise ValueError("secondary venue mismatch")
        if secondary.role not in {SourceRole.SECONDARY, SourceRole.REFERENCE}:
            raise ValueError("fallback candidate must be SECONDARY or REFERENCE")
