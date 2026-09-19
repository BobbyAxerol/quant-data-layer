"""One bounded, provider-neutral quality decision for one declared binding.

The canonical event is immutable evidence; its age is not a synonym for a
provider session being alive.  This module makes that distinction explicit so
the stable query edge and offline audit cannot accidentally apply different
rules to the same binding.  Rust carries the same pure decision through the
shared golden corpus in ``contracts/golden/quality``.
"""

from __future__ import annotations

from dataclasses import dataclass

from qdl._compat import StrEnum


class FeedSemantics(StrEnum):
    STRICT_EVENT = "STRICT_EVENT"
    QUIET_SESSION = "QUIET_SESSION"
    FINAL_SCHEDULED = "FINAL_SCHEDULED"


class AvailabilityClass(StrEnum):
    ACTIVE = "ACTIVE"
    EXPECTED_V1_PRIMARY = "EXPECTED_V1_PRIMARY"
    EXPECTED_DARK = "EXPECTED_DARK"
    OUT_OF_SESSION = "OUT_OF_SESSION"


@dataclass(frozen=True, slots=True)
class ComponentEvidence:
    """Receipt age and explicit quiet cadence for one paired component."""

    name: str
    receipt_age_ms: int
    quiet_after_ms: int

    def __post_init__(self) -> None:
        if (
            not self.name.strip()
            or self.receipt_age_ms < 0
            or self.quiet_after_ms < 1
        ):
            raise ValueError("component quality evidence is invalid")


@dataclass(frozen=True, slots=True)
class BindingQualityInput:
    """Typed facts used to judge a single binding without I/O or mutation."""

    binding_id: str
    instrument_uid: str
    feed: str
    source_role: str
    authoritative: bool
    acquisition_enabled: bool
    acquisition_mode: str
    market_open: bool
    event_present: bool
    event_age_ms: int | None
    event_limit_ms: int
    event_recency_policy: str
    session_state: str
    session_liveness_ms: int | None
    session_limit_ms: int | None
    # Provider delivery behavior is a source contract, not a caller choice.
    # Native BBO can legitimately be quiet while the best bid/offer is
    # unchanged; every other lane remains strict by default.
    delivery_semantics: str = "STRICT_EVENT"
    components: tuple[ComponentEvidence, ...] = ()
    generation_matches: bool = True
    config_matches: bool = True
    gap_open: bool = False
    book_verified: bool = True
    final_bar: bool = True
    require_final_bar: bool = False
    watermark_offset: int = 0
    allow_quiet_execution: bool = False
    flags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (
            not self.binding_id.strip()
            or not self.instrument_uid.strip()
            or not self.feed.strip()
            or self.source_role not in {"PRIMARY", "SECONDARY", "REFERENCE", "BACKFILL"}
            or not self.acquisition_mode.strip()
            or self.event_limit_ms < 1
            or self.event_recency_policy not in {"BLOCK", "PAUSE", "OBSERVE"}
            or self.delivery_semantics not in {"STRICT_EVENT", "ON_CHANGE"}
            or self.session_state not in {
                "LIVE", "STALE", "DISCONNECTED", "UNKNOWN", "NOT_APPLICABLE"
            }
            or self.watermark_offset < 0
        ):
            raise ValueError("binding quality input is invalid")
        if self.event_present != (self.event_age_ms is not None):
            raise ValueError("event presence and age must agree")
        if self.event_age_ms is not None and self.event_age_ms < 0:
            raise ValueError("event age cannot be negative")
        if self.session_liveness_ms is not None and self.session_liveness_ms < 0:
            raise ValueError("session liveness cannot be negative")
        if self.session_limit_ms is not None and self.session_limit_ms < 1:
            raise ValueError("session limit must be positive")


@dataclass(frozen=True, slots=True)
class BindingQualityDecision:
    """Serializable policy answer derived only from :class:`BindingQualityInput`."""

    binding_id: str
    instrument_uid: str
    feed: str
    semantics: FeedSemantics
    delivery_semantics: str
    availability: AvailabilityClass
    state: str
    event_recency_state: str
    provider_session_state: str
    provider_session_liveness_ms: int | None
    complete: bool
    execution_eligible: bool
    watermark_offset: int
    reason_codes: tuple[str, ...]

    def as_mapping(self) -> dict[str, object]:
        """Stable bounded audit shape; it contains no raw provider payload."""

        return {
            "binding_id": self.binding_id,
            "instrument_uid": self.instrument_uid,
            "feed": self.feed,
            "semantics": self.semantics.value,
            "delivery_semantics": self.delivery_semantics,
            "availability": self.availability.value,
            "state": self.state,
            "event_recency_state": self.event_recency_state,
            "provider_session_state": self.provider_session_state,
            "provider_session_liveness_ms": self.provider_session_liveness_ms,
            "complete": self.complete,
            "execution_eligible": self.execution_eligible,
            "watermark_offset": self.watermark_offset,
            "reason_codes": list(self.reason_codes),
        }


def semantics_for(
    *,
    feed: str,
    event_recency_policy: str,
    require_final_bar: bool,
    delivery_semantics: str = "STRICT_EVENT",
) -> FeedSemantics:
    normalized_feed = feed.upper()
    if normalized_feed == "BAR" or require_final_bar:
        return FeedSemantics.FINAL_SCHEDULED
    if (
        event_recency_policy == "OBSERVE"
        and (
            normalized_feed in {"TRADE", "BOOK_DELTA", "MARK_INDEX_PRICE"}
            or (
                normalized_feed == "QUOTE"
                and delivery_semantics == "ON_CHANGE"
            )
        )
    ):
        return FeedSemantics.QUIET_SESSION
    return FeedSemantics.STRICT_EVENT


def availability_for(value: BindingQualityInput) -> AvailabilityClass:
    """Classify declared non-serving inventory before looking at event age."""

    # VN uses the vendor-edge / V1 compatibility train until its independent
    # market-hours certificate.  It is intentionally not a broken V2 feed.
    if value.acquisition_mode == "PYTHON_VENDOR_SDK":
        return AvailabilityClass.EXPECTED_V1_PRIMARY
    if not value.acquisition_enabled:
        return AvailabilityClass.EXPECTED_DARK
    if not value.market_open:
        return AvailabilityClass.OUT_OF_SESSION
    return AvailabilityClass.ACTIVE


def _append_once(target: list[str], *values: str) -> None:
    for value in values:
        if value and value not in target:
            target.append(value)


def evaluate_binding_quality(value: BindingQualityInput) -> BindingQualityDecision:
    """Evaluate strict, quiet and scheduled feeds without weakening policy.

    A quiet channel is only *observable* after all of its session/generation/
    cadence fences pass.  ``allow_quiet_execution`` is deliberately explicit:
    the durable generic query path cannot make a quiet price executable merely
    because its session is alive.
    """

    semantics = semantics_for(
        feed=value.feed,
        event_recency_policy=value.event_recency_policy,
        require_final_bar=value.require_final_bar,
        delivery_semantics=value.delivery_semantics,
    )
    availability = availability_for(value)
    reasons = list(value.flags)
    event_state = "NOT_APPLICABLE"
    if value.event_present:
        assert value.event_age_ms is not None
        event_state = "STALE" if value.event_age_ms > value.event_limit_ms else "LIVE"
        if event_state == "STALE":
            _append_once(reasons, "LAST_EVENT_STALE")
    session_state = value.session_state
    session_ok = session_state == "NOT_APPLICABLE"
    if value.session_limit_ms is not None:
        session_ok = (
            session_state == "LIVE"
            and value.session_liveness_ms is not None
            and value.session_liveness_ms <= value.session_limit_ms
        )
    elif session_state in {"STALE", "DISCONNECTED", "UNKNOWN"}:
        session_ok = False

    component_ok = True
    for component in value.components:
        if component.receipt_age_ms > component.quiet_after_ms:
            component_ok = False
            _append_once(reasons, f"COMPONENT_{component.name.upper()}_STALE")
    if not value.generation_matches:
        _append_once(reasons, "GENERATION_MISMATCH")
    if not value.config_matches:
        _append_once(reasons, "CONFIG_REVISION_MISMATCH")
    if value.gap_open:
        _append_once(reasons, "OPEN_SEQUENCE_GAP")
    if not value.book_verified:
        _append_once(reasons, "BOOK_SEQUENCE_UNVERIFIED")
    if value.require_final_bar and not value.final_bar:
        _append_once(reasons, "BAR_NOT_FINAL")
    if not session_ok:
        if session_state in {"STALE", "DISCONNECTED", "UNKNOWN"}:
            _append_once(reasons, f"SOURCE_SESSION_{session_state}")
        else:
            _append_once(reasons, "SOURCE_SESSION_HEARTBEAT_EXPIRED")

    if availability is AvailabilityClass.EXPECTED_V1_PRIMARY:
        _append_once(reasons, "EXPECTED_V1_PRIMARY")
        state = "DISABLED"
    elif availability is AvailabilityClass.EXPECTED_DARK:
        _append_once(reasons, "EXPECTED_DARK")
        state = "DISABLED"
    elif availability is AvailabilityClass.OUT_OF_SESSION:
        _append_once(reasons, "OUT_OF_SESSION")
        state = "MARKET_CLOSED"
    elif not value.event_present:
        _append_once(reasons, "NO_DURABLE_EVENT")
        state = "NOT_READY"
    elif value.gap_open:
        state = "GAPPED"
    elif not value.book_verified or (value.require_final_bar and not value.final_bar):
        state = "SYNCING"
    elif not value.generation_matches or not value.config_matches or not session_ok:
        state = "STALE"
    elif not component_ok:
        state = "STALE"
    elif semantics is not FeedSemantics.QUIET_SESSION and event_state == "STALE":
        state = "STALE"
    else:
        state = "LIVE"

    complete = (
        value.event_present
        and not value.gap_open
        and value.book_verified
        and (not value.require_final_bar or value.final_bar)
    )
    event_ok = event_state in {"LIVE", "NOT_APPLICABLE"}
    quiet_execution_ok = (
        semantics is FeedSemantics.QUIET_SESSION
        and value.allow_quiet_execution
        and session_ok
        and component_ok
    )
    execution_eligible = (
        availability is AvailabilityClass.ACTIVE
        and value.authoritative
        and value.source_role == "PRIMARY"
        and state == "LIVE"
        and complete
        and (event_ok or quiet_execution_ok)
    )
    return BindingQualityDecision(
        binding_id=value.binding_id,
        instrument_uid=value.instrument_uid,
        feed=value.feed.upper(),
        semantics=semantics,
        delivery_semantics=value.delivery_semantics,
        availability=availability,
        state=state,
        event_recency_state=event_state,
        provider_session_state=session_state,
        provider_session_liveness_ms=value.session_liveness_ms,
        complete=complete,
        execution_eligible=execution_eligible,
        watermark_offset=value.watermark_offset,
        reason_codes=tuple(reasons),
    )


def freshness_verdict(
    *,
    state: str,
    freshness_ms: int,
    event_recency_policy: str,
    max_freshness_ms: int | None,
    provider_session_state: str,
    provider_session_liveness_ms: int | None,
    max_session_liveness_ms: int | None,
) -> tuple[bool, str | None]:
    """Shared query-admission predicate for already-materialized quality."""

    if state == "MARKET_CLOSED":
        return True, None
    # Gap/completeness are evaluated by the caller's declared gap policy after
    # this freshness verdict. Collapsing them into DATA_STALE would lose the
    # actionable OPEN_SEQUENCE_GAP error contract.
    if state in {"STALE", "OFFLINE", "UNAVAILABLE"}:
        return False, "EVENT_AGE"
    if provider_session_state in {"STALE", "DISCONNECTED", "UNKNOWN"}:
        return False, "SESSION_STATE"
    if max_session_liveness_ms is not None and not (
        provider_session_state == "LIVE"
        and provider_session_liveness_ms is not None
        and provider_session_liveness_ms <= max_session_liveness_ms
    ):
        return False, "SESSION_LIVENESS"
    if (
        max_freshness_ms is not None
        and freshness_ms > max_freshness_ms
        and event_recency_policy in {"BLOCK", "PAUSE"}
    ):
        return False, "EVENT_AGE"
    return True, None
