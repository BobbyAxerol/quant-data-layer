"""Compact current-observation evidence for Phase 10.5-D/B3.

The B3 release certificate must distinguish a fresh V2 route from a declared
V1-only venue.  This module consumes the payload-free C2 receipt and produces
the exact full release-route observation set without opening a provider
connection or retaining market payloads/cursors.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Iterable, Mapping
from typing import Any

from qdl.consumer import (
    ReleaseRouteObservation,
    StableReleaseRoutePlan,
    requirement_key,
)
from qdl.reference.runtime import reference_requirement_eligible
from qdl.runtime.provider_history import pass_through_eligible
from qdl.runtime.stable_catalog import StableSourceCatalog


OBSERVATION_BUNDLE_SCHEMA = "qdl.phase105.release-observations.v1"
MAX_OBSERVATION_AGE_MS = 300_000
_QUALITY_FIELDS = frozenset({"source_age_ms", "receive_age_ms", "gap_open"})
_SESSION_QUALITY_FIELDS = frozenset({
    "state", "provider_session_state", "provider_session_liveness_ms",
    "complete", "execution_eligible",
})
_CAPTURE_FIELDS = frozenset({"captured_at_ms", "cpu_millicores", "rss_bytes"})
_BUNDLE_FIELDS = frozenset({
    "schema",
    "release_route_plan_sha256",
    "captured_at_ms",
    "acceptance_sha256",
    "observations",
})
_DURABLE_NO_CURSOR_HANDOFFS = frozenset({
    "SIGNED_CURSOR_REOPENED_NO_NEW_EVENT",
    "LIVE_EVENT_AFTER_REOPEN_NO_CURSOR",
    "LIVE_OBSERVED_NO_NEW_CURSOR",
    "QUIET_OBSERVED_NO_CURSOR",
    "MIXED_LIVE_QUIET_NO_NEW_CURSOR",
})


def expected_release_delivery(
    catalog: StableSourceCatalog,
    requirement: object,
) -> str:
    """Return the only release-evidence delivery class for one requirement.

    Reference batches are bounded provider reads.  They are deliberately not
    represented as durable stream/replay evidence just because they are routed
    through a V2-primary release manifest.
    """

    try:
        instrument = catalog.instrument_for(requirement.instrument_uid)  # type: ignore[attr-defined]
    except (AttributeError, KeyError) as error:
        raise ValueError("Phase 10.5 B3 requirement has no catalog instrument") from error
    if reference_requirement_eligible(instrument, requirement):  # type: ignore[arg-type]
        return "ON_DEMAND"
    if pass_through_eligible(catalog, requirement):  # type: ignore[arg-type]
        return "PROVIDER_PASS_THROUGH"
    return "DURABLE"


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _require_non_negative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"Phase 10.5 B3 {field} must be a non-negative integer")
    return value


def _require_positive_int(value: object, field: str) -> int:
    result = _require_non_negative_int(value, field)
    if result == 0:
        raise ValueError(f"Phase 10.5 B3 {field} must be positive")
    return result


def _quality(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) not in (
        _QUALITY_FIELDS, _QUALITY_FIELDS | _SESSION_QUALITY_FIELDS
    ):
        raise ValueError(f"Phase 10.5 B3 {field} quality fields are invalid")
    gap_open = value.get("gap_open")
    if not isinstance(gap_open, bool):
        raise ValueError(f"Phase 10.5 B3 {field}.gap_open must be boolean")
    result = {
        "source_age_ms": _require_non_negative_int(
            value.get("source_age_ms"), f"{field}.source_age_ms"
        ),
        "receive_age_ms": _require_non_negative_int(
            value.get("receive_age_ms"), f"{field}.receive_age_ms"
        ),
        "gap_open": gap_open,
    }
    if "state" in value:
        result.update({key: value[key] for key in _SESSION_QUALITY_FIELDS})
        # Reuse the public observation's strict typed-field validation.
        ReleaseRouteObservation(
            consumer_id="validate", requirement_key="validate", route="V2_PRIMARY",
            reason="validate", v2_source_age_ms=result["source_age_ms"],
            v2_receive_age_ms=result["receive_age_ms"], v2_gap_open=gap_open,
            v1_source_age_ms=None, v1_receive_age_ms=None, consumer_lag=0,
            cpu_millicores=0, rss_bytes=0, **_session_observation_fields(result, result),
        )
    return result


def _session_observation_fields(primary, secondary) -> dict[str, object]:
    if "state" not in primary or "state" not in secondary:
        return {}
    states = (primary["state"], secondary["state"])
    sessions = (primary["provider_session_state"], secondary["provider_session_state"])
    ages = (primary["provider_session_liveness_ms"], secondary["provider_session_liveness_ms"])
    # Either replica can block readiness; a healthy peer never hides a fault.
    return {
        "v2_quality_state": "LIVE" if states == ("LIVE", "LIVE") else next(s for s in states if s != "LIVE"),
        "v2_session_state": sessions[0] if sessions[0] == sessions[1] else "UNKNOWN",
        "v2_session_liveness_ms": max(ages) if all(a is not None for a in ages) else None,
        "v2_complete": primary["complete"] and secondary["complete"],
        "v2_execution_eligible": primary["execution_eligible"] and secondary["execution_eligible"],
    }


def _durable_no_cursor_lag(raw: Mapping[str, object], *, index: int) -> int:
    """Accept a C2-proven live durable handoff with no new cursor event."""
    handoff = raw.get("stream_handoff")
    sessions = raw.get("stream_no_event_sessions")
    if handoff == "CURRENT_FINAL_BAR_OBSERVED_NO_CURSOR":
        if raw.get("feed") != "BAR" or sessions != ["CURRENT_FINAL_BAR", "CURRENT_FINAL_BAR"]:
            raise ValueError("Phase 10.5 B3 quiet final BAR requires both current sessions")
        return 0
    if handoff not in _DURABLE_NO_CURSOR_HANDOFFS:
        raise ValueError("Phase 10.5 B3 durable no-cursor handoff is unproven")
    if (
        not isinstance(sessions, list)
        or not sessions
        or any(not isinstance(item, str) or not item for item in sessions)
    ):
        raise ValueError(
            f"Phase 10.5 B3 product[{index}] durable no-cursor sessions are invalid"
        )
    return 0


def compact_view_quality(view: object, *, observed_at_ns: int | None = None) -> dict[str, object]:
    """Return age/gap evidence from one already SDK-validated V2 view."""
    now_ns = time.time_ns() if observed_at_ns is None else observed_at_ns
    if isinstance(now_ns, bool) or not isinstance(now_ns, int) or now_ns <= 0:
        raise ValueError("Phase 10.5 B3 observed_at_ns must be positive")
    quality = getattr(view, "quality", None)
    freshness_ms = getattr(quality, "freshness_ms", None)
    gap_open = getattr(quality, "gap_open", None)
    received_at_ns = getattr(view, "received_at_ns", None)
    source_age_ms = _require_non_negative_int(freshness_ms, "view.source_age_ms")
    if not isinstance(gap_open, bool):
        raise ValueError("Phase 10.5 B3 view.gap_open must be boolean")
    if isinstance(received_at_ns, bool) or not isinstance(received_at_ns, int) or received_at_ns <= 0:
        raise ValueError("Phase 10.5 B3 view.received_at_ns must be positive")
    if received_at_ns > now_ns:
        raise ValueError("Phase 10.5 B3 view.received_at_ns is in the future")
    result = {
        "source_age_ms": source_age_ms,
        "receive_age_ms": (now_ns - received_at_ns) // 1_000_000,
        "gap_open": gap_open,
    }
    if all(hasattr(quality, key) for key in _SESSION_QUALITY_FIELDS):
        result.update({key: getattr(quality, key) for key in _SESSION_QUALITY_FIELDS})
    return _quality(result, "view")


def build_release_observation_bundle(
    plan: StableReleaseRoutePlan,
    acceptance: Mapping[str, object],
    *,
    current_reads: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build exact B3 route evidence from one current C2-compatible receipt."""
    if (
        acceptance.get("schema") != "qdl.phase105.v2-identity-acceptance.v1"
        or acceptance.get("status") != "PASS_V2_DATA_PLANE_ONLY"
        or acceptance.get("release_route_plan_sha256") != plan.digest
    ):
        raise ValueError("Phase 10.5 B3 acceptance receipt is not the frozen release scope")
    capture = acceptance.get("release_capture")
    current_by_identity = None
    if current_reads is not None:
        if (
            current_reads.get("schema") != "qdl.release.current-reads.v1"
            or current_reads.get("status") != "PASS"
            or current_reads.get("release_route_plan_sha256") != plan.digest
            or current_reads.get("acceptance_sha256") != _canonical_sha256(acceptance)
            or not isinstance(current_reads.get("products"), list)
        ):
            raise ValueError("Phase 10.5 current reads do not bind to the passing C2")
        capture = current_reads.get("release_capture")
        current_by_identity = {}
        for item in current_reads["products"]:
            if not isinstance(item, Mapping):
                raise ValueError("Phase 10.5 current read product is invalid")
            key = (item.get("consumer_id"), item.get("instrument_uid"), item.get("feed"),
                   item.get("interval"), item.get("source_policy_id"))
            if key in current_by_identity:
                raise ValueError("Phase 10.5 current reads duplicate a product")
            current_by_identity[key] = item
    if not isinstance(capture, Mapping) or set(capture) != _CAPTURE_FIELDS:
        raise ValueError("Phase 10.5 B3 acceptance receipt lacks compact capture metrics")
    captured_at_ms = _require_positive_int(capture.get("captured_at_ms"), "captured_at_ms")
    if current_reads is not None and captured_at_ms < acceptance["release_capture"]["captured_at_ms"]:
        raise ValueError("Phase 10.5 current reads predate the passing C2")
    cpu_millicores = _require_non_negative_int(capture.get("cpu_millicores"), "cpu_millicores")
    rss_bytes = _require_non_negative_int(capture.get("rss_bytes"), "rss_bytes")
    raw_products = acceptance.get("products")
    if not isinstance(raw_products, list):
        raise ValueError("Phase 10.5 B3 acceptance products are unavailable")
    if current_by_identity is not None:
        expected_keys = {(item.get("consumer_id"), item.get("instrument_uid"), item.get("feed"),
                          item.get("interval"), item.get("source_policy_id")) for item in raw_products}
        if set(current_by_identity) != expected_keys or len(expected_keys) != len(raw_products):
            raise ValueError("Phase 10.5 current reads differ from the complete C2 scope")

    expected_v2 = {
        (consumer_id, product.requirement_key): product
        for consumer_id, product in plan.products()
        if product.route == "V2_PRIMARY"
    }
    requirements = {
        (consumer.consumer_id, requirement_key(requirement)): requirement
        for consumer in plan.consumers
        for requirement in consumer.manifest.requirements
    }
    catalog = StableSourceCatalog.load(plan.source_catalog.path)
    measured: dict[tuple[str, str], tuple[dict[str, object], dict[str, object], int]] = {}
    for index, raw in enumerate(raw_products):
        if not isinstance(raw, Mapping):
            raise ValueError(f"Phase 10.5 B3 product[{index}] is invalid")
        consumer_id = raw.get("consumer_id")
        instrument_uid = raw.get("instrument_uid")
        feed = raw.get("feed")
        interval = raw.get("interval")
        policy = raw.get("source_policy_id")
        if not all(isinstance(item, str) and item for item in (
            consumer_id, instrument_uid, feed, policy,
        )) or interval is not None and not isinstance(interval, str):
            raise ValueError(f"Phase 10.5 B3 product[{index}] identity is invalid")
        route_key = ":".join((instrument_uid, feed, interval or "", policy))
        identity = (consumer_id, route_key)
        if identity not in expected_v2 or identity in measured:
            raise ValueError("Phase 10.5 B3 products differ from frozen V2 routes")
        quality_source = raw.get("closing_v2_read", raw)
        if current_by_identity is not None:
            quality_source = current_by_identity[(consumer_id, instrument_uid, feed, interval, policy)]
        if not isinstance(quality_source, Mapping) or any(quality_source.get(key) != raw.get(key) for key in (
            "consumer_id", "instrument_uid", "feed", "interval", "source_policy_id"
        )):
            raise ValueError("Phase 10.5 closing quality identity differs from C2")
        quality = quality_source.get("release_quality")
        if not isinstance(quality, Mapping) or set(quality) != {"primary", "secondary"}:
            raise ValueError(f"Phase 10.5 B3 product[{index}] quality is unavailable")
        primary = _quality(quality.get("primary"), f"product[{index}].primary")
        secondary = _quality(quality.get("secondary"), f"product[{index}].secondary")
        acknowledged = raw.get("acknowledged_offset")
        resumed = raw.get("resumed_offset")
        delivery = raw.get("delivery")
        expected_delivery = expected_release_delivery(catalog, requirements[identity])
        if delivery != expected_delivery:
            raise ValueError("Phase 10.5 B3 product delivery differs from its declared data plane")
        if delivery in {"PROVIDER_PASS_THROUGH", "ON_DEMAND"} and acknowledged is None and resumed is None:
            consumer_lag = 0
        elif delivery == "DURABLE":
            if acknowledged is None and resumed is None:
                consumer_lag = _durable_no_cursor_lag(raw, index=index)
            else:
                acknowledged_offset = _require_non_negative_int(
                    acknowledged, f"product[{index}].acknowledged_offset"
                )
                resumed_offset = _require_non_negative_int(
                    resumed, f"product[{index}].resumed_offset"
                )
                if resumed_offset <= acknowledged_offset:
                    raise ValueError("Phase 10.5 B3 stream resume did not advance")
                consumer_lag = resumed_offset - acknowledged_offset
        else:
            raise ValueError("Phase 10.5 B3 product delivery/resume evidence is invalid")
        measured[identity] = (primary, secondary, consumer_lag)
    if set(measured) != set(expected_v2):
        raise ValueError("Phase 10.5 B3 products are incomplete")

    observations: list[ReleaseRouteObservation] = []
    for consumer_id, product in plan.products():
        if product.route == "V1_PRIMARY":
            observations.append(ReleaseRouteObservation(
                consumer_id=consumer_id,
                requirement_key=product.requirement_key,
                route="V1_PRIMARY",
                reason=product.reason or "",
                v2_source_age_ms=None,
                v2_receive_age_ms=None,
                v2_gap_open=False,
                v1_source_age_ms=None,
                v1_receive_age_ms=None,
                consumer_lag=0,
                cpu_millicores=cpu_millicores,
                rss_bytes=rss_bytes,
            ))
            continue
        primary, secondary, consumer_lag = measured[(consumer_id, product.requirement_key)]
        observations.append(ReleaseRouteObservation(
            consumer_id=consumer_id,
            requirement_key=product.requirement_key,
            route="V2_PRIMARY",
            reason="V2_READY",
            v2_source_age_ms=max(
                int(primary["source_age_ms"]), int(secondary["source_age_ms"])
            ),
            v2_receive_age_ms=max(
                int(primary["receive_age_ms"]), int(secondary["receive_age_ms"])
            ),
            v2_gap_open=bool(primary["gap_open"] or secondary["gap_open"]),
            v1_source_age_ms=None,
            v1_receive_age_ms=None,
            consumer_lag=consumer_lag,
            cpu_millicores=cpu_millicores,
            rss_bytes=rss_bytes,
            **_session_observation_fields(primary, secondary),
        ))
    result = {
        "schema": OBSERVATION_BUNDLE_SCHEMA,
        "release_route_plan_sha256": plan.digest,
        "captured_at_ms": captured_at_ms,
        "acceptance_sha256": _canonical_sha256(acceptance),
        "observations": [item.public_record() for item in observations],
    }
    if current_reads is not None:
        result["current_reads_sha256"] = _canonical_sha256(current_reads)
    return result


def parse_release_observation_bundle(
    plan: StableReleaseRoutePlan,
    value: object,
    *,
    now_ms: int | None = None,
    max_age_ms: int = MAX_OBSERVATION_AGE_MS,
) -> tuple[ReleaseRouteObservation, ...]:
    """Validate a fresh exact B3 observation bundle before certification."""
    if not isinstance(value, Mapping) or set(value) not in (
        _BUNDLE_FIELDS, _BUNDLE_FIELDS | {"current_reads_sha256"}
    ):
        raise ValueError("Phase 10.5 B3 observation bundle fields are invalid")
    if value.get("schema") != OBSERVATION_BUNDLE_SCHEMA:
        raise ValueError("Phase 10.5 B3 observation bundle schema is invalid")
    if value.get("release_route_plan_sha256") != plan.digest:
        raise ValueError("Phase 10.5 B3 observation bundle route plan differs")
    _require_positive_int(value.get("captured_at_ms"), "captured_at_ms")
    captured_at_ms = int(value["captured_at_ms"])
    current_ms = int(time.time() * 1000) if now_ms is None else now_ms
    if isinstance(current_ms, bool) or not isinstance(current_ms, int) or current_ms < captured_at_ms:
        raise ValueError("Phase 10.5 B3 observation bundle clock is invalid")
    if max_age_ms < 1 or current_ms - captured_at_ms > max_age_ms:
        raise ValueError("Phase 10.5 B3 observation bundle is stale")
    acceptance_sha256 = value.get("acceptance_sha256")
    if (
        not isinstance(acceptance_sha256, str)
        or len(acceptance_sha256) != 64
        or any(character not in "0123456789abcdef" for character in acceptance_sha256)
    ):
        raise ValueError("Phase 10.5 B3 observation bundle acceptance digest is invalid")
    if "current_reads_sha256" in value:
        digest = value["current_reads_sha256"]
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("Phase 10.5 B3 current-read digest is invalid")
    from qdl.certification.phase105_release import parse_release_observations

    return parse_release_observations(value.get("observations"))
