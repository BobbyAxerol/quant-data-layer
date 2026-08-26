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

from qdl.consumer import ReleaseRouteObservation, StableReleaseRoutePlan


OBSERVATION_BUNDLE_SCHEMA = "qdl.phase105.release-observations.v1"
MAX_OBSERVATION_AGE_MS = 300_000
_QUALITY_FIELDS = frozenset({"source_age_ms", "receive_age_ms", "gap_open"})
_CAPTURE_FIELDS = frozenset({"captured_at_ms", "cpu_millicores", "rss_bytes"})
_BUNDLE_FIELDS = frozenset({
    "schema",
    "release_route_plan_sha256",
    "captured_at_ms",
    "acceptance_sha256",
    "observations",
})


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


def _quality(value: object, field: str) -> dict[str, int | bool]:
    if not isinstance(value, Mapping) or set(value) != _QUALITY_FIELDS:
        raise ValueError(f"Phase 10.5 B3 {field} quality fields are invalid")
    gap_open = value.get("gap_open")
    if not isinstance(gap_open, bool):
        raise ValueError(f"Phase 10.5 B3 {field}.gap_open must be boolean")
    return {
        "source_age_ms": _require_non_negative_int(
            value.get("source_age_ms"), f"{field}.source_age_ms"
        ),
        "receive_age_ms": _require_non_negative_int(
            value.get("receive_age_ms"), f"{field}.receive_age_ms"
        ),
        "gap_open": gap_open,
    }


def compact_view_quality(view: object, *, observed_at_ns: int | None = None) -> dict[str, int | bool]:
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
    return {
        "source_age_ms": source_age_ms,
        "receive_age_ms": (now_ns - received_at_ns) // 1_000_000,
        "gap_open": gap_open,
    }


def build_release_observation_bundle(
    plan: StableReleaseRoutePlan,
    acceptance: Mapping[str, object],
) -> dict[str, object]:
    """Build exact B3 route evidence from one current C2-compatible receipt."""
    if (
        acceptance.get("schema") != "qdl.phase105.v2-identity-acceptance.v1"
        or acceptance.get("status") != "PASS_V2_DATA_PLANE_ONLY"
        or acceptance.get("release_route_plan_sha256") != plan.digest
    ):
        raise ValueError("Phase 10.5 B3 acceptance receipt is not the frozen release scope")
    capture = acceptance.get("release_capture")
    if not isinstance(capture, Mapping) or set(capture) != _CAPTURE_FIELDS:
        raise ValueError("Phase 10.5 B3 acceptance receipt lacks compact capture metrics")
    captured_at_ms = _require_positive_int(capture.get("captured_at_ms"), "captured_at_ms")
    cpu_millicores = _require_non_negative_int(capture.get("cpu_millicores"), "cpu_millicores")
    rss_bytes = _require_non_negative_int(capture.get("rss_bytes"), "rss_bytes")
    raw_products = acceptance.get("products")
    if not isinstance(raw_products, list):
        raise ValueError("Phase 10.5 B3 acceptance products are unavailable")

    expected_v2 = {
        (consumer_id, product.requirement_key): product
        for consumer_id, product in plan.products()
        if product.route == "V2_PRIMARY"
    }
    measured: dict[tuple[str, str], tuple[dict[str, int | bool], dict[str, int | bool], int]] = {}
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
        requirement_key = ":".join((instrument_uid, feed, interval or "", policy))
        identity = (consumer_id, requirement_key)
        if identity not in expected_v2 or identity in measured:
            raise ValueError("Phase 10.5 B3 products differ from frozen V2 routes")
        quality = raw.get("release_quality")
        if not isinstance(quality, Mapping) or set(quality) != {"primary", "secondary"}:
            raise ValueError(f"Phase 10.5 B3 product[{index}] quality is unavailable")
        primary = _quality(quality.get("primary"), f"product[{index}].primary")
        secondary = _quality(quality.get("secondary"), f"product[{index}].secondary")
        acknowledged = raw.get("acknowledged_offset")
        resumed = raw.get("resumed_offset")
        delivery = raw.get("delivery")
        if delivery == "PROVIDER_PASS_THROUGH" and acknowledged is None and resumed is None:
            consumer_lag = 0
        elif delivery == "DURABLE":
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
        ))
    return {
        "schema": OBSERVATION_BUNDLE_SCHEMA,
        "release_route_plan_sha256": plan.digest,
        "captured_at_ms": captured_at_ms,
        "acceptance_sha256": _canonical_sha256(acceptance),
        "observations": [item.public_record() for item in observations],
    }


def parse_release_observation_bundle(
    plan: StableReleaseRoutePlan,
    value: object,
    *,
    now_ms: int | None = None,
    max_age_ms: int = MAX_OBSERVATION_AGE_MS,
) -> tuple[ReleaseRouteObservation, ...]:
    """Validate a fresh exact B3 observation bundle before certification."""
    if not isinstance(value, Mapping) or set(value) != _BUNDLE_FIELDS:
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
    from qdl.certification.phase105_release import parse_release_observations

    return parse_release_observations(value.get("observations"))
