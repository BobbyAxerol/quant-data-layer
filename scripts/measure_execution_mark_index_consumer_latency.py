#!/usr/bin/env python3
"""Measure execution MARK/INDEX latency from a real V2 consumer's boundary.

The probe is read-only.  It uses the registered trading-system paper workload
identity and the public ``AsyncDataLayerClient`` exactly as a consumer does.
The primary metric is therefore not a server-side timestamp: it is the time
between initiating ``reference_batch`` and receiving the typed, SDK-validated
result which a consumer can use.

It also records immutable provider/component lineage age separately.  For a
quiet MARK/INDEX component this is deliberately *not* a delivery-latency
metric: the source timestamp remains unchanged while the governed session,
generation, gap fence and component cadence establish whether the result is
usable.  The gate measures consumer-call-to-usable latency and validates that
typed quiet-session evidence independently.

Run it from a disposable container on the stable internal network with the
existing ``trading-system`` identity mounted read-only.  It never calls V1,
opens a venue connection, or sends an order.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import statistics
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.reference_l2_acceptance import (  # noqa: E402
    reference_request_for_requirement,
)
from qdl.consumer.manifest import ConsumerManifestLoader  # noqa: E402
from qdl.query import ConsumerGrade, FeedType  # noqa: E402
from qdl_sdk import AsyncDataLayerClient  # noqa: E402
from qdl_sdk.reference import ReferenceProduct, ReferenceRequirement  # noqa: E402
from scripts.measure_consumer_request_latency import transports  # noqa: E402


_LIVE_ENDPOINT = "qdl://stable-stream/internal/v2/execution/mark-index/latest"
_LIVE_VIEW = "STABLE_STREAM_GATEWAY"
_VALID_STAGES = frozenset({"CANONICAL_READ_COMMITTED", "SPOOL_CONFIRMED"})
_VALID_RECENCY_MODES = frozenset({
    "STRICT_EVENT_SESSION_LIVE",
    "COMPONENT_SESSION_LIVE",
})
_DEFAULT_MANIFEST = ROOT / "consumers/stable/trading-system-paper.yaml"


def _percentile(values: Iterable[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot calculate percentile for an empty sample")
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * quantile) - 1))
    return ordered[index]


def _summary(samples: Iterable[float]) -> dict[str, float | int]:
    values = list(samples)
    if not values:
        return {"n": 0}
    return {
        "n": len(values),
        "min_ms": round(min(values), 3),
        "p50_ms": round(statistics.median(values), 3),
        "p95_ms": round(_percentile(values, 0.95), 3),
        "p99_ms": round(_percentile(values, 0.99), 3),
        "max_ms": round(max(values), 3),
    }


def execution_mark_index_requirements(manifest: Path) -> tuple[ReferenceRequirement, ...]:
    """Return the exact ten execution MARK/INDEX requirements of the manifest."""

    document = ConsumerManifestLoader.load(manifest)
    selected = [
        item
        for item in document.requirements
        if item.feed is FeedType.MARK_INDEX_PRICE
    ]
    if len(selected) != 10:
        raise ValueError(
            "execution MARK/INDEX consumer latency probe requires exactly 10 manifest bindings"
        )
    if any(item.consumer_grade is not ConsumerGrade.EXECUTION for item in selected):
        raise ValueError("execution MARK/INDEX manifest contains a non-execution requirement")
    if len({item.instrument_uid for item in selected}) != len(selected):
        raise ValueError("execution MARK/INDEX manifest contains duplicate instruments")
    mapped = tuple(
        reference_request_for_requirement(item, now_ns=time.time_ns())
        for item in selected
    )
    if any(
        item.product is not ReferenceProduct.MARK_INDEX_PRICE
        or item.consumer_grade.value != ConsumerGrade.EXECUTION.value
        or item.max_freshness_ms is None
        or getattr(item.event_recency_policy, "value", item.event_recency_policy)
        != "OBSERVE"
        or item.max_session_liveness_ms is None
        for item in mapped
    ):
        raise ValueError("execution MARK/INDEX mapping lost its governed reference policy")
    return mapped


def validate_live_response(
    requirements: tuple[ReferenceRequirement, ...],
    response: Any,
    *,
    usable_at_ns: int,
) -> dict[str, dict[str, Any]]:
    """Validate an SDK response and return only bounded per-binding evidence.

    This is intentionally stricter than a successful HTTP response.  It proves
    the live gateway route was selected and rejects a direct provider result,
    a partial response, a missing price pair, or an identity mix-up.
    """

    if bool(getattr(response, "partial", True)):
        raise ValueError("execution MARK/INDEX response is partial")
    results = tuple(getattr(response, "results", ()))
    if len(results) != len(requirements):
        raise ValueError("execution MARK/INDEX response cardinality differs from request")
    expected = {item.instrument_uid: item for item in requirements}
    values: dict[str, dict[str, Any]] = {}
    for item in results:
        instrument_uid = str(getattr(item, "instrument_uid", ""))
        requirement = expected.get(instrument_uid)
        data = getattr(item, "data", None)
        if (
            requirement is None
            or getattr(item, "product", None) is not ReferenceProduct.MARK_INDEX_PRICE
            or getattr(item, "status", None) != "OK"
            or getattr(item, "problem", None) is not None
            or data is None
            or getattr(data, "status", None) != "OK"
            or getattr(data, "instrument_uid", None) != instrument_uid
            or getattr(data, "product", None) is not ReferenceProduct.MARK_INDEX_PRICE
        ):
            raise ValueError("execution MARK/INDEX result is non-OK or identity-mismatched")
        observations = tuple(getattr(data, "observations", ()))
        if len(observations) != 1:
            raise ValueError("execution MARK/INDEX result must contain one current observation")
        observation = observations[0]
        if (
            getattr(observation, "instrument_uid", None) != instrument_uid
            or getattr(observation, "product", None) is not ReferenceProduct.MARK_INDEX_PRICE
        ):
            raise ValueError("execution MARK/INDEX observation identity differs from result")
        fields = {str(field.name) for field in getattr(observation, "fields", ())}
        if fields != {"mark_price", "index_price"}:
            raise ValueError("execution MARK/INDEX observation is not a complete price pair")
        labels = dict(getattr(observation, "labels", {}))
        received_at_ns = int(getattr(data, "received_at_ns", 0))
        provider_confirmation_ns = int(labels.get("provider_confirmation_ns", "0"))
        source_event_time_ns = int(labels.get("source_event_time_ns", "0"))
        stage = labels.get("delivery_stage")
        if (
            labels.get("execution_view") != _LIVE_VIEW
            or provider_confirmation_ns <= 0
            or provider_confirmation_ns != received_at_ns
            or source_event_time_ns <= 0
            or source_event_time_ns > provider_confirmation_ns
            or stage not in _VALID_STAGES
            or received_at_ns > usable_at_ns
        ):
            raise ValueError("execution MARK/INDEX live-view lineage or freshness is invalid")
        lineage = tuple(getattr(data, "lineage", ()))
        if not lineage or any(
            getattr(entry, "provider_endpoint", None) != _LIVE_ENDPOINT
            for entry in lineage
        ):
            raise ValueError("execution MARK/INDEX response did not use the internal live reader")
        values[instrument_uid] = _quiet_session_evidence(
            requirement,
            labels,
            provider_confirmation_ns=provider_confirmation_ns,
            usable_at_ns=usable_at_ns,
        )
        values[instrument_uid]["delivery_stage"] = stage
    if set(values) != set(expected):
        raise ValueError("execution MARK/INDEX response did not cover every requested binding")
    return values


def _quiet_session_evidence(
    requirement: ReferenceRequirement,
    labels: dict[str, str],
    *,
    provider_confirmation_ns: int,
    usable_at_ns: int,
) -> dict[str, Any]:
    """Validate the explicit quiet-channel contract at the SDK boundary.

    Provider timestamps are immutable lineage.  A quiet component is admitted
    only when the stream/query path proves a current provider session and the
    exact component receipt remains inside its signed cadence.  This mirrors
    the fail-closed query check without treating an unchanged component as a
    newly delivered market event.
    """

    policy = getattr(
        requirement.event_recency_policy,
        "value",
        requirement.event_recency_policy,
    )
    if policy != "OBSERVE" or requirement.max_session_liveness_ms is None:
        raise ValueError("execution MARK/INDEX requirement lacks quiet-session policy")
    if (
        labels.get("event_recency_policy") != "OBSERVE"
        or labels.get("recency_mode") not in _VALID_RECENCY_MODES
        or labels.get("provider_session_state") != "LIVE"
    ):
        raise ValueError("execution MARK/INDEX quiet-session evidence is not live")
    try:
        session_liveness_ms = int(labels["provider_session_liveness_ms"])
        session_checked_at_ns = int(labels["provider_session_checked_at_ns"])
        components = {
            name: (
                int(labels[f"component_{name.lower()}_received_at_ns"]),
                int(labels[f"component_{name.lower()}_quiet_after_ms"]),
            )
            for name in ("MARK", "INDEX")
        }
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("execution MARK/INDEX quiet-session evidence is malformed") from error
    if (
        session_liveness_ms < 0
        or session_checked_at_ns <= 0
        or session_checked_at_ns > usable_at_ns
    ):
        raise ValueError("execution MARK/INDEX quiet-session clock is invalid")
    session_age_ms = session_liveness_ms + (
        usable_at_ns - session_checked_at_ns
    ) / 1_000_000
    if session_age_ms > requirement.max_session_liveness_ms:
        raise ValueError("execution MARK/INDEX provider session exceeded its SLA")
    component_ages: dict[str, float] = {}
    for name, (receipt_ns, quiet_after_ms) in components.items():
        if (
            receipt_ns <= 0
            or receipt_ns > usable_at_ns
            or not 250 <= quiet_after_ms <= 120_000
        ):
            raise ValueError("execution MARK/INDEX component evidence is invalid")
        age_ms = (usable_at_ns - receipt_ns) / 1_000_000
        if age_ms > quiet_after_ms:
            raise ValueError("execution MARK/INDEX component exceeded its quiet cadence")
        component_ages[name] = age_ms
    return {
        # This is an immutable lineage diagnostic, never the quiet-channel SLA.
        "provider_confirmation_to_usable_ms": (
            usable_at_ns - provider_confirmation_ns
        ) / 1_000_000,
        "provider_session_liveness_to_usable_ms": session_age_ms,
        "component_mark_age_to_usable_ms": component_ages["MARK"],
        "component_index_age_to_usable_ms": component_ages["INDEX"],
        "recency_mode": labels["recency_mode"],
    }


async def collect(
    client: AsyncDataLayerClient,
    requirements: tuple[ReferenceRequirement, ...],
    *,
    duration_seconds: float,
    cadence_seconds: float,
) -> dict[str, Any]:
    if duration_seconds <= 0 or cadence_seconds <= 0:
        raise ValueError("duration and cadence must be positive")
    deadline = time.monotonic() + duration_seconds
    next_call = time.monotonic()
    calls_ms: list[float] = []
    provider_age_by_uid: dict[str, list[float]] = defaultdict(list)
    session_age_by_uid: dict[str, list[float]] = defaultdict(list)
    mark_age_by_uid: dict[str, list[float]] = defaultdict(list)
    index_age_by_uid: dict[str, list[float]] = defaultdict(list)
    stages: Counter[str] = Counter()
    recency_modes: Counter[str] = Counter()
    errors: list[str] = []
    batches = 0
    while time.monotonic() < deadline:
        delay = next_call - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        started = time.perf_counter()
        try:
            response = await client.reference_batch(requirements, require_all=True)
            usable_at_ns = time.time_ns()
            values = validate_live_response(
                requirements, response, usable_at_ns=usable_at_ns
            )
        except Exception as error:  # noqa: BLE001 - bounded evidence, no retry disguise
            errors.append(f"{type(error).__name__}: {error}"[:240])
            break
        calls_ms.append((time.perf_counter() - started) * 1_000)
        batches += 1
        for uid, value in values.items():
            provider_age_by_uid[uid].append(
                value["provider_confirmation_to_usable_ms"]
            )
            session_age_by_uid[uid].append(
                value["provider_session_liveness_to_usable_ms"]
            )
            mark_age_by_uid[uid].append(value["component_mark_age_to_usable_ms"])
            index_age_by_uid[uid].append(value["component_index_age_to_usable_ms"])
            stages[str(value["delivery_stage"])] += 1
            recency_modes[str(value["recency_mode"])] += 1
        next_call += cadence_seconds
    per_binding = {
        uid: {
            "provider_confirmation_to_usable_ms": _summary(
                provider_age_by_uid[uid]
            ),
            "provider_session_liveness_to_usable_ms": _summary(
                session_age_by_uid[uid]
            ),
            "component_mark_age_to_usable_ms": _summary(mark_age_by_uid[uid]),
            "component_index_age_to_usable_ms": _summary(index_age_by_uid[uid]),
        }
        for uid in sorted(provider_age_by_uid)
    }
    all_provider_age = [
        value for values in provider_age_by_uid.values() for value in values
    ]
    all_session_age = [
        value for values in session_age_by_uid.values() for value in values
    ]
    all_component_age = [
        value
        for values in (*mark_age_by_uid.values(), *index_age_by_uid.values())
        for value in values
    ]
    return {
        "batches": batches,
        "consumer_call_to_usable_ms": _summary(calls_ms),
        "provider_confirmation_to_usable_ms": _summary(all_provider_age),
        "provider_session_liveness_to_usable_ms": _summary(all_session_age),
        "component_age_to_usable_ms": _summary(all_component_age),
        "per_binding": per_binding,
        "delivery_stages": dict(sorted(stages.items())),
        "recency_modes": dict(sorted(recency_modes.items())),
        "errors": errors,
    }


def _write_evidence(path: Path, result: dict[str, Any]) -> None:
    target = path.expanduser().resolve()
    if target.exists():
        raise ValueError("consumer latency evidence path already exists")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    target.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    target.chmod(0o600)


async def run(args: argparse.Namespace) -> dict[str, Any]:
    requirements = execution_mark_index_requirements(args.manifest)
    query, stream = transports()
    client = AsyncDataLayerClient(
        query_transport=query,
        stream_transport=stream,
        consumer_id=os.environ.get("QDL_CONSUMER_ID", "trading-system.paper.stable"),
    )
    started_ns = time.time_ns()
    try:
        result = await collect(
            client,
            requirements,
            duration_seconds=args.duration_seconds,
            cadence_seconds=args.cadence_seconds,
        )
    finally:
        await client.close()
    minimum_samples, gate_passed = _acceptance_gate(
        result,
        requirement_count=len(requirements),
        duration_seconds=args.duration_seconds,
        cadence_seconds=args.cadence_seconds,
        max_consumer_call_p99_ms=args.max_p99_ms,
    )
    evidence = {
        "schema": "qdl.execution-mark-index-consumer-latency.v2",
        "started_at_ns": started_ns,
        "finished_at_ns": time.time_ns(),
        "consumer_id": os.environ.get("QDL_CONSUMER_ID", "trading-system.paper.stable"),
        "route": "V2_REFERENCE_BATCH_INTERNAL_EXECUTION",
        "v1_fallback_attempted": False,
        "direct_provider_request_attempted": False,
        "requirement_count": len(requirements),
        "minimum_samples_per_binding": minimum_samples,
        "max_consumer_call_to_usable_p99_ms": args.max_p99_ms,
        "gate_passed": gate_passed,
        **result,
    }
    if args.output:
        _write_evidence(args.output, evidence)
    return evidence


def _acceptance_gate(
    result: dict[str, Any],
    *,
    requirement_count: int,
    duration_seconds: float,
    cadence_seconds: float,
    max_consumer_call_p99_ms: float,
) -> tuple[int, bool]:
    """Apply the C2 gate without ever substituting immutable lineage for latency."""

    minimum_samples = math.floor(duration_seconds / cadence_seconds) - 1
    aggregate = result["consumer_call_to_usable_ms"]
    per_binding_complete = all(
        data["provider_session_liveness_to_usable_ms"].get("n", 0)
        >= minimum_samples
        and data["component_mark_age_to_usable_ms"].get("n", 0) >= minimum_samples
        and data["component_index_age_to_usable_ms"].get("n", 0) >= minimum_samples
        for data in result["per_binding"].values()
    )
    return (
        minimum_samples,
        (
            not result["errors"]
            and len(result["per_binding"]) == requirement_count
            and per_binding_complete
            and aggregate.get("n", 0) >= minimum_samples
            and float(aggregate.get("p99_ms", float("inf")))
            < max_consumer_call_p99_ms
        ),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=_DEFAULT_MANIFEST)
    parser.add_argument("--duration-seconds", type=float, default=300.0)
    parser.add_argument("--cadence-seconds", type=float, default=2.0)
    parser.add_argument("--max-p99-ms", type=float, default=2_000.0)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = asyncio.run(run(args))
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result["gate_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
