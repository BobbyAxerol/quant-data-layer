"""Fail-closed evidence gate for Phase 10.5-D stable release certification.

This module is deliberately pure. It validates compact evidence produced by
the separately approved runtime handoff; it never starts a service, changes a
route, or treats a fixture as runtime acceptance.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict
from typing import Any, Iterable, Mapping

from qdl.certification.gates import CertificationGate, CertificationReport, GateStatus
from qdl.certification.phase105_consumer_acceptance import PHASE105_PAPER_CONSUMER_IDS
from qdl.certification.phase105_handoff import V1_FALLBACK_COMMIT, V1_FALLBACK_VERSION
from qdl.consumer import (
    ReleaseRouteObservation,
    StableReleaseRoutePlan,
    evaluate_release_readiness,
    is_explicit_v1_exclusion,
    requirement_key,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_OBSERVATION_FIELDS = frozenset({
    "consumer_id",
    "requirement_key",
    "route",
    "reason",
    "v2_source_age_ms",
    "v2_receive_age_ms",
    "v2_gap_open",
    "v1_source_age_ms",
    "v1_receive_age_ms",
    "consumer_lag",
    "cpu_millicores",
    "rss_bytes",
})
_RUNTIME_SCHEMA = "qdl.phase105c.runtime-handoff-evidence.v1"
_ACCEPTANCE_SCHEMA = "qdl.phase105.v2-identity-acceptance.v1"
_FALLBACK_SCHEMA = "qdl.phase105.v1-fallback-return.v1"
_V1_SCHEMA = "qdl.phase105.v1-fallback-provenance.v1"
_SECRET_FIELD_NAMES = frozenset({
    "api_key",
    "apikey",
    "credential",
    "credentials",
    "password",
    "private_key",
    "secret",
    "secrets",
    "token",
})
_V1_FIELDS = frozenset({
    "schema",
    "status",
    "image_id",
    "source_commit",
    "source_tree",
    "dockerfile_sha256",
    "version",
})
_RUNTIME_FIELDS = frozenset({
    "schema",
    "status",
    "release_route_plan_sha256",
    "v2_python_image",
    "v2_rust_image",
    "authority_mode",
    "demanded_slices_status",
    "order_actions",
    "test_provenance",
})
_ACCEPTANCE_REQUIRED_FIELDS = frozenset({
    "schema",
    "status",
    "release_route_plan_sha256",
    "authority_revision",
    "scope_sha256",
    "product_count",
    "durable_product_count",
    "products",
    "provider_connections",
    "order_actions",
    "cursor_directory_removed",
    "secret_values_recorded",
    "test_provenance",
})
_FALLBACK_REQUIRED_FIELDS = frozenset({
    "schema",
    "status",
    "release_route_plan_sha256",
    "routes",
    "provider_connections",
    "order_actions",
    "cursor_directory_removed",
    "secret_values_recorded",
    "test_provenance",
})


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"Phase 10.5-D {field} must be an object")
    _reject_secret_material(value, field)
    return value


def _reject_secret_material(value: object, field: str) -> None:
    """Keep certification input reviewable without accepting credential material."""
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = str(key).lower().replace("-", "_")
            if (
                normalized in _SECRET_FIELD_NAMES
                or normalized.endswith(("_secret", "_token", "_password", "_private_key"))
            ):
                raise ValueError(
                    f"Phase 10.5-D {field} must not contain secret-like material"
                )
            _reject_secret_material(nested, field)
    elif isinstance(value, list):
        for nested in value:
            _reject_secret_material(nested, field)


def parse_release_observations(raw: object) -> tuple[ReleaseRouteObservation, ...]:
    """Parse public route observations without accepting hidden fields."""
    if not isinstance(raw, list) or not raw:
        raise ValueError("Phase 10.5-D observations must be a non-empty list")
    values: list[ReleaseRouteObservation] = []
    for index, item in enumerate(raw):
        value = _mapping(item, f"observation[{index}]")
        if set(value) != _OBSERVATION_FIELDS:
            raise ValueError("Phase 10.5-D observation fields differ from public contract")
        if not all(
            isinstance(value[field], str) and value[field]
            for field in ("consumer_id", "requirement_key", "route", "reason")
        ):
            raise ValueError("Phase 10.5-D observation identity is invalid")
        if not isinstance(value["v2_gap_open"], bool):
            raise ValueError("Phase 10.5-D observation gap state must be boolean")
        for field in (
            "v2_source_age_ms",
            "v2_receive_age_ms",
            "v1_source_age_ms",
            "v1_receive_age_ms",
            "consumer_lag",
            "cpu_millicores",
            "rss_bytes",
        ):
            measurement = value[field]
            if measurement is not None and (
                isinstance(measurement, bool) or not isinstance(measurement, int)
            ):
                raise ValueError(
                    "Phase 10.5-D observation measurements must be integer milliseconds/bytes"
                )
        values.append(ReleaseRouteObservation(**dict(value)))
    return tuple(values)


def _expected_v2_products(plan: StableReleaseRoutePlan) -> frozenset[tuple[str, str]]:
    return frozenset(
        (consumer_id, product.requirement_key)
        for consumer_id, product in plan.products()
        if product.route == "V2_PRIMARY"
    )


def _expected_v1_drills(plan: StableReleaseRoutePlan) -> frozenset[tuple[str, str]]:
    return frozenset(
        (consumer_id, product.requirement_key)
        for consumer_id, product in plan.products()
        if product.route == "V2_PRIMARY" and product.fallback == "V1"
    )


def _gate(gate_id: str, passed: bool, evidence: str) -> CertificationGate:
    return CertificationGate(
        gate_id=gate_id,
        status=GateStatus.PASS if passed else GateStatus.BLOCKED,
        evidence=evidence,
    )


def _validate_v1_provenance(
    plan: StableReleaseRoutePlan, value: object
) -> tuple[bool, str]:
    evidence = _mapping(value, "V1 provenance")
    source_tree = evidence.get("source_tree")
    dockerfile_sha256 = evidence.get("dockerfile_sha256")
    passed = (
        set(evidence) == _V1_FIELDS
        and evidence.get("schema") == _V1_SCHEMA
        and evidence.get("status") == "PASS"
        and evidence.get("source_commit") == plan.v1_fallback.source_commit
        and evidence.get("source_commit") == V1_FALLBACK_COMMIT
        and evidence.get("version") == plan.v1_fallback.release_tag
        and evidence.get("version") == V1_FALLBACK_VERSION
        and isinstance(source_tree, str)
        and re.fullmatch(r"[0-9a-f]{40}", source_tree) is not None
        and isinstance(dockerfile_sha256, str)
        and _SHA256.fullmatch(dockerfile_sha256) is not None
    )
    image_id = evidence.get("image_id")
    if not isinstance(image_id, str) or not _DIGEST.fullmatch(image_id):
        passed = False
    return passed, _canonical_sha256(evidence)


def _validate_runtime_evidence(
    plan: StableReleaseRoutePlan, value: object
) -> tuple[bool, str, str | None, str | None]:
    evidence = _mapping(value, "runtime handoff evidence")
    raw_python_image = evidence.get("v2_python_image")
    raw_rust_image = evidence.get("v2_rust_image")
    python_image = raw_python_image if isinstance(raw_python_image, str) else None
    rust_image = raw_rust_image if isinstance(raw_rust_image, str) else None
    valid_images = (
        isinstance(python_image, str)
        and _DIGEST.fullmatch(python_image) is not None
        and isinstance(rust_image, str)
        and _DIGEST.fullmatch(rust_image) is not None
    )
    passed = (
        set(evidence) == _RUNTIME_FIELDS
        and evidence.get("schema") == _RUNTIME_SCHEMA
        and evidence.get("status") == "PASS"
        and evidence.get("release_route_plan_sha256") == plan.digest
        and evidence.get("authority_mode") == "RUST_PRIMARY"
        and evidence.get("demanded_slices_status") == "PASS"
        and evidence.get("order_actions") == 0
        and evidence.get("test_provenance") is False
        and valid_images
    )
    return passed, _canonical_sha256(evidence), python_image, rust_image


def _measurements_are_current(
    plan: StableReleaseRoutePlan,
    observations: Iterable[ReleaseRouteObservation],
) -> bool:
    requirements = {
        (consumer.consumer_id, product.requirement_key): (
            product,
            {
                requirement_key(item): item
                for item in consumer.manifest.requirements
            }[product.requirement_key],
        )
        for consumer in plan.consumers
        for product in consumer.products
    }
    for item in observations:
        product, requirement = requirements[(item.consumer_id, item.requirement_key)]
        maximum = requirement.max_freshness_ms
        if product.route == "V1_PRIMARY":
            if not is_explicit_v1_exclusion(product, item):
                return False
            continue
        if item.route == "V2_PRIMARY":
            ages = (item.v2_source_age_ms, item.v2_receive_age_ms)
            if item.v2_gap_open or any(age is None for age in ages):
                return False
        else:
            return False
        if maximum is not None and any(age > maximum for age in ages if age is not None):
            return False
    return True


def _validate_acceptance(
    plan: StableReleaseRoutePlan, value: object
) -> tuple[bool, str]:
    evidence = _mapping(value, "V2 consumer acceptance")
    products = evidence.get("products")
    if not isinstance(products, list):
        return False, _canonical_sha256(evidence)
    identities: set[tuple[str, str]] = set()
    consumers: set[str] = set()
    for item in products:
        if not isinstance(item, Mapping):
            return False, _canonical_sha256(evidence)
        consumer_id = item.get("consumer_id")
        requirement_key = item.get("instrument_uid")
        feed = item.get("feed")
        interval = item.get("interval")
        policy = item.get("source_policy_id")
        if not all(isinstance(field, str) and field for field in (
            consumer_id, requirement_key, feed, policy,
        )):
            return False, _canonical_sha256(evidence)
        if interval is not None and not isinstance(interval, str):
            return False, _canonical_sha256(evidence)
        identities.add((
            consumer_id,
            ":".join((requirement_key, feed, interval or "", policy)),
        ))
        consumers.add(consumer_id)
    expected = {
        item for item in _expected_v2_products(plan)
        if item[0] in PHASE105_PAPER_CONSUMER_IDS
    }
    passed = (
        _ACCEPTANCE_REQUIRED_FIELDS <= set(evidence)
        and evidence.get("schema") == _ACCEPTANCE_SCHEMA
        and evidence.get("status") == "PASS_V2_DATA_PLANE_ONLY"
        and evidence.get("release_route_plan_sha256") == plan.digest
        and isinstance(evidence.get("authority_revision"), int)
        and not isinstance(evidence.get("authority_revision"), bool)
        and evidence.get("authority_revision", 0) >= 1
        and isinstance(evidence.get("scope_sha256"), str)
        and _SHA256.fullmatch(evidence.get("scope_sha256", "")) is not None
        and evidence.get("product_count") == len(products)
        and isinstance(evidence.get("durable_product_count"), int)
        and not isinstance(evidence.get("durable_product_count"), bool)
        and 0 <= evidence.get("durable_product_count", -1) <= len(products)
        and evidence.get("provider_connections") == 0
        and evidence.get("order_actions") == 0
        and evidence.get("cursor_directory_removed") is True
        and evidence.get("secret_values_recorded") is False
        and evidence.get("test_provenance") is False
        and consumers == PHASE105_PAPER_CONSUMER_IDS
        and identities == expected
        and len(products) == len(identities)
    )
    return passed, _canonical_sha256(evidence)


def _validate_fallback_drill(
    plan: StableReleaseRoutePlan, value: object
) -> tuple[bool, str]:
    evidence = _mapping(value, "V1 fallback drill")
    routes = evidence.get("routes")
    if not isinstance(routes, list):
        return False, _canonical_sha256(evidence)
    actual: set[tuple[str, str]] = set()
    for item in routes:
        if not isinstance(item, Mapping):
            return False, _canonical_sha256(evidence)
        consumer_id = item.get("consumer_id")
        requirement_key = item.get("requirement_key")
        if not isinstance(consumer_id, str) or not isinstance(requirement_key, str):
            return False, _canonical_sha256(evidence)
        if (
            item.get("before_route") != "V2_PRIMARY"
            or item.get("fallback_route") != "V1_FALLBACK"
            or item.get("returned_route") != "V2_PRIMARY"
        ):
            return False, _canonical_sha256(evidence)
        actual.add((consumer_id, requirement_key))
    passed = (
        _FALLBACK_REQUIRED_FIELDS <= set(evidence)
        and evidence.get("schema") == _FALLBACK_SCHEMA
        and evidence.get("status") == "PASS"
        and evidence.get("release_route_plan_sha256") == plan.digest
        and evidence.get("provider_connections") == 0
        and evidence.get("order_actions") == 0
        and evidence.get("cursor_directory_removed") is True
        and evidence.get("secret_values_recorded") is False
        and evidence.get("test_provenance") is False
        and actual == _expected_v1_drills(plan)
        and len(routes) == len(actual)
    )
    return passed, _canonical_sha256(evidence)


def certify_stable_release(
    plan: StableReleaseRoutePlan,
    *,
    observations: Iterable[ReleaseRouteObservation],
    v1_provenance: Mapping[str, object],
    runtime_handoff: Mapping[str, object],
    consumer_acceptance: Mapping[str, object],
    fallback_drill: Mapping[str, object],
) -> dict[str, object]:
    """Evaluate exactly one release plan; return compact, secret-free evidence."""
    values = tuple(observations)
    readiness = evaluate_release_readiness(plan, values)
    v1_ok, v1_hash = _validate_v1_provenance(plan, v1_provenance)
    runtime_ok, runtime_hash, python_image, rust_image = _validate_runtime_evidence(
        plan, runtime_handoff
    )
    acceptance_ok, acceptance_hash = _validate_acceptance(plan, consumer_acceptance)
    fallback_ok, fallback_hash = _validate_fallback_drill(plan, fallback_drill)
    readiness_ok = (
        readiness.ready
        and readiness.fallback_count == 0
        and _measurements_are_current(plan, values)
    )
    report = CertificationReport(
        scope="qdl.phase105.stable-release",
        gates=(
            _gate("release_route_readiness", readiness_ok, readiness.route_plan_sha256),
            _gate("v1_fallback_provenance", v1_ok, v1_hash),
            _gate("runtime_handoff", runtime_ok, runtime_hash),
            _gate("consumer_v2_primary", acceptance_ok, acceptance_hash),
            _gate("v1_fallback_return", fallback_ok, fallback_hash),
        ),
    )
    return {
        "schema": "qdl.phase105.stable-release-certification.v1",
        "status": "PASS" if report.production_eligible else "BLOCKED",
        "release_route_plan_sha256": plan.digest,
        "v2_python_image": python_image,
        "v2_rust_image": rust_image,
        "readiness": asdict(readiness),
        "gates": [
            {"gate_id": gate.gate_id, "status": gate.status.value, "evidence": gate.evidence}
            for gate in report.gates
        ],
        "input_sha256": {
            "v1_provenance": v1_hash,
            "runtime_handoff": runtime_hash,
            "consumer_acceptance": acceptance_hash,
            "fallback_drill": fallback_hash,
            "observations": _canonical_sha256([item.public_record() for item in values]),
        },
    }
