#!/usr/bin/env python3
"""Prepare a complete Phase 11.5 V2 release artifact from real metadata.

This tool is read-only toward providers and starts no Data Layer role.  It
creates a JSON artifact only at the caller-provided output path; it never
changes Kafka, Redis, SQLite, V1, consumer routing, alpha behavior or orders.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from qdl.consumer.universal_release import (
    UniversalReleaseCoverage,
    UniversalReleasePolicy,
    build_universal_release_manifest,
)
from qdl.certification.phase115_universal_release import (
    build_universal_no_order_acceptance_scope,
)
from qdl.demand import ActiveDemandSourceRegistry, converge_active_demand
from qdl.runtime.l2_demand import L2DemandPlan, build_l2_demand_plan
from scripts.phase112_universal_realtime_provider_admission import (
    DEFAULT_EXECUTION_ALPHA_ROOT,
    DEFAULT_SOURCE_REGISTRY,
    DEFAULT_TRADING_SYSTEM_ROOT,
    QDL_ROOT,
    _build_plan,
)


DEFAULT_POLICY = QDL_ROOT / "config/v2/universal-release-policy.yaml"
DEFAULT_REFERENCE_EVIDENCE = (
    QDL_ROOT / "upgrade/evidence/phase113-universal-warmup-reference-admission.json"
)
DEFAULT_REALTIME_EVIDENCE = (
    QDL_ROOT / "upgrade/evidence/phase112-universal-realtime-provider-admission.json"
)
DEFAULT_ADMISSION_EVIDENCE = (
    QDL_ROOT / "upgrade/evidence/phase115-active-demand-provider-admission.json"
)
DEFAULT_L2_EVIDENCE = (
    QDL_ROOT / "upgrade/evidence/phase114-l2-real-provider-capture.json"
)
DEFAULT_OUTPUT = QDL_ROOT / "upgrade/evidence/phase115-universal-release-preflight.json"


def _canonical_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _read_mapping(path: Path, *, label: str) -> Mapping[str, Any]:
    if not path.is_file():
        raise ValueError(f"{label} evidence is missing: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"{label} evidence is not JSON: {path}") from error
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} evidence must be a JSON object: {path}")
    return value


def _metadata(value: object, *, label: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} metadata_sha256 is missing")
    result = {str(key): str(item) for key, item in value.items()}
    if not result or any(
        len(item) != 64 or any(character not in "0123456789abcdef" for character in item)
        for item in result.values()
    ):
        raise ValueError(f"{label} metadata_sha256 is invalid")
    return dict(sorted(result.items()))


def _require_read_only(
    report: Mapping[str, Any],
    *,
    label: str,
    schema: str,
    provenance: str | None,
) -> None:
    if report.get("schema") != schema or report.get("status") != "PASS":
        raise ValueError(f"{label} evidence did not pass its declared schema")
    if provenance is not None and report.get("provenance") != provenance:
        raise ValueError(f"{label} evidence provenance is not real-provider read-only")
    if int(report.get("runtime_mutations", -1)) != 0 or int(report.get("production_writes", -1)) != 0:
        raise ValueError(f"{label} evidence has runtime or production writes")


def _require_identity(
    report: Mapping[str, Any],
    *,
    label: str,
    inventory_sha256: str,
    metadata_sha256: Mapping[str, str],
) -> None:
    if report.get("inventory_sha256") != inventory_sha256:
        raise ValueError(f"{label} evidence inventory differs from the current admission")
    if _metadata(report.get("metadata_sha256"), label=label) != dict(sorted(metadata_sha256.items())):
        raise ValueError(f"{label} evidence metadata differs from the current admission")


def _l2_binding_identity(item: Mapping[str, Any]) -> tuple[object, ...]:
    required = (
        "venue", "market", "product_type", "native_symbol", "instrument_uid", "instrument_id",
        "requirement_ids",
    )
    if any(name not in item for name in required) or not isinstance(item["requirement_ids"], list):
        raise ValueError("L2 evidence binding is incomplete")
    requirement_ids = tuple(sorted(str(value) for value in item["requirement_ids"]))
    if not requirement_ids or len(requirement_ids) != len(set(requirement_ids)):
        raise ValueError("L2 evidence binding has invalid requirement IDs")
    return tuple(str(item[name]) for name in required[:-1]) + (requirement_ids,)


def validate_evidence_bundle(
    *,
    inventory_sha256: str,
    admission_payload: Mapping[str, Any],
    metadata_sha256: Mapping[str, str],
    realtime_binding_ids: set[str],
    reference_pairs: set[tuple[str, str]],
    l2_plan: L2DemandPlan | None,
    admission_path: Path,
    realtime_path: Path,
    reference_path: Path,
    l2_path: Path | None,
) -> dict[str, str | None]:
    """Reject evidence from a different demand/metadata generation.

    The release compiler must never combine individually passing observations
    from different active-demand snapshots.  Paths are inputs only; their
    contents are reduced to canonical digests in the resulting manifest.
    """

    admission = _read_mapping(admission_path, label="admission")
    if admission != dict(admission_payload):
        raise ValueError("admission evidence differs from the current provider admission")
    admission_digest = _canonical_digest(admission)
    if admission.get("schema") != "qdl.v2.active-demand-provider-admission.v1":
        raise ValueError("admission evidence schema is invalid")
    if admission.get("inventory_sha256") != inventory_sha256:
        raise ValueError("admission evidence inventory differs from the current admission")
    if _metadata(admission.get("metadata_sha256"), label="admission") != dict(sorted(metadata_sha256.items())):
        raise ValueError("admission evidence metadata differs from the current admission")

    realtime = _read_mapping(realtime_path, label="realtime")
    _require_read_only(
        realtime,
        label="realtime",
        schema="qdl.phase112.universal-realtime-provider-admission.v1",
        provenance="REAL_PROVIDER_DIRECT_READ_ONLY",
    )
    _require_identity(
        realtime,
        label="realtime",
        inventory_sha256=inventory_sha256,
        metadata_sha256=metadata_sha256,
    )
    if realtime.get("admission_evidence_sha256") != admission_digest:
        raise ValueError("realtime evidence was not bound to the current admission")
    rows = realtime.get("bindings")
    if not isinstance(rows, list):
        raise ValueError("realtime evidence bindings are missing")
    observed_realtime = {str(item.get("binding_id")) for item in rows if isinstance(item, Mapping)}
    if len(observed_realtime) != len(rows) or observed_realtime != realtime_binding_ids:
        raise ValueError("realtime evidence bindings differ from the current plan")
    if int(realtime.get("raw_provider_frames_persisted", -1)) != 0:
        raise ValueError("realtime evidence retained raw provider frames")

    reference = _read_mapping(reference_path, label="reference")
    _require_read_only(
        reference,
        label="reference",
        schema="qdl.phase113.universal-warmup-reference-admission.v1",
        provenance="REAL_PROVIDER_READ_ONLY",
    )
    _require_identity(
        reference,
        label="reference",
        inventory_sha256=inventory_sha256,
        metadata_sha256=metadata_sha256,
    )
    if reference.get("raw_payload_persisted") is not False or int(reference.get("provider_writes", -1)) != 0:
        raise ValueError("reference evidence retained raw payloads or wrote to a provider")
    reference_rows = reference.get("reference_results")
    if not isinstance(reference_rows, list):
        raise ValueError("reference evidence results are missing")
    available_pairs = {
        (str(item.get("instrument_uid")), str(item.get("product")))
        for item in reference_rows
        if isinstance(item, Mapping) and item.get("expected") == "AVAILABLE" and item.get("status") == "OK"
    }
    if not reference_pairs <= available_pairs:
        raise ValueError("reference evidence missed an admitted requirement")

    if l2_plan is None:
        if l2_path is not None:
            raise ValueError("L2 evidence was provided without an active L2 plan")
        l2_digest: str | None = None
    else:
        if l2_path is None:
            raise ValueError("active L2 demand requires L2 evidence")
        l2 = _read_mapping(l2_path, label="L2")
        _require_read_only(
            l2,
            label="L2",
            schema="qdl.phase114.l2-real-provider-capture.v1",
            provenance=None,
        )
        if not isinstance(l2.get("provenance"), list) or not l2["provenance"]:
            raise ValueError("L2 evidence must retain its real-provider protocol provenance")
        _require_identity(
            l2,
            label="L2",
            inventory_sha256=inventory_sha256,
            metadata_sha256=metadata_sha256,
        )
        if l2.get("admission_sha256") != admission_digest:
            raise ValueError("L2 evidence was not bound to the current admission")
        if int(l2.get("raw_provider_bytes_persisted", -1)) != 0:
            raise ValueError("L2 evidence retained raw provider bytes")
        observed_l2_raw = l2.get("required_bindings")
        if not isinstance(observed_l2_raw, list):
            raise ValueError("L2 evidence has no required binding set")
        observed_l2 = {
            _l2_binding_identity(item) for item in observed_l2_raw if isinstance(item, Mapping)
        }
        expected_l2 = {
            _l2_binding_identity({
                "venue": binding.venue,
                "market": binding.market,
                "product_type": binding.product_type,
                "native_symbol": binding.native_symbol,
                "instrument_uid": binding.instrument_uid,
                "instrument_id": binding.instrument_id,
                "requirement_ids": list(binding.requirement_ids),
            })
            for binding in l2_plan.bindings
        }
        if len(observed_l2) != len(observed_l2_raw) or observed_l2 != expected_l2:
            raise ValueError("L2 evidence bindings differ from the current demand plan")
        l2_digest = _canonical_digest(l2)

    return {
        "admission_evidence_sha256": admission_digest,
        "realtime_evidence_sha256": _canonical_digest(realtime),
        "reference_evidence_sha256": _canonical_digest(reference),
        "l2_evidence_sha256": l2_digest,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-registry", type=Path, default=DEFAULT_SOURCE_REGISTRY)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--admission-evidence", type=Path, default=DEFAULT_ADMISSION_EVIDENCE)
    parser.add_argument("--realtime-evidence", type=Path, default=DEFAULT_REALTIME_EVIDENCE)
    parser.add_argument("--reference-evidence", type=Path, default=DEFAULT_REFERENCE_EVIDENCE)
    parser.add_argument("--l2-evidence", type=Path, default=DEFAULT_L2_EVIDENCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--repository-root", type=Path, default=QDL_ROOT)
    parser.add_argument("--execution-alpha-root", type=Path, default=DEFAULT_EXECUTION_ALPHA_ROOT)
    parser.add_argument("--trading-system-root", type=Path, default=DEFAULT_TRADING_SYSTEM_ROOT)
    parser.add_argument("--metadata-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--metadata-attempts", type=int, default=3)
    parser.add_argument("--release-revision", type=int, default=1)
    args = parser.parse_args(argv)
    if args.metadata_timeout_seconds <= 0 or args.metadata_attempts < 1:
        raise ValueError("metadata timeout/attempt bounds are invalid")
    provider = _build_plan(
        source_registry=args.source_registry,
        repository_root=args.repository_root,
        execution_alpha_root=args.execution_alpha_root,
        trading_system_root=args.trading_system_root,
        metadata_timeout_seconds=args.metadata_timeout_seconds,
        metadata_attempts=args.metadata_attempts,
    )
    if provider.inventory is None or provider.admission is None:
        raise RuntimeError("provider admission omitted authenticated inventory")
    registry = ActiveDemandSourceRegistry.load(args.source_registry)
    convergence = converge_active_demand(
        provider.inventory, provider.admission, registry.admission_policy
    )
    book_demand = any(
        row.state == "ADMITTED" and row.feed in {"BOOK_SNAPSHOT", "BOOK_DELTA"}
        for row in provider.admission.rows
    )
    l2_plan = (
        build_l2_demand_plan(
            inventory=provider.inventory,
            admission=provider.admission,
            convergence=convergence,
        )
        if book_demand
        else None
    )
    policy = UniversalReleasePolicy.load(args.policy, manifest_root=args.repository_root)
    evidence = validate_evidence_bundle(
        inventory_sha256=provider.inventory.manifest_sha256,
        admission_payload=provider.admission.report_payload(),
        metadata_sha256=provider.admission.metadata_sha256,
        realtime_binding_ids={item.binding_id for item in provider.bindings},
        reference_pairs={
            (str(row.instrument_uid), str(row.feed))
            for row in provider.admission.rows
            if row.state == "ADMITTED"
            and row.instrument_uid is not None
            and row.feed in {"FUNDING_RATE", "BASIS"}
        },
        l2_plan=l2_plan,
        admission_path=args.admission_evidence,
        realtime_path=args.realtime_evidence,
        reference_path=args.reference_evidence,
        l2_path=args.l2_evidence if l2_plan is not None else None,
    )
    coverage = UniversalReleaseCoverage.from_phase_plans(
        inventory=provider.inventory,
        admission=provider.admission,
        convergence=convergence,
        realtime_plan=provider.plan,
        realtime_evidence_sha256=str(evidence["realtime_evidence_sha256"]),
        reference_evidence_sha256=str(evidence["reference_evidence_sha256"]),
        l2_plan=l2_plan,
        l2_evidence_sha256=(
            str(evidence["l2_evidence_sha256"])
            if evidence["l2_evidence_sha256"] is not None else None
        ),
    )
    manifest = build_universal_release_manifest(
        policy=policy,
        inventory=provider.inventory,
        admission=provider.admission,
        convergence=convergence,
        coverage=coverage,
        release_revision=args.release_revision,
    )
    acceptance = build_universal_no_order_acceptance_scope(manifest)
    payload = {
        "schema": "qdl.phase115.universal-release-preflight.v1",
        "status": "PREPARED",
        "provenance": "REAL_PROVIDER_METADATA_READ_ONLY",
        "release_manifest": manifest.canonical_mapping(),
        "release_summary": manifest.report_payload(),
        "no_order_acceptance": acceptance.report_payload(),
        "evidence": evidence,
        "runtime_mutations": 0,
        "order_actions": 0,
        "raw_provider_bytes_persisted": 0,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": payload["status"],
        "manifest_sha256": manifest.digest,
        "product_count": len(manifest.products),
        "exclusion_count": len(manifest.exclusions),
        "output": str(args.output),
        "runtime_mutations": 0,
        "order_actions": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
