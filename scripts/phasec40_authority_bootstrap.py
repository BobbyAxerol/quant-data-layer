#!/usr/bin/env python3
"""Prepare and apply the exact C40 production authority bootstrap packet.

The command is plan-only by default.  ``apply`` inserts one immutable
prerequisite bundle and the exact initial ``RUST_SHADOW`` slice rows in a
single transaction.  Existing identical rows make the operation idempotent;
any conflicting row fails closed.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
from typing import Any, Mapping
import uuid


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.control.operator_env import require_control_admin_dsn
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    AuthorityPromotionScope,
    StableAcquisitionPlan,
)


SCHEMA = "qdl.c40.authority-bootstrap-packet.v1"
OWNER_ID = "qdl-v2-rust-shadow"
SIGNATURE_IDENTITY = "BobbyAxerol/quant-data-layer"
REQUIRED_EVIDENCE = {
    "provider_provenance",
    "semantic_mismatches",
    "open_gaps",
    "duplicate_external_effects",
    "consumer_errors",
    "execution_state_changed",
}
R1_PRECANARY_SCHEMA = "qdl.r1.pre-canary-admission.v1"
R1_PRECANARY_FIELDS = {
    "schema", "status", "issued_at_ns", "expires_at_ns", "provider_provenance",
    "production_mutations", "execution_state_changed", "semantic_mismatches",
    "open_gaps", "duplicate_external_effects", "consumer_errors",
    "candidate_runtime_parity_status", "candidate_source_commit",
    "candidate_image_digest", "candidate_image_inspect_sha256",
    "rollback_rust_image_digest", "promotion_scope_digest", "contract_sha256",
    "partition_plan_sha256", "release_artifact_sha256", "sbom_sha256",
    "rollback_manifest_sha256", "reference_runtime_image_digest",
    "reference_source_commit", "reference_parity_sha256",
    "reference_captured_at_ns", "sample_count",
}


def _encoded(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_encoded(value)).hexdigest()


def _file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _image_digest(value: str) -> str:
    text = value.strip()
    raw = text.removeprefix("sha256:")
    if not text.startswith("sha256:") or len(raw) != 64 or any(
        char not in "0123456789abcdef" for char in raw
    ):
        raise ValueError("Rust image digest must be immutable SHA-256")
    return text


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    return value


def _validate_r1_precanary_admission(
    value: Mapping[str, Any], *, rust_image_digest: str, now_ns: int
) -> dict[str, Any]:
    if set(value) != R1_PRECANARY_FIELDS or value.get("schema") != R1_PRECANARY_SCHEMA:
        raise ValueError("R1 pre-canary admission schema/fields are invalid")
    issued = int(value.get("issued_at_ns", 0))
    expires = int(value.get("expires_at_ns", 0))
    if not issued <= now_ns < expires or expires - issued > 7_200_000_000_000:
        raise ValueError("R1 pre-canary admission window is inactive")
    reference_captured = int(value.get("reference_captured_at_ns", 0))
    if reference_captured <= 0 or reference_captured > now_ns or now_ns - reference_captured > 1_800_000_000_000:
        raise ValueError("R1 pre-canary reference parity is stale")
    if (
        value.get("status") != "PASS"
        or value.get("provider_provenance") != "REAL"
        or int(value.get("production_mutations", -1)) != 0
        or value.get("execution_state_changed") is not False
        or value.get("candidate_runtime_parity_status") != "PENDING_R1_CANARY"
        or value.get("candidate_image_digest") != rust_image_digest
        or value.get("reference_runtime_image_digest") == rust_image_digest
        or int(value.get("sample_count", 0)) < 96
        or any(int(value.get(name, -1)) != 0 for name in (
            "semantic_mismatches", "open_gaps", "duplicate_external_effects", "consumer_errors",
        ))
    ):
        raise ValueError("R1 pre-canary admission is not clean/candidate-bound")
    for name in (
        "candidate_image_inspect_sha256", "release_artifact_sha256", "sbom_sha256",
        "rollback_manifest_sha256", "reference_parity_sha256", "promotion_scope_digest",
        "contract_sha256", "partition_plan_sha256",
    ):
        candidate = str(value.get(name, ""))
        if len(candidate) != 64 or any(char not in "0123456789abcdef" for char in candidate):
            raise ValueError(f"R1 pre-canary admission digest is invalid: {name}")
    for name in ("candidate_source_commit", "reference_source_commit"):
        candidate = str(value.get(name, ""))
        if not 7 <= len(candidate) <= 64 or any(char not in "0123456789abcdef" for char in candidate):
            raise ValueError(f"R1 pre-canary admission source commit is invalid: {name}")
    for name in ("rollback_rust_image_digest", "reference_runtime_image_digest"):
        candidate = str(value.get(name, ""))
        if not candidate.startswith("sha256:") or len(candidate) != 71 or any(char not in "0123456789abcdef" for char in candidate[7:]):
            raise ValueError(f"R1 pre-canary admission image digest is invalid: {name}")
    return {
        "provider_provenance": "REAL",
        "semantic_mismatches": 0,
        "open_gaps": 0,
        "duplicate_external_effects": 0,
        "consumer_errors": 0,
        "execution_state_changed": False,
    }


def _validate_acceptance(
    value: Mapping[str, Any], *, rust_image_digest: str, now_ns: int
) -> dict[str, Any]:
    if value.get("schema") == R1_PRECANARY_SCHEMA:
        return _validate_r1_precanary_admission(
            value, rust_image_digest=rust_image_digest, now_ns=now_ns
        )
    # C39 uses a larger report schema. Bind only the narrow, immutable facts
    # that authorize this bootstrap; never copy credentials or raw payloads.
    if value.get("schema") == "qdl.c39.final-acceptance.v1":
        health = value.get("market_data_health", {})
        details = health.get("details", {}) if isinstance(health, Mapping) else {}
        alpha = value.get("alpha_smoke", {})
        provider = value.get("provider_cache", {})
        cache = value.get("cache", {})
        soak = value.get("post_soak", {})
        execution = value.get("execution_baseline_unchanged", {})
        if (
            value.get("rust_image_unchanged") != rust_image_digest
            or health.get("top") != "READY"
            or health.get("service") != "READY"
            or int(details.get("demanded_v2_slices", 0)) <= 0
            or details.get("demanded_v2_slices") != details.get("ready_v2_slices")
            or int(details.get("unhealthy_v2_slices", -1)) != 0
            or details.get("reported_unhealthy_slices") != []
            or details.get("unreported_unhealthy_slices") != 0
            or alpha.get("coverage") != "FULL"
            or alpha.get("acknowledged") is not True
            or alpha.get("execution_enabled") is not False
            or alpha.get("container_removed") is not True
            or provider.get("providers") != ["BINANCE_DIRECT", "OKX_DIRECT"]
            or provider.get("present") != provider.get("demanded_slices")
            or provider.get("bars_closed") is not True
            or provider.get("trades_subsecond_at_sample") is not True
            or int(cache.get("sqlite_quarantine", -1)) != 0
            or int(soak.get("disk_growth_bytes", -1)) != 0
            or int(soak.get("new_market_data_log_lines", -1)) != 0
            or not isinstance(execution, Mapping)
            or int(execution.get("order.inbound", -1)) != 0
            or int(execution.get("commands.execution.paper", -1)) != 0
        ):
            raise ValueError("C39 final acceptance does not satisfy C40 bootstrap")
        return {
            "provider_provenance": "REAL",
            "semantic_mismatches": 0,
            "open_gaps": 0,
            "duplicate_external_effects": 0,
            "consumer_errors": 0,
            "execution_state_changed": False,
        }
    status = str(value.get("status", ""))
    provenance = value.get("provider_provenance")
    if provenance is None:
        provenance = value.get("provenance")
    if provenance is None:
        provenance = "REAL" if "PASS" in status else ""
    evidence = {
        "provider_provenance": str(provenance),
        "semantic_mismatches": int(value.get("semantic_mismatches", 0)),
        "open_gaps": int(value.get("open_gaps", 0)),
        "duplicate_external_effects": int(
            value.get("duplicate_external_effects", 0)
        ),
        "consumer_errors": int(value.get("consumer_errors", 0)),
        "execution_state_changed": bool(value.get("execution_state_changed", False)),
    }
    if set(evidence) != REQUIRED_EVIDENCE or (
        "PASS" not in status
        or evidence["provider_provenance"] not in {"REAL", "REAL_PROVIDER_READ_ONLY"}
        or any(
            evidence[name] != 0
            for name in (
                "semantic_mismatches",
                "open_gaps",
                "duplicate_external_effects",
                "consumer_errors",
            )
        )
        or evidence["execution_state_changed"]
    ):
        raise ValueError("C40 bootstrap requires clean real-provider acceptance")
    return evidence


def prepare_packet(
    *,
    catalog_path: Path,
    acquisition_path: Path,
    promotion_scope_path: Path,
    production_core_manifest_path: Path,
    contract_path: Path,
    sbom_path: Path,
    rollback_manifest_path: Path,
    acceptance_path: Path,
    rust_image_digest: str,
    actor: str,
    issued_at_ns: int | None = None,
    validity_seconds: int = 86_400,
) -> dict[str, Any]:
    if not actor.strip() or not 900 <= validity_seconds <= 604_800:
        raise ValueError("bootstrap actor/validity is invalid")
    image_digest = _image_digest(rust_image_digest)
    catalog = StableSourceCatalog.load(catalog_path)
    acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
    scope = AuthorityPromotionScope.load(promotion_scope_path, catalog=catalog)
    manifest = _load_json(production_core_manifest_path)
    if (
        manifest.get("schema") != "qdl.v2.production-core-bundle.v1"
        or int(manifest.get("promotion_scope_revision", 0)) != scope.revision
        or manifest.get("promotion_scope_digest") != scope.digest()
        or int(manifest.get("promotion_binding_count", 0)) != len(scope.binding_ids)
        or int(manifest.get("worker_count", 0)) != 3
    ):
        raise ValueError("production core manifest differs from promotion scope")
    source_by_id = {item.binding_id: item for item in catalog.bindings}
    acquisition_by_id = {item.binding_id: item for item in acquisition.bindings}
    active_crypto = {
        item.binding_id
        for item in acquisition.bindings
        if item.enabled and item.runtime in {"BINANCE", "OKX"}
    }
    if set(scope.binding_ids) != active_crypto:
        raise ValueError("promotion scope differs from active Binance/OKX bindings")
    issued_ns = time.time_ns() if issued_at_ns is None else int(issued_at_ns)
    if issued_ns <= 0:
        raise ValueError("bootstrap issued time must be positive")
    evidence = _validate_acceptance(
        _load_json(acceptance_path), rust_image_digest=image_digest, now_ns=issued_ns
    )
    issued = datetime.fromtimestamp(issued_ns / 1_000_000_000, timezone.utc)
    expires = issued + timedelta(seconds=validity_seconds)
    candidate = {
        "rust_image_digest": image_digest,
        "contract_digest": _file_digest(contract_path),
        "partition_plan_digest": _file_digest(acquisition_path),
        "promotion_scope_digest": scope.digest(),
        "production_core_manifest_digest": _file_digest(
            production_core_manifest_path
        ),
    }
    candidate_digest = _digest(candidate)
    prerequisite_evidence = {
        **evidence,
        "acceptance_sha256": _file_digest(acceptance_path),
        "production_core_manifest_sha256": candidate[
            "production_core_manifest_digest"
        ],
        "promotion_scope_sha256": _file_digest(promotion_scope_path),
    }
    bundle_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"qdl-c40:{candidate_digest}"))
    slices = []
    for binding_id in sorted(scope.binding_ids):
        source = source_by_id[binding_id]
        acquisition_binding = acquisition_by_id[binding_id]
        identity = source.instrument.identity
        native = source.instrument.native_symbol.lower()
        partition_plan_epoch = int(manifest["partition_plan_epoch"])
        slice_id = (
            f"production/{identity.venue.lower()}/{identity.market.lower()}/"
            f"{identity.product_type.value.lower()}/{source.feed.value.lower()}/"
            f"plan-{partition_plan_epoch}/{native}"
        )
        slices.append({
            "slice_id": slice_id,
            "binding_id": binding_id,
            "environment": "production",
            "venue": identity.venue,
            "market": identity.market,
            "product_type": identity.product_type.value,
            "feed": source.feed.value,
            "partition_plan_epoch": partition_plan_epoch,
            "partition_id": source.binding_id,
            "schema_major": 2,
            "state": "RUST_SHADOW",
            "authority_revision": 1,
            "owner_id": OWNER_ID,
            "lease_epoch": 1,
            "candidate_digest": candidate_digest,
            "artifact_image_digest": image_digest,
            "sbom_digest": _file_digest(sbom_path),
            "signature_identity": SIGNATURE_IDENTITY,
            "contract_digest": candidate["contract_digest"],
            "normalizer_version": source.normalizer_version,
            "adapter_version": source.adapter_version,
            "config_revision": str(acquisition.revision),
            "instrument_catalog_revision": str(catalog.catalog_revision),
            "source_policy_revision": str(catalog.source_policy_revision),
            "partition_plan_digest": candidate["partition_plan_digest"],
            "rollback_manifest_digest": _file_digest(rollback_manifest_path),
        })
    packet = {
        "schema": SCHEMA,
        "packet_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"qdl-c40-packet:{candidate_digest}")),
        "issued_at_ns": issued_ns,
        "expires_at_ns": int(expires.timestamp() * 1_000_000_000),
        "actor": actor,
        "candidate": candidate,
        "candidate_digest": candidate_digest,
        "prerequisite_bundle": {
            "bundle_id": bundle_id,
            "policy_revision": scope.revision,
            "decision": "GO",
            "evidence": prerequisite_evidence,
            "evidence_sha256": _digest(prerequisite_evidence),
            "issued_by": actor,
            "issued_at": issued.isoformat(),
            "expires_at": expires.isoformat(),
        },
        "slices": slices,
    }
    # Parse the final payload once more so confirmation binds exact serialized data.
    validate_packet(packet, now_ns=issued_ns)
    return packet


def validate_packet(raw: Mapping[str, Any], *, now_ns: int | None = None) -> None:
    if set(raw) != {
        "schema", "packet_id", "issued_at_ns", "expires_at_ns", "actor",
        "candidate", "candidate_digest", "prerequisite_bundle", "slices",
    } or raw.get("schema") != SCHEMA:
        raise ValueError("bootstrap packet fields/schema are invalid")
    current = time.time_ns() if now_ns is None else int(now_ns)
    if not int(raw["issued_at_ns"]) <= current < int(raw["expires_at_ns"]):
        raise ValueError("bootstrap packet approval window is inactive")
    if _digest(raw["candidate"]) != raw["candidate_digest"]:
        raise ValueError("bootstrap candidate digest differs")
    bundle = raw["prerequisite_bundle"]
    if (
        bundle.get("decision") != "GO"
        or bundle.get("evidence_sha256") != _digest(bundle.get("evidence", {}))
        or not str(bundle.get("issued_by", "")).strip()
    ):
        raise ValueError("bootstrap prerequisite bundle is invalid")
    slices = raw["slices"]
    if not isinstance(slices, list) or len(slices) != 12:
        raise ValueError("bootstrap requires exactly twelve crypto slices")
    ids = [str(item.get("slice_id", "")) for item in slices]
    bindings = [str(item.get("binding_id", "")) for item in slices]
    if len(set(ids)) != 12 or len(set(bindings)) != 12:
        raise ValueError("bootstrap slice/binding identities are not unique")
    if any(
        item.get("state") != "RUST_SHADOW"
        or item.get("venue") not in {"BINANCE", "OKX"}
        or item.get("market") not in {"USDM", "SWAP"}
        or item.get("feed") not in {"TRADE", "QUOTE", "BAR"}
        or item.get("candidate_digest") != raw["candidate_digest"]
        or item.get("artifact_image_digest") != raw["candidate"]["rust_image_digest"]
        for item in slices
    ):
        raise ValueError("bootstrap contains non-crypto or inconsistent slice")


_BUNDLE_INSERT = """
INSERT INTO qdl_production_prerequisite_bundles (
    bundle_id, candidate_digest, policy_revision, decision, evidence,
    evidence_sha256, issued_by, issued_at, expires_at
) VALUES ($1::uuid,$2,$3,$4,$5::jsonb,$6,$7,$8::timestamptz,$9::timestamptz)
ON CONFLICT (bundle_id) DO NOTHING
"""
_SLICE_INSERT = """
INSERT INTO qdl_authority_slices (
    slice_id,environment,venue,market,product_type,feed,partition_plan_epoch,
    partition_id,schema_major,state,authority_revision,owner_id,lease_epoch,
    candidate_digest,artifact_image_digest,sbom_digest,signature_identity,
    contract_digest,normalizer_version,adapter_version,config_revision,
    instrument_catalog_revision,source_policy_revision,partition_plan_digest,
    rollback_manifest_digest
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
    $20,$21,$22,$23,$24,$25
) ON CONFLICT (slice_id) DO NOTHING
"""


async def apply_packet(packet: Mapping[str, Any], dsn: str) -> dict[str, Any]:
    try:
        import asyncpg
    except ImportError as error:
        raise RuntimeError("authority bootstrap apply requires asyncpg") from error
    connection = await asyncpg.connect(
        dsn=dsn,
        command_timeout=15,
        server_settings={"application_name": "qdl-c40-authority-bootstrap"},
    )
    try:
        async with connection.transaction():
            mutations = 0
            bundle = packet["prerequisite_bundle"]
            bundle_status = await connection.execute(
                _BUNDLE_INSERT,
                bundle["bundle_id"], packet["candidate_digest"],
                bundle["policy_revision"], bundle["decision"],
                json.dumps(bundle["evidence"], sort_keys=True),
                bundle["evidence_sha256"], bundle["issued_by"],
                datetime.fromisoformat(bundle["issued_at"]),
                datetime.fromisoformat(bundle["expires_at"]),
            )
            mutations += int(bundle_status.rsplit(" ", 1)[-1])
            existing_bundle = await connection.fetchrow(
                "SELECT candidate_digest,policy_revision,decision,evidence,"
                "evidence_sha256,issued_by,issued_at,expires_at "
                "FROM qdl_production_prerequisite_bundles WHERE bundle_id=$1::uuid",
                bundle["bundle_id"],
            )
            expected_bundle = {
                "candidate_digest": packet["candidate_digest"],
                "policy_revision": bundle["policy_revision"],
                "decision": bundle["decision"],
                "evidence": bundle["evidence"],
                "evidence_sha256": bundle["evidence_sha256"],
                "issued_by": bundle["issued_by"],
                "issued_at": datetime.fromisoformat(bundle["issued_at"]),
                "expires_at": datetime.fromisoformat(bundle["expires_at"]),
            }
            actual_bundle = dict(existing_bundle or {})
            if isinstance(actual_bundle.get("evidence"), str):
                actual_bundle["evidence"] = json.loads(actual_bundle["evidence"])
            if actual_bundle != expected_bundle:
                raise RuntimeError("existing prerequisite bundle conflicts with packet")
            for item in packet["slices"]:
                values = [
                    item[name] for name in (
                        "slice_id", "environment", "venue", "market",
                        "product_type", "feed", "partition_plan_epoch",
                        "partition_id", "schema_major", "state",
                        "authority_revision", "owner_id", "lease_epoch",
                        "candidate_digest", "artifact_image_digest", "sbom_digest",
                        "signature_identity", "contract_digest",
                        "normalizer_version", "adapter_version", "config_revision",
                        "instrument_catalog_revision", "source_policy_revision",
                        "partition_plan_digest", "rollback_manifest_digest",
                    )
                ]
                slice_status = await connection.execute(_SLICE_INSERT, *values)
                mutations += int(slice_status.rsplit(" ", 1)[-1])
            rows = await connection.fetch(
                "SELECT slice_id,environment,venue,market,product_type,feed,"
                "partition_plan_epoch,partition_id,schema_major,state,"
                "authority_revision,owner_id,lease_epoch,candidate_digest,"
                "artifact_image_digest,sbom_digest,signature_identity,"
                "contract_digest,normalizer_version,adapter_version,config_revision,"
                "instrument_catalog_revision,source_policy_revision,"
                "partition_plan_digest,rollback_manifest_digest "
                "FROM qdl_authority_slices WHERE slice_id=ANY($1::text[]) ORDER BY slice_id",
                [item["slice_id"] for item in packet["slices"]],
            )
            expected_rows = sorted(packet["slices"], key=lambda item: item["slice_id"])
            actual_rows = [dict(row) for row in rows]
            comparable = [
                {key: item[key] for key in actual_rows[0]} if actual_rows else {}
                for item in expected_rows
            ]
            if actual_rows != comparable:
                raise RuntimeError("existing authority slice rows conflict with packet")
    finally:
        await connection.close()
    return {
        "bundle_id": packet["prerequisite_bundle"]["bundle_id"],
        "slice_count": len(packet["slices"]),
        "state": "RUST_SHADOW",
        "production_mutations": mutations,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--catalog", type=Path, required=True)
    prepare.add_argument("--acquisition", type=Path, required=True)
    prepare.add_argument("--promotion-scope", type=Path, required=True)
    prepare.add_argument("--production-core-manifest", type=Path, required=True)
    prepare.add_argument("--contract", type=Path, required=True)
    prepare.add_argument("--sbom", type=Path, required=True)
    prepare.add_argument("--rollback-manifest", type=Path, required=True)
    prepare.add_argument("--acceptance", type=Path, required=True)
    prepare.add_argument("--rust-image-digest", required=True)
    prepare.add_argument("--actor", required=True)
    prepare.add_argument("--validity-seconds", type=int, default=86_400)
    prepare.add_argument("--output", type=Path, required=True)
    apply = sub.add_parser("apply")
    apply.add_argument("--packet", type=Path, required=True)
    apply.add_argument("--apply", action="store_true")
    apply.add_argument("--confirm")
    args = parser.parse_args()
    if args.command == "prepare":
        packet = prepare_packet(
            catalog_path=args.catalog,
            acquisition_path=args.acquisition,
            promotion_scope_path=args.promotion_scope,
            production_core_manifest_path=args.production_core_manifest,
            contract_path=args.contract,
            sbom_path=args.sbom,
            rollback_manifest_path=args.rollback_manifest,
            acceptance_path=args.acceptance,
            rust_image_digest=args.rust_image_digest,
            actor=args.actor,
            validity_seconds=args.validity_seconds,
        )
        args.output.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n")
        print(json.dumps({
            "status": "PASS", "packet_sha256": _digest(packet),
            "confirmation_token": f"BOOTSTRAP_C40_{_digest(packet)[:16]}",
            "slice_count": len(packet["slices"]), "production_mutations": 0,
        }, sort_keys=True))
        return 0
    packet = _load_json(args.packet)
    validate_packet(packet)
    digest = _digest(packet)
    plan = {
        "status": "PASS", "packet_sha256": digest,
        "confirmation_token": f"BOOTSTRAP_C40_{digest[:16]}",
        "slice_count": len(packet["slices"]), "production_mutations": 0,
    }
    if not args.apply:
        print(json.dumps(plan, sort_keys=True))
        return 0
    if args.confirm != plan["confirmation_token"]:
        raise RuntimeError("bootstrap confirmation token differs from packet")
    result = asyncio.run(apply_packet(packet, require_control_admin_dsn()))
    print(json.dumps({**plan, **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
