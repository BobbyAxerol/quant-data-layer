#!/usr/bin/env python3
"""Prepare a review-only shared Rust-primary V2 runtime packet.

This generator writes a fresh non-secret runtime bundle plus one immutable
handoff packet. It never invokes Docker, Kafka, Redis, PostgreSQL, a provider,
or a consumer endpoint. An operator must separately approve the exact packet
before any topic/ACL/service/consumer-route mutation is attempted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import time
import uuid
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.consumer.stable import (
    StablePrimaryConsumerRoutePlan,
    primary_fallback_return_drill,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    AuthorityPromotionScope,
    SHARED_REALTIME_CORE_GROUP_ID,
    SHARED_REALTIME_CORE_ID_PREFIX,
    STABLE_CORE_WORKER_COUNT,
    StableAcquisitionPlan,
    stable_authority_record,
    write_stable_runtime_bundle,
)


SCHEMA = "qdl.v2.shared-primary-handoff-packet.v1"
RUNTIME_MANIFEST_SCHEMA = "qdl.v2.shared-primary-runtime-bundle.v1"
_ALLOWED_SERVICE_ORDER = (
    "ingestor_binance_usdm",
    "ingestor_okx_swap",
    "binance_bar_edge",
    "rust_core",
    "rust_core_2",
    "rust_core_3",
    "projector_v2",
    "projector_v2_2",
    "projector_v2_3",
    "query_v2_1",
    "query_v2_2",
    "stream_v2_active",
    "stream_v2_passive",
)
_CONDITIONAL_VN_SERVICE = "vn_edge_v2"
_FORBIDDEN_OPERATIONS = (
    "kafka_offset_reset",
    "kafka_seek",
    "kafka_topic_delete",
    "redis_flush",
    "sqlite_delete",
    "v1_restart",
    "alpha_config_rewrite",
    "production_core_start",
    "per_symbol_service",
    "per_symbol_image",
    "per_symbol_topic",
)


def _canonical_bytes(value: Mapping[str, Any] | list[Any]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()


def _sha256(value: Mapping[str, Any] | list[Any]) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _require_sha256(value: str, *, field: str) -> None:
    if not value.startswith("sha256:") or len(value) != 71:
        raise ValueError(f"{field} must be an immutable sha256 image digest")
    if any(char not in "0123456789abcdef" for char in value.removeprefix("sha256:")):
        raise ValueError(f"{field} must be an immutable sha256 image digest")


def _validate_runtime_files(
    runtime_dir: Path,
    *,
    authority: Mapping[str, Any],
) -> dict[str, str]:
    expected = {
        "authority.json",
        "core.json",
        "core-002.json",
        "core-003.json",
        "ingestor-binance-usdm.json",
        "ingestor-okx-swap.json",
    }
    actual = {item.name for item in runtime_dir.iterdir() if item.is_file()}
    if actual != expected:
        raise ValueError("shared primary runtime bundle files differ from the fixed topology")
    digests = {name: _file_digest(runtime_dir / name) for name in sorted(expected)}
    for name in ("core.json", "core-002.json", "core-003.json"):
        payload = json.loads((runtime_dir / name).read_text(encoding="utf-8"))
        if (
            payload.get("authority") != authority
            or payload.get("raw_topics") != ["md.raw.realtime.v2"]
            or payload.get("strict_subscription_scope") is not True
            or payload.get("shard_id")
            != f"{SHARED_REALTIME_CORE_ID_PREFIX}-{1 if name == 'core.json' else int(name[5:8]):03d}"
        ):
            raise ValueError("shared primary core bundle is not authority-bound")
    for name in ("ingestor-binance-usdm.json", "ingestor-okx-swap.json"):
        payload = json.loads((runtime_dir / name).read_text(encoding="utf-8"))
        if (
            payload.get("authority") != authority
            or payload.get("raw_stream") != "md.raw.realtime.v2"
        ):
            raise ValueError("shared primary ingestor bundle is not authority-bound")
    return digests


def _runtime_manifest(
    *,
    authority: Mapping[str, Any],
    runtime_digests: Mapping[str, str],
    sealed_route: Mapping[str, Any],
    source_commit: str,
    python_image_digest: str,
) -> dict[str, Any]:
    return {
        "schema": RUNTIME_MANIFEST_SCHEMA,
        "source_commit": source_commit,
        "authority_sha256": _sha256(dict(authority)),
        "authority_mode": "RUST_PRIMARY",
        "candidate_image_digest": str(authority["candidate_image_digest"]),
        "python_image_digest": python_image_digest,
        "core_group_id": SHARED_REALTIME_CORE_GROUP_ID,
        "core_transactional_id_prefix": f"{SHARED_REALTIME_CORE_ID_PREFIX}-",
        "core_worker_count": STABLE_CORE_WORKER_COUNT,
        "runtime_files": dict(runtime_digests),
        "sealed_consumer_route": dict(sealed_route),
        "forbidden_topology_tokens": ["production_core", "per_symbol"],
    }


def prepare_shared_primary_packet(
    *,
    output_dir: Path,
    rust_image_digest: str,
    python_image_digest: str,
    source_commit: str,
    actor: str,
    change_ticket: str,
    observation_seconds: int,
    issued_at_ns: int | None = None,
) -> dict[str, Any]:
    """Create the sealed bundle and review-only packet without runtime I/O."""
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError("shared primary output directory must be empty")
    _require_sha256(rust_image_digest, field="rust_image_digest")
    _require_sha256(python_image_digest, field="python_image_digest")
    if not source_commit.strip() or not actor.strip() or not change_ticket.strip():
        raise ValueError("source commit, actor and change ticket are required")
    if not 60 <= observation_seconds <= 1_800:
        raise ValueError("observation_seconds must be between 60 and 1800")
    now_ns = time.time_ns() if issued_at_ns is None else issued_at_ns
    if now_ns <= 0:
        raise ValueError("issued_at_ns must be positive")

    output_dir.mkdir(parents=True, mode=0o700)
    runtime_dir = output_dir / "runtime"
    catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
    acquisition_path = ROOT / "config/v2/stable-acquisition-bindings.yaml"
    acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
    scope = AuthorityPromotionScope.load(
        ROOT / "config/v2/stable-authority-promotion-scope.yaml",
        catalog=catalog,
    )
    authority = stable_authority_record(
        rust_image_digest=rust_image_digest,
        capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
        contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
        partition_plan=acquisition_path.read_bytes(),
        effective_at_ns=now_ns,
        mode="RUST_PRIMARY",
        revision=1,
        slice_id="qdl-v2-shared-realtime-primary",
        approved_by=actor,
    )
    runtime_digests = write_stable_runtime_bundle(
        runtime_dir,
        catalog=catalog,
        acquisition=acquisition,
        authority=authority,
    )
    runtime_digests = _validate_runtime_files(runtime_dir, authority=authority)
    route_plan = StablePrimaryConsumerRoutePlan.load(
        ROOT / "config/v2/stable-primary-consumer-routing.yaml",
        manifest_root=ROOT,
        catalog=catalog,
    )
    sealed_route = route_plan.seal(authority)
    drill = primary_fallback_return_drill(route_plan)
    route_path = runtime_dir / "consumer-route-primary.json"
    route_path.write_bytes(_canonical_bytes(sealed_route) + b"\n")
    route_path.chmod(0o644)
    runtime_digests[route_path.name] = _file_digest(route_path)
    manifest = _runtime_manifest(
        authority=authority,
        runtime_digests=runtime_digests,
        sealed_route=sealed_route,
        source_commit=source_commit,
        python_image_digest=python_image_digest,
    )
    manifest_path = runtime_dir / "shared-primary-runtime-manifest.json"
    manifest_path.write_bytes(_canonical_bytes(manifest) + b"\n")
    manifest_path.chmod(0o644)
    runtime_digests[manifest_path.name] = _file_digest(manifest_path)
    bundle_digest = _sha256({"files": runtime_digests})
    packet_seed = {
        "schema": SCHEMA,
        "source_commit": source_commit,
        "authority_sha256": _sha256(authority),
        "bundle_sha256": bundle_digest,
        "python_image_digest": python_image_digest,
        "actor": actor,
        "change_ticket": change_ticket,
        "issued_at_ns": now_ns,
    }
    packet_digest = _sha256(packet_seed)
    packet = {
        "schema": SCHEMA,
        "packet_id": str(uuid.uuid5(uuid.NAMESPACE_URL, packet_digest)),
        "packet_sha256": packet_digest,
        "confirmation_token": f"APPLY_QDL_PHASE103_{packet_digest[:16]}",
        "issued_at_ns": now_ns,
        "expires_at_ns": now_ns + 1_800_000_000_000,
        "actor": actor,
        "change_ticket": change_ticket,
        "apply_requested": False,
        "production_mutations": 0,
        "authority": authority,
        "runtime_bundle": {
            "sha256": bundle_digest,
            "manifest_sha256": _file_digest(manifest_path),
            "rust_image_digest": rust_image_digest,
            "python_image_digest": python_image_digest,
            "core_group_id": SHARED_REALTIME_CORE_GROUP_ID,
            "core_transactional_id_prefix": f"{SHARED_REALTIME_CORE_ID_PREFIX}-",
            "files": runtime_digests,
        },
        "consumer_route": {
            "sealed_route": sealed_route,
            "fallback_return_drill": drill,
        },
        "deployment": {
            "topic": {
                "name": "md.raw.realtime.v2",
                "partitions": 6,
                "replication_factor": 3,
                "min_insync_replicas": 2,
                "operation": "CREATE_OR_VERIFY_ONLY",
            },
            "acl_intent": {
                "producer": "phase8-producer",
                "core": "phase8-core",
                "core_group_id": SHARED_REALTIME_CORE_GROUP_ID,
                "core_transactional_id_prefix": f"{SHARED_REALTIME_CORE_ID_PREFIX}-",
            },
            "services": list(_ALLOWED_SERVICE_ORDER),
            "conditional_vn_service": {
                "name": _CONDITIONAL_VN_SERVICE,
                "requires": "verified_in_session_provider_admission",
            },
        },
        "acceptance": {
            "crypto_binding_count": len(scope.binding_ids),
            "required_crypto_evidence": [
                "freshness",
                "gap_count",
                "reconnect_count",
                "canonical_lag",
                "projector_lag",
                "consumer_receive_lag",
                "cpu_ram_io",
                "fallback_count",
            ],
            "observation_seconds": observation_seconds,
            "v1_fallback_return_required": True,
            "vn_primary_requires_in_session_evidence": True,
        },
        "rollback": {
            "consumer_route": "V1",
            "stop_only_services": list(_ALLOWED_SERVICE_ORDER),
            "retained_evidence": ["kafka_canonical", "kafka_raw", "cursor", "audit"],
            "forbidden_operations": list(_FORBIDDEN_OPERATIONS),
        },
    }
    validate_shared_primary_packet(packet)
    packet_path = output_dir / "shared-primary-handoff-packet.json"
    packet_path.write_bytes(_canonical_bytes(packet) + b"\n")
    packet_path.chmod(0o640)
    return packet


def validate_shared_primary_packet(packet: Mapping[str, Any]) -> None:
    """Fail closed on anything outside the one shared-core topology."""
    expected = {
        "schema", "packet_id", "packet_sha256", "confirmation_token",
        "issued_at_ns", "expires_at_ns", "actor", "change_ticket",
        "apply_requested", "production_mutations", "authority", "runtime_bundle",
        "consumer_route", "deployment", "acceptance", "rollback",
    }
    if set(packet) != expected or packet.get("schema") != SCHEMA:
        raise ValueError("shared primary packet schema/fields are invalid")
    authority = packet["authority"]
    if not isinstance(authority, dict):
        raise ValueError("shared primary packet authority is invalid")
    from qdl.runtime.stable_deployment import validate_shared_authority_record

    validate_shared_authority_record(authority)
    if authority.get("mode") != "RUST_PRIMARY":
        raise ValueError("shared primary packet requires RUST_PRIMARY authority")
    if packet.get("apply_requested") is not False or packet.get("production_mutations") != 0:
        raise ValueError("shared primary packet is review-only before explicit approval")
    runtime_bundle = packet["runtime_bundle"]
    if not isinstance(runtime_bundle, dict):
        raise ValueError("shared primary packet runtime bundle is invalid")
    rust_image_digest = runtime_bundle.get("rust_image_digest")
    python_image_digest = runtime_bundle.get("python_image_digest")
    if not isinstance(rust_image_digest, str) or not isinstance(python_image_digest, str):
        raise ValueError("shared primary packet image digests are invalid")
    _require_sha256(rust_image_digest, field="rust_image_digest")
    _require_sha256(python_image_digest, field="python_image_digest")
    if rust_image_digest != authority.get("candidate_image_digest"):
        raise ValueError("shared primary packet Rust image differs from authority")
    deployment = packet["deployment"]
    rollback = packet["rollback"]
    if not isinstance(deployment, dict) or not isinstance(rollback, dict):
        raise ValueError("shared primary packet deployment/rollback is invalid")
    services = deployment.get("services")
    if services != list(_ALLOWED_SERVICE_ORDER):
        raise ValueError("shared primary packet service topology differs from the fixed plan")
    if any("production_core" in str(service) for service in services):
        raise ValueError("shared primary packet contains a forbidden mutation/topology")
    if rollback.get("consumer_route") != "V1":
        raise ValueError("shared primary packet rollback must be V1")
    if rollback.get("stop_only_services") != list(_ALLOWED_SERVICE_ORDER):
        raise ValueError("shared primary packet rollback must stop only named V2 services")
    if rollback.get("forbidden_operations") != list(_FORBIDDEN_OPERATIONS):
        raise ValueError("shared primary packet rollback protections differ from the plan")
    route = packet["consumer_route"]
    if not isinstance(route, dict) or not isinstance(route.get("sealed_route"), dict):
        raise ValueError("shared primary packet route is invalid")
    sealed = route["sealed_route"]
    if (
        sealed.get("authority_mode") != "RUST_PRIMARY"
        or sealed.get("authority_sha256") != _sha256(authority)
        or sealed.get("rollback_route") != "V1"
        or sealed.get("target_route") != "V2_PRIMARY"
    ):
        raise ValueError("shared primary packet route is not bound to authority/rollback")
    drill = route.get("fallback_return_drill")
    if not isinstance(drill, dict) or drill.get("test_provenance") is not True:
        raise ValueError("shared primary packet route drill is missing test provenance")
    transitions = drill.get("transitions")
    if not isinstance(transitions, list) or not transitions:
        raise ValueError("shared primary packet route drill is empty")
    if any(
        item.get("before") != "V2_PRIMARY"
        or item.get("fallback") != "V1_FALLBACK"
        or item.get("returned") != "V2_PRIMARY"
        for item in transitions
        if isinstance(item, dict)
    ):
        raise ValueError("shared primary packet route drill does not prove fallback return")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--rust-image-digest", required=True)
    parser.add_argument("--python-image-digest", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--change-ticket", required=True)
    parser.add_argument("--observation-seconds", type=int, default=300)
    args = parser.parse_args()
    packet = prepare_shared_primary_packet(
        output_dir=args.output_dir,
        rust_image_digest=args.rust_image_digest,
        python_image_digest=args.python_image_digest,
        source_commit=args.source_commit,
        actor=args.actor,
        change_ticket=args.change_ticket,
        observation_seconds=args.observation_seconds,
    )
    print(json.dumps({
        "status": "REVIEW_REQUIRED",
        "packet_sha256": packet["packet_sha256"],
        "confirmation_token": packet["confirmation_token"],
        "apply_requested": False,
        "production_mutations": 0,
        "service_count": len(packet["deployment"]["services"]),
        "crypto_binding_count": packet["acceptance"]["crypto_binding_count"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
