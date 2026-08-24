#!/usr/bin/env python3
"""Prepare a review-only shared Rust-primary V2 runtime packet.

This generator writes a fresh non-secret runtime bundle plus one immutable
handoff packet. It never invokes Docker, Kafka, Redis, PostgreSQL, a provider,
or a consumer endpoint. An operator must separately approve the exact packet
before any topic/ACL/service/consumer-route mutation is attempted.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
from typing import Any, Mapping
import uuid


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
    STABLE_CORE_WORKER_COUNT,
    StableAcquisitionPlan,
    stable_authority_record,
    write_stable_runtime_bundle,
)
from scripts.phase103_packet_contract import (
    ACL_INTENT as _ACL_INTENT,
    ALLOWED_SERVICE_ORDER as _ALLOWED_SERVICE_ORDER,
    CONDITIONAL_VN_SERVICE_METADATA as _CONDITIONAL_VN_SERVICE_METADATA,
    CORE_RUNTIME_FILES as _CORE_RUNTIME_FILES,
    FORBIDDEN_OPERATIONS as _FORBIDDEN_OPERATIONS,
    REALTIME_RAW_TOPIC as _REALTIME_RAW_TOPIC,
    REQUIRED_CRYPTO_EVIDENCE as _REQUIRED_CRYPTO_EVIDENCE,
    SCHEMA,
    SHARED_REALTIME_CORE_GROUP_ID,
    SHARED_REALTIME_CORE_ID_PREFIX,
    canonical_bytes as _canonical_bytes,
    file_digest as _file_digest,
    require_host_runtime_dir as _require_host_runtime_dir,
    require_sha256 as _require_sha256,
    sha256 as _sha256,
    validate_prepared_shared_primary_bundle,
    validate_shared_primary_packet,
)


RUNTIME_MANIFEST_SCHEMA = "qdl.v2.shared-primary-runtime-bundle.v1"


def _validate_runtime_files(
    runtime_dir: Path,
    *,
    authority: Mapping[str, Any],
) -> dict[str, str]:
    expected = _CORE_RUNTIME_FILES
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
    host_runtime_dir: Path | None = None,
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

    output_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    runtime_dir = output_dir / "runtime"
    compose_runtime_dir = _require_host_runtime_dir(
        runtime_dir if host_runtime_dir is None else host_runtime_dir
    )
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
    compose_environment = {
        "QDL_STABLE_RUNTIME_DIR": str(compose_runtime_dir),
        "QDL_STABLE_PYTHON_IMAGE": python_image_digest,
        "QDL_STABLE_RUST_IMAGE": rust_image_digest,
        "QDL_STABLE_AUTHORITY_MODE": "RUST_PRIMARY",
        "QDL_STABLE_AUTHORITY_REVISION": str(authority["revision"]),
        "QDL_CONFIG_REVISION": f"phase103-shared-primary-r{authority['revision']}",
    }
    packet_body = {
        "schema": SCHEMA,
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
            "topic": dict(_REALTIME_RAW_TOPIC),
            "acl_intent": dict(_ACL_INTENT),
            "services": list(_ALLOWED_SERVICE_ORDER),
            "conditional_vn_service": dict(_CONDITIONAL_VN_SERVICE_METADATA),
        },
        "acceptance": {
            "crypto_binding_count": len(scope.binding_ids),
            "required_crypto_evidence": list(_REQUIRED_CRYPTO_EVIDENCE),
            "observation_seconds": observation_seconds,
            "v1_fallback_return_required": True,
            "vn_primary_requires_in_session_evidence": True,
        },
        "compose_environment": compose_environment,
        "rollback": {
            "consumer_route": "V1",
            "stop_only_services": list(_ALLOWED_SERVICE_ORDER),
            "retained_evidence": ["kafka_canonical", "kafka_raw", "cursor", "audit"],
            "forbidden_operations": list(_FORBIDDEN_OPERATIONS),
        },
    }
    packet_digest = _sha256(packet_body)
    packet = {
        "packet_id": str(uuid.uuid5(uuid.NAMESPACE_URL, packet_digest)),
        "packet_sha256": packet_digest,
        "confirmation_token": f"APPLY_QDL_PHASE103_{packet_digest[:16]}",
        **packet_body,
    }
    validate_shared_primary_packet(packet)
    packet_path = output_dir / "shared-primary-handoff-packet.json"
    packet_path.write_bytes(_canonical_bytes(packet) + b"\n")
    packet_path.chmod(0o640)
    return packet


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--rust-image-digest", required=True)
    parser.add_argument("--python-image-digest", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--change-ticket", required=True)
    parser.add_argument("--observation-seconds", type=int, default=300)
    parser.add_argument("--host-runtime-dir", type=Path)
    args = parser.parse_args()
    packet = prepare_shared_primary_packet(
        output_dir=args.output_dir,
        rust_image_digest=args.rust_image_digest,
        python_image_digest=args.python_image_digest,
        source_commit=args.source_commit,
        actor=args.actor,
        change_ticket=args.change_ticket,
        observation_seconds=args.observation_seconds,
        host_runtime_dir=args.host_runtime_dir,
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
