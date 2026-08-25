"""Dependency-free contract for the sealed Phase 10.3 handoff packet.

The review generator needs the full QDL runtime dependencies, but the operator
validator and bounded broker helper must fail closed on a clean host before
they invoke Docker Compose.  Keep this module stdlib-only and test its fixed
identity constants against the runtime deployment module.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
import time
from typing import Any, Mapping
import uuid


SCHEMA = "qdl.v2.shared-primary-handoff-packet.v2"
SHARED_REALTIME_CORE_GROUP_ID = "qdl-v2-realtime-core-v2"
SHARED_REALTIME_CORE_ID_PREFIX = "qdl-v2-realtime-core"
ALLOWED_SERVICE_ORDER = (
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
FORBIDDEN_OPERATIONS = (
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
COMPOSE_ENVIRONMENT_KEYS = (
    "QDL_STABLE_RUNTIME_DIR",
    "QDL_STABLE_PYTHON_IMAGE",
    "QDL_STABLE_RUST_IMAGE",
    "QDL_STABLE_AUTHORITY_MODE",
    "QDL_STABLE_AUTHORITY_REVISION",
    "QDL_CONFIG_REVISION",
    "QDL_STABLE_BAR_STATE_PATH",
)
BAR_STATE_DIRECTORY = PurePosixPath("/var/lib/qdl-stable/runtime")
CORE_RUNTIME_FILES = frozenset({
    "authority.json",
    "core.json",
    "core-002.json",
    "core-003.json",
    "ingestor-binance-usdm.json",
    "ingestor-okx-swap.json",
})
PACKET_RUNTIME_FILES = CORE_RUNTIME_FILES | {
    "consumer-route-primary.json",
    "shared-primary-runtime-manifest.json",
}
REALTIME_RAW_TOPIC = {
    "name": "md.raw.realtime.v2",
    "partitions": 6,
    "replication_factor": 3,
    "min_insync_replicas": 2,
    "operation": "CREATE_OR_VERIFY_ONLY",
}
ACL_INTENT = {
    "producer": "phase8-producer",
    "core": "phase8-core",
    "core_group_id": SHARED_REALTIME_CORE_GROUP_ID,
    "core_transactional_id_prefix": f"{SHARED_REALTIME_CORE_ID_PREFIX}-",
}
CONDITIONAL_VN_SERVICE_METADATA = {
    "name": "vn_edge_v2",
    "requires": "verified_in_session_provider_admission",
}
REQUIRED_CRYPTO_EVIDENCE = (
    "freshness",
    "gap_count",
    "reconnect_count",
    "canonical_lag",
    "projector_lag",
    "consumer_receive_lag",
    "cpu_ram_io",
    "fallback_count",
)
TRADING_SYSTEM_HANDOFF_LOCK_SCHEMA = "qdl.v2.external-consumer-route-lock.v1"
TRADING_SYSTEM_HANDOFF_CONSUMER_ID = "trading-system.paper.stable"
TRADING_SYSTEM_HANDOFF_SERVICE = "market_data"
TRADING_SYSTEM_ROUTE_MANIFEST_SCHEMA = "trading-system.data-layer-v2-routes.v1"
TRADING_SYSTEM_ROUTE_IDENTITIES = (
    ("BINANCE", "USDM", "PERPETUAL", "BTCUSDT", "TRADE", None, "V1"),
    ("BINANCE", "USDM", "PERPETUAL", "BTCUSDT", "BAR", "1m", "V1"),
    ("BINANCE", "USDM", "PERPETUAL", "ETHUSDT", "TRADE", None, "V1"),
    ("BINANCE", "USDM", "PERPETUAL", "ETHUSDT", "BAR", "1m", "V1"),
    ("OKX", "SWAP", "PERPETUAL", "BTC-USDT-SWAP", "TRADE", None, "BLOCK"),
    ("OKX", "SWAP", "PERPETUAL", "BTC-USDT-SWAP", "BAR", "1m", "BLOCK"),
    ("OKX", "SWAP", "PERPETUAL", "ETH-USDT-SWAP", "TRADE", None, "BLOCK"),
    ("OKX", "SWAP", "PERPETUAL", "ETH-USDT-SWAP", "BAR", "1m", "BLOCK"),
)
_PACKET_IDENTITY_FIELDS = {
    "packet_id",
    "packet_sha256",
    "confirmation_token",
}


def canonical_bytes(value: Mapping[str, Any] | list[Any]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()


def sha256(value: Mapping[str, Any] | list[Any]) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def authority_scoped_bar_state_path(authority: Mapping[str, Any]) -> str:
    """Return the sealed BAR checkpoint path for one authority identity.

    A checkpoint is valid only for the authority/configuration that wrote it.
    Keeping the old path as rollback evidence and starting a primary edge at a
    new deterministic path avoids mutating or trusting a shadow checkpoint.
    """
    validate_shared_authority_record(authority)
    identity = sha256({
        "slice_id": authority["slice_id"],
        "revision": authority["revision"],
        "partition_plan_digest": authority["partition_plan_digest"],
        "candidate_image_digest": authority["candidate_image_digest"],
    })
    return str(BAR_STATE_DIRECTORY / f"stable-crypto-bar-edge-{identity[:20]}.json")


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require_sha256(value: object, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value.startswith("sha256:")
        or len(value) != 71
        or any(char not in "0123456789abcdef" for char in value.removeprefix("sha256:"))
    ):
        raise ValueError(f"{field} must be an immutable sha256 image digest")


def _require_digest(value: object, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValueError(f"{field} must be a lowercase sha256 digest")


def validate_trading_system_handoff_lock(lock: Mapping[str, Any]) -> None:
    """Validate the one external market-data route lock embedded in a packet."""
    expected = {
        "schema", "consumer_id", "service", "repository", "route_manifest",
        "compose_override",
    }
    if (
        not isinstance(lock, Mapping)
        or set(lock) != expected
        or lock.get("schema") != TRADING_SYSTEM_HANDOFF_LOCK_SCHEMA
        or lock.get("consumer_id") != TRADING_SYSTEM_HANDOFF_CONSUMER_ID
        or lock.get("service") != TRADING_SYSTEM_HANDOFF_SERVICE
        or lock.get("repository") != "BobbyAxerol/ExecutorBroker"
    ):
        raise ValueError("Trading System handoff lock identity is invalid")
    route = lock.get("route_manifest")
    if not isinstance(route, Mapping) or set(route) != {
        "path", "schema", "revision", "sha256", "identities",
    }:
        raise ValueError("Trading System handoff route lock is invalid")
    if (
        route.get("path") != "config/_config/data_layer_v2_routes.yaml"
        or route.get("schema") != TRADING_SYSTEM_ROUTE_MANIFEST_SCHEMA
        or route.get("revision") != 2
    ):
        raise ValueError("Trading System handoff route revision is invalid")
    _require_digest(route.get("sha256"), field="Trading System route manifest")
    identities = route.get("identities")
    if not isinstance(identities, list):
        raise ValueError("Trading System handoff route identities are invalid")
    actual_identities = tuple(
        (
            item.get("venue"),
            item.get("market"),
            item.get("product"),
            item.get("native_symbol"),
            item.get("feed"),
            item.get("interval"),
            item.get("fallback"),
        )
        for item in identities
        if isinstance(item, Mapping)
        and set(item) == {
            "venue", "market", "product", "native_symbol", "feed",
            "interval", "fallback",
        }
    )
    if actual_identities != TRADING_SYSTEM_ROUTE_IDENTITIES:
        raise ValueError("Trading System handoff route scope is invalid")
    compose = lock.get("compose_override")
    if not isinstance(compose, Mapping) or set(compose) != {
        "path", "sha256", "binance_symbols", "okx_symbols",
    }:
        raise ValueError("Trading System handoff Compose lock is invalid")
    if (
        compose.get("path") != "docker-compose.data-layer-v2-primary.yml"
        or tuple(compose.get("binance_symbols", ())) != ("BTCUSDT", "ETHUSDT")
        or tuple(compose.get("okx_symbols", ()))
        != ("BTC-USDT-SWAP", "ETH-USDT-SWAP")
    ):
        raise ValueError("Trading System handoff Compose scope is invalid")
    _require_digest(compose.get("sha256"), field="Trading System Compose override")


def validate_trading_system_handoff(handoff: Mapping[str, Any]) -> None:
    if not isinstance(handoff, Mapping) or set(handoff) != {
        "route_lock", "route_lock_sha256",
    }:
        raise ValueError("Trading System handoff packet fields are invalid")
    lock = handoff.get("route_lock")
    if not isinstance(lock, Mapping):
        raise ValueError("Trading System handoff route lock is invalid")
    validate_trading_system_handoff_lock(lock)
    if handoff.get("route_lock_sha256") != sha256(dict(lock)):
        raise ValueError("Trading System handoff route lock digest is invalid")


def require_host_runtime_dir(path: Path) -> Path:
    """Validate the host Compose path without resolving it in the probe."""
    if not path.is_absolute() or path.name != "runtime" or ".." in path.parts:
        raise ValueError("host_runtime_dir must be an absolute runtime directory")
    return path


def _packet_body(packet: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in packet.items()
        if key not in _PACKET_IDENTITY_FIELDS
    }


def validate_shared_authority_record(authority: Mapping[str, Any]) -> None:
    """Validate the serialized authority record without runtime imports."""
    digest = str(authority.get("candidate_image_digest", ""))
    digest_fields = (
        "capability_manifest_digest",
        "contract_digest",
        "partition_plan_digest",
    )
    revision = authority.get("revision")
    effective_at_ns = authority.get("effective_at_ns")
    if (
        authority.get("schema") != "qdl.authority-record.v1"
        or not isinstance(authority.get("slice_id"), str)
        or not authority["slice_id"].strip()
        or not isinstance(revision, int)
        or isinstance(revision, bool)
        or revision < 1
        or authority.get("mode") not in {"RUST_SHADOW", "RUST_PRIMARY"}
        or authority.get("public_write_allowed") is not False
        or authority.get("legacy_write_allowed") is not False
        or not digest.startswith("sha256:")
        or len(digest) != 71
        or any(value not in "0123456789abcdef" for value in digest.removeprefix("sha256:"))
        or any(
            len(str(authority.get(field, ""))) != 64
            or any(value not in "0123456789abcdef" for value in str(authority.get(field, "")))
            for field in digest_fields
        )
        or not isinstance(authority.get("approved_by"), str)
        or not authority["approved_by"].strip()
        or not isinstance(effective_at_ns, int)
        or isinstance(effective_at_ns, bool)
        or effective_at_ns <= 0
    ):
        raise ValueError("stable authority is not an isolated shared Rust authority record")


def validate_shared_primary_packet(packet: Mapping[str, Any]) -> None:
    """Fail closed on anything outside the one shared-core topology."""
    expected = {
        "schema", "packet_id", "packet_sha256", "confirmation_token",
        "issued_at_ns", "expires_at_ns", "actor", "change_ticket",
        "apply_requested", "production_mutations", "authority", "runtime_bundle",
        "consumer_route", "trading_system_handoff", "deployment", "acceptance",
        "compose_environment", "rollback",
    }
    if set(packet) != expected or packet.get("schema") != SCHEMA:
        raise ValueError("shared primary packet schema/fields are invalid")
    packet_digest = sha256(_packet_body(packet))
    if (
        packet.get("packet_sha256") != packet_digest
        or packet.get("packet_id") != str(uuid.uuid5(uuid.NAMESPACE_URL, packet_digest))
        or packet.get("confirmation_token") != f"APPLY_QDL_PHASE103_{packet_digest[:16]}"
    ):
        raise ValueError("shared primary packet integrity is invalid")
    authority = packet["authority"]
    if not isinstance(authority, dict):
        raise ValueError("shared primary packet authority is invalid")
    validate_shared_authority_record(authority)
    if authority.get("mode") != "RUST_PRIMARY":
        raise ValueError("shared primary packet requires RUST_PRIMARY authority")
    if packet.get("apply_requested") is not False or packet.get("production_mutations") != 0:
        raise ValueError("shared primary packet is review-only before explicit approval")
    handoff = packet.get("trading_system_handoff")
    if not isinstance(handoff, Mapping):
        raise ValueError("shared primary packet Trading System handoff is invalid")
    validate_trading_system_handoff(handoff)
    runtime_bundle = packet["runtime_bundle"]
    if not isinstance(runtime_bundle, dict) or set(runtime_bundle) != {
        "sha256", "manifest_sha256", "rust_image_digest", "python_image_digest",
        "core_group_id", "core_transactional_id_prefix", "files",
    }:
        raise ValueError("shared primary packet runtime bundle fields are invalid")
    rust_image_digest = runtime_bundle.get("rust_image_digest")
    python_image_digest = runtime_bundle.get("python_image_digest")
    require_sha256(rust_image_digest, field="rust_image_digest")
    require_sha256(python_image_digest, field="python_image_digest")
    if rust_image_digest != authority.get("candidate_image_digest"):
        raise ValueError("shared primary packet Rust image differs from authority")
    files = runtime_bundle.get("files")
    if (
        not isinstance(files, dict)
        or set(files) != PACKET_RUNTIME_FILES
        or any(not isinstance(name, str) or not isinstance(digest, str) for name, digest in files.items())
        or any(
            len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
            for digest in files.values()
        )
    ):
        raise ValueError("shared primary packet runtime files are invalid")
    if runtime_bundle.get("sha256") != sha256({"files": files}):
        raise ValueError("shared primary packet runtime bundle digest is invalid")
    if runtime_bundle.get("manifest_sha256") != files["shared-primary-runtime-manifest.json"]:
        raise ValueError("shared primary packet runtime manifest digest is invalid")
    if (
        runtime_bundle.get("core_group_id") != SHARED_REALTIME_CORE_GROUP_ID
        or runtime_bundle.get("core_transactional_id_prefix")
        != f"{SHARED_REALTIME_CORE_ID_PREFIX}-"
    ):
        raise ValueError("shared primary packet core identity is invalid")
    compose_environment = packet["compose_environment"]
    if (
        not isinstance(compose_environment, dict)
        or set(compose_environment) != set(COMPOSE_ENVIRONMENT_KEYS)
        or not all(isinstance(value, str) and value for value in compose_environment.values())
    ):
        raise ValueError("shared primary packet Compose environment is invalid")
    runtime_path = Path(compose_environment["QDL_STABLE_RUNTIME_DIR"])
    if not runtime_path.is_absolute() or runtime_path.name != "runtime":
        raise ValueError("shared primary packet runtime directory is invalid")
    if (
        compose_environment["QDL_STABLE_PYTHON_IMAGE"] != python_image_digest
        or compose_environment["QDL_STABLE_RUST_IMAGE"] != rust_image_digest
        or compose_environment["QDL_STABLE_AUTHORITY_MODE"] != "RUST_PRIMARY"
        or compose_environment["QDL_STABLE_AUTHORITY_REVISION"] != str(authority["revision"])
        or compose_environment["QDL_CONFIG_REVISION"]
        != f"phase103-shared-primary-r{authority['revision']}"
        or compose_environment["QDL_STABLE_BAR_STATE_PATH"]
        != authority_scoped_bar_state_path(authority)
    ):
        raise ValueError("shared primary packet Compose environment differs from authority")
    deployment = packet["deployment"]
    rollback = packet["rollback"]
    if not isinstance(deployment, dict) or not isinstance(rollback, dict):
        raise ValueError("shared primary packet deployment/rollback is invalid")
    if set(deployment) != {"topic", "acl_intent", "services", "conditional_vn_service"}:
        raise ValueError("shared primary packet deployment fields are invalid")
    if deployment.get("topic") != REALTIME_RAW_TOPIC:
        raise ValueError("shared primary packet topic scope differs from the fixed plan")
    if deployment.get("acl_intent") != ACL_INTENT:
        raise ValueError("shared primary packet ACL scope differs from the fixed plan")
    if deployment.get("conditional_vn_service") != CONDITIONAL_VN_SERVICE_METADATA:
        raise ValueError("shared primary packet VN condition differs from the fixed plan")
    services = deployment.get("services")
    if services != list(ALLOWED_SERVICE_ORDER):
        raise ValueError("shared primary packet service topology differs from the fixed plan")
    if any("production_core" in str(service) for service in services):
        raise ValueError("shared primary packet contains a forbidden mutation/topology")
    if rollback.get("consumer_route") != "V1":
        raise ValueError("shared primary packet rollback must be V1")
    if rollback.get("stop_only_services") != list(ALLOWED_SERVICE_ORDER):
        raise ValueError("shared primary packet rollback must stop only named V2 services")
    if rollback.get("forbidden_operations") != list(FORBIDDEN_OPERATIONS):
        raise ValueError("shared primary packet rollback protections differ from the plan")
    acceptance = packet["acceptance"]
    if not isinstance(acceptance, dict) or set(acceptance) != {
        "crypto_binding_count", "required_crypto_evidence", "observation_seconds",
        "v1_fallback_return_required", "vn_primary_requires_in_session_evidence",
    }:
        raise ValueError("shared primary packet acceptance fields are invalid")
    if (
        acceptance.get("crypto_binding_count") != 12
        or acceptance.get("required_crypto_evidence") != list(REQUIRED_CRYPTO_EVIDENCE)
        or not isinstance(acceptance.get("observation_seconds"), int)
        or not 60 <= acceptance["observation_seconds"] <= 1_800
        or acceptance.get("v1_fallback_return_required") is not True
        or acceptance.get("vn_primary_requires_in_session_evidence") is not True
    ):
        raise ValueError("shared primary packet acceptance scope is invalid")
    route = packet["consumer_route"]
    if not isinstance(route, dict) or not isinstance(route.get("sealed_route"), dict):
        raise ValueError("shared primary packet route is invalid")
    sealed = route["sealed_route"]
    if (
        sealed.get("authority_mode") != "RUST_PRIMARY"
        or sealed.get("authority_sha256") != sha256(authority)
        or sealed.get("rollback_route") != "V1"
        or sealed.get("target_route") != "V2_PRIMARY"
    ):
        raise ValueError("shared primary packet route is not bound to authority/rollback")
    drill = route.get("fallback_return_drill")
    if not isinstance(drill, dict) or drill.get("test_provenance") is not True:
        raise ValueError("shared primary packet route drill is missing test provenance")
    transitions = drill.get("transitions")
    if not isinstance(transitions, list) or not transitions or any(
        item.get("before") != "V2_PRIMARY"
        or item.get("fallback") != "V1_FALLBACK"
        or item.get("returned") != "V2_PRIMARY"
        for item in transitions
        if isinstance(item, dict)
    ):
        raise ValueError("shared primary packet route drill does not prove fallback return")


def validate_prepared_shared_primary_bundle(
    packet: Mapping[str, Any],
    *,
    runtime_dir: Path,
    now_ns: int | None = None,
) -> dict[str, Any]:
    """Read-only validation of the sealed packet and its host runtime bundle."""
    validate_shared_primary_packet(packet)
    now = time.time_ns() if now_ns is None else now_ns
    issued_at_ns = packet.get("issued_at_ns")
    expires_at_ns = packet.get("expires_at_ns")
    if (
        not isinstance(issued_at_ns, int)
        or not isinstance(expires_at_ns, int)
        or expires_at_ns != issued_at_ns + 1_800_000_000_000
        or now < issued_at_ns
        or now >= expires_at_ns
    ):
        raise ValueError("shared primary packet is not within its approved time window")
    declared_runtime_dir = Path(packet["compose_environment"]["QDL_STABLE_RUNTIME_DIR"])
    if (
        not runtime_dir.is_absolute()
        or runtime_dir.absolute() != declared_runtime_dir.absolute()
        or runtime_dir.is_symlink()
        or not runtime_dir.is_dir()
    ):
        raise ValueError("shared primary runtime directory does not match the packet")
    files = packet["runtime_bundle"]["files"]
    actual = {item.name for item in runtime_dir.iterdir()}
    if actual != set(files):
        raise ValueError("shared primary runtime file set differs from the packet")
    for name, expected_digest in files.items():
        path = runtime_dir / name
        if path.is_symlink() or not path.is_file() or file_digest(path) != expected_digest:
            raise ValueError(f"shared primary runtime file digest mismatch: {name}")
    authority = json.loads((runtime_dir / "authority.json").read_text(encoding="utf-8"))
    route = json.loads((runtime_dir / "consumer-route-primary.json").read_text(encoding="utf-8"))
    manifest = json.loads(
        (runtime_dir / "shared-primary-runtime-manifest.json").read_text(encoding="utf-8")
    )
    if authority != packet["authority"]:
        raise ValueError("shared primary runtime authority differs from the packet")
    if route != packet["consumer_route"]["sealed_route"]:
        raise ValueError("shared primary runtime consumer route differs from the packet")
    manifest_files = {
        name: digest
        for name, digest in files.items()
        if name != "shared-primary-runtime-manifest.json"
    }
    if (
        manifest.get("authority_sha256") != sha256(authority)
        or manifest.get("sealed_consumer_route") != route
        or manifest.get("trading_system_handoff") != packet["trading_system_handoff"]
        or manifest.get("runtime_files") != manifest_files
    ):
        raise ValueError("shared primary runtime manifest differs from the packet")
    return {
        "status": "PASS",
        "packet_sha256": packet["packet_sha256"],
        "runtime_bundle_sha256": packet["runtime_bundle"]["sha256"],
        "runtime_file_count": len(files),
        "authority_mode": authority["mode"],
        "read_only": True,
    }
