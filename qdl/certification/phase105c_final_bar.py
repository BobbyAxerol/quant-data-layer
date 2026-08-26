"""Deterministic packet preparation for Phase 10.5-C1 final-BAR repair.

The deployed revision-9 bundle assigned an OKX final BAR to the native
ingestor.  Revision 10 makes the bounded Python REST edge the final-BAR owner
for every enabled Binance/OKX BAR.  C1 must repair that ownership without
changing the Rust authority record already mounted by the running core.

This module is deliberately source-only.  It writes a non-secret packet and
runtime bundle, but never starts containers or talks to a provider, Kafka,
Redis, SQLite, or an authority control plane.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    validate_shared_authority_record,
    write_stable_runtime_bundle,
)


ROOT = Path(__file__).resolve().parents[2]
SCHEMA = "qdl.phase105c.final-bar-repair.v1"
ACQUISITION_REVISION = 10
WARMUP_ROWS = 1_000
RECREATED_SERVICES = (
    "ingestor_okx_swap",
    "binance_bar_edge",
    "query_v2_1",
    "query_v2_2",
    "stream_v2_active",
    "stream_v2_passive",
)
FINAL_BAR_BINDINGS = frozenset({
    "binance-usdm-btcusdt-bar-1m",
    "binance-usdm-ethusdt-bar-1m",
    "okx-swap-btcusdt-bar-1m",
    "okx-swap-eth-usdt-swap-bar-1m",
})
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{7,64}\Z")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("utf-8")


def _require_digest(value: str, *, field: str) -> str:
    if not _DIGEST.fullmatch(value):
        raise ValueError(f"Phase 10.5-C1 {field} must be an immutable SHA-256 digest")
    return value


def _require_commit(value: str) -> str:
    if not _COMMIT.fullmatch(value):
        raise ValueError("Phase 10.5-C1 source commit must be a hexadecimal Git revision")
    return value


def _require_absolute_path(value: Path, *, field: str) -> Path:
    if not value.is_absolute() or ".." in value.parts:
        raise ValueError(f"Phase 10.5-C1 {field} must be an absolute normalized path")
    return value


def _load_active_authority(active_runtime_dir: Path) -> tuple[dict[str, Any], bytes]:
    authority_path = active_runtime_dir / "authority.json"
    try:
        encoded = authority_path.read_bytes()
        payload = json.loads(encoded)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Phase 10.5-C1 active authority.json is unreadable") from error
    if not isinstance(payload, dict):
        raise ValueError("Phase 10.5-C1 active authority.json must contain an object")
    validate_shared_authority_record(payload)
    return payload, encoded


def _load_revision_ten_acquisition() -> tuple[StableSourceCatalog, StableAcquisitionPlan]:
    catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
    acquisition = StableAcquisitionPlan.load(
        ROOT / "config/v2/stable-acquisition-bindings.yaml", catalog=catalog
    )
    if acquisition.revision != ACQUISITION_REVISION:
        raise ValueError(
            "Phase 10.5-C1 requires stable acquisition revision "
            f"{ACQUISITION_REVISION}, got {acquisition.revision}"
        )
    by_id = {item.binding_id: item for item in acquisition.bindings}
    if not FINAL_BAR_BINDINGS <= set(by_id):
        raise ValueError("Phase 10.5-C1 final-BAR binding set is incomplete")
    for binding_id in sorted(FINAL_BAR_BINDINGS):
        item = by_id[binding_id]
        if not item.enabled or item.mode != "PYTHON_REST":
            raise ValueError(
                "Phase 10.5-C1 final BAR is not owned by the Python REST edge: "
                + binding_id
            )
    return catalog, acquisition


def _validate_generated_bundle(
    runtime_dir: Path,
    *,
    authority: Mapping[str, Any],
    authority_bytes: bytes,
    acquisition: StableAcquisitionPlan,
) -> dict[str, str]:
    authority_path = runtime_dir / "authority.json"
    if authority_path.read_bytes() != authority_bytes:
        raise ValueError("Phase 10.5-C1 changed the active authority bytes")

    native_okx_path = runtime_dir / "ingestor-okx-swap.json"
    try:
        native_okx = json.loads(native_okx_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Phase 10.5-C1 generated OKX ingestor config is unreadable") from error
    if (
        native_okx.get("authority") != authority
        or native_okx.get("config_revision") != acquisition.revision
        or any(item.get("feed") == "BAR" for item in native_okx.get("bindings", ()))
    ):
        raise ValueError("Phase 10.5-C1 native OKX ingestor still owns a final BAR")

    for name in ("core.json", "core-002.json", "core-003.json"):
        try:
            core = json.loads((runtime_dir / name).read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("Phase 10.5-C1 generated core config is unreadable") from error
        if core.get("authority") != authority:
            raise ValueError("Phase 10.5-C1 generated core differs from active authority")

    return {
        item.name: _sha256_bytes(item.read_bytes())
        for item in sorted(runtime_dir.iterdir())
        if item.is_file()
    }


def _new_checkpoint_path(
    *,
    authority_bytes: bytes,
    python_image_digest: str,
    previous_state_path: str,
) -> str:
    if not previous_state_path.startswith("/var/lib/qdl-stable/runtime/"):
        raise ValueError("Phase 10.5-C1 prior bar checkpoint path is outside stable runtime")
    nonce = _sha256_bytes(
        authority_bytes + b"\x00" + python_image_digest.encode("ascii")
    )[:20]
    result = f"/var/lib/qdl-stable/runtime/stable-crypto-bar-edge-r10-{nonce}.json"
    if result == previous_state_path:
        raise ValueError("Phase 10.5-C1 new bar checkpoint must differ from the prior path")
    return result


def _render_dotenv(values: Mapping[str, str]) -> bytes:
    if any("\n" in value or "\r" in value for value in values.values()):
        raise ValueError("Phase 10.5-C1 compose override contains an unsafe line break")
    return "".join(f"{key}={values[key]}\n" for key in sorted(values)).encode("utf-8")


def prepare_final_bar_repair(
    *,
    active_runtime_dir: Path,
    output_dir: Path,
    host_runtime_dir: Path,
    python_image_digest: str,
    rust_image_digest: str,
    source_commit: str,
    previous_bar_state_path: str,
) -> dict[str, Any]:
    """Write one isolated revision-10 C1 packet without runtime side effects."""

    active_runtime_dir = _require_absolute_path(
        active_runtime_dir, field="active runtime directory"
    )
    output_dir = _require_absolute_path(output_dir, field="output directory")
    host_runtime_dir = _require_absolute_path(host_runtime_dir, field="host runtime directory")
    if output_dir.exists():
        raise FileExistsError("Phase 10.5-C1 output directory must not already exist")
    _require_digest(python_image_digest, field="Python image")
    _require_digest(rust_image_digest, field="Rust image")
    _require_commit(source_commit)

    authority, authority_bytes = _load_active_authority(active_runtime_dir)
    catalog, acquisition = _load_revision_ten_acquisition()
    checkpoint_path = _new_checkpoint_path(
        authority_bytes=authority_bytes,
        python_image_digest=python_image_digest,
        previous_state_path=previous_bar_state_path,
    )

    output_dir.mkdir(mode=0o700, parents=True)
    runtime_dir = output_dir / "runtime"
    try:
        write_stable_runtime_bundle(
            runtime_dir,
            catalog=catalog,
            acquisition=acquisition,
            authority=authority,
        )
        # The bundle writer canonicalizes JSON.  C1 requires the mounted
        # authority contract to remain byte-identical to the current Rust core.
        (runtime_dir / "authority.json").write_bytes(authority_bytes)
        (runtime_dir / "authority.json").chmod(0o644)
        runtime_digests = _validate_generated_bundle(
            runtime_dir,
            authority=authority,
            authority_bytes=authority_bytes,
            acquisition=acquisition,
        )

        compose_environment = {
            "QDL_CONFIG_REVISION": "phase105c-final-bar-r10",
            "QDL_STABLE_BAR_STATE_PATH": checkpoint_path,
            "QDL_STABLE_PYTHON_IMAGE": python_image_digest,
            "QDL_STABLE_RUST_IMAGE": rust_image_digest,
            "QDL_STABLE_RUNTIME_DIR": str(host_runtime_dir),
        }
        compose_path = output_dir / "compose.env"
        compose_path.write_bytes(_render_dotenv(compose_environment))
        compose_path.chmod(0o640)

        packet_body: dict[str, Any] = {
            "schema": SCHEMA,
            "status": "PREPARED",
            "source_commit": source_commit,
            "acquisition_revision": acquisition.revision,
            "recreated_services": list(RECREATED_SERVICES),
            "excluded_services": [
                "rust_core",
                "rust_core_2",
                "rust_core_3",
                "kafka1",
                "kafka2",
                "kafka3",
                "stable_redis",
                "projector_v2",
                "projector_v2_2",
                "projector_v2_3",
                "data_layer_service",
                "trading_system",
                "alpha",
            ],
            "runtime": {
                "host_runtime_dir": str(host_runtime_dir),
                "runtime_files": runtime_digests,
                "authority_sha256": _sha256_bytes(authority_bytes),
                "authority_bytes_preserved": True,
                "authority_mode": authority["mode"],
                "authority_revision": authority["revision"],
                "python_image_digest": python_image_digest,
                "rust_image_digest": rust_image_digest,
            },
            "final_bar": {
                "binding_ids": sorted(FINAL_BAR_BINDINGS),
                "owner": "PYTHON_REST",
                "native_okx_bar_bindings": 0,
                "warmup_rows_max": WARMUP_ROWS,
                "previous_checkpoint_path": previous_bar_state_path,
                "new_checkpoint_path": checkpoint_path,
            },
            "compose_environment": compose_environment,
            "prohibited_operations": [
                "docker",
                "provider_io",
                "kafka",
                "redis",
                "sqlite",
                "authority_transition",
                "consumer_route_change",
                "offset_reset",
            ],
            "rollback": {
                "action": "recreate only recreated_services with prior runtime directory, "
                "prior Python image and prior checkpoint path",
                "durable_data_deletion": False,
            },
        }
        packet_sha256 = _sha256_bytes(_canonical_bytes(packet_body))
        packet = {
            "packet_sha256": packet_sha256,
            "confirmation_token": f"APPLY_QDL_PHASE105C_{packet_sha256[:16]}",
            **packet_body,
        }
        packet_path = output_dir / "final-bar-repair-packet.json"
        packet_path.write_bytes(
            json.dumps(packet, indent=2, sort_keys=True, ensure_ascii=True).encode("utf-8")
            + b"\n"
        )
        packet_path.chmod(0o640)
        return packet
    except Exception:
        # The caller intentionally chose a new, exact packet directory.  A
        # failed preparation never leaves a partial bundle eligible for apply.
        for item in sorted(output_dir.rglob("*"), reverse=True):
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                item.rmdir()
        output_dir.rmdir()
        raise
