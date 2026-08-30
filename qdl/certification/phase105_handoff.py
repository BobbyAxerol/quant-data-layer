"""Pure, fail-closed helpers for the bounded Phase 10.5-C handoff packet."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Mapping


V1_FALLBACK_COMMIT = "b259b63ae73cf5f8bf75463578c9f5cb477c6c08"
V1_FALLBACK_VERSION = "v1.2.3"

EXTERNAL_IDENTITY_SPECS = {
    "stable-monitoring-rs256-v1": {
        "subject": "spiffe://qdl/paper/monitoring-multivenue-stable",
        "public_key": "monitoring-jwt/public.pem",
    },
    "stable-alpha-okx-rs256-v1": {
        "subject": "spiffe://qdl/paper/alpha-okx-stable",
        "public_key": "alpha-okx-jwt/public.pem",
    },
    "stable-reference-l2-rs256-v1": {
        "subject": "spiffe://qdl/paper/reference-l2-stable",
        "public_key": "reference-l2-jwt/public.pem",
    },
}

ALL_KEY_SUBJECTS = {
    "stable-trading-system-rs256-v1": "spiffe://qdl/paper/trading-system-stable",
    "stable-alpha-binance-rs256-v1": "spiffe://qdl/paper/alpha-binance-stable",
    **{
        key_id: str(value["subject"])
        for key_id, value in EXTERNAL_IDENTITY_SPECS.items()
    },
}

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
V1_PROVENANCE_FIELDS = frozenset({
    "schema",
    "status",
    "image_id",
    "source_commit",
    "source_tree",
    "dockerfile_sha256",
    "version",
})
V1_RUNTIME_BINDING_FIELDS = frozenset({
    "schema",
    "status",
    "service",
    "container_image_id",
    "container_id_sha256",
    "v1_provenance_sha256",
})
_ACTIVE_RUNTIME_SCHEMA = "qdl.v2.shared-primary-handoff-packet.v2"
_FINAL_BAR_RUNTIME_SCHEMA = "qdl.phase105c.final-bar-repair.v1"
_FINAL_BAR_ROLLBACK_SCHEMA = "qdl.phase105c.final-bar-repair.rollback.v1"
_FINAL_BAR_RECREATED_SERVICES = (
    "ingestor_okx_swap",
    "binance_bar_edge",
    "rust_core",
    "query_v2_1",
    "query_v2_2",
    "stream_v2_active",
    "stream_v2_passive",
)
_FINAL_BAR_ROLLBACK_ENTRY_FIELDS = frozenset({
    "image_digest",
    "runtime_dir",
    "checkpoint_path",
})
_ACTIVE_RUNTIME_SELECTORS = (
    "QDL_CONFIG_REVISION",
    "QDL_STABLE_AUTHORITY_MODE",
    "QDL_STABLE_AUTHORITY_REVISION",
    "QDL_STABLE_RUNTIME_DIR",
    "QDL_STABLE_RUST_IMAGE",
)
_ACTIVE_QUERY_ENV_FIELDS = (
    "QDL_STABLE_INTERNAL_INGEST_SECRET",
    "QDL_STABLE_CURSOR_KEYS_JSON",
    "QDL_DATA_JWT_KEYS_JSON",
    "QDL_STABLE_SCHEMA_DIGEST",
    "QDL_STABLE_AUTHORITY_MODE",
    "QDL_STABLE_AUTHORITY_REVISION",
    "QDL_CONFIG_REVISION",
)
_ACTIVE_QUERY_COMMITMENT_SCHEMA = "qdl.phase105.active-query-env-commitment.v1"
_ACTIVE_QUERY_COMMITMENT_FIELDS = frozenset({
    "schema",
    "status",
    "service",
    "container_image_id",
    "container_id_sha256",
    "runtime_packet_sha256",
    "verified_keys",
    "environment_sha256",
})
_C2_RECREATED_SERVICES = (
    "query_v2_1",
    "query_v2_2",
    "stream_v2_active",
    "stream_v2_passive",
)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: str | Path) -> str:
    return sha256_bytes(Path(path).read_bytes())


def _canonical_sha256(value: object) -> str:
    return sha256_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def validate_frozen_v1_provenance(raw: object) -> dict[str, str]:
    """Validate the immutable V1 identity without importing runtime adapters."""
    if not isinstance(raw, dict) or set(raw) != V1_PROVENANCE_FIELDS:
        raise ValueError("Phase 10.5 V1 provenance fields differ from the frozen contract")
    if (
        raw.get("schema") != "qdl.phase105.v1-fallback-provenance.v1"
        or raw.get("status") != "PASS"
        or raw.get("source_commit") != V1_FALLBACK_COMMIT
        or raw.get("version") != V1_FALLBACK_VERSION
        or not isinstance(raw.get("image_id"), str)
        or _DIGEST.fullmatch(str(raw["image_id"])) is None
        or not isinstance(raw.get("source_tree"), str)
        or _COMMIT.fullmatch(str(raw["source_tree"])) is None
        or not isinstance(raw.get("dockerfile_sha256"), str)
        or _SHA256.fullmatch(str(raw["dockerfile_sha256"])) is None
    ):
        raise ValueError("Phase 10.5 V1 fallback provenance is not frozen and attestable")
    return {
        "image_id": str(raw["image_id"]),
        "source_commit": str(raw["source_commit"]),
        "provenance_sha256": _canonical_sha256(raw),
    }


def validate_frozen_v1_runtime_binding(
    provenance: Mapping[str, object], raw: object
) -> dict[str, str]:
    """Verify a host-side V1 serving-container binding without Docker access."""
    if not isinstance(raw, dict) or set(raw) != V1_RUNTIME_BINDING_FIELDS:
        raise ValueError("Phase 10.5 V1 runtime binding fields differ from the frozen contract")
    image_id = provenance.get("image_id")
    provenance_sha256 = provenance.get("provenance_sha256")
    if (
        raw.get("schema") != "qdl.phase105.v1-runtime-binding.v1"
        or raw.get("status") != "PASS"
        or raw.get("service") != "data_layer_service"
        or raw.get("container_image_id") != image_id
        or not isinstance(image_id, str)
        or _DIGEST.fullmatch(image_id) is None
        or not isinstance(raw.get("container_id_sha256"), str)
        or _SHA256.fullmatch(str(raw["container_id_sha256"])) is None
        or raw.get("v1_provenance_sha256") != provenance_sha256
        or not isinstance(provenance_sha256, str)
        or _SHA256.fullmatch(provenance_sha256) is None
    ):
        raise ValueError("Phase 10.5 V1 runtime binding is not current and attestable")
    return {
        "service": "data_layer_service",
        "image_id": image_id,
        "container_id_sha256": str(raw["container_id_sha256"]),
        "binding_sha256": _canonical_sha256(raw),
    }


def load_dotenv(path: str | Path) -> dict[str, str]:
    """Parse the private, generated runtime env format without shell expansion."""
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:]
        if "=" not in line:
            raise ValueError(f"runtime env line {line_number} is not KEY=VALUE")
        key, value = line.split("=", 1)
        if not key or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError(f"runtime env line {line_number} has an invalid key")
        if key in values:
            raise ValueError(f"runtime env repeats {key}")
        if len(value) >= 2 and value[:1] == value[-1:] and value[:1] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    if not values:
        raise ValueError("runtime env is empty")
    return values


def render_dotenv(values: Mapping[str, str]) -> str:
    """Keep generated env deterministic without exposing it in source evidence."""
    if not values:
        raise ValueError("runtime env is empty")
    for key, value in values.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) or "\n" in value:
            raise ValueError("runtime env contains an invalid key or multiline value")
    return "".join(f"{key}={values[key]}\n" for key in sorted(values))


def active_runtime_binding(
    base_environment: Mapping[str, str],
    packet: Mapping[str, object],
) -> dict[str, object]:
    """Bind a handoff environment to one sealed, fenced V2 runtime packet.

    The historical candidate environment intentionally retains private values.
    The sealed runtime packet may replace only the small non-secret selector
    allowlist needed for Compose to mount the active authority record.  This
    prevents a stale host runtime path from being carried into a recreate.
    """
    schema = packet.get("schema")
    if schema == _FINAL_BAR_RUNTIME_SCHEMA:
        return _final_bar_runtime_binding(packet)
    if schema != _ACTIVE_RUNTIME_SCHEMA:
        raise ValueError("Phase 10.5-C active runtime packet schema is invalid")
    authority = packet.get("authority")
    compose = packet.get("compose_environment")
    runtime_bundle = packet.get("runtime_bundle")
    if not isinstance(authority, Mapping) or not isinstance(compose, Mapping):
        raise ValueError("Phase 10.5-C active runtime packet is incomplete")
    if not isinstance(runtime_bundle, Mapping):
        raise ValueError("Phase 10.5-C active runtime bundle is invalid")
    if (
        authority.get("mode") != "RUST_PRIMARY"
        or authority.get("public_write_allowed") is not False
        or authority.get("legacy_write_allowed") is not False
    ):
        raise ValueError("Phase 10.5-C active authority is not fenced RUST_PRIMARY")
    contract_digest = authority.get("contract_digest")
    if (
        not isinstance(contract_digest, str)
        or not re.fullmatch(r"[0-9a-f]{64}", contract_digest)
        or base_environment.get("QDL_STABLE_SCHEMA_DIGEST") != contract_digest
    ):
        raise ValueError("Phase 10.5-C active runtime contract digest mismatches base env")

    selectors: dict[str, str] = {}
    for key in _ACTIVE_RUNTIME_SELECTORS:
        value = compose.get(key)
        if not isinstance(value, str) or not value or "\n" in value:
            raise ValueError(f"Phase 10.5-C active runtime selector is invalid: {key}")
        selectors[key] = value
    if selectors["QDL_STABLE_AUTHORITY_MODE"] != authority["mode"]:
        raise ValueError("Phase 10.5-C active authority mode selector mismatches packet")
    if selectors["QDL_STABLE_AUTHORITY_REVISION"] != str(authority.get("revision")):
        raise ValueError("Phase 10.5-C active authority revision selector mismatches packet")
    if not _DIGEST.fullmatch(selectors["QDL_STABLE_RUST_IMAGE"]):
        raise ValueError("Phase 10.5-C active Rust image selector is invalid")
    if runtime_bundle.get("rust_image_digest") != selectors["QDL_STABLE_RUST_IMAGE"]:
        raise ValueError("Phase 10.5-C active Rust image mismatches runtime bundle")
    runtime_path = Path(selectors["QDL_STABLE_RUNTIME_DIR"])
    if not runtime_path.is_absolute() or runtime_path.parts[:5] != (
        "/", "home", "bobby", ".local", "state"
    ):
        raise ValueError("Phase 10.5-C active runtime directory is outside the governed state root")
    if "qdl-v2" not in runtime_path.parts:
        raise ValueError("Phase 10.5-C active runtime directory is not a QDL V2 state path")
    return {
        "packet_schema": schema,
        "packet_sha256": sha256_bytes(
            json.dumps(packet, sort_keys=True, separators=(",", ":")).encode()
        ),
        "selectors": selectors,
    }


def _governed_runtime_path(value: object) -> str:
    if not isinstance(value, str) or not value or "\n" in value:
        raise ValueError("Phase 10.5-C active runtime directory is invalid")
    runtime_path = Path(value)
    if not runtime_path.is_absolute() or runtime_path.parts[:5] != (
        "/", "home", "bobby", ".local", "state"
    ):
        raise ValueError("Phase 10.5-C active runtime directory is outside the governed state root")
    if "qdl-v2" not in runtime_path.parts or ".." in runtime_path.parts:
        raise ValueError("Phase 10.5-C active runtime directory is not a QDL V2 state path")
    return value


def _final_bar_runtime_binding(packet: Mapping[str, object]) -> dict[str, object]:
    """Accept only a self-consistent, exact-rollback final-BAR repair packet."""
    runtime = packet.get("runtime")
    compose = packet.get("compose_environment")
    if not isinstance(runtime, Mapping) or not isinstance(compose, Mapping):
        raise ValueError("Phase 10.5-C final-BAR runtime packet is incomplete")
    authority_sha256 = runtime.get("authority_sha256")
    runtime_files = runtime.get("runtime_files")
    authority_revision = runtime.get("authority_revision")
    if (
        runtime.get("authority_bytes_preserved") is not True
        or runtime.get("authority_mode") != "RUST_PRIMARY"
        or isinstance(authority_revision, bool)
        or not isinstance(authority_revision, int)
        or authority_revision <= 0
        or not isinstance(authority_sha256, str)
        or _SHA256.fullmatch(authority_sha256) is None
        or not isinstance(runtime_files, Mapping)
        or runtime_files.get("authority.json") != authority_sha256
    ):
        raise ValueError("Phase 10.5-C final-BAR authority evidence is invalid")
    host_runtime_dir = _governed_runtime_path(runtime.get("host_runtime_dir"))
    rust_image = runtime.get("rust_image_digest")
    python_image = runtime.get("python_image_digest")
    if (
        not isinstance(rust_image, str)
        or _DIGEST.fullmatch(rust_image) is None
        or not isinstance(python_image, str)
        or _DIGEST.fullmatch(python_image) is None
        or compose.get("QDL_STABLE_RUNTIME_DIR") != host_runtime_dir
        or compose.get("QDL_STABLE_RUST_IMAGE") != rust_image
        or compose.get("QDL_STABLE_PYTHON_IMAGE") != python_image
    ):
        raise ValueError("Phase 10.5-C final-BAR runtime image or path mismatches packet")
    config_revision = compose.get("QDL_CONFIG_REVISION")
    if not isinstance(config_revision, str) or not config_revision or "\n" in config_revision:
        raise ValueError("Phase 10.5-C final-BAR config revision is invalid")
    final_bar = packet.get("final_bar")
    rollback = packet.get("rollback")
    if not isinstance(final_bar, Mapping) or not isinstance(rollback, Mapping):
        raise ValueError("Phase 10.5-C final-BAR rollback evidence is missing")
    previous_checkpoint = final_bar.get("previous_checkpoint_path")
    if (
        not isinstance(previous_checkpoint, str)
        or not previous_checkpoint.startswith("/var/lib/qdl-stable/runtime/")
    ):
        raise ValueError("Phase 10.5-C final-BAR previous checkpoint is invalid")
    if (
        set(rollback) != {"schema", "services", "durable_data_deletion"}
        or rollback.get("schema") != _FINAL_BAR_ROLLBACK_SCHEMA
        or rollback.get("durable_data_deletion") is not False
        or not isinstance(rollback.get("services"), Mapping)
    ):
        raise ValueError("Phase 10.5-C final-BAR rollback schema is invalid")
    raw_services = rollback["services"]
    if set(raw_services) != set(_FINAL_BAR_RECREATED_SERVICES):
        raise ValueError("Phase 10.5-C final-BAR rollback services differ from recreated roles")
    sealed_services: dict[str, dict[str, object]] = {}
    for service in _FINAL_BAR_RECREATED_SERVICES:
        item = raw_services[service]
        if not isinstance(item, Mapping) or set(item) != _FINAL_BAR_ROLLBACK_ENTRY_FIELDS:
            raise ValueError(f"Phase 10.5-C final-BAR rollback entry is invalid for {service}")
        image = item.get("image_digest")
        prior_runtime = item.get("runtime_dir")
        checkpoint = item.get("checkpoint_path")
        if (
            not isinstance(image, str)
            or _DIGEST.fullmatch(image) is None
            or _governed_runtime_path(prior_runtime) == host_runtime_dir
        ):
            raise ValueError(f"Phase 10.5-C final-BAR rollback entry mismatches packet for {service}")
        if service == "binance_bar_edge":
            if checkpoint != previous_checkpoint:
                raise ValueError("Phase 10.5-C final-BAR rollback checkpoint mismatches packet")
        elif checkpoint is not None:
            raise ValueError("Phase 10.5-C final-BAR rollback checkpoint is not scoped to bar edge")
        sealed_services[service] = {
            "image_digest": image,
            "runtime_dir": prior_runtime,
            "checkpoint_path": checkpoint,
        }
    selectors = {
        "QDL_CONFIG_REVISION": config_revision,
        "QDL_STABLE_AUTHORITY_MODE": "RUST_PRIMARY",
        "QDL_STABLE_AUTHORITY_REVISION": str(authority_revision),
        "QDL_STABLE_RUNTIME_DIR": host_runtime_dir,
        "QDL_STABLE_RUST_IMAGE": rust_image,
    }
    return {
        "packet_schema": _FINAL_BAR_RUNTIME_SCHEMA,
        "packet_sha256": sha256_bytes(
            json.dumps(packet, sort_keys=True, separators=(",", ":")).encode()
        ),
        "selectors": selectors,
        "python_image_digest": python_image,
        "rollback_sha256": _canonical_sha256({
            "schema": _FINAL_BAR_ROLLBACK_SCHEMA,
            "services": sealed_services,
            "durable_data_deletion": False,
        }),
    }


def active_query_environment_commitment(
    base_environment: Mapping[str, str],
    query_environment: Mapping[str, str],
    runtime_binding: Mapping[str, object],
    expected_jwt_keyring: Mapping[str, str],
) -> dict[str, object]:
    """Compare the active reader in memory and retain only a commitment hash."""
    selectors = runtime_binding.get("selectors")
    if not isinstance(selectors, Mapping) or set(selectors) != set(_ACTIVE_RUNTIME_SELECTORS):
        raise ValueError("Phase 10.5-C active runtime binding selectors are invalid")
    missing = sorted(set(_ACTIVE_QUERY_ENV_FIELDS) - set(query_environment))
    if missing:
        raise ValueError(f"Phase 10.5-C active query environment is missing {missing}")
    expected = {
        "QDL_STABLE_INTERNAL_INGEST_SECRET": base_environment.get(
            "QDL_STABLE_INTERNAL_INGEST_SECRET"
        ),
        "QDL_STABLE_CURSOR_KEYS_JSON": base_environment.get(
            "QDL_STABLE_CURSOR_KEYS_JSON"
        ),
        "QDL_STABLE_SCHEMA_DIGEST": base_environment.get("QDL_STABLE_SCHEMA_DIGEST"),
        "QDL_STABLE_AUTHORITY_MODE": selectors["QDL_STABLE_AUTHORITY_MODE"],
        "QDL_STABLE_AUTHORITY_REVISION": selectors["QDL_STABLE_AUTHORITY_REVISION"],
        "QDL_CONFIG_REVISION": selectors["QDL_CONFIG_REVISION"],
    }
    if any(not isinstance(value, str) or not value for value in expected.values()):
        raise ValueError("Phase 10.5-C controlled query reference is incomplete")
    for key in _ACTIVE_QUERY_ENV_FIELDS:
        if key == "QDL_DATA_JWT_KEYS_JSON":
            continue
        if query_environment[key] != expected[key]:
            raise ValueError(f"Phase 10.5-C active query environment mismatches controlled reference: {key}")
    for key in (
        "QDL_CONFIG_REVISION",
        "QDL_STABLE_AUTHORITY_MODE",
        "QDL_STABLE_AUTHORITY_REVISION",
    ):
        if query_environment[key] != selectors[key]:
            raise ValueError(f"Phase 10.5-C active query selector mismatches sealed runtime: {key}")
    try:
        cursor_keys = json.loads(query_environment["QDL_STABLE_CURSOR_KEYS_JSON"])
        jwt_keys = json.loads(query_environment["QDL_DATA_JWT_KEYS_JSON"])
    except json.JSONDecodeError as error:
        raise ValueError("Phase 10.5-C active query cursor/JWT material is invalid JSON") from error
    if not isinstance(cursor_keys, dict) or not cursor_keys or not all(
        isinstance(key, str) and isinstance(value, str) and value
        for key, value in cursor_keys.items()
    ):
        raise ValueError("Phase 10.5-C active query cursor keyring is invalid")
    if set(expected_jwt_keyring) != set(ALL_KEY_SUBJECTS) or not all(
        isinstance(value, str) and "BEGIN PUBLIC KEY" in value and "PRIVATE KEY" not in value
        for value in expected_jwt_keyring.values()
    ):
        raise ValueError("Phase 10.5-C expected query JWT keyring is invalid")
    if set(jwt_keys) != set(expected_jwt_keyring) or not all(
        isinstance(value, str) and "BEGIN PUBLIC KEY" in value and "PRIVATE KEY" not in value
        for value in jwt_keys.values()
    ) or jwt_keys != dict(expected_jwt_keyring):
        raise ValueError("Phase 10.5-C active query JWT keyring mismatches public overlay")
    expected["QDL_DATA_JWT_KEYS_JSON"] = json.dumps(
        dict(expected_jwt_keyring), sort_keys=True, separators=(",", ":")
    )
    return {
        "runtime_packet_sha256": runtime_binding.get("packet_sha256"),
        "verified_keys": list(_ACTIVE_QUERY_ENV_FIELDS),
        "environment_sha256": sha256_bytes(
            render_dotenv({key: expected[key] for key in _ACTIVE_QUERY_ENV_FIELDS}).encode()
        ),
    }


def validate_active_query_environment_commitment(
    base_environment: Mapping[str, str],
    runtime_binding: Mapping[str, object],
    raw: object,
    expected_jwt_keyring: Mapping[str, str],
) -> dict[str, object]:
    """Validate a payload-free proof that the current query uses the sealed env."""
    if not isinstance(raw, dict) or set(raw) != _ACTIVE_QUERY_COMMITMENT_FIELDS:
        raise ValueError("Phase 10.5-C active query commitment fields are invalid")
    if (
        raw.get("schema") != _ACTIVE_QUERY_COMMITMENT_SCHEMA
        or raw.get("status") != "PASS"
        or raw.get("service") != "query_v2_1"
        or not isinstance(raw.get("container_image_id"), str)
        or _DIGEST.fullmatch(str(raw["container_image_id"])) is None
        or not isinstance(raw.get("container_id_sha256"), str)
        or _SHA256.fullmatch(str(raw["container_id_sha256"])) is None
    ):
        raise ValueError("Phase 10.5-C active query commitment identity is invalid")
    expected = active_query_environment_commitment(
        base_environment,
        {
            "QDL_STABLE_INTERNAL_INGEST_SECRET": base_environment.get(
                "QDL_STABLE_INTERNAL_INGEST_SECRET", ""
            ),
            "QDL_STABLE_CURSOR_KEYS_JSON": base_environment.get(
                "QDL_STABLE_CURSOR_KEYS_JSON", ""
            ),
            "QDL_DATA_JWT_KEYS_JSON": json.dumps(
                dict(expected_jwt_keyring), sort_keys=True, separators=(",", ":")
            ),
            "QDL_STABLE_SCHEMA_DIGEST": base_environment.get("QDL_STABLE_SCHEMA_DIGEST", ""),
            "QDL_STABLE_AUTHORITY_MODE": str(runtime_binding.get("selectors", {}).get(
                "QDL_STABLE_AUTHORITY_MODE", ""
            )),
            "QDL_STABLE_AUTHORITY_REVISION": str(runtime_binding.get("selectors", {}).get(
                "QDL_STABLE_AUTHORITY_REVISION", ""
            )),
            "QDL_CONFIG_REVISION": str(runtime_binding.get("selectors", {}).get(
                "QDL_CONFIG_REVISION", ""
            )),
        },
        runtime_binding,
        expected_jwt_keyring,
    )
    if (
        raw.get("runtime_packet_sha256") != expected["runtime_packet_sha256"]
        or raw.get("verified_keys") != expected["verified_keys"]
        or raw.get("environment_sha256") != expected["environment_sha256"]
    ):
        raise ValueError("Phase 10.5-C active query commitment mismatches controlled reference")
    sealed_python_image = runtime_binding.get("python_image_digest")
    if sealed_python_image is not None and raw["container_image_id"] != sealed_python_image:
        raise ValueError("Phase 10.5-C active query image differs from sealed final-BAR runtime")
    return {
        "binding_sha256": _canonical_sha256(raw),
        "environment_sha256": str(raw["environment_sha256"]),
        "verified_keys": list(_ACTIVE_QUERY_ENV_FIELDS),
    }


def prepare_handoff_environment(
    base_environment: Mapping[str, str],
    *,
    extension_dir: str | Path,
    python_image: str,
    runtime_binding: Mapping[str, object] | None = None,
) -> dict[str, str]:
    """Append only approved external public signing keys.

    Private key paths deliberately never enter the query/stream environment.
    """
    if not python_image or "\n" in python_image:
        raise ValueError("Phase 10.5-C Python image reference is invalid")
    required = {"QDL_STABLE_JWT_KEYS_JSON", "QDL_STABLE_RUNTIME_DIR"}
    missing = sorted(required - set(base_environment))
    if missing:
        raise ValueError(f"Phase 10.5-C base environment is missing {missing}")
    result = dict(base_environment)
    if runtime_binding is not None:
        selectors = runtime_binding.get("selectors")
        if not isinstance(selectors, Mapping) or set(selectors) != set(_ACTIVE_RUNTIME_SELECTORS):
            raise ValueError("Phase 10.5-C active runtime binding selectors are invalid")
        for key in _ACTIVE_RUNTIME_SELECTORS:
            value = selectors[key]
            if not isinstance(value, str) or not value or "\n" in value:
                raise ValueError(f"Phase 10.5-C active runtime binding is invalid: {key}")
            result[key] = value
        sealed_python_image = runtime_binding.get("python_image_digest")
        if sealed_python_image is not None:
            if (
                not isinstance(sealed_python_image, str)
                or _DIGEST.fullmatch(sealed_python_image) is None
                or python_image != sealed_python_image
            ):
                raise ValueError("Phase 10.5-C Python image differs from sealed final-BAR runtime")
    try:
        keys = json.loads(result["QDL_STABLE_JWT_KEYS_JSON"])
    except json.JSONDecodeError as error:
        raise ValueError("stable JWT public keyring is invalid JSON") from error
    if not isinstance(keys, dict) or not all(
        isinstance(key, str)
        and isinstance(value, str)
        and "BEGIN PUBLIC KEY" in value
        and "PRIVATE KEY" not in value
        for key, value in keys.items()
    ):
        raise ValueError("stable JWT public keyring is invalid")
    missing_existing = sorted(
        {"stable-trading-system-rs256-v1", "stable-alpha-binance-rs256-v1"} - set(keys)
    )
    if missing_existing:
        raise ValueError(f"stable JWT public keyring misses existing identities {missing_existing}")
    prior_subjects_raw = result.get("QDL_STABLE_JWT_KEY_SUBJECTS_JSON")
    if prior_subjects_raw is not None:
        try:
            prior_subjects = json.loads(prior_subjects_raw)
        except json.JSONDecodeError as error:
            raise ValueError("stable JWT key-subject bindings are invalid JSON") from error
        if not isinstance(prior_subjects, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in prior_subjects.items()
        ):
            raise ValueError("stable JWT key-subject bindings are invalid")
        unknown_subjects = sorted(set(prior_subjects) - set(ALL_KEY_SUBJECTS))
        if unknown_subjects:
            raise ValueError(
                "stable JWT key-subject bindings contain unapproved identities "
                f"{unknown_subjects}"
            )
        mismatched_subjects = sorted(
            key
            for key, subject in prior_subjects.items()
            if ALL_KEY_SUBJECTS[key] != subject
        )
        if mismatched_subjects:
            raise ValueError(
                "stable JWT key-subject bindings conflict with approved identities "
                f"{mismatched_subjects}"
            )
    extension = Path(extension_dir)
    for key_id, spec in EXTERNAL_IDENTITY_SPECS.items():
        public_key_path = extension / str(spec["public_key"])
        if not public_key_path.is_file():
            raise ValueError(f"Phase 10.5-C {key_id} public key is missing")
        public_key = public_key_path.read_text(encoding="utf-8")
        if "BEGIN PUBLIC KEY" not in public_key or "PRIVATE KEY" in public_key:
            raise ValueError(f"Phase 10.5-C {key_id} public key is invalid")
        prior = keys.get(key_id)
        if prior is not None and prior != public_key:
            raise ValueError(f"Phase 10.5-C {key_id} conflicts with the existing keyring")
        keys[key_id] = public_key
    if set(keys) != set(ALL_KEY_SUBJECTS):
        raise ValueError("Phase 10.5-C JWT keyring does not exactly match approved identities")
    result["QDL_STABLE_JWT_KEYS_JSON"] = json.dumps(keys, sort_keys=True, separators=(",", ":"))
    result["QDL_STABLE_JWT_KEY_SUBJECTS_JSON"] = json.dumps(
        ALL_KEY_SUBJECTS, sort_keys=True, separators=(",", ":")
    )
    result["QDL_STABLE_PYTHON_IMAGE"] = python_image
    return result


def public_handoff_overlay(environment: Mapping[str, str]) -> dict[str, str]:
    """Select the only two public C2 environment values Compose may layer."""
    keys = ("QDL_STABLE_JWT_KEYS_JSON", "QDL_STABLE_JWT_KEY_SUBJECTS_JSON")
    overlay = {key: environment.get(key, "") for key in keys}
    if any(not value or "PRIVATE KEY" in value for value in overlay.values()):
        raise ValueError("Phase 10.5-C public handoff overlay is invalid")
    return overlay


def v1_image_attestation(
    image: Mapping[str, object],
    *,
    source_commit: str,
    source_tree: str,
    dockerfile_sha256: str,
) -> dict[str, object]:
    """Validate Docker inspect data and return secret-free provenance evidence."""
    if source_commit != V1_FALLBACK_COMMIT or not _COMMIT.fullmatch(source_commit):
        raise ValueError("V1 fallback source commit is not the frozen release commit")
    if not re.fullmatch(r"[0-9a-f]{40}", source_tree):
        raise ValueError("V1 fallback source tree is invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", dockerfile_sha256):
        raise ValueError("V1 fallback Dockerfile digest is invalid")
    image_id = image.get("Id")
    config = image.get("Config")
    if not isinstance(image_id, str) or not _DIGEST.fullmatch(image_id):
        raise ValueError("V1 fallback image ID is invalid")
    if not isinstance(config, Mapping):
        raise ValueError("V1 fallback image config is invalid")
    labels = config.get("Labels")
    if not isinstance(labels, Mapping):
        raise ValueError("V1 fallback image labels are absent")
    expected = {
        "org.opencontainers.image.revision": source_commit,
        "org.opencontainers.image.version": V1_FALLBACK_VERSION,
        "io.qdl.source-tree": source_tree,
        "io.qdl.dockerfile-sha256": dockerfile_sha256,
    }
    if {key: labels.get(key) for key in expected} != expected:
        raise ValueError("V1 fallback image labels do not attest the frozen source")
    return {
        "schema": "qdl.phase105.v1-fallback-provenance.v1",
        "status": "PASS",
        "image_id": image_id,
        "source_commit": source_commit,
        "source_tree": source_tree,
        "dockerfile_sha256": dockerfile_sha256,
        "version": V1_FALLBACK_VERSION,
    }


def handoff_packet(
    *,
    environment: Mapping[str, str],
    extension_dir: str | Path,
    v1_attestation: Mapping[str, object],
    runtime_binding: Mapping[str, object] | None = None,
    query_environment_commitment: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Make an auditable packet without serializing a secret-bearing env file."""
    if v1_attestation.get("status") != "PASS":
        raise ValueError("Phase 10.5-C V1 fallback provenance is not accepted")
    extension = Path(extension_dir)
    client_bundle = extension / "client-ca-bundle.crt"
    if not client_bundle.is_file():
        raise ValueError("Phase 10.5-C additive client CA bundle is missing")
    required = {
        "QDL_STABLE_PYTHON_IMAGE",
        "QDL_STABLE_RUST_IMAGE",
        "QDL_STABLE_RUNTIME_DIR",
        "QDL_STABLE_JWT_KEYS_JSON",
        "QDL_STABLE_JWT_KEY_SUBJECTS_JSON",
    }
    missing = sorted(required - set(environment))
    if missing:
        raise ValueError(f"Phase 10.5-C handoff environment is missing {missing}")
    public_keyring = json.loads(environment["QDL_STABLE_JWT_KEYS_JSON"])
    subjects = json.loads(environment["QDL_STABLE_JWT_KEY_SUBJECTS_JSON"])
    if set(public_keyring) != set(ALL_KEY_SUBJECTS) or subjects != ALL_KEY_SUBJECTS:
        raise ValueError("Phase 10.5-C packet identity map is not exact")
    packet = {
        "schema": "qdl.phase105c.handoff-packet.v1",
        "status": "PREPARED",
        "v2_python_image": environment["QDL_STABLE_PYTHON_IMAGE"],
        "v2_rust_image": environment["QDL_STABLE_RUST_IMAGE"],
        "runtime_dir": environment["QDL_STABLE_RUNTIME_DIR"],
        "recreated_services": list(_C2_RECREATED_SERVICES),
        "client_authority_paths": [
            "/stable-certs/query/client-ca-bundle.crt",
            "/stable-certs/stream/client-ca-bundle.crt",
        ],
        "external_client_ca_sha256": sha256_file(client_bundle),
        "jwt_key_ids": sorted(public_keyring),
        "jwt_subjects": subjects,
        "v1_fallback_provenance_sha256": sha256_bytes(
            json.dumps(v1_attestation, sort_keys=True, separators=(",", ":")).encode()
        ),
        "exclusions": [
            "kafka", "stable_redis", "sqlite", "offset_reset", "authority_cas",
            "trading_system", "alpha_containers", "orders", "provider_direct_calls",
        ],
    }
    if runtime_binding is not None:
        packet["active_runtime_packet_sha256"] = runtime_binding.get("packet_sha256")
        packet["active_runtime_selectors"] = runtime_binding.get("selectors")
    if query_environment_commitment is not None:
        packet["active_query_commitment_sha256"] = query_environment_commitment.get("binding_sha256")
        packet["active_query_environment_sha256"] = query_environment_commitment.get("environment_sha256")
        packet["active_query_verified_keys"] = query_environment_commitment.get("verified_keys")
    return packet
