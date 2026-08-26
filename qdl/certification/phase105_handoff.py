"""Pure, fail-closed helpers for the bounded Phase 10.5-C handoff packet."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Mapping


V1_FALLBACK_COMMIT = "85c25df631e263281bd546de69efcaf6146c93ef"
V1_FALLBACK_VERSION = "v1.2.2"
RUST_CORE_MEMORY_LIMIT_BYTES = 512 * 1024 * 1024

EXTERNAL_IDENTITY_SPECS = {
    "stable-monitoring-rs256-v1": {
        "subject": "spiffe://qdl/paper/monitoring-multivenue-stable",
        "public_key": "monitoring-jwt/public.pem",
    },
    "stable-alpha-okx-rs256-v1": {
        "subject": "spiffe://qdl/paper/alpha-okx-stable",
        "public_key": "alpha-okx-jwt/public.pem",
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


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: str | Path) -> str:
    return sha256_bytes(Path(path).read_bytes())


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


def prepare_handoff_environment(
    base_environment: Mapping[str, str],
    *,
    extension_dir: str | Path,
    python_image: str,
) -> dict[str, str]:
    """Append only the two approved external public signing keys.

    Private key paths deliberately never enter the query/stream environment.
    """
    if not python_image or "\n" in python_image:
        raise ValueError("Phase 10.5-C Python image reference is invalid")
    required = {"QDL_STABLE_JWT_KEYS_JSON", "QDL_STABLE_RUNTIME_DIR"}
    missing = sorted(required - set(base_environment))
    if missing:
        raise ValueError(f"Phase 10.5-C base environment is missing {missing}")
    try:
        keys = json.loads(base_environment["QDL_STABLE_JWT_KEYS_JSON"])
    except json.JSONDecodeError as error:
        raise ValueError("stable JWT public keyring is invalid JSON") from error
    if not isinstance(keys, dict) or not all(
        isinstance(key, str) and isinstance(value, str) and "BEGIN PUBLIC KEY" in value
        for key, value in keys.items()
    ):
        raise ValueError("stable JWT public keyring is invalid")
    missing_existing = sorted(
        {"stable-trading-system-rs256-v1", "stable-alpha-binance-rs256-v1"} - set(keys)
    )
    if missing_existing:
        raise ValueError(f"stable JWT public keyring misses existing identities {missing_existing}")

    extension = Path(extension_dir)
    result = dict(base_environment)
    for key_id, spec in EXTERNAL_IDENTITY_SPECS.items():
        public_key = (extension / str(spec["public_key"])).read_text(encoding="utf-8")
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


def v1_image_attestation(
    image: Mapping[str, object],
    *,
    source_commit: str,
    source_tree: str,
    dockerfile_sha256: str,
) -> dict[str, object]:
    """Validate Docker inspect data and return secret-free provenance evidence."""
    if source_commit != V1_FALLBACK_COMMIT or not _COMMIT.fullmatch(source_commit):
        raise ValueError("V1 fallback source commit is not the frozen v1.2.2 commit")
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
    return {
        "schema": "qdl.phase105c.handoff-packet.v1",
        "status": "PREPARED",
        "v2_python_image": environment["QDL_STABLE_PYTHON_IMAGE"],
        "v2_rust_image": environment["QDL_STABLE_RUST_IMAGE"],
        "runtime_dir": environment["QDL_STABLE_RUNTIME_DIR"],
        "rust_core_memory_limit_bytes": RUST_CORE_MEMORY_LIMIT_BYTES,
        "recreated_services": [
            "data_layer_service",
            "rust_core",
            "query_v2_1",
            "query_v2_2",
            "stream_v2_active",
            "stream_v2_passive",
        ],
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
