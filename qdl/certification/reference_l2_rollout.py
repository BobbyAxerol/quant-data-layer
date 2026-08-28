"""Prepare one bounded Reference/L2 successor bundle without runtime I/O.

The active V2 runtime already has four paper-client identities.  This module
adds the fifth, ``stable-reference-l2-rs256-v1``, without rotating the prior
external CA, server certificate, Kafka mesh, authority record or cursor keys.
It is deliberately filesystem-only so a generated bundle can be inspected and
tested before the separately approved Compose roll.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

from qdl.certification.phase105_handoff import (
    load_dotenv,
    prepare_handoff_environment,
    render_dotenv,
    sha256_bytes,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    validate_shared_authority_record,
    write_stable_runtime_bundle,
)


ROOT = Path(__file__).resolve().parents[2]
STATE_ROOT = Path("/home/bobby/.local/state/qdl-v2")
SCHEMA = "qdl.phasec36.reference-l2-rollout.v1"
CONFIRM = "PREPARE_QDL_REFERENCE_L2_ROLLOUT"
ROLLING_SERVICES = (
    "ingestor_binance_usdm",
    "ingestor_okx_swap",
    "rust_core",
    "binance_bar_edge",
    "projector_v2",
    "projector_v2_2",
    "projector_v2_3",
    "query_v2_1",
    "query_v2_2",
    "stream_v2_active",
    "stream_v2_passive",
)
ROLLBACK_FIELDS = frozenset({"image_digest", "runtime_dir", "checkpoint_path"})
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{7,64}\Z")
_PEM_CERTIFICATE = re.compile(
    r"-----BEGIN CERTIFICATE-----\r?\n.*?-----END CERTIFICATE-----\r?\n?",
    re.DOTALL,
)
_EXTERNAL_COPY_DIRS = (
    "monitoring",
    "monitoring-jwt",
    "alpha-okx",
    "alpha-okx-jwt",
    "reference-l2",
    "reference-l2-jwt",
)
PYTHON_ROLLBACK_SERVICES = (
    "binance_bar_edge",
    "projector_v2",
    "projector_v2_2",
    "projector_v2_3",
    "query_v2_1",
    "query_v2_2",
    "stream_v2_active",
    "stream_v2_passive",
)
LEGACY_CONSUMER_MANIFESTS = (
    "/app/consumers/stable/monitoring-multivenue.yaml:"
    "/app/consumers/stable/alpha-binance-paper.yaml:"
    "/app/consumers/stable/alpha-okx-paper.yaml:"
    "/app/consumers/stable/alpha-vn-paper.yaml:"
    "/app/consumers/stable/trading-system-paper.yaml"
)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("utf-8")


def _require_digest(value: str, *, field: str) -> str:
    if _DIGEST.fullmatch(value) is None:
        raise ValueError(f"Reference/L2 rollout {field} must be an immutable SHA-256 digest")
    return value


def _require_host_path(path: Path, *, field: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_absolute() or STATE_ROOT not in (resolved, *resolved.parents):
        raise ValueError(f"Reference/L2 rollout {field} is outside the governed QDL V2 state root")
    return resolved


def _load_authority(active_runtime_dir: Path) -> tuple[dict[str, Any], bytes]:
    path = active_runtime_dir / "authority.json"
    try:
        encoded = path.read_bytes()
        payload = json.loads(encoded)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Reference/L2 rollout active authority is unreadable") from error
    if not isinstance(payload, dict):
        raise ValueError("Reference/L2 rollout active authority is not an object")
    validate_shared_authority_record(payload)
    return payload, encoded


def _load_rollback(
    value: Mapping[str, object],
    *,
    new_runtime_dir: Path,
    previous_checkpoint_path: str,
) -> dict[str, dict[str, str | None]]:
    if set(value) != set(ROLLING_SERVICES):
        raise ValueError("Reference/L2 rollout rollback map must name exactly the rolling services")
    result: dict[str, dict[str, str | None]] = {}
    for service in ROLLING_SERVICES:
        item = value[service]
        if not isinstance(item, Mapping) or set(item) != ROLLBACK_FIELDS:
            raise ValueError(f"Reference/L2 rollout rollback map is invalid for {service}")
        image = item.get("image_digest")
        runtime = item.get("runtime_dir")
        checkpoint = item.get("checkpoint_path")
        if not isinstance(image, str):
            raise ValueError(f"Reference/L2 rollout rollback image is invalid for {service}")
        _require_digest(image, field=f"rollback image for {service}")
        if not isinstance(runtime, str):
            raise ValueError(f"Reference/L2 rollout rollback runtime is invalid for {service}")
        runtime_path = _require_host_path(Path(runtime), field=f"rollback runtime for {service}")
        if runtime_path == new_runtime_dir:
            raise ValueError(f"Reference/L2 rollout rollback equals successor runtime for {service}")
        if service == "binance_bar_edge":
            if checkpoint != previous_checkpoint_path:
                raise ValueError("Reference/L2 rollout rollback bar checkpoint is not current")
        elif checkpoint is not None:
            raise ValueError(f"Reference/L2 rollout rollback checkpoint is invalid for {service}")
        result[service] = {
            "image_digest": image,
            "runtime_dir": str(runtime_path),
            "checkpoint_path": checkpoint if isinstance(checkpoint, str) else None,
        }
    return result


def _new_checkpoint_path(
    *,
    authority_bytes: bytes,
    python_image_digest: str,
    acquisition_revision: int,
    previous_checkpoint_path: str,
) -> str:
    if not previous_checkpoint_path.startswith("/var/lib/qdl-stable/runtime/"):
        raise ValueError("Reference/L2 rollout current bar checkpoint is outside stable runtime")
    nonce = sha256_bytes(
        authority_bytes
        + b"\x00"
        + python_image_digest.encode("ascii")
        + b"\x00"
        + str(acquisition_revision).encode("ascii")
    )[:20]
    successor = (
        "/var/lib/qdl-stable/runtime/"
        f"stable-crypto-bar-edge-r{acquisition_revision}-{nonce}.json"
    )
    if successor == previous_checkpoint_path:
        raise ValueError("Reference/L2 rollout successor bar checkpoint is unchanged")
    return successor


def _pem_certificates(path: Path) -> list[bytes]:
    try:
        value = path.read_bytes()
    except OSError as error:
        raise ValueError(f"Reference/L2 rollout trust source is unreadable: {path}") from error
    decoded = value.decode("ascii", errors="strict")
    blocks = [match.group(0).replace("\r\n", "\n").encode("ascii") for match in _PEM_CERTIFICATE.finditer(decoded)]
    if not blocks or _PEM_CERTIFICATE.sub("", decoded).strip():
        raise ValueError(f"Reference/L2 rollout trust source is not PEM-only: {path}")
    if len(set(blocks)) != len(blocks):
        raise ValueError(f"Reference/L2 rollout trust source has duplicate PEM certificates: {path}")
    return blocks


def _append_certificate(existing_path: Path, addition_path: Path) -> bytes:
    certificates = _pem_certificates(existing_path)
    addition = _pem_certificates(addition_path)
    if len(addition) != 1:
        raise ValueError("Reference/L2 rollout external CA must contain exactly one certificate")
    if addition[0] not in certificates:
        certificates.append(addition[0])
    return b"".join(certificates)


def _copy_directory(source: Path, destination: Path) -> None:
    if not source.is_dir() or source.is_symlink():
        raise ValueError(f"Reference/L2 rollout identity directory is invalid: {source}")
    if any(path.is_symlink() for path in source.rglob("*")):
        raise ValueError(f"Reference/L2 rollout identity directory contains a symlink: {source}")
    shutil.copytree(source, destination)


def _runtime_digests(runtime_dir: Path) -> dict[str, str]:
    return {
        path.name: _sha256_file(path)
        for path in sorted(runtime_dir.iterdir())
        if path.is_file()
    }


def _write_rollback_manifest_override(output_dir: Path) -> Path:
    """Render the exact old manifest set only for Python rollback roles.

    The retained C2 projector image predates the new Reference/L2 manifest.
    Reusing the current Compose file without this narrow override would make a
    recovery fail at config load even though its image/runtime provenance is
    otherwise valid. Rust roles do not load consumer manifests and are omitted.
    """
    lines = ["services:\n"]
    encoded_manifests = json.dumps(LEGACY_CONSUMER_MANIFESTS)
    for service in PYTHON_ROLLBACK_SERVICES:
        lines.extend((
            f"  {service}:\n",
            "    environment:\n",
            f"      QDL_STABLE_CONSUMER_MANIFESTS: {encoded_manifests}\n",
        ))
    path = output_dir / "rollback-legacy-manifests.override.yml"
    path.write_text("".join(lines), encoding="utf-8")
    path.chmod(0o640)
    return path


def _build_environment(
    *,
    base_environment: Mapping[str, str],
    active_query_environment: Mapping[str, str],
    active_bar_environment: Mapping[str, str],
    extension_dir: Path,
    python_image_digest: str,
    rust_image_digest: str,
    host_runtime_dir: Path,
    acquisition_revision: int,
    authority: Mapping[str, object],
    authority_bytes: bytes,
) -> dict[str, str]:
    required_query = {
        "QDL_STABLE_INTERNAL_INGEST_SECRET",
        "QDL_STABLE_CURSOR_KEYS_JSON",
        "QDL_DATA_JWT_KEYS_JSON",
        "QDL_DATA_JWT_KEY_SUBJECTS_JSON",
        "QDL_STABLE_SCHEMA_DIGEST",
        "QDL_STABLE_AUTHORITY_MODE",
        "QDL_STABLE_AUTHORITY_REVISION",
    }
    missing = sorted(required_query - set(active_query_environment))
    if missing:
        raise ValueError(f"Reference/L2 rollout active query environment is missing {missing}")
    if active_query_environment["QDL_STABLE_AUTHORITY_MODE"] != authority.get("mode"):
        raise ValueError("Reference/L2 rollout active query authority mode differs from runtime")
    if active_query_environment["QDL_STABLE_AUTHORITY_REVISION"] != str(authority.get("revision")):
        raise ValueError("Reference/L2 rollout active query authority revision differs from runtime")
    previous_checkpoint = active_bar_environment.get("QDL_STABLE_BAR_STATE_PATH")
    if not isinstance(previous_checkpoint, str) or not previous_checkpoint:
        raise ValueError("Reference/L2 rollout active bar environment lacks its checkpoint path")

    environment = dict(base_environment)
    for key in (
        "QDL_STABLE_INTERNAL_INGEST_SECRET",
        "QDL_STABLE_CURSOR_KEYS_JSON",
        "QDL_STABLE_SCHEMA_DIGEST",
        "QDL_STABLE_AUTHORITY_MODE",
        "QDL_STABLE_AUTHORITY_REVISION",
    ):
        environment[key] = active_query_environment[key]
    # Containers expose the derived names; Compose needs the source names.
    environment["QDL_STABLE_JWT_KEYS_JSON"] = active_query_environment[
        "QDL_DATA_JWT_KEYS_JSON"
    ]
    environment["QDL_STABLE_JWT_KEY_SUBJECTS_JSON"] = active_query_environment[
        "QDL_DATA_JWT_KEY_SUBJECTS_JSON"
    ]
    environment["QDL_STABLE_PYTHON_IMAGE"] = python_image_digest
    environment["QDL_STABLE_RUST_IMAGE"] = rust_image_digest
    environment["QDL_STABLE_RUNTIME_DIR"] = str(host_runtime_dir)
    environment["QDL_CONFIG_REVISION"] = f"phasec36-reference-l2-r{acquisition_revision}"
    environment["QDL_STABLE_BAR_STATE_PATH"] = _new_checkpoint_path(
        authority_bytes=authority_bytes,
        python_image_digest=python_image_digest,
        acquisition_revision=acquisition_revision,
        previous_checkpoint_path=previous_checkpoint,
    )
    prepared = prepare_handoff_environment(
        environment,
        extension_dir=extension_dir,
        python_image=python_image_digest,
    )
    prepared["QDL_STABLE_RUST_IMAGE"] = rust_image_digest
    prepared["QDL_STABLE_RUNTIME_DIR"] = str(host_runtime_dir)
    return prepared


def prepare_reference_l2_rollout(
    *,
    base_compose_env: Path,
    active_query_env: Path,
    active_bar_env: Path,
    active_runtime_dir: Path,
    prior_external_dir: Path,
    reference_extension_dir: Path,
    current_query_client_ca: Path,
    current_stream_client_ca: Path,
    output_dir: Path,
    host_runtime_dir: Path,
    python_image_digest: str,
    rust_image_digest: str,
    source_commit: str,
    rollback_provenance: Mapping[str, object],
) -> dict[str, object]:
    """Create a sealed successor bundle; never contacts Docker or a provider."""
    if _COMMIT.fullmatch(source_commit) is None:
        raise ValueError("Reference/L2 rollout source commit is invalid")
    _require_digest(python_image_digest, field="Python image")
    _require_digest(rust_image_digest, field="Rust image")
    active_runtime_dir = _require_host_path(active_runtime_dir, field="active runtime")
    host_runtime_dir = _require_host_path(host_runtime_dir, field="successor runtime")
    output_dir = _require_host_path(output_dir, field="output directory")
    if output_dir.exists():
        raise FileExistsError("Reference/L2 rollout output directory must not already exist")
    if host_runtime_dir.parent != output_dir or host_runtime_dir.name != "runtime":
        raise ValueError("Reference/L2 rollout successor runtime must be output_dir/runtime")

    base_environment = load_dotenv(base_compose_env)
    active_query_environment = load_dotenv(active_query_env)
    active_bar_environment = load_dotenv(active_bar_env)
    authority, authority_bytes = _load_authority(active_runtime_dir)
    catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
    acquisition = StableAcquisitionPlan.load(
        ROOT / "config/v2/stable-acquisition-bindings.yaml", catalog=catalog
    )
    previous_checkpoint = active_bar_environment.get("QDL_STABLE_BAR_STATE_PATH")
    if not isinstance(previous_checkpoint, str):
        raise ValueError("Reference/L2 rollout active bar checkpoint is invalid")
    rollback = _load_rollback(
        rollback_provenance,
        new_runtime_dir=host_runtime_dir,
        previous_checkpoint_path=previous_checkpoint,
    )

    output_dir.mkdir(mode=0o700, parents=True)
    try:
        extension_dir = output_dir / "external-identities"
        extension_dir.mkdir(mode=0o700)
        for name in ("monitoring", "monitoring-jwt", "alpha-okx", "alpha-okx-jwt"):
            _copy_directory(prior_external_dir / name, extension_dir / name)
        for name in ("reference-l2", "reference-l2-jwt"):
            _copy_directory(reference_extension_dir / name, extension_dir / name)
        reference_ca = reference_extension_dir / "external-client-ca.crt"
        if not reference_ca.is_file():
            raise ValueError("Reference/L2 rollout reference external CA is missing")
        shutil.copyfile(reference_ca, extension_dir / "external-client-ca.crt")

        trust_dir = output_dir / "trust"
        trust_dir.mkdir(mode=0o700)
        query_bundle = _append_certificate(current_query_client_ca, reference_ca)
        stream_bundle = _append_certificate(current_stream_client_ca, reference_ca)
        (trust_dir / "query-client-ca-bundle.crt").write_bytes(query_bundle)
        (trust_dir / "stream-client-ca-bundle.crt").write_bytes(stream_bundle)
        shutil.copyfile(trust_dir / "query-client-ca-bundle.crt", extension_dir / "client-ca-bundle.crt")
        for path in trust_dir.iterdir():
            path.chmod(0o440)
        for path in (extension_dir / "external-client-ca.crt", extension_dir / "client-ca-bundle.crt"):
            path.chmod(0o440)

        runtime_dir = output_dir / "runtime"
        write_stable_runtime_bundle(
            runtime_dir,
            catalog=catalog,
            acquisition=acquisition,
            authority=authority,
        )
        authority_path = runtime_dir / "authority.json"
        authority_path.write_bytes(authority_bytes)
        authority_path.chmod(0o644)
        if authority_path.read_bytes() != authority_bytes:
            raise ValueError("Reference/L2 rollout changed active authority bytes")
        runtime_digests = _runtime_digests(runtime_dir)
        if not runtime_digests:
            raise ValueError("Reference/L2 rollout generated no runtime files")

        environment = _build_environment(
            base_environment=base_environment,
            active_query_environment=active_query_environment,
            active_bar_environment=active_bar_environment,
            extension_dir=extension_dir,
            python_image_digest=python_image_digest,
            rust_image_digest=rust_image_digest,
            host_runtime_dir=host_runtime_dir,
            acquisition_revision=acquisition.revision,
            authority=authority,
            authority_bytes=authority_bytes,
        )
        env_path = output_dir / "rollout.env"
        env_path.write_text(render_dotenv(environment), encoding="utf-8")
        env_path.chmod(0o600)
        rollback_override = _write_rollback_manifest_override(output_dir)

        reference_manifest = ROOT / "config/v2/stable-reference-l2-demand.yaml"
        packet_body: dict[str, object] = {
            "schema": SCHEMA,
            "status": "PREPARED",
            "source_commit": source_commit,
            "catalog_revision": catalog.catalog_revision,
            "catalog_sha256": _sha256_file(ROOT / "config/v2/stable-source-bindings.yaml"),
            "acquisition_revision": acquisition.revision,
            "acquisition_sha256": _sha256_file(ROOT / "config/v2/stable-acquisition-bindings.yaml"),
            "reference_l2_manifest_sha256": _sha256_file(reference_manifest),
            "recreated_services": list(ROLLING_SERVICES),
            "excluded_services": [
                "rust_core_2",
                "rust_core_3",
                "production_core_1",
                "production_core_2",
                "production_core_3",
                "kafka1",
                "kafka2",
                "kafka3",
                "stable_redis",
                "stable_authority_db",
                "data_layer_service",
                "trading_system",
                "alpha",
                "orders",
            ],
            "runtime": {
                "host_runtime_dir": str(host_runtime_dir),
                "runtime_files": runtime_digests,
                "authority_sha256": sha256_bytes(authority_bytes),
                "authority_bytes_preserved": True,
                "authority_mode": authority["mode"],
                "authority_revision": authority["revision"],
                "python_image_digest": python_image_digest,
                "rust_image_digest": rust_image_digest,
            },
            "trust": {
                "previous_query_bundle_sha256": _sha256_file(current_query_client_ca),
                "previous_stream_bundle_sha256": _sha256_file(current_stream_client_ca),
                "successor_query_bundle_sha256": _sha256_file(trust_dir / "query-client-ca-bundle.crt"),
                "successor_stream_bundle_sha256": _sha256_file(trust_dir / "stream-client-ca-bundle.crt"),
                "reference_external_ca_sha256": _sha256_file(reference_ca),
                "reference_jwt_public_key_sha256": _sha256_file(extension_dir / "reference-l2-jwt/public.pem"),
                "prior_external_identities_preserved": ["monitoring", "alpha-okx"],
            },
            "environment": {
                "sha256": sha256_bytes(render_dotenv(environment).encode("utf-8")),
                "jwt_key_ids": sorted(json.loads(environment["QDL_STABLE_JWT_KEYS_JSON"])),
                "secret_values_recorded": False,
            },
            "rollback": {
                "services": rollback,
                "restore_query_client_ca": _sha256_file(current_query_client_ca),
                "restore_stream_client_ca": _sha256_file(current_stream_client_ca),
                "compose_override": {
                    "path": rollback_override.name,
                    "sha256": _sha256_file(rollback_override),
                    "python_services": list(PYTHON_ROLLBACK_SERVICES),
                    "legacy_manifest_sha256": sha256_bytes(
                        LEGACY_CONSUMER_MANIFESTS.encode("utf-8")
                    ),
                },
                "durable_data_deletion": False,
            },
            "forbidden_operations": [
                "docker",
                "provider_io",
                "kafka_topology",
                "offset_reset",
                "redis_flush",
                "sqlite_delete",
                "authority_cas",
                "consumer_route_cas",
                "execution_mutation",
            ],
        }
        packet = {
            "packet_sha256": sha256_bytes(_canonical_bytes(packet_body)),
            **packet_body,
        }
        encoded = json.dumps(packet, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        if "PRIVATE KEY" in encoded:
            raise ValueError("Reference/L2 rollout packet would expose private key material")
        packet_path = output_dir / "rollout-packet.json"
        packet_path.write_text(encoded, encoding="utf-8")
        packet_path.chmod(0o640)
        return packet
    except Exception:
        shutil.rmtree(output_dir, ignore_errors=True)
        raise


def dry_run_reference_l2_rollout(**kwargs: object) -> dict[str, object]:
    """Run the exact bundle compiler against a disposable state directory."""
    STATE_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".qdl-reference-l2-rollout-", dir=STATE_ROOT
    ) as raw:
        output_dir = Path(raw) / "bundle"
        values = dict(kwargs)
        values["output_dir"] = output_dir
        values["host_runtime_dir"] = output_dir / "runtime"
        packet = prepare_reference_l2_rollout(**values)
    return packet
