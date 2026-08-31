#!/usr/bin/env python3
"""Prepare the public-only, additive Phase 10.5-C handoff overlay and receipt."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase105_handoff import (
    active_runtime_binding,
    handoff_packet,
    load_dotenv,
    prepare_handoff_environment,
    public_handoff_overlay,
    render_dotenv,
    sha256_bytes,
    validate_active_query_environment_commitment,
)


CONFIRM = "PREPARE_QDL_PHASE105C_HANDOFF"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-env", type=Path, required=True)
    parser.add_argument(
        "--active-runtime-packet",
        type=Path,
        required=True,
        help="sealed current V2 runtime packet; only its allowlisted selectors apply",
    )
    parser.add_argument(
        "--active-query-commitment",
        type=Path,
        required=True,
        help="payload-free proof that the current query matches the sealed secret reference",
    )
    parser.add_argument("--extension-dir", type=Path, required=True)
    parser.add_argument("--python-image", required=True)
    parser.add_argument("--v1-provenance", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args(argv)
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")
    if args.output_dir.exists():
        raise SystemExit("Phase 10.5-C output directory must not already exist")

    base_environment = load_dotenv(args.base_env)
    try:
        active_packet = json.loads(args.active_runtime_packet.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit("Phase 10.5-C active runtime packet cannot be read") from error
    if not isinstance(active_packet, dict):
        raise SystemExit("Phase 10.5-C active runtime packet must be an object")
    runtime_binding = active_runtime_binding(base_environment, active_packet)
    environment = prepare_handoff_environment(
        base_environment,
        extension_dir=args.extension_dir,
        python_image=args.python_image,
        runtime_binding=runtime_binding,
    )
    try:
        query_commitment_raw = json.loads(
            args.active_query_commitment.read_text(encoding="utf-8")
        )
        expected_jwt_keyring = json.loads(environment["QDL_STABLE_JWT_KEYS_JSON"])
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit("Phase 10.5-C active query commitment cannot be read") from error
    if not isinstance(expected_jwt_keyring, dict):
        raise SystemExit("Phase 10.5-C public JWT keyring is invalid")
    query_commitment = validate_active_query_environment_commitment(
        base_environment, runtime_binding, query_commitment_raw, expected_jwt_keyring
    )
    overlay = public_handoff_overlay(environment)
    try:
        v1_provenance = json.loads(args.v1_provenance.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit("Phase 10.5-C V1 provenance cannot be read") from error
    packet = handoff_packet(
        environment=environment,
        extension_dir=args.extension_dir,
        v1_attestation=v1_provenance,
        runtime_binding=runtime_binding,
        query_environment_commitment=query_commitment,
    )
    packet["packet_sha256"] = sha256_bytes(
        json.dumps(packet, sort_keys=True, separators=(",", ":")).encode()
    )
    preview = {
        "schema": packet["schema"],
        "status": "DRY_RUN" if not args.apply else "PREPARED",
        "packet_sha256": packet["packet_sha256"],
        "recreated_services": packet["recreated_services"],
        "jwt_key_ids": packet["jwt_key_ids"],
        "public_overlay_sha256": sha256_bytes(render_dotenv(overlay).encode()),
        "secret_values_recorded": False,
    }
    if not args.apply:
        print(json.dumps(preview, sort_keys=True))
        return 0

    os.umask(0o077)
    args.output_dir.mkdir(mode=0o700, parents=True)
    env_path = args.output_dir / "handoff-public.env"
    packet_path = args.output_dir / "handoff-packet.json"
    env_path.write_text(render_dotenv(overlay), encoding="utf-8")
    packet_path.write_text(json.dumps(packet, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    env_path.chmod(0o600)
    packet_path.chmod(0o600)
    print(json.dumps(preview | {"output_dir": str(args.output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
