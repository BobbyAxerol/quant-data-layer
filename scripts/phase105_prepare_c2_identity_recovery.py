#!/usr/bin/env python3
"""Prepare the public-only additive C2 external-identity recovery overlay."""

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
    ALL_KEY_SUBJECTS,
    RECOVERY_ALL_KEY_SUBJECTS,
    RECOVERY_IDENTITY_SPECS,
    handoff_packet,
    load_dotenv,
    prepare_c2_identity_recovery_environment,
    public_handoff_overlay,
    render_dotenv,
    sha256_bytes,
    validate_frozen_v1_provenance,
)


CONFIRM = "PREPARE_QDL_PHASE105C_IDENTITY_RECOVERY"


def _public_keyring(environment: dict[str, str]) -> dict[str, str]:
    try:
        value = json.loads(environment["QDL_STABLE_JWT_KEYS_JSON"])
    except (KeyError, json.JSONDecodeError) as error:
        raise ValueError("Phase 10.5-C recovery base JWT keyring is invalid") from error
    if not isinstance(value, dict) or set(value) != set(ALL_KEY_SUBJECTS):
        raise ValueError("Phase 10.5-C recovery base JWT keyring is not the retained V1 set")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-env", type=Path, required=True)
    parser.add_argument("--extension-dir", type=Path, required=True)
    parser.add_argument("--reader-image", required=True)
    parser.add_argument("--v1-provenance", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args(argv)
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")
    if args.output_dir.exists():
        raise SystemExit("Phase 10.5-C recovery output directory must not already exist")

    base = load_dotenv(args.base_env)
    prior_keys = _public_keyring(base)
    environment = prepare_c2_identity_recovery_environment(
        base,
        extension_dir=args.extension_dir,
        python_image=args.reader_image,
    )
    try:
        v1_provenance_raw = json.loads(args.v1_provenance.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit("Phase 10.5-C recovery V1 provenance cannot be read") from error
    validate_frozen_v1_provenance(v1_provenance_raw)
    packet = handoff_packet(
        environment=environment,
        extension_dir=args.extension_dir,
        v1_attestation=v1_provenance_raw,
        approved_key_subjects=RECOVERY_ALL_KEY_SUBJECTS,
    )
    packet.update({
        "schema": "qdl.phase105c.identity-recovery.v1",
        "status": "PREPARED",
        "reader_image": args.reader_image,
        "retained_key_ids": sorted(prior_keys),
        "recovery_key_ids": sorted(RECOVERY_IDENTITY_SPECS),
    })
    packet["packet_sha256"] = sha256_bytes(
        json.dumps(packet, sort_keys=True, separators=(",", ":")).encode()
    )
    overlay = public_handoff_overlay(environment)
    preview = {
        "schema": packet["schema"],
        "status": "DRY_RUN" if not args.apply else "PREPARED",
        "packet_sha256": packet["packet_sha256"],
        "recreated_services": packet["recreated_services"],
        "retained_key_ids": packet["retained_key_ids"],
        "recovery_key_ids": packet["recovery_key_ids"],
        "public_overlay_sha256": sha256_bytes(render_dotenv(overlay).encode()),
        "secret_values_recorded": False,
    }
    if not args.apply:
        print(json.dumps(preview, sort_keys=True))
        return 0

    os.umask(0o077)
    args.output_dir.mkdir(mode=0o700, parents=True)
    env_path = args.output_dir / "identity-recovery-public.env"
    packet_path = args.output_dir / "identity-recovery-packet.json"
    env_path.write_text(render_dotenv(overlay), encoding="utf-8")
    packet_path.write_text(json.dumps(packet, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    env_path.chmod(0o600)
    packet_path.chmod(0o600)
    print(json.dumps(preview | {"output_dir": str(args.output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
