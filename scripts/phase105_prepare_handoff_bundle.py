#!/usr/bin/env python3
"""Prepare the private, additive Phase 10.5-C runtime environment and receipt."""

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
    handoff_packet,
    load_dotenv,
    prepare_handoff_environment,
    render_dotenv,
    sha256_bytes,
)


CONFIRM = "PREPARE_QDL_PHASE105C_HANDOFF"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-env", type=Path, required=True)
    parser.add_argument("--extension-dir", type=Path, required=True)
    parser.add_argument("--python-image", required=True)
    parser.add_argument("--v1-provenance", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args()
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")
    if args.output_dir.exists():
        raise SystemExit("Phase 10.5-C output directory must not already exist")

    environment = prepare_handoff_environment(
        load_dotenv(args.base_env),
        extension_dir=args.extension_dir,
        python_image=args.python_image,
    )
    try:
        v1_provenance = json.loads(args.v1_provenance.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit("Phase 10.5-C V1 provenance cannot be read") from error
    packet = handoff_packet(
        environment=environment,
        extension_dir=args.extension_dir,
        v1_attestation=v1_provenance,
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
        "secret_values_recorded": False,
    }
    if not args.apply:
        print(json.dumps(preview, sort_keys=True))
        return 0

    os.umask(0o077)
    args.output_dir.mkdir(mode=0o700, parents=True)
    env_path = args.output_dir / "stable.env"
    packet_path = args.output_dir / "handoff-packet.json"
    env_path.write_text(render_dotenv(environment), encoding="utf-8")
    packet_path.write_text(json.dumps(packet, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    env_path.chmod(0o600)
    packet_path.chmod(0o600)
    print(json.dumps(preview | {"output_dir": str(args.output_dir)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
