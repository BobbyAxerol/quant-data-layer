#!/usr/bin/env python3
"""Prepare a non-secret, source-only Phase 10.5-C1 final-BAR repair packet."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase105c_final_bar import prepare_final_bar_repair


CONFIRM = "PREPARE_QDL_PHASE105C_FINAL_BAR_REPAIR"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--active-runtime-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--host-runtime-dir", type=Path, required=True)
    parser.add_argument("--python-image-digest", required=True)
    parser.add_argument("--rust-image-digest", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--previous-bar-state-path", required=True)
    parser.add_argument("--rollback-provenance", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args(argv)
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")
    if not args.apply and args.output_dir.exists():
        raise SystemExit("dry-run output directory must not already exist")
    try:
        rollback_provenance = json.loads(args.rollback_provenance.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit("--rollback-provenance must be a readable JSON object") from error

    if args.apply:
        packet = prepare_final_bar_repair(
            active_runtime_dir=args.active_runtime_dir,
            output_dir=args.output_dir,
            host_runtime_dir=args.host_runtime_dir,
            python_image_digest=args.python_image_digest,
            rust_image_digest=args.rust_image_digest,
            source_commit=args.source_commit,
            previous_bar_state_path=args.previous_bar_state_path,
            rollback_provenance=rollback_provenance,
        )
        status = "PREPARED"
    else:
        with tempfile.TemporaryDirectory(prefix="qdl-phase105c-dry-run-") as raw:
            scratch = Path(raw) / "packet"
            packet = prepare_final_bar_repair(
                active_runtime_dir=args.active_runtime_dir,
                output_dir=scratch,
                host_runtime_dir=args.host_runtime_dir,
                python_image_digest=args.python_image_digest,
                rust_image_digest=args.rust_image_digest,
                source_commit=args.source_commit,
                previous_bar_state_path=args.previous_bar_state_path,
                rollback_provenance=rollback_provenance,
            )
        status = "DRY_RUN"

    print(json.dumps({
        "schema": packet["schema"],
        "status": status,
        "packet_sha256": packet["packet_sha256"],
        "confirmation_token": packet["confirmation_token"],
        "recreated_services": packet["recreated_services"],
        "rollback_services": sorted(packet["rollback"]["services"]),
        "acquisition_revision": packet["acquisition_revision"],
        "authority_bytes_preserved": packet["runtime"]["authority_bytes_preserved"],
        "production_mutations": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
