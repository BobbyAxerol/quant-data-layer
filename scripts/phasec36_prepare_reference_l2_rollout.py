#!/usr/bin/env python3
"""Prepare the bounded Reference/L2 V2 successor bundle without runtime I/O."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.reference_l2_rollout import (
    CONFIRM,
    dry_run_reference_l2_rollout,
    prepare_reference_l2_rollout,
)


def _rollback(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit("--rollback-provenance must contain a JSON object") from error
    if not isinstance(value, dict):
        raise SystemExit("--rollback-provenance must contain an object")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-compose-env", type=Path, required=True)
    parser.add_argument("--active-query-env", type=Path, required=True)
    parser.add_argument("--active-bar-env", type=Path, required=True)
    parser.add_argument("--active-runtime-dir", type=Path, required=True)
    parser.add_argument("--prior-external-dir", type=Path, required=True)
    parser.add_argument("--reference-extension-dir", type=Path, required=True)
    parser.add_argument("--current-query-client-ca", type=Path, required=True)
    parser.add_argument("--current-stream-client-ca", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--host-runtime-dir", type=Path, required=True)
    parser.add_argument("--python-image-digest", required=True)
    parser.add_argument("--rust-image-digest", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--rollback-provenance", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args(argv)
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")
    if not args.apply and args.output_dir.exists():
        raise SystemExit("dry-run output directory must not already exist")
    values = {
        "base_compose_env": args.base_compose_env,
        "active_query_env": args.active_query_env,
        "active_bar_env": args.active_bar_env,
        "active_runtime_dir": args.active_runtime_dir,
        "prior_external_dir": args.prior_external_dir,
        "reference_extension_dir": args.reference_extension_dir,
        "current_query_client_ca": args.current_query_client_ca,
        "current_stream_client_ca": args.current_stream_client_ca,
        "output_dir": args.output_dir,
        "host_runtime_dir": args.host_runtime_dir,
        "python_image_digest": args.python_image_digest,
        "rust_image_digest": args.rust_image_digest,
        "source_commit": args.source_commit,
        "rollback_provenance": _rollback(args.rollback_provenance),
    }
    packet = (
        prepare_reference_l2_rollout(**values)
        if args.apply
        else dry_run_reference_l2_rollout(**values)
    )
    print(json.dumps({
        "schema": packet["schema"],
        "status": "PREPARED" if args.apply else "DRY_RUN",
        "packet_sha256": packet["packet_sha256"],
        "catalog_revision": packet["catalog_revision"],
        "acquisition_revision": packet["acquisition_revision"],
        "recreated_services": packet["recreated_services"],
        "jwt_key_ids": packet["environment"]["jwt_key_ids"],
        "authority_bytes_preserved": packet["runtime"]["authority_bytes_preserved"],
        "secret_values_recorded": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
