#!/usr/bin/env python3
"""Read-only validation for a sealed Phase 10.3 shared-primary packet.

This command checks only the packet and generated host runtime bundle. It does
not invoke Docker, Kafka, Redis, PostgreSQL, provider endpoints or consumers.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.phase103_prepare_shared_primary_packet import (
    validate_prepared_shared_primary_bundle,
)


def _load_packet(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"packet cannot be read as JSON: {path}") from error
    if not isinstance(payload, dict):
        raise ValueError("packet root must be an object")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    args = parser.parse_args()
    result = validate_prepared_shared_primary_bundle(
        _load_packet(args.packet),
        runtime_dir=args.runtime_dir,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
