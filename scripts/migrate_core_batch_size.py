#!/usr/bin/env python3
"""Derive realtime-core runtime configs whose only change is ``batch_size``.

Purpose. P18.4 capacity step 1 raises the records per realtime-core
transaction from 256 to a chosen target (default 2048,
``STABLE_CORE_BATCH_SIZE``). The running cores read bind-mounted JSON files
from a release directory; regenerating them with
``phaseb_prepare_stable_candidate.py`` is forbidden on a running bundle (it
mints new secrets), and the refresh tools prove other, older deltas. This tool
writes new, versioned copies of the given core configs with exactly one value
changed.

Boundary. The edit is textual (the one ``"batch_size": <old>`` token), so every
other byte - bindings, authority, transactional id, key order, whitespace - is
kept, and it is then proven: both files parse, the parsed objects are equal
once ``batch_size`` is removed, and the new value is the target. It never
modifies a source file, never overwrites an output, never touches a container
and never regenerates a bundle. Dry run by default; ``--apply`` writes the
outputs (mode 0640) plus a receipt with both SHA-256 digests per file.

Target. ``--target-batch-size`` picks the new value (default
``STABLE_CORE_BATCH_SIZE``, 2048, the previous fixed behaviour; the owner's
2026-10-14 production step 1 uses 1024). It must be an integer in
``1..=CORE_BATCH_SIZE_MAX``, the bound ``RuntimeConfig::validate`` enforces in
``rust/qdl-kafka/src/bin/qdl-realtime-core.rs`` (``batch_size == 0 ||
batch_size > 10_000`` is rejected there), so the tool never writes a config the
core would refuse at start. The chosen target is recorded in the receipt.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from qdl.runtime.stable_deployment import STABLE_CORE_BATCH_SIZE  # noqa: E402

PREVIOUS_BATCH_SIZE = 256
# Mirrors qdl-realtime-core RuntimeConfig::validate (batch_size in 1..=10_000).
CORE_BATCH_SIZE_MAX = 10_000
_TOKEN = re.compile(rb'"batch_size"(\s*):(\s*)(\d+)')


def derive(data: bytes, *, target: int = STABLE_CORE_BATCH_SIZE) -> bytes:
    """Return ``data`` with only its ``batch_size`` value set to ``target``."""
    _check_target(target)
    matches = list(_TOKEN.finditer(data))
    if len(matches) != 1:
        raise ValueError(f"expected exactly one batch_size token, found {len(matches)}")
    current = int(matches[0].group(3))
    if current not in {PREVIOUS_BATCH_SIZE, target}:
        raise ValueError(f"unexpected current batch_size {current}")
    match = matches[0]
    derived = (
        data[: match.start(3)] + str(target).encode() + data[match.end(3):]
    )
    before = json.loads(data)
    after = json.loads(derived)
    if not isinstance(before, dict) or "core" not in before or "transactional_id" not in before:
        raise ValueError("not a realtime-core runtime config")
    if after.get("batch_size") != target:
        raise ValueError("derived batch_size is not the target")
    if {k: v for k, v in before.items() if k != "batch_size"} != {
        k: v for k, v in after.items() if k != "batch_size"
    }:
        raise ValueError("derived config changes more than batch_size")
    return derived


def _check_target(target: object) -> int:
    if isinstance(target, bool) or not isinstance(target, int):
        raise ValueError(f"target batch_size must be an integer, got {target!r}")
    if not 1 <= target <= CORE_BATCH_SIZE_MAX:
        raise ValueError(
            f"target batch_size {target} is outside 1..={CORE_BATCH_SIZE_MAX}"
            " (qdl-realtime-core RuntimeConfig::validate)"
        )
    return target


def _target_arg(raw: str) -> int:
    try:
        value = int(raw, 10)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an integer: {raw!r}") from None
    try:
        return _check_target(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from None


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run(
    sources: list[Path],
    output_dir: Path,
    *,
    apply: bool,
    target_batch_size: int = STABLE_CORE_BATCH_SIZE,
) -> dict:
    _check_target(target_batch_size)
    planned = []
    for source in sources:
        data = source.read_bytes()
        derived = derive(data, target=target_batch_size)
        target = output_dir / source.name
        if target.exists():
            raise FileExistsError(f"{target} exists; outputs are never overwritten")
        planned.append((source, target, data, derived))
    receipt = {
        "schema": "qdl.core-batch-size-migration.v1",
        "batch_size": {"from": PREVIOUS_BATCH_SIZE, "to": target_batch_size},
        "applied": apply,
        "files": [
            {
                "source": str(source),
                "output": str(target),
                "source_sha256": _sha256(data),
                "output_sha256": _sha256(derived),
                "unchanged": data == derived,
            }
            for source, target, data, derived in planned
        ],
    }
    if apply:
        output_dir.mkdir(parents=True, exist_ok=True)
        for _source, target, _data, derived in planned:
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(derived)
                handle.flush()
                os.fsync(handle.fileno())
        receipt_path = output_dir / "core-batch-size-receipt.json"
        descriptor = os.open(receipt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(receipt, handle, indent=2, sort_keys=True)
            handle.write("\n")
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("sources", nargs="+", type=Path, help="live core JSON files")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--target-batch-size",
        type=_target_arg,
        default=STABLE_CORE_BATCH_SIZE,
        help=(
            f"new batch_size, integer 1..={CORE_BATCH_SIZE_MAX}"
            f" (default {STABLE_CORE_BATCH_SIZE})"
        ),
    )
    args = parser.parse_args(argv)
    receipt = run(
        args.sources,
        args.output_dir,
        apply=args.apply,
        target_batch_size=args.target_batch_size,
    )
    print(json.dumps(receipt, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
