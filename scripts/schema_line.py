#!/usr/bin/env python3
"""Contract-line schema digest: what a record's ``schema_digest`` means, and the release check that keeps it honest.

Owner decision 2026-11-13 (DL v2.3.0 slice 2d). The ``schema_digest`` in every record contract is a consumer's
lineage key (the TS writer keys execution ordering and resume markers on it). Until now it was "sha256 of
market_data.proto" in name, but the roles carry a compose literal that never moved, so it already behaved like a
line id. It is now defined as the digest of the compatible contract LINE:

* a line is opened with a frozen baseline image of the protobuf module (``buf build``);
* every later proto change must pass ``buf breaking`` (FILE rules, ``contracts/buf.yaml``) against the current
  line's baseline, so additive changes such as v2.3.0 ``Bar.aggregation`` keep the digest;
* an incompatible change must open a new line (append it to ``contracts/schema-lines/qdl-v2-contract-lines.json``
  with its own baseline image), and that is a release-level lineage change consumers must hand over;
* the real sha256 of market_data.proto is reported separately as ``schema_file_sha256`` (release record, role
  manifest), never as lineage.

Commands (read-only; ``check`` runs buf in a ``--rm`` container with no network unless ``--buf`` names a local one):
  digest        print the current line's schema_digest (the value of QDL_STABLE_SCHEMA_DIGEST)
  file-sha256   print the sha256 of market_data.proto (QDL_STABLE_SCHEMA_FILE_SHA256)
  check         PASS when the module is compatible with the current line, FAIL (exit 2) otherwise; JSON verdict

Boundary: reads repository files and runs buf; writes nothing.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Sequence

ROOT = Path(__file__).resolve().parents[1]
LINES = ROOT / "contracts/schema-lines/qdl-v2-contract-lines.json"
SCHEMA = "qdl.v2.contract-lines.v1"
BUF_IMAGE = "bufbuild/buf:1.50.0"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_lines(path: Path = LINES) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != SCHEMA or not payload.get("lines"):
        raise ValueError("contract line registry schema is unsupported or empty")
    numbers = [item["line"] for item in payload["lines"]]
    if numbers != list(range(1, len(numbers) + 1)):
        raise ValueError("contract lines must be numbered 1..N in order")
    digests = [item["schema_digest"] for item in payload["lines"]]
    if len(set(digests)) != len(digests) or any(len(value) != 64 for value in digests):
        raise ValueError("contract line digests must be unique sha256 values")
    return payload


def current_line(payload: dict[str, Any]) -> dict[str, Any]:
    return payload["lines"][-1]


def schema_digest(root: Path = ROOT) -> str:
    """The value every role carries as QDL_STABLE_SCHEMA_DIGEST."""

    return current_line(load_lines(root / LINES.relative_to(ROOT)))["schema_digest"]


def docker_buf(contracts: Path) -> Callable[[Sequence[str]], int]:
    def run(args: Sequence[str]) -> int:
        return subprocess.run([
            "docker", "run", "--rm", "--network", "none", "--cpus", "1",
            "--user", f"{os.getuid()}:{os.getgid()}", "-e", "BUF_CACHE_DIR=/tmp/buf", "-e", "HOME=/tmp",
            "-v", f"{contracts}:/w:ro", "-w", "/w", BUF_IMAGE, *args,
        ], check=False).returncode
    return run


def local_buf(binary: str, contracts: Path) -> Callable[[Sequence[str]], int]:
    def run(args: Sequence[str]) -> int:
        return subprocess.run([binary, *args], cwd=contracts, check=False).returncode
    return run


def check(root: Path, buf: Callable[[Sequence[str]], int]) -> dict[str, Any]:
    payload = load_lines(root / LINES.relative_to(ROOT))
    line = current_line(payload)
    proto = root / payload["proto"]
    image = root / line["baseline_image"]
    file_sha = _sha256(proto)
    verdict: dict[str, Any] = {
        "schema": "qdl.v2.contract-line-check.v1",
        "line": line["line"],
        "schema_digest": line["schema_digest"],
        "schema_file_sha256": file_sha,
        "baseline_image": line["baseline_image"],
    }
    problems = []
    if not image.is_file() or _sha256(image) != line["baseline_image_sha256"]:
        problems.append("baseline image is missing or differs from its recorded sha256")
    elif file_sha == line["baseline_proto_sha256"]:
        verdict["compatible"] = True
        verdict["reason"] = "proto equals the line baseline"
    else:
        rc = buf(["breaking", "--against", str(image.relative_to(root / "contracts"))])
        verdict["buf_breaking_rc"] = rc
        verdict["compatible"] = rc == 0
        verdict["reason"] = (
            "additive within the line (buf breaking passes against the line baseline)" if rc == 0
            else "breaking change: open a new contract line (new baseline image + registry entry) in this release"
        )
        if rc != 0:
            problems.append(verdict["reason"])
    verdict["problems"] = problems
    verdict["status"] = "PASS" if not problems and verdict.get("compatible") else "FAIL"
    return verdict


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("digest", "file-sha256", "check"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--buf", help="local buf binary (default: the pinned buf image in a --rm container)")
    args = parser.parse_args(argv)
    payload = load_lines(args.root / LINES.relative_to(ROOT))
    if args.command == "digest":
        print(current_line(payload)["schema_digest"])
        return 0
    if args.command == "file-sha256":
        print(_sha256(args.root / payload["proto"]))
        return 0
    contracts = (args.root / "contracts").resolve()
    buf = local_buf(args.buf, contracts) if args.buf else docker_buf(contracts)
    verdict = check(args.root, buf)
    print(json.dumps(verdict, indent=2, sort_keys=True))
    return 0 if verdict["status"] == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
