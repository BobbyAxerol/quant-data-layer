#!/usr/bin/env python3
"""Move live OKX native-ingestor configs off WebSocket port 8443, transport only.

Purpose. OKX stops accepting WebSocket connections on port 8443 on 2026-10-31
(OKX changelog, "WebSocket port 8443 discontinuation"). The running OKX ingestor
reads its URLs from a bind-mounted runtime JSON. This tool rewrites only the
two OKX URL values in such a file, so the role can be restarted on port 443
without regenerating the bundle.

Boundary. It changes nothing but ``websocket_url`` and
``business_websocket_url`` and proves it: the file with those keys removed must
be identical before and after, so ``config_revision``, ``authority``,
``bindings`` and every catalog revision stay byte-for-byte the same and no edge
checkpoint is stranded (plan C.19/C.32). It never regenerates a bundle, never
touches ``stable.env`` or identities, and never starts or stops a container.

Dry run by default. ``--apply`` writes a sibling backup, then atomically
replaces the file, keeping its owner and mode. A running container keeps the
inode it started with until it is restarted. ``--rollback-from BACKUP``
restores a backup after checking that its identity equals the current file's.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from qdl.adapters.okx_endpoints import (  # noqa: E402
    require_okx_websocket_url,
    without_retired_port,
)

SERVICE_BY_FIELD = {"websocket_url": "public", "business_websocket_url": "business"}
TRANSPORT_FIELDS = ("websocket_url", "business_websocket_url", "market_websocket_url")
BACKUP_SUFFIX = ".pre-okx-port443-"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _decode(data: bytes, path: Path) -> dict:
    try:
        payload = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path}: not JSON") from error
    if not isinstance(payload, dict) or payload.get("runtime") != "OKX":
        raise ValueError(f"{path}: not an OKX native ingestor config")
    if not all(field in payload for field in TRANSPORT_FIELDS):
        raise ValueError(f"{path}: transport fields are incomplete")
    return payload


def identity_sha256(payload: dict) -> str:
    """Digest of the config with transport endpoints removed."""
    identity = {key: value for key, value in payload.items() if key not in TRANSPORT_FIELDS}
    return _sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode())


def migrate_bytes(data: bytes, path: Path) -> tuple[bytes, dict[str, dict[str, str]]]:
    """Return the migrated bytes and the per-field change list.

    The rewrite is textual so the file keeps its formatting and every other
    byte; the parsed result is then checked independently.
    """
    before = _decode(data, path)
    text = data.decode("utf-8")
    changes: dict[str, dict[str, str]] = {}
    for field, service in SERVICE_BY_FIELD.items():
        old = before[field]
        if not isinstance(old, str):
            raise ValueError(f"{path}: {field} is not a URL")
        new = without_retired_port(old)
        require_okx_websocket_url(new, service)
        if new == old:
            continue
        pattern = re.compile(
            "(" + re.escape(json.dumps(field)) + r"\s*:\s*)" + re.escape(json.dumps(old))
        )
        text, count = pattern.subn(lambda match: match.group(1) + json.dumps(new), text)
        if count != 1:
            raise ValueError(f"{path}: expected exactly one {field} value, found {count}")
        changes[field] = {"from": old, "to": new}
    migrated = text.encode("utf-8")
    after = _decode(migrated, path)
    if identity_sha256(after) != identity_sha256(before):
        raise RuntimeError(f"{path}: rewrite changed a non-transport field")
    for field, service in SERVICE_BY_FIELD.items():
        require_okx_websocket_url(after[field], service)
    if after["market_websocket_url"] != before["market_websocket_url"]:
        raise RuntimeError(f"{path}: rewrite changed market_websocket_url")
    return migrated, changes


def _atomic_replace(path: Path, data: bytes, like: os.stat_result) -> None:
    staging = path.with_name(f".{path.name}.okx443-staging-{time.time_ns()}")
    descriptor = os.open(staging, os.O_WRONLY | os.O_CREAT | os.O_EXCL, like.st_mode & 0o7777)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if os.geteuid() == 0:
            os.chown(staging, like.st_uid, like.st_gid)
        os.chmod(staging, like.st_mode & 0o7777)
        os.replace(staging, path)
    finally:
        if staging.exists():
            staging.unlink()
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def migrate(path: Path, *, apply: bool, clock=time.time_ns) -> dict[str, object]:
    data = path.read_bytes()
    stat = path.stat()
    migrated, changes = migrate_bytes(data, path)
    before = _decode(data, path)
    report: dict[str, object] = {
        "path": str(path),
        "status": "NO_CHANGE" if not changes else ("APPLIED" if apply else "DRY_RUN"),
        "changes": changes,
        "sha256_before": _sha256(data),
        "sha256_after": _sha256(migrated),
        "identity_sha256": identity_sha256(before),
        "config_revision": before.get("config_revision"),
        "backup": None,
    }
    if not changes or not apply:
        return report
    backup = path.with_name(f"{path.name}{BACKUP_SUFFIX}{clock()}")
    shutil.copy2(path, backup)
    if os.geteuid() == 0:
        os.chown(backup, stat.st_uid, stat.st_gid)
    if backup.read_bytes() != data:
        raise RuntimeError(f"{backup}: backup differs from the original")
    _atomic_replace(path, migrated, stat)
    written = path.read_bytes()
    if written != migrated or identity_sha256(_decode(written, path)) != report["identity_sha256"]:
        raise RuntimeError(f"{path}: verification after write failed; restore {backup}")
    report["backup"] = str(backup)
    return report


def rollback(path: Path, backup: Path) -> dict[str, object]:
    if not backup.name.startswith(path.name + BACKUP_SUFFIX) or backup.parent != path.parent:
        raise ValueError("rollback source is not a backup written for this config")
    current = path.read_bytes()
    restored = backup.read_bytes()
    if identity_sha256(_decode(current, path)) != identity_sha256(_decode(restored, backup)):
        raise ValueError("backup identity differs from the current config; refusing")
    _atomic_replace(path, restored, path.stat())
    if path.read_bytes() != restored:
        raise RuntimeError(f"{path}: rollback verification failed")
    return {
        "path": str(path),
        "status": "ROLLED_BACK",
        "sha256_before": _sha256(current),
        "sha256_after": _sha256(restored),
        "from_backup": str(backup),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, action="append", required=True,
                        help="OKX native ingestor runtime JSON; repeatable")
    parser.add_argument("--apply", action="store_true",
                        help="write the change; omit to report only")
    parser.add_argument("--rollback-from", type=Path,
                        help="restore this backup into the single --config path")
    args = parser.parse_args()
    if args.rollback_from is not None:
        if len(args.config) != 1 or args.apply:
            parser.error("--rollback-from takes exactly one --config and no --apply")
        result = [rollback(args.config[0], args.rollback_from)]
    else:
        # Validate every file before writing any, so a bad file cannot leave
        # the set half migrated.
        for path in args.config:
            migrate(path, apply=False)
        result = [migrate(path, apply=args.apply) for path in args.config]
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
