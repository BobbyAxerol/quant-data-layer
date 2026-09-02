#!/usr/bin/env python3
"""Atomically materialize protected V2 identity files for a non-root alpha.

The source identity remains owned by the operator and is never mounted into an
alpha container. This helper copies only the required client material to one
workload-scoped mount whose permissions match the runtime UID/GID.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import uuid
from typing import Mapping


CONFIRM = "MATERIALIZE_QDL_ALPHA_V2_IDENTITY"
DEFAULT_STATE_ROOT = Path("/home/bobby/.local/state/qdl-v2")
_WORKLOAD_ID = re.compile(r"[a-z0-9][a-z0-9._-]{2,120}\Z")
_TLS_FILES = {
    "ca.crt": 0o444,
    "client.crt": 0o444,
    "client.key": 0o400,
}
_JWT_FILES = {
    "private.key": 0o400,
    "public.pem": 0o444,
}


class IdentityMaterializationError(RuntimeError):
    """Raised when a protected identity cannot be safely materialized."""


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _regular_file_names(path: Path, *, field: str) -> set[str]:
    if not path.is_dir() or path.is_symlink():
        raise IdentityMaterializationError(f"{field} must be a real directory")
    values: set[str] = set()
    for child in path.iterdir():
        if child.is_symlink() or not child.is_file():
            raise IdentityMaterializationError(f"{field} contains a non-regular file")
        values.add(child.name)
    return values


def _validate_source(path: Path, *, expected: Mapping[str, int], field: str) -> None:
    actual = _regular_file_names(path, field=field)
    if actual != set(expected):
        raise IdentityMaterializationError(
            f"{field} files differ: expected {sorted(expected)}, got {sorted(actual)}"
        )
    for name in expected:
        source = path / name
        if source.stat().st_size <= 0:
            raise IdentityMaterializationError(f"{field}/{name} is empty")


def _validate_target(
    *, state_root: Path, workload_id: str
) -> tuple[Path, Path]:
    if _WORKLOAD_ID.fullmatch(workload_id) is None:
        raise IdentityMaterializationError("workload id is invalid")
    root = state_root.resolve()
    if not root.is_dir() or root.is_symlink():
        raise IdentityMaterializationError("state root must be a real existing directory")
    identities_root = root / "workload-identities"
    target = identities_root / workload_id
    if target.exists() or target.is_symlink():
        raise FileExistsError(f"identity target already exists: {target}")
    return identities_root, target


def _copy_mode(
    *, source: Path, destination: Path, mode: int, uid: int, gid: int
) -> None:
    shutil.copyfile(source, destination)
    os.chown(destination, uid, gid)
    destination.chmod(mode)


def _materialize_directory(
    *,
    source: Path,
    destination: Path,
    files: Mapping[str, int],
    uid: int,
    gid: int,
) -> None:
    destination.mkdir(mode=0o500)
    os.chown(destination, uid, gid)
    destination.chmod(0o500)
    for name, mode in files.items():
        _copy_mode(
            source=source / name,
            destination=destination / name,
            mode=mode,
            uid=uid,
            gid=gid,
        )


def _manifest(
    *, workload_id: str, tls_source: Path, jwt_source: Path, uid: int, gid: int
) -> dict[str, object]:
    return {
        "schema": "qdl.v2.alpha-workload-identity.v1",
        "workload_id": workload_id,
        "workload_uid": uid,
        "workload_gid": gid,
        "tls": {
            name: {"sha256": _sha256_file(tls_source / name), "mode": format(mode, "04o")}
            for name, mode in sorted(_TLS_FILES.items())
        },
        "jwt": {
            name: {"sha256": _sha256_file(jwt_source / name), "mode": format(mode, "04o")}
            for name, mode in sorted(_JWT_FILES.items())
        },
        "secret_values_recorded": False,
    }


def materialize_identity(
    *,
    tls_source: Path,
    jwt_source: Path,
    state_root: Path = DEFAULT_STATE_ROOT,
    workload_id: str,
    workload_uid: int,
    workload_gid: int,
    apply: bool,
) -> dict[str, object]:
    """Validate and optionally atomically create one workload-scoped mount."""

    if workload_uid < 1 or workload_gid < 1:
        raise IdentityMaterializationError("workload uid and gid must be positive")
    _validate_source(tls_source, expected=_TLS_FILES, field="tls source")
    _validate_source(jwt_source, expected=_JWT_FILES, field="jwt source")
    identities_root, target = _validate_target(
        state_root=state_root, workload_id=workload_id
    )
    manifest = _manifest(
        workload_id=workload_id,
        tls_source=tls_source,
        jwt_source=jwt_source,
        uid=workload_uid,
        gid=workload_gid,
    )
    result = {
        "status": "APPLIED" if apply else "DRY_RUN",
        "target": str(target),
        "tls_mount": str(target / "tls"),
        "jwt_mount": str(target / "jwt"),
        "manifest_sha256": hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "secret_values_recorded": False,
        "runtime_mutations": 0,
        "order_actions": 0,
    }
    if not apply:
        return result
    if os.geteuid() != 0:
        raise PermissionError("apply requires effective root to set workload ownership")

    root_stat = state_root.resolve().stat()
    identities_root.mkdir(mode=0o750, exist_ok=True)
    os.chown(identities_root, root_stat.st_uid, root_stat.st_gid)
    identities_root.chmod(0o750)
    staging = identities_root / f".{workload_id}.staging-{uuid.uuid4().hex}"
    try:
        staging.mkdir(mode=0o750)
        os.chown(staging, root_stat.st_uid, root_stat.st_gid)
        staging.chmod(0o750)
        _materialize_directory(
            source=tls_source,
            destination=staging / "tls",
            files=_TLS_FILES,
            uid=workload_uid,
            gid=workload_gid,
        )
        _materialize_directory(
            source=jwt_source,
            destination=staging / "jwt",
            files=_JWT_FILES,
            uid=workload_uid,
            gid=workload_gid,
        )
        manifest_path = staging / "identity-manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.chown(manifest_path, root_stat.st_uid, root_stat.st_gid)
        manifest_path.chmod(0o640)
        staging.rename(target)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tls-source", type=Path, required=True)
    parser.add_argument("--jwt-source", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, default=DEFAULT_STATE_ROOT)
    parser.add_argument("--workload-id", required=True)
    parser.add_argument("--workload-uid", type=int, default=1000)
    parser.add_argument("--workload-gid", type=int, default=1000)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args(argv)
    if args.apply and args.confirm != CONFIRM:
        raise RuntimeError(f"--apply requires --confirm {CONFIRM}")
    result = materialize_identity(
        tls_source=args.tls_source,
        jwt_source=args.jwt_source,
        state_root=args.state_root,
        workload_id=args.workload_id,
        workload_uid=args.workload_uid,
        workload_gid=args.workload_gid,
        apply=args.apply,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
