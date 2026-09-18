#!/usr/bin/env python3
"""Carry a bar-edge checkpoint across an acquisition revision, keeping its watermarks.

Dry-run by default. It writes nothing without `--apply`, and it refuses to write
while the bar edge is running.

Why it exists. `StableBinanceBarEdge._restore_state` compares the checkpoint's
identity against the runtime authority field by field and raises
`stable BAR checkpoint acquisition_revision differs from runtime authority` on a
mismatch. That is the right behaviour - a checkpoint from a different plan must
never be silently adopted - but it means bumping the acquisition revision stops
the edge from starting at all. Not for the bindings that changed: for **every**
BAR binding it owns, at every interval, on both venues.

A revision bump that changes only acquisition *modes* leaves every watermark
valid: the same bindings, the same closed bars, the same canonical cache. This
carries those watermarks across and refuses anything else. If the binding set
moved, or the catalog identity moved, or the canonical cache was rebuilt, that
is a bootstrap or a repair, not a migration, and this exits non-zero saying so.

    python3 -B scripts/migrate_stable_bar_edge_checkpoint.py
    python3 -B scripts/migrate_stable_bar_edge_checkpoint.py --apply
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

ROLE = "qdl_v2_stable_candidate-binance_bar_edge-1"
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"

# Fields the checkpoint carries that this migration is allowed to change. Every
# other field is identity the migration must find already equal, or state it
# must copy through untouched.
MIGRATABLE = ("acquisition_revision",)
# Identity that must already match. A difference here is a different migration.
MUST_MATCH = ("slice_id", "authority_revision", "warmup_rows", "canonical_cache_id")
CARRIED = ("connection_generation", "last_open_ms")


def role_is_running(role: str) -> bool:
    result = subprocess.run(
        ["docker", "ps", "--filter", f"name={role}", "--format", "{{.Names}}"],
        capture_output=True, text=True, timeout=60,
    )
    return role in result.stdout


STATE_VOLUME = "qdl_v2_stable_candidate_stable_state"


def read_checkpoint(role: str, path: str) -> dict:
    """Read the checkpoint through the state volume, not through the role.

    The migration's whole point is to run while the edge is stopped, so reading
    it with `docker exec` into that role fails exactly when it is needed.
    """

    script = f"import sys;sys.stdout.write(open({path!r}).read())"
    result = subprocess.run(
        ["docker", "run", "--rm", "-i",
         "-v", f"{STATE_VOLUME}:/var/lib/qdl-stable",
         "--entrypoint", "python", _image_of(role), "-c", script],
        capture_output=True, text=True, timeout=180,
    )
    if result.returncode != 0:
        raise SystemExit(f"cannot read the checkpoint: {result.stderr.strip()[:300]}")
    return json.loads(result.stdout)


def state_path_of(role: str) -> str:
    result = subprocess.run(
        ["docker", "inspect", role, "--format", "{{json .Config.Env}}"],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        raise SystemExit("cannot inspect the bar edge role")
    for item in json.loads(result.stdout):
        key, _, value = item.partition("=")
        if key == "QDL_STABLE_BAR_STATE_PATH":
            return value
    raise SystemExit("the bar edge role declares no QDL_STABLE_BAR_STATE_PATH")


def planned_identity() -> dict:
    """What `_state_identity_payload` will compute from the repository config.

    The two documents are read as YAML rather than through
    `StableSourceCatalog` / `StableAcquisitionPlan`, because this script needs
    the `docker` CLI on the host and those loaders live in the image. The three
    fields it reads are plain: the top-level revisions, and the BAR bindings the
    edge owns. The binding count is checked against the live checkpoint, so a
    parse that disagreed with the loader would be refused rather than applied.
    """

    catalog = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    acquisition = yaml.safe_load(ACQUISITION_PATH.read_text(encoding="utf-8"))
    feed_by_id = {
        item["binding_id"]: item.get("feed") for item in catalog.get("bindings", [])
    }
    binding_ids = sorted(
        item["binding_id"]
        for item in acquisition.get("bindings", [])
        if item.get("enabled", True)
        and item.get("runtime") in {"BINANCE", "OKX"}
        and feed_by_id.get(item["binding_id"]) == "BAR"
    )
    return {
        "catalog_revision": int(catalog["catalog_revision"]),
        "acquisition_revision": int(acquisition["revision"]),
        "binding_ids": binding_ids,
    }


def migration_plan(current: dict, planned: dict) -> tuple[dict | None, list[str], bool]:
    """Decide whether this checkpoint may cross to the planned revision.

    Pure, so the refusals can be tested without a running role. Returns the
    migrated payload, the reasons to refuse, and whether there is nothing to do.
    """

    problems: list[str] = []
    if sorted(current.get("binding_ids", [])) != planned["binding_ids"]:
        added = sorted(set(planned["binding_ids"]) - set(current.get("binding_ids", [])))
        removed = sorted(set(current.get("binding_ids", [])) - set(planned["binding_ids"]))
        problems.append(
            "the BAR binding set moved, which is a bootstrap and not a migration: "
            f"+{added[:5]} -{removed[:5]}"
        )
    if int(current.get("catalog_revision", -1)) != planned["catalog_revision"]:
        problems.append(
            "catalog_revision differs; the instrument catalog moved, so the watermarks "
            "cannot be assumed to describe the same bars"
        )
    for field in MUST_MATCH:
        if field not in current:
            problems.append(f"the checkpoint carries no {field}")
    if int(current.get("acquisition_revision", -1)) == planned["acquisition_revision"]:
        return None, problems, True
    if problems:
        return None, problems, False
    migrated = dict(current)
    migrated["acquisition_revision"] = planned["acquisition_revision"]
    for field in MUST_MATCH + CARRIED:
        if migrated.get(field) != current.get(field):
            raise AssertionError(f"migration must not change {field}")
    return migrated, [], False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", default=ROLE)
    parser.add_argument("--state-path", help="override the path read from the role's env")
    parser.add_argument("--apply", action="store_true", help="write; otherwise report only")
    args = parser.parse_args(argv)

    path = args.state_path or state_path_of(args.role)
    current = read_checkpoint(args.role, path)
    planned = planned_identity()

    print(f"  checkpoint            {path}")
    print(f"  schema                {current.get('schema')}")
    print(f"  acquisition_revision  {current.get('acquisition_revision')} -> {planned['acquisition_revision']}")
    print(f"  catalog_revision      {current.get('catalog_revision')} (planned {planned['catalog_revision']})")
    print(f"  binding_ids           {len(current.get('binding_ids', []))} (planned {len(planned['binding_ids'])})")
    print(f"  last_open_ms          {len(current.get('last_open_ms', {}))} watermarks carried")

    migrated, problems, noop = migration_plan(current, planned)
    if noop:
        print("\n  nothing to migrate: the checkpoint already names this acquisition revision")
        return 0
    if problems:
        print("\n  REFUSED:")
        for item in problems:
            print(f"    {item}")
        return 1
    assert migrated is not None

    if not args.apply:
        print("\n  DRY RUN: would rewrite acquisition_revision only, keeping "
              f"{len(current.get('last_open_ms', {}))} watermarks, the connection "
              "generation and the canonical cache identity. Pass --apply to write.")
        return 0

    if role_is_running(args.role):
        print("\n  REFUSED: the bar edge is running and would overwrite this file on its "
              "next publish. Stop the role, migrate, then start it.")
        return 1

    payload = json.dumps(migrated, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    backup = f"{path}.pre-r{planned['acquisition_revision']}"
    writer = (
        "import json,os,sys\n"
        f"path={path!r}\nbackup={backup!r}\npayload={payload!r}\n"
        "os.replace(path, backup) if not os.path.exists(backup) else None\n"
        "tmp=path+'.tmp'\n"
        "fd=os.open(tmp, os.O_WRONLY|os.O_CREAT|os.O_TRUNC, 0o600)\n"
        "os.write(fd, payload.encode('ascii')); os.fsync(fd); os.close(fd)\n"
        "os.replace(tmp, path)\n"
        "print('written')\n"
    )
    result = subprocess.run(
        ["docker", "run", "--rm", "-i",
         "-v", f"{STATE_VOLUME}:/var/lib/qdl-stable",
         "--entrypoint", "python", _image_of(args.role), "-c", writer],
        capture_output=True, text=True, timeout=180,
    )
    if result.returncode != 0:
        raise SystemExit(f"write failed: {result.stderr.strip()[:400]}")
    print(f"\n  APPLIED. previous checkpoint kept at {backup}")
    return 0


def _image_of(role: str) -> str:
    result = subprocess.run(
        ["docker", "inspect", role, "--format", "{{.Config.Image}}"],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        raise SystemExit("cannot resolve the bar edge image")
    return result.stdout.strip()


if __name__ == "__main__":
    raise SystemExit(main())
