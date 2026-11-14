"""Effective declaration of a role: which config files it runs, from where, at which revision.

DL v2.3.0 (declaration-only coverage). Adding an instrument or a derived
interval must be a config revision + coverage gate + ordered restart, never an
image. That holds only if every role reads its catalogue, acquisition plan and
consumer manifests from a mounted declaration and states what it loaded. Until
v2.3.0 the bar edge read all three from its image (``/app/config/v2``,
``/app/consumers/stable``) and Query read three manifests from its image, so a
declaration could not reach them and nobody could see the difference.

This module owns three things and nothing else:

* ``config_file_record``/``effective_declaration``: the provenance record of the
  files a role loaded (path, sha256, declared revision, ``IMAGE`` or ``MOUNTED``),
  logged at start and exposed by the role (bar edge heartbeat, Query
  ``/health/dependencies``).
* ``enforce_declaration``: optional fail-closed guards, both off by default so an
  unchanged deployment behaves exactly as before:
  ``QDL_STABLE_REQUIRE_MOUNTED_DECLARATION=1`` refuses any file under the image
  root; ``QDL_STABLE_DECLARATION_INDEX=<path>`` refuses any loaded file whose
  sha256 the rendered declaration index does not list.
* ``DeclaredUniverse``/``load_universes``: the universe files a consumer
  manifest selector expands against (see ``qdl.consumer.manifest``).

Boundary: reads files only. It never writes runtime state and never decides
what a role serves; the certified loaders (``StableSourceCatalog``,
``StableAcquisitionPlan``, ``ConsumerManifestLoader``) stay the only parsers.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

logger = logging.getLogger(__name__)

DECLARATION_INDEX_SCHEMA = "qdl.v2.declaration-index.v1"
EFFECTIVE_DECLARATION_SCHEMA = "qdl.v2.effective-declaration.v1"
DECLARED_UNIVERSE_SCHEMA = "qdl.v2.declared-universe.v1"
# D48 membership (scripts/kn_universe_top300.py) is accepted as a universe too:
# one signed membership, per-venue symbols.
MARKET_CAP_UNIVERSE_SCHEMA = "qdl.v2.market-cap-universe.v1"
IMAGE_ROOT = Path("/app")
REQUIRE_MOUNTED_ENV = "QDL_STABLE_REQUIRE_MOUNTED_DECLARATION"
DECLARATION_INDEX_ENV = "QDL_STABLE_DECLARATION_INDEX"
UNIVERSE_DIR_ENV = "QDL_STABLE_UNIVERSE_DIR"


class DeclarationError(RuntimeError):
    """A role's declaration is not the mounted, indexed one it was asked to run."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def config_file_record(
    path: str | os.PathLike[str],
    *,
    kind: str,
    revision: int | None,
    identity: str | None = None,
    image_root: Path = IMAGE_ROOT,
) -> dict[str, Any]:
    """One loaded config file: where it came from and what it declared."""

    resolved = Path(path)
    record: dict[str, Any] = {
        "kind": kind,
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "revision": revision,
        "source": "IMAGE" if _under(resolved, image_root) else "MOUNTED",
    }
    if identity is not None:
        record["identity"] = identity
    return record


def effective_declaration(
    records: Iterable[Mapping[str, Any]],
    *,
    role: str,
    index: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The role's effective declaration with one digest over every file record."""

    files = sorted((dict(item) for item in records), key=lambda item: (item["kind"], item["path"]))
    body = {"role": role, "files": [
        {key: item[key] for key in ("kind", "sha256", "revision") if key in item} for item in files
    ]}
    digest = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "schema": EFFECTIVE_DECLARATION_SCHEMA,
        "role": role,
        "declaration_revision": None if index is None else index.get("declaration_revision"),
        "digest": digest,
        "image_files": sum(1 for item in files if item["source"] == "IMAGE"),
        "files": files,
    }


def load_declaration_index(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Read a rendered declaration index (scripts/dl_declaration.py ``render``)."""

    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise DeclarationError("declaration index is unreadable") from error
    if not isinstance(payload, dict) or payload.get("schema") != DECLARATION_INDEX_SCHEMA:
        raise DeclarationError("declaration index schema is unsupported")
    files = payload.get("files")
    revision = payload.get("declaration_revision")
    if (
        not isinstance(files, dict)
        or not files
        or isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 1
    ):
        raise DeclarationError("declaration index fields are invalid")
    names = [Path(name).name for name in files]
    if len(names) != len(set(names)):
        raise DeclarationError("declaration index lists two files with one name")
    for name, value in files.items():
        if not isinstance(value, str) or len(value) != 64:
            raise DeclarationError(f"declaration index sha256 is invalid for {name}")
    return payload


def enforce_declaration(
    declaration: Mapping[str, Any],
    *,
    env: Mapping[str, str] | None = None,
) -> Mapping[str, Any] | None:
    """Apply the opt-in guards; return the verified index when one is configured."""

    values = os.environ if env is None else env
    files = declaration["files"]
    if values.get(REQUIRE_MOUNTED_ENV, "").strip() == "1":
        image = sorted(item["path"] for item in files if item["source"] == "IMAGE")
        if image:
            raise DeclarationError(
                "role is required to run a mounted declaration but loads image config: "
                + ",".join(image)
            )
    index_path = values.get(DECLARATION_INDEX_ENV, "").strip()
    if not index_path:
        return None
    index = load_declaration_index(index_path)
    listed = {Path(name).name: value for name, value in index["files"].items()}
    problems = []
    for item in files:
        expected = listed.get(Path(item["path"]).name)
        if expected is None:
            problems.append(f"{item['path']} is not in the declaration index")
        elif expected != item["sha256"]:
            problems.append(f"{item['path']} sha256 differs from the declaration index")
    if problems:
        raise DeclarationError("; ".join(problems))
    return index


def log_effective_declaration(declaration: Mapping[str, Any]) -> None:
    revisions = ",".join(
        f"{item['kind']}={item.get('identity') or ''}r{item['revision']}"
        for item in declaration["files"]
    )
    logger.info(
        "effective declaration role=%s digest=%s declaration_revision=%s image_files=%s %s",
        declaration["role"],
        declaration["digest"],
        declaration["declaration_revision"],
        declaration["image_files"],
        revisions,
    )


@dataclass(frozen=True, slots=True)
class DeclaredUniverse:
    """A named, revisioned instrument set a manifest selector may grant."""

    universe_id: str
    revision: int
    venue: str
    market: str
    members: tuple[str, ...]
    sha256: str
    path: str

    def __post_init__(self) -> None:
        if not self.universe_id.strip() or self.revision < 1:
            raise ValueError("declared universe identity is invalid")
        if not self.members or len(set(self.members)) != len(self.members):
            raise ValueError("declared universe members must be unique and non-empty")


def _membership_sha256(members: Iterable[str]) -> str:
    return hashlib.sha256(
        json.dumps(sorted(members), separators=(",", ":")).encode()
    ).hexdigest()


def load_universe(path: str | os.PathLike[str], *, venue: str, market: str) -> DeclaredUniverse:
    """Load one universe file for one venue/market.

    A declared universe names its venue/market and must match the selector. A
    D48 market-cap membership is venue-neutral and yields that venue's symbols.
    """

    source = Path(path)
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("declared universe must be a JSON object")
    schema = payload.get("schema")
    if schema == DECLARED_UNIVERSE_SCHEMA:
        if set(payload) != {
            "schema", "universe_id", "revision", "venue", "market", "members",
            "membership_sha256",
        }:
            raise ValueError("declared universe fields are incomplete or unknown")
        if (payload["venue"], payload["market"]) != (venue, market):
            raise ValueError(
                f"universe {payload['universe_id']} is {payload['venue']}/{payload['market']}, "
                f"selector asks {venue}/{market}"
            )
        members = tuple(str(item) for item in payload["members"])
        if payload["membership_sha256"] != _membership_sha256(members):
            raise ValueError(f"universe {payload['universe_id']} membership hash mismatch")
        universe_id = str(payload["universe_id"])
    elif schema == MARKET_CAP_UNIVERSE_SCHEMA:
        field = {"OKX": "okx_inst_id", "BINANCE": "binance_symbol"}.get(venue)
        if field is None or market not in {"SWAP", "USDM"}:
            raise ValueError("market-cap universe covers only OKX SWAP and Binance USDM")
        members = tuple(str(item[field]) for item in payload["members"])
        universe_id = source.stem
    else:
        raise ValueError(f"universe schema is unsupported: {schema}")
    return DeclaredUniverse(
        universe_id=universe_id,
        revision=int(payload["revision"]),
        venue=venue,
        market=market,
        members=members,
        sha256=_sha256_file(source),
        path=str(source),
    )


def declared_universe_document(
    *, universe_id: str, revision: int, venue: str, market: str, members: Iterable[str],
) -> dict[str, Any]:
    """The canonical universe document the declaration renderer writes."""

    ordered = sorted(set(members))
    return {
        "schema": DECLARED_UNIVERSE_SCHEMA,
        "universe_id": universe_id,
        "revision": int(revision),
        "venue": venue,
        "market": market,
        "members": ordered,
        "membership_sha256": _membership_sha256(ordered),
    }


def universe_directory(env: Mapping[str, str] | None, *, default: Path) -> Path:
    values = os.environ if env is None else env
    configured = values.get(UNIVERSE_DIR_ENV, "").strip()
    return Path(configured) if configured else default
