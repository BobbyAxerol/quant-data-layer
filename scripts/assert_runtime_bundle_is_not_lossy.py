#!/usr/bin/env python3
"""Refuse a runtime bundle that would remove a binding the running bundle serves.

R1.30 recorded the trap this check disarms. Regenerating ``<bundle>/runtime/*.json``
from the catalog silently drops every ``MARK_INDEX`` binding, because the catalog
describes the destination of a migration whose origin the runtime still holds
deliberately. A count check does not catch it: the Binance ingestor loses five
``@markPrice@1s`` bindings and gains five ``@kline_1m`` ones, a net of zero.

R1.28 caught it by diffing the regenerated bundle against the running one by hand
before applying anything. That worked because someone looked, and a control that
depends on someone looking is not a control. This is the refusal instead.

What it does: compares the binding identities of a proposed bundle against the
bundle that is running, per file, and exits non-zero naming every identity that
would disappear. An identity that merely moves to a sibling file - a core reshard -
is reported as a move, not a loss.

What it deliberately does not do: it never writes a config, starts or stops a
container, or decides whether a removal is *correct*. A removal that is intended
is named explicitly with ``--allow-removal`` (repeatable) or ``--allow-removal-file``,
so an intended removal leaves a written record in the command that performed it.

Boundary: identity is ``source_id`` in a core config and ``subscription_id`` in an
ingestor config - the same binding seen from the two ends of the wire. Reading only
the first would miss exactly the half of the R1.30 trap that is count-neutral, which
is what a first cut of this check did until it was run against the live bundle. A
binding whose identity survives while its ``native_channel``, ``provider_kind`` or
interval changes is a different question (that is the R1.28 capture-channel class of
defect) and is not what this refuses.

Exit codes: 0 nothing is lost, 1 something is lost, 2 the inputs could not be read.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterator, Mapping

# Every place a runtime config is known to carry a binding list. `core.json` nests
# them under `core`; the ingestor configs keep them at the top level.
BINDING_PATHS: tuple[tuple[str, ...], ...] = (
    ("bindings",),
    ("core", "bindings"),
)

# A core binding carries `source_id`; an ingestor binding carries `subscription_id`
# for the same thing. Both are checked, in this order.
IDENTITY_FIELDS: tuple[str, ...] = ("source_id", "subscription_id")


def _walk(document: Mapping[str, Any], path: tuple[str, ...]) -> list[Any] | None:
    node: Any = document
    for key in path:
        if not isinstance(node, Mapping) or key not in node:
            return None
        node = node[key]
    return node if isinstance(node, list) else None


def binding_identity(entry: Mapping[str, Any]) -> str | None:
    """The identity of one binding, whichever end of the wire wrote it."""
    for field in IDENTITY_FIELDS:
        value = entry.get(field)
        if isinstance(value, str) and value:
            return value
    return None


def binding_identities(document: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Index every binding in one runtime config by its identity."""
    found: dict[str, dict[str, Any]] = {}
    for path in BINDING_PATHS:
        entries = _walk(document, path)
        if entries is None:
            continue
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            source_id = binding_identity(entry)
            if source_id is None:
                continue
            # A duplicate identity inside one file is itself a defect the core
            # refuses at startup; this check is not the place to adjudicate it.
            found.setdefault(source_id, dict(entry))
    return found


def _runtime_configs(bundle: Path) -> Iterator[Path]:
    runtime = bundle / "runtime" if (bundle / "runtime").is_dir() else bundle
    yield from sorted(runtime.glob("*.json"))


def read_bundle(bundle: Path) -> dict[str, dict[str, dict[str, Any]]]:
    """Map ``filename -> source_id -> binding`` for one bundle directory."""
    bindings: dict[str, dict[str, dict[str, Any]]] = {}
    for config in _runtime_configs(bundle):
        try:
            document = json.loads(config.read_text())
        except (OSError, ValueError) as error:
            raise SystemExit(f"cannot read {config}: {error}") from error
        if isinstance(document, Mapping):
            identities = binding_identities(document)
            if identities:
                bindings[config.name] = identities
    return bindings


class Loss(tuple):
    """A binding present in the running bundle and absent from the proposed one."""

    __slots__ = ()

    def __new__(cls, filename: str, source_id: str, moved_to: str | None):
        return super().__new__(cls, (filename, source_id, moved_to))

    @property
    def filename(self) -> str:
        return self[0]

    @property
    def source_id(self) -> str:
        return self[1]

    @property
    def moved_to(self) -> str | None:
        return self[2]


def compare(
    running: Mapping[str, Mapping[str, Any]],
    proposed: Mapping[str, Mapping[str, Any]],
) -> tuple[list[Loss], list[Loss], list[str]]:
    """Return (losses, moves, additions) between two bundles."""
    everywhere_proposed: dict[str, str] = {}
    for filename, identities in proposed.items():
        for source_id in identities:
            everywhere_proposed.setdefault(source_id, filename)

    losses: list[Loss] = []
    moves: list[Loss] = []
    for filename, identities in sorted(running.items()):
        proposed_here = proposed.get(filename, {})
        for source_id in sorted(identities):
            if source_id in proposed_here:
                continue
            elsewhere = everywhere_proposed.get(source_id)
            if elsewhere is None:
                losses.append(Loss(filename, source_id, None))
            else:
                moves.append(Loss(filename, source_id, elsewhere))

    everywhere_running = {s for ids in running.values() for s in ids}
    additions = sorted(
        source_id
        for identities in proposed.values()
        for source_id in identities
        if source_id not in everywhere_running
    )
    return losses, moves, additions


def _allowed_removals(args: argparse.Namespace) -> set[str]:
    allowed = set(args.allow_removal or ())
    if args.allow_removal_file:
        path = Path(args.allow_removal_file)
        try:
            lines = path.read_text().splitlines()
        except OSError as error:
            raise SystemExit(f"cannot read {path}: {error}") from error
        allowed.update(
            line.strip() for line in lines if line.strip() and not line.startswith("#")
        )
    return allowed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--running", required=True, help="bundle directory now serving")
    parser.add_argument("--proposed", required=True, help="bundle directory to be applied")
    parser.add_argument(
        "--allow-removal",
        action="append",
        metavar="IDENTITY",
        help="a source_id or subscription_id whose removal is intended (repeatable)",
    )
    parser.add_argument(
        "--allow-removal-file",
        metavar="PATH",
        help="file of intended-removal identities, one per line, # comments allowed",
    )
    args = parser.parse_args(argv)

    running_dir, proposed_dir = Path(args.running), Path(args.proposed)
    for label, path in (("running", running_dir), ("proposed", proposed_dir)):
        if not path.is_dir():
            print(f"{label} bundle is not a directory: {path}", file=sys.stderr)
            return 2

    running = read_bundle(running_dir)
    proposed = read_bundle(proposed_dir)
    if not running:
        print(f"running bundle declares no bindings at all: {running_dir}", file=sys.stderr)
        return 2

    losses, moves, additions = compare(running, proposed)
    allowed = _allowed_removals(args)
    refused = [loss for loss in losses if loss.source_id not in allowed]
    permitted = [loss for loss in losses if loss.source_id in allowed]

    running_total = sum(len(ids) for ids in running.values())
    proposed_total = sum(len(ids) for ids in proposed.values())
    print(f"running  {running_dir}: {running_total} bindings in {len(running)} files")
    print(f"proposed {proposed_dir}: {proposed_total} bindings in {len(proposed)} files")

    for loss in moves:
        print(f"  moved   {loss.source_id}: {loss.filename} -> {loss.moved_to}")
    for source_id in additions:
        print(f"  added   {source_id}")
    for loss in permitted:
        print(f"  removed {loss.source_id} from {loss.filename} (allowed)")

    if not refused:
        print("no binding is lost; the proposed bundle may be applied")
        return 0

    print("", file=sys.stderr)
    print(
        f"REFUSED: {len(refused)} binding(s) the running bundle serves are absent "
        "from the proposed bundle:",
        file=sys.stderr,
    )
    for loss in refused:
        print(f"  {loss.filename}: {loss.source_id}", file=sys.stderr)
    print(
        "\nIf a removal is intended, name it with --allow-removal so the intent is "
        "recorded in the command that performed it.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
