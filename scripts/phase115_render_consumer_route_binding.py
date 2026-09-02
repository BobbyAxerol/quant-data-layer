#!/usr/bin/env python3
"""Render one sealed Phase-11.5 consumer route binding from a local artifact.

This is deliberately pure control-plane tooling: it reads an existing universal
release manifest/preflight artifact and writes one caller-named JSON document.
It never opens a provider connection, starts a role, changes a route, or writes
to Kafka, Redis, SQLite, V1, Trading System or an alpha.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

import yaml

from qdl.consumer.universal_release import (
    ConsumerRouteBinding,
    UniversalReleaseManifest,
)


def _read_mapping(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"release artifact is missing: {path}") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"release artifact is not JSON: {path}") from error
    if not isinstance(value, Mapping):
        raise ValueError("release artifact must be a JSON object")
    return value


def _manifest_from_artifact(value: Mapping[str, Any]) -> UniversalReleaseManifest:
    raw_manifest = value.get("release_manifest", value)
    if not isinstance(raw_manifest, Mapping):
        raise ValueError("release artifact has no canonical release_manifest")
    manifest = UniversalReleaseManifest.from_canonical_mapping(raw_manifest)
    summary = value.get("release_summary")
    if summary is not None:
        if (
            not isinstance(summary, Mapping)
            or summary.get("schema") != "qdl.phase115.universal-release-preflight.v1"
            or summary.get("status") != "PREPARED"
            or summary.get("manifest_sha256") != manifest.digest
        ):
            raise ValueError("release preflight summary does not bind the canonical manifest")
    return manifest


def _consumer_manifest_revision(path: Path, *, consumer_id: str) -> int:
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"consumer manifest is missing: {path}") from error
    if not isinstance(value, Mapping):
        raise ValueError("consumer manifest must be a mapping")
    metadata = value.get("metadata")
    if not isinstance(metadata, Mapping):
        raise ValueError("consumer manifest metadata is missing")
    if str(metadata.get("id", "")).strip() != consumer_id:
        raise ValueError("consumer manifest id differs from requested consumer")
    revision = metadata.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise ValueError("consumer manifest revision must be a positive integer")
    return revision


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-artifact", type=Path, required=True)
    parser.add_argument("--consumer-id", required=True)
    parser.add_argument(
        "--consumer-manifest",
        type=Path,
        help=(
            "Optional canonical consumer manifest. When supplied, render a "
            "generation-bound v2 binding that seals metadata.revision."
        ),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--independent-v1-venue",
        action="append",
        default=["DNSE"],
        help="Explicit V1-only venue retained outside this V2 release (repeatable).",
    )
    args = parser.parse_args(argv)
    if args.release_artifact.resolve() == args.output.resolve():
        raise ValueError("release artifact and binding output must be different files")
    manifest = _manifest_from_artifact(_read_mapping(args.release_artifact))
    consumer_id = str(args.consumer_id)
    consumer_manifest_revision = (
        _consumer_manifest_revision(args.consumer_manifest, consumer_id=consumer_id)
        if args.consumer_manifest is not None
        else None
    )
    binding = ConsumerRouteBinding.from_manifest(
        manifest,
        consumer_id=consumer_id,
        independent_v1_venues=tuple(str(item) for item in args.independent_v1_venue),
        consumer_manifest_revision=consumer_manifest_revision,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(binding.canonical_mapping(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "status": "RENDERED_SOURCE_ONLY",
        "consumer_id": binding.consumer_id,
        "release_revision": binding.release_revision,
        "universal_manifest_sha256": binding.universal_manifest_sha256,
        "binding_sha256": binding.binding_sha256,
        "consumer_manifest_revision": binding.consumer_manifest_revision,
        "product_count": len(binding.products),
        "runtime_mutations": 0,
        "order_actions": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
