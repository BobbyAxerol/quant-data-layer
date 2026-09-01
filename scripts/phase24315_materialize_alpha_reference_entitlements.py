"""Materialize declared alpha reference-data entitlements and release routes.

This compiler turns the already reviewed ``reference-l2`` provider capability
matrix into exact, bounded alpha entitlements for the five-liquid Binance USD-M
and OKX Swap instruments.  It never creates acquisition demand or a poller:
reference data stays on-demand provider I/O behind ``reference:batch``.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import yaml

from qdl.consumer import ConsumerManifestLoader, requirement_key


ROOT = Path(__file__).resolve().parents[1]
_REFERENCE_FEEDS = frozenset({
    "FUNDING_RATE",
    "OPEN_INTEREST",
    "LONG_SHORT_RATIO",
    "TAKER_FLOW",
    "MARK_INDEX_PRICE",
    "CONTRACT_METADATA",
    "BASIS",
})
_REFERENCE_ROUTE = {
    "route": "V2_PRIMARY",
    "fallback": "BLOCKED",
    "reason": "V1_REFERENCE_EQUIVALENCE_UNPROVEN",
}
_TARGETS = {
    "alpha.binance.paper.stable": "alpha-binance-paper.yaml",
    "alpha.okx.paper.stable": "alpha-okx-paper.yaml",
}


def _yaml_bytes(value: Mapping[str, Any]) -> bytes:
    return yaml.safe_dump(dict(value), sort_keys=False).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _is_managed_reference(item: Mapping[str, Any]) -> bool:
    return (
        str(item.get("feed")) in _REFERENCE_FEEDS
        and str(item.get("source_policy_id")) == "crypto_liquid_v2"
    )


def _source_uids(manifest: Mapping[str, Any]) -> tuple[str, ...]:
    values = []
    for item in manifest["spec"]["requirements"]:
        if str(item.get("feed")) in {"TRADE", "BAR"}:
            uid = str(item["instrument_uid"])
            if uid not in values:
                values.append(uid)
    if len(values) != 5:
        raise ValueError("alpha reference entitlement requires exactly five liquid instruments")
    return tuple(values)


def _templates_by_uid(reference_manifest: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for item in reference_manifest["spec"]["requirements"]:
        if not _is_managed_reference(item):
            continue
        result.setdefault(str(item["instrument_uid"]), []).append(deepcopy(item))
    for values in result.values():
        values.sort(key=lambda item: (str(item["feed"]), str(item.get("interval") or "")))
    return result


def _materialize_manifest(
    manifest: Mapping[str, Any],
    *,
    templates: Mapping[str, list[dict[str, Any]]],
) -> tuple[dict[str, Any], bool]:
    result = deepcopy(dict(manifest))
    requirements = list(result["spec"]["requirements"])
    base = [item for item in requirements if not _is_managed_reference(item)]
    generated: list[dict[str, Any]] = []
    for uid in _source_uids(result):
        try:
            rows = templates[uid]
        except KeyError as error:
            raise ValueError(f"reference capability has no template for {uid}") from error
        for row in rows:
            value = deepcopy(row)
            value["consumer_grade"] = "ALPHA"
            generated.append(value)
    result["spec"]["requirements"] = [*base, *generated]
    changed = result["spec"]["requirements"] != requirements
    if changed:
        result["metadata"]["revision"] = int(result["metadata"]["revision"]) + 1
    return result, changed


def _materialize_route(
    route: Mapping[str, Any],
    *,
    manifests: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], bool]:
    result = deepcopy(dict(route))
    changed = False
    by_id = {str(item["consumer_id"]): item for item in result["consumers"]}
    for consumer_id, manifest_payload in manifests.items():
        try:
            target = by_id[consumer_id]
        except KeyError as error:
            raise ValueError(f"stable release route has no consumer {consumer_id}") from error
        manifest = ConsumerManifestLoader.from_mapping(manifest_payload)
        current_products = {
            str(item["requirement_key"]): item for item in target["products"]
        }
        products = []
        for requirement in manifest.requirements:
            key = requirement_key(requirement)
            existing = current_products.get(key)
            if existing is None:
                products.append({"requirement_key": key, **_REFERENCE_ROUTE})
            else:
                products.append(deepcopy(existing))
        if (
            int(target["manifest_revision"]) != manifest.manifest_revision
            or str(target["manifest_sha256"]) != manifest.manifest_sha256
            or products != target["products"]
        ):
            changed = True
        target["manifest_revision"] = manifest.manifest_revision
        target["manifest_sha256"] = manifest.manifest_sha256
        target["products"] = products
    if changed:
        result["revision"] = int(result["revision"]) + 1
    return result, changed


def build_documents(
    *,
    reference_manifest: Mapping[str, Any],
    alpha_manifests: Mapping[str, Mapping[str, Any]],
    release_route: Mapping[str, Any],
    primary_route: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, object]]:
    templates = _templates_by_uid(reference_manifest)
    manifests: dict[str, dict[str, Any]] = {}
    manifest_changed = False
    for consumer_id in _TARGETS:
        manifest, changed = _materialize_manifest(alpha_manifests[consumer_id], templates=templates)
        manifests[consumer_id] = manifest
        manifest_changed = manifest_changed or changed
    route, route_changed = _materialize_route(release_route, manifests=manifests)
    primary = deepcopy(dict(primary_route))
    if manifest_changed:
        primary["revision"] = int(primary["revision"]) + 1
    summary = {
        "schema": "qdl.phase24315.alpha-reference-entitlements.v1",
        "status": "READY",
        "manifest_changed": manifest_changed,
        "release_route_changed": route_changed,
        "alpha_reference_counts": {
            consumer_id: sum(_is_managed_reference(item) for item in payload["spec"]["requirements"])
            for consumer_id, payload in manifests.items()
        },
        "manifest_revisions": {
            consumer_id: int(payload["metadata"]["revision"])
            for consumer_id, payload in manifests.items()
        },
        "release_revision": int(route["revision"]),
        "primary_route_revision": int(primary["revision"]),
    }
    return manifests, route, primary, summary


def _load(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must be a YAML mapping")
    return payload


def _write_if_changed(path: Path, payload: Mapping[str, Any]) -> bool:
    value = _yaml_bytes(payload)
    if path.read_bytes() == value:
        return False
    path.write_bytes(value)
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-manifest", type=Path, default=ROOT / "consumers/stable/reference-l2-stable.yaml")
    parser.add_argument("--release-route", type=Path, default=ROOT / "config/v2/stable-v2-release-routing.yaml")
    parser.add_argument("--primary-route", type=Path, default=ROOT / "config/v2/stable-primary-consumer-routing.yaml")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    paths = {consumer_id: ROOT / "consumers/stable" / name for consumer_id, name in _TARGETS.items()}
    manifests, route, primary, summary = build_documents(
        reference_manifest=_load(args.reference_manifest),
        alpha_manifests={consumer_id: _load(path) for consumer_id, path in paths.items()},
        release_route=_load(args.release_route),
        primary_route=_load(args.primary_route),
    )
    changed_files: list[str] = []
    if args.apply:
        for consumer_id, path in paths.items():
            if _write_if_changed(path, manifests[consumer_id]):
                changed_files.append(str(path))
        if _write_if_changed(args.release_route, route):
            changed_files.append(str(args.release_route))
        if _write_if_changed(args.primary_route, primary):
            changed_files.append(str(args.primary_route))
    print(json.dumps({**summary, "changed_files": changed_files, "status": "APPLIED" if args.apply else "DRY_RUN"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
