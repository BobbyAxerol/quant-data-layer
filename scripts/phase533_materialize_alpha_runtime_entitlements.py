"""Render bounded five-liquid V2 alpha entitlements from canonical demand.

The output is a declaration only.  It never opens a provider connection,
creates acquisition demand, or changes a runtime bundle.  Shared Rust ingest,
bar-edge and projector roles stay the only producers for these routes.
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
_TARGETS = {
    "alpha.binance.paper.stable": {
        "filename": "alpha-binance-paper.yaml",
        "venue": "BINANCE",
        "market": "USDM",
        "product_type": "PERPETUAL",
    },
    "alpha.okx.paper.stable": {
        "filename": "alpha-okx-paper.yaml",
        "venue": "OKX",
        "market": "SWAP",
        "product_type": "PERPETUAL",
    },
}
_REALTIME_FEEDS = frozenset({
    "BAR", "TRADE", "QUOTE", "BOOK_SNAPSHOT", "BOOK_DELTA",
})
_REFERENCE_FEEDS = frozenset({
    "FUNDING_RATE", "OPEN_INTEREST", "LONG_SHORT_RATIO", "TAKER_FLOW",
    "MARK_INDEX_PRICE", "CONTRACT_METADATA", "BASIS",
})
_FEED_ORDER = {
    "TRADE": 0,
    "QUOTE": 1,
    "BAR": 2,
    "BOOK_SNAPSHOT": 3,
    "BOOK_DELTA": 4,
}
_INTERVAL_ORDER = {
    interval: index
    for index, interval in enumerate(
        ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "2d", "3d", "1w")
    )
}
_BLOCKED_ROUTE_REASONS = {
    "BAR": "V1_FINAL_BAR_EQUIVALENCE_UNPROVEN",
    "QUOTE": "V1_QUOTE_EQUIVALENCE_UNPROVEN",
    "BOOK_SNAPSHOT": "V1_L2_EQUIVALENCE_UNPROVEN",
    "BOOK_DELTA": "V1_L2_EQUIVALENCE_UNPROVEN",
}
_REFERENCE_ROUTE = {
    "route": "V2_PRIMARY",
    "fallback": "BLOCKED",
    "reason": "V1_REFERENCE_EQUIVALENCE_UNPROVEN",
}


def _yaml_bytes(value: Mapping[str, Any]) -> bytes:
    return yaml.safe_dump(dict(value), sort_keys=False).encode("utf-8")


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


def _identity(row: Mapping[str, Any]) -> tuple[str, str, str, str, str, str | None, str]:
    return (
        str(row["venue"]).upper(),
        str(row["market"]).upper(),
        str(row["product_type"]).upper(),
        str(row["native_symbol"]).upper(),
        str(row["feed"]).upper(),
        str(row["interval"]).lower() if row.get("interval") is not None else None,
        str(row["source_policy_id"]),
    )


def _catalog_indexes(
    catalog: Mapping[str, Any],
) -> tuple[dict[str, Mapping[str, Any]], dict[tuple[str, str, str | None, str], Mapping[str, Any]]]:
    instruments = {
        str(item["instrument_uid"]): item
        for item in catalog.get("instruments", [])
        if isinstance(item, Mapping)
    }
    bindings: dict[tuple[str, str, str | None, str], Mapping[str, Any]] = {}
    for item in catalog.get("bindings", []):
        if not isinstance(item, Mapping):
            continue
        source = item.get("source")
        if not isinstance(source, Mapping):
            raise ValueError("stable source binding has no source mapping")
        key = (
            str(item["instrument_uid"]),
            str(item["feed"]).upper(),
            str(item["interval"]).lower() if item.get("interval") is not None else None,
            str(source["source_policy_id"]),
        )
        if key in bindings:
            raise ValueError(f"stable source binding is duplicated: {key}")
        bindings[key] = item
    return instruments, bindings


def _target_uids(
    manifest: Mapping[str, Any],
    *,
    target: Mapping[str, str],
    instruments: Mapping[str, Mapping[str, Any]],
) -> tuple[str, ...]:
    values: list[str] = []
    for item in manifest["spec"]["requirements"]:
        if (
            item.get("feed") != "TRADE"
            or item.get("source_policy_id") != "crypto_primary_v2"
        ):
            continue
        uid = str(item["instrument_uid"])
        instrument = instruments.get(uid)
        if instrument is None:
            raise ValueError(f"alpha manifest references an unknown instrument: {uid}")
        if all(
            str(instrument.get(field, "")).upper() == target[field]
            for field in ("venue", "market", "product_type")
        ) and uid not in values:
            values.append(uid)
    if len(values) != 5:
        raise ValueError("alpha runtime entitlement requires exactly five liquid native instruments")
    # The source manifest order is not a semantic identity. Sorting makes the
    # generated artifact stable after its first revision bump.
    return tuple(sorted(values))


def _demand_rows(
    demand: Mapping[str, Any],
    *,
    instrument: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    expected = (
        str(instrument["venue"]).upper(),
        str(instrument["market"]).upper(),
        str(instrument["product_type"]).upper(),
        str(instrument["native_symbol"]).upper(),
    )
    values: list[Mapping[str, Any]] = []
    for consumer in demand.get("consumers", []):
        if not isinstance(consumer, Mapping):
            continue
        for row in consumer.get("requirements", []):
            if not isinstance(row, Mapping) or str(row.get("feed", "")).upper() not in _REALTIME_FEEDS:
                continue
            identity = (
                str(row.get("venue", "")).upper(),
                str(row.get("market", "")).upper(),
                str(row.get("product_type", "")).upper(),
                str(row.get("native_symbol", "")).upper(),
            )
            if identity == expected:
                values.append(row)
    return values


def _manifest_requirement(
    row: Mapping[str, Any],
    *,
    instrument_uid: str,
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    feed = str(row["feed"]).upper()
    quality = binding.get("quality")
    if not isinstance(quality, Mapping):
        raise ValueError("stable source binding has no quality mapping")
    freshness = row.get("max_freshness_ms", quality.get("stale_after_ms"))
    if not isinstance(freshness, int) or isinstance(freshness, bool) or freshness <= 0:
        raise ValueError("stable source binding freshness is invalid")
    result: dict[str, Any] = {
        "instrument_uid": instrument_uid,
        "feed": feed,
        "consumer_grade": "ALPHA",
        "source_policy_id": str(row["source_policy_id"]),
        "interval": row.get("interval"),
        "warmup_limit": 10_000 if feed == "BAR" else 0,
        "max_freshness_ms": freshness,
        "require_full_coverage": True,
        "require_final_bars": bool(quality.get("require_final_bar", False)) if feed == "BAR" else False,
        "stale_policy": "BLOCK",
        "gap_policy": "BLOCK",
        "recovery": "SNAPSHOT_AND_REPLAY",
        "bar_revision_policy": "EMIT_REVISIONS" if feed == "BAR" else "LATEST",
    }
    if feed == "TRADE":
        result["event_recency_policy"] = "OBSERVE"
    if feed in {"TRADE", "QUOTE", "BOOK_SNAPSHOT", "BOOK_DELTA"}:
        result["max_session_liveness_ms"] = 45_000
    return result


def _realtime_templates(
    *,
    manifest: Mapping[str, Any],
    demand: Mapping[str, Any],
    instruments: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[tuple[str, str, str | None, str], Mapping[str, Any]],
    target: Mapping[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for uid in _target_uids(manifest, target=target, instruments=instruments):
        instrument = instruments[uid]
        source_rows = _demand_rows(demand, instrument=instrument)
        keys = {_identity(row) for row in source_rows}
        if len(keys) != len(source_rows):
            raise ValueError("stable crypto demand has duplicate alpha runtime identities")
        expected = {
            "TRADE": 1,
            "QUOTE": 1,
            "BOOK_SNAPSHOT": 1,
            "BOOK_DELTA": 1,
            "BAR": 14,
        }
        counts = {
            feed: sum(str(row["feed"]).upper() == feed for row in source_rows)
            for feed in expected
        }
        if counts != expected:
            raise ValueError(
                f"stable crypto demand is incomplete for {instrument['native_symbol']}: {counts}"
            )
        for source_row in source_rows:
            feed = str(source_row["feed"]).upper()
            interval = (
                str(source_row["interval"]).lower()
                if source_row.get("interval") is not None
                else None
            )
            policy_id = str(source_row["source_policy_id"])
            try:
                binding = bindings[(uid, feed, interval, policy_id)]
            except KeyError as error:
                raise ValueError(
                    f"stable source catalog misses {uid}/{feed}/{interval}/{policy_id}"
                ) from error
            rows.append(
                _manifest_requirement(source_row, instrument_uid=uid, binding=binding)
            )
    rows.sort(
        key=lambda item: (
            item["instrument_uid"],
            _FEED_ORDER[item["feed"]],
            _INTERVAL_ORDER.get(item.get("interval"), -1),
        )
    )
    return rows


def _reference_templates(
    reference_manifest: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for item in reference_manifest["spec"]["requirements"]:
        if (
            str(item.get("feed")) not in _REFERENCE_FEEDS
            or str(item.get("source_policy_id")) != "crypto_liquid_v2"
        ):
            continue
        result.setdefault(str(item["instrument_uid"]), []).append(deepcopy(item))
    for values in result.values():
        values.sort(key=lambda item: (str(item["feed"]), str(item.get("interval") or "")))
    return result


def _is_managed(item: Mapping[str, Any]) -> bool:
    feed = str(item.get("feed", ""))
    return (
        feed in _REALTIME_FEEDS
        or (feed in _REFERENCE_FEEDS and str(item.get("source_policy_id")) == "crypto_liquid_v2")
    )


def _materialize_manifest(
    manifest: Mapping[str, Any],
    *,
    realtime: list[dict[str, Any]],
    reference_templates: Mapping[str, list[dict[str, Any]]],
    target: Mapping[str, str],
    instruments: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], bool]:
    result = deepcopy(dict(manifest))
    existing = list(result["spec"]["requirements"])
    uids = _target_uids(result, target=target, instruments=instruments)
    references: list[dict[str, Any]] = []
    for uid in uids:
        try:
            rows = reference_templates[uid]
        except KeyError as error:
            raise ValueError(f"reference capability has no template for {uid}") from error
        for row in rows:
            value = deepcopy(row)
            value["consumer_grade"] = "ALPHA"
            references.append(value)
    base = [item for item in existing if not _is_managed(item)]
    requirements = [*base, *realtime, *references]
    result["spec"]["requirements"] = requirements
    result["spec"]["quotas"]["max_warmup_rows"] = 10_000
    changed = (
        requirements != existing
        or int(manifest["spec"]["quotas"]["max_warmup_rows"]) != 10_000
    )
    if changed:
        result["metadata"]["revision"] = int(result["metadata"]["revision"]) + 1
    return result, changed


def _default_route(
    requirement: Any,
    *,
    bindings: Mapping[tuple[str, str, str | None, str], Mapping[str, Any]],
) -> dict[str, Any]:
    feed = requirement.feed.value
    if feed in _REFERENCE_FEEDS:
        return dict(_REFERENCE_ROUTE)
    key = (
        requirement.instrument_uid,
        feed,
        requirement.interval,
        requirement.source_policy_id,
    )
    binding = bindings.get(key)
    if binding is None:
        raise ValueError(f"route source binding is missing: {key}")
    if feed == "TRADE" and str(binding.get("v1_compatibility", "NONE")) != "NONE":
        return {"route": "V2_PRIMARY", "fallback": "V1", "reason": None}
    return {
        "route": "V2_PRIMARY",
        "fallback": "BLOCKED",
        "reason": _BLOCKED_ROUTE_REASONS.get(feed, "V1_EQUIVALENCE_UNPROVEN"),
    }


def _materialize_route(
    route: Mapping[str, Any],
    *,
    manifests: Mapping[str, Mapping[str, Any]],
    bindings: Mapping[tuple[str, str, str | None, str], Mapping[str, Any]],
) -> tuple[dict[str, Any], bool]:
    result = deepcopy(dict(route))
    targets = {str(item["consumer_id"]): item for item in result["consumers"]}
    changed = False
    for consumer_id, payload in manifests.items():
        target = targets.get(consumer_id)
        if target is None:
            raise ValueError(f"stable release route has no consumer {consumer_id}")
        manifest = ConsumerManifestLoader.from_mapping(payload)
        current = {
            str(item["requirement_key"]): item for item in target["products"]
        }
        products = []
        for requirement in manifest.requirements:
            key = requirement_key(requirement)
            existing = current.get(key)
            products.append(
                {"requirement_key": key, **(deepcopy(existing) if existing is not None else _default_route(requirement, bindings=bindings))}
                if existing is None
                else deepcopy(existing)
            )
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
    catalog: Mapping[str, Any],
    demand: Mapping[str, Any],
    reference_manifest: Mapping[str, Any],
    alpha_manifests: Mapping[str, Mapping[str, Any]],
    release_route: Mapping[str, Any],
    primary_route: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, object]]:
    instruments, bindings = _catalog_indexes(catalog)
    reference_templates = _reference_templates(reference_manifest)
    manifests: dict[str, dict[str, Any]] = {}
    manifest_changed = False
    for consumer_id, target in _TARGETS.items():
        realtime = _realtime_templates(
            manifest=alpha_manifests[consumer_id],
            demand=demand,
            instruments=instruments,
            bindings=bindings,
            target=target,
        )
        manifest, changed = _materialize_manifest(
            alpha_manifests[consumer_id],
            realtime=realtime,
            reference_templates=reference_templates,
            target=target,
            instruments=instruments,
        )
        manifests[consumer_id] = manifest
        manifest_changed = manifest_changed or changed
    route, route_changed = _materialize_route(
        release_route,
        manifests=manifests,
        bindings=bindings,
    )
    primary = deepcopy(dict(primary_route))
    if manifest_changed:
        primary["revision"] = int(primary["revision"]) + 1
    summary = {
        "schema": "qdl.phase533.alpha-runtime-entitlements.v1",
        "status": "READY",
        "manifest_changed": manifest_changed,
        "release_route_changed": route_changed,
        "alpha_requirement_counts": {
            consumer_id: len(payload["spec"]["requirements"])
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=ROOT / "config/v2/stable-source-bindings.yaml")
    parser.add_argument("--demand", type=Path, default=ROOT / "config/v2/stable-crypto-demand.yaml")
    parser.add_argument("--reference-manifest", type=Path, default=ROOT / "consumers/stable/reference-l2-stable.yaml")
    parser.add_argument("--release-route", type=Path, default=ROOT / "config/v2/stable-v2-release-routing.yaml")
    parser.add_argument("--primary-route", type=Path, default=ROOT / "config/v2/stable-primary-consumer-routing.yaml")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    paths = {
        consumer_id: ROOT / "consumers/stable" / target["filename"]
        for consumer_id, target in _TARGETS.items()
    }
    manifests, route, primary, summary = build_documents(
        catalog=_load(args.catalog),
        demand=_load(args.demand),
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
