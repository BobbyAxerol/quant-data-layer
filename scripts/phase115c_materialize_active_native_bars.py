#!/usr/bin/env python3
"""Materialize active native BAR and execution-L2 demand for Binance/OKX.

This is the bounded Phase 11.5-C3.5 catalog compiler. It expands only the
already-active Binance USD-M and OKX Swap BAR requirements, materializes their
explicit execution-L2 requirements, regenerates stable source/acquisition
records from committed authentic metadata captures, and preserves non-crypto
V1/V2 records byte-for-byte at the mapping level.
It never talks to a provider or a runtime; the default is dry-run.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Any, Mapping

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.adapters.binance_spot import parse_spot_exchange_info
from qdl.adapters.intervals import (
    BINANCE_USDM_NATIVE_INTERVALS,
    OKX_NATIVE_INTERVALS,
    okx_candle_channel,
)
from qdl.runtime.production_catalog import (
    ProductionCatalogBuilder,
    ProductionDemandManifest,
    load_binance_exchange_info,
    load_okx_instruments,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import AuthorityPromotionScope, StableAcquisitionPlan


CRYPTO_VENUES = frozenset({"BINANCE", "OKX"})
ACTIVE_BAR_FAMILIES = {
    ("BINANCE", "USDM"): BINANCE_USDM_NATIVE_INTERVALS,
    ("OKX", "SWAP"): OKX_NATIVE_INTERVALS,
}


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _load_yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"YAML document must be an object: {path}")
    return value


def _yaml_bytes(value: Mapping[str, Any]) -> bytes:
    return yaml.safe_dump(dict(value), sort_keys=False, allow_unicode=False).encode("utf-8")


def _atomic_write(path: Path, value: Mapping[str, Any]) -> None:
    encoded = _yaml_bytes(value)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(encoded)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_if_bytes_differ(path: Path, value: Mapping[str, Any]) -> bool:
    """Keep release hashes bound to the exact canonical artifact bytes."""
    if path.read_bytes() == _yaml_bytes(value):
        return False
    _atomic_write(path, value)
    return True


def _requirement_key(value: Mapping[str, Any]) -> tuple[str, str, str, str, str, str | None]:
    return (
        str(value["venue"]).upper(),
        str(value["market"]).upper(),
        str(value["product_type"]).upper(),
        str(value["native_symbol"]).upper(),
        str(value["feed"]).upper(),
        None if value["interval"] is None else str(value["interval"]),
    )


def expand_demand(payload: Mapping[str, Any]) -> tuple[dict[str, Any], int]:
    """Return demand with every native active crypto BAR interval present.

    Disabled Spot records are deliberately excluded.  Adding a new active
    symbol still requires an explicit demand row; this compiler never turns an
    exchange-wide universe into an implicit runtime subscription.
    """
    result = deepcopy(dict(payload))
    if result.get("schema") != "qdl.v2.production-demand.v1":
        raise ValueError("Phase 11.5-C3.5 demand schema is invalid")
    consumers = result.get("consumers")
    if not isinstance(consumers, list) or not consumers:
        raise ValueError("Phase 11.5-C3.5 demand consumers are invalid")

    additions = 0
    for consumer in consumers:
        requirements = consumer.get("requirements") if isinstance(consumer, dict) else None
        if not isinstance(requirements, list):
            raise ValueError("Phase 11.5-C3.5 demand requirements are invalid")
        existing = {_requirement_key(item) for item in requirements if isinstance(item, dict)}
        generated: list[dict[str, Any]] = []
        for requirement in list(requirements):
            if not isinstance(requirement, dict) or str(requirement.get("feed", "")).upper() != "BAR":
                continue
            family = (str(requirement.get("venue", "")).upper(), str(requirement.get("market", "")).upper())
            intervals = ACTIVE_BAR_FAMILIES.get(family)
            if intervals is None:
                continue
            for interval in intervals:
                candidate = dict(requirement)
                candidate["interval"] = interval
                key = _requirement_key(candidate)
                if key not in existing:
                    generated.append(candidate)
                    existing.add(key)
        requirements.extend(generated)
        additions += len(generated)
    if additions:
        result["revision"] = int(result["revision"]) + 1
    return result, additions


def _load_temporary(value: Mapping[str, Any], name: str, loader):
    """Validate one generated document without leaving test files behind."""
    with tempfile.TemporaryDirectory(prefix="qdl-phase115c-demand-") as raw:
        path = Path(raw) / name
        path.write_bytes(_yaml_bytes(value))
        return loader(path)


def _manifest_from_payload(payload: Mapping[str, Any]) -> ProductionDemandManifest:
    return _load_temporary(
        payload,
        "demand.yaml",
        lambda path: ProductionDemandManifest.load_many([path]),
    )


def _crypto_generated_bundle(
    *,
    demand: Mapping[str, Any],
    current_catalog: StableSourceCatalog,
    revision_increment: int,
    binance_usdm_capture: Path,
    binance_spot_capture: Path,
    okx_swap_capture: Path,
    okx_spot_capture: Path,
):
    manifest = _manifest_from_payload(demand)
    spot_payload = json.loads(binance_spot_capture.read_text(encoding="utf-8"))
    builder = ProductionCatalogBuilder(
        catalog_revision=current_catalog.catalog_revision + revision_increment,
        source_policy_revision=current_catalog.source_policy_revision,
        authority_revision=current_catalog.authority_revision,
    )
    return builder.build(
        demand=manifest,
        binance_usdm=load_binance_exchange_info(binance_usdm_capture),
        binance_spot=parse_spot_exchange_info(spot_payload, valid_from_ns=0),
        okx_rows=(
            *load_okx_instruments(okx_swap_capture),
            *load_okx_instruments(okx_spot_capture),
        ),
        previous_catalog=current_catalog,
        metadata_provenance={
            "binance_usdm_exchange_info_sha256": _sha256_bytes(binance_usdm_capture.read_bytes()),
            "binance_spot_exchange_info_sha256": _sha256_bytes(binance_spot_capture.read_bytes()),
            "okx_swap_instruments_sha256": _sha256_bytes(okx_swap_capture.read_bytes()),
            "okx_spot_instruments_sha256": _sha256_bytes(okx_spot_capture.read_bytes()),
        },
    )


def _source_requirement_key(item: Mapping[str, Any]) -> tuple[str, str, str | None]:
    return (
        str(item["instrument_uid"]),
        str(item["feed"]),
        None if item.get("interval") is None else str(item["interval"]),
    )


def _merge_source_bindings(
    *,
    existing: list[dict[str, Any]],
    generated: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    existing_by_requirement = {
        _source_requirement_key(item): item for item in existing
    }
    generated_requirements = {_source_requirement_key(item) for item in generated}
    remap: dict[str, str] = {}
    result: list[dict[str, Any]] = []
    # Preserve all old mappings for an unchanged ID.  In particular, an active
    # source_id is cursor identity and must never change merely because a new
    # interval is added beside it.  Old catalogs used a different OKX spelling
    # for the same requirement, so identity (instrument/feed/interval), not
    # the cosmetic binding ID, is the matching key.
    for item in generated:
        current = existing_by_requirement.get(_source_requirement_key(item))
        chosen = deepcopy(current if current is not None else item)
        result.append(chosen)
        remap[str(item["binding_id"])] = str(chosen["binding_id"])
    result.extend(
        deepcopy(item)
        for item in existing
        if _source_requirement_key(item) not in generated_requirements
    )
    ids = [str(item["binding_id"]) for item in result]
    if len(ids) != len(set(ids)):
        raise ValueError("Phase 11.5-C3.5 merged binding IDs are not unique")
    return sorted(result, key=lambda item: str(item["binding_id"])), remap


def _merge_acquisition_bindings(
    *,
    existing: list[dict[str, Any]],
    generated: list[dict[str, Any]],
    source_binding_ids: set[str],
    remap: Mapping[str, str],
) -> list[dict[str, Any]]:
    existing_by_id = {str(item["binding_id"]): item for item in existing}
    result: dict[str, dict[str, Any]] = {}
    for item in generated:
        target_id = remap[str(item["binding_id"])]
        current = existing_by_id.get(target_id)
        # Acquisition policy is not cursor identity. Preserve every existing
        # policy by default, but refresh the one venue-wide final-BAR policy
        # whose real-provider admission changed. This keeps source IDs and all
        # unrelated active lanes stable while making a sealed manifest reflect
        # the catalog's admitted OKX native final-BAR path.
        refresh_okx_bar_policy = (
            target_id.startswith("okx-swap-")
            and
            str(item.get("runtime")) == "OKX"
            and str(item.get("provider_kind")) == "okx_bar"
        )
        chosen = deepcopy(item if refresh_okx_bar_policy or current is None else current)
        chosen["binding_id"] = target_id
        result[target_id] = chosen
    for binding_id in source_binding_ids:
        if binding_id not in result:
            try:
                result[binding_id] = deepcopy(existing_by_id[binding_id])
            except KeyError as error:
                raise ValueError(
                    "Phase 11.5-C3.5 source binding has no acquisition: " + binding_id
                ) from error
    if set(result) != source_binding_ids:
        raise ValueError("Phase 11.5-C3.5 acquisition/source coverage differs")
    return sorted(result.values(), key=lambda item: str(item["binding_id"]))


def build_documents(
    *,
    demand: Mapping[str, Any],
    source_catalog: Mapping[str, Any],
    acquisition: Mapping[str, Any],
    promotion_scope: Mapping[str, Any],
    binance_usdm_capture: Path,
    binance_spot_capture: Path,
    okx_swap_capture: Path,
    okx_spot_capture: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    expanded_demand, additions = expand_demand(demand)
    current_catalog = _load_temporary(
        source_catalog, "catalog.yaml", StableSourceCatalog.load
    )
    current_acquisition = _load_temporary(
        acquisition,
        "acquisition.yaml",
        lambda path: StableAcquisitionPlan.load(path, catalog=current_catalog),
    )
    provisional_bundle = _crypto_generated_bundle(
        demand=expanded_demand,
        current_catalog=current_catalog,
        revision_increment=0,
        binance_usdm_capture=binance_usdm_capture,
        binance_spot_capture=binance_spot_capture,
        okx_swap_capture=okx_swap_capture,
        okx_spot_capture=okx_spot_capture,
    )

    existing_requirement_keys = {
        _source_requirement_key(item) for item in source_catalog["bindings"]
    }
    generated_requirement_keys = {
        _source_requirement_key(item) for item in provisional_bundle.source_catalog["bindings"]
    }
    revision_increment = int(bool(additions or (generated_requirement_keys - existing_requirement_keys)))
    bundle = provisional_bundle if revision_increment == 0 else _crypto_generated_bundle(
        demand=expanded_demand,
        current_catalog=current_catalog,
        revision_increment=revision_increment,
        binance_usdm_capture=binance_usdm_capture,
        binance_spot_capture=binance_spot_capture,
        okx_swap_capture=okx_swap_capture,
        okx_spot_capture=okx_spot_capture,
    )
    generated_source = bundle.source_catalog
    generated_acquisition = bundle.acquisition_plan
    current_instrument_ids = {str(item["instrument_id"]) for item in source_catalog["instruments"]}
    generated_instrument_ids = {str(item["instrument_id"]) for item in generated_source["instruments"]}
    if not generated_instrument_ids.issubset(current_instrument_ids):
        raise ValueError("Phase 11.5-C3.5 generated an undeclared instrument")

    source_result = deepcopy(dict(source_catalog))
    source_result["catalog_revision"] = current_catalog.catalog_revision + revision_increment
    source_result["bindings"], remap = _merge_source_bindings(
        existing=list(source_catalog["bindings"]),
        generated=list(generated_source["bindings"]),
    )

    merged_acquisition = _merge_acquisition_bindings(
        existing=list(acquisition["bindings"]),
        generated=list(generated_acquisition["bindings"]),
        source_binding_ids={str(item["binding_id"]) for item in source_result["bindings"]},
        remap=remap,
    )
    acquisition_policy_changed = {
        str(item["binding_id"]): item for item in merged_acquisition
    } != {
        str(item["binding_id"]): item for item in acquisition["bindings"]
    }
    acquisition_result = deepcopy(dict(acquisition))
    acquisition_result["revision"] = current_acquisition.revision + int(
        bool(revision_increment or acquisition_policy_changed)
    )
    acquisition_result["bindings"] = merged_acquisition

    next_catalog = _load_temporary(
        source_result, "next-catalog.yaml", StableSourceCatalog.load
    )
    _load_temporary(
        acquisition_result,
        "next-acquisition.yaml",
        lambda path: StableAcquisitionPlan.load(path, catalog=next_catalog),
    )

    promotion_result = deepcopy(dict(promotion_scope))
    active_authority_ids = {
        item.binding_id
        for item in next_catalog.bindings
        if item.feed.value in {"BAR", "TRADE", "QUOTE", "BOOK_SNAPSHOT", "BOOK_DELTA"}
        and (item.instrument.identity.venue, item.instrument.identity.market) in ACTIVE_BAR_FAMILIES
    }
    next_scope_ids = sorted(
        set(str(item) for item in promotion_scope["binding_ids"]) | active_authority_ids
    )
    if additions or next_scope_ids != list(promotion_scope["binding_ids"]):
        promotion_result["revision"] = int(promotion_scope["revision"]) + 1
    promotion_result["binding_ids"] = next_scope_ids
    _load_temporary(
        promotion_result,
        "next-scope.yaml",
        lambda path: AuthorityPromotionScope.load(path, catalog=next_catalog),
    )

    summary = {
        "schema": "qdl.phase115c.native-bar-materialization.v1",
        "demand_additions": additions,
        "demand_revision": int(expanded_demand["revision"]),
        "catalog_revision": next_catalog.catalog_revision,
        "acquisition_revision": int(acquisition_result["revision"]),
        "acquisition_policy_changed": acquisition_policy_changed,
        "promotion_scope_revision": int(promotion_result["revision"]),
        "bar_binding_counts": {
            "binance_usdm": sum(
                1 for item in next_catalog.bindings
                if item.feed.value == "BAR"
                and (item.instrument.identity.venue, item.instrument.identity.market) == ("BINANCE", "USDM")
            ),
            "okx_swap": sum(
                1 for item in next_catalog.bindings
                if item.feed.value == "BAR"
                and (item.instrument.identity.venue, item.instrument.identity.market) == ("OKX", "SWAP")
            ),
            "dnse": sum(
                1 for item in next_catalog.bindings
                if item.feed.value == "BAR" and item.instrument.identity.venue in {"HNX", "HOSE"}
            ),
        },
        "price_binding_counts": {
            "binance_usdm": sum(
                1 for item in next_catalog.bindings
                if item.feed.value in {"BAR", "TRADE", "QUOTE"}
                and (item.instrument.identity.venue, item.instrument.identity.market) == ("BINANCE", "USDM")
            ),
            "okx_swap": sum(
                1 for item in next_catalog.bindings
                if item.feed.value in {"BAR", "TRADE", "QUOTE"}
                and (item.instrument.identity.venue, item.instrument.identity.market) == ("OKX", "SWAP")
            ),
        },
        "source_catalog_sha256": _sha256_bytes(_yaml_bytes(source_result)),
        "acquisition_sha256": _sha256_bytes(_yaml_bytes(acquisition_result)),
        "demand_sha256": _sha256_bytes(_yaml_bytes(expanded_demand)),
        "promotion_scope_sha256": _sha256_bytes(_yaml_bytes(promotion_result)),
    }
    return expanded_demand, source_result, acquisition_result, promotion_result, summary


def _update_release_route(
    route: Mapping[str, Any],
    *,
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    result = deepcopy(dict(route))
    referenced_artifact_changed = any(
        (
            str(result[section]["sha256"]) != str(summary[summary_sha])
            or int(result[section]["revision"]) != int(summary[summary_revision])
        )
        for section, summary_sha, summary_revision in (
            ("source_catalog", "source_catalog_sha256", "catalog_revision"),
            ("crypto_demand", "demand_sha256", "demand_revision"),
        )
    )
    if referenced_artifact_changed:
        result["revision"] = int(result["revision"]) + 1
    result["source_catalog"]["sha256"] = summary["source_catalog_sha256"]
    result["source_catalog"]["revision"] = summary["catalog_revision"]
    result["crypto_demand"]["sha256"] = summary["demand_sha256"]
    result["crypto_demand"]["revision"] = summary["demand_revision"]
    for consumer in result["consumers"]:
        consumer["demand_revision"] = summary["demand_revision"]
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demand", type=Path, default=ROOT / "config/v2/stable-crypto-demand.yaml")
    parser.add_argument("--catalog", type=Path, default=ROOT / "config/v2/stable-source-bindings.yaml")
    parser.add_argument("--acquisition", type=Path, default=ROOT / "config/v2/stable-acquisition-bindings.yaml")
    parser.add_argument("--promotion-scope", type=Path, default=ROOT / "config/v2/stable-authority-promotion-scope.yaml")
    parser.add_argument("--release-routing", type=Path, default=ROOT / "config/v2/stable-v2-release-routing.yaml")
    parser.add_argument("--binance-usdm-capture", type=Path, default=ROOT / "config/v2/captures/binance-usdm-exchangeinfo.filtered.json")
    parser.add_argument("--binance-spot-capture", type=Path, default=ROOT / "config/v2/captures/binance-spot-exchangeinfo.filtered.json")
    parser.add_argument("--okx-swap-capture", type=Path, default=ROOT / "config/v2/captures/okx-instruments-swap.filtered.json")
    parser.add_argument("--okx-spot-capture", type=Path, default=ROOT / "config/v2/captures/okx-instruments-spot.filtered.json")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    demand, catalog, acquisition, scope, summary = build_documents(
        demand=_load_yaml(args.demand),
        source_catalog=_load_yaml(args.catalog),
        acquisition=_load_yaml(args.acquisition),
        promotion_scope=_load_yaml(args.promotion_scope),
        binance_usdm_capture=args.binance_usdm_capture,
        binance_spot_capture=args.binance_spot_capture,
        okx_swap_capture=args.okx_swap_capture,
        okx_spot_capture=args.okx_spot_capture,
    )
    route = _update_release_route(_load_yaml(args.release_routing), summary=summary)
    changed_files: list[str] = []
    if args.apply:
        for path, value in (
            (args.demand, demand),
            (args.catalog, catalog),
            (args.acquisition, acquisition),
            (args.promotion_scope, scope),
            (args.release_routing, route),
        ):
            if _write_if_bytes_differ(path, value):
                changed_files.append(str(path))
    print(json.dumps({
        **summary,
        "changed_files": changed_files,
        "status": "APPLIED" if args.apply else "DRY_RUN",
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
