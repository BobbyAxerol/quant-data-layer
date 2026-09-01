#!/usr/bin/env python3
"""Materialize one bounded final-BAR projection from sealed V2 consumers.

The stable catalog intentionally carries more venue/interval capability than a
declared consumer set needs. This control-plane tool derives a minimal catalog
and acquisition plan from sealed V2 consumer-route bindings plus, when
supplied, the exact route set already materialized for a retained active
consumer. The shared BAR edge cannot acquire an undeclared interval merely
because it exists in the image, and a new consumer union cannot silently remove
an active baseline route. It never contacts a provider or changes a running
role.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any, Mapping

import yaml


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.consumer.universal_release import ConsumerRouteBinding
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


CONFIRMATION = "MATERIALIZE_PHASE12_BOUND_BAR_EDGE"


@dataclass(frozen=True, slots=True)
class BoundBarProjection:
    catalog: dict[str, Any]
    acquisition: dict[str, Any]
    summary: dict[str, Any]


def _load_mapping(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"{label} is missing: {path}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a YAML mapping: {path}")
    return value


def _load_binding(path: Path) -> ConsumerRouteBinding:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"consumer route binding is missing: {path}") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"consumer route binding is not JSON: {path}") from error
    if not isinstance(value, Mapping):
        raise ValueError("consumer route binding must be a JSON object")
    return ConsumerRouteBinding.from_canonical_mapping(value)


def _load_retained_projection(path: Path) -> tuple[dict[str, Any], str]:
    """Load an active projection solely as a retained-route authority."""

    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except FileNotFoundError as error:
        raise ValueError(f"retained BAR projection is missing: {path}") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"retained BAR projection is not JSON: {path}") from error
    if not isinstance(value, dict):
        raise ValueError("retained BAR projection must be a JSON object")
    return value, _sha256_bytes(raw)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _yaml_bytes(value: Mapping[str, Any]) -> bytes:
    return yaml.safe_dump(dict(value), sort_keys=False, allow_unicode=False).encode("utf-8")


def _bar_key_from_product(product: object) -> tuple[str, ...]:
    if getattr(product, "feed") != "BAR":
        raise ValueError("bound BAR projection received a non-BAR product")
    if (
        getattr(product, "route") != "V2_PRIMARY"
        or not getattr(product, "require_final_bars")
        or getattr(product, "provider_plane") != "REALTIME"
        or not getattr(product, "interval")
    ):
        raise ValueError("bound BAR projection requires a live final V2 BAR product")
    return (
        str(getattr(product, "venue")),
        str(getattr(product, "market")),
        str(getattr(product, "product_type")),
        str(getattr(product, "native_symbol")),
        str(getattr(product, "instrument_uid")),
        str(getattr(product, "instrument_id")),
        str(getattr(product, "source_policy_id")),
        str(getattr(product, "interval")),
    )


def _bar_key_from_source(source: object) -> tuple[str, ...]:
    identity = source.instrument.identity
    return (
        identity.venue,
        identity.market,
        identity.product_type.value,
        source.instrument.native_symbol,
        source.instrument.instrument_uid,
        source.instrument.instrument_id,
        source.source_policy_id,
        source.interval or "",
    )


def _retained_bar_binding_ids(projection: Mapping[str, Any] | None) -> tuple[str, ...]:
    """Return only a verified active final-BAR baseline route set."""

    if projection is None:
        return ()
    if projection.get("schema") != "qdl.phase12.bound-bar-edge-projection.v1":
        raise ValueError("retained BAR projection schema is invalid")
    if projection.get("status") != "MATERIALIZED":
        raise ValueError("retained BAR projection is not materialized")
    if not isinstance(projection.get("consumer_id"), str):
        raise ValueError("retained BAR projection consumer_id is invalid")
    for field in ("catalog_sha256", "acquisition_sha256"):
        value = projection.get(field)
        if not isinstance(value, str) or len(value) != 64:
            raise ValueError(f"retained BAR projection {field} is invalid")
    raw_ids = projection.get("binding_ids")
    if not isinstance(raw_ids, list) or not raw_ids or any(
        not isinstance(item, str) or not item for item in raw_ids
    ):
        raise ValueError("retained BAR projection binding_ids are invalid")
    return tuple(sorted(set(raw_ids)))


def _validate_projection(
    *,
    catalog: Mapping[str, Any],
    acquisition: Mapping[str, Any],
) -> None:
    """Run both strict runtime loaders against generated documents."""

    with tempfile.TemporaryDirectory(prefix="qdl-phase12-bound-bars-") as raw:
        root = Path(raw)
        catalog_path = root / "catalog.yaml"
        acquisition_path = root / "acquisition.yaml"
        catalog_path.write_bytes(_yaml_bytes(catalog))
        acquisition_path.write_bytes(_yaml_bytes(acquisition))
        parsed_catalog = StableSourceCatalog.load(catalog_path)
        parsed_acquisition = StableAcquisitionPlan.load(
            acquisition_path, catalog=parsed_catalog
        )
        if {item.binding_id for item in parsed_catalog.bindings} != {
            item.binding_id for item in parsed_acquisition.bindings
        }:
            raise ValueError("generated BAR projection coverage differs")


def _project_acquisition_for_python_bar_edge(
    *,
    source: object,
    acquisition: Mapping[str, Any],
) -> dict[str, Any]:
    """Keep the bounded recovery edge as the explicit final-BAR owner.

    The broad acquisition plan may describe an OKX final BAR as ``RUST_NATIVE``
    for the shared native plane.  The existing bounded Python edge is still the
    certified recovery/finality owner for its sealed consumer projection.  It
    must therefore project only that exact OKX final-BAR lane to REST rather
    than silently accepting arbitrary native feeds or venues.
    """

    result = deepcopy(dict(acquisition))
    mode = str(result.get("mode", ""))
    if mode == "PYTHON_REST":
        return result

    identity = getattr(getattr(source, "instrument", None), "identity", None)
    if (
        mode == "RUST_NATIVE"
        and getattr(source, "feed", None).value == "BAR"
        and bool(getattr(source, "require_final_bar", False))
        and getattr(identity, "venue", None) == "OKX"
        and getattr(identity, "market", None) == "SWAP"
        and str(result.get("provider_kind")) == "okx_bar"
        and str(result.get("native_channel", "")).startswith("candle")
    ):
        result["mode"] = "PYTHON_REST"
        result["websocket_url"] = None
        result["business_websocket_url"] = None
        return result

    raise ValueError(
        "bound Python BAR edge only permits PYTHON_REST or declared OKX final BAR recovery"
    )


def build_bound_bar_projection_set(
    *,
    bindings: tuple[ConsumerRouteBinding, ...],
    catalog_document: Mapping[str, Any],
    acquisition_document: Mapping[str, Any],
    retained_projection: Mapping[str, Any] | None = None,
    retained_projection_sha256: str | None = None,
) -> BoundBarProjection:
    """Return the exact final-BAR subset for a declared sealed consumer set.

    This is intentionally derived by canonical identity rather than source
    binding ID: a source catalog may change an implementation identifier, but
    it may never silently change venue, contract, symbol, policy or interval.
    """
    if not bindings:
        raise ValueError("bound BAR projection requires at least one consumer binding")

    full_catalog = StableSourceCatalog.from_mapping(catalog_document)
    full_catalog_ids = {item.binding_id for item in full_catalog.bindings}
    source_by_binding_id = {item.binding_id: item for item in full_catalog.bindings}
    source_rows = catalog_document.get("bindings")
    acquisition_rows = acquisition_document.get("bindings")
    instruments = catalog_document.get("instruments")
    if (
        not isinstance(source_rows, list)
        or not isinstance(acquisition_rows, list)
        or not isinstance(instruments, list)
    ):
        raise ValueError("stable source/acquisition documents are incomplete")

    requested = tuple(
        _bar_key_from_product(product)
        for binding in bindings
        for product in binding.products
        if product.feed == "BAR"
    )
    if not requested:
        raise ValueError("consumer route bindings have no BAR product")
    requested_unique = tuple(sorted(set(requested)))

    by_key: dict[tuple[str, ...], list[object]] = {}
    for source in full_catalog.bindings:
        if source.feed.value != "BAR":
            continue
        by_key.setdefault(_bar_key_from_source(source), []).append(source)

    selected_ids: list[str] = []
    for key in requested_unique:
        matches = by_key.get(key, [])
        if len(matches) != 1:
            raise ValueError(
                "sealed BAR route does not resolve to exactly one catalog binding: "
                + "/".join(key)
            )
        source = matches[0]
        if not source.require_final_bar:
            raise ValueError("sealed BAR route resolved to a non-final catalog binding")
        selected_ids.append(source.binding_id)

    retained_ids = _retained_bar_binding_ids(retained_projection)
    for binding_id in retained_ids:
        source = source_by_binding_id.get(binding_id)
        if source is None:
            raise ValueError(
                "retained BAR route no longer resolves to a catalog binding: " + binding_id
            )
        if source.feed.value != "BAR" or not source.require_final_bar:
            raise ValueError(
                "retained BAR route is not a final catalog BAR binding: " + binding_id
            )

    selected = frozenset((*selected_ids, *retained_ids))
    if (
        len(set(selected_ids)) != len(requested_unique)
        or not selected
        or not selected <= full_catalog_ids
    ):
        raise ValueError("sealed BAR selection is invalid")
    source_raw_by_id = {
        str(item.get("binding_id")): item
        for item in source_rows
        if isinstance(item, Mapping)
    }
    acquisition_raw_by_id = {
        str(item.get("binding_id")): item
        for item in acquisition_rows
        if isinstance(item, Mapping)
    }
    if set(source_raw_by_id) != full_catalog_ids:
        raise ValueError("stable source document and strict catalog differ")
    if set(acquisition_raw_by_id) != full_catalog_ids:
        raise ValueError("stable acquisition document and strict catalog differ")

    selected_source = [deepcopy(source_raw_by_id[item]) for item in sorted(selected)]
    selected_acquisition = [
        _project_acquisition_for_python_bar_edge(
            source=source_by_binding_id[item],
            acquisition=acquisition_raw_by_id[item],
        )
        for item in sorted(selected)
    ]

    selected_uids = {str(item.get("instrument_uid")) for item in selected_source}
    selected_instruments = [
        deepcopy(item)
        for item in instruments
        if isinstance(item, Mapping) and str(item.get("instrument_uid")) in selected_uids
    ]
    if {str(item.get("instrument_uid")) for item in selected_instruments} != selected_uids:
        raise ValueError("sealed BAR projection is missing an instrument declaration")

    catalog = {
        key: deepcopy(catalog_document[key])
        for key in (
            "schema", "canonical_stream", "catalog_revision",
            "source_policy_revision", "authority_revision",
        )
    }
    catalog["instruments"] = selected_instruments
    catalog["bindings"] = selected_source
    acquisition = {
        key: deepcopy(acquisition_document[key])
        for key in ("schema", "revision", "topics")
    }
    acquisition["bindings"] = selected_acquisition
    _validate_projection(catalog=catalog, acquisition=acquisition)

    consumer_ids = tuple(sorted({binding.consumer_id for binding in bindings}))
    binding_sha256s = tuple(sorted({binding.binding_sha256 for binding in bindings}))
    summary: dict[str, Any] = {
        "schema": "qdl.phase12.bound-bar-edge-projection.v1",
        "status": "MATERIALIZED",
        "consumer_ids": list(consumer_ids),
        "binding_sha256s": list(binding_sha256s),
        "bar_route_count": len(selected),
        "binding_ids": sorted(selected),
        "catalog_sha256": _sha256_bytes(_yaml_bytes(catalog)),
        "acquisition_sha256": _sha256_bytes(_yaml_bytes(acquisition)),
        "runtime_mutations": 0,
        "provider_requests": 0,
        "order_actions": 0,
    }
    if retained_projection is not None:
        summary["retained_projection"] = {
            "consumer_id": retained_projection["consumer_id"],
            "binding_ids": list(retained_ids),
            "sha256": retained_projection_sha256,
        }
    if len(bindings) == 1:
        summary["consumer_id"] = bindings[0].consumer_id
        summary["binding_sha256"] = bindings[0].binding_sha256
    return BoundBarProjection(catalog=catalog, acquisition=acquisition, summary=summary)


def build_bound_bar_projection(
    *,
    binding: ConsumerRouteBinding,
    catalog_document: Mapping[str, Any],
    acquisition_document: Mapping[str, Any],
) -> BoundBarProjection:
    """Backward-compatible single-binding form of the union compiler."""

    return build_bound_bar_projection_set(
        bindings=(binding,),
        catalog_document=catalog_document,
        acquisition_document=acquisition_document,
    )


def _write_projection(*, output_dir: Path, projection: BoundBarProjection) -> None:
    if output_dir.exists():
        raise ValueError(f"output directory already exists: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        for name, payload in (
            ("catalog.yaml", _yaml_bytes(projection.catalog)),
            ("acquisition.yaml", _yaml_bytes(projection.acquisition)),
            ("projection.json", json.dumps(projection.summary, indent=2, sort_keys=True).encode() + b"\n"),
        ):
            target = stage / name
            target.write_bytes(payload)
            os.chmod(target, 0o644)
        os.chmod(stage, 0o755)
        os.replace(stage, output_dir)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--consumer-binding",
        type=Path,
        required=True,
        action="append",
        help="sealed V2 consumer binding; repeat to project their exact BAR union",
    )
    parser.add_argument(
        "--retain-projection",
        type=Path,
        help="active materialized BAR projection whose exact baseline routes must remain",
    )
    parser.add_argument(
        "--catalog", type=Path, default=ROOT / "config/v2/stable-source-bindings.yaml"
    )
    parser.add_argument(
        "--acquisition", type=Path, default=ROOT / "config/v2/stable-acquisition-bindings.yaml"
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args(argv)
    if args.apply and args.confirm != CONFIRMATION:
        raise SystemExit(f"--apply requires --confirm {CONFIRMATION}")
    if args.output_dir.resolve() in {
        *[path.resolve() for path in args.consumer_binding],
        *(
            (args.retain_projection.resolve(),)
            if args.retain_projection is not None
            else ()
        ),
        args.catalog.resolve(),
        args.acquisition.resolve(),
    }:
        raise SystemExit("output directory must differ from every input file")

    retained_projection = None
    retained_projection_sha256 = None
    if args.retain_projection is not None:
        retained_projection, retained_projection_sha256 = _load_retained_projection(
            args.retain_projection
        )

    projection = build_bound_bar_projection_set(
        bindings=tuple(_load_binding(path) for path in args.consumer_binding),
        catalog_document=_load_mapping(args.catalog, label="stable source catalog"),
        acquisition_document=_load_mapping(args.acquisition, label="stable acquisition"),
        retained_projection=retained_projection,
        retained_projection_sha256=retained_projection_sha256,
    )
    if args.apply:
        _write_projection(output_dir=args.output_dir, projection=projection)
    print(json.dumps({
        **projection.summary,
        "output_dir": str(args.output_dir),
        "status": "APPLIED" if args.apply else "DRY_RUN",
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
