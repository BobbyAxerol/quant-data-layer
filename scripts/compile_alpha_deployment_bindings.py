"""Compile secret-free V2 route bindings from alpha deployment configuration.

The Execution Alpha repository owns the portable
``execution-alpha.data-requirements.v1`` inventory.  This compiler resolves
that inventory against the Data Layer's existing catalog and reference/L2
entitlements, then emits the *existing* ``qdl.v2.consumer-route-binding.v1``
contract which Trading System and alpha SDK already parse.

It is control-plane/source tooling only: it never opens a provider connection,
starts a subscriber, writes a runtime bundle, or changes an existing consumer.
An unsupported or mismatched deployment is represented by one typed BLOCKED
result.  The compiler never drops one required route and returns a partial
binding.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping

import yaml

from qdl.consumer.universal_release import ConsumerRouteBinding


ROOT = Path(__file__).resolve().parents[1]
INVENTORY_SCHEMA = "execution-alpha.data-requirements.v1"
REPORT_SCHEMA = "qdl.v2.alpha-deployment-binding-compilation.v1"
CONTRACT_VERSION = "2.0.0"
_V2_VENUES = frozenset({"BINANCE", "OKX"})
_REFERENCE_FEEDS = frozenset(
    {
        "FUNDING_RATE",
        "OPEN_INTEREST",
        "LONG_SHORT_RATIO",
        "TAKER_FLOW",
        "MARK_INDEX_PRICE",
        "CONTRACT_METADATA",
        "BASIS",
    }
)
_METRIC_INTERVAL_FEEDS = frozenset({"OPEN_INTEREST", "LONG_SHORT_RATIO", "TAKER_FLOW", "BASIS"})
_PROFILE_CLASSES = {
    "directional_bar": "SINGLE_SYMBOL_ALPHA",
    "multi_symbol_bar": "PORTFOLIO_MULTI_SYMBOL",
    "bracket_context": "SINGLE_SYMBOL_ALPHA",
    "grid_l2": "GRID_REACTIVE_BRACKET",
    "basis_reference": "BASIS_ARB",
}
_SAFE_FILE_COMPONENT = re.compile(r"[^a-zA-Z0-9_.-]+")


class DeploymentBindingError(ValueError):
    """Raised when an inventory or compiler source artifact is malformed."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _text(value: object, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise DeploymentBindingError(f"{field} is required")
    return result


def _sha256(value: object, field: str) -> str:
    result = _text(value, field).lower()
    if len(result) != 64 or any(character not in "0123456789abcdef" for character in result):
        raise DeploymentBindingError(f"{field} must be a SHA-256 digest")
    return result


def _positive_int(value: object, field: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise DeploymentBindingError(f"{field} must be a positive integer")
    if maximum is not None and value > maximum:
        raise DeploymentBindingError(f"{field} must not exceed {maximum}")
    return value


def _bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise DeploymentBindingError(f"{field} must be boolean")
    return value


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DeploymentBindingError(f"{field} must be a mapping")
    return value


def _load_mapping(path: Path) -> dict[str, Any]:
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise DeploymentBindingError(f"source file is missing: {path}") from error
    if not isinstance(value, dict):
        raise DeploymentBindingError(f"{path} must be a mapping")
    return value


def _load_json_mapping(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise DeploymentBindingError(f"inventory is missing: {path}") from error
    except json.JSONDecodeError as error:
        raise DeploymentBindingError("inventory is not valid JSON") from error
    if not isinstance(value, dict):
        raise DeploymentBindingError("inventory must be a JSON object")
    return value


def _route_key(
    *,
    instrument_uid: str,
    feed: str,
    interval: str | None,
    source_policy_id: str,
) -> tuple[str, str, str | None, str]:
    return (
        instrument_uid,
        feed.upper(),
        interval.lower() if interval is not None else None,
        source_policy_id,
    )


def _inventory_digest(inventory: Mapping[str, Any]) -> str:
    raw = dict(inventory)
    reported = _sha256(raw.pop("inventory_sha256", None), "inventory_sha256")
    if _digest(raw) != reported:
        raise DeploymentBindingError("inventory checksum differs")
    return reported


def _validate_inventory(inventory: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    expected = {
        "schema", "revision", "registry_path", "registry_sha256", "deployments", "inventory_sha256"
    }
    if set(inventory) != expected or inventory.get("schema") != INVENTORY_SCHEMA:
        raise DeploymentBindingError("inventory schema or fields are invalid")
    _positive_int(inventory.get("revision"), "inventory revision")
    _sha256(inventory.get("registry_sha256"), "inventory registry_sha256")
    _inventory_digest(inventory)
    deployments = inventory.get("deployments")
    if not isinstance(deployments, list) or not deployments:
        raise DeploymentBindingError("inventory deployments are required")
    result: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for item in deployments:
        deployment = _mapping(item, "inventory deployment")
        deployment_id = _text(deployment.get("deployment_id"), "deployment_id")
        if deployment_id in seen:
            raise DeploymentBindingError("inventory deployment_id is duplicated")
        seen.add(deployment_id)
        status = _text(deployment.get("status"), "deployment status")
        if status not in {"DECLARED", "DECLARED_NO_ORDER_PROBE", "BLOCKED"}:
            raise DeploymentBindingError("inventory deployment status is invalid")
        result.append(deployment)
    return tuple(sorted(result, key=lambda item: str(item["deployment_id"])))


def _catalog_indexes(
    catalog: Mapping[str, Any],
) -> tuple[
    dict[tuple[str, str, str, str], Mapping[str, Any]],
    dict[tuple[str, str, str | None, str], Mapping[str, Any]],
]:
    if catalog.get("schema") != "qdl.v2.stable-source-bindings.v1":
        raise DeploymentBindingError("stable source catalog schema is unsupported")
    records: dict[tuple[str, str, str, str], Mapping[str, Any]] = {}
    by_uid: dict[str, Mapping[str, Any]] = {}
    for item in catalog.get("instruments", []):
        instrument = _mapping(item, "catalog instrument")
        identity = tuple(
            _text(instrument.get(field), f"catalog instrument {field}").upper()
            for field in ("venue", "market", "product_type", "native_symbol")
        )
        uid = _text(instrument.get("instrument_uid"), "catalog instrument_uid")
        if identity in records or uid in by_uid:
            raise DeploymentBindingError("catalog instrument identity is duplicated")
        records[identity] = instrument
        by_uid[uid] = instrument
    bindings: dict[tuple[str, str, str | None, str], Mapping[str, Any]] = {}
    for item in catalog.get("bindings", []):
        binding = _mapping(item, "catalog binding")
        source = _mapping(binding.get("source"), "catalog binding source")
        key = _route_key(
            instrument_uid=_text(binding.get("instrument_uid"), "catalog binding instrument_uid"),
            feed=_text(binding.get("feed"), "catalog binding feed"),
            interval=(str(binding["interval"]) if binding.get("interval") is not None else None),
            source_policy_id=_text(source.get("source_policy_id"), "catalog source policy"),
        )
        if key in bindings:
            raise DeploymentBindingError("catalog binding identity is duplicated")
        if key[0] not in by_uid:
            raise DeploymentBindingError("catalog binding references undeclared instrument")
        bindings[key] = binding
    return records, bindings


def _reference_keys(reference_manifest: Mapping[str, Any]) -> set[tuple[str, str, str | None, str]]:
    spec = _mapping(reference_manifest.get("spec"), "reference manifest spec")
    requirements = spec.get("requirements")
    if not isinstance(requirements, list):
        raise DeploymentBindingError("reference manifest requirements are invalid")
    keys: set[tuple[str, str, str | None, str]] = set()
    for item in requirements:
        row = _mapping(item, "reference requirement")
        keys.add(_route_key(
            instrument_uid=_text(row.get("instrument_uid"), "reference instrument_uid"),
            feed=_text(row.get("feed"), "reference feed"),
            interval=(str(row["interval"]) if row.get("interval") is not None else None),
            source_policy_id=_text(row.get("source_policy_id"), "reference source policy"),
        ))
    return keys


def _consumer_id(deployment: Mapping[str, Any]) -> str:
    alpha_id = _text(deployment.get("alpha_id"), "deployment alpha_id")
    status = _text(deployment.get("status"), "deployment status")
    suffix = "okx.no-order" if status == "DECLARED_NO_ORDER_PROBE" else "binance.paper"
    return f"alpha.{alpha_id}.{suffix}"


def _consumer_class(deployment: Mapping[str, Any]) -> str:
    profile = _text(deployment.get("profile"), "deployment profile")
    try:
        return _PROFILE_CLASSES[profile]
    except KeyError as error:
        raise DeploymentBindingError(f"deployment profile is not classified: {profile}") from error


def _route_product(
    *,
    deployment: Mapping[str, Any],
    route: Mapping[str, Any],
    instrument: Mapping[str, Any],
    consumer_id: str,
    consumer_class: str,
) -> dict[str, Any]:
    feed = _text(route.get("feed"), "route feed").upper()
    interval = str(route["interval"]).strip().lower() if route.get("interval") is not None else None
    if feed == "BAR" and not interval:
        raise DeploymentBindingError("BAR interval identity is invalid")
    if feed not in {"BAR", *_METRIC_INTERVAL_FEEDS} and interval is not None:
        raise DeploymentBindingError("non-BAR route declares an interval")
    fallback = _text(route.get("fallback"), "route fallback").upper()
    if fallback not in {"V1", "BLOCKED"}:
        raise DeploymentBindingError("route fallback is invalid")
    venue = _text(route.get("venue"), "route venue").upper()
    market = _text(route.get("market"), "route market").upper()
    product_type = _text(route.get("product_type"), "route product_type").upper()
    if fallback == "V1" and (venue, market, product_type, feed, interval) != (
        "BINANCE", "USDM", "PERPETUAL", "TRADE", None
    ):
        raise DeploymentBindingError("V1 fallback is allowed only for native Binance USD-M TRADE")
    fallback_rule_id = route.get("fallback_rule_id") if fallback == "V1" else None
    blocked_reason = route.get("blocked_reason") if fallback == "BLOCKED" else None
    if fallback == "V1" and not _text(fallback_rule_id, "route fallback_rule_id"):
        raise DeploymentBindingError("V1 route requires fallback_rule_id")
    if fallback == "BLOCKED" and not _text(blocked_reason, "route blocked_reason"):
        raise DeploymentBindingError("blocked route requires blocked_reason")
    identity = {
        "deployment_id": _text(deployment.get("deployment_id"), "deployment_id"),
        "venue": venue,
        "market": market,
        "product_type": product_type,
        "native_symbol": _text(route.get("native_symbol"), "route native_symbol").upper(),
        "feed": feed,
        "interval": interval,
        "source_policy_id": _text(route.get("source_policy_id"), "route source_policy_id"),
    }
    return {
        "consumer_id": consumer_id,
        "consumer_class": consumer_class,
        "requirement_id": _digest(identity),
        "instrument_uid": _text(instrument.get("instrument_uid"), "instrument_uid"),
        "instrument_id": _text(instrument.get("instrument_id"), "instrument_id"),
        "venue": venue,
        "market": market,
        "product_type": product_type,
        "native_symbol": identity["native_symbol"],
        "feed": feed,
        "interval": interval,
        "source_policy_id": identity["source_policy_id"],
        "provider_plane": _text(route.get("provider_plane"), "route provider_plane").upper(),
        "max_freshness_ms": _positive_int(route.get("max_freshness_ms"), "route max_freshness_ms"),
        "require_final_bars": _bool(route.get("require_final_bars", False), "route require_final_bars"),
        "require_live": _bool(route.get("require_live", False), "route require_live"),
        # Alpha context remains advisory.  Risk owns execution-grade rereads.
        "execution_grade": False,
        "route": "V2_PRIMARY",
        "fallback": fallback,
        "fallback_rule_id": str(fallback_rule_id) if fallback_rule_id is not None else None,
        "blocked_reason": str(blocked_reason) if blocked_reason is not None else None,
        "gap_policy": "BLOCK",
    }


def _missing_reason(
    *,
    route: Mapping[str, Any],
    instrument: Mapping[str, Any],
    bindings: Mapping[tuple[str, str, str | None, str], Mapping[str, Any]],
    reference_keys: set[tuple[str, str, str | None, str]],
) -> str | None:
    feed = _text(route.get("feed"), "route feed").upper()
    interval = str(route["interval"]).strip().lower() if route.get("interval") is not None else None
    key = _route_key(
        instrument_uid=_text(instrument.get("instrument_uid"), "instrument_uid"),
        feed=feed,
        interval=interval,
        source_policy_id=_text(route.get("source_policy_id"), "route source_policy_id"),
    )
    binding = bindings.get(key)
    if binding is None and key not in reference_keys:
        return f"CAPABILITY_UNAVAILABLE:{feed}:{key[3]}"
    if feed == "BAR" and bool(route.get("require_final_bars", False)):
        quality = _mapping(binding.get("quality"), "catalog BAR quality") if binding is not None else None
        if quality is None or quality.get("require_final_bar") is not True:
            return "FINAL_BAR_UNAVAILABLE"
    if feed not in _REFERENCE_FEEDS and binding is None:
        return f"CATALOG_BINDING_UNAVAILABLE:{feed}:{key[3]}"
    return None


def _unavailable_optional(route: Mapping[str, Any], reason: str) -> dict[str, Any]:
    """Preserve a missing optional route as explicit absence, never as zero."""

    return {
        "venue": _text(route.get("venue"), "route venue").upper(),
        "market": _text(route.get("market"), "route market").upper(),
        "product_type": _text(route.get("product_type"), "route product_type").upper(),
        "native_symbol": _text(route.get("native_symbol"), "route native_symbol").upper(),
        "feed": _text(route.get("feed"), "route feed").upper(),
        "interval": str(route["interval"]).strip().lower() if route.get("interval") is not None else None,
        "source_policy_id": _text(route.get("source_policy_id"), "route source_policy_id"),
        "reason": reason,
    }


def _blocked_result(deployment: Mapping[str, Any], reason: str) -> dict[str, Any]:
    return {
        "deployment_id": _text(deployment.get("deployment_id"), "deployment_id"),
        "alpha_id": str(deployment.get("alpha_id") or ""),
        "status": "BLOCKED",
        "reason": reason,
        "optional_unavailable": [],
        "binding": None,
    }


def compile_inventory(
    *,
    inventory: Mapping[str, Any],
    catalog: Mapping[str, Any],
    reference_manifest: Mapping[str, Any],
    release_routing: Mapping[str, Any],
    release_routing_sha256: str,
    policy: Mapping[str, Any],
    policy_sha256: str,
    catalog_sha256: str,
    reference_manifest_sha256: str,
) -> dict[str, Any]:
    """Resolve one portable alpha inventory into standard sealed bindings."""

    deployments = _validate_inventory(inventory)
    records, bindings = _catalog_indexes(catalog)
    reference_keys = _reference_keys(reference_manifest)
    if release_routing.get("schema") != "qdl.v2.stable-release-routing.v1":
        raise DeploymentBindingError("release routing schema is unsupported")
    if policy.get("schema") != "qdl.v2.universal-release-policy.v1":
        raise DeploymentBindingError("release policy schema is unsupported")
    release_revision = _positive_int(release_routing.get("revision"), "release routing revision")
    capability = _mapping(release_routing.get("capability_matrix"), "release capability matrix")
    rollback = _mapping(policy.get("v1_rollback"), "release rollback")
    required_rollback = {"release_tag", "source_commit", "image_reference", "manifest_revision"}
    if set(rollback) != required_rollback:
        raise DeploymentBindingError("release rollback fields are invalid")
    _sha256(release_routing_sha256, "release_routing_sha256")
    _sha256(policy_sha256, "policy_sha256")
    _sha256(catalog_sha256, "catalog_sha256")
    _sha256(reference_manifest_sha256, "reference_manifest_sha256")
    _sha256(capability.get("sha256"), "capability_matrix.sha256")
    capability_revision = _positive_int(capability.get("revision"), "capability_matrix.revision")

    rendered: list[dict[str, Any]] = []
    consumers: set[str] = set()
    for deployment in deployments:
        status = _text(deployment.get("status"), "deployment status")
        if status == "BLOCKED":
            rendered.append(_blocked_result(
                deployment,
                _text(deployment.get("blocked_reason"), "deployment blocked_reason"),
            ))
            continue
        try:
            history = _mapping(deployment.get("history"), "deployment history")
            maxlen = _positive_int(history.get("maxlen"), "deployment history maxlen", maximum=10_000)
            min_bars = _positive_int(history.get("min_bars"), "deployment history min_bars", maximum=10_000)
            if min_bars > maxlen:
                raise DeploymentBindingError("deployment history min_bars exceeds maxlen")
            consumer_id = _consumer_id(deployment)
            if consumer_id in consumers:
                raise DeploymentBindingError("compiled consumer_id is duplicated")
            consumer_class = _consumer_class(deployment)
            routes = deployment.get("routes")
            if not isinstance(routes, list) or not routes:
                raise DeploymentBindingError("declared deployment routes are required")
            products: list[dict[str, Any]] = []
            optional_unavailable: list[dict[str, Any]] = []
            for route_raw in routes:
                route = _mapping(route_raw, "deployment route")
                required = _bool(route.get("required", True), "route required")
                identity = tuple(
                    _text(route.get(field), f"route {field}").upper()
                    for field in ("venue", "market", "product_type", "native_symbol")
                )
                if identity[0] not in _V2_VENUES:
                    raise DeploymentBindingError("route venue is outside V2")
                instrument = records.get(identity)
                if instrument is None:
                    raise DeploymentBindingError(
                        "CATALOG_IDENTITY_UNAVAILABLE:" + ":".join(identity)
                    )
                missing = _missing_reason(
                    route=route,
                    instrument=instrument,
                    bindings=bindings,
                    reference_keys=reference_keys,
                )
                product = _route_product(
                    deployment=deployment,
                    route=route,
                    instrument=instrument,
                    consumer_id=consumer_id,
                    consumer_class=consumer_class,
                )
                if missing is not None:
                    if required:
                        raise DeploymentBindingError(missing)
                    optional_unavailable.append(_unavailable_optional(route, missing))
                    continue
                products.append(product)
            if not products:
                raise DeploymentBindingError("NO_ADMITTED_REQUIRED_ROUTE")
            if len({
                (row["venue"], row["market"], row["product_type"], row["native_symbol"], row["feed"], row["interval"])
                for row in products
            }) != len(products):
                raise DeploymentBindingError("deployment routes are duplicated")
            binding_without_digest = {
                "schema": "qdl.v2.consumer-route-binding.v1",
                "contract_version": CONTRACT_VERSION,
                "consumer_id": consumer_id,
                "consumer_class": consumer_class,
                "release_revision": release_revision,
                # Phase B is source-only.  This anchors the binding to the
                # exact approved release-routing generation; Phase C replaces
                # it with the sealed runtime generation before mount.
                "universal_manifest_sha256": release_routing_sha256,
                "policy_sha256": policy_sha256,
                "capability_matrix": {
                    "sha256": _sha256(capability.get("sha256"), "capability_matrix.sha256"),
                    "revision": capability_revision,
                },
                "inventory_sha256": _inventory_digest(inventory),
                "v1_rollback": {key: _text(rollback[key], f"v1_rollback.{key}") for key in sorted(rollback)},
                "independent_v1_venues": ["DNSE"],
                "products": sorted(products, key=lambda row: row["requirement_id"]),
            }
            binding = {
                **binding_without_digest,
                "binding_sha256": _digest(binding_without_digest),
            }
            # Reuse the strict in-repository parser; TS/SDK use the same
            # canonical field/digest contract independently.
            ConsumerRouteBinding.from_canonical_mapping(binding)
            consumers.add(consumer_id)
            rendered.append({
                "deployment_id": _text(deployment.get("deployment_id"), "deployment_id"),
                "alpha_id": _text(deployment.get("alpha_id"), "alpha_id"),
                "status": "ADMITTED",
                "reason": None,
                "history": {"maxlen": maxlen, "min_bars": min_bars},
                "optional_unavailable": sorted(
                    optional_unavailable,
                    key=lambda item: (
                        item["venue"], item["market"], item["product_type"],
                        item["native_symbol"], item["feed"], item["interval"] or "",
                        item["source_policy_id"],
                    ),
                ),
                "binding": binding,
            })
        except DeploymentBindingError as error:
            rendered.append(_blocked_result(deployment, str(error)))

    report_without_digest = {
        "schema": REPORT_SCHEMA,
        "contract_version": CONTRACT_VERSION,
        "inventory_sha256": _inventory_digest(inventory),
        "catalog_sha256": catalog_sha256,
        "reference_manifest_sha256": reference_manifest_sha256,
        "release_routing_sha256": release_routing_sha256,
        "policy_sha256": policy_sha256,
        "deployments": sorted(rendered, key=lambda item: item["deployment_id"]),
    }
    return {**report_without_digest, "compilation_sha256": _digest(report_without_digest)}


def _write_json(path: Path, payload: Mapping[str, Any]) -> bool:
    rendered = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    if path.exists() and path.read_bytes() == rendered:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(rendered)
    return True


def write_compilation(output_dir: Path, report: Mapping[str, Any]) -> tuple[str, ...]:
    """Write only named derived artifacts to a caller-owned output directory."""

    changed: list[str] = []
    for deployment in report["deployments"]:
        binding = deployment.get("binding")
        if binding is None:
            continue
        name = _SAFE_FILE_COMPONENT.sub("-", str(deployment["deployment_id"])).strip("-")
        path = output_dir / f"{name}.binding.json"
        if _write_json(path, binding):
            changed.append(str(path))
    if _write_json(output_dir / "compilation-report.json", report):
        changed.append(str(output_dir / "compilation-report.json"))
    return tuple(changed)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, default=ROOT / "config/v2/stable-source-bindings.yaml")
    parser.add_argument("--reference-manifest", type=Path, default=ROOT / "consumers/stable/reference-l2-stable.yaml")
    parser.add_argument("--release-routing", type=Path, default=ROOT / "config/v2/stable-v2-release-routing.yaml")
    parser.add_argument("--policy", type=Path, default=ROOT / "config/v2/universal-release-policy.yaml")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args(argv)
    if args.write and args.output_dir is None:
        raise DeploymentBindingError("--write requires --output-dir")

    report = compile_inventory(
        inventory=_load_json_mapping(args.inventory),
        catalog=_load_mapping(args.catalog),
        reference_manifest=_load_mapping(args.reference_manifest),
        release_routing=_load_mapping(args.release_routing),
        release_routing_sha256=_sha256_file(args.release_routing),
        policy=_load_mapping(args.policy),
        policy_sha256=_sha256_file(args.policy),
        catalog_sha256=_sha256_file(args.catalog),
        reference_manifest_sha256=_sha256_file(args.reference_manifest),
    )
    changed = write_compilation(args.output_dir, report) if args.write else ()
    summary = {
        "status": "WRITTEN_SOURCE_ONLY" if args.write else "DRY_RUN_SOURCE_ONLY",
        "admitted": sum(item["status"] == "ADMITTED" for item in report["deployments"]),
        "blocked": sum(item["status"] == "BLOCKED" for item in report["deployments"]),
        "compilation_sha256": report["compilation_sha256"],
        "changed_files": list(changed),
        "runtime_mutations": 0,
        "order_actions": 0,
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
