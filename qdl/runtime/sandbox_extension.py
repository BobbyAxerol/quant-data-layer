"""Pure additive sandbox read packet; activation and certification are separate."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile

import yaml

from qdl.consumer.manifest import ConsumerManifestLoader
from qdl.consumer.realtime_route import requirement_key
from qdl.consumer.universal_release import ConsumerRouteBinding
from qdl.runtime.production_catalog import ProductionCatalogBuilder
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _key(row):
    return (row["instrument_uid"], row["feed"], row.get("interval"), row["source_policy_id"])


def prepare_sandbox_extension(*, catalog, acquisition, manifest, binding, demand,
                              okx_rows, template_uid):
    """Compile four reviewed inverse feeds without changing other consumer scopes.

    The old execution template supplies policy, never its instrument identity.
    The new generation is a prepared config receipt, not a release certificate.
    """
    consumer = ConsumerManifestLoader.from_mapping(manifest)
    base = ConsumerRouteBinding.from_canonical_mapping(binding)
    current = StableSourceCatalog.from_mapping(catalog)
    if (consumer.environment != "sandbox"
            or consumer.consumer_id != "trading-system.sandbox.stable"
            or consumer.subject != "spiffe://qdl/sandbox/trading-system-stable"
            or consumer.execution_dependency != "ALLOWED"
            or base.consumer_id != consumer.consumer_id
            or base.consumer_manifest_revision != consumer.manifest_revision):
        raise ValueError("sandbox identity/revision mismatch")
    expected = {"QUOTE", "MARK_INDEX_PRICE", "BOOK_SNAPSHOT", "BOOK_DELTA"}
    if (len(demand.demands) != 4 or {r.feed.value for r in demand.demands} != expected
            or any(r.consumer_id != consumer.consumer_id or r.consumer_grade.value != "EXECUTION"
                   or (r.venue, r.market, r.product_type, r.native_symbol) !=
                   ("OKX", "SWAP", "PERPETUAL", "BTC-USD-SWAP") for r in demand.demands)):
        raise ValueError("extension outside approved inverse sandbox scope")
    requirements = {_key(r): r for r in manifest["spec"]["requirements"]}
    products = {_key(r): r for r in binding["products"]}
    if (len(requirements) != len(manifest["spec"]["requirements"])
            or len(products) != len(binding["products"]) or products.keys() != requirements.keys()):
        raise ValueError("base binding coverage mismatch")
    for key, route in products.items():
        record = current.instrument_for(key[0])
        if (route["instrument_id"], route["native_symbol"], route["venue"],
                route["market"], route["product_type"]) != (
                record.identity.instrument_id, record.native_symbol, record.identity.venue,
                record.identity.market, record.identity.product_type.value):
            raise ValueError("base binding/catalog identity mismatch")
    template = current.instrument_for(template_uid)
    if (template.identity.venue, template.identity.market, template.identity.product_type.value) != (
            "OKX", "SWAP", "PERPETUAL"):
        raise ValueError("template venue/product mismatch")
    extension = ProductionCatalogBuilder(
        catalog_revision=current.catalog_revision + 1,
        source_policy_revision=catalog["source_policy_revision"],
        authority_revision=catalog["authority_revision"],
        canonical_stream=catalog["canonical_stream"],
        raw_topic=acquisition["topics"]["raw"],
        quarantine_topic=acquisition["topics"]["quarantine"],
    ).build(demand=demand, binance_usdm=None, okx_rows=okx_rows)
    inverse = extension.source_catalog["instruments"]
    if (len(inverse) != 1 or inverse[0]["base_asset"] != "BTC"
            or inverse[0]["quote_asset"] != "USD" or inverse[0]["settlement_asset"] != "BTC"
            or inverse[0]["attributes"].get("ctType") != "inverse"):
        raise ValueError("inverse collateral/identity mismatch")
    result_catalog = deepcopy(catalog)
    for field, identifier in (("instruments", "instrument_id"), ("bindings", "binding_id")):
        old_ids = {r[identifier] for r in catalog[field]}
        if any(r[identifier] in old_ids for r in extension.source_catalog[field]):
            raise ValueError("extension already exists; explicit revision review required")
        result_catalog[field].extend(deepcopy(extension.source_catalog[field]))
    result_catalog["catalog_revision"] += 1
    result_acquisition = deepcopy(acquisition)
    result_acquisition["revision"] += 1
    result_acquisition["bindings"].extend(extension.acquisition_plan["bindings"])
    parsed_catalog = StableSourceCatalog.from_mapping(result_catalog)
    with tempfile.TemporaryDirectory(prefix="qdl-sandbox-validate-") as raw:
        path = Path(raw) / "acquisition.yaml"
        path.write_text(yaml.safe_dump(result_acquisition))
        StableAcquisitionPlan.load(path, catalog=parsed_catalog)
    target = deepcopy(manifest)
    target["metadata"]["revision"] += 1
    target_binding = deepcopy(binding)
    added = []
    for row in extension.source_catalog["bindings"]:
        key = (template_uid, row["feed"], row.get("interval"), row["source"]["source_policy_id"])
        if key not in requirements or key not in products:
            raise ValueError("missing exact execution policy template")
        req, route = deepcopy(requirements[key]), deepcopy(products[key])
        if (req["consumer_grade"] != "EXECUTION" or route["route"] != "V2_PRIMARY"
                or not route["execution_grade"] or route["fallback"] != "BLOCKED"
                or req.get("gap_policy", "BLOCK") != "BLOCK"
                or req.get("stale_policy", "BLOCK") != "BLOCK"
                or not req.get("require_full_coverage", True)):
            raise ValueError("template is not fail-closed execution policy")
        if (route["max_freshness_ms"] != req["max_freshness_ms"]
                or route["require_final_bars"] != req.get("require_final_bars", True)):
            raise ValueError("template quality mismatch")
        req["instrument_uid"] = row["instrument_uid"]
        instrument = parsed_catalog.instrument_for(row["instrument_uid"])
        route.update(instrument_uid=instrument.identity.instrument_uid,
                     instrument_id=instrument.identity.instrument_id,
                     native_symbol=instrument.native_symbol)
        target["spec"]["requirements"].append(req)
        added.append((req, route))
    parsed_manifest = ConsumerManifestLoader.from_mapping(target)
    for req, route in added:
        parsed = next(r for r in parsed_manifest.requirements if (
            r.instrument_uid == req["instrument_uid"] and r.feed.value == req["feed"]))
        parsed_catalog.binding_for(parsed)
        route["requirement_id"] = hashlib.sha256(
            f"{consumer.consumer_id}:{requirement_key(parsed)}".encode()).hexdigest()
        target_binding["products"].append(route)
    provenance = {
        "schema": "qdl.v2.sandbox-extension-preparation.v1",
        "status": "PREPARED_NOT_ACTIVATED_NOT_CERTIFIED",
        "base_binding_sha256": base.binding_sha256,
        "base_catalog_sha256": _digest(catalog),
        "catalog_sha256": _digest(result_catalog),
        "acquisition_sha256": _digest(result_acquisition),
        "consumer_manifest_sha256": parsed_manifest.manifest_sha256,
        "metadata_sha256": _digest(okx_rows),
        "demand_sha256": _digest([{"consumer": r.consumer_id, "key": [str(v) for v in r.requirement_key],
                      "index": r.index_native_symbol, "depth": r.depth_per_side,
                      "freshness": r.max_freshness_ms, "live": r.require_live}
                     for r in demand.demands]),
        "added_products": len(added),
    }
    target_binding.update(consumer_manifest_revision=parsed_manifest.manifest_revision,
                          release_revision=base.release_revision + 1,
                          universal_manifest_sha256=_digest(provenance),
                          inventory_sha256=provenance["demand_sha256"])
    target_binding["products"].sort(key=lambda r: r["requirement_id"])
    target_binding.pop("binding_sha256")
    target_binding["binding_sha256"] = _digest(target_binding)
    ConsumerRouteBinding.from_canonical_mapping(target_binding)
    return dict(catalog=result_catalog, acquisition=result_acquisition,
                manifest=target, binding=target_binding, provenance=provenance)
