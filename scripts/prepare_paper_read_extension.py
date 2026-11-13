"""Prepare an owner-approved Paper entitlement extension; never activate it.

Two extensions share one fail-closed base check and one sealing path:

* IDR-075 (default): the four BTC-USD-SWAP inverse feeds cloned from the ETH-USDT-SWAP execution template.
* v2.2.6 linear OKX swaps (``--linear-okx-swaps FILE``): the given OKX linear USDT swaps, only the requested feeds
  (default QUOTE + MARK_INDEX_PRICE, the Paper BBO_EXECUTABLE minimum), cloned from the same ETH template. Every
  new requirement must already have a catalog source and acquisition binding (``StableSourceCatalog.binding_for``),
  so the catalog/acquisition change lands first.

Optional quota raise (P18.4 D1, owner 2026-10-09): only max_streams / requests_per_minute, only upward, recorded in the
provenance. Without it the IDR-075 packet is byte-identical to the original IDR-075 preparation."""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import yaml

from qdl.consumer.manifest import ConsumerManifestLoader
from qdl.consumer.realtime_route import requirement_key
from qdl.consumer.universal_release import ConsumerRouteBinding
from qdl.runtime.stable_catalog import StableSourceCatalog

CONSUMER = "trading-system.paper.stable"
SUBJECT = "spiffe://qdl/paper/trading-system-stable"
FEEDS = frozenset({"QUOTE", "MARK_INDEX_PRICE", "BOOK_SNAPSHOT", "BOOK_DELTA"})


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def key(row):
    return (row["instrument_uid"], row["feed"], row.get("interval"), row["source_policy_id"])


QUOTA_KEYS = frozenset({"max_streams", "requests_per_minute"})


def _validated_base(catalog, manifest, binding):
    """Fail-closed identity/quality checks of the deployed manifest + sealed binding pair."""
    current = StableSourceCatalog.from_mapping(catalog)
    consumer = ConsumerManifestLoader.from_mapping(manifest)
    base = ConsumerRouteBinding.from_canonical_mapping(binding)
    if (consumer.consumer_id != CONSUMER or consumer.subject != SUBJECT
            or consumer.environment != "paper" or consumer.execution_dependency != "PAPER_ONLY"
            or base.consumer_id != CONSUMER
            or base.consumer_manifest_revision != consumer.manifest_revision):
        raise ValueError("Paper identity/revision mismatch")
    requirements = {key(r): r for r in manifest["spec"]["requirements"]}
    products = {key(r): r for r in binding["products"]}
    if (len(requirements) != len(manifest["spec"]["requirements"])
            or len(products) != len(binding["products"])
            or not products.keys() <= requirements.keys()):
        raise ValueError("duplicate or unentitled base product")
    old_policies = {p["requirement_id"]: p for p in binding.get("request_policies", [])}
    for k, route in products.items():
        req = requirements[k]
        record = current.instrument_for(k[0])
        if (route["instrument_id"], route["native_symbol"], route["venue"],
                route["market"], route["product_type"]) != (
                record.identity.instrument_id, record.native_symbol, record.identity.venue,
                record.identity.market, record.identity.product_type.value):
            raise ValueError("base catalog identity mismatch")
        if (route["max_freshness_ms"] != req["max_freshness_ms"]
                or route["require_final_bars"] != req.get("require_final_bars", True)):
            raise ValueError("base product quality mismatch")
        expected = dict(requirement_id=route["requirement_id"],
                        event_recency_policy=req.get("event_recency_policy"),
                        max_session_liveness_ms=req.get("max_session_liveness_ms"))
        if old_policies and old_policies.get(route["requirement_id"]) != expected:
            raise ValueError("base sealed request policy mismatch")
    return current, consumer, base, requirements, products


def _okx_swap(catalog, symbol):
    rows = [r for r in catalog["instruments"] if r.get("native_symbol") == symbol
            and r.get("venue") == "OKX" and r.get("market") == "SWAP"
            and r.get("product_type") == "PERPETUAL"]
    if len(rows) != 1:
        raise ValueError("missing or ambiguous already-acquired instrument")
    return rows[0]


def _execution_template(requirements, products, template, feed):
    policy = "crypto_primary_v2" if feed == "QUOTE" else "crypto_liquid_v2"
    k = (template["instrument_uid"], feed, None, policy)
    if k not in requirements or k not in products:
        raise ValueError("missing exact ETH execution template")
    req, route = deepcopy(requirements[k]), deepcopy(products[k])
    if (req["consumer_grade"] != "EXECUTION" or req.get("stale_policy") != "BLOCK"
            or req.get("gap_policy") != "BLOCK" or not req.get("require_full_coverage")
            or route["route"] != "V2_PRIMARY" or route["fallback"] != "BLOCKED"
            or not route["execution_grade"]):
        raise ValueError("template is not fail-closed execution policy")
    return req, route


def _raise_quotas(manifest, target, quotas):
    if not quotas:
        return None
    current_quotas = manifest["spec"]["quotas"]
    if (set(quotas) - QUOTA_KEYS or any(type(v) is not int or v <= current_quotas[k] for k, v in quotas.items())):
        raise ValueError("quota change must raise max_streams/requests_per_minute only")
    target["spec"]["quotas"].update(quotas)
    return {k: {"from": current_quotas[k], "to": v} for k, v in sorted(quotas.items())}


def _seal(*, current, consumer, base, products, catalog, target, new_binding, quota_change, **provenance_fields):
    parsed = ConsumerManifestLoader.from_mapping(target)
    parsed_requirements = {requirement_key(r): r for r in parsed.requirements}
    new_binding["request_policies"] = []
    for row in new_binding["products"]:
        k = f"{row['instrument_uid']}:{row['feed']}:{row.get('interval') or ''}:{row['source_policy_id']}"
        req = parsed_requirements[k]
        current.binding_for(req)
        expected_id = hashlib.sha256(f"{CONSUMER}:{k}".encode()).hexdigest()
        if key(row) in products and row["requirement_id"] != expected_id:
            raise ValueError("base requirement identity mismatch")
        row["requirement_id"] = expected_id
        recency = req.event_recency_policy
        new_binding["request_policies"].append(dict(
            requirement_id=expected_id,
            event_recency_policy=None if recency is None else str(recency.value),
            max_session_liveness_ms=req.max_session_liveness_ms))
    provenance = dict(schema="qdl.v2.paper-extension-preparation.v1",
                      status="PREPARED_NOT_ACTIVATED_NOT_CERTIFIED", runtime_mutations=0,
                      base_binding_sha256=base.binding_sha256,
                      base_manifest_sha256=consumer.manifest_sha256,
                      consumer_manifest_sha256=parsed.manifest_sha256,
                      deployed_catalog_sha256=digest(catalog), **provenance_fields)
    if quota_change is not None:
        provenance["quota_change"] = quota_change
    new_binding.update(schema="qdl.v2.consumer-route-binding.v3",
                       consumer_manifest_revision=parsed.manifest_revision,
                       release_revision=base.release_revision + 1,
                       universal_manifest_sha256=digest(provenance))
    for field in ("products", "request_policies"):
        new_binding[field].sort(key=lambda row: row["requirement_id"])
    new_binding.pop("binding_sha256")
    new_binding["binding_sha256"] = digest(new_binding)
    ConsumerRouteBinding.from_canonical_mapping(new_binding)
    return dict(manifest=target, binding=new_binding, provenance=provenance,
                consumer_environment={
                    "DATA_LAYER_V2_JWT_MANIFEST_REVISION": str(parsed.manifest_revision),
                    "DATA_LAYER_V2_RELEASE_MANIFEST_SHA256": new_binding["universal_manifest_sha256"]})


def prepare_paper_extension(*, catalog, manifest, binding, quotas=None):
    """Reuse deployed inverse feeds; no acquisition, credential or runtime mutation."""
    current, consumer, base, requirements, products = _validated_base(catalog, manifest, binding)

    def instrument(symbol):
        rows = [r for r in catalog["instruments"] if r.get("native_symbol") == symbol
                and r.get("venue") == "OKX" and r.get("market") == "SWAP"
                and r.get("product_type") == "PERPETUAL"]
        if len(rows) != 1:
            raise ValueError("missing or ambiguous already-acquired instrument")
        return rows[0]

    inverse, linear, template = map(instrument, ("BTC-USD-SWAP", "BTC-USDT-SWAP", "ETH-USDT-SWAP"))
    if (inverse["base_asset"], inverse["quote_asset"], inverse["settlement_asset"],
            inverse.get("attributes", {}).get("ctType")) != ("BTC", "USD", "BTC", "inverse"):
        raise ValueError("inverse collateral identity mismatch")
    fx_key = (linear["instrument_uid"], "MARK_INDEX_PRICE", None, "crypto_liquid_v2")
    if fx_key not in products:
        raise ValueError("existing BTC-USDT-SWAP MARK/INDEX FX entitlement missing")
    if any(k[0] == inverse["instrument_uid"] for k in requirements):
        raise ValueError("inverse extension already present; explicit revision review required")
    target, new_binding = deepcopy(manifest), deepcopy(binding)
    target["metadata"]["revision"] += 1
    quota_change = _raise_quotas(manifest, target, quotas)
    for feed in sorted(FEEDS):
        req, route = _execution_template(requirements, products, template, feed)
        req["instrument_uid"] = inverse["instrument_uid"]
        route.update(instrument_uid=inverse["instrument_uid"],
                     instrument_id=inverse["instrument_id"], native_symbol=inverse["native_symbol"])
        target["spec"]["requirements"].append(req)
        new_binding["products"].append(route)
    return _seal(current=current, consumer=consumer, base=base, products=products, catalog=catalog,
                 target=target, new_binding=new_binding, quota_change=quota_change,
                 added_products=4, existing_fx_product_preserved=True)


LINEAR_FEEDS = frozenset({"QUOTE", "MARK_INDEX_PRICE"})


def prepare_paper_linear_swap_extension(*, catalog, manifest, binding, symbols, feeds=LINEAR_FEEDS, quotas=None):
    """Entitle OKX linear USDT swaps for the requested execution feeds (v2.2.6 crossec Paper coverage).

    Fail-closed: every symbol must be a catalog OKX SWAP PERPETUAL with ctType linear and USDT quote/settlement, no
    requested (instrument, feed) may already be entitled (explicit revision review), and every new requirement must
    resolve to a catalog binding with acquisition. No acquisition, credential or runtime mutation."""
    current, consumer, base, requirements, products = _validated_base(catalog, manifest, binding)
    feeds = frozenset(feeds)
    if not feeds or feeds - FEEDS:
        raise ValueError("linear extension feeds must be a subset of QUOTE/MARK_INDEX_PRICE/BOOK_SNAPSHOT/BOOK_DELTA")
    symbols = list(symbols)
    if not symbols or len(set(symbols)) != len(symbols):
        raise ValueError("linear extension symbols must be a non-empty unique list")
    template = _okx_swap(catalog, "ETH-USDT-SWAP")
    templates = {feed: _execution_template(requirements, products, template, feed) for feed in sorted(feeds)}
    target, new_binding = deepcopy(manifest), deepcopy(binding)
    target["metadata"]["revision"] += 1
    quota_change = _raise_quotas(manifest, target, quotas)
    added = []
    for symbol in symbols:
        row = _okx_swap(catalog, symbol)
        if (row.get("quote_asset"), row.get("settlement_asset"), row.get("attributes", {}).get("ctType")) != (
                "USDT", "USDT", "linear"):
            raise ValueError(f"{symbol} is not an OKX linear USDT swap")
        for feed in sorted(feeds):
            req, route = deepcopy(templates[feed][0]), deepcopy(templates[feed][1])
            if (row["instrument_uid"], feed, None, req["source_policy_id"]) in requirements:
                raise ValueError(f"{symbol} {feed} already entitled; explicit revision review required")
            req["instrument_uid"] = row["instrument_uid"]
            route.update(instrument_uid=row["instrument_uid"], instrument_id=row["instrument_id"],
                         native_symbol=row["native_symbol"])
            target["spec"]["requirements"].append(req)
            new_binding["products"].append(route)
            added.append(f"{symbol}:{feed}")
    return _seal(current=current, consumer=consumer, base=base, products=products, catalog=catalog,
                 target=target, new_binding=new_binding, quota_change=quota_change,
                 added_products=len(added), linear_okx_swaps=sorted(symbols), feeds=sorted(feeds))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("catalog", "manifest", "binding", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--max-streams", type=int)
    parser.add_argument("--requests-per-minute", type=int)
    parser.add_argument("--linear-okx-swaps", type=Path,
                        help="JSON list of OKX linear USDT swap native symbols (v2.2.6 mode)")
    parser.add_argument("--feeds", default="QUOTE,MARK_INDEX_PRICE", help="linear mode only")
    args = parser.parse_args()
    quotas = {k: v for k, v in (("max_streams", args.max_streams),
                                ("requests_per_minute", args.requests_per_minute)) if v is not None}
    inputs = {name: yaml.safe_load(getattr(args, name).read_text()) for name in ("catalog", "manifest", "binding")}
    if args.linear_okx_swaps:
        result = prepare_paper_linear_swap_extension(
            **inputs, symbols=json.loads(args.linear_okx_swaps.read_text()),
            feeds=[f.strip().upper() for f in args.feeds.split(",") if f.strip()], quotas=quotas or None)
    else:
        result = prepare_paper_extension(**inputs, quotas=quotas or None)
    # Never overwrite an earlier preparation or a live manifest.
    with args.output.open("x") as out:
        json.dump(result, out, indent=2, sort_keys=True)
        out.write("\n")
    print(json.dumps(dict(status=result["provenance"]["status"],
                         products=len(result["binding"]["products"]),
                         manifest_revision=result["manifest"]["metadata"]["revision"])))


if __name__ == "__main__":
    main()
