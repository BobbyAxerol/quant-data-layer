"""Offline TS-side acceptance of the stage B render, with the real TS loaders (run inside the TS image, --network none).

Checks, exactly as the TS writer does at start (adapters/market_data/data_layer_v2.py:3375-3430):
  1. DataLayerConsumerRouteBinding.load(r26, expected release sha, expected manifest revision 14, require=True);
  2. DataLayerV2RouteManifest.load(r26, same expectations) (the ROUTE_MANIFEST_FILE path);
  3. the negative coupling: r26 with JWT revision 13 is refused (so the writer cannot keep 13);
  4. r25 still loads with JWT 13 (the Paper roles that are not recreated);
  5. services.market_data.declared_scope.parse_declared_scope on the rendered execution scope + r26 bytes, then
     classify with the binding's own route selection: every route admitted (0 unbound);
  6. every one of the 71 instruments routes QUOTE and MARK_INDEX_PRICE to V2.
usage (cwd = TS source): python -B ts_accept_check.py OUT_DIR R25 RELEASE_SHA_R14 RELEASE_SHA_R13 SYMBOLS_JSON
"""
import json, sys
from pathlib import Path

sys.path.insert(0, ".")
from alpha_sdk.data_layer_release_binding import DataLayerConsumerRouteBinding  # noqa: E402
from adapters.market_data.data_layer_v2_routes import DataLayerV2RouteManifest  # noqa: E402
from services.market_data.declared_scope import classify, parse_declared_scope  # noqa: E402

out, r25, sha14, sha13, syms = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3], sys.argv[4], Path(sys.argv[5])
cid = "trading-system.paper.stable"
r26 = out / "bindings/trading-system.paper.stable-r26.json"
res = {}


def attempt(name, fn):
    try:
        value = fn()
        res[name] = {"ok": True, **(value or {})}
    except Exception as error:  # noqa: BLE001 - recorded verbatim
        res[name] = {"ok": False, "error": f"{type(error).__name__}: {error}"}


kw = dict(expected_consumer_id=cid, expected_release_manifest_sha256=sha14, expected_consumer_manifest_revision=14,
          require_consumer_manifest_revision=True)
binding = {}
attempt("binding_r26_jwt14", lambda: binding.setdefault("b", DataLayerConsumerRouteBinding.load(str(r26), **kw)) and
        {"products": len(json.loads(r26.read_text())["products"])})
attempt("route_manifest_r26_jwt14", lambda: DataLayerV2RouteManifest.load(str(r26), **kw) and {})
attempt("binding_r26_jwt13_must_refuse", lambda: DataLayerConsumerRouteBinding.load(
    str(r26), **{**kw, "expected_consumer_manifest_revision": 13}) and {"refused": False})
res["binding_r26_jwt13_must_refuse"]["expected_refusal_seen"] = not res["binding_r26_jwt13_must_refuse"]["ok"]
attempt("binding_r25_jwt13_paper_roles", lambda: DataLayerConsumerRouteBinding.load(
    str(r25), **{**kw, "expected_release_manifest_sha256": sha13, "expected_consumer_manifest_revision": 13}) and {})

b = binding.get("b")


def admits(route):
    sel = b.select_provider(route.venue.lower(), route.symbol, feed=route.feed, market="auto")
    return sel is not None and bool(getattr(sel, "use_v2", False))


def scope():
    raw = (out / f"execution-scope/{cid}.json").read_bytes()
    parsed = parse_declared_scope(raw, consumer_id=cid, binding_bytes=r26.read_bytes(), token_manifest_revision=14)
    done = classify(parsed, admits)
    return {"state": done.state, "revision": done.revision, "manifest_revision": done.manifest_revision,
            "routes": len(done.routes), "unbound": len(done.unbound),
            "unbound_sample": [f"{r.venue}:{r.symbol}:{r.feed}" for r in done.unbound[:5]]}


attempt("declared_scope_parse_classify", scope)


def the71():
    missing = []
    for s in json.loads(syms.read_text()):
        for feed in ("QUOTE", "MARK_INDEX_PRICE"):
            sel = b.select_provider("okx", s, feed=feed, market="auto")
            if sel is None or not getattr(sel, "use_v2", False):
                missing.append(f"{s}:{feed}")
    return {"checked": 142, "not_v2": missing[:10], "not_v2_count": len(missing)}


attempt("stageB71_routes_v2", the71)
ok = (res["binding_r26_jwt14"]["ok"] and res["route_manifest_r26_jwt14"]["ok"]
      and res["binding_r26_jwt13_must_refuse"]["expected_refusal_seen"] and res["binding_r25_jwt13_paper_roles"]["ok"]
      and res["declared_scope_parse_classify"].get("state") == "LOADED"
      and res["declared_scope_parse_classify"].get("unbound") == 0
      and res["stageB71_routes_v2"].get("not_v2_count") == 0)
print(json.dumps({"schema": "stageb.ts-acceptance.v1", "status": "PASS" if ok else "FAIL", "checks": res}, indent=1,
                 default=str))
sys.exit(0 if ok else 1)
