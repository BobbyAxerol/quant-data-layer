"""Independent read-only check that a stage B render keeps every retained row byte-identical (JSON-equal) to the base.

Compares, per document, the rows keyed by their identity: base rows must all exist in the render with equal content,
and the render may only add rows. Prints one JSON verdict; exit 0 PASS, 1 FAIL.
usage: retained_check.py BASE_REPO LIVE_DIR OUT_DIR
"""
import json, sys
from pathlib import Path
import yaml

base_repo, live, out = map(Path, sys.argv[1:4])
y = lambda p: yaml.safe_load(Path(p).read_text())
j = lambda p: json.loads(Path(p).read_text())
results = {}


def compare(name, old_rows, new_rows, key):
    old = {}
    for r in old_rows:
        old.setdefault(key(r), []).append(r)
    new = {}
    for r in new_rows:
        new.setdefault(key(r), []).append(r)
    missing = [k for k in old if k not in new]
    changed = [k for k in old if k in new and old[k] != new[k]]
    results[name] = {"base": len(old_rows), "render": len(new_rows), "added": len(new_rows) - len(old_rows),
                     "missing": [str(k) for k in missing[:5]], "changed": [str(k) for k in changed[:5]],
                     "status": "PASS" if not missing and not changed and len(new_rows) >= len(old_rows) else "FAIL"}


r = out / "repo"
cat_o, cat_n = y(base_repo / "config/v2/stable-source-bindings.yaml"), y(r / "config/v2/stable-source-bindings.yaml")
compare("catalog.instruments", cat_o["instruments"], cat_n["instruments"], lambda x: x["instrument_uid"])
compare("catalog.bindings", cat_o["bindings"], cat_n["bindings"], lambda x: x["binding_id"])
acq_o, acq_n = y(base_repo / "config/v2/stable-acquisition-bindings.yaml"), y(r / "config/v2/stable-acquisition-bindings.yaml")
rows = lambda d: d.get("bindings") or d.get("acquisitions") or []
compare("acquisition(repo)", rows(acq_o), rows(acq_n), lambda x: x["binding_id"])
qa_o, qa_n = y(live / "query-stable-acquisition-bindings.yaml"), y(out / "runtime/stable-acquisition-bindings.yaml")
compare("acquisition(query runtime)", rows(qa_o), rows(qa_n), lambda x: x["binding_id"])
qc_n = y(out / "runtime/stable-source-bindings.yaml")
qc_o = y(live / "query-stable-source-bindings.yaml")
compare("catalog(query runtime).bindings", qc_o["bindings"], qc_n["bindings"], lambda x: x["binding_id"])
sc_o, sc_n = y(base_repo / "config/v2/stable-authority-promotion-scope.yaml"), y(r / "config/v2/stable-authority-promotion-scope.yaml")
compare("promotion_scope.binding_ids", [{"id": v} for v in sc_o["binding_ids"]], [{"id": v} for v in sc_n["binding_ids"]],
        lambda x: x["id"])
dm_o, dm_n = y(base_repo / "config/v2/stable-crypto-demand.yaml"), y(r / "config/v2/stable-crypto-demand.yaml")
flat = lambda d: [dict(rq, _c=c["consumer_id"]) for c in d["consumers"] for rq in c["requirements"]]
compare("demand.requirements", flat(dm_o), flat(dm_n), lambda x: json.dumps(x, sort_keys=True))
m_o = y(live / "trading-system.paper.stable-r13.yaml")
m_n = y(out / "consumer-realms/trading-system.paper.stable-r14.yaml")
compare("ts_manifest.requirements", m_o["spec"]["requirements"], m_n["spec"]["requirements"],
        lambda x: (x["instrument_uid"], x["feed"], x.get("interval")))
results["ts_manifest.metadata"] = {"base": m_o["metadata"], "render": m_n["metadata"],
                                   "quotas": [m_o["spec"]["quotas"], m_n["spec"]["quotas"]]}
b_o = j(live / "trading-system.paper.stable-r25.json")
b_n = j(next((out / "bindings").glob("trading-system.paper.stable-r26.json")))
compare("binding.products", b_o["products"], b_n["products"], lambda x: x["requirement_id"])
compare("binding.request_policies", b_o["request_policies"], b_n["request_policies"],
        lambda x: x.get("requirement_id") or json.dumps(x, sort_keys=True))
for f in ("ingestor-okx-swap.json", "ingestor-binance-usdm.json"):
    if (out / "ingestor" / f).exists():
        i_o, i_n = j(live / f), j(out / "ingestor" / f)
        compare(f"ingestor/{f}.bindings", i_o["bindings"], i_n["bindings"],
                lambda x: x["subscription_id"])
        results[f"ingestor/{f}.non_binding_fields_changed"] = sorted(
            k for k in set(i_o) | set(i_n) if k != "bindings" and i_o.get(k) != i_n.get(k))
for f in ("rust_core.json", "rust_core_2.json", "rust_core_3.json"):
    c_o, c_n = j(live / "cores" / f), j(out / "cores" / f)
    compare(f"cores/{f}.bindings", c_o["core"]["bindings"], c_n["core"]["bindings"],
            lambda x: (x.get("source_id"), x.get("native_channel"), x.get("native_symbol"), x.get("provider_kind")))
    results[f"cores/{f}.non_binding_fields_changed"] = sorted(
        k for k in set(c_o["core"]) | set(c_n["core"]) if k != "bindings" and c_o["core"].get(k) != c_n["core"].get(k)) + sorted(  # noqa
        k for k in set(c_o) | set(c_n) if k != "core" and c_o.get(k) != c_n.get(k))
status = "PASS" if all(v.get("status", "PASS") == "PASS" for v in results.values() if isinstance(v, dict)) and all(
    not v for k, v in results.items() if k.endswith("non_binding_fields_changed") and "cores/" in k) else "FAIL"
print(json.dumps({"schema": "stageb.retained-rows-check.v1", "status": status, "checks": results}, indent=1, sort_keys=True,
                 default=str))
sys.exit(0 if status == "PASS" else 1)
