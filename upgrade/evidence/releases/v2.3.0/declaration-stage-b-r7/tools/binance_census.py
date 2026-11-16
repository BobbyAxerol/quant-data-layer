"""Read-only census: which consumers grant / demand each Binance USD-M feed for BNB/BTC/DOGE/ETH/SOL.

Inputs (files only, no runtime): the out6 catalogue + demand (repo form), every consumer manifest in the mounted
realm dir, the out6 repo manifests, the live Binance ingestor config, the live acquisition plan.
usage: binance_census.py OUT6_REPO REALM_DIR BINANCE_INGESTOR_JSON QUERY_ACQUISITION_YAML OUT.json
"""
import collections, json, sys
from pathlib import Path
import yaml

repo, realm, ingestor, acq_path, out = map(Path, sys.argv[1:6])
SYMS = ("BNBUSDT", "BTCUSDT", "DOGEUSDT", "ETHUSDT", "SOLUSDT")
cat = yaml.safe_load((repo / "config/v2/stable-source-bindings.yaml").read_text())
inst = {i["instrument_uid"]: i for i in cat["instruments"]}
bin_uids = {u for u, i in inst.items() if i["venue"] == "BINANCE" and i["native_symbol"] in SYMS}
key = lambda u: f'{inst[u]["venue"]}:{inst[u]["market"]}:{inst[u]["native_symbol"]}'

def rows_of(doc):
    return (doc.get("spec") or {}).get("requirements") or []

manifests = {}
for path in sorted(list(realm.glob("*.yaml")) + list((repo / "consumers/stable").glob("*.yaml"))):
    doc = yaml.safe_load(path.read_text())
    if not isinstance(doc, dict) or "metadata" not in doc:
        continue
    md = doc["metadata"]
    sel = (doc.get("spec") or {}).get("selectors") or []
    manifests[f"{path.parent.name}/{path.name}"] = {"id": md.get("id"), "revision": md.get("revision"),
                                                    "rows": rows_of(doc), "selectors": len(sel)}

grants = collections.defaultdict(lambda: collections.defaultdict(set))  # (instrument, feed) -> consumer -> files
for name, m in manifests.items():
    for r in m["rows"]:
        u = r.get("instrument_uid")
        if u in bin_uids:
            f = r["feed"] + (f':{r["interval"]}' if r.get("interval") else "")
            grants[(key(u), f)][m["id"]].add(f'{name}@r{m["revision"]}')

demand = yaml.safe_load((repo / "config/v2/stable-crypto-demand.yaml").read_text())
dem = collections.defaultdict(set)
for c in demand["consumers"]:
    for r in c.get("requirements") or []:
        if r.get("venue") == "BINANCE" and r.get("native_symbol") in SYMS:
            f = r["feed"] + (f':{r["interval"]}' if r.get("interval") else "")
            dem[(f'BINANCE:{r["market"]}:{r["native_symbol"]}', f)].add(c.get("consumer_id"))

ing = json.loads(ingestor.read_text())
subs = collections.Counter(b["feed"] for b in ing["bindings"])
acq = yaml.safe_load(acq_path.read_text())
bind_by_id = {b["binding_id"]: b for b in cat["bindings"]}
modes = collections.Counter()
for row in acq.get("bindings") or acq.get("acquisitions") or []:
    b = bind_by_id.get(row.get("binding_id"))
    if b and b["instrument_uid"] in bin_uids:
        modes[(b["feed"], row.get("mode"), row.get("provider_kind"))] += 1

feeds = sorted({f for (_, f) in grants} | {f for (_, f) in dem})
table = {}
for f in feeds:
    per = {}
    for s in SYMS:
        k = f"BINANCE:USD_M:{s}" if any(key(u) == f"BINANCE:USD_M:{s}" for u in bin_uids) else None
        hits = {c: sorted(v) for (kk, ff), cs in grants.items() if ff == f and kk.endswith(":" + s) and ":SPOT:" not in kk
                for c, v in cs.items()}
        per[s] = {"granted_to": sorted(hits), "demanded_by": sorted({c for (kk, ff), cs in dem.items()
                                                                     if ff == f and kk.endswith(":" + s) and ":SPOT:" not in kk for c in cs})}
    table[f] = per
result = {"schema": "stageb.binance-feed-census.v1", "symbols": SYMS, "manifests_read": sorted(manifests),
          "selector_manifests": {n: m["selectors"] for n, m in manifests.items() if m["selectors"]},
          "binance_instruments": sorted(key(u) for u in bin_uids),
          "by_feed": table, "ingestor_subscriptions_by_feed": dict(subs),
          "acquisition_modes": {f"{a}|{b}|{c}": n for (a, b, c), n in sorted(modes.items(), key=str)}}
out.write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
print(json.dumps({f: {s: v["granted_to"] for s, v in per.items()} for f, per in table.items()}, indent=1))
