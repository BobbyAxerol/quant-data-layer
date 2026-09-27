#!/usr/bin/env python3
"""Account KN serving components from simultaneous cgroup samples.

Never relabel a composed shadow measurement as production-after-cutover proof.
Missing runtime roles remain explicit, even when the subtotal meets the gate.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path

SHARED = tuple("qdl_v2_stable_candidate-"+s+"-1" for s in (
    "kafka1", "kafka2", "kafka3", "rust_core", "rust_core_2", "rust_core_3",
    "ingestor_binance_usdm", "ingestor_okx_swap"))
KN = ("kn4-proj-a", "kn4-proj-b", "kn4-query-1", "kn4-query-2",
      "kn4-stream-a", "kn4-stream-b", "kn4-cache", "kn4-quota", "kn4-edge", "kn4-core")
MISSING = ("qdl_v2_stable_candidate-stable_redis-1",)


def fields(text):
    return {k:int(v) for k,v in (item.split("=",1) for item in text.split())}


def summarize(rows):
    if len(rows)<2 or rows[-1]["t"]<=rows[0]["t"]:
        raise ValueError("non-empty timed sample window required")
    selected = SHARED+KN+tuple(k for k in MISSING if all(k in row["c"] for row in rows))
    for row in rows:
        if set(selected)-row["c"].keys():
            raise ValueError("missing selected serving role")
    for before,after in zip(rows,rows[1:]):
        if after["t"]<=before["t"]:
            raise ValueError("nonmonotonic clock")
        for role in selected:
            if fields(after["c"][role]["cpu"])["usage_usec"] < fields(before["c"][role]["cpu"])["usage_usec"]:
                raise ValueError("counter reset: runtime identity changed")
    seconds=(rows[-1]["t"]-rows[0]["t"])/1e9
    result={role:{"mean_cores":(fields(rows[-1]["c"][role]["cpu"])["usage_usec"]-
        fields(rows[0]["c"][role]["cpu"])["usage_usec"])/1e6/seconds,
        "peak_cgroup_bytes":max(r["c"][role]["mem"] for r in rows)} for role in selected}
    subtotal=sum(v["mean_cores"] for v in result.values())
    missing=[k for k in MISSING if any(k not in r["c"] for r in rows)]
    return {"schema":"qdl.kn5.serving-cpu-accounting.v1", "seconds":seconds,
        "method":"simultaneous shared canonical producers/brokers plus candidate KN readers/projectors/cache/edge",
        "roles":result,"measured_subtotal_mean_cores":subtotal,"gate_mean_cores":5.0,
        "missing_roles":missing,"full_stack_gate":"INCOMPLETE" if missing else ("PASS" if subtotal<=5 else "FAIL"),
        "production_cutover_certified":False,
        "excluded_shadow_overhead":["kn4-mirror","kn4-kafka"],
        "excluded_retired_path":"old Python Query/Stream and SQLite projectors",
        "excluded_consumers":"TS and alpha are clients, not Data Layer serving roles"}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    raw=args.samples.read_bytes()
    result=summarize([json.loads(line) for line in raw.splitlines() if line.strip()])
    result["samples_sha256"]=hashlib.sha256(raw).hexdigest()
    args.output.write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps({k:v for k,v in result.items() if k!="roles"}))


if __name__=="__main__":
    main()
