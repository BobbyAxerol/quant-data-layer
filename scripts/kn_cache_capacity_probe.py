#!/usr/bin/env python3
"""TEST_ONLY allocator capacity probe, never provider/canonical certification.

Fills an EMPTY dedicated Redis using the real bucket/index layout and padded
real codec row sizes. Full catalog caps include disabled legacy products as a
conservative upper bound. No Kafka, spool or production client is supported.
"""
from __future__ import annotations
import argparse
import base64
import json
from pathlib import Path
import time
import redis

from qdl.marketdata.v2 import market_data_pb2
from qdl.runtime.kn_bar_readback import BUCKET_OPENS, binding_product_key
from qdl.runtime.stable_catalog import StableSourceCatalog

ROOT = Path(__file__).resolve().parents[1]
URL = "redis://kn5-close-cache:6379/0"
PREFIX = "kn3:kn5-capacity-test:"


def sample_sizes():
    sizes = {}
    for record in json.loads((ROOT/"contracts/golden/kn_v220/state_codec.json").read_text())["records"]:
        if record["synthetic"] or not record["tag"].startswith("bar|"):
            continue
        payload = base64.b64decode(record["canonical_b64"])
        env = market_data_pb2.EventEnvelope.FromString(payload)
        # Test-only padding guards longer future native identities / decimals.
        sizes[env.venue] = max(sizes.get(env.venue, 0), len(payload) + 56 + 32)
    return sizes


def snapshot(client):
    info = client.info("memory")
    return {k: info[k] for k in ("used_memory", "used_memory_rss", "used_memory_peak",
        "allocator_frag_ratio", "mem_fragmentation_ratio", "maxmemory", "mem_clients_normal")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=12064)
    args = parser.parse_args()
    if not 1 <= args.rows <= 12064:
        raise ValueError("bounded retention only")
    client = redis.Redis.from_url(URL, socket_timeout=15)
    if client.dbsize():
        raise RuntimeError("refusing non-empty test Redis")
    config = client.config_get("hash-max-listpack-*")
    if config["hash-max-listpack-entries"] != "128" or config["hash-max-listpack-value"] != "2048":
        raise RuntimeError("candidate listpack config mismatch")
    catalog = StableSourceCatalog.load(ROOT/"config/v2/stable-source-bindings.yaml")
    bars = [b for b in catalog.bindings if b.feed.value == "BAR"]
    sizes = sample_sizes()
    memory_before = snapshot(client)
    start = time.monotonic()
    expected_keys = expected_rows = 0
    key_samples = []
    def product(binding, generation):
        nonlocal expected_keys, expected_rows
        lpk = binding_product_key(binding, "kn5-capacity-test").encode()
        size = sizes.get(binding.instrument.identity.venue, max(sizes.values()))
        payload = b"TEST_ONLY_ALLOCATOR_NOT_CANONICAL|".ljust(size, b"x")
        step = 60000  # identical field width; not a provider calendar simulation
        base = 1800000000000 // (BUCKET_OPENS * step) * (BUCKET_OPENS * step)
        pipe = client.pipeline(transaction=False)
        for offset in range(0, args.rows, BUCKET_OPENS):
            opens = [base+i*step for i in range(offset, min(args.rows, offset+BUCKET_OPENS))]
            suffix = f"{generation}:{lpk}:{opens[0]//(BUCKET_OPENS*step)}"
            bk = PREFIX+"b:"+suffix
            pipe.hset(bk, mapping={str(o):payload for o in opens})
            pipe.hset(PREFIX+"bd:"+suffix, mapping={str(o):"N" for o in opens})
            pipe.set(PREFIX+"bs:"+suffix, json.dumps([1,step,len(opens),[[str(opens[0]),str(opens[-1])]],[]]))
            expected_keys += 3
            expected_rows += len(opens)
            if offset == 0:
                key_samples.append(bk)
            if offset // BUCKET_OPENS % 8 == 7:
                pipe.execute()
        pipe.hset(PREFIX+f"bm:{generation}:{lpk}", mapping={"first":base,"last":base+(args.rows-1)*step,"rows":args.rows})
        pipe.hset(PREFIX+f"ptr:{lpk}", mapping={"ready":1,"fence":1})
        pipe.execute()
    for i,binding in enumerate(bars):
        product(binding, 1)
        if (i+1) % 50 == 0:
            print(json.dumps({"products":i+1,"rows":expected_rows,"used":snapshot(client)["used_memory"]}),flush=True)
    steady = snapshot(client)
    args.output.write_text(json.dumps({"stage":"FULL_STEADY", "steady":steady,
        "test_only":True, "products":len(bars), "rows":expected_rows,
        "padded_value_bytes_by_venue":sizes}, indent=2)+"\n")
    # Two Stage-B owners: at most one product each staged/reclaimed. Reserve
    # TWO generations per owner to include not-yet-reclaimed previous view.
    for generation in (2,3):
        for b in sorted(bars, key=lambda b:sizes.get(b.instrument.identity.venue,0), reverse=True)[:2]:
            product(b,generation)
    time.sleep(2)
    peak = snapshot(client)
    stats = client.info("stats")
    encodings = {client.object("encoding",key).decode() for key in key_samples}
    assert encodings == {"listpack"}, encodings
    assert stats["evicted_keys"] == 0
    result = {"schema":"qdl.kn5.allocator-capacity.v1","test_only":True,
        "provenance":"synthetic layout fill using padded real canonical row size upper bounds",
        "canonical_data_written":False,"products":len(bars),"rows_per_product":args.rows,
        "steady_rows":len(bars)*args.rows,"staging_products":4,
        "padded_value_bytes_by_venue":sizes,"bucket_opens":BUCKET_OPENS,
        "before":memory_before,"steady":steady,"with_staging":peak,
        "evicted_keys":stats["evicted_keys"],"encodings":sorted(encodings),
        "seconds":round(time.monotonic()-start,3),"all_keys":client.dbsize(),
        "limitations":["allocator/layout capacity only, not native Kafka replay or market correctness",
                        "future canonical payloads over the measured padded bound require resizing",
                        "no provider history was invented or served"]}
    args.output.write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps(result),flush=True)


if __name__ == "__main__":
    try:
        main()
    except redis.exceptions.RedisError as error:
        # redis-py includes every command/value in pipeline errors. Never dump
        # megabytes of fixture payload into the operator log.
        print(json.dumps({"status":"FAIL", "error_type":type(error).__name__}), flush=True)
        raise SystemExit(1) from None
