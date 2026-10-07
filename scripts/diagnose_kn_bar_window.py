#!/usr/bin/env python3
"""Read-only P18.4 retained-window diagnostic for existing KN 15m/30m bindings.

Uses the production generation/fence-checked reader; never repairs or publishes.
An empty/missing generation is a finding, never zero gaps. Does not certify missing
history before/after the retained window. No credential or payload is reported.
"""
from __future__ import annotations
import json
import os
import time
from qdl.adapters.intervals import canonical_interval_ms
from qdl.runtime.kn_bar_readback import binding_product_key
from qdl.runtime.kn_market_cache import reader_from_environment
from qdl.runtime.stable_catalog import StableSourceCatalog


def diagnose(reader, bindings, topic_id, *, max_rows=12064, seconds=30):
    deadline = time.monotonic() + seconds
    rows = []
    def budget():
        if time.monotonic() >= deadline:
            raise TimeoutError("BAR diagnostic deadline exceeded")
    for binding in bindings:
        if binding.feed.value != "BAR" or binding.interval not in {"15m", "30m"}:
            continue
        row = {"binding_id": binding.binding_id, "interval": binding.interval}
        try:
            budget()
            step = canonical_interval_ms(binding.interval)
            boundary, spans, flags = reader.bar_diagnostic_ranges(
                binding_product_key(binding, reader.environment), step,
                last=max_rows, check_budget=budget)
            if boundary.topic_id != topic_id:
                raise ValueError("source topic identity mismatch")
            if not spans:
                raise ValueError("no retained BAR generation/window")
            missing = sum(max(0, (start - end) // step - 1)
                          for (_, end), (start, _) in zip(spans, spans[1:]))
            row.update(state="PASS" if not missing and not flags else "GAP",
                       missing_opens=missing, flagged_rows=len(flags),
                       retained_rows=sum((end-start)//step+1 for start,end in spans),
                       first_open_ms=spans[0][0], last_open_ms=spans[-1][1],
                       boundary={"topic_id": boundary.topic_id, "partition": boundary.partition,
                                 "offset": boundary.offset},
                       missing_ranges=[[end+step, start-step] for (_,end),(start,_) in zip(spans,spans[1:]) if start-end>step][:20])
        except Exception as error:
            row.update(state="ERROR", error_type=type(error).__name__)
        rows.append(row)
        time.sleep(.025)
    return {"schema": "qdl.p184.bar-retained-window.v1", "writes": 0,
            "scope": "retained window only; no assertion before/after retained bounds",
            "status": "PASS" if rows and all(r["state"] == "PASS" for r in rows) else "FINDINGS",
            "bindings": rows}


if __name__ == "__main__":
    reader = reader_from_environment(os.environ)
    try:
        catalog = StableSourceCatalog.load(os.environ["QDL_STABLE_SOURCE_BINDINGS"])
        report = diagnose(reader, catalog.bindings, os.environ["QDL_KN_TOPIC_ID"])
        print(json.dumps(report, sort_keys=True))
    finally:
        reader.client.close()
