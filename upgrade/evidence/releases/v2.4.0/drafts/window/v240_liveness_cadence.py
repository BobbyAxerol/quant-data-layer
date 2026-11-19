"""DL v2.4.0 window 3 gate (READ-ONLY): provider session-liveness write cadence, run inside a Query container.

Purpose: the P3 ingestor configs lower `session_liveness_write_interval_ms` 1000 -> 250 (v2.3.2 SessionAlive).
Samples the RO `/session-liveness/<venue>` lane files for 6 s and prints distinct `updated_at_ns` per second per live
file (updated in the last 10 s). Baseline 2026-11-19 at 1000 ms: median about 1.17/s.
Usage: docker exec -i <query container> python3 -B - <venue: binance-usdm|okx-swap> < this file
"""
import json, os, sys, time, statistics
venue = sys.argv[1]; root = f"/session-liveness/{venue}"; seen = {}
end = time.time() + 6
while time.time() < end:
    for f in os.listdir(root):
        if not f.endswith(".json"): continue
        try: d = json.load(open(os.path.join(root, f)))
        except Exception: continue
        if time.time_ns() - d.get("updated_at_ns", 0) > 10e9: continue
        seen.setdefault(f, set()).add(d["updated_at_ns"])
    time.sleep(0.05)
rates = {f: round(len(v) / 6, 2) for f, v in sorted(seen.items())}
print(venue, "live_files", len(rates), "median_writes_per_s", statistics.median(rates.values()) if rates else 0, json.dumps(rates))
