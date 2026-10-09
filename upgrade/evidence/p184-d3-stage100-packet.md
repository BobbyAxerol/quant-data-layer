# P18.4 D3: Stage-100 Measurement Packet

Status: PREPARED, NOT RUN. Each window needs its own owner approval. Owner decisions 2026-10-09 (journal "Owner decisions
for the DL closure"):
- G4/G5 baseline rule: G4 <= baseline p95 x 1.2; G5 uses baseline p95 x 1.5 for shapes without a stage-50 target.
- The load comes from a synthetic driver with real SDK sessions.
- Live/sandbox realms stay unchanged.

## Tools (committed, tested locally)

- `scripts/phase3_consumer_load_acceptance.py --mode target [--budget <stage-100 budget>]`. It is the certified stage-50
  driver. With `--budget` it runs stages 20/50/100 (600/600/1800 s), 40/30/20/10 classes at 100, and adds G5 COLD gates
  (f831571).
- `scripts/p184_g4_observer.py`: read_committed assigned reader with no group join and no commit, all symbols/feeds
  (a78696a). `--summarize` gives per feed/venue the visibility upper bound and venue->DL receipt.
- Identities: the alpha platform identities `alpha.binance.paper.stable` / `alpha.okx.paper.stable`, set
  `~/.local/state/qdl-v2/r135-local-batch-fair-e905747-20260920T135150Z/identities`.
  - TLS chain verified against the deployed query-client CA bundle; expiry 2026-11-20 / 2026-12-19.
  - JWT key ids `stable-alpha-{binance,okx}-rs256-v1` are present in the deployed jwt-config.
- Client containers: driver default 512 MiB / 1 CPU per worker, up to 4 workers, `--rm`. Image
  `qdl-v2-python:2.2.2-d6d2637` (sha256:514506122111), runtime dir `releases/v2.2.0-02cd827/runtime` read-only.

## Window A - baseline on the current runtime (no config change)

1. G4 observer, `--mode canonical`, all feeds, 4 x 120 s windows, `--sample-ms 1000`, `--max-records 10000`, output
   `~/.local/state/qdl-v2/p184-d3-baseline-<date>/g4-<n>/`. Load: one assigned consumer inside query_kn_1 for 8 min.
2. Driver stage 50 with the v2.1.1 budget (`--mode target --sessions 50 --duration-seconds 300`). It is the certified
   stage-50 workload on the deployed alpha quotas (2400 RPM / 60 streams fit stage 50), with real data and the TS writer
   watched.
3. Freeze `config/v2/p184-stage100-budget.json` from these results, committed BEFORE any candidate load:
   - G5 COLD_2500/COLD_5000 per venue = baseline max x 1.5 (n = 1 per venue/rows, small-sample rule);
   - the latency classes keep the certified stage-50 targets;
   - TS demanded routes 64;
   - the G4 baseline table is attached.

Stop: any Query/Stream/projector restart or OOM, TS writer not READY, 429 on the TS identity, disk < 20 GB free.
Rollback: none needed (no config change); stop the driver/observer.

## Window B - capacity configuration (separate approval)

1. Alpha manifests `alpha.binance.paper.stable` and `alpha.okx.paper.stable`: revision 14 -> 15, `requests_per_minute`
   2400 -> 4950, `max_streams` 60 -> 300 (capacity100 candidate). Same rendering method as D1: new realm dir copied from
   the active rev11 dir, Query manifest list paths for the two alpha files, new gateway bundle with parity proof, no other
   change.
2. Stream `QDL_KN_MAX_SUBSCRIPTIONS` 384 -> 1024 on stream_kn_1/2. MemoryPlan for 1024 + 32 replay is 307 MiB modeled,
   under the unchanged 512 MiB cap; this is not a measured RSS claim.
3. Readers are recreated one at a time (same images), as in the D1 window. No alpha caller runs today. Before Phase E an
   alpha must mint JWT revision 15 (documented hand-off).

Rollback: the rev11 realm dir and the 384 env, per reader (saved composes).

## Window C - ramp 20 / 50 / 100 (separate approval)

`--mode target --budget config/v2/p184-stage100-budget.json --sessions {20,50,100}` with durations 600/600/1800 s. The
final stage 100 includes the BURST / SLOW_READER / RECONNECT windows. The G4 observer runs during stage 100. The driver
monitor samples CPU, throttling, memory and restarts of every DL role plus the clients (G14).

Stop at the first: internal errors, unbounded queue growth, an unsafe execution eligibility, TS writer not READY 64/64,
Query/Stream/projector OOM or restart, a 429 on the TS identity, disk < 20 GB. Each stage must pass before the next.

## Open question for the owner

The target driver implements the realistic (mixed) profile. The capacity100 stress profile (500 alpha streams) is not
implemented in it. Options: a driver extension plus a stress window (more work), or the mixed profile plus the final
fault windows as acceptance and stress reported only through the planner arithmetic.
