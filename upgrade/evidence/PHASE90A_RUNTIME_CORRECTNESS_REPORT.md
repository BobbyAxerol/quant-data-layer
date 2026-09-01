# Phase 9.0-A Runtime Correctness Closure Report

Date: 2026-08-18
Branch: `feat/phase9-runtime-correctness`
Commits: `765a7f1`, `7302c45`
Decision: `PASS_ISOLATED_NO_PRODUCTION_CUTOVER`

## Scope And Safety Boundary

Phase 9.0-A closed the runtime correctness defects found after migration without
restarting or mutating the running V1 service. The candidate used a dedicated
Compose project, Redis instance, loopback port, networks and volumes. It held no
canonical authority and was removed after evidence capture.

The public V1 surface remains unchanged. Live and candidate OpenAPI each exposed
40 paths, with zero additions and zero removals.

## Implemented

- Split transport connection from source data readiness and report TRADE/KLINE
  independently per source and shard.
- Require a valid provider frame before readiness. Subscription ACK, malformed
  payload and wrong-feed payload cannot make a shard healthy.
- Add first-frame and idle watchdogs with typed outage counters and jittered,
  bounded reconnect backoff. A data outage survives transport reconnect and is
  cleared only by a valid provider frame.
- Replace drop-oldest queue behavior with bounded backpressure. Queue pressure is
  observable and a sustained full queue reconnects the source instead of
  silently deleting an earlier event.
- Add one demand-only Binance USD-M closed-kline recovery manager with TTL
  ownership, bounded concurrency, per-feed backoff, final-bar validation,
  symbol/interval/open-time deduplication and explicit
  `BINANCE_REST_GAP_FILL` provenance.
- Preserve the provider interval from `k.i`; recovered 5m or other supported bars
  cannot be projected into a legacy 1m key.
- Preserve all gap rows in a publisher batch while retaining latest-state
  coalescing for ordinary trade/kline updates.
- Keep existing V1 health response keys while fixing the TRADE/KLINE booleans and
  adding source/recovery detail under the existing nested payload.
- Add an immutable candidate Compose boundary: UID/GID 10001, read-only root,
  no source bind, dedicated writable data/log volumes, loopback-only ingress,
  dropped capabilities and explicit CPU/RAM/PID limits.

## Verification

| Gate | Result |
|---|---|
| Targeted runtime/demand/watchdog tests | 35/35 pass |
| Full repository tests | 345 run: 340 pass, 5 environment-gated skips, 0 fail |
| Python compileall and diff check | Pass |
| V1 live-vs-candidate OpenAPI paths | 40/40, added 0, removed 0 |
| Candidate identity | `10001:10001` |
| Read-only root / no source bind | Pass / pass |
| Limits | 1.5 CPU, 1.5 GiB RAM, 256 PIDs |
| Candidate cleanup | 0 containers, networks, volumes and images remain |
| Production V1 restart/state mutation | None |

## Real Provider Evidence

Binance USD-M TRADE produced valid frames on all 8 shards. The provider accepted
all 8 KLINE connections but produced zero valid kline frames, so the candidate
correctly reported KLINE unavailable and top-level health `degraded`. Transport
reconnect never made the source green again.

A BTCUSDT 1m API read created one TTL demand lease. Recovery fetched a fully
closed Binance REST bar, projected it with finality and provenance, and a direct
Binance REST query for the same timestamps matched open time, close time and all
OHLCV fields exactly. No generated or substituted market data was used.

After lease expiry, demand and active recovery counts returned to zero. Provider
fetch count remained 2 across the following poll. Candidate queue drop and
pressure counts were both zero.

A single resource snapshot while broad TRADE and unavailable KLINE shards were
active measured approximately 20.07% of one CPU and 131 MiB for the app, plus
2.27% CPU and 5.129 MiB for isolated Redis. These are bounded by Compose; this
was a correctness smoke, not a long soak or capacity certification.

## Defects Caught Before Release

1. The pinned Redis image could not call `setpriv` after `cap_drop: ALL` when no
   user was declared. The candidate now runs Redis directly as `999:999`.
2. A transport reconnect initially reset source startup state, briefly making
   top-level health green while KLINE still had no data. Data-outage state now
   survives reconnect until a valid frame arrives.
3. Multi-interval recovery initially inherited the stream default interval.
   Projection now uses the authoritative payload interval and has a 5m
   regression test.

## Decision And Remaining Boundary

Phase 9.0-A is complete for isolated implementation and acceptance. It is ready
for review and a separately approved controlled rollout. The currently running
V1 container remains `data-layer:v0.1.0` and therefore still has legacy health
semantics until that rollout occurs.

Phase 9.1 remains blocked. This phase does not provide production Kafka/OTel,
workload identity, external secret rotation, signed-image admission,
independent-failure-domain DR, complete consumer registration or exact authority
slice approval.

Machine-readable evidence is in
[`phase90a-runtime-correctness.json`](phase90a-runtime-correctness.json).
