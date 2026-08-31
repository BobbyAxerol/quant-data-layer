# Phase 10.2 Universal Warmup, History And Batch Handoff

Date: 2026-08-24

Branch: `fix/v2-rollout-preflight`

Baseline commit: `62bfa63`

## Status

`IMPLEMENTATION COMPLETE / SOURCE AND ISOLATED CERTIFIED / NO CUTOVER`

The provider-neutral V2 warmup/history and batch handoff implementation is
complete for the approved source-only scope. Binance and OKX demanded BAR
slices passed bounded real-provider admission. V1 contracts and the running
V1/V2 topology were not changed.

This report does not claim production activation. A current authenticated DNSE
open-session admission remains an explicit external acceptance gate. Existing
real DNSE history evidence and deterministic VN session tests are useful but
are not relabelled as a new live-session certification.

## Implemented Domain Behavior

1. A BAR consumer declares exactly one horizon: an exact row count or an exact
   half-open nanosecond time range. Instrument, interval, source policy, cache
   age and deadline are part of the typed requirement.
2. The demand resolver coalesces compatible declared consumers. The universal
   planner chooses a native interval first, otherwise the largest exact native
   divisor, and creates bounded provider/session-aware chunks.
3. Provider history accepts only consecutive already-closed bars. It rejects a
   short page, overlap, conflicting duplicate, pagination stall, source gap,
   unfinished bar or wrong interval instead of returning partial `FULL` data.
4. Every physical warmup page consumes a provider token and concurrency slot.
   The request deadline covers token waiting, singleflight waiting and fetch
   timeout. A timed-out background socket keeps its slot until the worker truly
   exits, preventing hidden concurrency growth.
5. Single and batch V2 warmups use the same bounded executor, retry, jitter,
   `Retry-After` and circuit policy. Slow provider work runs outside the API
   event loop. Batch identity, order and cardinality remain explicit.
6. The closed-bar cache is bounded and keyed by instrument, target interval,
   closed boundary, source policy and source interval. Age is enforced and a
   shorter cached window never answers a larger request.
7. Exact resampling accepts only final/revised canonical constituents from one
   instrument/source and volume unit. It records constituent count, first/last
   watermark and a deterministic lineage digest.
8. Python SDK handoff seeds a bounded FIFO, appends the newest final candle
   before strategy evaluation, calls the strategy once per new boundary and
   fails closed on a gap or late bar. There is no deliberate one-bar delay.
9. Batch and real-provider evidence report nearest-rank p50/p95 rather than a
   lower order statistic that can hide the slowest sample in a small batch.

## Contract And Compatibility Evidence

- V1 and V2 frozen OpenAPI tests passed. The semantic comparison found 10
  operations and 46 schemas, with no removed operation, response, schema or
  enum; no security change and no newly required legacy parameter.
- Buf format and lint passed. Breaking checks passed against both
  `baseline/qdl-v2-phase1.binpb` and `baseline/qdl-v2-phase7-beta.binpb`.
- Generated Python and Rust demand/query bindings were regenerated from the
  checked-in Protobuf source and contain only expected contract changes.
- Rust workspace `cargo fmt --check`, strict workspace clippy with
  `-D warnings`, and all workspace tests passed: 100 tests, 0 failures.
- The standalone SDK wheel contains no service internals and imports outside
  the repository:
  - artifact: `qdl_sdk-2.0.0-py3-none-any.whl`;
  - SHA-256: `9211485f1db927118cefa5c79b1fe2f87c7f3b9257288e32997f4b8d39d3d578`;
  - Python requirement: `>=3.10`;
  - smoke: `SDK_STANDALONE_IMPORT_OK /site/qdl_sdk/__init__.py 2.0.0`.

## Deterministic Test Matrix

Focused Phase 10.2 suite:

```text
python -m unittest tests.test_phase10_universal_warmup
Ran 41 tests in 1.013s - OK
```

The focused cases cover:

- typed and legacy contract identity;
- exact rows and time-range bounds;
- native, non-native exact resample and Monday-anchored weekly boundaries;
- VN lunch/session-aware planning and in-session missing-bar detection;
- compatible-demand coalescing and ambiguous-horizon rejection;
- bounded chunking, provider fairness, concurrency and singleflight;
- caller cancellation, deadline, retry, `Retry-After` and circuit opening;
- page-level token budgeting and typed deadline exhaustion;
- timed-out worker slot ownership and singleflight-key cleanup;
- final cutoff, partial pages, source gaps, cache age and overlapping windows;
- FIFO length, duplicate/revision behavior and immediate sync/async callback;
- SDK warmup-to-stream handoff, wide-universe batching and explicit partial
  result behavior;
- nearest-rank percentile accuracy and bounded admission provenance.

Targeted API/provider regression after the final reliability changes passed 56
tests with no failure. The final repository-wide isolated Python gate was:

```text
python -m unittest discover -s tests -t .
Ran 812 tests in 36.198s - OK (skipped=6)
```

The six skips are predeclared environment-dependent integration cases; there
were no unexpected skips or failures. Fault-injection tests intentionally emit
warning/error logs for timeout, provider failure, queue exhaustion and recovery
paths and asserted the expected fail-closed behavior.

## Real Provider Evidence

Command scope: read-only source mount, disposable container/tmpfs, five final
1m rows per currently demanded crypto BAR slice, no output file and no raw
payload persistence.

All six demanded slices passed:

1. Binance Spot `BTCUSDT`.
2. Binance USD-M `BTCUSDT`.
3. Binance USD-M `ETHUSDT`.
4. OKX Spot `BTC-USDT`.
5. OKX Swap `BTC-USDT-SWAP`.
6. OKX Swap `ETH-USDT-SWAP`.

Final bounded measurements:

| Metric | Result |
|---|---:|
| Slices | 6/6 |
| Final canonical bars | 30/30 |
| p50 | 97.040 ms |
| p95 nearest-rank | 137.834 ms |
| Process CPU | 0.113 s |
| Maximum RSS | 76,616 KiB |
| Provider source calls | 6 |
| Provider source rows | 30 |
| Cache hits / misses | 6 / 12 |
| Retry / circuit rejection | 0 / 0 |
| Provider 429 / 5xx / failure | 0 / 0 / 0 |
| Provider budget waits | 0 |
| Production writes | 0 |
| Raw payload persisted | false |

Every slice matched the provider-envelope and typed V2 BAR boundaries, carried
raw payload hash lineage, was final and complete, and remained explicitly
non-authoritative/non-execution-eligible because this was provider
pass-through evidence rather than a materialized primary replay path.

## VN Evidence Boundary

- Deterministic tests prove Asia/Ho_Chi_Minh session boundaries, lunch break,
  exact range counting, gap detection and session-aware chunking.
- Existing repository evidence records a real DNSE `VN30F1M` session with 241
  expected bars and zero missing, outside-session or fabricated rows.
- The market was closed during this Phase 10.2 final run. Therefore a new
  authenticated open-session admission was not claimed. That external gate
  must pass before a VN route is promoted; it does not block Binance/OKX source
  implementation or change V1 authority.

## Runtime Impact And Cleanup

- No running service/container was restarted, recreated or reconfigured.
- No authority, consumer route, alpha config, Kafka offset/topic, Redis key,
  SQLite state, provider credential or production database row was changed.
- All test containers used `--rm`; test writes were limited to bounded `/tmp`
  artifact directories and container tmpfs. The four named Phase 10.2 `/tmp`
  directories were removed after certification; no matching container or
  Docker volume remained.
- No image prune or broad cleanup command was run.
- No provider payload was written to evidence. Only aggregate metrics,
  identifiers and digests are recorded.
- V1 remains the rollback path. Phase 10.2 authorizes no consumer cutover.

## Decision

The Phase 10.2 implementation and isolated certification are complete. The
next implementation phase requires separate operator approval. Production
promotion still requires the consumer-manifest/cutover gates in later phases,
and VN remains fail-closed until its current open-session evidence is accepted.
