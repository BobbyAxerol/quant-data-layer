# Phase 2 Implementation Report

## Conclusion

Phase 2 is complete in **dark/shadow mode with no V1 cutover**. The selected
Binance USD-M BTCUSDT/ETHUSDT trade slice now has a transport-neutral durable
contract, bounded SQLite WAL bridge, restartable raw-to-canonical pipeline,
idempotent Redis compatibility projector and deterministic Rust foundation.

This result certifies the migration foundation, not a broad-universe or HA
production backbone. `data_layer_service`, `redis_marketdata`, `/v1`, SDK V1
and all current Redis channels/keys remain authoritative and unchanged.

## Implemented

- Added portable `EventSink`, `EventSource`, batch append, logical cursor,
  checkpoint, retry classification and partition-key contracts without leaking
  Redis or Kafka identifiers.
- Added a SQLite WAL bridge with `synchronous=FULL`, atomic batches,
  deterministic IDs, immutable payload checksums, startup/replay corruption
  detection and monotonic per-partition offsets.
- Enforced fail-closed bounds for records, payload bytes, event size, physical
  storage, free-disk reserve, partitions, consumer checkpoints and quarantine.
- Added consumer TTL, replay retention, cursor-expiry behavior, idempotent retry,
  collision detection, quarantine metadata and explicit `DEGRADED/BLOCKED`
  publisher states. No accepted trade is silently dropped.
- Added raw-first durable acceptance and crash recovery. Canonicalization and
  Redis projection are replayable and idempotent; canonical events retain their
  durable raw-event reference.
- Added exact Decimal/time/event-ID canonicalizers for Binance USD-M and OKX,
  checked-in golden bytes and an OKX protocol simulator covering REST envelope,
  subscribe acknowledgement, ping/pong, snapshot/update, sequence gap,
  connection generation and maintenance reset.
- Added Rust contract/core crates for exact decimal, canonicalization, broker
  traits, bounded queue policy, rate limits, jittered backoff, fencing lease,
  supervisor state and deterministic replay tooling. Production Rust remains
  pinned to `1.82.0` and forbids unsafe code.
- Added an isolated Redis replay smoke and Lua projector. Redis is latest-state
  and V1 compatibility projection only; it is reconstructable from canonical
  durable records.
- Added an immutable, digest-pinned Rust fixture image, CI gates, ADR 0006,
  example disabled shadow configuration and an operator runbook.

## Verification

| Gate | Result |
|---|---|
| Full Python regression | 146 run: 143 passed, 3 environment-gated skips |
| Phase 2 focused suite | 20 tests passed |
| Rust fmt/clippy/tests | PASS, 9 tests, zero warnings |
| Buf contract gate | format/lint/breaking/codegen diff PASS |
| Rust dependency policy | advisories/bans/licenses/sources all PASS with `cargo-deny 0.20.2` |
| Redis recovery | restart, AOF persistence, flush and replay rebuild PASS |
| Cross-language golden | Binance and OKX exact bytes/checksums PASS |
| Immutable artifact | two builds produced identical image ID |
| Read-only live shadow | 2 V1 reads, 2 raw, 2 canonical, 0 production writes |
| Runtime isolation | V1 service and Redis both stayed running with restart count 0 |
| Cleanup | no `qdl_phase2_*` container/network/key remained |

The 10,000-event durability benchmark used 10 partitions, 8 consumer groups,
512-byte payloads and atomic batches of 100:

- durable append: 1,470.85 events/s;
- append p50/p95/p99/p99.9: 37.52/57.42/62.28/63.95 ms per fsynced batch;
- replay: 8,211.74 events/s;
- duplicate retry: 1,156.00 events/s;
- 80 durable checkpoint rows: 1,666.28 writes/s;
- max RSS: 33,004 KiB;
- disk amplification: 2.072x;
- all configured acceptance gates passed.

Compact machine evidence is in
[`phase2-verification.json`](phase2-verification.json),
[`phase2-performance.json`](phase2-performance.json) and
[`phase2-live-shadow-smoke.json`](phase2-live-shadow-smoke.json).

## Decisions And Remaining Gates

- Kafka-compatible transport remains the long-term target, but it is **not
  provisioned or promoted**. Current evidence does not justify its operational
  cost for this two-symbol shadow slice. Promotion requires the explicit trigger
  and approval in ADR 0006.
- SQLite is single-host transitional infrastructure. It is not HA and is not
  approved for broad trade/book authority.
- The bounded benchmark retained 10,100 records, equal to about 101 seconds at
  100 events/s. That is an explicit capacity result, not a 24-hour replay claim;
  Phase 3 sustained source-rate evidence decides whether the configured spool
  can meet its horizon or whether the Kafka promotion trigger has fired.
- Rust and the separated services remain dark. Phase 3 must establish sustained
  demand-backed ingestion and V1 projection parity before any authority flag is
  changed.
- A read-only audit of the existing Python image found known advisories in
  pre-existing V1/build dependencies, including the currently constrained
  PyArrow line and vnstock/tooling transitives. Phase 2 introduced no Python
  dependency. Remediation needs a separate compatibility-tested dependency/image
  upgrade before production promotion; this finding is not hidden by the Rust
  security PASS.
- OpenTelemetry export and multi-node broker failover are later production
  certification gates; Phase 2 only establishes the instrumentation and replay
  interfaces.

## Rollback

No production rollback is required because no running path changed. Remove a
dark test deployment by stopping its shadow process, draining checkpoints,
verifying the final checksum and deleting only its isolated spool and
`shadow:qdl:v2` namespace according to ADR 0006.
