# Phase 3 Implementation Report

## Conclusion

Phase 3 is complete and frozen in **dark/shadow mode**. It establishes
demand-driven ingestion, explicit shard ownership, loss semantics, real
Binance USD-M and OKX V5 adapters, a Rust Binance hot path and fenced V1
compatibility projection without changing the running V1 authority.

The implementation uses provider-authentic bytes in production/shadow code.
Synthetic data is confined to isolated deterministic, load and extension tests
and carries explicit test provenance. No synthetic event is accepted as proof
that a live provider works.

## Implemented

- TTL demand leases, baseline demand, deterministic no-truncation sharding and
  zero-demand Spot behavior.
- PostgreSQL lease acquire/renew/release with monotonically increasing fencing
  epochs and stale-owner rejection.
- Bounded lossless trade/book queues and same-key-only latest-state coalescing
  for BBO/bar.
- Binance USD-M exchange-info discovery, demanded-only streams, exact metadata,
  reconnect/backoff and stop-safe WebSocket supervision.
- A Rust `1.82.0` Binance shadow binary with bounded Tokio channel, real frame
  retention, exact canonical bytes and fsynced isolated WAL.
- OKX V5 endpoint-scoped rate limits, retries, public/business supervisors,
  subscription acknowledgement correlation, heartbeat, reconnect and a
  sequence-valid executable order book.
- Raw-first BBO/bar/book events, deterministic canonical projection, frozen V1
  bar shape, idempotent checkpoints and atomic Redis lease fencing.
- Runtime `SHADOW/CANONICAL/LEGACY` authority routing at feed granularity,
  including Redis integration proof that switch and rollback require no process
  restart.
- A capability-based option/order-book extension boundary whose Deribit-shaped
  fixture is explicitly test-only and cannot imply venue certification.

## Verification

| Gate | Result |
|---|---|
| Full Python/V1 regression | 177 run: 172 pass, 5 expected environment skips |
| Phase 3 focused suite | 29/29 pass |
| Adapter failure cases | malformed, duplicate/idempotence, out-of-order/gap, reconnect storm, inactive/delist and graceful shutdown pass |
| Rust checks | fmt, Clippy `-D warnings`, 11/11 tests pass |
| Canonical contracts | Buf format/lint/breaking/codegen diff pass |
| Cross-language parity | exact Python/Rust trade, BBO and bar golden bytes pass |
| PostgreSQL fencing | exclusive owner, renewal, release, takeover and stale epoch pass |
| Redis recovery | AOF restart, rebuild checksum, stale epoch and authority rollback pass |
| Real provider | Binance trade/BBO/closed bar and OKX trade/book pass; 0 production writes |
| Rust real provider | 3/3 real Binance frames exactly match Python canonical bytes |
| Burst/restart | 20,000/20,000 retained/replayed across 80 partitions; 0 queue rejects |
| Sustained | 500.70 events/s for 5,000 events; p95/p99 157.09/163.60 ms; 0 rejects |
| Runtime isolation | existing Data Layer and Redis restart count remained 0; V1 health `ok` |

The bounded real-provider result contains `20` raw/canonical durable records:
one Binance trade, one BBO, one closed REST bar, one OKX trade, one OKX book
snapshot and five OKX book deltas. It wrote only a disposable spool.

Burst throughput was `1,343.03 events/s`; all `20,000` events survived reopen
and replay, queue high-watermark was `512`, rejection was `0`, and peak traced
Python memory was `1,965,064` bytes. The paced sustained profile held the
requested `500 events/s`, queue high-watermark was `50`, and peak traced memory
was `335,239` bytes.

Compact evidence:

- [`phase3-real-provider-smoke.json`](phase3-real-provider-smoke.json)
- [`phase3-rust-binance-real-parity.json`](phase3-rust-binance-real-parity.json)
- [`phase3-load-recovery.json`](phase3-load-recovery.json)
- [`phase3-sustained-load.json`](phase3-sustained-load.json)
- [`phase3-freeze.json`](phase3-freeze.json)

## Provider Boundaries

- Binance individual trade and BBO WebSockets are live-certified from this
  host. Closed bars are live-certified through the official REST kline source.
  The USD-M kline WebSocket parser has deterministic parity but did not emit
  during bounded network probes and is not claimed as live-certified.
- OKX public trades and `books` snapshot/delta are live-certified. Executable
  continuity uses `seqId/prevSeqId`; the deprecated/fixed checksum is declared
  unavailable rather than treated as a valid CRC. REST `/books` cannot bridge a
  missing WebSocket delta.
- OKX VIP/deep-book channels, historical pagination completeness and future
  Deribit activation are not certified by this phase.

## Freeze And Remaining Gates

The implementation commits `f2ac229` through `50e2cbb` are frozen as the Phase
3 shadow baseline. Its Rust image is
`sha256:206bca9222c2633daae1c293e49a5c90e5d1c1a356a986e24c4369e074cfe5a7`.
Any semantic or contract change requires reopening the phase through an ADR,
new parity evidence and an updated freeze manifest.

This is not a production-authority claim. SQLite remains a single-host bridge;
Kafka-compatible HA promotion is evidence/approval driven. Phase 4 owns quality
ledger, historical/replay completeness, retention, quarantine, failover and
gap-free snapshot/cursor handoff. Phase 6 owns immutable production deployment,
HA/capacity certification and governed authority cutover.

## Rollback

No live rollback is necessary because V1 never stopped being authoritative. A
shadow rollback stops only the selected shard, restores its feed flag to
`LEGACY`, confirms the new fencing epoch and removes only isolated shadow state
after recording its checksum. Production Redis, PostgreSQL, Parquet and running
consumer groups remain untouched.
