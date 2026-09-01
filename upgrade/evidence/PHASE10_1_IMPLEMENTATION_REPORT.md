# Phase 10.1 Implementation Report

**Status:** `PASS - SOURCE / ISOLATED TEST / READ-ONLY PROVIDER ADMISSION`
**Date:** 2026-08-24
**Runtime impact:** none. No V1/V2 service was started, restarted, recreated or
reconfigured. No authority record, Kafka offset, Redis/SQLite state, alpha
configuration, order, database row or provider payload was written.

## Delivered Scope

- Added `qdl.demand.v1`, a versioned provider-neutral control-plane Protobuf
  contract with generated Python and Rust bindings. It is intentionally
  separate from `qdl.query.v2`: `qdl.demand.v1` captures unresolved consumer
  intent; `qdl.query.v2` remains the resolved read/query contract.
- Added strict universe registry and manifest parsing, canonical selector
  resolution, capability truth, per-instrument resolved identity, priority
  merging, owner lease TTL/release/reactivation and fail-closed lifecycle
  transitions.
- Added a dynamic topology planner. Subscription shard membership is data in a
  fixed shared role; it never creates a container or image per symbol.
- Added a source-only bridge from selected resolved bindings to the existing
  fixed core worker group. Applying dynamic WebSocket subscriptions to a live
  runtime is deliberately deferred to Phase 10.3.
- Corrected VN identity to the catalog truth: `HNX/VN_DERIVATIVES/FUTURE`, not
  the legacy shorthand `HNX/DERIVATIVES`. `BINANCE/SPOT` now also has its own
  explicit capability profile rather than borrowing USD-M semantics.
- Added a bounded provider diagnostic for every currently declared crypto
  slice. It validates semantic positive price/quantity/volume, identity,
  timestamps and closed-bar finality, then retains only payload SHA-256 and
  timing metadata in its report.

## Test Evidence

All deterministic commands used disposable `docker run --rm` containers. Where
app code needs a file logger, `/app/logs` was a container tmpfs; source remained
read-only and no host log was written.

| Gate | Result |
|---|---|
| `tests.test_phase10_universal_demand` | 11 passed |
| Buf format, lint, breaking against Phase 1 and Phase 7 baselines | pass |
| Rust `cargo fmt --check` | pass |
| Rust `cargo test -p qdl-contracts -p qdl-venue-core --locked` | 2 + 34 passed |
| Rust clippy (`-D warnings`) | pass |
| Catalog/deployment/V2 API/stream/SDK targeted regression | 75 passed |
| V1 golden/client/demand-reliability/stream/alpha targeted regression | functional cases passed; logger-dependent OpenAPI/control-plane retry with tmpfs: 7 passed |
| Final combined isolated Python gate | 138 passed |
| `phase10_real_provider_admission.py` | 18/18 real Binance/OKX public REST slices passed |

The first V1 retry under a completely read-only root reported only
`/app/logs/app.log` creation failure before the logger-dependent import. It was
a test harness filesystem constraint, not a source failure. Re-running the
exact affected tests with a disposable `mode=1777` tmpfs at `/app/logs` passed.

## Real Provider Admission

The admission invoked no Data Layer service and no authenticated/private
provider endpoint. It read public provider REST responses directly and wrote no
output file. The tested manifest slices were:

- Binance Spot BTCUSDT: `TRADE`, `QUOTE`, final `BAR 1m`.
- Binance USD-M BTCUSDT and ETHUSDT: `TRADE`, `QUOTE`, final `BAR 1m`.
- OKX Spot BTC-USDT: `TRADE`, `QUOTE`, final `BAR 1m`.
- OKX Swap BTC-USDT-SWAP and ETH-USDT-SWAP: `TRADE`, `QUOTE`, final `BAR 1m`.

Result: `REAL_PROVIDER_READ_ONLY`, 18 slices, 0 production writes. The terminal
output contained only slice identity, provider/receive time and payload hash;
no raw market payload was persisted in this repository.

## Boundary And Next Gate

Phase 10.1 closes only the universal demand contract/topology foundation. It
does **not** make V2 primary, start a Rust core, modify provider subscriptions,
or migrate any Trading System/alpha consumer.

VN provider capability and canonical identity are represented correctly, but
VN is not primary-eligible until a separate open-session authenticated provider
admission observes fresh data. Historical warmup/batch coalescing is Phase
10.2; real-time dynamic adapter application, reconnect/resync and V2 primary
routing are Phase 10.3 and Phase 10.5 respectively.
