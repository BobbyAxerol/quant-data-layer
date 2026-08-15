# Phase 8.1 Raw Envelope And Rust Core Report

Date: 2026-08-15  
Decision: PASS for provider-neutral shadow core; no authority change

## Contract

- Added `qdl.provider.v1.RawProviderEnvelope` and `QuarantineRecord`.
- Exact raw bytes and SHA-256 are bound to an explicit pre/post decompression,
  SDK-delivery or replay capture boundary.
- Session ID, connection generation, lease epoch, authority revision and
  partition-plan epoch are mandatory and independent.
- EventEnvelope additions use new field numbers 28-32. Both frozen Buf breaking
  baselines pass; V1 and Phase 7 beta fields are unchanged.
- Python and Rust serialize the same 256-byte raw-envelope golden with SHA-256
  `2b67e5169171702c6c0352ff838473abc668a10f74624af6dc55d6302823236b`.

## Core Behavior

- Full session transition and subscription ACK/reject tracking.
- Heartbeat/read deadlines and bounded reconnect budget.
- Old connection generation and invalid/expired lease rejection.
- Duplicate, out-of-order, gap, source-session reset and stale-session decisions.
- Lossless events backpressure/spool/disconnect; latest state and in-progress
  bars can coalesce deterministically. Final bars remain lossless.
- Stable `sha256-rendezvous-v1` assignment with explicit plan epoch.
- Machine-readable capability boundaries for Binance, OKX, DNSE/VN and
  fixture-only Deribit options.

## Verification

| Gate | Result |
|---|---|
| Buf format/lint | passed |
| Buf breaking vs Phase 1 baseline | passed |
| Buf breaking vs Phase 7 beta baseline | passed |
| Python raw/canonical regression | 17 passed |
| Rust fmt/clippy | passed, warnings denied |
| Rust workspace tests | 24 passed |
| Missing required provider fields | fail closed |
| Raw hash mismatch | fail closed |
| Stale generation / wrong lease | fail closed |
| Sequence duplicate/gap/reset | deterministic |
| Full lossless queue + full spool | disconnect, no silent drop |
| Add one instrument | zero existing assignment churn |
| Add third owner over 10,000 instruments | 33.6% bounded churn |

## Boundary

This subphase certifies contracts and deterministic core behavior. Authentic
exact-frame Python/Rust shadow parity, cross-venue captures, concurrent replay,
capacity and soak belong to 8.2. No Rust event writes a public endpoint, legacy
Redis key/channel or authoritative feed.
