# Quant Data Layer v2.1.0

## Scope

This release certifies the existing 299-product Binance USD-M/OKX Swap V2
data plane plus actual Trading System adoption of its 60 sealed routes:
BTC/ETH/SOL/DOGE/BNB on both venues, TRADE, QUOTE, final BAR 1m,
MARK_INDEX_PRICE, BOOK_SNAPSHOT and BOOK_DELTA. SDK 2.0.3 / consumer revision
10 / release routing revision 22. No public API/schema change.

## Changes

- Avoid repeated 12,064-offset SQLite retention scans. Dense retained windows
  use an indexed boundary, with foreign-commit/rollback invalidation and an
  exact sparse-history fallback. Identity, retained rows, checksums, cursor
  expiry and durability are unchanged.
- Converge four reader/stream roles on source `1c0844f562ff419a98d4374a838acf7764569219`.
  This includes previously tested SQLite recovery/hydration contention fixes.
  Projector recovery fixes remain on their already-deployed `e4a7377` image.
- Raise only the two existing Stream memory ceilings from 512MiB to 1GiB
  after a measured A/B test. CPU limits remain unchanged. The actual writer
  used about 677MiB, with no direct reclaim during final acceptance.

## Evidence

- 207 selected source tests passed; 123 tests passed on the immutable image.
- Authentic read-plane preflight: all 299 products, both Query replicas.
- Real Trading System: 60/60 routes for 305.120 seconds after 30.081-second
  convergence; 413 SDK reads validated, all 60 routes published by TS,
  no test orders, direct-provider calls or fallback.
- Independent same-projection SDK probe: 240/240 reads passed.
- Existing 299-product C2 cursor/reconnect/fallback proof is inherited by hash;
  unrelated passing provider tests were not repeated after each patch.
- Failed observer setup/API/strict-TRADE probes and the failed 512MiB load
  window remain recorded in the implementation journal; they are not passes.

### Consumer-Call Latency

Measured request start through SDK validation/projection during real TS load.
Each venue/feed bucket has 30-33 steady samples. Report p50/p95, not a p99 SLA.

| Feed | Binance p50 / p95 | OKX p50 / p95 |
|---|---:|---:|
| TRADE | 9.46 / 13.71ms | 8.91 / 15.36ms |
| QUOTE | 9.58 / 12.97ms | 9.39 / 19.75ms |
| MARK_INDEX_PRICE | 16.78 / 27.42ms | 15.85 / 28.08ms |
| BOOK_SNAPSHOT | 55.16 / 74.78ms | 50.52 / 77.71ms |
| BOOK_DELTA | 43.69 / 75.71ms | 35.64 / 57.10ms |
| Final BAR snapshot | 532.38 / 806.16ms | 530.93 / 812.70ms |

These are not venue-event ages or candle-close reaction times. Quiet TRADE
may be observable while still ineligible for execution. Risk retains that
distinction. BAR is for signal computation, not a substitute for current
QUOTE/L2/MARK execution context. Warm up once, then append/deduplicate stream
updates instead of polling a full history window.

Projector sampled weighted mean canonical age was 288.555ms (maximum
1,074.2ms); durable append mean 57.392ms (maximum 558.5ms). These stage
statistics are not request percentiles or an unlimited-capacity guarantee.

## Deployment And Rollback

Reader/Stream immutable image:
`qdl-v2-python:2.1.0-1c0844f` /
`sha256:579d578e30814192ae5d20c4f29b653b5c5bc173cfaad73f86e9c192dcb6aa6c`.
Existing service names, mounts, TLS, Kafka offsets and data remain unchanged.
Exact per-role rollback digests are in [certificate.json](certificate.json).
V1 remains legacy/fallback; DNSE/VN and dark Spot are not newly certified.

This release grants no order authority, starts no alpha and does not complete
Trading System P18. It certifies bounded same-host consumer load, not mainnet
execution or independent failure-domain HA. GitHub CI must pass before the
approved `dev -> main -> v2.1.0` publication.
