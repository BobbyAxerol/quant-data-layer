# Quant Data Layer v2.0.16

## Certified Patch Scope

`v2.0.16` returns every realtime V2 feed to its freshness policy. The defect was
not capacity and not a venue: it was four costs on the delivery path that a
consumer paid for every record.

### What was wrong

Measured on 2026-09-16 against a 2,000 ms policy, `TRADE` freshness was
**355,952 ms**. `BOOK_SNAPSHOT` and OKX `MARK_INDEX_PRICE` were rejected
outright. The projector backlog reached **3,002,343** records and the consumer
reported 27 of 60 slices ready.

Four causes, each in the module that carried it:

- `qdl/stream/gateway.py` — the stream gateway took the spool's single `RLock`
  to do a **durable read on the live delivery path**, so every subscription's
  delivery rate was bounded by disk, not by the feed.
- `qdl/stream/gateway.py` — a latest-state feed queued a **FIFO of superseded
  records**. A reader that fell one buffer behind could never reach the newest
  quote, because the newest one was always behind the stale ones.
- `qdl/replay/handoff.py` and `qdl/stream/grpc_service.py` — a replay token
  was **re-signed on a worker thread once per record**, paying a thread hop
  for a watermark the loop already knew.
- `qdl/query/contracts.py` and `qdl/query/service.py` — `DATA_STALE` **did not
  say which predicate failed**, so the consumer could not distinguish a slow
  feed from a dead session and tore the slice down for both.

### What changed

| Slice | Change |
|---|---|
| R1.1 | the delivery path reads a cached high watermark instead of taking the spool lock |
| R1.2 | the replay loop collapses token advances over unmatched runs |
| R1.3 | `DATA_STALE` names `EVENT_AGE`, `SESSION_STATE` or `SESSION_LIVENESS` |
| R1.8 | a latest-state feed keeps the newest record in a bounded buffer and drops the superseded ones |
| R1.9 | a cursor whose watermark is known is signed on the loop, not on a worker |
| R1.11 | the projector drain window is a configurable budget, not a hard-coded 10 ms |
| R1.12 | an overflowing subscription reports backpressure instead of discarding in silence |

Seven python roles were recreated on `qdl-v2-python:2.0.16-df4b8aa`:
both stream processes, both query readers, all three projectors.

### Measured result

Consumer-side, through the real data plane with the production binding, eight
iterations at 2026-09-17T04:11:44Z, after the offset recovery:

| Feed | p50 event age at serve | Policy |
|---|---|---|
| `QUOTE` | 437-700 ms | 2,000 ms |
| `TRADE` | 804-1,711 ms | 2,000 ms |
| `BOOK_SNAPSHOT` | 1,186-1,361 ms | was rejected |
| OKX `MARK_INDEX_PRICE` | 785-892 ms | was rejected |

Request latency p50: snapshot reads `6.5-7.2 ms`, book snapshot `76.6 ms`,
feed status `6.9 ms`, instrument lookup `4.1 ms`, 1m warmup `119 ms`, batched
warmup `649 ms`.

OHLCV against the venues' own REST klines: **20/20 bars exact**, all `FINAL`.

Projector steady state, 24 consumer-group snapshots over 5m56s: produced
**373** records/s (132,880), consumed **373** records/s (132,894). The projector
consumed 14 records *more* than were produced: there is no backlog.

Thirty-one minute certification window, thirty samples: the consumer stayed
`V2_PRIMARY` with **zero** fallback to V1 on every sample, 60 slices demanded,
`READY` on 26 of 30, worst sample 3 of 60 slices transiently unhealthy.

### Also in this release

- **The Rust runtime rollout v2.0.15 deferred is done.** All five Rust roles run
  `qdl-v2-rust:2.0.15-c5a5be0` with `rustls 0.23.45`, closing
  **RUSTSEC-2026-0285** in the runtime and not only in source.
- **CPU ceilings are per service with the measurement that justified each one.**
  Declared total `12.25 -> 13.35` on a 16-core host, with eight services
  *reduced*.
- **Canonical Kafka retention `24h -> 6h`.** Canonical is derived and can be
  rebuilt from raw; raw stays at 24h because it cannot be refetched.

## Gates

- Python suite in the release image, network disabled, read-only source mount,
  both log directories on tmpfs: **1,495 tests, 0 failures, 0 errors, 7 skipped**
  in 421 s. The four "pre-existing import errors" recorded against v2.0.15 were
  a missing `/app/logs` tmpfs, not a defect; with it, discovery is clean.
- Rust gate **inherited**: no `.rs`, `Cargo.toml` or `Cargo.lock` change between
  `c5a5be0` and `df4b8aa`, and the running image is the one that passed
  `fmt`, `clippy -D warnings`, 81 tests across 13 targets and `cargo-deny`.
- CI on `dev`: `contract-tests`, `sdk-python310`, `unit-tests`.
- Buf contract checks verified locally in `bufbuild/buf:1.50.0`:
  `format --diff --exit-code`, `lint`, and `breaking --against
  baseline/qdl-v2-phase1.binpb` all exit 0.

## Boundaries

- Binance `MARK_INDEX_PRICE` still answers `DATA_NOT_READY`. Pre-existing, not
  touched here, open on its own slice.
- One of sixteen OKX `MARK_INDEX_PRICE` samples was rejected on `EVENT_AGE`;
  the mark-price/index-tickers pairing recorded in ledger entry 21 is still the
  cause.
- **The 500-total/250-per-partition lag bound is a convergence gate, not a
  health gate.** It belongs to the projection-cache rebuild runbook, where its
  job is to prove a replay drained. At 373 records/s a 500-record bound is
  1.3 seconds of work, so an instantaneous sample of a perfectly healthy queue
  crosses it — which is exactly what 5 of 30 window samples did while the
  consumer stayed ready and never fell back. Steady-state health is the
  produced-versus-consumed rate and the seconds-of-work figure. Correcting the
  gate's *use* is a scheduled slice; the runbook's own use of it is correct and
  was left alone.
- `binance_bar_edge` stays on `qdl-v2-python:2.0.15-5130f6f`: it does not use
  the stream delivery path and keeps the certified BAR settlement build.
- The OKX last closed 1m bar serves about 60 s behind the Binance one
  (96.6 s vs 36.1 s age at measurement). Both are inside the 180 s consumer
  contract; the gap is the OKX calendar boundary, not a regression.
- A governed offset reset to latest was performed on `stable-projector-v1`
  during recovery, on a pre-production stack and with the owner's authorisation.
  Canonical records between the old committed offset and latest were not
  projected; raw still holds them.
- The companion consumer change R1.4 lives in the `trading_system` repository
  and is not released by this tag.
- This patch inherits the `v2.0.15` certificate and does not recertify the full
  product matrix.
