# Quant Data Layer v2.0.15

## Certified Patch Scope

`v2.0.15` corrects how the stable V2 edge captures a closed Binance bar, makes
the stable stack recover itself after a host boot, and takes one published
dependency advisory.

### Binance final-BAR settlement

Polling `GET /fapi/v1/klines` for a freshly closed 1m bar returns different
answers from different Binance replicas: measured on 2026-09-16, trade counts
cycled `3155 -> 3232 -> 3263` for about five seconds before every replica
converged. The edge read that bar once at close plus `0.10s` and never revised
it, so whichever partial answer it drew became the durable bar permanently.

Measured against the venue's own REST klines before the fix, five consecutive
1m bars per Binance symbol were `FINAL`, `revision=0` and wrong: trade count
always lower than the venue, volume short by `0.17-307 bps`, close off by up to
`0.46 bps`. OKX matched exactly.

The edge now reads the same target bar until two consecutive reads return an
identical row **and** the bar is at least six seconds old, then publishes
exactly that venue row. It merges nothing, invents nothing, and fails closed
when the venue does not settle inside its read budget, leaving the existing
retry and coverage repair to own the outcome. OKX keeps its single read: its
`confirm=1` candles are final on arrival.

Verification, two independent rounds over fresh bars: **40/40 bars exact** on
open, high, low, close, volume, base volume and trade count across Binance
`BTCUSDT`/`ETHUSDT` and OKX `BTC-USDT-SWAP`/`ETH-USDT-SWAP`.

**Cost, stated plainly:** the final 1m bar now lands `6.7-8.2s` after close
instead of about `2s`. The consumer contract allows `180s`. The previously
published close-to-final-BAR availability figure of p50 `2.151s` no longer
describes Binance and must be re-measured before it is quoted again.

### Host-boot recovery

`stable_redis` is ephemeral by design, so after a boot the projectors stop
fail-closed on `ProjectionCacheMismatch`. Every runtime role now declares
`unless-stopped`, and `scripts/v2_stable_boot_recovery.py` with its systemd
oneshot performs exactly the governed projection-cache rebuild when, and only
when, it observes that post-boot state. Rehearsed end to end on the running
stack in `17m 44s`, ending `HEALTHY` with the projector group inside its
bounded lag gate.

### Dependency advisory

`rustls` moves from `0.23.43` to `0.23.45` for **RUSTSEC-2026-0285**, published
2026-09-14: TLS 1.3 handshake messages incorrectly accepted across encryption
level boundaries.

## Gates

- Pinned `rust:1.82-slim`: `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets --locked -- -D warnings`,
  `cargo test --workspace --locked` **81 passed / 0 failed** across 13 targets,
  `cargo-deny 0.20.2 check` advisories, bans, licenses and sources ok.
- Python discovery `1436` tests with the four import errors that a pristine
  `dev` worktree also has.
- CI run `35077114929` on `dev`: `unit-tests`, `sdk-python310`,
  `contract-tests` all green.

## Boundaries

- Only `binance_bar_edge` was recreated, on
  `qdl-v2-python:2.0.15-5130f6f`. V1, Kafka topology, Redis, SQLite, every
  other role, Trading System and alpha were untouched.
- The deployed `qdl-v2-rust:2.0.12-3f1c50e` still contains `rustls 0.23.43`.
  The source is fixed; that runtime rollout is a separate packet.
- This patch inherits the `v2.0.14` certificate and does not recertify the full
  product matrix. The venue comparison covers the 1m interval for four
  instruments; other intervals inherit the existing OHLCV certification.
