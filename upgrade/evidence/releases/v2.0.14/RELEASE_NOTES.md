# Quant Data Layer v2.0.14

## Certified Patch Scope

`v2.0.14` repairs an exact durable-replay optimization in the stable V2
projector. When a canonical Kafka record is already byte-identical and durable,
the projector skips a duplicate canonical sink write while retaining lineage and
idempotent compatibility-cache projection. It also corrects the release
acceptance client so its quiet-trade request exactly matches the governed
manifest entitlement.

This patch inherits the `v2.0.13` certificate. Its bounded, real V2 no-order
acceptance verifies the changed runtime through two query replicas and two
stream aliases for:

- Binance USD-M `BTCUSDT` and `ETHUSDT`.
- OKX Swap `BTC-USDT-SWAP` and `ETH-USDT-SWAP`.
- Five closed 1m BAR warmup rows plus authoritative TRADE, durable cursor ACK
  and contiguous reconnect/resume for every case.

The disposable client made no provider-direct connection and no order, signal
or sizing action. This is a single-host data-plane patch certification, not a
broker execution, independent HA/DR or VN market-hours certificate.

## Runtime And Rollback

- Active V2 Python projector image:
  `sha256:3e062a3ba38d52d31718162bd21cd52a246e414ee117eae56481da00b8db7b4a`.
- Explicit V2 projector rollback image:
  `sha256:d190d7696f4ebe5c34f2b83bf690ac0027e2c356ca548952cf58b5a3293b134d`.
- Active Rust core image:
  `sha256:407a67131ca6567f803950aefd56c547306a20bec6992a42c44ae1719beccabd`.
- V1 fallback, Kafka, Redis, SQLite, query/stream, Trading System, alpha and
  order paths were unchanged by this release packet.

The machine-readable scope and certificate are in
[scope-evidence.json](scope-evidence.json) and
[certificate.json](certificate.json). The implementation journal is
[DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md](../../../../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md).
