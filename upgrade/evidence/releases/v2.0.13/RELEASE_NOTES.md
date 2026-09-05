# Quant Data Layer v2.0.13

## Certified Patch Scope

`v2.0.13` closes the SOL/OKX `MARK_INDEX_PRICE` freshness defect without
weakening execution eligibility. The stable projector now schedules owned
canonical partitions fairly, so live partitions cannot be starved by historical
backlog. A value older than `2,000ms` remains typed `STALE` and is not execution
eligible.

This patch inherits the formal `v2.0.12` 299-product Binance USD-M and OKX Swap
data-plane certificate, then adds one sealed real-provider, no-order C2 for the
changed Trading System paper manifest:

- 60/60 opening and closing V2 reads across Binance and OKX.
- BTC, ETH, SOL, DOGE and BNB; six products per instrument per venue: final
  BAR, TRADE, QUOTE, MARK_INDEX_PRICE, BOOK_SNAPSHOT and BOOK_DELTA.
- 50 durable products, signed cursor/reconnect proof, zero direct-provider
  client connections, zero order actions and disposable client cleanup.
- All selected products are V2-only at the product route: V1 remains an explicit
  service-level rollback revision, not a hidden read path.

The selected receipt observed for `300.099s` after quota-paced opening. It is
single-host real-provider data-plane evidence, not a broker execution, signal,
sizing, independent HA/DR or VN market-hours certificate.

## Runtime And Rollback

- Active V2 Python projector image:
  `sha256:d190d7696f4ebe5c34f2b83bf690ac0027e2c356ca548952cf58b5a3293b134d`.
- Explicit V2 projector rollback image:
  `sha256:94c9ef02bfc13f99eebabe641d4723ae6ec08fbaabffb3a217248add88b58820`.
- Active Rust core image:
  `sha256:407a67131ca6567f803950aefd56c547306a20bec6992a42c44ae1719beccabd`.
- V1, Kafka topology and offsets, Redis, SQLite, Trading System, alpha and the
  order path were not changed by this patch-release publication.

## Provenance

The tracked source catalog intentionally includes disabled bindings that are
removed when compiling the sealed active-runtime manifest. The source and sealed
route-plan hashes are therefore recorded separately in
[scope-evidence.json](scope-evidence.json); they are not asserted equal. The
sealed C2 output is the authority for this delta.

The machine-readable certification is in [certificate.json](certificate.json).
The implementation journal records the source test, runtime repair, C2 and
cleanup evidence in
[DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md](../../../../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md).
