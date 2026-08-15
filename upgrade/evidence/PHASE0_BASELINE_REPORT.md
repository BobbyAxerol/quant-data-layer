# Phase 0 Baseline And Containment Report

> **Observed:** 2026-08-13 UTC
> **Branch:** `feat/fund-grade-data-layer-v2`
> **Operational mode:** read-only inspection plus isolated/mocked tests
> **Runtime cutover:** none; no running container was restarted or redirected

## Scope And Evidence

- Frozen V1 OpenAPI and SDK surface: [`contracts/v1`](../../contracts/v1).
- Frozen Redis payload shapes without live values: [`redis-payload-shapes.snapshot.json`](../../contracts/v1/redis-payload-shapes.snapshot.json).
- Two bounded runtime windows: [`phase0-runtime-baseline.json`](phase0-runtime-baseline.json) and [`phase0-runtime-baseline-window2.json`](phase0-runtime-baseline-window2.json).
- Workspace/Trading System/active-alpha inventory: [`phase0-consumer-inventory.json`](phase0-consumer-inventory.json).
- Bounded real-provider/API checks: [`phase0-provider-smoke.json`](phase0-provider-smoke.json).
- Deterministic Binance/OKX/VN/malformed fixture corpus: [`tests/fixtures/phase0`](../../tests/fixtures/phase0).

## Contract And Consumer Findings

- V1 snapshot contains the current OpenAPI document, all `/v1` route/method/name tuples and public `DataLayerClient` signatures.
- Redis golden records shape only for USD-M trade, Spot trade, legacy trade alias, 1m kline and VN quote. Dynamic prices, quantities and timestamps are not committed.
- Inventory scanned 1,748 workspace source/config files, including 361 Trading System files and 261 active-alpha files.
- Active migrated alpha tree has three SDK integration files and no detected direct Binance/OKX/DNSE/vnstock endpoint imports after excluded logs/state/research data.
- Workspace-wide direct-provider references remain high because legacy alpha trees, provider adapters, broker execution SDKs and research utilities are intentionally present. They are not evidence that each path is an active market-data consumer.
- Trading System still consumes legacy `stream:trade:{symbol}`. The current producer lets Spot and USD-M both project to this alias, so Spot removal is a source-authority cutover requiring V1 parity evidence, not merely a container flag.

## Runtime Baseline

Two ten-second windows were captured from the existing service without restart:

| Metric | Window 1 | Window 2 |
|---|---:|---:|
| HTTP service health | `ok` / `ok` | `degraded` / `degraded` |
| Redis command delta | 42,968 | 45,270 |
| Redis input-byte delta | 19,757,878 | 20,416,067 |
| Redis output-byte delta | 1,474,224 | 1,075,203 |
| Redis evictions/rejected connections | 0 / 0 | 0 / 0 |
| Queue cumulative drops | 3,790,249 | 3,790,249 |
| Queue recent drops | 0 | 0 |

Window 2 degraded because demand added `kline:binance_usdm:1m:BTCUSDT`, while raw kline publication lost its internal source metadata. The Redis V1 payload itself remained valid. Commit `965275e` fixes supervisor correlation by carrying source as publisher metadata without changing the Redis payload or endpoint response. It has unit coverage but is not loaded by the running process because Phase 0 deliberately performed no restart.

Additional snapshot:

- 44/44 Binance WebSocket shards connected; one cumulative reconnect.
- 9 declared demanded feeds in the first window: five Binance Futures trade, three VN kline and one VN quote; no Spot demand.
- Redis used about 7.7 MB with no eviction/rejection; AOF is disabled as intended for the ephemeral market-data cache.
- `data_layer_service`: 40.72% of one CPU, 646.4 MiB RSS at sampled instant.
- `redis_marketdata`: 6.46% of one CPU, 12.46 MiB RSS at sampled instant.
- Host had about 3.2 GB available RAM and 18.47 GB free filesystem space.
- Local VN preload occupied about 43.2 MB; data-layer logs about 58.5 MB.

The broad universe expected 4,202 feed states and reported over two thousand missing plus hundreds stale while all currently demanded feeds were initially fresh. Broad-universe telemetry must remain separate from demand-backed execution readiness.

## Spot Containment Result

Current cached universe and configured batch size provide a deterministic topology estimate:

- Spot: 1,377 symbols, 14 shards per feed, 28 trade+kline shards.
- USD-M: 731 symbols, 8 shards per feed, 16 trade+kline shards.
- Full current topology: 44 shards.
- Demand-backed USD-M-only default: 16 shards.
- Expected WebSocket shard reduction: 63.636%.

The new runtime configuration defaults to USD-M trade+kline only and validates all source/boolean values at startup. An isolated lifespan test proves Spot creates no ownership when disabled while USD-M remains enabled. The existing container remains on its already-loaded four-source configuration until a separately approved immutable-image cutover.

Rollback for that future cutover is one configuration change:

```text
DATA_LAYER_BINANCE_SOURCES=binance_spot_trade,binance_futures_trade,binance_spot_kline,binance_futures_kline
```

## Verification

- Host dependency-light Phase 0 tests: 17 passed.
- Application-image focused contract/source/fixture tests: 33 passed.
- Full application-image regression: 100 passed, 2 skipped by their existing conditions.
- Bounded provider smoke: 7/7 passed, covering health, Binance USD-M latest trade/kline, Binance two-bar history, OKX two-bar history, VN two-bar preload and last VN quote.
- Test cleanup: `test:*` key scan returned no residual Redis keys; no test container, volume, shared Redis flush or production Parquet mutation was used.
- Existing deprecation warnings for `websockets.legacy`/`InvalidStatusCode` remain visible and belong to the later adapter/runtime upgrade, not a Phase 0 regression.

## Phase 0 Conclusion

Phase 0 establishes a reproducible compatibility and load baseline and adds a safe source-control path. It does not claim the current queue semantics are fund-grade: cumulative drops confirm that feed-class-aware backpressure/durability work in Phases 2-3 is necessary. Kafka is not provisioned in Phase 0; the staged transport contract and bounded bridge remain the approved next durability path.

The running service was not changed. Before deploying this branch, build an immutable image, run shadow V1/Redis parity, then recreate the selected data-layer producer once with health/demand observation and the documented one-variable rollback.
