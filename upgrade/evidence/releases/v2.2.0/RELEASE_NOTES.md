# Quant Data Layer v2.2.0

## Scope And Architecture

Kafka-native V2 is the active data path for the declared Binance USD-M and OKX
Swap consumer demand. Rust owns ingestion, canonical normalization/L2, Kafka
state projection and streaming. Python owns provider adapters and public Query
contracts. Bounded Redis market cache rebuilds from Kafka state; SQLite is not
in the active read path. Ten old reader/projector roles are stopped; historical
state is retained, not erased.

TS consumes 60 crypto routes for BTC/ETH/SOL/DOGE/BNB on both venues. Five-symbol
price/bar execution demand and the 255-member universe are different scopes:
510 universe venue-products use daily batch history, not 510 execution streams.
Warmup supports declared limits through 10,000 when authentic provider history
exists; initialize once, then append/deduplicate final BAR with bounded maxlen.

SDK 2.0.5 provides bounded off-loop durable cursor ACK. Apply data first, then
await acknowledgement; fsync, monotonic checkpoint and generation fencing remain.
Cancellation/global shutdown drains accepted I/O. Shared alpha source is tested
but this release does not start an alpha or place an order.

## Measured Acceptance

Final actual TS window: 300.000849 seconds, 29/29 samples at 60/60 READY,
no fallback or V2 error. Native Stream delivered 82,522 events, no new overflow,
replay, reconnect or closure; final queue zero, both Stream RSS values unchanged,
no restart/OOM. This is bounded acceptance, not a multi-day leak or HA certificate.

Timer wakeup (29 samples, 10-second cadence): p50/p95/max 1.08/2.51/4.96 ms.
No p99 is claimed. Durable cursor counters: 29,995 acknowledged checkpoints,
7,805 batched commits, zero errors, pending peak since startup 28/64, final zero.
Mean batch completion 15.09 ms runs off-loop; maximum since startup 406.20 ms is
not an in-window percentile. Separate syscall probe found 0 main-thread fsync
calls; fsync was not removed. External TS Redis GET worst measured route p99
0.905 ms is cache-read time, NOT exchange-to-consumer delivery latency.

Inherited production workload: 50 logical alpha sessions (20 candle,15 realtime,
10 grid,5 multi-feed), 15,504 requests without errors/missed offers,90 streams,
12 reconnect probes. Full serving stack including Kafka/provider coordination
averaged4.669 vCPU over374.415 seconds. Limits and synthetic allocator capacity
are reported separately in certificate.json; allocator fill is not market data.

Consumer SDK call until validated usable result, milliseconds:

| Data | Binance p50/p95/p99 | OKX p50/p95/p99 | Samples/venue |
|---|---:|---:|---:|
| QUOTE | 9.72/24.38/46.53 | 9.91/21.59/33.30 | 2750 |
| MARK/INDEX | 11.07/23.26/39.69 | 11.18/22.52/38.11 | 4125 |
| TRADE | 9.79/23.78/unavailable | 13.23/29.51/unavailable | 55 |
| BOOK snapshot | 25.76/44.75/unavailable | 28.92/44.53/unavailable | 54/55 |
| Final BAR latest | 15.31/29.55/unavailable | 18.58/36.15/unavailable | 55 |

Cold history under load:2500 rows3189-5144 ms;5000 rows4838-6956 ms, four reads,
not a percentile. Full per-binding source-age-to-TS-cache, GET measurements,
stream latency and sample counts are in endpoint-report.json. Quiet source age
is not wire latency and does not itself grant execution eligibility.

## Consumer API

HTTP: instruments/list and identity lookup, per-feed status, snapshot, history,
warmup, warmup:batch, reference:batch, readiness/readiness:check, retained-window
gap diagnostics. Native gRPC: GetSnapshot, Subscribe, Replay, GetFeedStatus.
Use the authenticated paired targets in DATA_LAYER_SERVICE_ACCESS_GUIDE.md.

Final BAR drives signals. QUOTE and verified L2 support executable price/impact
context; MARK/INDEX requires the declared trigger/risk policy. Risk remains the
order-admission authority. Reference metrics include funding, OI, long/short,
taker flow, metadata and supported basis variants, subject to venue capability
and explicit identity entitlement. A wrapper is not implicit execution authority.

## Boundaries And Rollback

- DNSE/Vietnam remain V1; new images exclude quarantined vnstock/vnai.
- Deferred Spot is not certified by this crypto KN release.
- Five Binance3d histories contain provider discontinuities. Strict complete
  history is rejected, never interpolated or silently padded.
- Gap scan covers retained windows:702 scanned,6excluded,4VN unavailable;
  it does not prove listing-to-now completeness.
- V1 fallback is allowed only by product policy; unsupported products stay
  BLOCKED, with no cross-venue substitution or direct-provider bypass.
- Reader rollback is the exact image/config in certificate.json. Old SQLite
  rollback was rehearsed, but restarting its retired groups requires checking
  current Kafka retention/offset floors. No reset/flush/delete to force recovery.
- Same-host replicas do not certify an independent failure domain. Future
  payload growth and arbitrary50-alpha workloads require capacity validation.

## Provenance

Runtime acceptance is PASS in certificate.json, with15 hashed evidence files.
Source/SDK/image components are individually identified; rebuilding is not
assumed byte-identical. Publication requires green remote CI, feature->dev->main,
and the v2.2.0 tag/release. This note alone is not publication proof.
