# Quant Data Layer v2.0.17

## Certified Patch Scope

`v2.0.17` returns OKX native bars to the canonical stream, removes a physical
ceiling that failed every canonical write closed for two hours, and lifts a
throughput limit that made the resulting backlog unable to drain. Three defects,
each proven against the running stack before it was changed.

### What was wrong

**OKX bars never reached the canonical stream.** Every closed candle was
quarantined `StaleGeneration` - 1,183 records across two windows on 2026-09-17 -
while provisional candles were filtered correctly and the raw stream carried all
70 candle channels from all five instruments. The spool's newest OKX `bar-1m`
record was 7,158 s old.

The cause was not another connection. It was the bar edge:

```
partition_key      …/okx_bar/okx-swap-bnb-usdt-swap-bar-1m-primary-v2
frame_session      okx-business-001-25-…          frame_generation   25
tracked_session    qdl-v2-stable-okx-rest-r1-g1789653808007759187
tracked_generation 1789653808007759187
```

The bar edge publishes REST bootstrap and repair rows carrying a **nanosecond
timestamp** where a connection generation belongs. One such row on a bar
partition fences every native candle behind it for the life of the core
process, because 25 will never exceed 1.79e18. That timestamp decodes to
14:03:28Z, the minute the bar edge was recreated in this session's rollout and
bootstrapped its history; the quarantines began at 14:20. The first outage has
the same shape - the bar edge was given a new state path at 07:53 and the
candles died at 09:16.

Two hypotheses fitted the partial evidence before this one and neither survived
contact with it, so the rejection was made self-describing rather than guessed
at a third time: a stale-generation rejection now names the tracked session and
generation it compared against. That diagnostic is part of this release.

**The spool failed every write closed while inside its own retention policy.**
From 10:09:06Z the gateway answered `bridge physical storage bound would be
violated`: main file 2,252,414,976 B plus WAL 966,902,232 B plus shm 1,900,544 B
against a 3 GiB ceiling - 7,720 bytes of headroom. The WAL frames had already
been checkpointed, because `PRAGMA wal_checkpoint(PASSIVE)` recycles a WAL but
never shrinks the file. A single `TRUNCATE` reclaimed all 922 MB in 0.08 s with
no reader blocking it. The ceiling itself was also tighter than the rows
`max_records` permits: 1,841,712 rows at the payload size measured on the live
cache need roughly 3.3 GB on disk.

**The backlog could not drain.** Consumption had converged on production at
759 events/s with nothing saturated: projectors at 0.49 of a 2.00 CPU ceiling,
no throttling, and a benchmark on the spool's own volume under its own pragmas
returning 40,280 rows/s at `synchronous=FULL`. The projector asked Kafka for one
record at a time and paid a thread hop for each.

### What changed

| Slice | Change |
|---|---|
| R1.25.1 | a producer the ordering fence cannot identify may not fence the live ingestor; generations are only comparable inside one identified lane |
| R1.25.2 | a stale-generation rejection logs both sides of the comparison |
| R1.25.3 | the projector fetches a bounded batch in one broker call; brokers without it keep the original fill loop |
| R1.25.4 | the spool reclaims a WAL that outgrew its declared `journal_size_limit`, and reclaims again before the physical bound refuses a write |
| R1.25.5 | the physical bound is sized from the rows retention permits, not chosen |
| R1.25.6 | the canonical sink retries a dead pooled connection once before failing over to the passive peer |
| R1.25.7 | Compose records the broker memory bound raised live after the kernel memcg killed brokers at 768m |
| R1.25.8 | the drift verifier seeks the spool by key instead of scanning it, and watches the bound that failed writes closed |
| R1.25.9 | an endpoint report that separates delivery lag, durable-cache age and the contract, per instrument, with the sample count behind every number |

The test helpers built synthetic session identities (`s1`, `session-1`) that the
running system never emits, which is why the suite stayed green while production
did not. They now use the production shape, and 1,106 missing bars were
republished through `scripts/repair_stable_final_bar_history.py` - 1,014 of them
OKX history the fence had been swallowing since 09:16Z.

### What this release does not change

Binance `MARK_INDEX_PRICE` stays outside policy, and Binance `BAR` stays on
provider REST. Both were re-tested directly against the venue on 2026-09-17:
Binance USD-M acknowledges a subscription for `@markPrice@1s` and `@kline_1m`
(`{"result":null}`) and then sends nothing - 80 s, zero frames - on the same
socket where `btcusdt@trade` delivered 353 frames in 12 s. Four subscription
shapes were tried for mark price and all returned zero. Our configuration is
correct on both sides. This is the condition `production_catalog.py` already
records for klines, and it holds for mark price too, so neither can be repaired
from the WebSocket path. They need the reference/REST pair path, which is a
catalog migration and is not in this tag.

Binance `BAR` delivery stays at roughly 7 s for the same reason and by design:
the bar edge holds a closed bar for 6 s and confirms it twice, because a closed
Binance 1m kline was still changing **5.04 s** (BTCUSDT) and **3.96 s**
(ETHUSDT) after its close boundary when measured on 2026-09-17. Cutting that
delay would publish bars the venue then revises.
