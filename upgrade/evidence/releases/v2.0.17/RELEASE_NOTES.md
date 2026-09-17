# Quant Data Layer v2.0.17

## Certified Patch Scope

`v2.0.17` returns OKX native bars to the canonical stream, removes a physical
ceiling that failed every canonical write closed for two hours, and lifts a
throughput limit that made the resulting backlog unable to drain. Three defects,
each proven against the running stack before it was changed.

### What was wrong

**OKX bars never reached the canonical stream.** Every closed candle was
quarantined `StaleGeneration` - 845 records between 09:16Z and 11:00Z on
2026-09-17, then 222 more from a session that had only just opened - while
provisional candles were filtered correctly and the raw stream carried all 70
candle channels from all five instruments. The spool's newest OKX `bar-1m`
record was 7,158 s old.

The ingestor opens one socket per feed class and each keeps its own connection
generation counter (`partition_feed_lanes`). OKX bars are the first feed to
arrive on a second lane: the business socket stood at generation 23 while the
same instruments' public book socket had reconnected 53,986 times. The ordering
fence compared those counters as bare integers *before* it looked at the
session, so the lower number read as superseded and stayed that way. The proof
is that the lane reconnected on its own at 11:29Z, and the brand-new session at
generation 23 was fenced exactly like the old one at 22.

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
| R1.25.1 | connection generations are compared within one provider lane; an unparsable session id keeps the original comparison |
| R1.25.2 | the projector fetches a bounded batch in one broker call; brokers without it keep the original fill loop |
| R1.25.3 | the spool reclaims a WAL that outgrew its declared `journal_size_limit`, and reclaims again before the physical bound refuses a write |
| R1.25.4 | the physical bound is sized from the rows retention permits, not chosen |
| R1.25.5 | the canonical sink retries a dead pooled connection once before failing over to the passive peer |
| R1.25.6 | Compose records the broker memory bound raised live after the kernel memcg killed brokers at 768m |
| R1.25.7 | the drift verifier seeks the spool by key instead of scanning it, and watches the bound that failed writes closed |

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
