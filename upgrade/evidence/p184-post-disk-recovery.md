# Post-Disk Recovery Verification

2026-10-08. Status: INFRA_PROGRESS_RECOVERED_CONSUMER_ACCEPTANCE_INCOMPLETE.
Owner scope: bounded read-only verification before load; no rollout, reset,
offset alteration, orders, source fix or freshness/authority policy change.

## Kafka And Materialization

Existing admin.properties, broker endpoints kafka1/2/3:9092. Quorum describe:
leader3, epoch150,3voters, max follower lag0 (52ms reported time). Under-replicated
partition describe exited0 with empty output both before and after observation.
First3 admin commands mistakenly targeted controller9093 and were refused with
UnsupportedVersionException. Corrected to the existing broker9092 contract;
this was an observer mistake, not a new runtime fault or configuration change.

Six all-group committed-offset samples span99.814s,36 group/partition rows:
all sampled offsets nondecreasing, no partition lag strictly grew at every sample.
These are record-count samples, NOT a proof of zero event delay or no historical
loss during disk exhaustion. Kafka transaction/control markers may contribute to
reported offset lag; do not force lag0 by offset reset.

| Group | Peak lag, records | Final max lag, records |
| --- | ---: | ---: |
| Core | 172 | 71 |
| Projector A canonical | 142 | 90 |
| Projector B bars/latest | 116 | 16 |
| Binance resync feedback | 3 | 3 |
| OKX resync feedback | 5 | 5 |

Projector1/2 input advancement30933/62098; latest cache applied16094/12570.
No new status errors, conflict increments or cache reconnects across these samples.
The bounded3minute log check found no selected ERROR/Traceback/ENOSPC lines in
three brokers or TS stdout/stderr. This is not an exhaustive audit of mounted
application logs. Runtime IDs/images/start times/restart counts stayed unchanged
during the measured interval; no observed OOM. No manual restart was performed.

## Real TS Cache

Existing scripts/measure_kn_ts_cache_delivery.py ran for120.095s via stdin inside
market_data_service_p184w, using its own configured Redis URL without printing it.
No additional DL Stream subscription or TS quota consumed. External PubSub/MGET
observation, NOT Redis SET ACK measurement. All60 execution/BAR groups observed;
legacy last-trade/quote paths counted separately and not added to60.

The table lists the single worst-max instrument in each feed, NOT pooled
percentiles. Samples are observer time samples, not necessarily unique events.
All values ms. BAR time origin is1m close; MARK age is not component eligibility.

| Feed / worst instrument | n | p50 age | p95 age | max age |
| --- | ---: | ---: | ---: | ---: |
| QUOTE / OKX ETH | 178 | 391.192 | 883.025 | 1461.599 |
| TRADE / Binance ETH | 170 | 469.686 | 5633.734 | 9810.625 |
| BOOK_DELTA / Binance ETH | 147 | 1708.987 | 14003.404 | 16676.651 |
| BOOK_SNAPSHOT / OKX SOL | 109 | 851.333 | 1498.599 | 2237.455 |
| MARK_INDEX / Binance DOGE | 93 | 1663.906 | 2625.370 | 2951.333 |
| BAR1m / OKX DOGE | 2 | 1610.568 | insufficient n | 2160.823 |

MGET batch n183,p50106.778,p95348.949,max612.189ms. This includes observer/client
cost, not Query request latency. Observer exit1/INCOMPLETE:7MESSAGE_SIZE_LIMIT
notifications exceeded its256KiB bound; discarded by observer, not evidence of
DL dropping these records. Do not relabel this run PASS or silently change limits.
No simultaneous provider witness establishes quiet market versus pipeline delay
for TRADE. BOOK_DELTA delay remains open; no inference that disk caused the
pre-existing downstream issue. Canonical-to-ACK/root-cause trace still required.

One real writer heartbeat read:READY60/60,age7.3s,fallback0,v2error0. Readiness
is NOT proof every stored view remains execution-eligible at the time of use.
No decision to start100-alpha load follows from this heartbeat.

## Evidence, Cleanup And Next Boundary

OutsideGit: /home/bobby/.local/state/qdl-v2/recovery-after-disk-20261008.
Receipt SHA256 d0fc2764c74aa5bd5a8c7f28b8d72c44da35f1df63b62518b30295b9efe3e542. Per-command outputs and bounded metric hashes in receipt.
Source helper bytes from feature2dbfe31; no application changes. No containers,
images or persistent test services created; exec observer/admin processes ended.
No cleanup needed beyond process completion, no shared data deletion/prune.
Prior cleanup retention set and namedBARrollbackf7351c3b preserved.

Canonical /home/bobby/data_layer main5bf5cd5, publishedv2.2.2. Activefeature
/home/bobby/.worktrees/data-layer-p184-closure fix/p184-dl-closure.
Runtime unchanged: core658a9570,ingestor0f6876e1,Query/BAR51450612,
projectora696cfba,Stream1f13408f,writer5849d716. Detailed full digests/config and
state identities in runtime-during.json; releasev2.2.3HOLD remains.

Recovery observation is completed, not whole-system certification. Kafka and
materialization are moving again; downstream BOOK continuity/throughput and
observer payload coverage must close before heavy load. Continue existing
DR05/DR08/caller work, do not create a new broad Data Layer campaign.

## Observer Repair And Fresh Observation

2026-10-08: OBSERVER_REPAIRED_BOOK_CONTINUITY_OPEN. Observer per-message
limit is now 1 MiB; latest pending notifications have an aggregate 8 MiB cap.
These are observer bounds, not changes to production queues or eligibility.
Counters retain channel, size, over-256KiB count and rejections without payload.
Cache objects exceeding the decode bound remain INCOMPLETE. Pending serialized
bytes are bounded; this is not an RSS bound on Redis parser/decoded objects.

Real run 120.322s: 118215 messages, 8 above256KiB, max496519 bytes on Binance
BTC BOOK_DELTA; zero size/budget/other observer errors. Peak pending1029467 bytes.
All60 execution/BAR groups observed. Status OBSERVED is sampling completeness,
NOT execution eligibility or full event delivery. Old7refusals remain unchanged.
MGET176batches p50137.536/p95350.644/max477.204ms: observer roundtrip only.

Worst-max instrument per feed, source-event age at cache read, ms; time samples
not unique events or a paired improvement benchmark:

| Feed / instrument | n | p50 | p95 | max |
| --- | ---: | ---: | ---: | ---: |
| QUOTE OKX BTC | 175 | 359.840 | 678.661 | 1171.773 |
| TRADE OKX BNB | 86 | 740.769 | 2251.782 | 5306.195 |
| BOOK_DELTA OKX BTC | 135 | 6828.955 | 11777.762 | 13375.109 |
| BOOK_SNAPSHOT OKX ETH | 112 | 931.998 | 1436.762 | 1972.581 |
| MARK_INDEX Binance BTC | 90 | 1592.823 | 2501.769 | 3370.146 |
| BAR1m OKX BNB | 2 | 626.366 | insufficient n | 3202.268 |

Evidence outsideGit: observer-payload-20261008/observer.json sha256
a6ebfac67aa730fdf71890df052c9f04d71e2eebdf60cb8b54355e8a69c66146. Receipt and18selectedruntimeidentities alongside.
32unittest PASS; large-payload regression fails on previous source as expected.
Host and retainedPythonimage have no pytest; those attempts failed before tests.
Used stdlib unittest for both test modules; no test silently skipped.

### BOOK Caller Handoff

Existing writer5849d716, SDK2.0.7 retains trace hooks but expiry is
2026-10-07T19:24:14Z. Fresh canonical-to-Redis-ACK remains NOT_MEASURED.
Historical same-event summary at dr07-runtime-20261007/continuation/
writer-same-event-preliminary/summary.json has2556ETH BOOKjoins, no validation
failures; SDK-to-RedisACK p95 Binance230.4/OKX243.2ms. This is historical, not
current recovery proof and does not isolate Stream versus SDK pre-writer delay.
Current BOOK writes and cursor ACK are serialized per batch. That is a candidate
bottleneck, not a proved cause warranting concurrent writes that could break CAS.
Do not overwrite Claude-owned TS source or run a second subscription.

Next exact dependency: coordinate one current-writer packet, preserved credentials/
namespace/cursors and named rollback, bounded trace expiry <=600s, max4096records
perprocess, plus SDK ACK/cursor correlation in Claude-owned caller. Existing
SDK-return/Redis-EVAL hooks alone cannot prove exact cursor persistence timing.
No runtime activation performed here. No claim of complete BOOK remediation.

No images built. Disposable failedpytest container auto-removed. Dockerinventory
86images/69containers/196cacheentries unchanged; buildcache reclaimable0B.
Disk available99461668864bytes at check. No deletion/prune needed this slice;
active images and explicit rollbacks retained. Canonicalmain5bf5cd5/v2.2.2,
featurefix/p184-dl-closure unchanged; v2.2.3 remains HOLD.
