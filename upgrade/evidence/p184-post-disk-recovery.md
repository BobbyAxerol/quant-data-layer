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
