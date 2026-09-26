# KN-5 Astra Corrections / Claude Review Receipt

Status: **IMPLEMENTED_SHADOW_TESTED_PENDING_CLAUDE_REVIEW. NOT DEPLOYED. NOT A RELEASE CERTIFICATE.**
> **Current review: see [post-patch authentic acceptance](#post-patch-acceptance).**
> The source-only and old-shadow report below is preserved as history, not the
> latest acceptance result. No production cutover or release is authorized here.

Date: 2026-09-26. Canonical `/home/bobby/data_layer`, branch
`feat/consumer-endpoint-benchmark`, source baseline `718631d`.
The commit containing this receipt is the review candidate. No push/merge/tag.
Main journal: [Astra handoff](../../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kn5-astra-correctness-handoff).

## Findings And Fixes

1. **D48 deletion:** `sync_demand` now returns ownership alongside demand and
   summary. It can delete only an exact owned BAR 1d row. Unrelated BAR 1h/5m,
   existing equivalent rows, changed rows and active execution rows survive.
   Initial ledger is the exact 500 rows added by `c22f4d3..718631d`, not a scan
   adopting arbitrary daily demand. Demand is written before an atomic ledger
   replace; interrupted ownership update loses deletion rights conservatively.
   Baseline code was run in memory: it did remove independent PRIVATEUSDT BAR
   1h. Current regression preserves it. Same-base native identity changes now
   produce a revision/change record rather than silently retaining old symbols.
2. **OKX entitlement:** reuse shared Reference/L2 rendering and admitted catalog
   identity, with an explicit additive `--extend-from-demand` compiler option.
   Five symbols each get OI history, long/short and taker flow at 1d: +15 alpha
   requirements. Current OI remains. Demand/admission policy/manifest/routes
   agree. No acquisition/subscription was added. Both alpha manifests now have
   375 total requirements, 35 reference; both revisions are 14. Reference/L2
   revision 5, release route 26, primary route 8. Negative auth verifies no
   provider call for wrong venue/policy/interval/grade or stale JWT revision.
   Identity subject and manifest are the actual source alpha identity; JWT key
   and provider are **test fixtures**, not production credentials/real I/O.
3. **Budget:** whole-cache formula includes the old execution/VN baseline and
   500 incremental daily products without counting the ten liquid daily bars
   twice. 7,731,216 retained rows -> 5,844,799,296 B steady, plus inherited
   20,991,944 B rebuild delta -> 5,865,791,240 B. These are extrapolations from
   756 B/row, not new measurements. RSS/client buffers/expanded row mix/old-new
   overlap still need measurement. 1.5 GB does not fit full retention. No
   retention, resource limit or promised 10,000-row service contract was cut.
4. **Evidence:** original files are unchanged; an additive correction names
   diagnostic PARTIAL_RESULT as incomplete. Live harness now fails incomplete
   or malformed diagnostics. Probe percentiles withhold p99 for N < 100 and p95
   for N < 20. Attempted and refused requests remain in the availability
   denominator; timings drawn only from successes are explicitly labelled.
5. **Related defects:** KN BAR cache now binds a verified row to its open-time
   field and bucket, not just its content hash. Cold bucket decoding cooperates
   with cancellation; diagnostic deadlines are checked during bucket decoding,
   not only after an entire product has been materialized. No wire/state schema
   or Rust writer change. Existing generation/fence/watermark rules remain.

## Verification

Final affected suite: **235 run, 232 PASS, 0 failures, 3 SKIP**, 107.961 s.
Modules: `test_kn_universe_top300`, `test_kn_capacity_budget`,
`test_kn_alpha_reference_access`, `test_kn5_review_evidence`,
`test_phase24315_reference_entitlement_materialization`,
`test_okx_reference_completion`, `test_reference_l2_materializer`,
`test_phase113_reference_v2`, `test_kn_query_backend`, `test_kn_state_codec`,
`test_kn_native_slice_probe`, `test_kn_v220_budget`, `test_kn_resource_sizing`,
`test_kn_pre5_sdk`, `test_kn_universe_benchmark`, `test_query_cold_work`,
`test_phase3_consumer_load_driver`.

Tests ran against source mounted read-only in the existing `kn4-abda016` Python
image. A private network-none Redis container supplied real cache/Lua reads,
with no volumes, published ports or production network. Reader tests did NOT
skip. Three deliberate skips: opt-in OKX public GET, unavailable large KN3
capture, real Kafka window-oracle integration. Committed codec golden vectors
ran. No Rust source changed, so no broad Rust/Kafka recertification was run.

Negative/edge cases include independent demand, absent ownership, edits,
idempotency, same-base native changes; 15 authenticated metric requirements;
wrong identity/interval/policy/grade/revision; provider paging/rate-limit/
retention/missing values; cache corruption/open mismatch, generation turnover,
watermark/retention boundaries, gaps and no-write reads; cancellation holding
leases, deadlines inside decode; exact SDK batch row budgets, no cross-mix,
no second warmup at handoff; partial diagnostics and small-sample reporting.

The early test runs caught an out-of-bounds OI freshness declaration and an
incorrect assumption that selector product_type was an enum; both were fixed
before config materialization. An old test adding now-existing demands was
updated to reconstruct its old fixture. The new positive API test initially
omitted required `long_short_kind`; it now explicitly sends GLOBAL_ACCOUNT.
These were source/test failures, not runtime incidents.

## Latency: Existing Live Shadow Evidence, Not Post-Patch Production

Recomputed from raw `stage-35-064645/receipt.json` samples using nearest-rank
percentiles. This is SDK request latency on KN shadow; it is not event age or
completed TS Redis-write latency. No fresh live benchmark is claimed here.

| Read | Venue | N | p50 ms | p99 ms | Max ms |
|---|---|---:|---:|---:|---:|
| BAR_LATEST | BINANCE | 36 | 13.869 | withheld | 27.477 |
| BAR_LATEST | OKX | 36 | 13.559 | withheld | 202.267 |
| FUNDING_REFERENCE | BINANCE | 6 | 88.061 | withheld | 101.508 |
| FUNDING_REFERENCE | OKX | 6 | 109.56 | withheld | 115.663 |
| L2_SNAPSHOT | BINANCE | 36 | 33.558 | withheld | 56.826 |
| L2_SNAPSHOT | OKX | 35 | 26.889 | withheld | 126.683 |
| MARK_INDEX_REFERENCE | BINANCE | 1980 | 11.343 | 40.772 | 260.228 |
| MARK_INDEX_REFERENCE | OKX | 1800 | 11.264 | 42.626 | 283.125 |
| QUOTE_SNAPSHOT | BINANCE | 1260 | 9.313 | 53.055 | 144.188 |
| QUOTE_SNAPSHOT | OKX | 1260 | 10.173 | 40.986 | 248.202 |
| TRADE_SNAPSHOT | BINANCE | 36 | 9.719 | withheld | 45.879 |
| TRADE_SNAPSHOT | OKX | 36 | 9.202 | withheld | 21.317 |

`cmatrix-065612/ts60.json`: 600 attempts, 26 refusals, 59/60 products successful
on both replicas at least once. TRADE is 74/100 usable, not 100%. Its successful
call-to-usable maximum was 584.9 ms; no p99 from 74 samples. That runner queued
60 reads into eight slots; do not call its approximately 600 ms total tail the
intrinsic Query latency, and do not drop queue time from consumer latency.

Old event-to-TS-projector-callback p99: QUOTE 893 ms, TRADE 1,504 ms,
BOOK_DELTA 720 ms, MARK 2,111 ms. These remain callback, not cache-write, numbers.
MARK source age is not request latency and is not by itself component execution
eligibility. Production runtime still needs the actual apply/write endpoint.

Old stage-35 history: 2,500 rows 2,069-4,770 ms, 5,000 rows 5,652-7,099 ms;
four samples, no p99. Matrix batch-50 4,896-5,262 ms is not a full 255-symbol
universe batch. Gap diagnostic returned 409 at 5,045.069 / 5,014.192 ms: bounded
failure, not complete scan or endpoint certification.

Local profiling only: isolated derived-test BAR 5,000-row read, 1 CPU cap,
cProfile enabled, cold 2,957.661 ms and cached 992.086 ms. Decimal validation and
initial public payload construction dominate cold work; existing immutable-row
cache removes much of repeat work. These two samples include profiler overhead,
exclude HTTP/SDK, and are neither before/after speedup nor production latency.
No validation was removed and no freshness verdict cached for a speed claim.

## Why Old TRADE Refusals Must Remain Safe

A connected session can have no recent trade. A stale last-trade price must not
be made execution-eligible by a heartbeat or a new timestamp. If an alpha needs
an executable price reference for MARKET/LIMIT, it must explicitly request the
fresh QUOTE/L2 product, with Risk revalidation. Do not silently substitute one
feed for another. Available/fresh quote coverage and fail-closed trade coverage
are separate requirements; neither guarantees the eventual venue fill price.

## Evidence And Remaining KN-5 Work

Evidence root: `/home/bobby/.local/state/qdl-v2/kn5-astra-20260926/`.
- `targeted-tests.txt`: SHA256 `6613f20899f1cabff802b9559d4c605264d6c5f74c05db5d592f0de7de397132`.
- `corrected-kn4-evidence.json`: SHA256 `1475d9e2065670011e052ead29abbd0fe1575b24e6c1311b1f910d02619b7e06`.
- `inherited-artifact-verification.json`: 21/21 match, SHA256 `04efdfcc745e35873bebc2c39ac9ad54a919d4d9578dc1019db1b73c9208c714`.

Claude reviews the source/entitlement diff, ownership seed, byte compatibility,
and statistics boundaries. Then continue the EXISTING KN-5 plan, not new phases:
1. Reseal candidate config/JWT revisions and shared Rust admission for the new
   OKX statistics; offline entitlement success is not deployed capability.
2. Measure whole-cache RSS, staging and batch buffers; decide limits against
   the unchanged 10,000-row promise. Run the actual daily universe matrix with
   honest short history and typed missing outcomes.
3. Measure cold/warm caller -> SDK validation -> application -> completed TS
   Redis write, per binding/replica. Include failures and request queue time;
   stream source age and component eligibility are separate dimensions.
4. Validate diagnostic completeness independently. An expected partial scan
   may prove safety but cannot be labelled whole-endpoint acceptance.
5. K5.2 load evidence and K5.3 bounded deployment/rollback precede KN-5 release.
   No all-endpoint, 50-alpha or mainnet execution certification is issued here.

## Runtime And Hygiene

No production container/network/topic/offset/cache/secret/order mutation.
No image build or new worktree. Query still `2.1.1-83fa1bc` (`dd065fdf8c43`),
Stream `2.1.1-ae2d62a` (`37d7f5182ea1`), TS market-data `06e992004735`;
all four readers and TS market-data restart=0, OOM=false at the read-only check.
Local stable main tag remains `v2.1.0`; this is not a new published release.
Active/rollback and Claude's shared candidate images are retained. Exact cleanup
and final commit/diff inventory are recorded in the main journal.


<a id="post-patch-acceptance"></a>
## Post-Patch Authentic Acceptance - 2026-09-26

**Status: IMPLEMENTED_SHADOW_TESTED_PENDING_CLAUDE_REVIEW; scoped cleanup complete.**
The owner rejected source-only closure. This section supersedes the old latency
and remaining-measurement statements above, without erasing failed runs.
All new data is real provider history or read-committed canonical mirror bytes.
The namespace is isolated; this is NOT production, independent HA or order certification.

### Implementation And Root Causes

- D48 ownership and OKX reference entitlements above are retained and now exercised
  with actual alpha identity, not only test credentials.
- Global gap scan decoded/sorted too much retained history for its 5,000ms deadline.
  Rust projector now maintains per-bucket interval/run summaries in the SAME Lua
  transaction as rows, floors, revisions and checkpoints. Query validates generation,
  source, head, count and floor, then reads bounded summaries. Legacy missing indexes
  use the exact scanner, not an empty success. Corrupt/incomplete state fails closed.
- KN history no longer inherits the old 1,095-day cap. Positive-time history checkpoints
  and OKX weekly REST boundaries handle 10k requested lookback without negative Unix time.
- SDK2.0.3 rejected `diagnostics:null`; omission of absent diagnostic fields preserves
  strict compatibility without ignoring unknown fields.
- Two mixed-load CPU bottlenecks: batch warmup rendered the whole response without
  the single-warmup chunking/lease policy; prefetched row validation also ran on the
  asyncio loop. Both now use the existing bounded cold workers and cooperative yields.
  Cancellation holds admission until the worker exits. No provider limiter or hot SLA changed.
- SDK default aggregate warmup chunk is2,500 rows; a single5k/10k requirement is not
  truncated. Bounded admission recovery makes at most3 attempts ONLY on retryable
  RATE_LIMITED cold reads, retaining identity, interval, count and policy. Snapshot,
  quality, auth and mixed partial errors are not retried. Attempts and waits are measured.
- Slow-reader test deliberately queued5s of QUOTE, then wrongly required that backlog
  to pass2s current-price freshness. The corrected oracle validates ordered replay as
  non-executable, then requires strict current snapshot recovery. It never relabels
  old quotes/trades as prices. Unexpected stream failures now retain bounded typed quality.
- Reference/L2 certification had a frozen55-product assumption; it now derives exact
  identities from the signed manifest, retaining uniqueness/policy checks.
- Percentile rank correction uses nearest-rank ceil(q*n)-1. Sparse cohorts withhold
  p99 below100 and p95 below20; raw older gate output is retained, not rewritten.

### Artifact Provenance

Reader image: `qdl-v2-python:kn5-9e81171`,
`sha256:17a359779702e7b1ced2bd38acdf07267246b54db969f9bc25d6aec238599d06`.
Native projector semantic source20e5062 (a95310a formatting); isolated history edge822a150.
Final50 test client21fda1d; subsequent SDK recovery source2dad966 is a read-only,
Git-archived client overlay, not a new server image. State the tuple, not one false SHA.
Production images, manifests, consumer Redis, Kafka offsets and authority did not change.

### Tests And Honest Denominators

- Python affected/API/cancellation/history/identity suite189/189 PASS, plus48/48
  environment/legacy modules and133/133 SDK/read-backpressure regressions. Suites overlap;
  do not add these as unique test counts.
- Earlier full2339 run had12 failures,20 errors,46 skips; all failures/errors were
  classified and their affected modules rerun after correction. This is NOT a claim
  of a new single full-suite green run. Test-only compiled SafeLoader was checked
  against standard parsed configs; production parser unchanged.
- Real Redis reader27/27; Rust real-Redis atomic cache6/6 and Stage-B27/27; clippy
  all-targets PASS. Isolated summary initialization12,523 buckets,0 failures.
- Read-plane matrix:132/132 target cases,48/48 history ladders,20/20 batch shapes,
 16/16 handoffs,24/24 replica parity,8/8 freshness. Its two original diagnostic
  failures were then tested separately after the Rust summary patch.
- Actual alpha Reference70/70 PASS across two replicas. Current/history OI, long/short,
  taker flow identities/selectors are checked, not inferred from provider support.
- All10 execution1m products have5,000 contiguous retained rows,0 sequence gaps.
- Final50-bounded:15,504 hot requests,0 failure/missed/starved;90 streams,0 errors;
  all30 BAR streams received final bars;12/12 cursor reconnects restored. Deliberate
  slow reader drained8 non-executable frames and passed strict current recovery.
  Four cold2500/5000 reads complete; no leaked tasks, OOM or restart. Startup took
 69,387ms with119 bounded admission retries, not instantaneous fleet readiness.
  Production TS observed33/33 samples60/60, one DATA_STALE disconnect within the
  frozen baseline gate. Actual isolated TS writer is measured separately below.

### Request To Validated Consumer Result (ms)

Final50 load uses real SDK calls, including decode/validation. This table is not
source event age or TS Redis latency. No synthetic processing delay was added.

| Read | Venue | Successful N | p50 | p95 | p99 | Gate p95 / p99 |
|---|---|---:|---:|---:|---:|---|
| QUOTE | Binance |2750|11.443|51.341|129.338|100 /250|
| QUOTE | OKX |2750|12.138|57.779|149.327|100 /250|
| MARK/INDEX | Binance |4125|12.931|53.510|149.794|250 /500|
| MARK/INDEX | OKX |4125|12.941|54.318|141.716|250 /500|
| TRADE | Binance |55|10.050|56.619|withheld|100 /reported only|
| TRADE | OKX |55|11.469|78.993|withheld|100 /reported only|
| L2 snapshot | Binance |54|28.683|79.125|withheld|300 /reported only|
| L2 snapshot | OKX |55|40.476|257.138|withheld|300 /reported only|
| Latest BAR | Binance |55|16.327|47.524|withheld|1000 /reported only|
| Latest BAR | OKX |55|22.225|77.375|withheld|1000 /reported only|

The separate TS3600-read run measured30 requests per binding per replica, all60
bindings positive on BOTH replicas. Five feed groups were600/600; TRADE514/600,
86 correctly rejected execution-freshness reads. Consumer60-read burst uses8 slots:
its complete call-to-usable p99 includes runner queue, not just Query speed.

| TS read | SDK p99 ms | Queue-inclusive call-to-usable p99 ms | Usable / attempted |
|---|---:|---:|---:|
| BAR |249.9|964.7|600/600|
| BOOK_DELTA |219.8|930.4|600/600|
| BOOK_SNAPSHOT |237.5|788.5|600/600|
| MARK_INDEX_PRICE |155.9|896.4|600/600|
| QUOTE |212.1|856.3|600/600|
| TRADE |203.8|927.3|514/600|

Those runner-burst numbers are not the scheduling pattern of deployed TS. Per-binding
N30 p95/max, rejection quality and source ages remain in `acceptance/ts-final.json`.
MARKET/LIMIT price selection must explicitly request QUOTE/L2 and let Risk revalidate;
TRADE rejection cannot be fixed by changing its timestamp or quietly substituting feed.

### Actual TS Cache Boundary (ms)

Unmodified TS image/adapter/bridge/cache-projector wrote only the isolated Redis.
The measurement calls the actual pipeline, waits for ACK, then verifies written keys
by MGET. It adds verification work and is not a production load claim.
600s run:133,878 writes,258,764 verified keys; ACK p50/p95/p99=0.8/3.3/6.7;
ACK+readback=1.1/4.8/11.1. All115 steady samples60/60 READY.

| Source event or BAR close -> Redis readback, steady | N sampled | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|
| BAR |90|1094|3002|withheld|
| BOOK_DELTA |5120|379|661|868|
| BOOK_SNAPSHOT |190|1041|1599|1843|
| MARK_INDEX_PRICE |5120|1007|1640|2152|
| QUOTE |5120|471|1006|1931|
| TRADE |4761|512|2307|3584|

Raw stored fact age is NOT execution-price eligibility, especially quiet TRADE and
component-aware MARK/INDEX. Reservoir samples are uniform per binding, not a tail-only
sample. The read-only canonical mirror adds a hop that final production will not use;
do not subtract an unmeasured number to claim a future latency.
Additional450s run overlapping final50 had84/85 steady samples60/60; one58/60
QUOTE-age sample self-recovered. It is retained, not merged into the600s green result.
The subsequent600s consumer-headroom run is complete:115/115 steady samples60/60;
149,656 writes,305,252 verified keys, ACKp997.9ms, ACK+readbackp9913.4ms.
It used a2CPU TEST-client ceiling (not a Data Layer or production TS change),
with0.683core mean,144.2MB peak cgroup memory and0 throttle/OOM. Five startup
RATE_LIMITED stream opens recovered; zero steady unhealthy samples. This is
additional evidence, not a controlled proof that CPU caused the earlier transient.

### HTTP, gRPC, History And Reference Coverage

| Public endpoint / method | Actual scope / result |
|---|---|
| GET /v2/instruments; /v2/instruments/{identity} | Both replicas200;12-16ms small matrix samples, notp99 |
| GET /v2/market-data/{uid}/snapshot | TRADE,QUOTE,BAR,BOOK_SNAPSHOT,BOOK_DELTA; timing tables above |
| GET /v2/feeds/{uid}/status | Typed identity/session/event/gap/watermark status,17-20ms small samples |
| GET /v2/market-data/{uid}/warmup | Signed boundary, requested lookback up to10k, final-only; no generated candles |
| GET /v2/market-data/{uid}/history | Bounded retained ranges; replicas/ordering/identity checked |
| POST /v2/market-data/warmup:batch | Same interval/multiple symbols, bounded SDK chunks, strict/partial errors explicit |
| POST /v2/market-data/reference:batch | Funding,OI,long-short,taker,mark/index,metadata,basis by signed capability; metric/type/selector is not interchangeable |
| GET /v2/system/readiness; POST /v2/system/readiness:check | Manifest/product readiness, not an execution grant |
| GET /v2/data-quality/gaps | Complete HTTP200 on both replicas/alpha identities;833.8-1250.3ms unloaded,1594.7-3571.8ms under load |
| gRPC Subscribe / Replay / GetSnapshot / GetFeedStatus | KN native stream; inherited exact oracle/negative RPC evidence plus affected90-stream live load/reconnect |

Intervals are typed parameters/bindings, not separate REST route paths.5liquid
symbols on both venues retain140 BAR bindings (14 native intervals per venue),
plus510 daily universe products,500 of them additional to existing execution history.
Universe is BAR1d batch input, not510 extra WS subscriptions or an execution grant.
The original market catalog has716 bindings including4 legacy Spot/VN exclusions;
this run does not activate those exclusions.

Fresh history matrix (before final cold-worker scheduling patch):48 reads at
2500/5000/10000, range751-9869ms depending venue/interval and actual available
listing history. Each row count/short-history label is in the receipt. Under
final50 load:2500=3641/6909ms;5000=8150/11321ms (Binance/OKX). Four observations,
notp99. Hot isolation improved; history is still multi-second, not an instant
callback or a promised universal speedup. Final batch50 samples3912-4369ms in
the earlier matrix; whole-universe wall time is reported independently.

Five Binance deep3d provider windows have authentic timestamp discontinuities.
The history edge isolates typed failures per binding and never fabricates missing
bars. Recent demanded1m and daily acceptance are complete; do not certify deep3d
continuity across those specific old discontinuities. Venue/listing history can
be shorter than10k; the request is a lookback cap, not permission to invent rows.

### Capacity, Limits And Review Boundary

Whole measured cache:1,525,787BAR rows,1,171,251,296B used,1,169,522,688B RSS,
zero evictions. This includes execution plus universe, not just a376MB increment.
7,731,216 full-retention rows remain an extrapolation (~5.94GB before additional
allocator/buffer/staging headroom), NOT a filled-cap stress measurement.
Shadow Redis cap7GiB/maxmemory6.5GB does not mean7GiB was consumed. Production caps
and10k retention promises were not silently changed. Final release packet must
use measured current demand plus explicit growth/rebuild allowance.

Final50 sampled read-plane averageCPU: Query1/2=0.261/0.509 core, projectors=
0.078/0.080, streams=0.083/0.013, cache=0.041, isolated Kafka=0.229. This sums
about1.294 cores, but EXCLUDES production ingress/core, mirror and clients. It
is not the guide's full production-stack<=5CPU certificate. No Query throttling;
Stream-A0.7% throttled periods. Whole-host/new-old overlap is captured separately.

Claude must review this source/evidence tuple, replay oracle, SDK bounded retries,
atomic summary migration and complete memory budget before K5.3 paired handoff.
K5.3 rollout/rollback and remote release provenance remain the EXISTING approved
KN-5 work, not a new phase or permission to call shadow production.

### Whole Universe And Retry Closure

New source2dad966: **1,020/1,020 PASS**,510 daily products on both replicas,
255symbols per venue. Requested480rows, with explicit authentic shorter listing
history where applicable. Each255-symbol SDK call used51bounded chunks,0retries:

| Venue | Replica | Complete usable ms | Returned rows | Decoded HTTP body bytes |
|---|---|---:|---:|---:|
| OKX | Query1 |126498.411|104812|304267454|
| Binance | Query1 |129963.392|108250|313734265|
| OKX | Query2 |115681.395|104812|304267454|
| Binance | Query2 |119922.661|108250|313734265|

This is about1.9-2.2minutes for a255-symbol cold initialization, NOT milliseconds
for a single symbol and not a daily signal/execution path. Payload sizes above
are decoded response bytes, not encrypted wire bytes. No continuity/finality
or history-depth requirement was relaxed. The runner does not self-attest
provider provenance (`provenance_verified=false`); pair it with the isolated
read-committed mirror/history-edge setup and logs, not a fabricated true flag.

Earlier full2500/5000 universe attempts had12/8 RATE_LIMITED reads. Their exact
failed subsets recovered12/12 and8/8. Successful row/identity/window evidence
is inherited; these are NOT freshly rerun1020/1020 zero-rejection deep profiles.
To prove the new SDK retry path really executes, an additional real contention
probe made4concurrent5000-row requests for each alpha identity/replica:
**16/16PASS,24attempts,8typed retries**, exact5000rows each,5,322.7-19,417.3ms
including queue and retry. Negative source tests prove stale/auth/gap/snapshot
and mixed-partial responses are not silently retried or converted to success.

### Resource, Cleanup And Next Review

Separate597.5s whole-host/component capture distinguishes production from shadow.
New Query peaks580.7/488.3MB, no throttling/OOM; native projectors65.5/78.0MB;
streams113.5/85.0MB; cache1,164.7MB. Isolated Kafka cgroup peak1,610.5MB includes
page cache, not just heap;0OOM. Query average0.324/0.251core during that window.
Production ingress/core/Kafka are recorded separately in `resource-summary.json`.
Do not add maxima from different times into a claimed simultaneous RSS, or claim
final whole-stack<=5CPU from a read-plane subtotal. Rebuild/full-retention growth
and final paired deployment still use existing K5.3 gates, not this shadow receipt.

Cleanup:14owned test containers and2empty test networks removed;8test Python
image tags removed;35exact reclaimable cache references and9build contexts/Rust
target removed. Exact scoped TLS/private-secret copies deleted. One tested reader
image9e81171 and the native binary are retained for independent review; production
active/rollback and Claude's unrelated artifacts are unchanged. The guard removed
some stopped containers concurrently, so the first `rm` returned already-absent;
all owned IDs were verified absent, without broadening cleanup. No volume deletion:
pre-existing test Kafka and5anonymous test volumes are retained, alongside every
production/shared volume. This is NOT a blanket Docker cleanup claim.

Disk free121,209,298,944 ->122,719,633,408B, net+1,510,334,464B while production
continued running. All57outside-scope containers preserved image/restart/start;
comparison to pre-shadow start also finds no restart changes. No surviving owned
guard/watch/client session. Canonical `/home/bobby/data_layer`, one checkout on
`feat/consumer-endpoint-benchmark`; main is`e6955f3`, the release-closure docs
commit after tag`v2.1.0` (`v2.1.0-1-ge6955f3`); no push/merge.

Machine-readable [acceptance index](KN5_ASTRA_ACCEPTANCE_INDEX.json) contains37
hashed artifacts, exact source/image tuple, per-file add/delete counts from718631d,
latencies and all material limits. Review the12source/testing commits aftere8d3435
plus that first ownership/entitlement correction; don't treat this docs commit as
one magically tested binary. Main plan includes every tested slice and failed run.

**Handoff to Claude:** review atomic diagnostic summaries/generation safety,
SDK backpressure/partial semantics, non-executable replay oracle, source ownership,
and new evidence denominators. If accepted, continue the existing paired deployment
and release packet K5.3-K5.6. No additional phase. Do not rerun unrelated certified
venue/domain tests just because a documentation SHA changes; packaging/SDK changes
still need affected smoke. No release/production readiness assertion before review
and actual handoff, and no unconditional execution permission from this data proof.
