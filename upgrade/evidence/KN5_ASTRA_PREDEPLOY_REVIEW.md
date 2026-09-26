# KN-5 Astra Corrections / Claude Review Receipt

Status: **SOURCE_TESTED_PENDING_CLAUDE_REVIEW. NOT DEPLOYED. NOT A RELEASE CERTIFICATE.**
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
