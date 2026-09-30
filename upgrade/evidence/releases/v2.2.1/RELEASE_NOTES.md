# Quant Data Layer v2.2.1

## Scope
Kafka-native execution-readiness repair, retaining V2 primary and policy-limited V1 fallback. No broker orders, TS/alpha rollout, Kafka offset reset, cache flush, or history deletion.

- Rust core flushes against an absolute batch deadline; arrivals cannot extend the deadline indefinitely.
- Existing transactional quarantine carries bounded, generation-fenced L2 resnapshot feedback to the existing venue ingestor. BOOK recovery does not restart TRADE/QUOTE lanes.
- Fair socket polling and coalesced control checkpoints prevent feedback pressure from starving live input. Genuine gaps still fail closed.
- Includes additive OKX inverse sandbox binding and previously integrated realm/diagnostic fixes; not a grant of new alpha execution rights.
- SDK2.0.6 gives existing typed refusal diagnostics a distinct reproducible artifact identity. API schema remains2.0.0; existing consumer pins are unchanged.

## Evidence
64 entitlement products x two Query replicas:128 typed views,126 usable, two OKX TRADE refusals matched to latest public trade ID/time at observation. All MARK/QUOTE/BOOK/BAR preflight reads usable. Request-to-validated-usable observed maxima are below50ms in these small groups; they are not p99.

One300s production observation: paper60/60 and sandbox22/22 READY on11 distinct published heartbeats each. Four OKX transport disconnects produced12paper/8sandbox session refusals followed by recovery. This is fail-closed recovery, not continuous availability. Paper cache was measured176s after correcting the observer Redis selection; sandbox300s. Polling ages include up to1s observation delay and are not exact Redis commit latency.

Isolated authentic RF3 replay at unchanged core caps:330000 records across6partitions, bounded4k/s and5k/s windows, all final offsets caught up. Not indefinite5000/s per consumer or unlimited fanout. Whole runtime observation averaged4.70cores, sampled peak5.12, no cap increase.

## Deployment And Rollback
Three Rust cores and two native ingestors use immutable658a9570c5fc...54023. Query/Stream, KN market projectors, BAR edge, V1 and TS images stay unchanged. Certificate contains full per-role digests. Native rollback is coref2040ac9...ca79 and ingestor7fe34806...f69f with original per-role config;7fe remains active in KN market projectors. No reset needed. Historical authority handoff digest is provenance, not a false claim of current executable identity.

## Limits
Binance3d and DNSE/VN V2 are excluded by owner. Quiet last trades can legitimately fail execution eligibility; no stale price is made fresh. Unchanged warmup/batch/reference/diagnostics capacity inherits dated v2.2.0 evidence, not a new full-catalogue benchmark. TS E lifecycle/order certification remains separate. See certificate and endpoint report for denominators, sampling limitations and observed reconnects.
