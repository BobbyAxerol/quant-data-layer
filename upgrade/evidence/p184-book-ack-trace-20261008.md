# BOOK Consumer Exact-Event Trace

Status: MEASURED_CONSUMER_BACKLOG_OPEN. Not release acceptance.

## Approved Runtime Action

Owner approved one recreate of market_data_service_p184w, same immutable image
sha256:5849d7167561273af08bda2103c38cec5f4480f7a8809ea592c57abff7716ea1.
Only TS_DELIVERY_TRACE_UNTIL_UNIX_NS changed: 300s,4096records/process.
Identity, binding r22, SDK2.0.7, mounts, Redis/cursors, resource settings and
network configuration preserved. Same-image prior-config rollback compose is
private in book-trace-20261008/rollback.compose.json. No two-writer interval.
Trace expired naturally; no additional recreate needed to disable it.
Writer running, restart0/OOMfalse. No preexisting non-writer role changed.
A concurrent unrelated trusting_ptolemy container appeared: whole-host equality
assertion therefore failed; it was not created or cleaned by this operation.

Canonical observer initial330s/10000 request rejected by tool; reduced to
300s/4096, no rejected action executed. Existing read_committed assigned reader,
BTC/ETH BOOK only; no group join, commits, Stream quota or orders. It hit4096
records at150.136s. Observer wall time is durable visibility, NOT commit time.

## Matched Evidence

Four writer files,16384 attempts; exports capped,1 omitted at export and6
in-flight not recorded. Canonical4096 rows. Exact join on event hash, offset,
feed, source hash, with identity/generation/native sequence/envelope clocks
validated.3425 matched attempts:2885 EVAL1 APPLIED/NONE,523 rejected,17 other.
Zero validation failures/partition ambiguities. Exact counts:
```json
{
  "canonical_observations": 4096,
  "invalid_canonical_rows": 0,
  "non_book_canonical_rows": 0,
  "trace_records": 16384,
  "non_book_trace_records": 1181,
  "no_exact_join_key": 102,
  "unmatched_trace_records": 11676,
  "ambiguous_trace_records": 0,
  "validation_failures": 0,
  "exact_validated_matches": 3425,
  "matched_EVAL1_APPLIED": 2885,
  "rejected_records": 1784,
  "other_non_success_records": 64,
  "dropped_at_export": 1,
  "in_flight_at_export_not_recorded": 6,
  "canonical_keys_without_validated_match": 671,
  "ambiguous_canonical_keys": 0
}
```

Unmatched/capped observations are NOT loss. Rejected older updates are NOT
orders rejected: trace reports no EVAL for those older BOOK observations.
Success latencies must not hide their separate denominator. The runtime source
returns False on relation<0 before CAS; no inference that every replay is faulty.

| Same-event BOOK_DELTA | n | SDK->writer p95 ms | Writer->Redis ACK p95 ms | SDK->Redis ACK p95 ms | Event->Redis ACK p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| BINANCE:USD_M:BTCUSDT BOOK_DELTA | 542 | 7345.375 | 307.010 | 7595.909 | 13490.411 |
| BINANCE:USD_M:ETHUSDT BOOK_DELTA | 587 | 3766.698 | 284.820 | 3890.387 | 5506.640 |
| OKX:SWAP:BTC-USDT-SWAP BOOK_DELTA | 622 | 3882.532 | 301.008 | 3956.305 | 5815.562 |
| OKX:SWAP:ETH-USDT-SWAP BOOK_DELTA | 635 | 3355.554 | 249.270 | 3432.634 | 6966.953 |

SDK/writer/ACK durations use process monotonic clocks; event age uses wall
clocks. Quantiles computed per stage, not subtracted. No p99 with n<100.
The exact OKX BTC sample still has pre-SDK durable-observer->SDK p95
3527.101ms; this signed measurement includes observer scheduling and replay.
It cannot isolate Stream delivery from caller read pacing, and is not commit lag.

## What This Establishes

A material wait occurs AFTER SDK return and BEFORE writer entry; provider quiet
alone cannot explain that measured interval. Prior writer intervals overlap the
long waits in the same worker. For waits>1s, median overlap fraction1.0 on BTC
OKX(n598) and Binance(n681). This is wall-interval overlap, NOT CPU accounting
or proof which serialization/encoding/I/O stage dominates. Both BTCs share the
same worker, consistent with existing IDR-081; ETH is affected too.

Fresh120.231s cache probe OBSERVED, all60groups. BOOK_DELTA max age still
OKX BTC11589.312ms(n154), Binance BTC14553.248ms(n138). These time samples
are not the exact trace subset and are NOT a paired improvement benchmark.
Heartbeat13/13 READY60/session60, reported execution-ready50, not60. No claim
that status READY means every feed can be used for an order.
Cursor counters acknowledged14961->35617, commits10603->25041,errors0,pending
8->0,peak29. Cumulative io_seconds153.811->384.214 over the120s window is
aggregated acrossworkers and includes lock/executor waits; NOT CPU or per-event
cursor latency. SDK ACK/cursor correlation remains NOT_MEASURED.

Bounded Docker log check:8writer TimeoutError, no denied/trace-export failures;
Stream stdout contained no matching overflow, but mounted logs were NOT exhaustively
read. Do not declare overflow fixed from this check. Restart-window/trace sampling
does not replace steady-state acceptance or prove trace has zero overhead.

## Narrow Handoff To Claude

1. Keep ownership of TS source; do not rerun E03/E04 or modify DL freshness.
2. Integrate planned venue/family ownership (IDR-081) with an explicit single-writer
   packet. It addresses shared BTC worker, not all ETH delay by itself.
3. Profile current project_book_deltas preparation, prior per-item store_advancing
   MGET/CAS, scheduler and SDK ACK as one bounded batch. Include batch/event IDs,
   ACK admission/completion and cursor persistence correlation. Reuse this trace.
4. Optimize measured batch path while maintaining per-product sequence, generation
   fence, CAS adoption and deadline. Prefer reducing repeated serialization/round
   trips over unrestricted concurrent writes. Do not advance cursor before all
   intended batch effects are applied or safely recognized as replay.
5. Regression: delayed/failed CAS, partial batch failure, crash before/after ACK,
   generation/reset, old replay, same/different venue ownership, slow consumer
   isolation. Then one affected BOOK/caller comparison; no full catalogue.
6. If pre-SDK delay remains after caller catches up, correlate offered/dequeued
   Stream counters with transport and caller pacing before changing DL service.

This run closes measurement activation, not the throughput defect. No TS source
was edited; no unreviewed caller optimization deployed.

## Reproduction, Cleanup And Provenance

Source analyzer scripts/p184_book_trace_join.py;15 unittest PASS,0skip.
Command: python3 -B -m unittest discover -s tests -p test_p184_book_trace_join.py -v.
Use CLI --canonical canonical/observations.jsonl --trace delivery-*.json (repeat
flag perfile) --output new exclusive path. Negative identity/source/clock/
generation/offset/partition tests included; malformed evidence fails closed.
Evidence root: /home/bobby/.local/state/qdl-v2/book-trace-20261008.
fresh-summary-v2.json SHA256 e9d776f78b8eb587ce4ee1df48acbfe88a71b8bdacbb135900e7fb70b4073cd9
Original summary retained. Packet/env snapshots0600; no credential output/Git.
All observer processes completed; no new image/build/container/network/volume.
Docker inventory86images,69containers,196cacheentries,0Breclaimable BuildKit.
No cleanup of concurrent artifacts or rollback. Disk free98887442432bytes at end.
Canonical /home/bobby/data_layer main5bf5cd5, publishedv2.2.2; active worktree
/home/bobby/.worktrees/data-layer-p184-closure fix/p184-dl-closure. ExistingDL
core658a9570,ingestor0f6876e1,Query/BAR51450612,projectora696cfba,Stream1f13408f
unchanged. Runtime fullidentity receipts retained; v2.2.3HOLD. No push/merge.
