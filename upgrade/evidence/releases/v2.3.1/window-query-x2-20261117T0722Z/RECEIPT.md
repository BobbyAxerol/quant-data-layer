# DL v2.3.1 Query x2 window receipt (2026-11-17)

**Approval:** owner GO for exactly query_kn_2 then query_kn_1, from a235c0a1 to 26c3b051. Host lock 07:23:24Z to 08:32:22Z (released).

## R0: PASS
- NEW_ENV differs only in QDL_IMAGE_QUERY.
- The render differs only in query_kn_1 and query_kn_2. binance_bar_edge stays on a235c0a1.

## R1 baseline: 07:23-07:33Z
- Query CPU: 0.73-0.75 (kn_1) / 0.68-0.72 (kn_2) vCPU.
- slow_request: 98-143/min.
- MARK/QUOTE p99: 665/763 ms.
- DL: about 6.0 vCPU.

## R2: query_kn_2 PASS at 07:42:43Z, on the 3rd attempt
- Attempts 1 and 2 were rolled back automatically, because of MY gate-script defects: a missing space in the B4 smoke call, and a 2.3.0-rc3 client image without a snapshot-batch parser (ImportError).
- Both rollbacks restored a235c0a1 healthy.
- Side effect of 3 recreates in about 7 min (07:36-07:42Z):
  - writer stream reconnect RATE_LIMITED;
  - native worker NATIVE_HEALTH_BLOCKED x2, recovered, writer RestartCount 0;
  - freshness MARK p99 1047 / QUOTE p99 1605 at 07:40Z, recovered by 07:44Z.
- B4 gate, from attempt 3 on (b4_gate.py): 18/18. Non-BAR batches return 200; BAR batches are refused INVALID_ARGUMENT, which is by design (router.py:1583-1592, no warmup in a snapshot batch).

## R3: query_kn_1 PASS at 07:48:09Z, first attempt, clean
- B4 18/18, writer refusals 0, 246/246.

## R4 soak: 07:48-08:18Z
| Gate | Result |
|---|---|
| MARK p99 | 765 ms, PASS |
| QUOTE p99 | 870 ms, PASS |
| DL vCPU | median 5.97, max 6.45, PASS |
| stall > 2 s | 0, PASS |
| slow_request (R1 +10 %) | **FAIL**: median 196/min, max 338/min |
| Query balance | **imbalanced**: kn_1 0.39-0.42 vs kn_2 0.88-0.99 vCPU |

**Root cause (proved by mapping socket inodes to processes):**
- The writer's Rust native client (market-data-writer, restarted 07:42Z in the R2 churn) holds 86 ESTABLISHED connections to kn_2 and 1 to kn_1.
- The Python W-QRR transport is balanced (4/4).
- W-QRR covers only the Python transport. The Rust writer uses the alias and stuck to kn_2 after the kn_1 recreate.
- This is a TS writer defect, NOT the Query image. A Query rollback would not fix it.

## R5: four-quantity latency, interleaved, 08:21-08:32Z
- Run: /home/bobby/.local/state/qdl-v2/v231-latency-q12/run-after-v231-20261117T082137Z
- PASS: feed_status 285, snapshot 279, reference_batch 230, instrument 204, readiness_check 36, history 20, warmup 20, warmup_batch 4, snapshot_batch 18 (QUOTE-only batches).
- Expected or explained failures:
  - snapshot_batch INVALID_ARGUMENT 18: BAR items, by design;
  - RATE_LIMITED 8 (snapshot 7, feed_status 1): the benchmark shares the TS identity quota with the writer;
  - DATA_NOT_READY 4: quiet instruments.
- Interleaving shows the load effect: kn_2 (loaded by the writer) is about 5-8x slower at p50 than kn_1.
  - snapshot QUOTE p50/p95: 11/69 ms on kn_1 vs 89/238 ms on kn_2.
  - snapshot_batch QUOTE x8: 56/140 ms on kn_1 vs 225/314 ms on kn_2.

## Decision pending from the owner
- Tag v2.3.1 with R4 slow_request recorded as FAIL with the cause attributed to the writer.
- Writer Rust client rebalance.
- The TS v2.1.5 fix: per-replica pools in the Rust writer.

**Not touched:** Kafka, redis, cache, cores, projectors, streams, ingestors, bar_edge, the writer, the Paper/PM roles.
