# DL v2.3.1 binding-endpoint latency report (owner's four quantities)

Status:
- **"before" measured 2026-11-17 02:47-02:56Z** on the current runtime. That is v2.3.0 images (rust `58bdd282`,
  python `a235c0a1`), stage B r7, W-A1 bw50, and TS writer image `942aad98`.
- **"after" not yet run.** It follows the v2.3.1 rollout (`ROLLOUT_PLAN.md` section 5) with the same command set.
- The run was read-only and light, under the coordinator-held host measurement lock (owner "trading system upgrade DL
  v2.3.1 latency-before"). It made no runtime change.

Raw evidence (outside Git, not copied): `~/.local/state/qdl-v2/v231-latency-before-20261117T0250Z/`.
- `capture.bin` (326,985 canonical records, sha256 `715092385eaf...`);
- `redis-sweep-{1,2,3}.txt`;
- `inventory/`;
- `profile.json` (paths only).

Repository copies: `latency-before-20261117T0247Z/`, with `SHA256SUMS`. It holds the per-manifest delivery reports
(`delivery/*.json`, one row per declared requirement) and the two summaries.

## 1. Method, per quantity

| # | Quantity | Unit | Measurement point | Tool / command |
|---|---|---|---|---|
| 1 | Request latency | ms | Query call to validated response, from a disposable consumer container on `executor_network` | `scripts/benchmark_consumer_endpoints.py run` (rounds 1, 2 rps capped at rpm/600, batch 8, timeout 30 s, max 900 s per identity, no Subscribe, gaps SAFETY_BLOCKED) |
| 2 | Durable event age | ms | `measured_freshness_ms` in the same responses | same run |
| 3 | Delivery lag, venue event -> canonical | s | Kafka `CreateTime` of the `md.canonical.v2` record minus `source_event_time` (BAR: minus close time) | read-only console-consumer capture, 300 s idle timeout / 400,000 messages; then `scripts/report_stable_endpoints.py --capture --catalog cores-bw50/rust_core.json --manifest <each> --json` offline (`--network none`) |
| 4 | End to end to the consumer cache | ms | TS writer Redis `redis_marketdata` db1 | read-only `SCAN`/`GET` sweeps (3 x 246 keys, 10 s apart): `TIME` minus `observed_at_ms`, and the stored `quality.freshness_ms` |

Manifests are the deployed stage-B r7 copies, `upgrade/evidence/releases/v2.3.0/declaration-stage-b-r7/repo/consumers/stable/`.
There are 6 manifests and 1,104 declared requirements:
- trading-system.paper.stable r14: 247;
- alpha.okx.paper.stable r16: 375;
- alpha.binance.paper.stable r16: 380;
- alpha.vn.paper.stable r1: 2;
- monitoring.multivenue.stable r2: 5;
- qdl.crypto.reference-l2.stable r7: 95.

## 2. "Before" results

### Quantities 1 and 2: NOT MEASURED

- **trading-system.paper.stable.** The current-mesh TLS identity directory
  `~/.local/state/qdl-v2/tls/gen-20261019T084200Z/identities` is `root:root 0750`. The tool's
  `Path(...).resolve(strict=True)` raises `PermissionError` before any call. This was reported and not worked around.
- **alpha.binance.paper.stable.** Its current-mesh TLS identity is in the same root-only directory.
- **alpha.okx, alpha.vn, monitoring.multivenue, reference-l2.** No registered credential set was found in the existing
  benchmark profiles. No identity was substituted.
- **Inventory (no network).** It reports 104 TS products. The tool mounts `config/v2` and `consumers` from the repo tree,
  which is still the v2.3.0 r13 form. The stage-B r7 configs are carried as evidence, not in `config/`. A runtime-exact
  run therefore needs that tree, or the r7 evidence copy mounted in its place, as well as readable identities.
- **Proxy for quantity 2 on the TS path.** Quantity 4's `freshness_ms` at write is the Data Layer's measured freshness
  as the writer received it.

### Quantity 3: delivery lag, s (n = samples; p50 = median of row p50; p95 = max over rows of row p95)

| Manifest | Venue | Feed | Interval | Rows measured / declared | n | p50 | p95 | max |
|---|---|---|---|---:|---:|---:|---:|---:|
| trading-system r14 | OKX | QUOTE | - | 97/97 | 103,121 | 0.095 | 0.223 | 0.332 |
| trading-system r14 | OKX | MARK_INDEX_PRICE | - | 97/97 | 98,361 | 0.700 | 2.050 | 2.157 |
| trading-system r14 | OKX | TRADE | - | 5/5 | 11,093 | 0.078 | 0.268 | 0.534 |
| trading-system r14 | OKX | BOOK_DELTA | - | 6/6 | 19,574 | 0.066 | 0.131 | 0.391 |
| trading-system r14 | OKX | BOOK_SNAPSHOT | - | 6/6 | 2,496 | 0.072 | 0.180 | 0.391 |
| trading-system r14 | OKX | BAR | 1m | 5/5 | 30 | 0.756 | 1.394 | 1.394 |
| trading-system r14 | BINANCE | QUOTE | - | 5/5 | 21,262 | 0.088 | 0.133 | 0.220 |
| trading-system r14 | BINANCE | TRADE | - | 5/5 | 38,335 | 0.083 | 0.157 | 0.226 |
| trading-system r14 | BINANCE | MARK_INDEX_PRICE | - | 5/5 | 1,726 | 0.124 | 0.186 | 0.215 |
| trading-system r14 | BINANCE | BOOK_DELTA | - | 5/5 | 16,094 | 0.068 | 0.095 | 0.196 |
| trading-system r14 | BINANCE | BOOK_SNAPSHOT | - | 5/5 | 2,077 | 0.069 | 0.113 | 0.196 |
| trading-system r14 | BINANCE | BAR | 1m | 5/5 | 30 | 0.292 | 3.050 | 3.050 |
| trading-system r14 | HNX | TRADE | - | 0/1 | 0 | - | - | - |
| alpha.okx r16 | OKX | BAR | 1m / 3m / 5m | 15/15 | 30 / 10 / 5 | 0.756 / 0.798 / 0.881 | 1.394 / 1.127 / 1.063 | 1.394 |
| alpha.okx r16 | OKX | QUOTE, TRADE, BOOK_*, MARK_INDEX | - | 25/25 | see TS rows (same records) | | | |
| alpha.binance r16 | BINANCE | BAR | 1m / 3m / 5m | 15/15 | 30 / 10 / 5 | 0.292 / 6.621 / 6.353 | 3.050 / 7.355 / 6.441 | 7.355 |
| alpha.binance r16 | BINANCE | QUOTE, TRADE, BOOK_*, MARK_INDEX | - | 25/25 | see TS rows | | | |
| monitoring r2 | BINANCE / OKX | TRADE | - | 4/4 | 27,318 / 9,441 | 0.084 / 0.078 | 0.153 / 0.221 | 0.534 |
| monitoring r2 | HNX | BAR | 1m | 0/1 | 0 | - | - | - |
| reference-l2 r7 | BINANCE / OKX | BOOK_*, MARK_INDEX | - | 30/30 | 2,886-12,097 per group | 0.069-0.625 | 0.099-1.561 | 2.116 |
| alpha.vn r1 | HNX | BAR 1m, TRADE | | 0/2 | 0 | - | - | - |

Not measurable in a 6-minute capture:
- BAR >= 15m: no bar closed in the window.
- The on-demand products (FUNDING_RATE, OPEN_INTEREST, LONG_SHORT_RATIO, TAKER_FLOW, BASIS, CONTRACT_METADATA). These
  are Query reference reads, not canonical stream feeds, so they belong to quantity 1.
- The alpha manifests carry 255 BAR 1d rows each.

Observation, not root-caused: Binance BAR 3m/5m are delivered 6.3-7.4 s after close and Binance BAR 1m has a p95 of
3.05 s, while OKX bars arrive within 1.4 s. This matches the Binance REST bar path with its 6 s guard. I have not
checked the code in this report.

### Quantity 4: consumer cache (TS writer Redis), ms

The values carry `observed_at_ms` (the venue event) and `quality.freshness_ms` (age at writer receive). They carry no
write timestamp. "Age at read" is therefore an **upper bound** on end-to-end, because it includes the time since the
last update. The writer-side end-to-end figure stays TS-owned: it comes from the writer delivery trace in the writer
rollout window.

| Key family | Venue | n | age at read p50 / p95 / p99 / max | freshness_ms at write p50 / p95 (n) |
|---|---|---:|---|---|
| execution_quote | OKX | 291 | 884 / 8,730 / 15,842 / 58,725 | 769 / 8,323 (291) |
| execution_quote | BINANCE | 15 | 184 / 364 / 419 / 419 | 111 / 152 (15) |
| execution_mark_index_price | OKX | 291 | 1,358 / 2,425 / 2,895 / 3,334 | 968 / 2,038 (291) |
| execution_mark_index_price | BINANCE | 15 | 1,091 / 1,308 / 1,346 / 1,346 | 856 / 1,030 (15) |
| execution_trade | OKX | 15 | 875 / 2,111 / 2,333 / 2,333 | 105 / 113 (15) |
| execution_trade | BINANCE | 15 | 974 / 2,734 / 5,272 / 5,272 | 96 / 119 (15) |
| execution_book_delta | OKX | 18 | 146 / 220 / 242 / 242 | 106 / 132 (12) |
| execution_book_delta | BINANCE | 15 | 170 / 213 / 222 / 222 | 86 / 98 (5) |
| execution_book_snapshot | OKX | 18 | 527 / 1,031 / 1,088 / 1,088 | not stored |
| execution_book_snapshot | BINANCE | 15 | 685 / 1,058 / 1,075 / 1,075 | not stored |

MARK `evaluated_at_ns - source_event_time_ns`: n 306, p50 942 ms, p95 1,940 ms, max 2,644 ms.

The OKX QUOTE tail is event age on quiet instruments. The QUOTE liveness probe renews the same record
(`renewed_same_record`), so `observed_at_ms` stays at the last real quote. Canonical delivery for the same feed is
p95 0.22 s (quantity 3).

## 3. "After W-QRR" (TS writer replicated Query transport, writer `cf88571c`), measured 2026-11-17 04:22-04:34Z

Runtime: the DL side is unchanged (Query `a235c0a1` x2). The writer splits its requests 51/49 across the replicas
(coordinator receipt `~/.local/state/qdl-v2/wqrr-window-20261117T0310Z/RECEIPT.md`). The run was made under the
coordinator's host lock.

Evidence: `latency-after-wqrr-20261117T0422Z/`, with `SHA256SUMS`.
- `report.md`: the benchmark tool's per-requirement table;
- `summary-q12.txt`: per operation x replica x venue x feed, with response/usable p50/p95/max, freshness_ms and age at
  the client;
- `q2-event-age.txt`;
- `q4-redis-cache-age.txt`.

Raw data: `~/.local/state/qdl-v2/v231-latency-q12/run-after-wqrr-20261117T042211Z/report.json`.

### Quantity 1: request latency, ms

- Scope and method: identity `trading-system.paper.stable`, manifest r14, 246 products, usable latency (call to a
  validated response) per replica.
- Client: an owner-approved root client (`--rm`, read-only identity mounts).
- Pacing: 2 rps; rounds 1, so each row has n = 1 per replica.
- Code: tool commit `f4b4b37`. The selector-context fix ran from the worktree and was committed afterwards as
  `1a01b20`, with the same content.

| Operation | Venue / feed | query_v2_1 (kn_1) p50 / p95 / max (n) | query_v2_2 (kn_2) p50 / p95 / max (n) |
|---|---|---|---|
| snapshot | OKX QUOTE | 75.9 / 191.6 / 191.6 (97) | 13.5 / 157.0 / 157.0 (97) |
| snapshot | OKX TRADE, BOOK_DELTA, BOOK_SNAPSHOT, BAR 1m | 16.8-61.5 / <= 149.6 (5-6 each) | 12.5-26.0 / <= 140.6 (5-6 each) |
| snapshot | BINANCE QUOTE, TRADE, BOOK_*, BAR 1m | 35.4-80.7 / <= 255.9 (5 each) | 22.4-132.3 / <= 196.2 (5 each) |
| feed_status | OKX QUOTE | 77.3 / 169.0 (97) | 15.5 / 165.5 (97) |
| feed_status | other feeds | 13.1-83.7 / <= 190.2 | 9.2-57.7 / <= 162.1 |
| reference_batch, single item | OKX MARK_INDEX | 122.8 / 336.2 / 412.7 (97) | 17.0 / 220.4 / 400.7 (97) |
| reference_batch, single item | BINANCE MARK_INDEX | 42.7 / 361.6 (5) | 78.2 / 180.4 (5) |
| reference_batch, batch of 8 | OKX MARK_INDEX | 105.1 / 198.9 (9) | 36.2 / 48.7 (9) |
| reference_batch, batch | BINANCE+OKX MARK_INDEX | 158.2 / 237.2 (4) | 42.3 / 45.6 (4) |
| instrument | MARK_INDEX instruments | 5.3 / 30.9 (91) | 40.1 / 83.6 (91) |
| instruments (one page of 500) | - | 325.3 (1) | 61.7 (1) |
| readiness / readiness:check | - | 82.9 (1) / 122.8-559.7 (18) | 48.6 (1) / 110.6-501.0 (18) |
| history BAR 1m | BINANCE / OKX | 674.9 / 555.4 p50 (5 each) | 756.4 / 863.6 p50 (5 each) |
| warmup BAR 1m | BINANCE / OKX | 1,042.3 / 1,026.4 p50 (5 each) | 1,305.8 / 1,234.1 p50 (5 each) |
| warmup:batch BAR 1m | BINANCE+OKX / OKX | 9,943.6 / 1,679.0 (1 each) | 9,951.2 / 1,730.6 (1 each) |
| snapshot:batch (B4) | all | FAIL `INTERNAL_ERROR`, 0 usable (13 batches) | FAIL, same |

Notes:
- **snapshot:batch FAIL is expected.** The route is new in v2.3.1, and Query still runs v2.3.0 `a235c0a1`. This is
  the R2/R3 smoke gate after the recreate.
- **The replica asymmetry is a measurement artifact, not a Query defect** (RCA 2026-11-17 about 04:45Z, read only:
  logs, `/proc/net/tcp` via `docker exec`, `docker stats`; the coordinator accepted it).
  - In this run, kn_1 was about 5-7x slower than kn_2 at p50 on hot single reads (OKX QUOTE snapshot 75.9 vs 13.5 ms;
    OKX MARK reference 122.8 vs 17.0 ms).
  - Cause: the tool measured the replicas sequentially. `scripts/benchmark_consumer_endpoints.py` (at f4b4b37,
    `for replica in profile["queries"]:` around the whole case list) ran all 547 kn_1 samples first, at about
    04:22:34-04:27Z, and then all kn_2 samples, at about 04:27-04:32Z. The two replicas were therefore measured in
    different host-load phases (W-QRR scoring and PM-L1/L3 starting, load 20-25).
  - Server load is symmetric (10 minutes to about 04:44Z):
    - `qdl.stall slow_request`: kn_1 823 vs kn_2 886. Of these, `reference:batch` is 695 vs 693; stall p50
      311 vs 305 ms.
    - `loop_lag` is 55-75 ms on both.
    - CPU is 70-78 % vs 69-80 % (3 samples); RSS is about 300 MiB on each.
    - Hot-lane refusals are low on both: RATE_LIMITED 5 vs 1.
  - The only client peer on either replica (150 snapshots each) is the writer `172.18.0.12`, with 50 vs 46
    connections. The hypothesis "another consumer pins the alias to kn_1" is rejected.
  - Fix in the tool: replicas are now interleaved per case, the first replica alternates by case, and each sample
    records `started_wall_ms` (test `test_replicas_are_interleaved_per_case_with_alternating_first_and_wall_time`).
    The "after v2.3.1" run uses it.
  - Follow-up, TS-owned, not in this DL release: `ts-gateway`, `ts-risk`, `ts-performance` and `ts-paper` still use
    the alias `DATA_LAYER_V2_QUERY_URL=https://qdl-v2-query:8200`, which is a single pool. They are the remaining
    adopters of SDK 2.6.0 `build_replicated_rest_query`. They are low volume: no connection was held in the samples.
- Not measured: `stream` (Subscribe disabled by the plan, so no stream slots are taken) and `gaps` (SAFETY_BLOCKED).
- Not measured: the other identities (alpha.okx, alpha.binance, alpha.vn, monitoring, reference-l2), because only
  the TS identity was approved.

### Quantity 2: durable event age, ms

These are the server's `quality.freshness_ms` and the client-side `event_age_at_client_ms` from the same responses.
Both replicas are pooled.

| Operation | Venue | Feed | n | freshness p50 / p95 / max | age at client p50 / p95 |
|---|---|---|---:|---|---|
| snapshot | OKX | QUOTE | 194 | 802 / 10,343 / 29,529 | 809 / 10,373 |
| snapshot | OKX | TRADE | 10 | 1,149 / 6,469 | 1,153 / 6,472 |
| snapshot | OKX | BOOK_DELTA | 12 | 369 / 428 / 463 | 375 / 433 |
| snapshot | OKX | BOOK_SNAPSHOT | 12 | 940 / 1,184 / 1,258 | 962 / 1,196 |
| snapshot | BINANCE | QUOTE | 10 | 345 / 405 | 356 / 436 |
| snapshot | BINANCE | TRADE | 10 | 471 / 4,634 | 476 / 4,655 |
| snapshot | BINANCE | BOOK_DELTA | 10 | 316 / 480 | 321 / 488 |
| snapshot | BINANCE | BOOK_SNAPSHOT | 10 | 557 / 1,263 | 583 / 1,297 |
| snapshot / warmup / history | BINANCE | BAR 1m | 10 each | 35,318-37,545 / <= 60,462 | 35,320-37,806 / <= 59,987 |
| snapshot / warmup / history | OKX | BAR 1m | 10 each | 36,794-39,060 / <= 60,614 | 96,800-99,367 / <= 121,215 |

BAR rows: the age of the latest *closed* 1m bar is 0-60 s by construction. For OKX, the client-side age is about
60 s above `freshness_ms`. That points to a different reference point (bar open vs close) for OKX in the client
computation; I have not checked the code. Quiet OKX QUOTE instruments explain the QUOTE tail, as in section 2.

### Quantity 3: delivery lag

Not re-measured after W-QRR. W-QRR changed only the TS writer's Query client; the DL publish path is unchanged.
The "before" capture (section 2) stands. A new capture needs a container under the lock. It is planned for
"after v2.3.1".

### Quantity 4: consumer cache (TS writer Redis), ms; read-only sweeps 04:33Z, 3 x 246 keys

| Key family | Venue | n | age at read p50 / p95 / p99 / max | freshness_ms at write p50 / p95 (n) |
|---|---|---:|---|---|
| execution_quote | OKX | 291 | 727 / 9,357 / 11,656 / 22,473 | 160 / 8,751 (291) |
| execution_quote | BINANCE | 15 | 153 / 309 / 352 / 352 | 104 / 174 (15) |
| execution_mark_index_price | OKX | 291 | 1,453 / 2,506 / 2,772 / 3,123 | 1,195 / 2,313 (291) |
| execution_mark_index_price | BINANCE | 15 | 758 / 1,341 / 1,567 / 1,567 | 407 / 873 (15) |
| execution_trade | OKX | 15 | 761 / 2,047 / 3,167 / 3,167 | 86 / 123 (15) |
| execution_trade | BINANCE | 15 | 766 / 1,178 / 2,261 / 2,261 | 106 / 121 (15) |
| execution_book_delta | OKX | 18 | 179 / 251 / 269 / 269 | 135 / 176 (12) |
| execution_book_delta | BINANCE | 15 | 208 / 233 / 294 / 294 | 98 / 168 (2) |
| execution_book_snapshot | OKX | 18 | 686 / 1,058 / 1,107 / 1,107 | not stored |
| execution_book_snapshot | BINANCE | 15 | 746 / 966 / 1,063 / 1,063 | not stored |

MARK `evaluated_at - source_event`: n 306, p50 1,190 ms, p95 2,284 ms (before: 942 / 1,940).

Before vs after W-QRR (quantity 4, OKX):
- QUOTE freshness at write p50: 769 -> 160 ms;
- MARK age p50: 1,358 -> 1,453 ms;
- MARK freshness p50: 968 -> 1,195 ms.

These are single 30-second samples, so treat them as indicative, not as a gate.

## 4. "After v2.3.1": to be filled after the Query recreate

Run the same q1/q2 command (`run_q12.sh after-v231`), plus a quantity-3 capture and the quantity-4 sweeps.
Compare per replica.
