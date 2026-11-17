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

## 3. "After": to be filled after the rollout

Use the same command set and the same caps. For quantity 1, the coordinator supplies readable identity files.
Compare the following, before vs after:
- reference:batch and snapshot request latency per replica;
- writer requests/min;
- per-replica `qdl.stall slow_request`/min.
