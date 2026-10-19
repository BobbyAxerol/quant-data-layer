# Amendment A4 - TS-eligibility rescore of G2 MARK BINANCE and G4 OKX_INVERSE (POST_HOC_OWNER_DECISION)

Status: **FROZEN, POST-HOC, applied to one run only** (`p184-final-campaign-20261018T123222Z`). Machine form:
[`config/v2/p184-stage100-a4-ts-eligibility.json`](../../../../config/v2/p184-stage100-a4-ts-eligibility.json) (schema
`qdl.p184.stage100-a4.v1`, version 1, sha256 `14958a681848192c8b9e93feaf0ee3137a1851c38fe6d6763f3bcf6ce0c81312`).
Rescore tool: `scripts/p184_a4_post_hoc.py` (sha256 `23b78e4ae7a367252cf8033aa64ee6357987c2117e7f6516679fd4c6efafba26`).
Analysis behind it: [drafts/A4_TS_ELIGIBILITY_TAIL_DRAFT.md](drafts/A4_TS_ELIGIBILITY_TAIL_DRAFT.md) and its JSON
(commit `5e496bf`; the A4 JSON `pinned` block carries the sha256 of both draft files).

## 1. What makes A4 post-hoc

- The counted run 123222Z was scored at 2026-10-18 13:09Z under G4-A1 + G4-A2 + A3: **G2 FAIL, G4 FAIL**.
- The draft was written after that, and the owner chose two of its options at ~14:45Z. A4 was frozen after that.
- So A4 is **not** a pre-registered rule. The frozen TS gate table says "a threshold changes only by a new recorded
  owner decision with its reason; no number is widened after measuring". A4 is that recorded decision, and it was taken
  after measuring. Every certificate surface shows the verdict without A4 next to the verdict with A4.
- Not edited: the frozen budget, the G4 addendum, G4-A1, G4-A2, A3, and the stored `scorecard.json` of every run.

## 2. Owner decisions (AskUserQuestion 2026-10-18 ~14:45Z, verbatim answers)

| Question | Answer | A4 rule |
| --- | --- | --- |
| G4 OKX_INVERSE | "M1: chấm theo p95 pooled (Recommended)" | `book_delta` / `book_snapshot` / `quote` `|OKX_INVERSE|` are gated by the frozen p95 limit of the pooled OKX row: 289.1 / 270.1 / 283.3 ms. The frozen-table p95 rule applies per window with n >= 20: FAIL when the p95 bucket lower edge is over the limit, and a bucket that straddles the limit is unresolved (NOT_MEASURED). The window max is REPORT_ONLY. Every admitted event above the replaced addendum limit (282.5 / 300.7 / 322.6 ms) must be retained by observer v7; otherwise the row is FAIL. |
| G2 MARK_INDEX_PRICE BINANCE | "Nới max lên 1200 ms (Recommended)" | `max <= 1200 ms` (0.6 x the TS 2000 ms MARK budget) instead of `max < 1000 ms`. `p95 <= 500 ms` is unchanged. |

The instruction behind both answers (~14:20Z): "Xem lại cách chấm, nếu do bản thân mình sai thì sửa nhanh rồi Release,
còn nếu là do OKX thì nới gate 1 xíu rồi pass rồi release. Miễn Risk Engine và Execution Eligibility của TS chấp nhận
là được".

Why M1 is a scoring correction and not a venue excuse (draft section 1.1, verified):
- Each inverse max limit is 1.2 x **one** baseline event (baseline window 1).
- The candidate schedule has a 180 s boundary window that the baseline never had.
- These are the only fast-feed G4 rows gated on a max. The same events pass the pooled `*|OKX|` rows on p95.
- The latency is real and **internal to DL** (venue -> DL about 30 ms). A4 changes the statistic, not the latency.

## 3. TS acceptance bounds (code reading at TS `bc713113`; services/adapters/rust identical to the measured writer `2deb90b0` = TS `30f50a91`)

| Feed | What TS enforces at use | Where |
| --- | --- | --- |
| Order budget | `market_max_age_seconds` 2 -> 2000 ms, min with the sealed `max_freshness_ms` | `services/risk_pool/ordinary.py:65-67`, `scripts/p184_prepare_runtime.py:285`, `services/market_data/runtime.py:55-60`, `services/market_data/execution_recovery.py:96-100, 235` |
| MARK_INDEX_PRICE | Quiet contract: no event-age check. Session liveness must be <= 45000 ms and component receipt age <= cadence (Binance USD-M 5000 ms). Fallback: event age <= 2000 ms. | `services/risk_engine/execution_market_context.py:188-211, 244-249, 270-281`; `adapters/market_data/data_layer_v2.py:781-782`; DL `qdl/runtime/production_catalog.py:81-84` |
| QUOTE | event age <= 2000 ms or quiet on-change session <= 2000 ms; refresh then `EXECUTION_RECOVERY_NOT_USABLE_QUOTE` | `execution_market_context.py:216-231, 280`; `execution_recovery.py:250-262`; Rust `view.rs:340-348, 434` |
| BOOK_SNAPSHOT | 2000 ms, only for L2 orders. P18.4 runs `BBO_EXECUTABLE`. The DL 60000 ms read contract does not permit execution. | `execution_market_context.py:322-331, 432-446` |
| BOOK_DELTA | Risk never reads it. In the writer: event age OBSERVE, session continuity <= 45000 ms, silence watchdog 60 s. | `execution_recovery.py:21`; `data_layer_v2.py:440-460`; `data_layer_bridge.py:95-104` |

Deployed requirement values: Paper manifest revision 11 `efd482fa...` (DOGEUSDT MARK 2000, BTC-USD-SWAP BOOK_DELTA
2000, BOOK_SNAPSHOT 60000, QUOTE 2000 ms). Run evidence for 123222Z:
- G6 D3_EXECUTION: MARK BINANCE n 2990, max age 1394 ms, refusals 0, unsafe 0. QUOTE OKX n 3582, refusals 0, unsafe 0.
- G9: 0 data-path eligibility losses.
- TS Python worker logs 12:00-13:59Z: no line above INFO.

## 4. Rescore of 123222Z (receipt `scorecard.a4-post-hoc.json`, sha256 `7a821d2d60c45ae290023e9434af7b320c06b906a4185dc299eee467a71fab7e`)

How the receipt was made:
- `scripts/p184_a4_post_hoc.py` ran the unchanged frozen scorer (`e179ec78`, the A3 pin). It first reproduced the stored
  `scorecard.json` (`b9e76a83...`) byte for byte.
- It then re-ran the scorer with only the four rows re-judged.
- Every gate other than G2 and G4 is byte-identical between the two runs; the tool refuses to write otherwise.

| Gate / row | Without A4 (frozen rule) | With A4 |
| --- | --- | --- |
| G2 MARK_INDEX_PRICE BINANCE | max 1082.9 ms >= 1000: **FAIL** | p95 418.6 <= 500, max 1082.9 <= 1200: **PASS** |
| G4 book_delta OKX_INVERSE | max 358.9 (w2), 387.1 (w5) > 282.5: **FAIL** | p95 147 / 185 / 198 / 143 / 170 <= 289.1 (n 113-169): **PASS**; 2 retained outliers |
| G4 book_snapshot OKX_INVERSE | max 301.4 / 399.9 / 311.2 / 406.7 > 300.7: **FAIL** | p95 166 / 193 / 183 / 160 / 174 <= 270.1: **PASS**; 5 retained outliers |
| G4 quote OKX_INVERSE | max 531.9 (w5) > 322.6: **FAIL** | p95 178 / 231 / 208 / 181 / 213 <= 283.3: **PASS**; 1 retained outlier |
| **G2 gate** | **FAIL** | **PASS** |
| **G4 gate** | **FAIL** | **PASS** (no other row fails, no NOT_MEASURED cause) |
| Campaign summary | PASS 10, FAIL G2 G4, NOT_MEASURED G12 G13 (separate runs) | PASS 12, NOT_MEASURED G12 G13 (credited separately: G12 PASS, G13 76/76) |

Retained outliers. All 8 are in the receipt with topic, partition, offset and clocks:
- book_delta: p1 225073169 at 358.9 ms and p1 225135820 at 387.1 ms.
- book_snapshot: p1 225059194, 225073224, 225085577, 225135837 and 225135821.
- quote: p3 655928413 at 531.9 ms.

Observer v7 recorded overflow 0 and admitted_seen equal to n in every window.

For comparison only (report-only, the tests' mirrored receipts, not certificate input):
- 053803Z: G4 stays FAIL on the out-of-scope `mark_index_price|OKX|` row. All three inverse rows PASS.
- 135719Z: `quote|OKX_INVERSE|` FAIL. Its windows 2 and 3 (723.5 / 711.8 ms) predate observer v7, so there is no
  retention, which is UNKNOWN = FAIL. G2 stays FAIL on QUOTE|OKX, which is out of scope.
- 074624Z (protocol not met): `book_delta|OKX_INVERSE|` FAIL for the same reason.
- 092255Z (invalid): `book_snapshot|OKX_INVERSE|` FAIL for the same reason.

Every p95 of every inverse row in those receipts is inside the pooled limit. A4 does not pass every historical
receipt.

## 5. Limits stated with A4

1. **Post-hoc.** A4 was adopted after observing 123222Z. A PASS under A4 is not evidence that a pre-registered rule
   passed.
2. **G4.** The inverse tails are internal to DL under load:
   - receive -> CreateTime 154-295 ms;
   - CreateTime -> observer 115-206 ms;
   - venue -> DL about 30 ms.

   Under A4 the inverse maxima (387.1 / 406.7 / 531.9 ms) are report-only and no max limit applies to these rows. The
   optimisation items stay open: IDR-116 / IDR-117 / IDR-118.
3. **G2.** It is unknown how much of the MARK BINANCE tail belongs to DL Query and how much to the TS writer's own read
   scheduling (wake, 500 ms minimum interval, 1 s refresh). The trace basis is `ADAPTER_REFERENCE_RETURN`.
   - IDR-126 adds wake/poll markers to the TS trace.
   - IDR-119 stays open for the Query MARK read path, after the attribution.
4. **G2 headroom.** A4 gives up the 2x headroom frozen on 2026-10-09; it is now 1.67x. Binance markPrice has a 1000 ms
   cadence, so a 1082.9 ms transit leaves the previous event older than 2000 ms for about 83 ms. On the MARK quiet
   contract, TS Risk applies no event-age check. This is from code reading; no read was captured at that instant.
5. **TS evidence.** No TS order admission was observed during the load. "No refusal" rests on the G6 and G9 samplers
   and the Python worker logs. The native Rust writer and Risk-engine logs were not read.
