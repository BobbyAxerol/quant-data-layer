# A4 draft - TS-eligibility-anchored tail limits (G2 MARK BINANCE, G4 OKX_INVERSE)

Status: **DRAFT. Not frozen, not applied, no scorer change, no runtime change.** Machine form:
[A4_TS_ELIGIBILITY_TAIL_DRAFT.json](A4_TS_ELIGIBILITY_TAIL_DRAFT.json).

Owner instruction (AskUserQuestion 2026-10-18 ~14:20Z, verbatim): "Xem lại cách chấm, nếu do bản thân mình sai thì
sửa nhanh rồi Release, còn nếu là do OKX thì nới gate 1 xíu rồi pass rồi release. Miễn Risk Engine và Execution
Eligibility của TS chấp nhận là được".

**Post-hoc label.** This draft was written after the counted run `p184-final-campaign-20261018T123222Z` was scored.
If it is applied to that run, that is a **post-hoc owner decision**. The certificate must label it
`POST_HOC_OWNER_DECISION`, show the original FAIL verdicts next to the A4 verdicts, and pin this file's sha256. The
frozen budget, the addendum, G4-A1, G4-A2 and A3 are not edited. The frozen gate table says "a threshold changes only
by a new recorded owner decision with its reason; no number is widened after measuring" (TS
`upgrade/p184-phase-d-gates.md`, FROZEN section). A4 is that recorded decision, and it is post-measurement by
construction.

Every number below comes from the run files named in the JSON `sources_sha256` (recomputed with `sha256sum`). Each
point is labelled **[verified]** (read from a file or recomputed) or **[inferred]** (a reasoned explanation that was
not proven).

## 1. Scoring review: is the scoring itself wrong?

### 1.1 G4 OKX_INVERSE rows (book_delta / book_snapshot / quote, BTC-USD-SWAP)

- **[verified] Source of the limit.** The addendum (`config/v2/p184-stage100-g4-addendum.json`, frozen 2026-10-11)
  sets `candidate_max_limit_ms = 1.2 x baseline max`. The baseline is the single campaign
  `p184-g4-baseline-20261011T175720Z`: 4 regular 120 s windows, all with `boundary: null`, on older runtime (writer
  `44baacc0`, DL source `84713b0`). Per-window baseline maxima:

  | Row | Baseline max per window (ms) | n per window | Limit |
  | --- | --- | --- | --- |
  | book_delta | **235.4** / 132.4 / 126.3 / 131.5 | 112 / 111 / 112 / 112 | 282.5 |
  | book_snapshot | **250.6** / 136.2 / 141.8 / 152.3 | 141 / 122 / 122 / 131 | 300.7 |
  | quote | **268.8** / 225.5 / 157.3 / 181.3 | 93 / 81 / 74 / 86 | 322.6 |

  Each limit is 1.2 x **one baseline event**, taken from baseline window 1. The other windows sit at about 126-225 ms.
- **[verified] Schedule mismatch.** A candidate has 5 windows. Window 5 is a 180 s **boundary** window (13:00:00Z in
  123222Z) with about 50 % more samples (n 165-183). The baseline had no boundary window. In 123222Z, window 5 set the
  maximum of all three failing rows.
- **[verified] The scorer implements the frozen rule exactly. No inflation bug.** For every retained exceedance event
  (observer v7 capsule, no overflow), venue->DL + DL receipt->CreateTime + CreateTime->observer equals the reported
  `visibility_ms` to within 0.001 ms (8/8 events). Histogram bucket indices are log-linear indices, not ms, so there
  is no bucket/ms confusion.
- **[verified] The cause is internal to DL, not OKX.** venue->DL is 28-33 ms on every failing event. The tail is in the
  two internal segments: receive->CreateTime 154-295 ms and CreateTime->observer 115-206 ms. The 8 exceedance events
  in 123222Z fall on only **4 instants**: 12:36:05.7, 12:40:27.0, 12:44:21.8 and 13:01:00.7-01.1Z. book_delta and
  book_snapshot at 13:01:01.001 are **the same venue frame**: same `source_event_ns`, same receipt, adjacent offsets
  225135820/225135821. The "three failing rows" are therefore a few shared episodes, not independent failures.
- **[inferred] A likely internal mechanism.** In the A3 local sampler (`a3-local/local.jsonl`, 1 s resolution), the
  OKX ingestor (cgroup `NanoCpus` 0.5 CPU) shows these values at the tail instants:
  - worst-thread run-queue wait 34-133 ms, against a p99 of 46 ms over the load;
  - a CFS throttle (`d_nr_throttled` 1-2) at 3 of the 4 instants. Over the load there were 12 throttled samples in
    1,797.

  This points to ingestor CPU-quota bursts. Correlation at 1 s resolution is not proof.
- **[verified] Inconsistent statistic.** The same events also belong to the pooled `*|OKX|` rows. Those rows are gated
  on p95 and pass (for example, the `quote|OKX|` window-5 max 531.927 ms is the same event). The inverse addendum rows
  are the **only fast-feed rows in G4 that are gated on a max**. The inverse rows' own p95 per window (113-231 ms
  across the four receipts) is inside the pooled-row p95 limits (289.1 / 270.1 / 283.3).
- **[verified] Instability across receipts.** Over 4 receipts x 3 rows, 5 of 12 row-runs FAIL, each time on a
  different set of rows: 135719Z quote; 053803Z none; 074624Z book_delta; 123222Z all three. That pattern fits a max
  of about 600 samples tested against a limit set by a single baseline sample.
- **[inferred] Why the statistic is weak.** If candidate and baseline came from identical distributions, the chance
  that the overall maximum falls among the candidate's samples would be about N_c / (N_c + N_b), roughly 621 / 1,068,
  or 58 %. The 1.2 factor helps little on heavy tails.

**Verdict for G4: no scorer bug. "Mình sai" applies to the rule design.** The max statistic for these rows rests on a
one-sample baseline and a boundary-window schedule the baseline never had. The latency itself is real and internal to
DL, not OKX. The tails are genuine DL tails, so this is not an "OKX" case. The relaxation is justified by the weakness
of the methodology plus TS acceptance, not by blaming the venue.

### 1.2 G2 MARK_INDEX_PRICE|BINANCE

- **[verified] Definition** (`scripts/p184_g2_mark_trace_transit.py`): new-event transit = TS Redis APPLY reply -
  max(mark, index source event time), counted at the first apply of each strictly newer event. The basis is
  `ADAPTER_REFERENCE_RETURN` for 9,030 of 9,030 Binance records. That stamp is applied after the TS reference
  batcher returns (TS `adapters/market_data/data_layer_v2.py:1230`), so `upstream_ms` = sdk_receive - newest includes
  the **TS writer's own read scheduling** as well as the DL path.
- **[verified] Designed TS read delays are in the quantity.** MARK is not a native feed (TS
  `services/market_data/native_plan.py:28`). The Python bridge reads it on a 1 s refresh
  (`shared/config.py:215` `DATA_LAYER_V2_MARK_REFRESH_SECONDS = 1.0`, `data_layer_bridge.py:1012`) plus a Stream wake.
  A woken read waits `max(min_interval - since_last_read, read_delay)` (`data_layer_bridge.py:1013-1026`). The running
  writer sets the min interval to 500 ms (`config.py:228`) and a Binance read delay of 100 ms
  (`DATA_LAYER_V2_MARK_WAKE_READ_DELAY_BY_VENUE=BINANCE=100`, from the writer compose snapshot
  `.../native-trace-roll/trace-20261018T123107Z.compose.json`). It also does one IDR-113 re-read if a woken read did
  not advance (`:897, 993-999`). So every Binance sample carries about 100 ms of designed delay. A read that misses
  or loses its wake falls back to the 1 s refresh, which can add up to about 1000 ms. The TS gates doc says MARK
  moved "off the 1 s Query poll (stream-triggered reference reads)", but the poll is still the fallback.
- **[verified] Values.** Over the venue: n 9,000, p50 328.5, p95 418.6, p99 488.5, max 1082.9 ms. Per-instrument
  maxima: BNB 949.9, BTC 642.6, **DOGE 1082.9**, ETH 960.3, SOL 884.9. Only DOGE is above 1000. For DOGE, upstream
  (DL + SDK read) is 1069.8 ms and the writer part is <= 95.3 ms, so the tail is upstream of the writer.
- **[verified] Across runs**, max / p95: 135719Z 1097.7 / 396.2; 053803Z 911.3 / 376.0; 074624Z 887.4 / 388.0;
  123222Z 1082.9 / 418.6. The p95 is stable at about 80 % of its limit; the max straddles 1000.
- **[verified] The limit is already anchored to TS.** The G2 row was frozen on 2026-10-09 as "max keeps 2x headroom
  under D3", with D3 MARK at 2000 ms (TS gates doc, G2 row).
- **Not checked.** I did not re-read the raw per-event DOGE records: the trace files are root 0600 and would need a
  disposable container. So the number of samples above 1000 ms is "1 or more" (p99 is 519.6 ms).

**Verdict for G2: no scorer bug** (the quantity is end-to-end to the TS cache by definition). There is, however, an
**attribution gap**. The certificate RCA puts the tail in the DL Query MARK read path (IDR-119), but this trace
basis cannot separate DL time from the TS writer's wake/min-interval/refresh scheduling.

**[inferred]** A 1082.9 ms max on DOGE alone (other instruments <= 960 ms), with p95 at 418.6 ms, looks like one
read that fell back to the 1 s refresh or the min-interval gap. That is a TS-side designed path, not necessarily a DL
stall. Not proven: per-read wake/poll provenance is not in the trace.

## 2. TS acceptance bounds actually applied at use time

### 2.1 Requirement values (deployed manifest)

The deployed consumer manifest is `trading-system.paper.stable` **revision 11**. Its sha256 is `efd482fa...52b8fc`, the
same for `/consumer-realms/trading-system.paper.stable-r11.yaml` inside `query_kn_1` and for
`~/.local/state/qdl-v2/p184-d1-rev11-20261009/trading-system-paper-r11.yaml`. Note: the repo file
`consumers/stable/trading-system-paper.yaml` is still revision 10 (61 requirements, no BTC-USD-SWAP); r11 has 65 and
adds the inverse cells.

| Feed / instrument | max_freshness_ms | session liveness | event recency | r11 line | TS binding r23-v3 |
| --- | --- | --- | --- | --- | --- |
| MARK_INDEX_PRICE BINANCE DOGEUSDT | 2000 | 45000 | OBSERVE | 642-644 | 2000 |
| BOOK_DELTA OKX BTC-USD-SWAP | 2000 | 45000 | OBSERVE | 854-856 | 2000 |
| BOOK_SNAPSHOT OKX BTC-USD-SWAP | 60000 (DL read contract) | - | - | 867-868 | 60000 |
| QUOTE OKX BTC-USD-SWAP | 2000 | 2000 | OBSERVE | 894-896 | 2000 |

TS gates doc G6 / D3 semantics: "BOOK_SNAPSHOT DL60000 ms is a read contract, NOT permission for TS to execute on a
60-second-old book; TS stricter2000 ms use-time check still applies". A4 therefore uses **2000 ms for every row**.

### 2.2 TS enforcement code (TS writer image `2deb90b0`, TS `30f50a91`)

This is a code reading at TS `bc713113`. `git diff 30f50a91 bc713113` does not touch `services/`, `adapters/` or
`rust/`, so the lines match the running image. Runtime env comes from the saved compose snapshots and was not read
from the live containers.

- **Order admission budget.** `market_max_age_seconds` is 2 in P18.4 (`services/risk_pool/ordinary.py:65-67`,
  `scripts/p184_prepare_runtime.py:285`). Reader cap: `services/market_data/runtime.py:55-60` takes
  min(180 s, 2 s) = 2000 ms. Per-feed route cap: `services/market_data/execution_recovery.py:96-100, 235`
  takes min(bound, sealed `max_freshness_ms`). Effective bound: QUOTE 2000, MARK 2000, BOOK_SNAPSHOT
  min(2000, 60000) = 2000 ms.
- **The check** (`services/risk_engine/execution_market_context.py:270-281`, read at the line level):
  `EXECUTION_CONTEXT_NOT_EXECUTION_ELIGIBLE` if `execution_eligible` is not true; otherwise
  `if not (quiet_reference or quiet_quote) and now_ms - timestamp_ms > _strict_event_budget_ms(...)` gives
  `EXECUTION_CONTEXT_STALE`. On the quiet contract, `_strict_event_budget_ms` returns the order budget
  (`:244-245`), and the event-age check is skipped altogether when `quiet_reference` holds.
- **MARK (Binance DOGEUSDT and OKX inverse).** On the quiet contract (OBSERVE + lineage + mark/index session proof),
  Risk does **not** check event age. It validates session liveness <= 45000 ms and component receipt age <= cadence
  (Binance USD-M BOTH 5000 ms; OKX SWAP MARK 15000 / INDEX 70000 ms; DL `qdl/runtime/production_catalog.py:81-84`)
  through qdl_sdk `validate_quiet_execution_mark_index_evidence` (`execution_market_context.py:188-211`). On breach:
  `EXECUTION_CONTEXT_REFERENCE_SESSION_EXPIRED` / `..._COMPONENT_EXPIRED`. Off the quiet contract the fallback is
  event age <= min(2000, sealed 2000) = 2000 ms (`:246-249, 280`). At fetch, the writer adapter enforces
  `freshness_ms > max_freshness_ms` and raises `StaleExecutionReferenceError` (`adapters/market_data/data_layer_v2.py:781-782`;
  max = min(env 15000, sealed 2000)).
- **QUOTE (OKX BTC-USD-SWAP).** Event age <= 2000 ms at use (`:280`), or the quiet on-change session proof
  `now - heard <= 2000` (`:216-231`). On breach, a Query refresh within 3 s; if that fails,
  `EXECUTION_RECOVERY_NOT_USABLE_QUOTE` (`execution_recovery.py:250-262`). Writer view eligibility is event and
  session 2000 ms (Rust `view.rs:340-348, 434`).
- **BOOK_SNAPSHOT.** 2000 ms at use, and only for orders that require L2 (post-only, price-match, L2 queue model,
  `execution_market_context.py:322-331, 432-446`). P18.4 runs `BBO_EXECUTABLE`, so normally it is not required.
- **BOOK_DELTA.** Never read by Risk (`execution_recovery.py:21` has no BOOK_DELTA field). In the writer, event age is
  OBSERVE (non-blocking, DL `qdl/data_quality/binding_decision.py:340-344`). Session continuity <= 45000 ms
  (`data_layer_v2.py:440-460`). Silence watchdog 60 s (`data_layer_bridge.py:95-104`).
- There is no inverse-vs-linear branch in TS freshness logic (both map to product `SWAP`,
  `data_layer_v2.py:411-412`).

**Consequence for these rows.**
- G4 inverse book_delta: no TS use-time event check. The 2000 ms figure is the DL/writer requirement value, not an
  admission bound.
- G4 quote: 2000 ms at admission.
- G4 book_snapshot: 2000 ms, and only for L2 orders.
- G2 Binance MARK on the quiet contract: no event-age check at admission. The binding bounds are session 45 s and
  component 5 s, so a 1.1 s transit is far inside what Risk accepts. Only the non-quiet fallback uses 2000 ms.

### 2.3 Did any TS refusal happen in 123222Z?

- **[verified] G6 (D3_EXECUTION probe of the writer's own consumer, 32 targets, one read per binding about every
  3.2 s):** MARK BINANCE n 2,990, max age 1,394 ms, refusals 0, unsafe 0. QUOTE OKX n 3,582, max 5,459 ms (40
  ELIGIBLE_QUIET), refusals 0, unsafe 0. G6 has no BOOK rows.
- **[verified] G9:** `eligibility_losses_data_path` 0, demanded == session-ready on all 60 samples, v1 fallback 0. In
  all 192 TS heartbeats (about every 10 s), the only `not_ready` entries are TRADE `TRADE_EVENT_STALE` (legitimately
  quiet). There is no MARK, QUOTE or BOOK entry.
- **[verified] TS Python workers** (`market_data_worker-0..4.log`; Python-owned feeds MARK and BOOK; worker-3 logged
  the BTC-USD-SWAP BOOK slices): zero non-INFO lines between 12:00 and 13:59Z. The last `DATA_STALE` disconnect is at
  2026-10-18 03:41Z, before the run. **Not checked:** the native Rust writer's own logs (QUOTE and TRADE are native,
  `MARKET_DATA_NATIVE_FEEDS=TRADE,QUOTE`) and Risk-engine logs; whether any order admission ran
  during the load was not checked. So "no risk rejection" here means no eligibility loss on the samplers above, not
  observed order admissions.
- **Limit of this evidence.** These samplers cannot see a sub-second window. A 1082.9 ms MARK transit on a 1000 ms
  venue cadence (Binance `markPrice@1s`) means the previous event's age is about 2083 ms for about 83 ms before the
  late event applies. A G6 read every 3.2 s or a heartbeat every 10 s would almost surely miss that window. Whether TS
  would refuse in that window: per 2.2, Risk does not refuse on that age while the MARK quiet contract holds (session
  45 s and component 5 s are the binding checks); only the non-quiet fallback applies 2000 ms. This conclusion comes
  from code reading, not from a captured read at that instant.

## 3. Proposed A4 (for exactly these rows)

Rule: `new max limit = min(0.5 x TS use-time bound, 2.0 x current max limit)`. Every p95 (and p99) limit is unchanged.

Conditions:
- zero TS eligibility refusals or risk rejections attributable to these tails (G6 D3 refusals = 0 and unsafe = 0, G9
  data-path losses = 0, no `DATA_STALE` / `EXECUTION_CONTEXT_NOT_EXECUTION_ELIGIBLE` in TS logs over the load);
- every event above the **current** limit stays in the receipt (observer v7 exceedance capsule, no overflow, or
  UNKNOWN = FAIL);
- applying A4 to 123222Z is labelled `POST_HOC_OWNER_DECISION`.

| Row | Current max | TS bound | 0.5 x bound | 2 x current | **A4 max** | p95 (unchanged) |
| --- | --- | --- | --- | --- | --- | --- |
| G2 MARK BINANCE | < 1000 | 2000 | 1000 | 2000 | **1000 (no change)** | <= 500 |
| G4 book_delta OKX_INVERSE | 282.5 | 2000 | 1000 | 565.0 | **565.0** | none in the addendum |
| G4 book_snapshot OKX_INVERSE | 300.7 | 2000 | 1000 | 601.4 | **601.4** | none in the addendum |
| G4 quote OKX_INVERSE | 322.6 | 2000 | 1000 | 645.2 | **645.2** | none in the addendum |

For G4, A4 binds through the 2 x cap ("1 xíu"); the worst-case age at use stays well under 2000 ms. For example,
book_delta 565 ms plus the observed OKX books inter-push gap max of 700.7 ms is about 1266 ms. For G2, the frozen
limit **already equals** the 50 % headroom rule, so A4 cannot relax it without giving up the 2x headroom the owner
froze on 2026-10-09.

Owner-only option, **not the default**: a 0.6 fraction for G2 MARK BINANCE only, giving a 1200 ms limit (1.67x
headroom). It passes 123222Z and 135719Z. Its cost is the about 83 ms window above 2000 ms described in 2.3.

### 3.1 Dry rescore (report-only; rows in scope only, every other row keeps its recorded verdict)

| Run (protocol_met) | G2 MARK BINANCE max: now / A4 | book_delta max: now / A4 | book_snapshot max: now / A4 | quote max: now / A4 | Gates under A4 |
| --- | --- | --- | --- | --- | --- |
| **123222Z (true, counted)** | 1082.9 FAIL / **FAIL** | 387.1 FAIL / PASS | 406.7 FAIL / PASS | 531.9 FAIL / PASS | **G4 PASS, G2 FAIL** |
| 135719Z (true) | 1097.7 FAIL / FAIL | 220.4 PASS / PASS | 284.6 PASS / PASS | 723.5 FAIL / **FAIL** (w2, w3 > 645.2) | G2 FAIL, G4 FAIL |
| 053803Z (true) | 911.3 PASS / PASS | 274.8 PASS / PASS | 289.5 PASS / PASS | 311.2 PASS / PASS | G4 FAIL (mark_index_price OKX, out of scope; later G4-A1) |
| 074624Z (false) | 887.4 PASS / PASS | 412.4 FAIL / PASS | 259.9 PASS / PASS | 237.9 PASS / PASS | not countable (protocol); G4 also bar BINANCE 8h at that tool version |

The rescore is consistent across receipts: A4 does not pass every historical row. The 135719Z quote tail of 723.5 ms
would still FAIL. With the G2 0.6 option, G2 MARK BINANCE is PASS on all four receipts.

## 4. The "mình sai" alternatives (a fix instead of a relaxation)

- **M1 - methodology correction (scoring only, no rerun).** Score the inverse rows with their pooled row's p95 rule
  (p95 <= pooled-row limit, n >= 20 per window), and keep the max as report-only, bounded by the A4 cap. All four
  receipts PASS the inverse rows. This removes the one-sample-baseline max statistic instead of widening it. It does
  not help G2.
- **M2 - system fix (needs a roll and a new stage-100).** Raise `ingestor_okx_swap` from 0.5 to 1.0 CPU as a single
  variable, with A/B (IDR-116..118 order still applies). The basis is the inferred throttle correlation in 1.1. It
  needs owner approval of the roll and of the resource, plus a 45-minute run.
- **G2:** there is no scoring flaw to fix. A quick system fix is the Query MARK read path (IDR-119), which is not
  quick and needs a run.

## 5. Recommendation

1. G4: apply A4 (or M1) to the inverse max rows, labelled post-hoc. Under A4, G4 on 123222Z is PASS, and the
   evidence available shows no TS refusal.
2. G2: under the default A4 (0.5 x 2000 ms) it stays **FAIL**, because the frozen limit already equals 50 % of the
   bound. The 0.6 owner option (1200 ms) is defensible from TS acceptance: on the quiet contract Risk applies no MARK
   event-age check, and the strict fallback would still have 1.67x headroom. It passes all four receipts. The cost:
   it gives up the 2x headroom frozen on 2026-10-09, it is post-hoc, and the tail's attribution between DL and TS
   read scheduling is open (1.2). Alternatively, keep G2 FAIL and fix after release: stamp per-read wake/poll
   provenance in the TS trace, then decide between IDR-119 (DL Query path) and the TS wake/refresh scheduling.
3. The repo manifest `consumers/stable/trading-system-paper.yaml` is r10 while runtime runs r11. Record this as a
   packaging finding before the release.

## Freeze steps (not done)

Owner approval of the exact limits and of the post-hoc label. Freeze into `config/v2/p184-stage100-a4-*.json` with
`frozen_at`. Change the scorer to apply A4 only to a campaign that recorded A4's sha256, or write a separate
report-only rescore receipt for 123222Z. Update the certificate with original and A4 verdicts side by side.
