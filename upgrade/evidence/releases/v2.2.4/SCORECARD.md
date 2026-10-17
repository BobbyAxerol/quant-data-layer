# Quant Data Layer v2.2.4 - gate scorecard G1-G14 (DRAFT, frozen rules)

Status: **DRAFT.** Written 2026-10-17 ~11:00Z. It holds only results that exist today. Everything that needs the final
stage-100 is a `<<PENDING:...>>` marker (the G13 runtime window v5.4 is filled below). Machine form (`scorecard.json`, schema
`qdl.release-scorecard.v1`, as in [v2.2.3](../v2.2.3/scorecard.json)) is written from this file at finalisation.

## Frozen inputs (unchanged; no number is widened after measuring)

- Gate table: TS `upgrade/p184-phase-d-gates.md`, FROZEN 2026-10-09, sha256
  `c67bf69bb921c992964eb8ff8b2e1bb17085a943fdf756d55526b6269641d5c4` (verified with `sha256sum` on 2026-10-17).
- Budget: `config/v2/p184-stage100-budget.json` sha256 `ff85335b3c8762d9acd8e7230f3c81ee1ec8403aff34cf2fb8f67714521c2541`
  (verified).
- Scoring rule (table, "Measurement rules"): a PASS needs steady state >= 30 min at the stage-100 workload plus one
  burst window, with `protocol_met: true`. A measurement outside that rule can FAIL a gate but cannot PASS it. A
  NOT_MEASURED is never upgraded.
- Release identity measured: DL roles on `qdl-v2-rust:2.2.4-c7ce7d3` `sha256:60f83a51...` / `qdl-v2-python:2.2.4-c7ce7d3`
  `sha256:c21e164a...` (REV `c7ce7d3`), cores on rc22 `7c848aad`, TS writer `a44349a1` (TS `6af62a7e`), Kafka config
  effective sha256 `d3d77176...` (B x3, C, A', cleaner 4 MiB/s).

## Scorecard

Legend: PASS = measured under the frozen rule; CREDIT = receipt credited under the stated basis; PENDING = not yet
measured on the release digests; HISTORY = measured on other digests, kept, not credited.

| Gate | Quantity (frozen) | Rule (frozen) | v2.2.4 status | Evidence / placeholder |
| --- | --- | --- | --- | --- |
| G1 | End-to-end to TS cache, BOOK_DELTA / BOOK_SNAPSHOT | p95 <= 500 ms, p99 <= 1000 ms (n >= 100), max < 2000 ms | `<<PENDING:G1_VERDICT>>` | `<<PENDING:G1_NUMBERS>>` per feed/venue (p50/p95/p99/max/n) |
| G2 | New-event transit TRADE / QUOTE / MARK_INDEX_PRICE | TRADE max < 1500 ms; QUOTE, MARK max < 1000 ms; p95 <= 500 ms each | `<<PENDING:G2_VERDICT>>` | `<<PENDING:G2_NUMBERS>>` |
| G3 | BAR from close | p95 <= 5 s, max < 60 s | `<<PENDING:G3_VERDICT>>` | `<<PENDING:G3_NUMBERS>>` |
| G4 | Delivery lag venue -> canonical vs frozen baseline | rows with a frozen baseline judged; others NOT_MEASURED (never upgraded) | `<<PENDING:G4_VERDICT>>` | `<<PENDING:G4_NUMBERS>>` baselined rows and unbaselined rows listed separately; addendum sha256 `4ed59dca...` (quoted from the plan, not re-hashed here) |
| G5 | Request latency, Query warmup / snapshot / reference batch | stage-50 p95/p99 targets per class; cold-overlap | `<<PENDING:G5_VERDICT>>` | `<<PENDING:G5_NUMBERS>>` incl. failed/missed of offered |
| G6 | Durable-view event age and typed execution quality | per-binding policy; 0 unsafe eligibility | `<<PENDING:G6_VERDICT>>` | `<<PENDING:G6_NUMBERS>>` |
| G7 | TS consumer path (SDK -> writer, writer -> Redis) | p95 <= 100 ms and p95 <= 50 ms per feed | `<<PENDING:G7_VERDICT>>` | `<<PENDING:G7_NUMBERS>>` |
| G8 | TS execution Redis | commandstats <= 60 % of a core per 10 min; SLOWLOG 0 in steady state; PING p99 <= 5 ms | `<<PENDING:G8_VERDICT>>` | `<<PENDING:G8_NUMBERS>>` |
| G9 | Readiness | demanded == session-ready all window; 0 data-path eligibility losses; v1_fallback 0 | `<<PENDING:G9_VERDICT>>` | `<<PENDING:G9_NUMBERS>>` (HISTORY: FAIL on 7c71e3af, see below) |
| G10 | Integrity TRADE (LOSSLESS) | 0 gaps, 0 duplicates applied, 0 replay-expired, 0 overflow | `<<PENDING:G10_VERDICT>>` | `<<PENDING:G10_NUMBERS>>` native typed counters |
| G11 | Recovery, writer restart | fault cleared -> first valid apply <= 30 s, caught up <= 60 s; no rewind; cursor never ahead | `<<PENDING:G11_VERDICT>>` | `<<PENDING:G11_NUMBERS>>` (HISTORY: PASS on 7c71e3af, see below) |
| G12 | Recovery, Kafka broker / projector / Stream restart | caught up <= 600 s, then G1-G3 inside; same four timestamps | **PASS - CREDIT (final)** | see "G12" below |
| G13 | Acceptance DR01-DR08 (76 cells) | every cell closed once on the final DL + writer digests | isolated **12/12 PASS**; runtime window v5.4 **PASS, 76/76 closed** | see "G13" below |
| G14 | Resources, every DL role and the TS writer | CPU/RAM at stage 100 recorded with headroom (records resources; not a headroom gate) | `<<PENDING:G14_VERDICT>>` | `<<PENDING:G14_NUMBERS>>` per role; `host.json` of the final stage-100 |

Summary line for the certificate (fill from the rows above): PASS `<<PENDING:SUMMARY_PASS>>`, FAIL
`<<PENDING:SUMMARY_FAIL>>`, NOT_MEASURED `<<PENDING:SUMMARY_NOT_MEASURED>>`, DEFERRED `<<PENDING:SUMMARY_DEFERRED>>`.

## G12 - final credit (PASS)

- Receipt: `~/.local/state/qdl-v2/p184-g12-v224-final-a44349a1-20261017T084728Z/` (namespace
  `p184-g12-dl-2e2219b81a8c6dc4`), run 08:48:32-09:15:32Z, lock "claude P18.4 G12 v224 FINAL a44349a1
  20261017T084728Z" (08:47:28Z, 75 min, released ~09:17Z). Values below are from the plan journal; the file hashes were
  re-computed with `sha256sum` on 2026-10-17.
- Verdict v3 COMPLETED, `pass: true`, `images.ts = a44349a1`. `evidence/verdict.json`
  `30648759e5766f5e085e1ea3874efe2ab69ceca0475fcca2ab23e3016b67d1be`.
- `caught_up_s` BINANCE / OKX: broker stop/start 0.217 / 0.218, projector restart 0.174 / 0.174, Stream restart
  0.759 / 0.760 (bound 600 s). Every fault: G1, G2 (TRADE/QUOTE/MARK lost 0), G3 BAR, monitor and sampler PASS.
- `--credit` exit 0: creditable true, reason PASS, `production_parity` MATCH, `cleaner_binding` RUN, final profile
  checked. Effective describe `d3d77176c3dfca46e7f44cba465d271afe2709a1fc9d97f75e808808fa959094` before and after
  (`credit-snapshot.json` `6bb2021ab3e51065bf54697a7f424991fc4125bd29fb4ac0536597be9f4ff2fe`).
- Writer clones: precheck ok, Redis UNCAPPED, strict_resume VERIFIED on both venues. Teardown: 12 removed, 0 left,
  clone state erased (`evidence/teardown.json` `b9e9fccf33e8f320b8d76a534b49833caa31e3d3314cb3863570d0967f67d272`).
- Inherited from other digests: nothing (`FINAL_CREDIT.json` `inherited: "nothing"`). Earlier runs on 7c71e3af and
  734b08f1 are SIGNAL ONLY.
- Basis caveats (stated, not hidden): WriterClone apparatus pin `77a6d671` (decision H1, deferred follow-up);
  integrity layer report-only; harness shape differences listed in CERTIFICATE.md section 5 item 8; foreign-job overlap
  with this window is NOT_DETERMINABLE (docker keeps only the last events; no lock held by anyone else when acquired).
- Not rerun for the roll that put `c7ce7d3` images in production: the credited run's image set is
  `g12-image-set-v224.json` sha256 `4740c995550737e7d30325aaa043f07cf3656014ca5611dd97e482619504ca61`
  (core 7c848aad, projector/stream 60f83a51, python c21e164a, ts a44349a1).

## G13 - 76 cells

Isolated phase (credited on writer a44349a1, DL run worktree at `f6db0b82`, TS `6af62a7e`, roles
`g13-roles-v224.json` `31e0716b...`):

| Step | Cells | Result | Receipt | Evidence sha256 |
| --- | --- | --- | --- | --- |
| dr08-isolated | 4 | 4/4 OK, harness_ok, 124.2 s | `p184-g13-20261017T091825Z` | evidence.jsonl `5f9f70799b1c6f4ec15ef16b139fa9bc8b3a345d3ae19a8d35eaae3e823cd541` |
| dr05-upstream | 6 | 6/6 OBSERVED ok after one apparatus rerun, 334.9 s | `p184-g13-20261017T093001Z` | rows `189de29ed52ce2b9826508423dcc4d307e1a24dc5ba7a6e7abd811e8d8e92681` |
| dr05-quiet (amendment-1) | 2 | 2/2 ISOLATED_FAULT OBSERVED, `harness_ok` true, 139.7 s | `p184-g13-20261017T092651Z` | rows `11991a2342a55f54dfa729fa816db00d706e871b666d534aa9694525d8c0e179` |

- dr05-upstream run 1 (`T092050Z`) is NOT SCORED: `DR05.GENERATION_CHANGE_FENCED@OKX_INVERSE` failed its positive
  anchor because the harness sampled before the writer subscribed (apparatus readiness race). Kept, summary sha256
  `4b28325528a8cb3951640b1c7505080b1a890fb5138b119d22b3a4c158369bd5`. Fixed in TS `fc644cd1` (IDR-111).
- W05 verifier over the three credited files: DR05 8/17 and DR08 4/4 closed, only issue CASE_OPEN on the runtime
  cells not yet run (`w05-isolated-final-a44349a1.json` `b5519639fd0c23923322063025c566afcb1775c63adca574df067f3db46dce6f`).
- amendment-1 (TS `1401ccb1`, owner-approved 2026-10-17): DR05.QUIET_TYPED@BINANCE / @OKX are closed by an
  ISOLATED_FAULT producer with predicate `p184.dr05-quiet-typed.view-vocabulary.v1` (IDR-109). v2 matrix not edited.

Runtime window (66 cells; dr01-deployed 7, dr01-wrong-account 2, dr02-dr03 14, dr04-dr06 views 22, dr05-transport 9,
dr07 12):

| Window | Result | Status |
| --- | --- | --- |
| v5.2 (2026-10-17 02:43Z, writer 7c71e3af) | 74/76 (2 QUIET cells NOT_OBSERVED) | HISTORY, other writer, not credited |
| v5.3 (09:37Z, writer a44349a1) | 67/76, STOPPED at dr05-transport `DR05_CLONE_STRICT_RESUME_NOT_VERIFIED` (IDR-111 apparatus) | informational; the 9 transport cells stayed open. summary `2f6b9ddd84795dfff9ce3801559f95c01ada5d3910bd5297a6ab44dba071aba0`, W05 `23eeb0e89b8e1dd0d0f531c80739028a153bd6ee7bbe999900f9ac108a8128da` |
| **v5.4 (final)** | 76 of 76 | PASS; receipt `p184-g13-20261017T103357Z`; summary `9d22e11100e8f23ce76f4b79242ac02455ea7ad2097a7e57cd11436738c68367`; W05 `verify/w05-final-v54.json` `5490c21e66bf33bae326f2ea2051677f70cf95372b13b823d4c2912835da9895` (verifier exit 1 only from 260 CASE_OPEN cells of the 336-cell matrix outside G13; 0 DR issues; DR cells per case: DR01 9, DR02 8, DR03 6, DR04 14, DR05 17, DR06 6, DR07 12, DR08 4); all 6 runtime steps `harness_ok`, exit codes 0; run ids `g13r7wa20261017` / `g13r7fx01`; packet `packet-g13-v5.4-20261017.md` `872265593331a0cc51ef3abb0f301bfdcb45a3978c5d4f845608813a0236101d`; TS CI run `39024582832` success on `c9ebd619` (per the packet; not re-verified here); harness fix `fc644cd1` = IDR-111, not in the writer image closure |

Final G13 verdict rule: PASS when isolated 12/12 + runtime window together close all 76 cells with no provenance or
drift issue on `a44349a1` / REV `c7ce7d3` digests; otherwise record the open cells as limits only by an owner decision.

## Stage-100 attempts (all kept; only the last protocol-met run on the release digests is credited)

| Run | Date | Digests | protocol_met | Outcome | Credit |
| --- | --- | --- | --- | --- | --- |
| #3 `p184-final-campaign-20261016T024530Z` | 2026-10-16 02:45Z | writer B16, stream `baa72c96`, rc19/rc14/rc18/rc22 | false | G9 FAIL (3 samples = one 30 s heartbeat counted 3x); every measured gate inside its rule | HISTORY, diagnostic |
| final attempt `p184-final-campaign-20261017T032230Z` | 2026-10-17 03:20-03:48Z | release DL digests, writer 7c71e3af | false (guard stopped the driver 03:43Z, 20.4 min into the 30 min load) | G9 FAIL (7 heartbeats 63/64, QUOTE OKX BTC-USD-SWAP STALE); G4 FAIL on 2 rows outside the protocol window (bar BINANCE 1m p95 258.133 vs 257.6; book_snapshot OKX_INVERSE max 300.788 vs 300.7); G11 PASS (outage 0.138 s, first-apply/caught-up max 20.096 s); G1/G2/G7/G10 NOT_MEASURED | HISTORY: writer was replaced (IDR-110, IDR-110b); nothing inherited. scorecard sha256 `63330fed...` (prefix quoted from the plan) |
| **final** | `<<PENDING:S100_DATE_UTC>>` | release DL digests + writer `a44349a1` | `<<PENDING:S100_PROTOCOL_MET>>` | `<<PENDING:S100_OUTCOME>>` | credit only if protocol_met true |

Final stage-100 details to record: dir `<<PENDING:S100_DIR>>`; scorecard sha256 `<<PENDING:S100_SCORECARD_SHA256>>`;
load window `<<PENDING:S100_LOAD_WINDOW_UTC>>`; lock owner and window `<<PENDING:S100_LOCK>>`; apparatus (DL run
worktree sha, G4 observer v5, sampler dedupe `1e09f87`, G11 v2 oracle, native typed G10 counters)
`<<PENDING:S100_APPARATUS_PINS>>`; trace re-arm 3600 s receipt `<<PENDING:S100_TRACE_REARM_RECEIPT>>`; taint watch
`<<PENDING:S100_TAINT_WATCH>>`; production actions taken inside the protocol (writer re-arm, G11 coordinator restart)
`<<PENDING:S100_PRODUCTION_ACTIONS>>`.

Specific checks the final run must show (from the IDR-110 dispositions):
- native writer `probes_failed` 0 and no native session churn (residual R2 monitored);
- QUOTE `probes_unavailable` count and its codes, `probes_renewed_same_record` > 0 (R1/R2 fixes in IDR-110b);
- stream gateway overflow / refused / hub_live_budget_refused deltas 0; the pre-existing TRADE BINANCE BTCUSDT
  RESOURCE_EXHAUSTED pattern recorded as recurring or not;
- `ts:ready_60_every_sample` acceptance and the guard (`guard_v2.sh`, 3 consecutive non-"READY 64 64" samples; the
  guard counts one heartbeat snapshot up to 3 times and is frozen).

## History kept (not credited for v2.2.4)

- v2.2.3 scorecard: [../v2.2.3/scorecard.json](../v2.2.3/scorecard.json) (campaign `p184-final-campaign-20261011T123525Z`,
  rc12-rc13 digests): PASS G1-G3, G5-G12, G14; G4 NOT_MEASURED; G13 DEFERRED.
- G12 v1-predicate PASS receipts (rc10-full3, rc12, rc18-control2) are not credited (OF-G12-1, inheritance audit
  [g12-g13-inheritance.md](g12-g13-inheritance.md)).
- G12 qualification (`...final-20261016T151558Z`, pre-release images) and the 734b08f1 signal run
  (`...final-idr110-20261017T060133Z`) are not credited.

## PENDING index for this file

`G1`-`G11`, `G14` `_VERDICT` / `_NUMBERS`; `S100_*` (DIR, SCORECARD_SHA256, DATE_UTC, PROTOCOL_MET, OUTCOME,
LOAD_WINDOW_UTC, LOCK, APPARATUS_PINS, TRACE_REARM_RECEIPT, TAINT_WATCH, PRODUCTION_ACTIONS); `SUMMARY_PASS` / `_FAIL` / `_NOT_MEASURED` /
`_DEFERRED`.
