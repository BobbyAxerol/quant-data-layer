# Quant Data Layer v2.2.4 - gate scorecard G1-G14 (FINAL, frozen rules)

Status: **FINAL. Under the frozen rule (G4-A1 + G4-A2 + A3): G2 and G4 FAIL. Under A4 (POST_HOC_OWNER_DECISION,
adopted after the counted run was scored): every gate PASS.** Both verdicts are shown side by side below.
- First assembled 2026-10-18 ~13:40Z and re-issued ~15:30Z with A4. Every sha256 was recomputed with `sha256sum`.
- Machine form: [scorecard.json](scorecard.json) (schema `qdl.release-scorecard.v1`).
- Owner decision 2026-10-18 ~13:40Z (AskUserQuestion "Release kèm limit + gói tối ưu sau (Recommended)"): package with
  the FAIL rows stated as limits, and do the optimisation package later. It changed no verdict.
- Owner answers ~14:45Z: G4 "M1: chấm theo p95 pooled (Recommended)" and G2 "Nới max lên 1200 ms (Recommended)". These
  became A4 ([a4-post-hoc-ts-eligibility.md](a4-post-hoc-ts-eligibility.md)). A4 changes the G2 and G4 verdicts only;
  the latency is unchanged.

## Frozen inputs (pre-registered; A4 below is the one post-measurement change)

- Gate table: TS `upgrade/p184-phase-d-gates.md`, FROZEN 2026-10-09, sha256
  `c67bf69bb921c992964eb8ff8b2e1bb17085a943fdf756d55526b6269641d5c4`.
- Budget: `config/v2/p184-stage100-budget.json` `ff85335b3c8762d9acd8e7230f3c81ee1ec8403aff34cf2fb8f67714521c2541`;
  G4 addendum `config/v2/p184-stage100-g4-addendum.json` `4ed59dca0212566d3ff03c5a4c23eed3d252d0150844b9a4fd6fa543333d78b5`.
- Amendments frozen BEFORE the counted run (each owner-approved, each recorded in `campaign.json`):
  - G4-A1 `config/v2/p184-stage100-g4-amendment-1.json` `1c8a276f6cc6c03cdf768cdc9d31b65902bd849a1f3b7df56cda07ea47e6efa6`
    (row `mark_index_price|OKX|` only: same-event internal split; rationale
    [g4-amendment-1-okx-mark-index.md](g4-amendment-1-okx-mark-index.md)).
  - G4-A2 `config/v2/p184-stage100-g4-amendment-2.json` `99961c41e08993944d158ae812ca61514271862a2d03e011f5d01ce71a0f9997`
    (bar rows >= 8h without a frozen baseline are REPORT_ONLY; no threshold change).
  - A3 `config/v2/p184-stage100-a3-external-attribution.json` `cc5fc7dcf07e30b1b52cbc51b78c4a5485a63be84d5b814dcf89452007e57326`
    (frozen 2026-10-18T12:17:51Z; external-venue attribution can rescue a tail only when the same event's internal
    segments are inside their limits; rationale [a3-external-attribution.md](a3-external-attribution.md)).
- **Adopted AFTER the counted run was scored (POST_HOC_OWNER_DECISION, applied to 123222Z only):**
  - A4 `config/v2/p184-stage100-a4-ts-eligibility.json` `14958a681848192c8b9e93feaf0ee3137a1851c38fe6d6763f3bcf6ce0c81312`
    (frozen 2026-10-18T15:18:14Z). G2 MARK_INDEX_PRICE|BINANCE: max <= 1200 ms instead of < 1000, p95 <= 500 unchanged.
    G4 book_delta / book_snapshot / quote |OKX_INVERSE|: pooled OKX p95 limits 289.1 / 270.1 / 283.3 on windows with
    n >= 20, max report-only, every event above the replaced limits 282.5 / 300.7 / 322.6 retained by observer v7 or
    FAIL.
  - Rescore: `scripts/p184_a4_post_hoc.py` (`23b78e4a...`) drives the unchanged frozen scorer (`e179ec78`, the A3 pin;
    editing it would drop A3 and with it the A1/A2 re-pins). Receipt
    `~/.local/state/qdl-v2/p184-final-campaign-20261018T123222Z/scorecard.a4-post-hoc.json`
    `7a821d2d60c45ae290023e9434af7b320c06b906a4185dc299eee467a71fab7e`. The stored `scorecard.json` is untouched.
- Scoring rule: a PASS needs steady state >= 30 min at the stage-100 workload plus one burst window with
  `protocol_met: true`. A measurement outside the protocol can FAIL a gate but cannot PASS it. NOT_MEASURED is never
  upgraded.

## Measured identity (docker inspect at assembly, 2026-10-18 13:2xZ; RestartCount 0, OOMKilled false everywhere)

| Role | Image | Tag | Revision label |
| --- | --- | --- | --- |
| query_kn_1, query_kn_2 | `sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff` | `qdl-v2-python:2.2.4-31a5155` | `31a5155` |
| ingestor_okx_swap, ingestor_binance_usdm | `sha256:a9679defdd970aaa961736d414cf8c443c2ead2f26ebf2e68eb481dda6725784` | `qdl-v2-rust:2.2.4-83bef59` | `83bef59` |
| stream_kn_1/2, market_projector_1/2 | `sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e` | `qdl-v2-rust:2.2.4-c7ce7d3` | `c7ce7d3` |
| rust_core x3 | `sha256:7c848aadcb0dd28178bc6dc2b58c42df6c2c235de6d4bfd6602ef7c19fa64bfb` | `qdl-v2-rust:2.2.3-rc22-a4e7b3b` | `a4e7b3b` |
| binance_bar_edge (held, scoped out) | `sha256:08f356a84151c6e865fbdc4a2efe6924df9336d15353254c2b2da684261794f5` | `qdl-v2-python:2.2.3-rc14-a388537` | `a388537` |
| market_data_service_p184w (TS writer) | `sha256:2deb90b0cc52eae7610fa44692f90df077e9db100ca259e38fd652a2dc1c5dc5` | `local/trading-system:p184-writer-release-30f50a91` | TS `30f50a91` |

Kafka effective describe `d3d77176c3dfca46e7f44cba465d271afe2709a1fc9d97f75e808808fa959094` (B x3, C, A', cleaner
4 MiB/s), recorded before and after the G12 credit.

## Scorecard (final stage-100 `p184-final-campaign-20261018T123222Z`, load 12:34:45-13:04:45Z, protocol_met true)

| Gate | Rule (frozen) | Verdict (without A4 / with A4) | Numbers (from `scorecard.json` of the campaign) |
| --- | --- | --- | --- |
| G1 | BOOK p95 <= 500, p99 <= 1000, max < 2000 ms | **PASS** | BOOK_DELTA BINANCE p95 197.5 / p99 273.4 / max 780.7 (n 83,901); BOOK_DELTA OKX 205.4 / 278.0 / 829.4 (94,211); BOOK_SNAPSHOT BINANCE 215.1 / 340.4 / 702.2 (9,344); BOOK_SNAPSHOT OKX 213.2 / 315.3 / 814.0 (11,223) |
| G2 | TRADE p95 <= 500, max < 1500; QUOTE and MARK p95 <= 500, max < 1000 ms (A4: MARK BINANCE max <= 1200) | **FAIL** (one row) / **PASS** under A4 | **MARK_INDEX_PRICE BINANCE p95 418.6 / p99 488.5 / max 1082.9 (n 9,000) - max over 1000**; inside: TRADE BINANCE 248 / 611.3, TRADE OKX 241 / 507.3, QUOTE BINANCE 190 / 627.3, QUOTE OKX 200 / 684.4, MARK OKX 353.2 / 743.9 (p95 / max). A3 G2 report: UNKNOWN by construction (Query-read MARK records carry no per-sample DL receipt clock); no lane episode in the load |
| G3 | BAR p95 <= 5 s, max < 60 s | **PASS** | pooled p95 1718 / max 3263 ms (n 320); BINANCE p95 2366, OKX p95 1452; read errors 0 |
| G4 | per row vs frozen baseline (table p95 x 1.2; addendum max x 1.2) + A1/A2/A3 (A4: the three OKX_INVERSE rows on the pooled OKX p95, max report-only) | **FAIL** (three addendum rows) / **PASS** under A4 (inverse p95 per window book_delta 143-198 vs 289.1, book_snapshot 160-193 vs 270.1, quote 178-231 vs 283.3; n 102-183; 8 outliers retained, overflow 0) | **book_delta OKX_INVERSE** max per window 235.0 / **358.9** / 259.0 / 171.0 / **387.1** vs 282.5; **book_snapshot OKX_INVERSE** **301.4 / 399.9 / 311.2** / 277.3 / **406.7** vs 300.7; **quote OKX_INVERSE** 264.9 / 310.5 / 288.1 / 221.9 / **531.9** vs 322.6 (all BTC-USD-SWAP). A3 attempted all three, rescued none: venue -> DL ~30 ms, internal split receive -> CreateTime 154-295 ms and CreateTime -> observer 115-206 ms (INTERNAL_SEGMENT_OVER_LIMIT); two near-limit units UNKNOWN on local checks. G4-A1 `mark_index_price|OKX|` PASS (internal p95 99-126, max 214-374 ms, EXTERNAL_VENUE_COMPONENT_CADENCE 5/5). Every frozen-table row PASS (e.g. bar BINANCE 1m max 160-246 vs 257.6). OKX books probe 5/5 covered, 0 book tails > 150 ms venue -> DL |
| G5 | stage-50 p95/p99 targets per class; cold overlap | **PASS** | 11 latency/cold gates PASS (e.g. MARK_INDEX_REFERENCE BINANCE p95 61.8 / p99 102.2 of 250 / 500; L2_SNAPSHOT BINANCE 151.8 / 198.8 of 300 / 750) |
| G6 | per-binding policy; 0 unsafe | **PASS** | every required D3 binding measured, unsafe 0 |
| G7 | SDK -> writer p95 <= 100 ms; writer -> Redis p95 <= 50 ms | **PASS** | every feed inside (p95 SDK -> writer / writer -> Redis ms: BOOK_DELTA 30.7 / 6.3, BOOK_SNAPSHOT 18.1 / 15.8, MARK 0.9 / 7.7, QUOTE 1.0 / 2.0, TRADE 8.0 / 8.0) |
| G8 | commandstats <= 60 % core / 10 min; SLOWLOG 0 steady; PING p99 <= 5 ms | **PASS** | core fraction ~0.07, slowlog_new_total 0, PING p99 1.305 ms, ping failures 0 |
| G9 | demanded == session-ready all window; 0 data-path losses; v1 fallback 0 | **PASS** | 60 samples, demanded 64 == session-ready every sample, data-path losses 0 (38 legit quiet TRADE), v1 fallback 0, workers LIVE |
| G10 | TRADE 0 gaps / 0 duplicates applied / 0 replay-expired / 0 overflow | **PASS** | all zero (TRADE BINANCE applied 100,331, OKX 22,352) |
| G11 | first valid apply <= 30 s, caught up <= 60 s; no rewind; cursor never ahead | **PASS** | outage 0.14 s, first valid apply / caught up max 4.822 s (n 64), future events 0, rewinds 0 |
| G12 | Kafka broker / projector / Stream restart caught up <= 600 s, then G1-G3 inside | **PASS - CREDIT** | separate isolated run on the final image set (below) |
| G13 | DR01-DR08, 76 cells | **PASS - 76/76** | separate runs on the final image set (below) |
| G14 | CPU/RAM recorded per role (not a headroom gate) | **PASS** (recorded) | 18 roles, mem at/over limit none; DL mean CPU sum 5.63 vCPU (above the owner's 5 vCPU DL budget: stated limit) |

Summary without A4: PASS G1 G3 G5 G6 G7 G8 G9 G10 G11 G14 (campaign) + G12 G13 (separate credited runs) = 12; **FAIL
G2, G4** = 2; NOT_MEASURED 0; DEFERRED 0.

Summary with A4 (post-hoc): PASS 14 (G1-G14); FAIL 0; NOT_MEASURED 0; DEFERRED 0. The rescore leaves every gate other
than G2 and G4 byte-identical (checked by the tool).

Campaign files (sha256): `scorecard.json` `b9e76a83795f1af8f64b802b22ff1f410087a8f87ee4ef5d23b26a1d39cb4964`, `campaign.json`
`2f96f55347dfd69e090436416364685f0479c3801c466ff5d3418af99ed8b7fc`, `timeline.jsonl`
`cbf05017242feed6925c4c921fabcc924ced313933e6b736e543bdfe5eb54f1e`, `stage100/receipt.json`
`3fd22d861ae434c7f7e0e29ce21d3430b0982f376a50f06c8a0ee1a8b8f64ea0`, `stage100/gates.json`
`0c039b197546e3c77ba83712aa65d4cc3c219faf75ba431013580c78170d5aa4`, `stage100/host.json`
`97e1a89372e5a4a5c609ecbc2e6ff1bc46176effb0c5f18597a5acbacdbd701d`, `client-smoke/receipt.json`
`b165a597be86de271ede2293e01712d7806dcae8e65c1afd7b8cd60c9a55daef`, `query-funding-allowance.json`
`d4fdd26ef2fb9473e47f34b5b7d0e6046c3032594a9cda4fffdd3c05734858b5`, `a3-timeline/timeline.jsonl`
`90861375ce6d996d419faa42b1bc7cdfff907781030cdaa79e404a168e8e6974`, `a3-local/receipt.json`
`166bfb4536d714a2998695c7ea8242368f067896a59df23626da6a1e6b56323d`, `.run.log`
`c8e454d44fdb7387c6c7e5caf82491764cb9293e725ea932e33820915f4aadde`, `.guard.log`
`d36027f6367257c5cd576e7895b94ef0166a32bef1f22019f0a24bcd241db1e1`, `.guard/summary.json`
`b7baed86956012fb9479dccbe7070b175f39e14f9f46742d2dd0b3343efda887`, `.g11-watcher.log`
`3e38889ba15b2b70b1239be6fc4156b63924592b5c3567a62af40d692b0c4bed`, `.extra/SHA256SUMS`
`91b405c7fa3523303f6aede3c609c0f5e449314a1998dd9cab22d92291d9e4c2`.

Apparatus: clean run worktree `data-layer-p184-final-run-6d17b56f` (DL `6d17b56`, gate tool `e179ec78` = A3 pin), lock
`claude-s100-final-a3-6d17b56f` 12:18:58-13:10Z, Codex IDE tree paused 12:19:09-13:09:57Z, guard CLEAN, exact-client
smoke PASS (154 reads, 0 failed), Query funding allowance 60000 ms resolved from the deployed Query images, client
profile `qdl-v2-python:2.2.2-d6d2637` (`profile-stage100-rc4-r16.json` `12a8b578...`, quoted from the A3 freeze). Writer
recreates inside the protocol: trace re-arm x2 (12:19:39Z, 12:31:12Z, same image) and the G11 restart 13:05:42Z.

## RCA of the two gates that FAIL without A4 (what is known, what is not; A4 does not change it)

- **G2 MARK_INDEX_PRICE BINANCE max 1082.9 ms.** MARK is not a native writer feed: the value comes from a Query
  reference read woken by the Stream event (IDR-029; TS IDR-113 re-read on a stale woken read). Stalls measured in this
  load (`qdl.stall`): query_kn_1 slow_request 25 / loop_lag 1, query_kn_2 slow_request 105 / loop_lag 2, gc 0 (IDR-114
  fixed the GC cycles). The 1082.9 ms sample's segment split is not measurable on this trace basis (no per-sample DL
  receipt clock), so the cause of this sample is **not determined**. The trace basis (`ADAPTER_REFERENCE_RETURN`)
  also includes the TS writer's own read scheduling (Binance wake read delay 100 ms, 500 ms minimum interval, 1 s
  refresh fallback); how the tail divides between DL and TS is unknown. Next: IDR-126 (wake/poll markers in the TS
  trace), then IDR-119 (Query MARK read path).
- **G4 OKX_INVERSE book_delta / book_snapshot / quote maxima.** All on BTC-USD-SWAP; venue -> DL ~30 ms on every
  failing unit and the fresh-connection books probe saw no venue tail, so these are **internal** (DL ingest -> Kafka
  CreateTime -> observer path), not venue. Which internal stage (ingestor send, core commit, broker append, cleaner I/O,
  CPU quota) is **not separated** by this run. Optimisation items, one variable at a time with A/B: IDR-116 (Kafka cleaner
  I/O, C1), IDR-117 (broker CPU quota), IDR-118 (core commit batching).

## G12 - final credit (PASS, final image set)

- Receipt `~/.local/state/qdl-v2/p184-g12-v224-final-2deb90b0-20261018T042415Z/` (namespace `p184-g12-dl-202cf867bb6a5628`),
  run 04:25:02-04:51:45Z, lock "claude P18.4 G12 v224 FINAL 2deb90b0 20261018T041107Z" 04:24:15-04:52:31Z.
- Verdict v3 COMPLETED, `pass: true`; images ts 2deb90b0 / python 3e9e4d3a / stream+projector 60f83a51 / core 7c848aad
  (`g12-image-set-v224.json` `6303cdcb57eaefa16cdd33de2d010ac05e5944ee887ca0b9938e8f79afb4e2f9`). caught_up_s
  BINANCE / OKX: broker stop/start 0.095 / 0.095, projector restart 0.100 / 0.046, Stream restart 0.572 / 0.573 (bound
  600 s). Every fault: G1, G2 (TRADE/QUOTE/MARK lost 0, unseen 0), G3 BAR 1m, sampler PASS.
- `--credit` exit 0: creditable true, `production_parity` MATCH, `cleaner_binding` RUN; effective describe `d3d77176...`
  before and after. Writer clones strict_resume VERIFIED both venues. Teardown 12 removed, 0 left.
- sha256: `evidence/verdict.json` `170af83797df3309d4c9778042ee5b4e8f4c7ec5fca89fede22d9d1d0eeadc27`, `FINAL_CREDIT.json`
  `c6bfaedd1edefdcc76de1095e012b87fa85ce57c79fb43f645e2d272171dc8fc`, `production-parity.json`
  `d4e1f82d8e28a5585babc633dbdf93b85eb9cf369dfcf50472ea37f3a1355f82`, `credit-snapshot.json`
  `3aa5db2c800ed2e9d80249d88a3309201f802a469fcc238b7cbc2d5122448676`, `evidence/writer-precheck.json`
  `cc448e2de8beadd4e0aec7a5cadee19ee9f365004169ddf99d2d2a62a4706da3`, `c-observe.json`
  `598f05f87db0be06e938ce61aaebfc93cb06648add752ac4321d3ec077c948cd`, `c-observe-after.json`
  `14f9365ec98bd11a6d67d4cfb47cc95f5238efea8237cca31754cb1aa315a6ae`, `evidence/teardown.json`
  `53691d959d9f925e0b9856a807352475723fe89f11b6281407f13bf5e9ab12ff`.
- Pins: DL run worktree `data-layer-p184-final-run-bb054498`, TS WriterClone apparatus `trading-system-final-run-77a6d671`
  (coordinator decision H1), TS run worktree `trading-system-final-run-aa126794`. Inherited from other image sets:
  nothing. The ingestor image is not part of the G12 harness (synthetic TEST_ONLY publisher).
- Caveats (stated): integrity layer report-only; harness shape differs from production (one core member at 0.5 CPU,
  Kafka 0.75 CPU each, latest topic without production segment settings, synthetic publisher at 4 Hz BOOK); container
  overlap with foreign jobs NOT_DETERMINABLE after the fact (docker events rolled).

## G13 - 76/76 on writer 2deb90b0 (TS 30f50a91), packet v5.5

| Step | Cells | Result | Receipt | sha256 |
| --- | --- | --- | --- | --- |
| runtime window v5.5 (dr01-deployed 7, dr01-wrong-account 2, dr02-dr03 14, dr04-dr06 22, dr05-transport 9, dr07 12) | 66 | 66/66 OBSERVED ok, all 6 steps harness_ok | `p184-g13-20261018T051237Z` | summary `147a90aafbe755f83ae1d36cbd42be6f9b860ccded5de8fd44b4becdb434a0d7`, plan `a3c1a8193b41e2a31627cc9a07978c24fb310767db24b23df6a180d8c43be7ad` |
| dr08-isolated | 4 | 4/4 OBSERVED ok | `p184-g13-20261018T045558Z` | summary `17e731d670174a00a67950ef2cfe1594d37c896e72d2d337bdd6cbf78ef0d5bd`, evidence.jsonl `7eb4119b0cd38b8b4b06097438152bc71ed157bb5242f28189bcd1aabacb831d` |
| dr05-quiet (amendment-1 predicate) | 2 | 2/2 ISOLATED_FAULT OBSERVED, harness_ok | `p184-g13-20261018T071750Z` | summary `4f5aecbb5737f6295e4e595dbadf6d065b62ad25aad926b56b60a07b43acc084`, rows `eb0efbdecfd946b4d99eec9f215a9e014293b64dd402c5a31d4fe857554dd1f9` |
| dr05-upstream (after readiness predicate fix TS e9ae5484) | 6 | 6/6 ISOLATED_FAULT OBSERVED, harness_ok | `p184-g13-20261018T085316Z` | summary `b67dff6653879af030edb8d0bd0a7898a97342b3425a9f264302768e87a93489`, rows `4593442cdbe246be9ff9dea024971a4966a49c2816ea3df004de23577553dbb1` |

- W05 verifier in image 2deb90b0 (`--expect-source-sha 30f50a91...`, `--expect-image-digest sha256:2deb90b0...`) over the
  four receipts: **DR 76/76** (DR01 9, DR02 8, DR03 6, DR04 14, DR05 17, DR06 6, DR07 12, DR08 4), 0 DR issues, 0
  provenance/drift issues; status FAIL only from CASE_OPEN on the 260 non-DR matrix cells outside G13. Output
  `p184-g13-20261018T085316Z/verify/w05-final-v55-76.json` `5490c21e66bf33bae326f2ea2051677f70cf95372b13b823d4c2912835da9895`.
  Observation: this file is byte-identical to the v5.4 verifier output (`w05-final-v54.json`, same sha256), because the
  verifier output holds case states only and no run or image identity. The binding to writer 2deb90b0 rests on the
  verifier arguments and the four input receipts (recorded in the DL plan), not on this hash alone.
- Packet `packet-g13-v5.5-20261018.md` `507f9096ba8fba76d97441484d4c0d2146a9388aed298a1f01af47d3ca7320fc` (= its sidecar);
  run ids `g13r8wa20261017` / `g13r8fx01`; TS CI "Quality And V1 Contract" 39048572869 on 30f50a91 (success per the
  GitHub API at the time; not re-queried at assembly). Pins: DL run worktree `bb054498`, TS run worktrees `aa126794`
  (runtime, dr08), `a9cf1b41` (dr05-quiet), `e9ae5484` (dr05-upstream).
- Level: shadow-certified on the final image set (isolated cells TEST_ONLY harness; runtime cells TS stack on the
  production writer image; one host). Failed apparatus attempts kept, not scored: `045826Z`, `050244Z`, `070444Z`
  (IDR-111 follow-ups).

## Stage-100 attempts (receipts immutable; only `123222Z` is counted)

| Run | Digests (DL / writer) | protocol_met | Outcome | Status |
| --- | --- | --- | --- | --- |
| `20261016T024530Z` (#3) | pre-release / B16 | false | FAIL G9 (heartbeat counted 3x) | HISTORY, diagnostic |
| `20261017T032230Z` | c7ce7d3 set / 7c71e3af | false | FAIL G4 G9 | HISTORY (writer replaced, IDR-110) |
| `20261017T105025Z` | c7ce7d3 set / a44349a1 | true | FAIL G4 G8 | HISTORY (RCA; later fixes) |
| `20261017T120552Z`, `131831Z`, `132336Z`, `133644Z` | c7ce7d3 set / a44349a1 | - | guard TAINTED, no scorecard | TAINTED |
| `20261017T135719Z` (run 5) | c7ce7d3 set / a44349a1 | true | FAIL G2 G4 | HISTORY (led to IDR-112/113/114) |
| `20261018T053803Z` | final set / 2deb90b0 | true | FAIL G4 (mark_index OKX window 1) | HISTORY (led to G4-A1) |
| `20261018T074624Z` | final set / 2deb90b0 | false | FAIL G4 (book_delta OKX_INVERSE); funding-boundary driver failure | HISTORY (led to A1, A2) |
| `20261018T091556Z` | final set / 2deb90b0 | - | guard TAINTED (foreign ssh job) | TAINTED |
| `20261018T092255Z` | final set / 2deb90b0 | false | INVALID: driver client image lacked the A1 constant (ImportError on every read) | INVALID, preserved |
| `20261018T122102Z` | final set / 2deb90b0 | - | guard TAINTED (fwupdmgr) | TAINTED |
| **`20261018T123222Z`** | **final set / 2deb90b0** | **true** | **PASS G1 G3 G5-G11 G14; FAIL G2 G4 (frozen rule); PASS G2 G4 under post-hoc A4** | **COUNTED** |

Scorecard sha256 prefixes of the history rows: 024530Z `68c250ad72d3`, 032230Z `63330fed78fd`, 105025Z `23ed2ef25336`,
135719Z `da30687cd4f2`, 053803Z `8da1c190608e`, 074624Z `e0a786cb64d7`, 092255Z `ffb5b98e3400`; TAINTED marker of 122102Z
`537df486acda...`.

Report-only comparison of A4 on the history receipts (not certificate input; tests' mirrored receipts):
- 053803Z: the inverse rows PASS; G4 stays FAIL on `mark_index_price|OKX|`, which is out of scope.
- 135719Z: `quote|OKX_INVERSE|` FAIL. Its windows 2 and 3 predate observer v7 and have no outlier retention, which is
  UNKNOWN = FAIL. G2 stays FAIL on QUOTE|OKX.
- 074624Z: `book_delta|OKX_INVERSE|` FAIL for the same reason.
- 092255Z: `book_snapshot|OKX_INVERSE|` FAIL for the same reason.

So A4 does not pass every historical receipt.
