# Quant Data Layer v2.2.4 - gate scorecard G1-G14 (FINAL, counted run 040311Z)

Status: **FINAL. Counted run `p184-final-campaign-20261019T040311Z` on the deployed set (rustls 0.23.46 images + OPT
caps).**
- Under the pre-registered rules (G4-A1 + G4-A2 + A3, A4 v2 bound before load): PASS G1-G10 and G14, **FAIL G11 on
  one TRADE slice**.
- Under A5 (POST_HOC_OWNER_DECISION, EXTERNAL_QUIET): every gate PASS.
- Both verdicts are shown side by side below. A4 v2 changes no verdict.

Re-issued 2026-10-19. It supersedes the scorecard of 2026-10-18 on `123222Z` (PASS under post-hoc A4; release
scorecard.json `52979689...` at `288a5805`), which measured the pre-rustls set. Every sha256 was recomputed. Machine
form: [scorecard.json](scorecard.json) (schema `qdl.release-scorecard.v1`).

## Frozen inputs and amendments

- Gate table: TS `upgrade/p184-phase-d-gates.md`, FROZEN 2026-10-09, sha256
  `c67bf69bb921c992964eb8ff8b2e1bb17085a943fdf756d55526b6269641d5c4`.
- Budget `config/v2/p184-stage100-budget.json` `ff85335b3c8762d9acd8e7230f3c81ee1ec8403aff34cf2fb8f67714521c2541`; G4
  addendum `config/v2/p184-stage100-g4-addendum.json` `4ed59dca0212566d3ff03c5a4c23eed3d252d0150844b9a4fd6fa543333d78b5`.
- Frozen before the run and recorded in `campaign.json`:
  - G4-A1 (A1) `1c8a276f6cc6c03cdf768cdc9d31b65902bd849a1f3b7df56cda07ea47e6efa6`: row `mark_index_price|OKX|`,
    same-event internal split.
  - G4-A2 (A2) `99961c41e08993944d158ae812ca61514271862a2d03e011f5d01ce71a0f9997`: bar rows >= 8h without a frozen
    baseline REPORT_ONLY; no threshold change.
  - A3 `384e85121c7c4c2fec06de12dd031a78550ac199fe9b6da9771dec118df5cec9`. This is a runtime-image pin refresh of
    `cc5fc7dc...` for the rustls images; rules and limits are unchanged
    ([a3-external-attribution.md](a3-external-attribution.md)). A3 was applied in the run and rescued no unit.
- Pre-registered and bound to the run before load: A4 v2 `63f98668150bfda776317f97114b73855800515d0cfc9b7aa90ccb050f500324`
  ([a4-v2-preregistered.md](a4-v2-preregistered.md)).
  - Binding `f08b594a...` was written at 04:03:11Z, before the campaign start at 04:03:26Z.
  - The close receipt `ba332523...` recorded the same images and caps after the run.
  - Receipt `scorecard.a4-v2.json` `7fffb04e3d06fb6dfbe32d749f59de64aa951bbd71d1ffe4b3284a1b8e4fceee`.
  - Applied, not needed: every verdict equals the scorecard without it.
- **Adopted after the run was scored (POST_HOC_OWNER_DECISION):** A5
  `config/v2/p184-stage100-a5-g11-trade-clock.json` `c5b46c5faefe20e31061a3bd647b9e60537b982eee43b484715ec9cdcc5aae78`
  ([a5-g11-trade-clock.md](a5-g11-trade-clock.md)).
  - Rule: a G11 TRADE slice is timed from max(fault cleared, DL Kafka time of its first canonical TRADE event after the
    fault, with a 3 s guard). Canonical-topic evidence is required. Caught up, no rewind and cursor never ahead are
    unchanged.
  - Rescore `scripts/p184_a5_g11_trade_clock.py` (`4892abd1...`) drives the unchanged frozen scorer (`e179ec78`, the A3
    pin). Receipt `scorecard.a5.json` `b1c2e7b450363f094c25adb34988f9356da327ef3d0b6a863a001dab2692d08d`. The stored
    `scorecard.json` is untouched.
- Scoring rule: a PASS needs steady state >= 30 min at the stage-100 workload plus one burst window, with
  `protocol_met: true`. A measurement outside the protocol can FAIL a gate but cannot PASS it. NOT_MEASURED is never
  upgraded.

## Measured identity (docker inspect; equal to the A4 v2 binding and close of 040311Z; RestartCount 0, OOMKilled false)

| Role | Image | Tag | Revision label |
| --- | --- | --- | --- |
| ingestor_okx_swap, ingestor_binance_usdm, stream_kn_1, stream_kn_2 | `sha256:27c79b53400eea5582713a3477034c8561f533755e09a94223e254bffadd5c92` | `qdl-v2-rust:2.2.4-9dab224` | `9dab224` |
| market_projector_1, _2 | `sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e` | `qdl-v2-rust:2.2.4-c7ce7d3` | `c7ce7d3` |
| rust_core x3 | `sha256:7c848aadcb0dd28178bc6dc2b58c42df6c2c235de6d4bfd6602ef7c19fa64bfb` | `qdl-v2-rust:2.2.3-rc22-a4e7b3b` | `a4e7b3b` |
| query_kn_1, query_kn_2 | `sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff` | `qdl-v2-python:2.2.4-31a5155` | `31a5155` |
| binance_bar_edge (held, scoped out) | `sha256:08f356a84151c6e865fbdc4a2efe6924df9336d15353254c2b2da684261794f5` | `qdl-v2-python:2.2.3-rc14-a388537` | `a388537` |
| market_data_service_p184w (TS writer) | `sha256:2deb90b0cc52eae7610fa44692f90df077e9db100ca259e38fd652a2dc1c5dc5` | `local/trading-system:p184-writer-release-30f50a91` | TS `30f50a91` |

Caps: kafka1-3 2.25 vCPU; query_kn_1/2 2.0 vCPU / 2 GiB / swap 4 GiB; ingestor_okx_swap 1.0 vCPU; ingestor_binance_usdm
0.75 vCPU.

## Scorecard (final stage-100 `p184-final-campaign-20261019T040311Z`, load 04:05:17-04:35:17Z, protocol_met true)

| Gate | Rule (frozen) | Verdict | Numbers (from the campaign `scorecard.json`) |
| --- | --- | --- | --- |
| G1 | BOOK p95 <= 500, p99 <= 1000, max < 2000 ms | **PASS** | p95 / p99 / max (n): BOOK_DELTA BINANCE 164.8 / 223.6 / 617.7 (84,460); BOOK_DELTA OKX 171.6 / 231.4 / 589.3 (93,546); BOOK_SNAPSHOT BINANCE 180.9 / 256.5 / 583.1 (9,253); BOOK_SNAPSHOT OKX 181.5 / 253.1 / 609.5 (11,222) |
| G2 | TRADE p95 <= 500, max < 1500; QUOTE and MARK p95 <= 500, max < 1000 ms | **PASS** (with and without A4 v2) | p95 / max: TRADE BINANCE 196 / 376.8, TRADE OKX 180 / 388.4, QUOTE BINANCE 153 / 405.8, QUOTE OKX 162 / 410.0, MARK BINANCE 350.2 / 515.5 (n 9,000), MARK OKX 332.9 / 586.8 |
| G3 | BAR p95 <= 5 s, max < 60 s | **PASS** | pooled p95 1641 / max 3208 ms (n 320); BINANCE p95 1891, OKX p95 1560; read errors 0 |
| G4 | per row vs the frozen baseline (table p95 x 1.2; addendum max x 1.2) + A1 / A2 / A3 | **PASS** (with and without A4 v2) | every frozen-table row inside, e.g. bar BINANCE 1m p95 per window 106 / 105 / 142 / 146 / 225 vs 257.6; book_snapshot BINANCE 144 / 141 / 135 / 141 / 173 vs 260; book_delta OKX 136-157 vs 289.1; quote OKX 152-181 vs 283.3. Addendum OKX_INVERSE max per window: book_delta <= 215.8 vs 282.5, book_snapshot <= 276.3 vs 300.7, quote <= 320.0 vs 322.6, mark_index_price <= 2174.9 vs 2597.8. G4-A1 `mark_index_price|OKX|` internal p95 89-101 vs 200.9. 11 bar rows report-only |
| G5 | stage-50 p95/p99 targets per class; cold overlap | **PASS** | 15 latency/cold gates PASS (e.g. MARK_INDEX_REFERENCE BINANCE p95 32.0 / p99 52.8 of 250 / 500; L2_SNAPSHOT BINANCE 52.6 / 71.1 of 300 / 750; TRADE_SNAPSHOT BINANCE 50.1 / 65.5 of 100 / 250) |
| G6 | per-binding policy; 0 unsafe | **PASS** | 12 required D3 bindings measured; unsafe 0, refusals 0 |
| G7 | SDK -> writer p95 <= 100 ms; writer -> Redis p95 <= 50 ms | **PASS** | p95 SDK -> writer / writer -> Redis ms: BOOK_DELTA 28.6 / 5.7, BOOK_SNAPSHOT 16.2 / 12.3, MARK 0.8 / 7.5, QUOTE 1.0 / 2.0, TRADE 6.0 / 6.0 |
| G8 | commandstats <= 60 % core / 10 min; SLOWLOG 0 steady; PING p99 <= 5 ms | **PASS** | core fraction 0.071-0.082; new SLOWLOG entries 0; PING steady p99 1.048 ms; ping failures 0; monitor clients 0 |
| G9 | demanded == session-ready all window; 0 data-path losses; v1 fallback 0 | **PASS** | 60 samples, demanded 64 == session-ready on every sample; data-path losses 0 (26 legitimate quiet TRADE_EVENT_STALE refusals excluded); v1 fallback 0 |
| G10 | TRADE 0 gaps / 0 duplicates applied / 0 replay-expired / 0 overflow | **PASS** | all zero (TRADE BINANCE applied 113,269, OKX 28,687) |
| G11 | first valid apply <= 30 s, caught up <= 60 s; no rewind; cursor never ahead | **FAIL** without A5 / **PASS** under A5 | outage 0.113 s; 64 slices; first valid apply max **36.415 s** (OKX BNB-USDT-SWAP TRADE only; every other slice <= 4.928 s); caught up max 36.415 s; rewinds 0; future events 0; cursor never ahead (3 samples, 21 of 21 compared). Under A5 that slice is 0.413 s from its first post-fault event (trade 241971263, Kafka 04:36:57.191Z; fault cleared 04:36:21.189Z; the previous trade 241971262 was at 04:36:15.209Z) and the max is 4.928 s |
| G12 | Kafka broker / projector / Stream restart caught up <= 600 s, then G1-G3 inside | **PASS - CREDIT** | separate isolated run on the rolled set (below) |
| G13 | DR01-DR08, 76 cells | **PASS - 76/76** | separate runs on the rolled set (below) |
| G14 | CPU/RAM recorded per role (not a headroom gate) | **PASS** (recorded) | 18 roles; memory at/over limit none; DL mean CPU sum 5.75 vCPU (owner budget 5: stated limit); query_kn_1 mem max 1,822.7 of 2,048 MiB; throttled <= 0.3 % of wall on every role |

Summary without A5: PASS G1-G10 and G14 (campaign) plus G12 and G13 (separate credited runs), 13 in all; **FAIL G11**;
NOT_MEASURED 0; DEFERRED 0.

Summary with A5 (post hoc): PASS 14 (G1-G14); FAIL 0; NOT_MEASURED 0; DEFERRED 0. The rescore leaves every gate other
than G11 byte-identical (checked by the tool).

Campaign files (sha256):

| File | sha256 |
| --- | --- |
| `scorecard.json` | `ebe11b8c10145bb92b7a850c4134d81101aaf7000a210f198b175316b20acbbf` |
| `scorecard.a5.json` | `b1c2e7b450363f094c25adb34988f9356da327ef3d0b6a863a001dab2692d08d` |
| `scorecard.a4-v2.json` | `7fffb04e3d06fb6dfbe32d749f59de64aa951bbd71d1ffe4b3284a1b8e4fceee` |
| `campaign.json` | `6f6098cb950c177237cd663cc36b6891c85d12f19aaa8b1b35a84c174e9b3ee0` |
| `g11/g11-summary.json` | `77cac9b625bd1729cd46f7b003b40b397fd5d4de2f29dee7f8da79cc4b59253f` |
| `g11/g11-observations.jsonl` | `01eddf13fbe0802f1ecc32fff1c0da73517ee68a024524786450e86be1385adf` |
| `.guard/summary.json` (CLEAN, aborted false) | `8593cbeaf986d89bf22e36a3d225a3b0b35ea1f2d13f60e815f85e03aebded01` |
| `.guard/host.jsonl` | `a18a6bf80414829b9a9b0ce326308573acea278aecc323e2f2f8b77340d6007c` |

The remaining files (timeline, stage100 receipt / gates / host, client smoke, query allowance, A3 timeline and local
receipt, trace, logs, A4 v2 binding and close) are listed with their sha256 in [scorecard.json](scorecard.json)
`campaign.files` / `campaign.logs`.

Apparatus:
- Run worktree `data-layer-p184-final-run-b58e40dd` (DL `b58e40d`, gate tool `e179ec78` = the A3 pin).
- Lock `claude-s100-final-lowload-rerun-b58e40dd`, taken at 03:57:44Z and released after the run.
- Driver 04:03:52-04:35:22Z. 5 G4 windows (04:05:52, 04:09:37, 04:13:22, 04:17:07, 04:29:30Z). G11 restart
  04:36:21Z.
- Guard CLEAN, aborted false. One foreign `package-data-downloader` at 04:37:04Z (0.06 core) ran after the load, in
  phase post_load_g11, after the G11 slice in question had applied.
- **Load reduction:**
  - 15 portal containers `docker pause`d by id for the run and unpaused at the end (rc 0);
  - Codex tree SIGSTOP / SIGCONT by `p184_codex_pause_watch.py`.
  - Host load1 in the load phase: p50 16.2 / p95 20.6 / max 25.1 (n 1,864) on 16 vCPU. For comparison, 021044Z with
    the portal stack running: 22.0 / 26.6 / 29.4 (n 1,861). Source: guard `host.jsonl`.

## G11 under A5 (EXTERNAL_QUIET)

- Evidence, read-only (`p184-final-campaign-20261019T040311Z.g11-forensic/`): md.canonical.v2 read_committed, assign
  only, all 6 partitions finished, 64,589 records scanned (`canonical.jsonl` `cf96554e...`, `reader.py` `5fcc088a...`).
  - OKX BNB-USDT-SWAP trades sit on partition 2, connection generation 210.
  - Trade 241971262 (venue 04:36:15.209Z) is followed directly by 241971263 (venue 04:36:57.139Z, DL receive +28 ms,
    Kafka 04:36:57.191Z).
  - OKX public REST history agrees, with adjacent ids (`okx-history-trades-after-241971264.json` `b579fdb9...`).
- Writer: the TS cache key was rewritten at 04:36:57.604Z (one 500 ms poll upper bound), carrying 04:36:57.139Z.
- During the 36.4 s the key stayed readable, with DL quality LAST_EVENT_STALE / not execution-eligible (78 POST polls):
  the correct at-use state for a stale last trade.
- A5 slice record (receipt `b1c2e7b4...`):
  - clock start 04:36:57.191Z (first_trade_event), A5 first valid apply 0.413 s (0.465 s from the venue time);
  - 1 post-fault event before the apply;
  - venue trade ids adjacent (report-only).
- The other 9 TRADE slices have no canonical evidence and keep the frozen clock (<= 2.408 s).

## G12 - final credit on the rolled set (PASS)

- Receipt `~/.local/state/qdl-v2/p184-g12-v224-final-27c79b53-20261019T005952Z/`. Lock 00:59:46-01:28:35Z. Same
  shape as the 10-18 credit. Image set `g12-image-set-v224.json` (stream 27c79b53).
- Verdict COMPLETED, `pass: true`. caught_up_s BINANCE / OKX: broker stop/start 0.245 / 0.245, projector restart
  0.313 / 0.314, Stream restart 0.984 / 0.985 (bound 600 s).
- `--credit`: creditable true, production_parity MATCH, cleaner_binding RUN. Teardown 12 removed, 0 left.
- sha256: `evidence/verdict.json` `87386ad0f34423136132b4abb83077decd3573a382694b145b515cec3055449e`, `FINAL_CREDIT.json`
  `eb9d1e29a7b4769d4d5e9a2e96be3e0397bf24b3d56f695fb096116c25874dbf`, `SHA256SUMS`
  `61dc4d641862674e1da06d405f294091c9769bf63a3dd7ca40c9c373e1874291` (others in scorecard.json `gates.G12.files`).
- Caveats (unchanged): integrity layer report-only; the harness shape differs from production; WriterClone apparatus
  pin `77a6d671` (H1).

## G13 - 76/76 on the rolled set, writer 2deb90b0 (TS 30f50a91), packet v5.6

| Step | Cells | Result | Receipt | summary sha256 |
| --- | --- | --- | --- | --- |
| runtime window v5.6 (dr01-deployed 7, dr01-wrong-account 2, dr02-dr03 14, dr04-dr06 22, dr05-transport 9, dr07 12) | 66 | every step harness_ok | `p184-g13-20261019T015047Z` | `5b897eaf105b5bcdf8ae3723e43ec11dc3ec0c35abfae25007e211ff5befb747` |
| dr08-isolated | 4 | 4/4 OBSERVED ok | `p184-g13-20261019T012840Z` | `1f2dc10526cc913e452392a807cf8ab2ae8961897ca1c7c908b381e5c2eb4240` |
| dr05-upstream (rerun; the first attempt 013112Z failed a precondition, a harness readiness race, kept) | 6 | 6/6 OBSERVED ok | `p184-g13-20261019T014124Z` | `0bb783ed4c663044b2e86c14e60f13a0b4407334137447bfaabc652cb7bc3294` |
| dr05-quiet | 2 | 2/2 ISOLATED_FAULT OBSERVED ok | `p184-g13-20261019T013746Z` | `d68f866afd408f7a62e536f83946037c1a60a04673b269af7bcdc368d7d56897` |

- W05 in 2deb90b0 over the 6 runtime receipts and the three isolated runs: **DR 76/76** (DR01 9, DR02 8, DR03 6, DR04
  14, DR05 17, DR06 6, DR07 12, DR08 4).
- Output `verify/w05-final-v56.json` `5490c21e66bf33bae326f2ea2051677f70cf95372b13b823d4c2912835da9895`. It is
  byte-identical to the v5.5 output, because it holds case states only. The binding to this run rests on the
  verifier arguments and the input receipts.
- Packet `packet-g13-v5.6-20261019.md` `44bdecd4201e53592b2b520d040c9e0fd9a9a43e70fe85916eae4f4e382fb3da`.
- Level: shadow-certified (isolated cells on the TEST_ONLY harness; runtime cells on the TS stack with the production
  writer image; one host).

## Stage-100 attempts (receipts immutable; only `040311Z` is counted)

| Run | Digests (DL / writer) | protocol_met | Outcome | Status |
| --- | --- | --- | --- | --- |
| `20261016T024530Z` (#3) | pre-release / B16 | false | FAIL G9 (heartbeat counted 3x) | HISTORY, diagnostic |
| `20261017T032230Z` | c7ce7d3 set / 7c71e3af | false | FAIL G4 G9 | HISTORY (writer replaced, IDR-110) |
| `20261017T105025Z` | c7ce7d3 set / a44349a1 | true | FAIL G4 G8 | HISTORY |
| `20261017T120552Z`, `131831Z`, `132336Z`, `133644Z` | c7ce7d3 set / a44349a1 | - | guard TAINTED, no scorecard | TAINTED |
| `20261017T135719Z` (run 5) | c7ce7d3 set / a44349a1 | true | FAIL G2 G4 | HISTORY (led to IDR-112/113/114) |
| `20261018T053803Z` | pre-rustls set / 2deb90b0 | true | FAIL G4 (mark_index OKX window 1) | HISTORY (led to G4-A1) |
| `20261018T074624Z` | pre-rustls set / 2deb90b0 | false | FAIL G4; funding-boundary driver failure | HISTORY (led to A1, A2) |
| `20261018T091556Z` | pre-rustls set / 2deb90b0 | - | guard TAINTED (foreign ssh job) | TAINTED |
| `20261018T092255Z` | pre-rustls set / 2deb90b0 | false | INVALID: driver client image lacked the A1 constant | INVALID, preserved |
| `20261018T122102Z` | pre-rustls set / 2deb90b0 | - | guard TAINTED (fwupdmgr) | TAINTED |
| `20261018T123222Z` | pre-rustls set / 2deb90b0 | true | FAIL G2 G4 (frozen); PASS under post-hoc A4 | **SUPERSEDED** (was counted 2026-10-18; set changed since) |
| `20261019T021044Z` | rustls set + OPT caps / 2deb90b0 | true | FAIL G4 (bar BINANCE 1m, book_snapshot BINANCE, book_snapshot OKX_INVERSE window 5), FAIL G8 (writer EVAL 12.25 ms); portal stack running, load1 p50 22.0 | HISTORY (led to the low-load rerun and IDR-128) |
| `20261019T030130Z` | rustls set + OPT caps / 2deb90b0 | - | guard TAINTED + aborted (foreign `(mandb)` 0.34 core in the load) | TAINTED, not scored |
| **`20261019T040311Z`** | **rustls set + OPT caps / 2deb90b0** | **true** | **PASS G1-G10 G14; FAIL G11 (frozen, one EXTERNAL_QUIET slice); PASS G11 under post-hoc A5** | **COUNTED** |

Scorecard sha256 prefixes of the history rows:
- 024530Z `68c250ad72d3`, 032230Z `63330fed78fd`, 105025Z `23ed2ef25336`, 135719Z `da30687cd4f2`;
- 053803Z `8da1c190608e`, 074624Z `e0a786cb64d7`, 092255Z `ffb5b98e3400`, 123222Z `b9e76a83795f`;
- 021044Z `c0504f4bb6fa`; guard summary of 030130Z `c3d551ba566c`.

A5 on the history receipts (report-only, from the tests): none of them has canonical evidence, so A5 changes no
verdict on any of them.
