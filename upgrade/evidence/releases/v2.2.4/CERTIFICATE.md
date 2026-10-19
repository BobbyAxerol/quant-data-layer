# Quant Data Layer v2.2.4 - scoped release certificate

Status: **PASS on the final counted run `p184-final-campaign-20261019T040311Z`, under G4-A1, G4-A2 and A3 (frozen
before the run; A3 refreshed `384e8512` for the rustls images), A4 v2 (PRE_REGISTERED, applied, not needed) and A5
(POST_HOC_OWNER_DECISION, adopted after the run was scored, EXTERNAL_QUIET). Without A5 the frozen rule gives FAIL on
G11 only (one TRADE slice, venue silence). Both verdicts are kept side by side. NOT TAGGED, NOT PUBLISHED.**

Re-issued 2026-10-19. It supersedes the certificate of 2026-10-18 ~15:30Z, which was PASS under the post-hoc A4 on
`p184-final-campaign-20261018T123222Z` (certificate.json `da2fd484...` at `288a5805`). That run measured the pre-rustls
image set before the resource caps. The deployed set changed after it, so 123222Z is no longer the counted run, and
A4 v1 is not an input of this certificate.

Every sha256 below was recomputed with `sha256sum` or Python `hashlib`. Every image digest was read with
`docker inspect` / `docker image inspect` at the re-issue, read-only. Machine form: [certificate.json](certificate.json)
(schema `qdl.scoped-release-certificate.v2`). Its status is `PASS`, and its required `amendments` list flags A5
`post_hoc: true`.

Companion files: [SCORECARD.md](SCORECARD.md) / [scorecard.json](scorecard.json), [RELEASE_NOTES.md](RELEASE_NOTES.md),
[endpoint-report.json](endpoint-report.json), [equivalence.json](equivalence.json),
[a5-g11-trade-clock.md](a5-g11-trade-clock.md).

## 1. Certificate decision rule (fixed before the final numbers existed, unchanged)

- `status` is `PASS` only when all of these hold. `release.yml` refuses any other status.
  1. No gate is FAIL in the scorecard. A NOT_MEASURED gate is never upgraded.
  2. Final stage-100 on the release digests has `protocol_met: true`.
  3. G12 is the credited final run. G13 is 76/76 on the final digests (or each open cell is an owner-recorded limit).
  4. Required CI jobs are green on the release PR head.
- Anything else: do not create `certificate.json` with status PASS. No frozen threshold is widened after measuring.

Result on 040311Z:
- (1) holds **under A5**. Rescore receipt `scorecard.a5.json`
  `b1c2e7b450363f094c25adb34988f9356da327ef3d0b6a863a001dab2692d08d`: G11 PASS, every other gate byte-identical to the
  stored scorecard.
- (2) holds.
- (3) holds: G12 credited on the rolled set (`005952Z`), G13 76/76 on the rolled set (packet v5.6).
- (4) is an external gate. It runs on the PR head after the push.

So `status` = **PASS**. Stated plainly, the last sentence of the rule ("No frozen threshold is widened after measuring")
is **not met by A5**. A5 keeps the 30 s and 60 s limits, but it moves where the G11 clock starts for TRADE slices, after
measuring, by a recorded owner rule. That is why A5 is labelled `POST_HOC_OWNER_DECISION`, why the G11 verdict without
A5 stays in every surface, and why this certificate cannot be read as "the pre-registered rule passed".

What A5 rests on: the venue printed no OKX BNB-USDT-SWAP trade between ids 241971262 (04:36:15.209Z) and 241971263
(04:36:57.139Z). Both md.canonical.v2 and the OKX public trade history show this; the ids are adjacent. The writer
applied 241971263 0.413 s after its DL Kafka time. Full argument: [a5-g11-trade-clock.md](a5-g11-trade-clock.md).

Owner decisions:
- 2026-10-18 ~16:00Z (verbatim): "nếu gate không đạt xác định là lỗi external thì nới gate rồi release gọn nhé", and
  "duyệt hết theo recommend của bạn, k hỏi lại". Applied as A5. A4 v2 was pre-registered on 2026-10-18 ~16:40Z
  ("Duyệt A4 chốt trước (Recommended)").
- Earlier decisions that stay in force: release with stated limits and the optimisation package later (~13:40Z); the
  rustls roll (~15:50Z); the OPT resource package (~16:00Z) with the adjacent-baseline HOLD method (~20:05Z).

## 2. Identity

| Item | Value | Source |
| --- | --- | --- |
| Previous published release | `v2.2.3` (main merge `28e605ec6a359d462cd3c425a78519f9f8e70f63`, source `481fd67`) | git |
| Release head carrying this certificate | the packaging commit on `fix/p184-dl-closure` after `288a5805` (SHA reported in the PR) | git |
| Runtime source after the newest image revisions | rust: since `9dab224` only `539d28c`, a `#[cfg(test)]` observation hook + test in `qdl-native-raw-ingestor.rs` (no product code); python: apparatus only since `31a5155` (`qdl/certification/reference_l2_acceptance.py` and the `config/v2/p184-stage100-*` amendment files, read only by the gate tooling); compose: the OPT caps record (`b58e40d`) | `git diff` (RELEASE_NOTES.md "Source And Images") |
| Rust image (ingestors x2, stream x2) | `qdl-v2-rust:2.2.4-9dab224` `sha256:27c79b53400eea5582713a3477034c8561f533755e09a94223e254bffadd5c92`, label `9dab2242...` (rustls 0.23.46) | `docker image inspect` |
| Rust image (projectors x2) | `qdl-v2-rust:2.2.4-c7ce7d3` `sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e`, label `c7ce7d30...`; its projector equals the 9dab224 build whole-file | `docker image inspect`, 9dab224 build receipt |
| Rust image (rust_core x3, kept) | `qdl-v2-rust:2.2.3-rc22-a4e7b3b` `sha256:7c848aadcb0dd28178bc6dc2b58c42df6c2c235de6d4bfd6602ef7c19fa64bfb`; `qdl-realtime-core` contains no rustls code in either build | `docker image inspect`, symbol diff |
| Python image (query x2) | `qdl-v2-python:2.2.4-31a5155` `sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff`, label `31a51552...` | `docker image inspect` |
| SDK | `qdl-sdk 2.4.1`, wheel `9f79e032604bf79db5ca9070c7f8a99b242e7cf22e1659b395b307dc798c28e7` (two `--network none` builds at `c7ce7d3`, byte-identical; `.cdx.json` `6bda0014...`, `.release.json` `273e5326...`); `git diff c7ce7d3 HEAD -- qdl_sdk generated contracts scripts/build_qdl_sdk_release.py` is empty | `sha256sum`, `git diff` |
| TS writer measured with the release | `local/trading-system:p184-writer-release-30f50a91` `sha256:2deb90b0cc52eae7610fa44692f90df077e9db100ca259e38fd652a2dc1c5dc5` (TS `30f50a91`), receipt `release/30f50a91/receipt.json` `7040ba9fc68f17641ca69ffb9e52fa4240c9e9e4c956f06c2a3568ea97f64a1c` PASS | `docker inspect`, `sha256sum` |
| Resource caps (OPT K/Q/I, live) | kafka1-3 2.25 vCPU; query_kn_1/2 2.0 vCPU, 2 GiB, swap 4 GiB; ingestor_okx_swap 1.0 vCPU; ingestor_binance_usdm 0.75 vCPU; evidence `opt-resource-20261018/SHA256SUMS` `ba5e465e...` | `docker inspect` (`NanoCpus`, `Memory`) |
| Gate table (frozen 2026-10-09) | TS `upgrade/p184-phase-d-gates.md` `c67bf69bb921c992964eb8ff8b2e1bb17085a943fdf756d55526b6269641d5c4` | `sha256sum` |
| Frozen budget / addendum | `ff85335b3c8762d9acd8e7230f3c81ee1ec8403aff34cf2fb8f67714521c2541` / `4ed59dca0212566d3ff03c5a4c23eed3d252d0150844b9a4fd6fa543333d78b5` | `sha256sum`, campaign.json |
| Amendments frozen before the run (recorded in `campaign.json`) | G4-A1 (A1) `1c8a276f6cc6c03cdf768cdc9d31b65902bd849a1f3b7df56cda07ea47e6efa6`; G4-A2 (A2) `99961c41e08993944d158ae812ca61514271862a2d03e011f5d01ce71a0f9997`; A3 `384e85121c7c4c2fec06de12dd031a78550ac199fe9b6da9771dec118df5cec9` (runtime-image pin refresh of `cc5fc7dc...`, rules unchanged) | `sha256sum`, campaign.json |
| Amendment pre-registered and bound before load | A4 v2 `config/v2/p184-stage100-a4-v2-preregistered.json` `63f98668150bfda776317f97114b73855800515d0cfc9b7aa90ccb050f500324`; binding `f08b594a...` (04:03:11Z, before the campaign start 04:03:26Z), close `ba332523...`; receipt `scorecard.a4-v2.json` `7fffb04e...` | `sha256sum` |
| Amendment adopted after the run was scored (**POST_HOC_OWNER_DECISION**) | A5 `config/v2/p184-stage100-a5-g11-trade-clock.json` `c5b46c5faefe20e31061a3bd647b9e60537b982eee43b484715ec9cdcc5aae78` (frozen 2026-10-19T05:03:03Z); tool `scripts/p184_a5_g11_trade_clock.py` `4892abd1...`; frozen scorer unchanged `e179ec784621089795516ed25b2ea1f2e2ba30c86eea0de852ed4728aa6d104b` (the A3 pin) | `sha256sum` |

### Runtime at the re-issue (`docker inspect`, 2026-10-19 ~05:10Z)

| Role | Image | Revision label | Caps | RestartCount | OOM |
| --- | --- | --- | --- | --- | --- |
| query_kn_1, query_kn_2 | python `3e9e4d3a` | `31a5155` | 2.0 vCPU, 2 GiB | 0, 0 | false |
| ingestor_okx_swap, ingestor_binance_usdm | rust `27c79b53` | `9dab224` | 1.0 / 0.75 vCPU | 0, 0 | false |
| stream_kn_1, stream_kn_2 | rust `27c79b53` | `9dab224` | (unchanged) | 0, 0 | false |
| market_projector_1, _2 | rust `60f83a51` | `c7ce7d3` | (unchanged) | 0, 0 | false |
| rust_core, _2, _3 | rc22 `7c848aad` (kept) | `a4e7b3b` | (unchanged) | 0, 0, 0 | false |
| binance_bar_edge | rc14 `08f356a8` (held, scoped out) | `a388537` | (unchanged) | 0 | false |
| kafka1-3 | `9516fb76` (config + caps only) | none | 2.25 vCPU each | 0, 0, 0 | false |
| market_cache, stable_redis | `dfa18828` | none | (unchanged) | 0, 0 | false |
| market_data_service_p184w (TS) | `2deb90b0` (started 04:36:21Z by the G11 restart, same image) | TS `30f50a91` | - | 0 | false |

These images equal the ones the A4 v2 binding recorded before the load of 040311Z, and the ones its close receipt
recorded after the run. 8 DL application roles run v2.2.4 images: 4 on `9dab224`, 2 on `c7ce7d3`, 2 on `31a5155`.
The 3 cores stay on rc22 and the bar edge on rc14. That is not "all roles on one revision".

## 3. Evidence index

| Evidence | Path | sha256 |
| --- | --- | --- |
| Build receipt rust 9dab224 (PASS) + symbol diff vs `a9679def` | `~/.local/state/qdl-v2/build-v2.2.4-9dab224/{receipt.json, symbol-diff-vs-a9679def/summary.json}` | `e38fd27548f7482621d5ed1e7d2e6bc2225fd14aa39d365696d2eef0ab86674c` / `49f2164e88972600a1a80903f8b724c33f9bc23c83f6e362201636707c8762ef` |
| rustls roll M1-M4 (2026-10-18 18:46-19:06Z, HOLD PASS each) | `~/.local/state/qdl-v2/releases/v2.2.4/rollout-rustls-0.23.46/` | `SHA256SUMS.roll-20261018T1846Z` `c518f9b307bec97190f504a40b6d429b238d1feab4f7d77169a6a03f68d95ddc`; per-role receipts in [equivalence.json](equivalence.json) `roles[].rustls_roll` |
| OPT caps K/Q/I (2026-10-19 00:20-00:44Z, HOLD PASS each) | `~/.local/state/qdl-v2/releases/v2.2.4/opt-resource-20261018/` | `SHA256SUMS` `ba5e465e014138bb1b529859d04f97a238b6b478352a27d3ee6e3ebeebf4cdd1` |
| Earlier builds and rolls (83bef59, 31a5155, c7ce7d3 + addendum, 6d61710) | see [equivalence.json](equivalence.json) `final_builds`, `final_roll`, `production_roll` | (per file) |
| G12 final credit on the rolled set | `~/.local/state/qdl-v2/p184-g12-v224-final-27c79b53-20261019T005952Z/` | `evidence/verdict.json` `87386ad0f34423136132b4abb83077decd3573a382694b145b515cec3055449e`, `FINAL_CREDIT.json` `eb9d1e29a7b4769d4d5e9a2e96be3e0397bf24b3d56f695fb096116c25874dbf` |
| G13 76/76 on the rolled set (packet v5.6) | runtime `p184-g13-20261019T015047Z`, dr08 `012840Z`, dr05-upstream `014124Z`, dr05-quiet `013746Z` | summaries `5b897eaf...`, `1f2dc105...`, `0bb783ed...`, `d68f866a...`; W05 `5490c21e66bf33bae326f2ea2051677f70cf95372b13b823d4c2912835da9895`; packet `44bdecd4...` |
| Final stage-100 (counted) | `~/.local/state/qdl-v2/p184-final-campaign-20261019T040311Z/` | scorecard (frozen rule, untouched) `ebe11b8c10145bb92b7a850c4134d81101aaf7000a210f198b175316b20acbbf`, campaign.json `6f6098cb...`, guard summary `8593cbea...` |
| A5 rescore of the counted run | same directory, `scorecard.a5.json` (+ `.sha256`) | `b1c2e7b450363f094c25adb34988f9356da327ef3d0b6a863a001dab2692d08d` |
| A4 v2 rescore of the counted run (no verdict change) | same directory, `scorecard.a4-v2.json` (+ `.sha256`) | `7fffb04e3d06fb6dfbe32d749f59de64aa951bbd71d1ffe4b3284a1b8e4fceee` |
| G11 forensic evidence (canonical read, venue history, reader) | `~/.local/state/qdl-v2/p184-final-campaign-20261019T040311Z.g11-forensic/` | `canonical.jsonl` `cf96554e...`, `okx-history-trades-after-241971264.json` `b579fdb9...`, `reader.py` `5fcc088a...` |
| A5 rationale | [a5-g11-trade-clock.md](a5-g11-trade-clock.md) | (certificate.json `evidence_sha256`) |
| Endpoint report | [endpoint-report.json](endpoint-report.json) | `013b66e683a9ed6846a82d8b858a5a5bc5dcb28cebf80688eb7c099fae4740b7` |
| Deployed Paper consumer manifest r11 (packaging finding, section 6a) | [trading-system.paper.stable-r11.yaml](trading-system.paper.stable-r11.yaml), provenance [consumer-manifest-r11-provenance.json](consumer-manifest-r11-provenance.json) | `efd482fa4ffe7f9176387324f47d47d7e1c801bae6afb2c0747c3ce43652b8fc` (= deployed) |
| Image sets | `g12-image-set-v224.json` / `g13-roles-v224.json` (this directory) | in certificate.json `evidence_sha256` |
| Remote CI | PR head of `fix/p184-dl-closure` -> `dev` | external gate, recorded in the PR (not in this file) |

## 4. Gates

[SCORECARD.md](SCORECARD.md): PASS G1-G10 and G14 (counted stage-100, with and without A4 v2), PASS G12 (credited),
PASS G13 (76/76). G11, side by side:

| Gate | Without A5 (frozen rule) | With A5 (POST_HOC_OWNER_DECISION) |
| --- | --- | --- |
| G11 | **FAIL**: first valid apply max 36.415 s > 30 s, on OKX BNB-USDT-SWAP TRADE only; caught up max 36.415 s (<= 60); 0 rewinds; cursor never ahead | **PASS**: that slice 0.413 s from its first post-fault event; max over 64 slices 4.928 s; caught-up, rewind and cursor checks unchanged |
| Summary | PASS 13, FAIL 1 (G11) | PASS 14, FAIL 0, NOT_MEASURED 0, DEFERRED 0 |

A4 v2 changes no verdict. G2 and G4 PASS without it. Under the frozen addendum the OKX_INVERSE maxima are 215.8 /
276.3 / 320.0 ms, against limits 282.5 / 300.7 / 322.6. MARK_INDEX_PRICE BINANCE max is 515.5 ms, against < 1000.

Amendments, all required to read this certificate: G4-A1 `1c8a276f...` (pre-run), G4-A2 `99961c41...` (pre-run), A3
`384e8512...` (pre-run), A4 v2 `63f98668...` (pre-registered, not needed), **A5 `c5b46c5f...` (post hoc for 040311Z)**.
Full values are in section 2 and in certificate.json `amendments`.

## 5. Limits stated by this certificate

1. **PASS rests on a post-hoc amendment for one gate.** G11 FAILs under the frozen rule and PASSES only under A5,
   adopted after observing 040311Z.
   - The frozen clock times venue silence on the least liquid demanded TRADE slice. OKX BNB-USDT-SWAP TRADE was at
     the G11 first-valid-apply max in 19 of the 28 G11 runs with a restart since 2026-10-10 (13 alone, 6 tied),
     counted from the stored `g11-summary.json` files.
   - The A5 evidence is a canonical-topic read produced after the run. Its read window is recorded in the DL plan; the
     tool proves coverage from the records.
2. **Host workload isolation (capacity limit, IDR-128 open).** The counted run was measured with 15 portal containers
   paused and the Codex tree stopped: host load1 p50 16.2 / p95 20.6 / max 25.1 on 16 vCPU. The same images and caps
   with the portal stack running (021044Z, load1 22.0 / 26.6 / 29.4) FAILED G4 (bar BINANCE 1m, book_snapshot BINANCE,
   book_snapshot OKX_INVERSE window 5) and G8 (one writer EVAL of 12.25 ms). **Production must not run unrelated heavy
   workloads on the DL host.**
3. Scoped Data Layer certificate. It is not a Trading System order, accounting or alpha certification. Not HA: one
   host.
4. rustls 0.23.46 (RUSTSEC-2026-0346..0349): at machine-code level the change is not confined to rustls functions in
   the ingestor and gateway binaries (same-source codegen; symbol diff `49f2164e...`). Behaviour rests on the per-role
   HOLDs, G12, G13 and this stage-100, not on the diff.
5. rust_core x3 stay on rc22 `7c848aad`. `qdl-realtime-core` has no rustls code in either build, so the advisories do
   not apply to it. `binance_bar_edge` is held on rc14 `08f356a8` (not a v2.2.4 revision).
6. G14: DL mean CPU sum 5.75 vCPU at stage 100, above the owner's 5 vCPU budget (recorded, not a gate). query_kn_1
   peaked at 1,822.7 MiB of its 2,048 MiB limit; it would have exceeded the old 1,536 MiB limit.
7. IDR-108 OPEN: Stream accepts on 8210 before its hot view serves.
8. The G12 harness shape is not the production shape. Its integrity layer is report-only. WriterClone apparatus pin
   `77a6d671` (IDR-120).
9. G13 cells are shadow-certified. The W05 output carries no run identity (byte-identical to the v5.5 output).
10. The TS writer vendors SDK 2.4.0 while this release publishes 2.4.1.
11. OKX FUNDING_RATE `max_freshness_ms` equals the 8 h cadence. The A1 apparatus parity applies Query's +60 s allowance
    in the verifier; the contract option is IDR-125.
12. Consumer manifest packaging (section 6a): the deployed Paper manifest is r11, and the repository's loadable file
    stays r10 (IDR-127).
13. Still open from the optimisation package: IDR-116..IDR-119, IDR-126. On 040311Z they bound no gate.

## 6. Rollback

See [RELEASE_NOTES.md](RELEASE_NOTES.md) "Rollback". In short:
- ingestors and stream -> `a9679def` / `60f83a51` (rustls rollback composes);
- query -> `c21e164a`;
- writer -> `a44349a1`;
- caps -> `docker update` back to the pre-OPT values;
- Kafka config commands as before.

Not touched by this release: kafka1-3 images, rust_core x3, binance_bar_edge, market_cache, stable_redis, catalog,
consumer manifests, JWT revisions, realm dirs, offsets, Redis data.

## 6a. Packaging finding: deployed consumer manifest r11 (unchanged since 2026-10-18)

- Deployed in query_kn_1 and query_kn_2: `/consumer-realms/trading-system.paper.stable-r11.yaml` `efd482fa...`
  (`docker exec ... sha256sum`, read-only, 2026-10-18). It comes from the realm dir mount
  `p184-d3-capacity-20261009/reader-public-r16`.
- The repository file `consumers/stable/trading-system-paper.yaml` is r10 `1e489237...` (61 requirements, no
  BTC-USD-SWAP). The image also bakes this r10 file, but Query does not use it for the Paper entry.
- r11 source: `scripts/prepare_paper_read_extension.py` (commit `76db472`) with quotas 77 / 3000, from the deployed
  runtime catalog `fc3a1e1e...` (revision 12), the r10 manifest and the r22 binding. Re-run on 2026-10-18 in a
  `--network none` container of `qdl-v2-python:2.2.4-31a5155`: byte-identical, `efd482fa...` = deployed.
- Packaged as a byte copy ([trading-system.paper.stable-r11.yaml](trading-system.paper.stable-r11.yaml)) with
  [consumer-manifest-r11-provenance.json](consumer-manifest-r11-provenance.json).
- **Not done:** replacing the loadable repository file. With r11 there, 13 manifest/catalog test modules gave 1
  failure and 38 errors, because the repository catalog revision 11 does not declare BTC-USD-SWAP, the routing pins the
  r10 binding and one test expects exactly 10 MARK bindings. This needs one separate slice (IDR-127). Runtime unchanged.

## 7. Publication (STOP before tagging)

- certificate.json has `status: "PASS"`, which `release.yml` accepts.
- The other checks of that step hold. The endpoint report sha256 in certificate.json
  (`013b66e683a9ed6846a82d8b858a5a5bc5dcb28cebf80688eb7c099fae4740b7`) equals the file. RELEASE_NOTES.md exists.
- The SDK wheel check (`9f79e032...`) is unaffected: SDK sources are unchanged since `c7ce7d3`.
- `release.yml` was not changed and was not weakened.
- Still required before a tag:
  - the release head on `origin/main` (`release.yml` `git merge-base --is-ancestor`);
  - green required CI on the PR head;
  - an explicit owner instruction to tag.
- **No tag was created. Nothing was merged.**

The analysis at the first assembly (2026-10-18 13:40Z, status FAIL, options A/B/C) is in git history at `6909f71`. It
is no longer needed: no workflow change was made.
