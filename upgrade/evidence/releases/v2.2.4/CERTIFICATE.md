# Quant Data Layer v2.2.4 - scoped release certificate (DRAFT)

Status: **DRAFT - NOT CERTIFIED, NOT PUBLISHED.** Prepared 2026-10-17 ~11:00Z while the host lock
`claude P18.4 G13 v5.4 window (packet-g13-v5.4-20261017)` was held by another agent. The only actions were file
edits, `sha256sum`, read-only `docker inspect` / `docker image inspect` and git reads.

This file holds the certificate text and the field-by-field plan for the machine certificate. The machine form that
`.github/workflows/release.yml` reads is `certificate.json` (schema `qdl.scoped-release-certificate.v2`, same shape as
[v2.2.3](../v2.2.3/certificate.json)). It is written from this file at finalisation, never before the PENDING items
below are closed. Every `<<PENDING:...>>` marker names the receipt that fills it.

Companion drafts in this directory: [SCORECARD.md](SCORECARD.md), [RELEASE_NOTES.md](RELEASE_NOTES.md),
[equivalence.json](equivalence.json), [drafts/](drafts/).

## 1. Certificate decision rule (fixed before the final numbers exist)

- `status` is `PASS` only when all of these hold. `release.yml` refuses any other status.
  1. No gate is FAIL in the scorecard. A gate that is NOT_MEASURED stays NOT_MEASURED and is listed as a limit; it is
     never upgraded (gate table rule).
  2. Final stage-100 on the release digests has `protocol_met: true` (see SCORECARD G1-G11, G14).
  3. G12 is the credited final run (PASS, below). G13 is 76/76 on the final digests, or each open cell is carried as
     a stated limit by an owner decision recorded in the Unified Plan (as v2.2.3 did for the TS-stack cells).
  4. Required CI jobs are green on the release PR head.
- Anything else: do not create `certificate.json` with status PASS. Publish nothing, record the result in the plan, and
  report the blocker. No frozen threshold is widened after measuring.

## 2. Identity

| Item | Value | Source (verified 2026-10-17) |
| --- | --- | --- |
| Previous published release | `v2.2.3`, tag object annotated, main merge `28e605ec6a359d462cd3c425a78519f9f8e70f63`, source `481fd67` | `git rev-parse v2.2.3^{commit}`, `git cat-file -t v2.2.3` = tag |
| Release source (image build REV) | `c7ce7d3087cb8a2daf82ba941982bd296d488f45` on `fix/p184-dl-closure` | build receipt `identity.rev` |
| Runtime-path change after the REV | none: `git diff --stat c7ce7d3 5dfabe6 -- rust qdl qdl_sdk contracts generated config consumers Cargo.toml Cargo.lock 'Dockerfile*' app` is empty | git, run for this draft |
| Branch head at drafting | `5dfabe6fec1cabebbb2b542798094e462aea216a` (134 commits after `481fd67`) | `git rev-list --count 481fd67..HEAD` |
| Release head carrying the certificate | `<<PENDING:DL_RELEASE_HEAD_SHA>>` (the certificate commit; later evidence-only commits allowed under `release_source_rule`) | |
| Rust image | `qdl-v2-rust:2.2.4-c7ce7d3` `sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e`, revision label = REV | `docker image inspect` |
| Python image | `qdl-v2-python:2.2.4-c7ce7d3` `sha256:c21e164ac4db14af498b73104e344f003fa7117fdae2e465f50df8668cdca252`, revision label = REV | `docker image inspect` |
| SDK | `qdl-sdk 2.4.1`, wheel sha256 `9f79e032604bf79db5ca9070c7f8a99b242e7cf22e1659b395b307dc798c28e7`, `generated_contract_digest` `2ee96b1edab01d3185e4fb973f7e3a0715956f8648938039108eb6156187c116`, two `--network none` builds byte-identical (also `.cdx.json` `6bda0014...`, `.release.json` `273e5326...`) | `sha256sum` of `build-v2.2.4-c7ce7d3/sdk-build-{1,2}/` |
| TS writer (measured with the release) | `market_data_service_p184w` image `sha256:a44349a10e5284efae290ea38062dd23dea0ef015088be6cf041d3f4d0923c73`, label TS `6af62a7ed3d2770ac468d4b5dffe3bd3a0a29e8e`, rolled 2026-10-17 08:43:06Z, RestartCount 0 | `docker inspect` |
| Gate table (frozen 2026-10-09) | TS `upgrade/p184-phase-d-gates.md` sha256 `c67bf69bb921c992964eb8ff8b2e1bb17085a943fdf756d55526b6269641d5c4` | `sha256sum` |
| Frozen budget | `config/v2/p184-stage100-budget.json` sha256 `ff85335b3c8762d9acd8e7230f3c81ee1ec8403aff34cf2fb8f67714521c2541` | `sha256sum` |

### Runtime at drafting (`docker inspect`, 2026-10-17 ~10:50Z; re-read at assembly: `<<PENDING:RUNTIME_RECHECK_AT_ASSEMBLY>>`)

| Role | Image | Revision label | RestartCount | OOM |
| --- | --- | --- | --- | --- |
| query_kn_1, query_kn_2 | python `c21e164a` | REV `c7ce7d3` | 0, 0 | false |
| stream_kn_1, stream_kn_2 | rust `60f83a51` | REV | 0, 0 | false |
| market_projector_1, _2 | rust `60f83a51` | REV | 0, 0 | false |
| ingestor_okx_swap, ingestor_binance_usdm | rust `60f83a51` | REV | 0, 0 | false |
| rust_core, _2, _3 | rc22 `7c848aad` (kept, owner option B5a) | `a4e7b3b` | 0, 0, 0 | false |
| binance_bar_edge | rc14 `08f356a8` (HELD, scoped out) | `a388537` | 0 | false |
| kafka1-3 | `9516fb76` (config change only) | none | 0, 0, 0 | false |
| market_cache, stable_redis | `dfa18828` | none | 0, 0 | false |
| market_data_service_p184w (TS) | `a44349a1` | TS `6af62a7e` | 0 | not read |

8 DL application roles are on the release digests (2 Python, 6 Rust); 3 cores stay on rc22 and 1 bar edge stays on
rc14. That is not "all roles on one revision": the certificate states the split (`runtime_scope_totals`).

## 3. Evidence index (sha256 computed from the files on 2026-10-17)

| Evidence | Path | sha256 |
| --- | --- | --- |
| Build receipt (status **FAIL**, kept as is) | `~/.local/state/qdl-v2/build-v2.2.4-c7ce7d3/receipt.json` | `40d727b28a3a581eb9c5911e150a39ae0dac68d8d0a9afbb5cceee47b4cb4f7f` |
| Build postcheck addendum (resolves the two whole-file expectations) | `.../postcheck-addendum.json` / `.md` | `dad31fe8dc7f768de72e7577951af61d726689d8b32fcf5153046fd9bb190260` / `1506aa4e597f4e2d667f34df4ad9f62ca691c2f16a4b190aefe76983c502f1be` |
| Production roll summary (8 roles, 2026-10-16 17:14-17:53Z) | `~/.local/state/qdl-v2/releases/v2.2.4/rollout/roll-summary-20261016.json` | `eefb041c5ece53c17d28148eb3fa1c2e6be64f84b79ab20e6442f1bd44c0b271` |
| Roll receipts (query rc15 tool; others rc17v2) | `query_kn_1` `500fc392...`, `query_kn_2` `8036c25c...`, `stream_kn_1` `d5ed0dba...`, `stream_kn_2` `bc6a234a...`, `market_projector_1` `dfe23b42...`, `market_projector_2` `35393d2b...`, `ingestor_okx_swap` `5ca5d146...`, `ingestor_binance_usdm` `1732ad71...` | full values in [equivalence.json](equivalence.json) |
| Equivalence record (this directory) | `equivalence.json` | `<<PENDING:EQUIVALENCE_JSON_SHA256_AT_FINAL>>` (file changes until the final stage-100 fields are filled) |
| G12 final credit | `~/.local/state/qdl-v2/p184-g12-v224-final-a44349a1-20261017T084728Z/` | `evidence/verdict.json` `30648759e5766f5e085e1ea3874efe2ab69ceca0475fcca2ab23e3016b67d1be` |
| G12 credit extras | same dir | `FINAL_CREDIT.json` `675c883c...`, `production-parity.json` `e861e762...`, `credit-snapshot.json` `6bb2021ab3e51065bf54697a7f424991fc4125bd29fb4ac0536597be9f4ff2fe`, `evidence/writer-precheck.json` `092406d9...`, `c-observe.json` `f1413c33...`, `c-observe-after.json` `56dd1032...`, `evidence/teardown.json` `b9e9fccf...` |
| Effective Kafka describe at the credit (cleaner 4194304, B x3, C, A') | `credit-snapshot.json` field `effective.sha256` | `d3d77176c3dfca46e7f44cba465d271afe2709a1fc9d97f75e808808fa959094` (taken 2026-10-17T09:16:16Z) |
| Effective Kafka describe after B (2026-10-16 14:28:49Z, older) | `~/.local/state/qdl-v2/p184-kafka-b-20261016/final-check-20261016T142831Z/production-parity.json` | file `74e5066febb19d12c3f3f77cdffcfdce0e6efe9ffbdff7cabec287db3d9682b7`; field `effective.sha256` = `f4836dfc9c9f09a72882bcdb6fcc557b678bfc29738426854f09dd038b78f8c4` (schema v1, no cleaner field; superseded by the v2 describe above) |
| Coordinator load 4,783 ms (kafka3 roll) | `.../roll-kafka3-20261016T142516Z/coordinator-loads.json` | `a7c0a8661482a4c30ebc303c8d9d4992c62e60a9cf317ddc20c3ed78dd26f473` |
| G13 isolated dr08 4/4 | `p184-g13-20261017T091825Z` | summary `3403bba6...`, plan `b74c4c31...`, receipt `53e492cd...`, evidence.jsonl `5f9f70799b1c6f4ec15ef16b139fa9bc8b3a345d3ae19a8d35eaae3e823cd541` |
| G13 isolated dr05-upstream 6/6 (rerun) | `p184-g13-20261017T093001Z` | summary `445281ec...`, plan `3cacd1d0...`, receipt `202f8a2e...`, rows `189de29ed52ce2b9826508423dcc4d307e1a24dc5ba7a6e7abd811e8d8e92681` |
| G13 isolated dr05-upstream run 1 (NOT SCORED, apparatus race) | `p184-g13-20261017T092050Z` | summary `4b283255...`; kept, not credited |
| G13 isolated dr05-quiet 2/2 (amendment-1) | `p184-g13-20261017T092651Z` | summary `c9240569...`, plan `eb45e350...`, receipt `f7ab3525...`, rows `11991a2342a55f54dfa729fa816db00d706e871b666d534aa9694525d8c0e179`, W05 `verify/w05-isolated-final-a44349a1.json` `b5519639fd0c23923322063025c566afcb1775c63adca574df067f3db46dce6f` |
| G13 runtime v5.3 (67/76, STOPPED at dr05-transport; informational, superseded by v5.4) | `p184-g13-20261017T094028Z` | summary `2f6b9ddd84795dfff9ce3801559f95c01ada5d3910bd5297a6ab44dba071aba0`, W05 `verify/w05-final-v53.json` `23eeb0e89b8e1dd0d0f531c80739028a153bd6ee7bbe999900f9ac108a8128da` |
| G13 runtime v5.4 (final) | `<<PENDING:G13_V54_RECEIPT_DIR>>` | summary `<<PENDING:G13_V54_SUMMARY_SHA256>>`, W05 `<<PENDING:G13_V54_W05_SHA256>>` |
| Final stage-100 (on release digests, writer a44349a1) | `<<PENDING:S100_DIR>>` | scorecard `<<PENDING:S100_SCORECARD_SHA256>>`, campaign.json `<<PENDING:S100_CAMPAIGN_SHA256>>` |
| Image sets | `g12-image-set-v224.json` / `g13-roles-v224.json` (this directory) | `4740c995550737e7d30325aaa043f07cf3656014ca5611dd97e482619504ca61` / `31e0716b04f05ee156b0527b8905cf3cb3487501a9a4566f0742b80664342991` |
| TS writer build receipt (PASS) | `~/.local/state/trading-system/p184-n1-opt-20261013/release/6af62a7e/receipt.json` | `8f30560c115e08f8fcc2c2e02838253bcce53ba94ccc45109ff451a7c9250f2e` |
| Release preparation packet | `release-preparation.json` | `7b0e90a52c2525ad93ded1b1e4ee6be1141f0943f5a1f451c625d3751730f8b3` |
| Local CI summary for the release head | `~/.local/state/qdl-v2/p184-local-ci-<sha7>/summary.json` | `<<PENDING:DL_LOCAL_CI_SUMMARY_SHA256>>` |
| Remote CI | PR head run id | `<<PENDING:DL_CI_RUN_ID_PR_HEAD>>` (required jobs: consolidated-source-preflight, contract-tests, sdk-python310, unit-tests, kn-native-integration) |

## 4. Gates

The G1-G14 table with PASS / credit / PENDING status is [SCORECARD.md](SCORECARD.md). Summary at drafting:

- Credited: **G12** (final, writer a44349a1).
- Isolated part credited: **G13** 12/12 isolated cells; the runtime window v5.4 is `<<PENDING:G13_V54_W05_CELLS_CLOSED>>`.
- Not yet measured on the release digests: G1-G11, G14 (`<<PENDING:S100_*>>`).
- Not credited, kept as history: the stage-100 attempt of 2026-10-17 03:20Z on writer 7c71e3af (protocol_met false, G9
  and G4 FAIL; writer changed afterwards).

## 5. Limits to state in the certificate (carry forward unless the numbers change them)

1. Scoped Data Layer certificate: not a Trading System order, accounting, alpha or DR01/DR02/DR03/DR07 certification
   beyond the G13 cells listed. Not HA: every run used one host.
2. `binance_bar_edge` is held on rc14 `08f356a8` and is not on the release revision. Its IDR-087 supervised probe
   ships in the python image only for the roles that run it.
3. rust_core x3 stay on rc22 `7c848aad` (revision `a4e7b3b`). The release image's `qdl-realtime-core` equals rc22
   (`2d82167336a4268a9407d565325f656f2315632b26bdd18bd4527e73dd15e475`), so the cores are certified by binary equality,
   not by a recreate on the release digest.
4. stream gateway: the release binary is not byte-equal to the measured `baa72c96` gateway (build-path strings); it is
   classified DIFFERENT_BUILD_PATH and backed by the DR08 A/B. Projector: equal to rc22 in every loaded ELF section
   (symbol tables and build-id differ).
5. Build receipt status is FAIL. It was resolved by a coordinator, owner-delegated decision recorded in the postcheck
   addendum (2026-10-16), without a rebuild and without editing the receipt. Say so; do not present the build as PASS.
6. Kafka C is converging (`__consumer_offsets-43` measured ~5 s coordinator load at 2026-10-16 14:25Z, estimated ~49 h
   to converge); B applied 2026-10-16 14:14-14:25Z; A' effect arrives over up to 7 d.
7. IDR-108 OPEN: Stream accepts on 8210 before its hot view serves (TEST_ONLY: serving at port-ready +356.6 ms).
8. G12 harness shape not mirrored (recorded, not claimed): one core member at 0.5 CPU instead of three
   (1.0/1.0/0.5), Kafka 0.75 CPU each (production 1.75), latest topic 3 partitions without production `segment.ms`
   3600000 / `segment.bytes` 128 MiB, synthetic TEST_ONLY publisher at 4 Hz BOOK. The G12 integrity layer is
   report-only (NOT_MEASURED), per OF-G12-2.
9. G12 final used the TS WriterClone apparatus pinned at `77a6d671` (`--ts-root trading-system-final-run-77a6d671`,
   coordinator decision H1, deferred follow-up) because the later WriterClone refuses `keep_exited=False`; the writer
   under test was the real a44349a1.
10. The TS writer vendors SDK 2.4.0 (`qdl-sdk 2.4.0 equal` in the build receipt), while this release publishes 2.4.1.
    Re-vendor is a separate TS delta (FINAL_STEPS Q3).
11. TEST_ONLY burst results (ladder, amendment-1, parity rerun) are internal capacity, not production capacity claims.
12. `<<PENDING:LIMIT_G4_G9_FINAL>>` - the final stage-100 outcome for G4 and G9 decides whether they are limits or
    gates; the 03:20Z attempt showed G9 FAIL (QUOTE OKX BTC-USD-SWAP STALE, fixed by IDR-110/110b) and G4 FAIL on two
    rows outside the protocol window.
13. CI: `ci.yml` job `consolidated-source-preflight` still passes `--release v2.2.3` to the recipe preflight
    (`.github/workflows/ci.yml:45`). It passed for v2.2.3; run `scripts/p184_local_ci.py --sha <head>` first and treat
    any failure there as a CI blocker, not a certificate matter.

## 6. TS writer receipt value (verified)

`sha256sum ~/.local/state/trading-system/p184-n1-opt-20261013/release/6af62a7e/receipt.json` = `8f30560c115e08f8fcc2c2e02838253bcce53ba94ccc45109ff451a7c9250f2e` (computed 2026-10-17).

## 7. Publication path (FINAL_STEPS h, owner rule G6: push and merge only on an explicit owner request)

1. `python3 -B scripts/p184_local_ci.py --sha <head>` first.
2. Push `fix/p184-dl-closure` (origin is at `98b421f`, 118 commits behind the local head at drafting). PR into `dev`:
   draft in [drafts/PR_DL_fix-p184-dl-closure_to_dev.md](drafts/PR_DL_fix-p184-dl-closure_to_dev.md).
3. Release PR `dev` -> `main`: [drafts/PR_DL_dev_to_main.md](drafts/PR_DL_dev_to_main.md).
4. Annotated tag `v2.2.4` on the main merge commit. `release.yml` checks: main ancestry, tag format, certificate
   `status == PASS`, `endpoint-report.json` sha256 equal to `endpoint_report_sha256`, `RELEASE_NOTES.md` present, and the
   CI-built SDK wheel sha256 equal to `sdk.wheel_sha256` (`9f79e032...`).
5. Tag message precedent (v2.2.3): "Quant Data Layer v2.2.3: P18.4 phase D closure." plus scope, certificate path and
   SDK wheel prefix.

## 8. Finalisation commands (read-only, run when every PENDING marker is filled)

```
cd /home/bobby/.worktrees/data-layer-p184-closure
grep -rn '<<PENDING' upgrade/evidence/releases/v2.2.4 --include='*.md' --include='*.json' --exclude-dir=drafts   # must print nothing
sha256sum ~/.local/state/trading-system/p184-n1-opt-20261013/release/6af62a7e/receipt.json
sha256sum upgrade/evidence/releases/v2.2.4/equivalence.json upgrade/evidence/releases/v2.2.4/endpoint-report.json
python3 -B -c "import json,sys; [json.load(open(f)) for f in sys.argv[1:]]" upgrade/evidence/releases/v2.2.4/*.json
```

## 9. Rollback (failure only; carry into the certificate `rollback` field)

Per role, one container at a time, with the saved composes in
`~/.local/state/qdl-v2/releases/v2.2.4/rollout/` (sha256 in `equivalence.json`):

| Role | Roll back to | Digest |
| --- | --- | --- |
| query_kn_1, query_kn_2 | rc19 `qdl-v2-python:2.2.3-rc19-4d3476c` | `sha256:b9ca6c36fb0ad003b77e47bb3a777c3e565944d59cb3a2e9bc5dfb68e3448481` |
| stream_kn_1, stream_kn_2 | `baa72c96` (TEST overlay, label `qdl.test.only`) | `sha256:baa72c96d5255cbfc0e332f893c470ff293d88fde4e20ea4c7c94ba5ea09a38b` |
| market_projector_1, _2 | rc14 | `sha256:ce629061a8c180ae7e6cf88e32ebaddcf965ffe3924c88a3f1b942fc1623d4f9` |
| ingestor_okx_swap, ingestor_binance_usdm | rc18 | `sha256:7fba25779f72a3adffc88f195935a316ac94acbfcb362996fafbff89f62c9103` |
| market_data_service_p184w (TS) | IDR-110 `734b08f1` via the saved compose (then R1 `7c71e3af`, B17 `cb5216b8`) | `sha256:734b08f1fefab51e765dbaed2a24080010eb5427dc930ba2e5da7e1aaf354f12` |

Kafka config: cleaner `--add-config log.cleaner.io.max.bytes.per.second=8388608`; markers
`scripts/kafka_md_latest_marker_retention.py --rollback-to 604800000 --apply`; internal topics
`scripts/kafka_internal_topic_compaction.py --rollback --apply --cert-dir <stable cert dir>`; broker rebalance
`scripts/kafka_auto_leader_rebalance_roll.py --rollback <packet> --broker kafkaN --apply`.

Not touched by this release: kafka1-3 images, rust_core x3, binance_bar_edge, market_cache, stable_redis, catalog,
consumer manifests, JWT revisions, realm dirs, offsets, Redis data.

## 10. Open PENDING markers in this file

`DL_RELEASE_HEAD_SHA`, `RUNTIME_RECHECK_AT_ASSEMBLY`, `EQUIVALENCE_JSON_SHA256_AT_FINAL`, `G13_V54_RECEIPT_DIR`,
`G13_V54_SUMMARY_SHA256`, `G13_V54_W05_SHA256`, `G13_V54_W05_CELLS_CLOSED`, `S100_DIR`, `S100_SCORECARD_SHA256`,
`S100_CAMPAIGN_SHA256`, `S100_*`, `DL_LOCAL_CI_SUMMARY_SHA256`, `DL_CI_RUN_ID_PR_HEAD`, `LIMIT_G4_G9_FINAL`.
