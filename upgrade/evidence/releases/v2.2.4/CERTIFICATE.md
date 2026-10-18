# Quant Data Layer v2.2.4 - scoped release certificate

Status: **FAIL - NOT CERTIFIED, NOT PUBLISHED. Released as a package with stated limits by owner decision; tag blocked
by the release workflow (section 7).** Assembled 2026-10-18 ~13:40Z. Every sha256 below was recomputed with `sha256sum`
and every image digest read with `docker inspect` / `docker image inspect` at assembly. The machine form is
[certificate.json](certificate.json) (schema `qdl.scoped-release-certificate.v2`, same shape as
[v2.2.3](../v2.2.3/certificate.json)); its status is `FAIL`.

Companion files: [SCORECARD.md](SCORECARD.md) / [scorecard.json](scorecard.json), [RELEASE_NOTES.md](RELEASE_NOTES.md),
[endpoint-report.json](endpoint-report.json), [equivalence.json](equivalence.json).

## 1. Certificate decision rule (fixed before the final numbers existed, unchanged)

- `status` is `PASS` only when all of these hold. `release.yml` refuses any other status.
  1. No gate is FAIL in the scorecard. A NOT_MEASURED gate is never upgraded.
  2. Final stage-100 on the release digests has `protocol_met: true`.
  3. G12 is the credited final run. G13 is 76/76 on the final digests (or each open cell is an owner-recorded limit).
  4. Required CI jobs are green on the release PR head.
- Anything else: do not create `certificate.json` with status PASS. No frozen threshold is widened after measuring.

Result at assembly: (2) holds, (3) holds, **(1) does not hold: G2 and G4 are FAIL.** (4) is an external gate that runs
after the push. So `status` = **FAIL**.

Owner decision (2026-10-18 ~13:40Z, AskUserQuestion "Release kèm limit + gói tối ưu sau (Recommended)"): package
v2.2.4 now with G2 and G4 stated as FAIL rows with RCA, NOT as a full PASS; open IDRs for the optimisation package
(Kafka cleaner I/O C1, broker CPU quota, core commit batching, Query MARK read path), each done one variable at a time
with A/B in a later release; push, open PRs, clean up. The decision changes no verdict.

## 2. Identity

| Item | Value | Source |
| --- | --- | --- |
| Previous published release | `v2.2.3` (main merge `28e605ec6a359d462cd3c425a78519f9f8e70f63`, source `481fd67`) | git |
| Release head carrying this certificate | the packaging commit on `fix/p184-dl-closure` after `0c55957` (SHA reported in the PR) | git |
| Runtime source after the newest image revisions | rust: none since `83bef59`; python: apparatus only since `31a5155` (`qdl/certification/reference_l2_acceptance.py`, three frozen `config/v2/p184-stage100-*` amendment files) | `git diff` (RELEASE_NOTES.md "Source And Images") |
| Rust image (ingestors x2) | `qdl-v2-rust:2.2.4-83bef59` `sha256:a9679defdd970aaa961736d414cf8c443c2ead2f26ebf2e68eb481dda6725784`, label `83bef596...` | `docker image inspect` |
| Rust image (stream x2, projector x2) | `qdl-v2-rust:2.2.4-c7ce7d3` `sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e`, label `c7ce7d30...`; its stream gateway and projector binaries equal the 83bef59 build | `docker image inspect`, 83bef59 build receipt verdicts |
| Python image (query x2) | `qdl-v2-python:2.2.4-31a5155` `sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff`, label `31a51552...` | `docker image inspect` |
| SDK | `qdl-sdk 2.4.1`, wheel `9f79e032604bf79db5ca9070c7f8a99b242e7cf22e1659b395b307dc798c28e7` (two `--network none` builds at `c7ce7d3`, byte-identical; `.cdx.json` `6bda0014b9ee304b679e44079976bc5d576a524571b5a771807e3aa0cdd2d442`, `.release.json` `273e532665f634b3737d0fa014a3ced15efc468e9dd380ae034d8a2ee451be69`); SDK sources unchanged since | `sha256sum` of `build-v2.2.4-c7ce7d3/sdk-build-{1,2}/` |
| TS writer measured with the release | `sha256:2deb90b0cc52eae7610fa44692f90df077e9db100ca259e38fd652a2dc1c5dc5` (TS `30f50a91`), receipt `release/30f50a91/receipt.json` `7040ba9fc68f17641ca69ffb9e52fa4240c9e9e4c956f06c2a3568ea97f64a1c` PASS | `docker inspect`, `sha256sum` |
| Gate table (frozen 2026-10-09) | TS `upgrade/p184-phase-d-gates.md` `c67bf69bb921c992964eb8ff8b2e1bb17085a943fdf756d55526b6269641d5c4` | `sha256sum` |
| Frozen budget / addendum | `ff85335b3c8762d9acd8e7230f3c81ee1ec8403aff34cf2fb8f67714521c2541` / `4ed59dca0212566d3ff03c5a4c23eed3d252d0150844b9a4fd6fa543333d78b5` | `sha256sum` |
| Amendments (frozen before the run) | G4-A1 `1c8a276f6cc6c03cdf768cdc9d31b65902bd849a1f3b7df56cda07ea47e6efa6`, G4-A2 `99961c41e08993944d158ae812ca61514271862a2d03e011f5d01ce71a0f9997`, A3 `cc5fc7dcf07e30b1b52cbc51b78c4a5485a63be84d5b814dcf89452007e57326` | `sha256sum` |

### Runtime at assembly (`docker inspect`, 2026-10-18 ~13:2xZ)

| Role | Image | Revision label | RestartCount | OOM |
| --- | --- | --- | --- | --- |
| query_kn_1, query_kn_2 | python `3e9e4d3a` | `31a5155` | 0, 0 | false |
| ingestor_okx_swap, ingestor_binance_usdm | rust `a9679def` | `83bef59` | 0, 0 | false |
| stream_kn_1, stream_kn_2 | rust `60f83a51` | `c7ce7d3` | 0, 0 | false |
| market_projector_1, _2 | rust `60f83a51` | `c7ce7d3` | 0, 0 | false |
| rust_core, _2, _3 | rc22 `7c848aad` (kept, owner option B5a) | `a4e7b3b` | 0, 0, 0 | false |
| binance_bar_edge | rc14 `08f356a8` (held, scoped out) | `a388537` | 0 | false |
| kafka1-3 | `9516fb76` (config change only) | none | 0, 0, 0 | false |
| market_cache, stable_redis | `dfa18828` | none | 0, 0 | false |
| market_data_service_p184w (TS) | `2deb90b0` (started 13:05:46Z by the G11 restart, same image) | TS `30f50a91` | 0 | false |

8 DL application roles run v2.2.4 images (2 python on `31a5155`, 2 ingestors on `83bef59`, 4 stream/projector on
`c7ce7d3`); 3 cores stay on rc22 and the bar edge on rc14. That is not "all roles on one revision".

## 3. Evidence index

| Evidence | Path | sha256 |
| --- | --- | --- |
| Build receipt rust 83bef59 (PASS) | `~/.local/state/qdl-v2/build-v2.2.4-83bef59/receipt.json` | `7e630925462e5cac3de7423c36b964834eeacba9f8c3e94cb9bfbd49d40e5671` |
| Build receipt python 31a5155 (PASS) + equivalence vs c21e164a | `~/.local/state/qdl-v2/build-v2.2.4-31a5155/{receipt,equivalence-vs-c21e164a}.json` | `e1a9bcc57847a83ea1425dfc66ed3598322a46e46948375bb47f97bbe1c93e38` / `2ec87f343e30152d117b341abecd68ef27382a5e2a947f37fdf7252230e20424` |
| Build receipt rust 6d61710 (PASS, superseded by 83bef59, ingestor rollback image) | `~/.local/state/qdl-v2/build-v2.2.4-6d61710/receipt.json` | `6f004ce7ba11d66d05124067d1ce0e09a8d0753581e4f6963690d7d10f4db17f` |
| Build receipt c7ce7d3 (status **FAIL**, kept) + addendum | `~/.local/state/qdl-v2/build-v2.2.4-c7ce7d3/{receipt,postcheck-addendum}.json` | `40d727b28a3a581eb9c5911e150a39ae0dac68d8d0a9afbb5cceee47b4cb4f7f` / `dad31fe8dc7f768de72e7577951af61d726689d8b32fcf5153046fd9bb190260` |
| First roll (c7ce7d3 images, 8 roles, 2026-10-16) | `~/.local/state/qdl-v2/releases/v2.2.4/rollout/roll-summary-20261016.json` | `eefb041c5ece53c17d28148eb3fa1c2e6be64f84b79ab20e6442f1bd44c0b271` |
| Final roll (ingestors 83bef59, query 31a5155, 2026-10-18 03:35-04:09Z) | `~/.local/state/qdl-v2/releases/v2.2.4/rollout-idr112b-idr114/driver.log` / `SHA256SUMS` | `6b6173e563812703b2a54ad3bdb76ea69bedb3538790f1baf7789696381d22f2` / `8b034e7cee2e4879bd01069f302955e6d3aa08d847da85d8dc182867866327a4` |
| Per-role roll receipts and rollback composes | see [equivalence.json](equivalence.json) `roles[].roll` | (per file) |
| Formatting-only change to qdl-stream-gateway (PR #33 CI fix, commit f6ce816): equivalence to the deployed c7ce7d3 binaries (projector EQUAL; gateway loaded code/rodata/relocations equal, only panic-location line numbers differ) | [fmt-equivalence-addendum.md](fmt-equivalence-addendum.md) | `d17d04240edd96f955c9bba1bfb7ddbf7c1abdf414bb126040b5dc8c1beb4f9e` |
| G12 final credit (final image set) | `~/.local/state/qdl-v2/p184-g12-v224-final-2deb90b0-20261018T042415Z/` | `evidence/verdict.json` `170af83797df3309d4c9778042ee5b4e8f4c7ec5fca89fede22d9d1d0eeadc27`, `FINAL_CREDIT.json` `c6bfaedd1edefdcc76de1095e012b87fa85ce57c79fb43f645e2d272171dc8fc` |
| G13 76/76 (writer 2deb90b0) | runtime `p184-g13-20261018T051237Z`, dr08 `045558Z`, dr05-quiet `071750Z`, dr05-upstream `085316Z` | summaries `147a90aa...`, `17e731d6...`, `4f5aecbb...`, `b67dff66...` (full values in SCORECARD.md); W05 `5490c21e66bf33bae326f2ea2051677f70cf95372b13b823d4c2912835da9895` |
| G13 packet v5.5 | `~/.local/state/trading-system/p184-20261005/runtime/p1/packet-g13-v5.5-20261018.md` | `507f9096ba8fba76d97441484d4c0d2146a9388aed298a1f01af47d3ca7320fc` |
| Final stage-100 (counted) | `~/.local/state/qdl-v2/p184-final-campaign-20261018T123222Z/` | scorecard `b9e76a83795f1af8f64b802b22ff1f410087a8f87ee4ef5d23b26a1d39cb4964`, campaign.json `2f96f55347dfd69e090436416364685f0479c3801c466ff5d3418af99ed8b7fc`, stage100/receipt.json `3fd22d861ae434c7f7e0e29ce21d3430b0982f376a50f06c8a0ee1a8b8f64ea0` |
| Image sets | `g12-image-set-v224.json` / `g13-roles-v224.json` (this directory) | `6303cdcb57eaefa16cdd33de2d010ac05e5944ee887ca0b9938e8f79afb4e2f9` / `984c4566223b196f05abe8b76c88de4127c91be0caebc33168f79457b9a471a2` |
| Remote CI | PR head of `fix/p184-dl-closure` -> `dev` | external gate, recorded in the PR (not in this file) |

## 4. Gates

[SCORECARD.md](SCORECARD.md): PASS G1 G3 G5 G6 G7 G8 G9 G10 G11 G14 (counted stage-100), PASS G12 (credited), PASS G13
(76/76); **FAIL G2** (MARK_INDEX_PRICE BINANCE max 1082.9 ms > 1000 ms) and **FAIL G4** (OKX_INVERSE book_delta /
book_snapshot / quote maxima over the addendum limits, internal segments). NOT_MEASURED 0, DEFERRED 0.

## 5. Limits stated by this certificate

1. **G2 FAIL** and **G4 FAIL** as above, with the RCA in SCORECARD.md. The optimisation package is IDR-116 (Kafka cleaner
   I/O, C1), IDR-117 (broker CPU quota), IDR-118 (core commit batching), IDR-119 (Query MARK read path); IDR-115 (OKX
   BOOK lane recycle with re-snapshot) is a separate product item.
2. Scoped Data Layer certificate: not a Trading System order, accounting or alpha certification. Not HA: one host.
3. `binance_bar_edge` is held on rc14 `08f356a8` (not a v2.2.4 revision).
4. rust_core x3 stay on rc22 `7c848aad`; certified by binary equality of `qdl-realtime-core`
   (`2d82167336a4268a9407d565325f656f2315632b26bdd18bd4527e73dd15e475`), not by a recreate.
5. Stream gateway: DIFFERENT_BUILD_PATH vs the measured `baa72c96`, backed by the DR08 A/B; projector equal in loaded
   sections to rc22. The c7ce7d3 build receipt status is FAIL, resolved by the owner-delegated addendum (not rebuilt).
6. Kafka C converging (`__consumer_offsets-43` coordinator load ~5 s on 2026-10-16 14:25Z); A' effect over up to 7 d.
7. IDR-108 OPEN: Stream accepts on 8210 before its hot view serves.
8. G12 harness shape not production shape; integrity layer report-only; WriterClone apparatus pin `77a6d671` (H1,
   IDR-120).
9. G13 cells are shadow-certified; the W05 output carries no run identity (byte-identical to the v5.4 output).
10. G14: DL mean CPU sum 5.63 vCPU at stage 100 (owner budget 5 vCPU).
11. The TS writer vendors SDK 2.4.0 while this release publishes 2.4.1.
12. TEST_ONLY burst results are internal capacity, not production capacity claims.
13. OKX FUNDING_RATE `max_freshness_ms` equals the 8 h cadence; the A1 apparatus parity fix applies Query's +60 s
    allowance in the verifier; the contract option is IDR-125.

## 6. Rollback

[RELEASE_NOTES.md](RELEASE_NOTES.md) "Rollback" (query -> `c21e164a`; ingestors -> `5e261809` then `60f83a51`;
writer -> `a44349a1`; Kafka config commands). Not touched by this release: kafka1-3 images, rust_core x3,
binance_bar_edge, market_cache, stable_redis, catalog, consumer manifests, JWT revisions, realm dirs, offsets, Redis
data.

## 7. Publication: the release workflow constraint (STOP before tagging)

Exact constraint, `.github/workflows/release.yml` (job `publish`, step "Verify stable ancestry and certificate"):

```
if certificate['status'] != 'PASS':
    raise SystemExit('Release certificate is not PASS')
```

The workflow runs on a pushed tag `v2.*`; every certificate in `upgrade/evidence/releases/` so far (v2.0.12-v2.2.3) is
PASS and no other status is defined by any schema in the repo (the only consumer of `status` is that line). With
status FAIL, a `v2.2.4` tag would fail this step and publish nothing. This package does **not** fake PASS and does
**not** tag. Minimal honest options (owner decision; none taken here):

- **A. Publish no v2.2.4 GitHub release now.** Merge the branch into `dev` (and `main`) as source + evidence, keep the
  runtime as deployed, and cut the next certified release (v2.2.5) after the optimisation package passes G2/G4.
  No workflow change.
- **B. Add a documented status to the workflow.** A reviewed change to `release.yml` that also accepts, for example,
  `status == "LIMITED"` only when `certificate['failed_gates']` is non-empty, every failed gate has an RCA reference and
  an open IDR, and the GitHub release is marked pre-release with the limits in its notes. This is a policy change to
  the release gate itself and needs explicit owner approval; then `certificate.json` would be re-issued with that status.
- **C. Tag without the workflow** (manual GitHub release). Not recommended: it bypasses the repository's own release
  gate.

Recommended: **A** (or B if the owner wants a published artifact for v2.2.4 now).
