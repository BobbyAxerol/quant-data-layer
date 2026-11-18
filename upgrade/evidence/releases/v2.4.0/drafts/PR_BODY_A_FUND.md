## Summary

DL v2.4.0 A-FUND (`feat/dl-v240-ts-funding-entitlement` -> `dev`), for TS Paper funding accrual:
- FUNDING_RATE reference reads for the TS Paper writer on every perpetual it reads MARK_INDEX_PRICE for (OKX SWAP 97,
  Binance USDM 5), ALPHA grade (only MARK_INDEX_PRICE is execution-grade reference data).
- Historical 1m MARK/INDEX candles on the existing MARK_INDEX_PRICE reference product: an optional bounded window at a
  non-execution grade (`interval` 1m, `mark_index_kind` MARK or INDEX, whole minutes, `limit` 1..100), OKX only,
  e.g. the mark price at a past funding time; singleflight + 6 h TTL for closed candles. SDK 2.9.0 (additive).
- TS manifest r14 -> r15 (declaration r9, rendered with `scripts/dl_declaration.py`; +102 FUNDING_RATE, +97 MARK 1m
  rows; 5 Binance MARK-history rows typed blocked), release routing r33 -> r34, gate 15/15 PASS.
- Release recipe: AGENTS 34b correctness class already on dev; `--cpu-shares 256` on compile and probe containers.

Interfaces: additive only. No new product, feed, proto or permission (`buf breaking`: no proto change). The snapshot
shape and execution-grade MARK_INDEX_PRICE are byte-unchanged; a 2.8 SDK request serializes unchanged.

## Validation

Local (journal entries "DL v2.4.0 A-FUND ..."): `qdl-v2-python` image, host slot runner, `--cpus 1 --cpu-shares 256`,
tree read-only: 101 impacted modules, Ran 1650, 1 error fixed (TTL test double), rerun 43 OK;
`tests.test_build_qdl_release_images` 46 OK on the host. Release images from c17472c: rust `abafab2c…`
(binaries byte-identical to 3d4d84b), python `9cffe89b…`; trivy 0 fixable CRITICAL/HIGH on both.
Image source vs PR head: `git diff --name-only c17472c 80e0ea4` = the journal and 4 `drafts/*.md` files only, so the
image source tree equals the head tree minus evidence/docs.

## CI plan (CI2 / CI11)

`scripts/ci_change_scope.py --base 7199901 --head 80e0ea4 --event pull_request --base-ref dev`: `full: false`,
reason `scoped: python`, 31 files (qdl 7, qdl_sdk 4, scripts 3, tests 4, upgrade evidence 12, journal 1), no rust,
no CI-defining path, `base_check: true`.

| Job | Runs / inherited | Source (for inherited) | Est. min (from run 44325107773) |
|---|---|---|---|
| changes, ci-result | runs | - | 1.1 + 0.1 |
| consolidated-source-preflight, repository-scan | runs | - | 0.2 + 0.4 |
| contract-tests (`base_check`) | runs | - | 0.2 |
| sdk-python310 | runs | - | 0.3 |
| unit-tests (4 shards) | runs | - | 11.4 + 12.7 + 13.8 + 10.4 |
| release-gates | runs | - | 3.6 |
| rust-workspace | inherited | dev push run 44325107773 (head 7199901, success) | 0 (no `rust/`, `Cargo.*`, `generated/rust` change) |
| kn-native-integration | inherited | same run | 0 (same reason) |

About 54 job-minutes, about 15 minutes wall-clock, GitHub-hosted `ubuntu-latest` (DL is a public repository: CI9
minutes are not billed against the TS budget). One push; no `ci:full` label or dispatch.

## Risk And Rollback

Runtime (separate owner-approved window, `drafts/ROLLOUT_PLAN.md` step 9): realm file r15 + routing r34 + bundle
successor, then query_kn x2 / stream_kn x2 on the c17472c images, the TS JWT to r15 last. Rollback per step (previous
realm dir and routing r33, previous images, TS JWT r14; r14 tokens stay valid under r15).
