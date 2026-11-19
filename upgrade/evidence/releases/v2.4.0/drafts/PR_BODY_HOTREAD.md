## Summary

DL v2.4.0 window 1 RCA fix (`fix/dl-v240-hot-read-mismatch-fallback` -> `dev`). Window 1 (2026-11-19) and stage C S6
(2026-11-18) degraded the TS writer for the same reason: a Stream replica on another catalog revision refused
Query's private hot read (`HOT_AUTHORITY_MISMATCH`, gateway `hot_view.rs:115-119`), and Query did not try the other
replica, so quiet EXECUTION QUOTEs became `DATA_NOT_READY: HOT_QUIET_PRIMARY_UNVERIFIED`.
- `qdl/runtime/kn_hot_view.py`: `HOT_AUTHORITY_MISMATCH` / `HOT_AUTHORITY_CHANGED` (FAILED_PRECONDITION) fall
  through to the next replica; `HOT_AUTHORITY_OR_PROTOCOL_REFUSED` only when every replica refuses; a transient
  refusal elsewhere keeps its retryable reason. Integrity, auth, expiry, data-loss and invalid-argument refusals are
  unchanged. Counter `authority_fallbacks` + one bounded `qdl_hot_read_authority_fallback` line per key / 10 s.
  The gateway (each replica's exact match) is unchanged.
- Docs: journal + guide delta (RCA, stage C S6 re-attribution), ROLLOUT_PLAN section 8 (corrected order: stream_kn_2,
  Query x2, stream_kn_1; gates G-stream/G-query, G-hot, G-query-refusals, G-writer), the read-only window gate
  script and hot-read probe under `drafts/window/`, R0 build/security records for python `2.4.0-acf42d7`.

Interfaces: none changed (internal Query client behaviour only; no proto, manifest, SDK or HTTP change).

## Validation

- Local: `tests/test_kn_hot_view.py` +5 tests (mismatch then OK; all mismatch refused; mismatch + transient keeps
  the reason; auth/other refusals never fall through; bounded log). Impacted set (9 modules: hot view, hot book
  invalidation, mark hot memo, query backend, shared reads, cursor topic id, binance history overlap, view event
  identity, phase10 warmup) in `qdl-v2-python:2.4.0-c17472c`, host slot, `--cpus 1 --cpu-shares 256`, tree
  read-only: Ran 212, OK (skipped 33).
- Image: `qdl-v2-python:2.4.0-acf42d7` = `sha256:18ada571c72856cefc2844c8cb2c3809c3e30ac77927bc4c73160e7b2787cd18`
  (recipe receipt PASS; vs `9cffe89b` 3 files differ, pip freeze equal); trivy once: 0 fixable CRITICAL/HIGH. Rust
  unchanged (`abafab2c`).
- Read-only on the live stack: the gate script's G-hot / G-query-refusals / G-writer PASS on the current state.

## CI plan (CI2 / CI11)

`scripts/ci_change_scope.py --base 03d0aca --head <head> --event pull_request --base-ref dev`: `full: false`,
reason `scoped: python`, 11 files (qdl 1, tests 1, journal 1, guide 1, upgrade evidence 7), no rust, no CI-defining
path, `base_check: true`.

| Job | Runs / inherited | Source (for inherited) | Est. min |
|---|---|---|---|
| changes, ci-result | runs | - | 1.2 |
| consolidated-source-preflight, repository-scan | runs | - | 0.6 |
| contract-tests (`base_check`) | runs | - | 0.3 |
| sdk-python310 | runs | - | 0.4 |
| unit-tests (4 shards) | runs | - | about 48 |
| release-gates | runs | - | 4.1 |
| rust-workspace | inherited | dev push run 44446296647 (head 03d0aca, job success) | 0 (no `rust/`, `Cargo.*`, `generated/rust` change) |
| kn-native-integration | inherited | same run (job success) | 0 (same reason) |

About 55 job-minutes, about 15 minutes wall-clock, GitHub-hosted `ubuntu-latest` (public repository). One push.
Note: dev run 44446296647 is red only on `unit-tests (1)`: `test_v230_rcert2_shadow.ComparatorTests.
test_compare_once_records_and_summarizes` (`4 != 3` on the second pass), a pre-existing wall-clock test unrelated to
this change (this PR's unit-tests re-run it; CI7: rerun only that job if it recurs, then open a fix slice).

## Risk And Rollback

Code: a Query only gains a second attempt on a replica-scoped authority refusal; the second replica must still match
exactly, and the reply still goes through Query's monotonic/quality checks. Worst case is one extra bounded hot-read
call within the same deadline. Runtime rollout: `drafts/ROLLOUT_PLAN.md` section 8 (separate coordinator windows);
rollback per role to the live images and composes recorded there (Query `26c3b051`, Stream `58bdd282`).
