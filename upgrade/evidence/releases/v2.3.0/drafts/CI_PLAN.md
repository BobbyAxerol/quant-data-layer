# DL v2.3.0 release CI plan (CLAUDE.md section 7b CI1-CI11)

Status: dry run only. Prepared 2026-11-13; no push and no workflow run.

## Classifier dry run (`scripts/ci_change_scope.py`, read-only, host `python3 -B`)

- Base: the last full-green tree on the release path, `098c94b43cd7f83767f25c9f80190583fe29bc60`.
  - That is the push to dev, run `39380601966` (2026-10-20, attempt 1, every job success).
  - Its tree `1e621911...` equals the tree of v2.2.5 main `c578cba`; the main push run `39382027281` inherited it.
- Head: `release/v2.3.0-final-prep` `495e3b96` (tree `926f91a1...`; rc3 integration tip `3d8ccd7` plus this package and
  the F2 compose/env/config). Earlier dry run: tree `3d25c827...` on rc2 `e724712`, superseded.
- Command: `--base 098c94b4... --head <head> --event pull_request --base-ref dev`.
- Result:

```
full=true python=true rust=true contracts=true base_check=true reuse_run=
reason=full: ci-defining path .github/workflows/ci.yml; ci-defining path
       deploy/compose/qdl-v2-stable-kn.v2.2.6-stage-a.env; ci-defining path
       deploy/compose/qdl-v2-stable-kn.v2.3.0.env; ci-defining path deploy/compose/qdl-v2-stable-kn.yml
changed_files=152
```

- So nothing can be inherited. The diff changes CI-defining files (CI5): `ci.yml` gains the `schema_line.py check`
  step, and the versioned compose and env files change.
- The release therefore needs exactly **one full run on the final release tree** (CI6).

## Jobs (CI11: run or inherit, per job)

Minutes are from the last full run `39380601966`. All jobs run on `ubuntu-latest` (DL is a public repo; hosted
runner, no TS droplet).

| Job | Decision | Source run / reason | Minutes (approx.) |
|---|---|---|---|
| changes | run | classifier | 0.7 |
| repository-scan | run | always (secret scan) | 0.3 |
| consolidated-source-preflight | run | always | 0.2 |
| contract-tests | run | `contracts/**` + `generated/**` changed (proto Bar field 19, schema line check); `base_check` | 0.2 |
| rust-workspace | run | `rust/**` changed (qdl-core derived_bar, realtime-core, stream-gateway, kafka ingestor, contracts) | 3.0 |
| unit-tests (1..4) | run | python changed | 7.7 + 11.6 + 11.5 + 10.7 |
| sdk-python310 | run | full flag | 0.2 |
| release-gates | run | full flag | 4.3 |
| kn-native-integration | run | full flag (rust + compose) | 6.0 |
| ci-result | run | verify needs | 0.1 |

- Runner time: about **56.5 runner-minutes**; wall time about 13 min (the 4 shards run in parallel).
- CI4 caches (cargo/pip) apply as configured.
- Expected: no job inherits, because the CI-defining paths changed since the last full green.

## The single full run (CI6) and where it lands

1. Finish every fill in `RELEASE_FILL_LIST.md` first: window receipts, R-cert 2 verdict, `certificate.json`,
   `endpoint-report.json`, final `RELEASE_NOTES.md`, ledger rows, compose env file.
   - A later docs commit changes the tree.
   - Its scoped docs-only run is never a release receipt (CI2/CI6), so it would force a second full run.
2. Open **one** PR `release/v2.3.0-final-prep` -> `dev`, with no `ci:full` label (the classifier already forces
   full) and no `workflow_dispatch`.
   - The pull_request run is **the release full run**, on the merge-ref tree T.
   - Push to the branch only once (CI3/CI9). Do not push while that run is in progress.
3. Merge locally (CLAUDE.md G7): `git merge --no-ff <head-sha>` into dev in a clean worktree, check `%an`, then a
   plain `git push origin dev`.
   - If dev has not moved, the merge commit's tree equals T.
   - The push-to-dev run then inherits (tree-identity reuse, `reuse_run=<release run id>`, `ci-result` records
     "inherited run <id>"). No second suite.
4. PR `dev` -> `main`. The classifier sets full (release path), but tree reuse still applies when the merge-ref tree
   equals T.
   - That holds when main's tree is contained in dev, as for v2.2.5: main `c578cba` tree == dev `098c94b` tree.
   - The PR-base `buf breaking` check still runs (base-dependent, CI5): about 0.2 min.
   - The main push inherits the same way.
5. Tag `v2.3.0` on main. This triggers `release.yml` once (about 5 min), which checks `certificate.json` PASS, the
   `endpoint-report.json` sha, the presence of `RELEASE_NOTES.md`, and the SDK artifact build.

- Total expected: one full run (about 56.5 runner-min), about 1 min of inherited classifier/ci-result jobs per later
  event, and one `release.yml` run.
- If any job flakes, rerun only that job (CI7); two red runs with the same cause means stop and fix.
