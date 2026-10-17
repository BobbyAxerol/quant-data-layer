# DL PR draft 1/2: `fix/p184-dl-closure` -> `dev` (DRAFT, not opened, nothing pushed)

Status: draft written 2026-10-17. Opening the PR needs a push of `fix/p184-dl-closure` (origin is at `98b421f`;
`git rev-list --count origin/fix/p184-dl-closure..HEAD` was 118 at drafting) and the owner's explicit request (CLAUDE.md
G6). No `gh` binary exists on this host (`which gh` empty), so the PR is opened from the GitHub UI or another client.

Governance in this repo (verified): there is **no** `scripts/validate_pr_governance.py` and no PR-governance workflow in
the DL repo (`ls scripts | grep -i govern` is empty; workflows are `ci.yml` and `release.yml` only). The rules that
exist are `CONTRIBUTING.md` (branch flow: feature branch -> `dev` PR -> release PR `dev` -> `main`; never commit to
`main`) and `.github/pull_request_template.md` (Summary, Type, Checks, Test Evidence). The title below also satisfies the
stricter Trading System `TITLE_RE`, so it is safe if the same validator is added later. Required CI jobs
(`ci.yml`): consolidated-source-preflight, contract-tests, sdk-python310, unit-tests, kn-native-integration.

Precedent (v2.2.3): PR #31 "Merge P18.4 phase D: Data Layer v2.2.3 release candidate" (merge commit `5c51a42`) into
`dev`, then PR #32 "Release v2.2.3: P18.4 phase D Data Layer closure" (merge `28e605e`) into `main`.

## Title

`fix(p184): Data Layer v2.2.4 closure release candidate`

(Matches the TS `TITLE_RE`; checked with `re.fullmatch` against `scripts/validate_pr_governance.py` of the TS repo.)

## Body

```
## Summary

Release candidate for Quant Data Layer v2.2.4 (P18.4 phase D closure on the Kafka-native stack). 134 commits since
v2.2.3 (481fd67) on fix/p184-dl-closure; release source c7ce7d3087cb8a2daf82ba941982bd296d488f45.

Runtime changes (all additive on the public interfaces; endpoint versions unchanged):
- rust_core: pipelined transaction commit (IDR-094), stop intent / epoch abort (K1/K3), rewind fence (RB1), native Kafka
  error classification (DL-R1), consumer session 10 s / heartbeat 3 s, batch 1024.
- Stream gateway: bounded replay batching, stalled-peer isolation, live-budget reserve, exact floor, gateway clocks, and
  the opt-in replay-budget status (IDR-096, SubscribeRequest field 8).
- Projector: bounded retry of transient poll errors (IDR-107).
- Query: BAR warmup fast render, typed CANONICAL_LINEAGE_MISMATCH (IDR-099), Binance OI / statistics / funding coverage
  (IDR-090/091/092).
- Kafka tooling and the md.latest.v2 no-tombstone contract (A'); SDK qdl-sdk 2.4.1.
- Apparatus (scripts/tests only): G4 observer v5, G9/G10/G11 oracles, G12 oracle v3, DR05/DR08/G13 harness, release
  image recipe.

Evidence and drafts: upgrade/evidence/releases/v2.2.4/ (RELEASE_NOTES.md, CERTIFICATE.md, SCORECARD.md,
equivalence.json). Release images: qdl-v2-rust:2.2.4-c7ce7d3 sha256:60f83a51... and qdl-v2-python:2.2.4-c7ce7d3
sha256:c21e164a...; SDK wheel sha256 9f79e032604bf79db5ca9070c7f8a99b242e7cf22e1659b395b307dc798c28e7.

State at opening: <<PENDING:G13_V54_AND_STAGE100_STATE>>. This PR must not be merged as "certified" until the
certificate.json status is PASS in this branch.

## Type

- [x] Data contract / API (additive: SubscribeRequest field 8, StreamRecord clocks, SDK 2.4.1)
- [x] Streaming / Redis
- [x] Diagnostics / monitoring
- [x] Docs / repo hygiene

## Checks

- [x] I did not commit directly to `main`.
- [x] Docs are updated for public behavior changes (RELEASE_NOTES.md, SDK CHANGELOG).
- [x] Tests are added or updated.
- [ ] Docker tests pass, or a blocker is documented: <<PENDING:DL_LOCAL_CI_SUMMARY_SHA256>> (scripts/p184_local_ci.py --sha <head>)
- [x] No secrets, generated logs, parquet data, or local caches are included.

## Test Evidence

- Required CI on the PR head: <<PENDING:DL_CI_RUN_ID_PR_HEAD>> (consolidated-source-preflight, contract-tests,
  sdk-python310, unit-tests, kn-native-integration).
- Local CI: <<PENDING:DL_LOCAL_CI_SUMMARY_SHA256>>.
- Release python image unit runs (--network none): 376 OK / 1 skipped (dr05 quiet, g13, dr05, g12 modules, 2026-10-17),
  422 OK / 5 skipped (g12, g13, lock, dr08, dr05 verdict/quiet/reset/dl_fault); SDK release tests 15 OK.
- G12 final credit PASS: ~/.local/state/qdl-v2/p184-g12-v224-final-a44349a1-20261017T084728Z (verdict.json
  sha256 30648759e5766f5e085e1ea3874efe2ab69ceca0475fcca2ab23e3016b67d1be).
- G13 isolated 12/12; runtime window: <<PENDING:G13_V54_W05_CELLS_CLOSED>>/76 (SCORECARD.md).
- Final stage-100: <<PENDING:S100_DIR>> protocol_met <<PENDING:S100_PROTOCOL_MET>>.

Risk and rollback: roles were already rolled to the release digests on 2026-10-16 (this PR changes source and
evidence, not the running stack). Per-role rollback digests and the saved composes are in RELEASE_NOTES.md.
Not touched: kafka images, rust_core x3, binance_bar_edge, market_cache, stable_redis, catalog, consumer manifests.
```

## Reviewer notes (not part of the body)

- `origin/dev` is one merge commit ahead of the branch point (`5c51a42`, PR #31 merge of `c9f53d1`); the trees are
  identical to `28e605e` on `main` (`git diff --stat 28e605e origin/dev` empty), so no conflict is expected.
- `ci.yml:45` pins `--release v2.2.3` in the recipe preflight; it passed for v2.2.3. If it fails here, fix the CI file
  in a separate commit before merge; it does not affect the certificate.
- Fill every `<<PENDING:...>>` or delete the line before opening; none may remain in the PR text.
