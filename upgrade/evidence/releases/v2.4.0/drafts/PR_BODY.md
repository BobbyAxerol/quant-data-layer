## Summary

DL v2.4.0 (`release/v2.4.0` -> `dev`): the v2.3.2 content (SessionAlive, DELIVERY_ON_CHANGE, R0 recipe) plus
S1-S9: verified-token cache; advisory token revision (readers accept 1..manifest revision, manifests byte-unchanged);
quota charged only on admission with per-replica token buckets (TS full rate, session share, optional Redis); direct
MARK/INDEX view call; shared latest view with singleflight; session-fair warmup and hot lanes with warmup
singleflight and waits below client timeouts; role claims ignored on the data plane; SDK 2.8.0 (`session_id`,
warmup timeout); shared100 acceptance profile + A6 (prepared, not run).

Interfaces: additive only. No endpoint, schema or proto change (`buf breaking` expected 0). SDK 2.x clients and the
TS writer are unchanged on the wire.

## Validation

Local, CI-shaped (journal entry "DL v2.4.0 S1-S9"):
- Python impacted set in `qdl-v2-python:2.3.1-aa75218`: 1470 run, fixes, reruns 191 / 262 / 43 OK.
- Rust `qdl-contracts` + `qdl-stream-gateway`: fmt 0, clippy -D warnings 0, test 187 passed / 0 failed / 5 ignored.

## CI plan (CI2 / CI11)

Classifier (`scripts/ci_change_scope.py --base 7a9c00b --head <HEAD>`, run locally): `full: true`, reason
"ci-defining path deploy/compose/qdl-v2-stable-kn.yml" (v2.3.2 compose change, CI5); 82 files. So every job runs
once; nothing is inherited:

| Job | Runs | Est. minutes |
|---|---|---|
| changes, ci-result | yes | 1 + 1 |
| consolidated-source-preflight, repository-scan | yes | 4 + 2 |
| contract-tests (incl. `buf breaking` vs base) | yes | 4 |
| rust-workspace (fmt, clippy, test, cargo-deny) | yes | 15 |
| sdk-python310 | yes | 3 |
| unit-tests (4 shards) | yes | 4 x 8 |
| release-gates | yes | 10 |
| kn-native-integration | yes | 10 |

About 80 job-minutes, about 30 minutes wall-clock. DL is a public repository (CI9: minutes not billed against the TS
budget). One push; no `ci:full` label or dispatch needed (the classifier already selects full). A later dev -> main
on the identical tree inherits this run (CI1).

## Risk And Rollback

Runtime blast radius when rolled out (separate, owner-approved window, `upgrade/evidence/releases/v2.4.0/drafts/
ROLLOUT_PLAN.md`): new python image for query_kn x2 and binance_bar_edge, new rust image for stream_kn x2; no
manifest, realm-revision or JWT change. Rollback: previous digests (`26c3b051`, `58bdd282`, `a235c0a1`).
