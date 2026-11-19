## Summary

DL v2.4.0 CI6 unblock (`fix/dl-v240-deterministic-timing-tests` -> `dev`, from 5bc1718). The CI6 dev push run
44491247372 failed `rust-workspace` twice on two different wall-clock assertions in tests (a 267 ms scheduler stall
against `< 100 ms`; session-alive gaps against a +-50 ms window). This PR fixes the class: every Rust test assertion
that combines a real clock with a numeric bound was audited and is now either deterministic (wait-until with a
bounded deadline instead of a fixed sleep), bounded by the defect it detects (e.g. an active-lane pause vs the
establish budget, make-before-break proven by ordering), or given load headroom. Tokio paused-time tests and lower
bounds were already safe and are unchanged. Test-only: no production code, image or runtime change.

Files: `rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs` (test module), `rust/qdl-stream-gateway/tests/native_stream.rs`,
`rust/qdl-stream-gateway/src/{replay,hub}.rs` (test modules), `rust/qdl-projector/src/{kafka_pipe,shutdown}.rs`
(test modules), `rust/qdl-projector/tests/stage_a_kafka.rs`, journal.

## Validation

Builder `qdl-rust-builder` (`2cb66ad6`), offline, host slot, `--cpus 2`, `--test-threads=4`, host load 19.7-25.6:
each affected test binary run 20 times: `qdl_native_raw_ingestor` 20/20, `native_stream` 20/20,
`qdl_stream_gateway` lib 20/20, `qdl_projector` lib 20/20, `qdl_kafka` lib 20/20. `cargo fmt --check` OK, clippy
`-D warnings` (all targets of the three crates) OK. `stage_a_kafka` needs the KN broker harness and runs in
`kn-native-integration`.

## CI plan (CI2 / CI11)

`scripts/ci_change_scope.py --base 5bc1718 --head <head> --event pull_request --base-ref dev`: `scoped: rust`,
8 files, no python, no contracts, no CI-defining path.

| Job | Runs / inherited | Est. min |
|---|---|---|
| changes, preflight, repository-scan, ci-result | runs | 1.5 |
| contract-tests (PR-base `buf breaking`) | runs | 0.3 |
| rust-workspace | runs | 3 |
| kn-native-integration | runs | 6 |
| python suites (unit-tests x4, sdk, release-gates) | not in scope (no python change) | 0 |

About 11 job-minutes on the PR. After the local `--no-ff` merge, the `dev` push is a full run (CI5, about 64
job-minutes): that run is the v2.4.0 CI6 receipt on the final tree.

## Risk And Rollback

Test-only. A loosened bound could hide a regression only of the size it now allows; every changed bound still
separates the defect it targets (documented next to each assertion). Rollback: revert the commit.
