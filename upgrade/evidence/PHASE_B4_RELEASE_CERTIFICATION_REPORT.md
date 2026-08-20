# Phase B.4 Release Certification And Cleanup

Date: 2026-08-20

Conclusion: `PASS` for B.4. Data Layer `2.0.0 Internal Stable` artifacts are
ready for operator review. No push, merge, release publication, consumer
authority migration or production cutover was performed. Phase B overall
remains `PARTIAL_EXTERNAL` because the official DNSE provider path could not be
certified from this host; no synthetic substitute was accepted.

## Frozen Artifacts

- Code source: `2412572eaa89864ce74910b0f2e5f8b50833fb15`.
- Python: `qdl-v2-python:2.0.0-2412572`, image
  `sha256:fec269ec555624baa68ee15fdd0281d72996e55f847b7347856be6b2fa51ea25`.
- Rust: `qdl-v2-rust:2.0.0-2412572`, image
  `sha256:fbff0ed3c4390831a2aebf12f57c266eb6f01dde258b3cffacacbcbaa30d6c97`.
- Both images carry the exact OCI revision/version and run as non-root.
- Retained rollback: Python `2.0.0-c61fa39` and Rust `2.0.0-cfc0246`.

## Correctness And Contract Gates

- Final Python discovery: 506 cases, 500 passed and six explicit conditional
  infrastructure skips. No domain assertion failed.
- Rust: format passed; locked Clippy with warnings denied passed; 62/62
  workspace tests passed.
- BAR ownership/checkpoint repair: 18/18 targeted tests passed. Five Phase B
  modules ran 65 cases: 64 passed and one separately proven Redis integration
  case skipped in network-disabled execution.
- Final package/deployment regression: 25/25 passed.
- Buf format/lint, two frozen-baseline breaking checks and generated-artifact
  equality passed. V1 OpenAPI semantic compatibility had zero removals or
  incompatible changes.
- Final Rust benchmark processed 100,000 events at 133,477.5 events/s with
  p99 14,124 ns, zero duplicate and zero quarantine; gate was 50,000 events/s.

## Security Gates

- Cargo-deny passed advisories, bans, licenses and sources with the tracked
  policy. Pip-audit found no known Python vulnerability.
- Pinned Trivy 0.73.0, using the repository CI policy `ignore-unfixed=true`,
  found zero fixable HIGH/CRITICAL vulnerability and zero secret in both final
  images. A stricter diagnostic without that policy reported only currently
  unfixed Debian findings; it was not hidden or used to weaken the release
  policy.

## Real-Provider Runtime Acceptance

- Fresh isolated RF3/minISR2 Kafka accepted 500 closed one-minute BARs for each
  Binance USD-M, Binance Spot, OKX SWAP and OKX Spot binding: 2,000 total.
- Acquisition revision 2 used one REST owner for final BARs. Native Rust
  ingestors retained TRADE/QUOTE; Rust remained the only canonical core.
- Restart restored the ACK-authoritative checkpoint, skipped overlapping
  bootstrap and caught up the exact closed-bar backlog. Kafka quarantine stayed
  zero across all six partitions.
- Cache contained 75,187 canonical records across 12 bounded partitions,
  maximum 10,000 each, with zero internal offset gap, duplicate event ID or
  quarantine row. Projector lag was 35 against the 250 bound.
- Redis contained 51 bounded projection keys and used 1.22 MiB of its 128 MiB
  maxmemory. Largest Python role used 68.86 MiB/512 MiB; Rust roles used at
  most 24.56 MiB/256 MiB; Kafka brokers remained below 439 MiB/768 MiB.
- Application log scan over the acceptance window found no warning, error,
  panic, collision, stale generation or unresolved gap.

## Consumer Acceptance

- Signed SDK warmup returned 500 final real-provider BARs for both Binance and
  OKX. Both query replicas had identical identity, payload, contract, source,
  watermark and quality semantics after excluding request-clock freshness.
- Binance and OKX alpha streams emitted `REPLAYING -> LIVE`; acknowledged
  offsets were contiguous and a new client resumed exactly at prior offset + 1.
- Trading System paper read Binance/OKX TRADE and QUOTE snapshots at 132-158 ms
  freshness; all four were authoritative and execution eligible.
- Monitoring read authoritative Binance/OKX TRADE at 100-180 ms freshness.
- No order, synthetic market event or production state mutation occurred.

## Cleanup And V1 Invariant

- Removed the fresh `qdl_v2_b4_candidate` project, its networks and all five
  disposable volumes.
- Removed all stopped containers/networks from `qdl_v2_stable_candidate`, then
  deleted only its approved Kafka1/Kafka2/Kafka3/state test volumes. Preserved
  `qdl_v2_stable_candidate_stable_tls`.
- Removed three B.4 builder image tags, superseded `ea84a21` Python/Rust tags
  and the unused Python `cfc0246` tag. Final images and one tested rollback pair
  remain.
- Pruned only 41 exact BuildKit IDs from the B.4 build window. Cache fell from
  168 records/12.94 GB to 154 records/10.94 GB. No broad prune ran.
- Deleted the exact temporary bundle containing test secrets, Trivy JSON and the
  SDK acceptance harness after bounded evidence was recorded.
- V1 containers were not restarted; port 8100 remained `status=ok`, Redis true,
  recent queue drops zero and DNSE `OPEN_HEALTHY` after cleanup.

## Decision Boundary

B.4 does not authorize V2 authority, route or consumer cutover. The next action
is review and PR into `dev`. Any later deployment requires the exact topology,
ports, image digests, credentials, volumes, affected consumer manifests and
rollback command to be approved as a separate transaction.
