# Phase 9.1 Rust Canary Runbook

## Scope

Phase 9.1 certifies a provider-neutral, exact-slice Rust canary path while
Python V1 remains the sole authoritative public and legacy writer. The frozen
rehearsal slice is `BINANCE / USDM / PERPETUAL / TRADE / BTCUSDT /
partition-plan epoch 1`.

The current Phase 9.0-C decision is `NO_GO_EXTERNAL`. Therefore this runbook
permits only isolated rehearsal. It cannot persist production
`RUST_CANARY`, publish into public/V1 namespaces, alter V1 Redis ownership or
authorize a cutover.

## Build And Test

Run from the repository root on the reviewed Phase 9.1 revision:

```bash
make phase91-test
```

This builds the Rust builder and non-root runtime images, runs Rust format,
clippy with warnings denied, the complete Rust workspace, focused Phase 8-9
regressions and the full Python suite.

## Isolated Certification

```bash
make phase91-certify
```

The certification:

1. Replays the frozen authentic Binance provider capture through Python and
   three clean Rust processes.
2. Requires exact canonical bytes, decimal/timestamp/identity/event hashes and
   aggregate SHA-256 parity.
3. Uses a separate three-node TLS/ACL Kafka project and isolated Redis.
4. Exercises exact authority fencing, one-replica loss, below-min-ISR failure,
   full broker restart, compacted authority recovery, audit ordering,
   slow-consumer catch-up and rollback to `RUST_SHADOW`.
5. Requires zero public and legacy writes and unchanged V1 identity/health.
6. Cleans its containers, networks and volumes in `finally`.

Expected decision on the current prerequisite bundle:

```text
COMPLETE_IMPLEMENTATION / CANARY_NOT_AUTHORIZED
```

Required evidence:

- `upgrade/evidence/phase91-rust-canary-certification.json`
- `upgrade/evidence/PHASE91_RUST_CANARY_REPORT.md`
- `upgrade/evidence/phase91-evidence.sha256`

Verify independently:

```bash
sha256sum -c upgrade/evidence/phase91-evidence.sha256
```

## Failure Interpretation

Any semantic mismatch, stale owner/revision/lease/partition plan, wrong target,
duplicate watermark, guardrail block, broker durability failure, replay gap,
public/legacy write or V1 topology change fails the certification. Do not
weaken a gate or relabel local evidence as production evidence.

A compacted authority topic is read directly by partition after restart;
complete state-transition history is verified separately on the non-compacted
audit topic. Fault tests stop broker processes while clients bootstrap only
through surviving broker names, avoiding Docker DNS artifacts.

## Cleanup

```bash
make phase91-clean
```

The target removes only the isolated Phase 9.1 project and its two disposable
images. It must not prune Docker globally or remove V1 volumes/images.

After cleanup verify:

```bash
docker ps -a --filter label=com.docker.compose.project=qdl_phase91_certification
curl -fsS http://127.0.0.1:8100/v1/health
```

## Production Promotion

Production promotion remains forbidden until Phase 9.0-C yields a fresh,
unexpired exact-candidate `GO` bundle from production/independent failure
domains and an operator separately approves the exact slice, blast radius,
hold window and rollback manifest. At that point, rerun this certification
against the immutable candidate, verify all consumers and execute the formal
authority CAS/audit procedure. Never promote through an environment flag or
direct database edit.
