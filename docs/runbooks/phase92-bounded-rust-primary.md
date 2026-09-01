# Phase 9.2 Bounded Rust Primary Runbook

## Authority Boundary

Phase 9.2 is additive and exact-slice only. Current repository evidence is an
isolated rehearsal because Phase 9.0-C is `NO_GO_EXTERNAL`. Do not disable a
production Python subscription, write production authority, or point isolated
projection topics at real V1/V2 destinations.

The only valid local close state is:

```text
COMPLETE_IMPLEMENTATION / PRIMARY_NOT_AUTHORIZED
```

## Preconditions

1. Worktree is on the approved Phase 9.2 branch and candidate digest matches
   `config/phase9/candidate-slice.yaml`.
2. V1 health is HTTP 200 and its container identity/restart count are recorded.
3. The Phase 9.0-C decision remains checksummed and is not edited for testing.
4. Docker has enough bounded capacity for three disposable KRaft replicas.
5. No production topic, Redis namespace, credential, volume or subscription is
   mounted into the rehearsal.

## Certification

```bash
cd /home/bobby/data_layer
make phase92-test
make phase92-migration
make phase92-certify
sha256sum -c upgrade/evidence/phase92-evidence.sha256
```

The certification must prove:

- authentic Python/Rust canonical parity has zero mismatch;
- direct primary bypass, stale owner/revision/lease/plan, N, N+2, duplicate and
  wrong-target writes fail closed;
- terminal checkpoint and accepted handoff records are immutable;
- `RUST_CANARY -> RUST_PRIMARY -> BLOCKED -> ROLLBACK_PENDING ->
  PYTHON_PRIMARY` is ordered and reconstructable after broker restart;
- isolated canonical/public/legacy projections all contain exactly watermarks
  101 through 181 with the owner boundary at 164/165;
- a fresh Rust process loading the terminal primary authority rejects all three
  targets until each durable projection watermark is reconstructed, rejects W
  again after restore and resumes all targets exactly at W+1;
- one replica loss still ACKs, below-min-ISR fails closed;
- production public/legacy writes remain zero;
- V1 topology and health are unchanged before/after;
- all disposable resources are absent after cleanup.

## Production Promotion Gate

Do not reuse local rehearsal evidence as production approval. A future primary
cutover additionally requires a fresh exact Phase 9.0-C `GO`, a completed real
`RUST_CANARY` hold, consumer registry approval, immutable rollback artifact,
change ticket and explicit operator approval naming the exact slice.

At cutover, persist the old-owner checkpoint at W, accept clean handoff evidence,
execute the database CAS, load the new authority into final sink and projector,
then emit first authoritative watermark W+1. Disable only that Python
subscription after all of those gates pass.

## Rollback

Fence Rust first. Persist its final watermark, enter `ROLLBACK_PENDING`, accept
a Rust-to-Python handoff, grant Python a new authority revision and lease, then
resume from the reconciled next watermark. Never restart Python as an
uncoordinated writer.

For local cleanup:

```bash
make phase92-clean
```

This command removes only Phase 9.2 certification containers, networks, volumes
and images. It does not prune global Docker state or touch V1 data.
