# Phase 9.3 Hold, Close And Expand Runbook

## Current Authority Boundary

Repository status may reach only:

`COMPLETE_CONTROL_PLANE / PRODUCTION_HOLD_NOT_STARTED`

while Phase 9.0-C is `NO_GO_EXTERNAL`. Local tests do not start a production
hold, close a production rollback window, authorize an expansion, decommission
Python or change authority. V1 remains authoritative.

## Local Certification

Run from `/home/bobby/data_layer`:

```bash
make phase93-test
make phase93-migration
make phase93-certify
sha256sum -c upgrade/evidence/phase93-evidence.sha256
```

Expected local evidence:

- current Phase 9.0-C no-go rejects production closure;
- Phase 9.2 authentic replay remains parent provenance only;
- accelerated hold observations are marked `TEST_CONTROL_PLANE_FIXTURE`;
- PostgreSQL closure leaves authority state/revision/owner/lease/watermark
  unchanged;
- all five expansion types remain
  `INDEPENDENT_CERTIFICATION_REQUIRED` with no write authority;
- Python decommission remains blocked while any ownership, rollback or consumer
  dependency exists;
- V1 health/topology is unchanged and disposable resources are absent.

## Starting A Real Hold

A real hold is legal only after a production-authorized exact
`RUST_PRIMARY` cutover and a fresh Phase 9.0-C `GO@.

1. Freeze the hold policy and its digest. The approved minimum wall-clock
   duration, sample interval, maximum sample gap, correctness zero-tolerance and
   resource/lag thresholds cannot change inside one hold ID.
2. Persist the hold identity bound to slice, candidate, prerequisite bundle,
   owner, authority revision, lease and partition-plan epoch.
3. Append observations from production telemetry. Do not backfill or interpolate
   missing intervals. A changed owner/epoch or a missing interval blocks that
   hold.
4. Persist a `BLOCKED@ decision immediately on any semantic mismatch, open gap,
   duplicate external write, accepted stale writer, authority ambiguity, durable
   ACK failure, projection mismatch, consumer checkpoint regression or
   unexplained quality failure.
5. A blocked hold is immutable. Restart only with a new hold ID and a new
   operator decision.
6. A passing decision requires the real wall-clock duration and terminal sample.
   Accelerated fixtures, replay duration and same-host time compression never
   count as production hold evidence.

## Closing The Rollback Window

Window closure is a governed audit decision, not a data-plane authority
transition and not deletion of the Python rollback manifest.

Before calling `qdl_close_authority_window`, freeze:

- passing production hold decision;
- consumer registry snapshot with every critical consumer ready, fully migrated,
  rollback-ready and checkpointed through the current authority watermark;
- authority registry snapshot matching current `RUST_PRIMARY`
  owner/revision/lease/partition/candidate/bundle;
- fresh production-scope rollback rehearsal reconciled through the same
  watermark;
- operator approval bound to hold policy, ticket and bounded expiry.

The function locks and rechecks the exact authority row. A concurrent authority
change, stale registry, expired rollback rehearsal or mismatched approval fails.
Successful closure inserts one immutable row and changes no authority field.

## Expansion

Create one manifest per expansion class:

- `INSTRUMENT_PARTITION`
- `BBO`
- `L2_BOOK`
- `BAR_LIFECYCLE`
- `VENUE_MARKET`

Each manifest needs a new candidate digest and capability-specific gate set.
Instrument/partition expansion also requires a newer partition-plan epoch.
Parent closure evidence is provenance only. Repeat provider-authentic parity,
chaos, capacity, authority handoff and rollback certification independently.
Never combine classes merely to reuse an approval.

## Python Runtime Decommission

Removal is denied until all are true:

- runtime owns zero authority slices;
- no active rollback manifest references it;
- no registered consumer depends on it;
- every replacement rollback window is governed closed;
- repository cleanup has separate explicit approval;
- shared contracts, fixtures and provider/compatibility knowledge remain.

Closing one slice does not authorize deleting a reusable adapter.

## Incident And Rollback

A closed window does not remove emergency rollback capability. On incident:

1. fence Rust at final sink;
2. persist terminal watermark and incident evidence;
3. enter `BLOCKED@ then `ROLLBACK_PENDING@;
4. accept a new Rust-to-Python handoff under a newer revision/lease;
5. resume Python from the reconciled next watermark;
6. record a new audit decision.

Do not delete or mutate hold/registry/closure/expansion rows. For local residue,
run `make phase93-clean`; it removes only containers whose names begin with
`qdl_phase93_` and performs no global Docker prune.
