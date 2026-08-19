# QDL control-plane migrations

These migrations are additive, forward-only and dark in Phase 1. They store
instrument/control metadata only; tick, trade and order-book event streams are
explicitly excluded from PostgreSQL.

Production application requires an approved backup, an immutable migration
artifact and a maintenance/change record. The Phase 1 rollback is to disable
the new resolver and continue from its exported read-only registry snapshot;
tables are retained for audit instead of being dropped automatically.

Validation runs twice against a clean disposable PostgreSQL instance and once
against an instance containing an unrelated legacy table. No production-like
database or volume is used by the validation script.

Phase 7 adds `0005_phase7_data_plane_identity.sql`. It binds an authenticated
workload subject and environment to one immutable consumer-manifest revision,
including allowed purposes, data-plane permissions, execution-dependency policy
and bounded quotas. It does not store market events or alter V1 tables.

Phase 9.0-C adds `0006_phase9_authority_prerequisites.sql`. It stores immutable
prerequisite bundles, one persistent authority record per exact slice and an
append-only transition audit. Its CAS transition function rejects stale
state/revision/owner/lease/partition expectations, binds release provenance,
requires a terminal watermark and approval hold window, and cannot enter Rust
canary or primary without a non-expired `GO` bundle bound to the candidate digest.
The migration is dark: it neither seeds an approval nor changes V1 authority.


Phase 9.3 adds `0008_phase93_hold_close_expand.sql`. It stores immutable
primary-hold observations/decisions, frozen consumer and authority registry
snapshots, rollback rehearsal and operator approval evidence, one governed
rollback-window closure record and independently uncertified expansion
manifests. The closure function locks and rechecks the exact `RUST_PRIMARY`
authority row but never updates authority ownership, revision, lease or
watermark. Expansion rows are write-disabled and cannot inherit parent
certification.
