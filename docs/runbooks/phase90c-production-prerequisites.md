# Phase 9.0-C Production Prerequisite Runbook

## Scope

This runbook certifies the infrastructure, security, recovery and exact-slice
approval prerequisites for a future Phase 9.1 Rust canary. It does not promote
Rust, expose V2 publicly, alter V1 Redis namespaces or grant write authority.
The frozen candidate remains `RUST_SHADOW` with public and legacy writes off.

## Evidence Trust Boundary

Evidence scope is part of the contract:

- `TEST` proves a deterministic unit or contract behavior only.
- `LOCAL_REHEARSAL` proves an isolated same-host integration behavior only.
- `PRODUCTION` must originate from the real production control plane.
- `INDEPENDENT_FAILURE_DOMAIN` must cross a real independent failure domain.

Never relabel local containers, debug telemetry, self-signed test credentials or
same-host replicas as production evidence. Evidence files contain identifiers,
checksums and measurements only; never put credentials, tokens or private keys
in an inventory.

## Local Control-Plane Certification

From the repository root on the approved feature revision:

```bash
make phase90c-certify
sha256sum -c upgrade/evidence/phase90c-evidence.sha256
```

Expected local decision is `NO_GO_EXTERNAL`. The command validates strict
schema/evidence handling, authority CAS and immutable audit behavior in an
isolated PostgreSQL instance, applicable Phase 8/9 contracts, and V1 identity
and health before/after evaluation. It removes the disposable database
container. It never writes production PostgreSQL or Redis.

Required local evidence:

- `upgrade/evidence/phase90c-production-prerequisites.json`
- `upgrade/evidence/PHASE90C_PRODUCTION_PREREQUISITES_REPORT.md`
- `upgrade/evidence/phase90c-authority-migration.json`
- `upgrade/evidence/phase90c-evidence.sha256`

## Production Evidence Intake

1. Freeze the exact candidate manifest and verify all digests and revisions.
2. Collect each gate from its owning production system. Use repository-relative,
   immutable evidence artifacts and SHA-256 hashes; keep secrets external.
3. Use `PRODUCTION` only for real production evidence. DR evidence must use
   `INDEPENDENT_FAILURE_DOMAIN`.
4. Validate without mutation:

```bash
python3 scripts/phase90c_prerequisite_certification.py   --inventory path/to/approved-production-inventory.yaml   --expect GO   --output upgrade/evidence/phase90c-production-go.json   --report upgrade/evidence/PHASE90C_PRODUCTION_GO_REPORT.md
```

5. Independently verify evidence ownership, expiry, candidate digest, image
   signature identity, consumer approvals, blast radius and hold window.
6. Apply `0006_phase9_authority_prerequisites.sql` only through the normal
   reviewed database migration lane with backup/PITR confirmed. Register the
   checksummed decision bundle through the restricted control-plane database
   identity; ingestion/projector identities must not have insert/update access.
7. Obtain a separate operator approval for the exact slice. A general Phase 9
   approval is insufficient.

A `GO` report is a prerequisite artifact, not an authority transition. Phase
9.1 still requires its own approved canary procedure.

## Authority Safety

`qdl_transition_authority` rejects stale state, revision, owner, lease and
partition-plan expectations. Entering `RUST_CANARY` or `RUST_PRIMARY` also
requires a non-negative terminal watermark, unexpired matching `GO` bundle and
a future hold window that does not outlive the bundle. Every successful
transition is appended to immutable audit history.

The authority tables are additive. Do not mutate transition audit rows, reuse a
bundle for another candidate digest, or bypass the function with direct updates.
Sink-side fencing remains mandatory; the database record alone cannot prevent a
misconfigured producer from attempting a write.

## Rollback And Revocation

Before Phase 9.1, rollback is simply to keep V1 authoritative and remove only
disposable certification resources. Revoke, expire or supersede compromised
evidence and issue a new bundle; do not edit an existing bundle or audit row.
Any missing, expired, malformed, lower-scope or mismatched evidence returns the
decision to `NO_GO_EXTERNAL` and must not restart or promote a producer.

## Current Decision

Phase 9.0-C may close as `COMPLETE_CONTROL_PLANE / NO_GO_EXTERNAL`. Real
replicated transport, production observability, workload identity/RBAC/network,
external secret rotation, signed registry admission, PITR/object restore,
independent DR, production projector rebuild, complete consumer ownership and
exact operator approval remain external deployment gates.
