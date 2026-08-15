# Phase 7.3 Public Beta Decision Runbook

## Scope And Authority

This gate certifies only the protected, read-only V2 public beta. V1 remains the
sole source authority and rollback contract. A passing result does not permit a
live execution service to depend solely on V2, promote Rust authority, write a
legacy Redis namespace, or retire a V1 endpoint.

The gate uses provider-authentic closed bars read through the bounded internal
V1 adapter. It admits zero generated market events into certification evidence.
Synthetic load remains limited to separately labelled unit/performance tests.

## Preconditions

1. Run Phase 7.0-7.2 contract, security, topology and consumer-canary gates.
2. Build the candidate from a committed worktree and resolve it to an immutable
   image ID or registry digest.
3. Resolve Redis and init-helper images to immutable IDs.
4. Supply disposable beta-only JWT, cursor and internal-ingest keys. Never reuse
   production credentials.
5. Confirm V1 health before starting. The certification must not restart or
   reconfigure a V1 container.

## Automated Gate

```bash
docker build --provenance=false -t data-layer:phase7-test .

QDL_BETA_IMAGE="$(docker image inspect data-layer:phase7-test --format '{{.Id}}')" \
QDL_BETA_REDIS_IMAGE="$(docker image inspect redis:7.2-alpine --format '{{.Id}}')" \
QDL_BETA_INIT_IMAGE="$(docker image inspect redis:7.2-alpine --format '{{.Id}}')" \
QDL_BETA_CURSOR_KEYS_JSON='<disposable-beta-cursor-keyring>' \
QDL_BETA_JWT_KEYS_JSON='<two-disposable-beta-jwt-keys>' \
QDL_BETA_INTERNAL_INGEST_SECRET='<disposable-32-byte-secret>' \
scripts/phase73_public_beta_certification.sh
```

The script performs all of the following as one fail-fast transaction:

- captures V1 containers, images, mounts, networks and restart counts;
- starts isolated query, active/passive stream and AOF-backed beta Redis roles;
- ingests only real V1/provider closed bars into the isolated canonical spool;
- measures normal/burst REST query traffic and four-way stream fan-out;
- verifies three fast consumers drain contiguous offsets while one bounded slow
  consumer is explicitly disconnected and can replay;
- tests missing/invalid identity, two-key rotation, malformed/oversized input,
  rate limiting, cursor tamper/expiry/consumer mismatch;
- stops/restarts beta Redis, requires query fail-closed `503`, then requires
  recovery and a strictly increasing persisted fencing epoch;
- verifies V1 fallback remains `200`;
- removes all beta containers, networks, volumes, Redis keys and cursor files;
- compares the exact V1 topology before and after.

## Frozen Thresholds

| Metric | Gate |
|---|---:|
| Normal query rate | at least 10 requests/s |
| Burst query rate | at least 20 requests/s |
| Query p99.9 | at most 1000 ms |
| Query/admitted stream error budget | 0 unexplained errors/loss |
| Closed-bar freshness | at most 240000 ms |
| Per-container RSS | at most 512 MiB |
| Per-container CPU | at most one CPU core |
| Durable spool growth | at most 32 MiB |
| Redis growth | at most 16 MiB |
| Cursor/replay lag after drain | exactly 0 |
| V1 topology or production beta-key mutation | exactly 0 |
| Temporary beta resources after cleanup | exactly 0 |

These are beta certification thresholds for the current VPS, not universal
production capacity claims. Any hardware, load shape, message-size or topology
change requires new evidence rather than reusing this result.

## Failure And Rollback

Any failed check yields `BETA-NO-GO`. The trap always runs Compose `down -v
--remove-orphans`; the operator then confirms zero resources with the project
label `qdl_phase73_certification` and zero `qdl:beta:v2:*` keys in production
Redis. V1 requires no restart, replay or source resubscription.

## Evidence

- `upgrade/evidence/phase7-capacity.json`
- `upgrade/evidence/phase7-security-adversarial.json`
- `upgrade/evidence/phase7-evidence-freeze.json`
- `upgrade/evidence/PHASE7_PUBLIC_BETA_REPORT.md`

The evidence records the commit, immutable image, load shape, latency tails,
resource peaks, failure results and exact cleanup decision. Credentials and raw
JWT/cursor values are deliberately excluded.
