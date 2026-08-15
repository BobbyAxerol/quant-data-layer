# Phase 7 Public Beta Certification Report

## Decision

**BETA-GO: protected read-only V2 beta only.**

V1 remains authoritative. V2 remains forbidden as a sole live-execution
dependency and has no permission to own provider subscriptions, publish legacy
Redis payloads or promote Rust authority. Production authority is still blocked
on the Phase 6 infrastructure gates and the Phase 8-9 promotion program.

## Certified Candidate

- Branch: `feat/fund-grade-data-layer-v2`
- Runtime commit: `49c576468870a4760893f34b53f5a87132a42b8f`
- Runtime and certification harness commit: `49c576468870a4760893f34b53f5a87132a42b8f`
- Immutable runtime image: `sha256:bc7b40bcb773b1cefd58a71d2625b769ee64bbd37f3d93e0780b9f013ddf0d1f`
- Contract: `2.0.0-beta.1`
- Frozen OpenAPI SHA-256: `bea44d3920db52f5893eb773aa195ae7f4abd2684d5ca65d904e995934fabcea`
- Authority during every test: `V1_SHADOW_READ_ONLY`

## Implementation Completed

- Enforced `max_streams` from each consumer manifest at the durable gateway,
  independently from the global subscriber ceiling.
- Added a bounded read-only capacity consumer; no execution entitlement and no
  direct venue/legacy Redis access were granted.
- Added one reproducible certification harness for real-provider normal/burst
  query traffic, replay/live fan-out, slow-consumer isolation, security abuse,
  dependency outage, failover, resource measurement and exact cleanup.
- Made the isolated beta Redis fencing epoch durable with beta-only AOF state.
  This closes the epoch-reset defect found by the Redis restart gate. The volume
  is isolated from V1 and deleted after certification.

## Verification Results

### Functional And Compatibility

- Targeted Phase 7 regression: 34/34 passed.
- Full Python regression: 309 passed, 5 conditional skips covered by separate
  Docker/Buf/migration/provider gates.
- Rust: fmt and clippy with warnings denied passed; 11/11 tests passed.
- Buf format/lint and breaking checks passed against both immutable Phase 1 and
  Phase 7 beta baselines.
- V1 topology, image IDs, mounts, networks and restart counts were byte-equal
  before/after the beta topology test.

### Capacity And Continuity

- Real-provider rows: at least 64 closed BTCUSDT 1m bars; generated events: 0.
- Normal profile: 30 requests, concurrency 5, 39.120 requests/s, p99.9
  211.058 ms, zero errors.
- Burst profile: 60 requests, concurrency 20, 36.362 requests/s, p99.9
  628.694 ms, zero errors.
- Stream: four subscribers, three fast plus one intentionally slow; 652.316
  events/s and 351153.377 bytes/s.
- All fast consumers observed contiguous offsets and drained cursor/replay lag
  from 64 to 0. The slow consumer was explicitly disconnected after its bounded
  buffer filled; durable data remained replayable.
- End-to-end latest closed-bar freshness was 18125.331 ms.

### Security, Failure And Resource Bounds

- Missing token/scope, wrong audience/environment and consumer mismatch failed
  closed with `401/403`; both JWT verification keys passed rotation.
- Malformed request returned typed `400`, oversized request `413`, quota
  exhaustion `429`, cursor tamper/scope mismatch `CURSOR_INVALID`, and expired
  cursor `CURSOR_EXPIRED`.
- Redis outage changed query/readiness to `503`; recovery returned `200`.
  Fencing epoch persisted and advanced from 1 to 2 through restart plus owner
  failover.
- Peak application RSS was about 68.2 MiB; peak CPU was 75.21% of one core.
- Durable spool grew 73728 bytes. Redis growth was non-monotonic after expiry
  and remained below the configured bound.

## Cleanup Proof

- Beta containers after gate: 0.
- Beta networks after gate: 0.
- Beta volumes, including AOF and canonical spool: 0.
- Cursor files and disposable runtime state: 0.
- `qdl:beta:v2:*` keys in production Redis: 0 before and after.
- V1 fallback remained HTTP 200 and V1 was never restarted or reconfigured.

## Remaining Boundaries

No open blocker remains for the **read-only public beta scope**. The following
are deliberate later-phase boundaries, not Phase 7 debt:

- the Phase 7 bounded V1 bridge is not an authority-capable durable broker;
- file cursor stores remain limited to monitoring/disposable paper consumers;
- critical consumers need an approved transactional checkpoint adapter;
- production source/Rust authority still requires Phase 8 shadow evidence and
  Phase 9 slice-by-slice canary/primary approval;
- any post-freeze contract change requires a new digest, compatibility report
  and SDK support decision.
