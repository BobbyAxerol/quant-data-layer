# KN-5 Paired Cutover And Rollback

Status: PAIRED HANDOFF / ROLLBACK-RETURN / PRODUCTION LOAD PASS (2026-09-27).
Old writer/read roles stopped; publication and dependency-clean artifact pending. Governing plan:
[KN-5](../../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md#kn5-predeploy-gap-closure).
No additional phase. This runbook is not a deployment authorization.

## Artifact Freeze

- Build Python reader/BAR edge and Rust runtime from one committed tree; package
  `qdl-projector` and `qdl-stream-gateway` INSIDE the Rust image, not host mounts.
  Build standalone SDK from the same tree. Record image IDs, labels, wheel hash,
  source commit and affected test receipts. Do not retag an old binary as new.
- Runtime TS was read-only checked at SDK2.0.3. Candidate SDK2.0.4 adds offset0
  cursor support: include the market-data consumer SDK artifact/update in the
  handoff packet, or prove that exact deployed client against the KN cursor
  boundary before routing. A built wheel does not update a running consumer.
  No TS executor/risk/portfolio or strategy behavior change is included.
- If locked upstream packages are unavailable, `Dockerfile` accepts
  `QDL_DEPENDENCY_IMAGE=<name>@sha256:<digest>`. It verifies active main-lock
  versions and installed RECORD hashes before copying only `/opt/venv`; the
  receipt is embedded at `/opt/qdl/dependency-receipt.json`. No source from the
  dependency image is reused. Keep that immutable dependency artifact available
  for reproducibility; do not silently update VN packages or use a mutable tag.
  A local unpublished digest can be supplied with Buildx named context
  `<name>@sha256:<digest>=oci-layout://<export-dir>@sha256:<digest>` after
  verifying its exported OCI blobs. Remote CI needs that same OCI artifact or
  an approved registry copy; a local daemon tag is not a remote build input.
- Candidate source: catalog11/712 bindings, acquisition19, promotion scope10,
  release routing27, alpha manifests14/14, TS10, research6. Compile gateway
  bundle with `scripts/kn_gateway_bundle.py`; verify routing hashes using the
  existing `StableReleaseRoutePlan` loader. JWT revisions must match manifests.
- Retired four research book requirements retain instrument metadata; new
  successor futures require provider metadata and separate demand, not guessing.
- Snapshot exact current role image IDs, runtime-directory mounts, configuration
  hashes and restart counts immediately before handoff. Keep source and runtime
  revisions separate. Never regenerate keys over the live runtime directory.

## Production Packet

- Reuse the three canonical brokers, native producers, quota/provider Redis and
  TLS network. Provision only `md.latest.v2` / `md.bars.v2` plus exact KN ACLs
  with `scripts/kn_state_topics_packet.py` review/verify/apply. Use the previously
  approved dedicated `User:kn-projector`, not `phase8-consumer`. Its client CA
  must be added alongside existing trust before activation; exact broker trust
  paths/reload or roll and old-client continuity must be in the runtime packet.
  The offline topic/ACL packet does not itself install that identity. Preserve canonical
  partition plan/offsets. Verify segment and retention policies, RF3/minISR2.
- Add one dedicated market cache (`noeviction`, listpack128/2048, no AOF/save),
  candidate maxmemory8,000,000,000 B / container9GiB. This is a measured padded
  allocator envelope, not permission to allocate unbounded RAM. Measure native
  replay peak, output buffers and whole stack during actual candidate operation.
- Two native projector replicas own Stage A/B through the existing fenced
  consumer groups. Two native Stream replicas and existing two Python Query
  roles use ONE versioned bundle/topic identity/cursor-key/route generation.
  Query selects `QDL_STABLE_QUERY_BACKEND=kn3`; it must not mount/read SQLite.
- Production catalog9/216 is not candidate11/712. New daily-universe BARs need
  producer/acquisition registration and real BAR bootstrap through the existing
  edge/core, not just an entitlement change. Compile/diff native maps and sealed
  BAR checkpoint migration before deciding the exact producer-config roll list.
  Keep valid current authority; do not reset offsets or invent another writer.
- Use retained Kafka canonical/state history and bounded vendor BAR bootstrap;
  no production spool import. Provider historical overlap remains explicitly
  unavailable for strict full history; never interpolate or relabel it FULL.
- Start watcher BEFORE candidate setup. Include all three brokers, all native
  producers, edge, native projectors, Query/Stream, market cache AND existing
  quota/provider Redis in same-window CPU/memory/lag measurements. Current
  composed receipt is missing that Redis and is NOT full-stack PASS.

## Handoff And Exit

1. Fast per-binding/replica readiness, exact requested history coverage and
   `gaps?include_coverage=true`: disabled/absent products are explicit. Check
   expiry, schema, component eligibility and source watermark. Diagnostic
   retained-window success is never a listing-to-now history certificate.
2. Move the versioned Query/Stream target pair, never DNS aliases alone. Bound
   persistent-channel drain/reconnect and incompatible cursor resnapshot. Test
   old/new JWT revisions and both replicas. No order/signal/sizing mutation.
3. Paired rollback to old V2 and return to KN. Old V2 does NOT serve the expanded
   catalog automatically. New universe/reference products stay BLOCKED during
   rollback unless explicitly proven available there. V1 is only the approved
   Binance TRADE/legacy policy fallback, not a universal substitute for L2/BAR.
4. Run one final 300s no-order acceptance after fast matrices pass; report actual
   TS Redis ACK/readback and alpha call-to-usable milliseconds, all denominators,
   execution refusals, recovery, lag and full-stack resources. No SLA relaxation.
5. Only after handoff/rollback-return pass: stop six old SQLite projectors and
   old Stream writer. Assert no process still writes the spool. Keep exact old
   state/images/config for a bounded rollback window; recompute the last safe
   rollback time from broker retention and committed offsets, not a guessed TTL.
6. Preserve old state; no Redis flush, SQLite deletion or volume removal. After
   approved remote feature->dev CI->main release, publish immutable2.2.0 and
   clean exact disposable images/worktrees. Keep one named rollback set only.

## Executed Packet

Runtime state: `/home/bobby/.local/state/qdl-v2/releases/v2.2.0-02cd827`.
Evidence: `/home/bobby/.local/state/qdl-v2/kn5-close-20260927`.
TS query alias `qdl-v2-query:8200` and stream pair `qdl-v2-stream-a/b:8210`
on `executor_network` now resolve only to KN roles. Actual market-data reader
SDK2.0.4; binding/JWT10 unchanged. Rollback/return and final50 passed.

Ten old roles are STOPPED (not removed): `projector_v2`, `_2`..`_6`,
`query_v2_1/2`, `stream_v2_active/passive`. The old
`qdl-v2-stable-boot-recovery.service` is DISABLED; do not run its spool rebuild
against the native architecture. Exact unit, Compose and inspect backup live at
`legacy-retirement-backup` inside the runtime packet. Native Docker restart and
Kafka-state cache rebuild own recovery. Old SQLite/Redis/offsets were not deleted.

Before old-path rollback, verify retained canonical offsets are still readable;
expired retention requires an explicit history rebuild and is not instant rollback.
Start exact old role images with the additive union catalog from the packet,
catch up, stop the TS reader before reversing aliases, then use the exact old
TS image/config. Do not attach two generations to the same aliases concurrently.
Restore the old boot unit only with old-architecture ownership, never by default.

The initial dependency-preserving build option above is NOT permission to use a
quarantined package. Owner approved excluding vnstock/vnai from new KN images;
DNSE/Vietnam remain served by unchanged V1 until a separate migration. No GHCR
token change is required. New release packaging must build from the clean lock.
