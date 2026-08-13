# Phase 6 Certification And Recovery Runbook

## Scope And Safety

This runbook covers the V2 shadow pipeline and a future capability-scoped
authority cutover. It does not authorize a broad-universe or combined-runtime
cutover. Production V1 remains authoritative until the acceptance report marks
the exact venue/market/feed slice `PASS` and an operator approves promotion.

Synthetic simulator output is allowed only under `tests/`. Provider smoke
evidence must contain authentic provider bytes and provenance. Never seed a
production Redis, durable log or historical store to make a gate pass.

## Preflight

1. Record release SHA, schema bundle SHA, config revision, source policy and
   instrument-registry revision.
2. Verify the target `DataRequirement` owner and consumer rollback manifest.
3. Verify control identity, audit sink, telemetry collector and alert routing.
4. Confirm the old producer still owns authority and the new producer is fenced
   in `SHADOW`.
5. Confirm durable storage, Redis shadow namespace and history bucket have safe
   capacity. Never reuse a production consumer group for testing.

## Venue Loss Or Reconnect Storm

1. Mark only the affected feed `DEGRADED`/`OFFLINE`; remove execution eligibility.
2. Inspect heartbeat age, provider status, DNS, rate budget and credentials.
3. Reconnect with bounded exponential backoff and jitter under connection limits.
4. Create a new source session and resubscribe from the registry snapshot.
5. For sequence feeds, fetch a provider snapshot and reconcile buffered deltas.
6. Close the gap ledger only after sequence/checksum/freshness pass.
7. Restore `LIVE` after the configured stability window. Do not silently promote
   a reference source.

For OKX maintenance notice `64008`, use make-before-break: open a replacement,
subscribe and obtain a fresh book snapshot, switch generation atomically, then
close the old socket. Old-generation frames are discarded.

## Durable Transport Or Network Partition

1. Stop acknowledging canonical success while the sink is unavailable.
2. Apply bounded backpressure. Use the approved local spool only while disk and
   scope bounds remain healthy.
3. Before capacity exhaustion, mark the feed degraded and disconnect cleanly.
4. After recovery, drain in source order, verify cursor continuity and reconcile
   the unacknowledged source range.
5. A missing acknowledged event is SEV-1. Preserve spool and logs as evidence.

## Redis Loss

1. Keep durable ingestion running; mark latest projection unavailable.
2. Rebuild to an isolated namespace from canonical events.
3. Compare key count, content checksum and freshness with the previous namespace.
4. Switch projection/API traffic atomically, then restore V1 compatibility
   publication. Pub/Sub messages are not backfilled; V1 consumers warm up again.

## Projector Or Consumer Failure

1. Compare durable checkpoint with idempotent output before restart.
2. Replay from the last confirmed cursor; duplicate events must not produce a
   second visible state transition.
3. A slow stream consumer is disconnected with a typed error. Other consumers
   remain live; the slow consumer uses its signed cursor to replay.
4. An expired cursor blocks trading until a new snapshot-plus-cursor handoff.

## Historical/Object-Store Failure

1. Never expose uploaded data or manifests before the atomic head update.
2. On upload/commit timeout, retain the old head and classify new objects as
   orphans.
3. Verify checksums and lineage, then retry from the old parent snapshot.
4. Purge orphans only after the retention floor and exact dataset confirmation.
5. For corruption, move readers to the last verified snapshot and rebuild a new
   immutable revision; do not overwrite files in place.

## Authority Promotion And Rollback

Promotion unit is `(environment, provider, venue, market, feed, instrument/hash
range)`. Required gates are contract, correctness, durability, projection,
recovery, compatibility, performance, security and operations.

1. `OFF -> SHADOW`: no public write authority.
2. `SHADOW -> CANARY`: compare source identity, counts, values, timestamps,
   sequence and quality against the current producer.
3. `CANARY -> PRIMARY`: approve only after rollback rehearsal and clean error
   budget. Keep V1 compatibility projection.
4. Rollback by fencing the new writer, restoring the old authority flag and
   replaying/reconciling the affected cursor range. Never delete a topic or
   canonical event to roll back.

OKX SBE rollback always returns to JSON for the same capability. Unknown SBE
template/version fails closed and cannot fall through to best-effort decoding.

## Game-Day Matrix

Exercise and attach evidence for process kill, broker outage, Redis rebuild,
projector checkpoint boundary, object-store commit failure, malformed frame,
sequence gap, reconnect storm, duplicate shard owner, slow consumer and bad
config rollback. Regional DR requires real replicated infrastructure and cannot
be certified by a same-host test.

## Cleanup

Remove disposable containers, networks, volumes, test spool files, test Redis
prefixes, consumer groups and generated signing keys. Keep compact reports and
checksums. Never remove production state during Phase 6 cleanup.
