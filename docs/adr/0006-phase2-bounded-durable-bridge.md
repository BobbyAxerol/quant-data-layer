# ADR 0006: Phase 2 Bounded Durable Bridge

## Status

Accepted for dark/shadow use only on 2026-08-13.

## Decision

Phase 2 uses a local SQLite WAL spool behind transport-neutral `EventSink` and
`EventSource` contracts. It is not a public API and it is not the long-term
canonical backbone.

The spool uses `journal_mode=WAL`, `synchronous=FULL`, atomic batches,
deterministic event IDs, immutable payload SHA-256, monotonic logical cursors,
consumer checkpoint expiry and fail-closed bounds for events, bytes, physical
storage, partitions, consumers and quarantine metadata. Accepted raw data is
committed before canonicalization. A crash after raw commit is recovered by
replay; canonical and Redis projections are idempotent.

The current AOF-off `redis_marketdata` is never used as durable storage. Redis
remains a rebuildable latest-state/V1 compatibility projection. All Phase 2
keys use an isolated shadow namespace and production legacy publication remains
disabled.

## Why Not Kafka Yet

Kafka protocol remains the target infrastructure boundary, but Phase 2 evidence
does not justify introducing another always-on cluster merely to prove domain
semantics. Promotion requires an approved material trigger: more independent
replay consumers, a longer replay horizon, sustained trade/book volume beyond
the bridge budget, multi-node HA, or unacceptable measured lag/recovery time.

The bridge benchmark is intentionally reported, not advertised as a universal
capacity claim. It certifies only the selected BTCUSDT/ETHUSDT USD-M trade shadow
slice.

## Consequences

- V1 ingestion and Redis publication remain authoritative and unchanged.
- A dedicated process can later replace SQLite with Kafka without changing
  canonical events, partition keys or cursor semantics exposed to applications.
- SQLite is a single-host failure-domain bridge. It is not HA and must never be
  promoted to broad-universe/book-delta authority.
- Phase 3 may connect the dark shadow slice after an explicit deployment plan;
  this ADR alone authorizes no runtime cutover.

## Sunset Procedure

1. Stop accepting new shadow events.
2. Drain every active checkpoint and record final canonical/projector checksum.
3. Confirm another approved durable backend owns the replay horizon.
4. Close the spool and archive evidence if required.
5. Remove the isolated spool file and shadow Redis namespace only after owner
   approval. Never delete the authoritative V1 Redis/data files as part of this
   procedure.
