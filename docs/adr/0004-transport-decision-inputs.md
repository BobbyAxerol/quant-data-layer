# ADR-0004: Transport Decision Inputs

- Status: Accepted as a decision boundary; transport not activated in Phase 1
- Date: 2026-08-13

## Decision

Canonical contracts and event identity are transport-neutral. Phase 1 defines
the inputs required by a durable transport: deterministic event ID, source and
partition sequence, lease epoch, config/instrument revision, four timestamps,
quality flags and raw lineage hash.

No Kafka broker or Redis Stream is made authoritative in this phase. Phase 2
must first pass the durability decision gate in the unified plan. Redis remains
the V1 latest-state/PubSub compatibility path, not the new canonical source of
truth by implication.

## Consequences

The Phase 2 bridge/Kafka decision can change transport implementation without
renaming domain fields or rewriting adapters. No new infrastructure resource is
consumed by Phase 1 runtime.

