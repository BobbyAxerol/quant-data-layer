# ADR-0003: Runtime Role Boundaries

- Status: Accepted for Phase 1 dark deployment
- Date: 2026-08-13

## Decision

Phase 1 adds independently deployable Python entrypoints for `api`, `control`
and `history`. These roles are dark and cannot own live ingestion. The existing
`app.main:app` process is explicitly named `compat_combined` and remains the
only V1 ingestion/legacy-projection authority.

Each entrypoint validates `QDL_RUNTIME_ROLE` and the ingestion ownership flag at
startup. Contradictory ownership fails closed. API replicas use a passive stream
status view and do not import venue WebSocket loops.

## Consequences

Scaling a query API does not scale provider connections. The split is available
for topology validation without changing current deployment authority. A later
phase may extract the ingestor after durable ownership, lease and fencing are
available; Phase 1 does not fake multi-owner safety.

