# ADR-0005: V1 Compatibility Ownership

- Status: Accepted
- Date: 2026-08-13

## Decision

`app.main:app`, existing `/v1` routes, current Redis keys/channels and the V1 SDK
remain authoritative until an approved per-feed cutover. V2 schemas, role apps,
control tables and registry code remain dark.

The compatibility facade owns projection into the exact observed V1 shapes.
Provider adapters and future canonicalizers do not write legacy fields by
accident. Every cutover must prove golden compatibility and retain a feed-level
rollback to the last certified producer.

## Consequences

Existing alpha and Trading System consumers require no endpoint, payload or
import change in Phase 1. Deploying a dark role does not grant it publication
authority.

