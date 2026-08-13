# Phase 1 Implementation Report

Date: 2026-08-13  
Branch: `feat/fund-grade-data-layer-v2`  
Authority: dark/additive; existing `app.main:app`, `/v1` and Redis V1 remain authoritative.

## Delivered

- Canonical Protobuf packages for common decimal/enums, instrument metadata,
  trade, quote/BBO, bar, book snapshot/delta, funding, OI, mark/index, ticker,
  quality and feed-state events.
- Buf format/lint/breaking/generation gates with pinned Python and Rust plugins.
- Exact decimal and deterministic event-ID domain utilities. Python and Rust
  decode/encode the same binary golden fixture.
- UUIDv5 canonical instrument identity, metadata revision, temporal aliases,
  collision guards and static snapshot export.
- Capability profiles with explicit availability, tier/region constraints,
  source authority and resubscribe/resnapshot semantics.
- OKX `/public/instruments` parser for Spot, Swap, dated Futures, Options and
  Event contracts. No derivative `instId` is fabricated.
- Additive PostgreSQL control-plane migrations for instruments/revisions,
  aliases, calendars, source profiles/policies, subscriptions, config revisions,
  ingestion leases/fencing, jobs and audit. No tick stream is stored there.
- Dark Python entrypoints for `api`, `control` and `history`; contradictory
  ingestion ownership fails startup. The V1 combined facade remains authority.
- ADRs for contract representation, identity, role ownership, transport inputs
  and V1 compatibility ownership.

## Verification

- Buf format, lint, build, generate and breaking against the initial Phase 1
  binary image: PASS.
- Python canonical contract/domain tests: PASS.
- Rust `qdl-contracts` golden-byte parity: 1 PASS.
- Instrument/capability/OKX registry/migration unit tests: 12 PASS.
- Runtime ownership/topology plus V1 golden tests: 8 PASS.
- Full application-image regression: 125 PASS; its two environment-gated Redis
  tests were then run separately against disposable Redis and both PASS.
- Redis integration load case: 1,000 leases/10 demanded feeds in 0.2782s,
  snapshot in 0.0985s, memory delta 623,408 bytes; temporary container/network
  removed after test.
- Disposable PostgreSQL migration smoke: clean and legacy-seeded databases
  both produced 11 QDL tables and identical schema SHA; second apply and legacy
  row preservation PASS.
- Three API app replicas instantiated with zero live-ingestion ownership and a
  passive external stream status view.
- Bounded read-only smoke against the unchanged running V1 service: 7/7 PASS
  across health, Binance latest/history, OKX history and VN preload/quote.

## Runtime Impact

- No running data-layer/Redis container was recreated or restarted.
- No production-like PostgreSQL or Redis state was mutated.
- No V1 route, Redis payload/channel or SDK surface changed according to Phase 0
  golden artifacts.
- New Compose role services are profile-gated and were not started.

## Deferred By Design

- PostgreSQL control tables are not connected to the authoritative runtime yet.
- A Kafka-compatible broker or transitional durable bridge is not provisioned;
  that remains the explicit Phase 2 decision gate.
- OKX live/historical adapter certification and feed activation remain Phase 3+.
- The first schema bootstrap uses a checked-in Buf binary baseline. After this
  branch lands on the protected base branch, CI can additionally compare against
  that Git ref without changing the schema contract.
