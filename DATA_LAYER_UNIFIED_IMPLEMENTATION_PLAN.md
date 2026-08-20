# Quant Data Layer Unified Implementation Plan

> **Status:** Phases 0-5 are complete; Phase 6 implementation and shadow certification pass, while production authority remains `NO-GO` on explicit infrastructure gates. Phase 7 is complete with a protected read-only `BETA-GO`; Phase 8 is complete with an immutable, signed, multi-venue Rust realtime-core candidate fenced to `RUST_SHADOW`; Phase 9.0-A and 9.0-B are complete in isolation; Phase 9.0-C is `COMPLETE_CONTROL_PLANE / NO_GO_EXTERNAL`; Phase 9.1 is `COMPLETE_IMPLEMENTATION / CANARY_NOT_AUTHORIZED`; Phase 9.2 is `COMPLETE_IMPLEMENTATION / PRIMARY_NOT_AUTHORIZED`; Phase 9.3 is `COMPLETE_CONTROL_PLANE / PRODUCTION_HOLD_NOT_STARTED` after isolated hold/closure/expansion governance certification. Authority promotion, production hold/closure and every expansion remain blocked on explicit production infrastructure, real canary/primary evidence and exact-slice approval gates. V1 remains authoritative and no runtime cutover has started.
> **Working branch:** `feat/v2-stable-rust-binance-okx`, based on `dev`; Phase B artifact certification is complete while the overall multi-venue conclusion remains `PARTIAL_EXTERNAL` for DNSE. No push, merge or authority cutover is implied.
> **Detailed architecture:** [Fund-grade architecture and migration guide](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md)
> **OKX V5 market-data specification:** [OKX Market Data V5 implementation guide](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md)
> **Compatibility boundary:** Existing `/v1`, SDK v1, Redis keys and Redis Pub/Sub remain supported until a governed per-consumer sunset.

## 1. How To Use This Tracker

This file is the tracked implementation journal for the fund-grade data-layer upgrade. The architecture guide owns detailed design and rationale; this file owns execution order, status, evidence, decisions and remaining debt.

Every phase must keep the following fields current:

- **Goal:** the measurable outcome, not merely files to create.
- **Guide index:** links to the detailed architecture sections that govern implementation.
- **To do:** approved scope for the phase.
- **Completed:** exact code, configuration, migration and operational work actually performed.
- **Verification:** commands, fixtures, data comparisons, latency/capacity results and cleanup evidence that actually ran.
- **Technical debt / decision gate:** only unresolved matters that require a user, infrastructure, cost or business-semantics decision. In-scope defects are fixed before phase closure, not relabeled as debt.
- **Rollback:** a tested path back to the last authoritative producer/read path.

Phase status is one of `PLANNED`, `IN_PROGRESS`, `BLOCKED`, `COMPLETE`. A phase is not `COMPLETE` while a required gate is untested or while test artifacts affect live data.

## 2. Program-Wide Rules And Invariants

These rules apply to all phases.

1. **No big-bang cutover.** Use strangler migration, shadow reads/writes, parity reports and per-feed authority flags as defined in [Sections 30-33](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#30-migration-strategy-no-big-bang-rewrite).
2. **V1 remains stable.** Existing alpha and Trading System consumers must not change merely because internal transport, schemas or implementation language changes. Protect `/v1`, SDK v1 and legacy Redis payloads with golden tests.
3. **One canonical contract across languages.** Python and Rust generate types from the same Protobuf/OpenAPI sources. External projects are behavioral references, not naming authorities for established data-layer fields.
4. **Correctness before throughput.** Validate exact decimal representation, venue/market/instrument identity, source/event/receive timestamps, sequence semantics, bar closure, provenance and quality flags before accepting performance results.
5. **No silent loss.** Trade and order-book delta queues must never use “drop oldest and continue healthy”. Backpressure, spool, reconnect, resnapshot and feed-state transitions must be explicit and observable.
6. **Demand controls cost, capability controls architecture.** Unused Spot feeds are disabled by configuration and zero-demand evidence, not deleted. A new consumer can re-enable them through a reviewed `DataRequirement` without code changes.
7. **Multi-venue by capability.** Core code must not route with growing `if venue == ...` branches. Market types include spot, equity, perpetual, dated futures, option and index/reference products. Future Deribit support must fit the existing instrument/event/order-book contracts.
8. **Rust is promoted by evidence.** Rust owns approved hot paths only after shadow parity, replay determinism, failure recovery and capacity gates pass. Python remains the control/query/history authority where it is the better fit.
9. **Tests are isolated and cleaned.** Use deterministic venue fixtures and disposable Compose project names, topics, Redis prefixes, PostgreSQL schemas and object-store buckets. Do not flush shared Redis, alter production parquet, or reuse live consumer groups. Remove test resources and report cleanup after each phase.
10. **Real-provider tests are bounded.** Read-only venue smoke tests use small symbol/feed sets, respect rate limits and never seed/bypass missing data. They supplement deterministic tests; they do not replace them.
11. **Evidence is concise and durable.** Store checksums, counts, latency percentiles, gap/duplicate results and compact report files. Do not paste unbounded logs into this plan.
12. **Commit discipline.** Commit one coherent, tested implementation slice at a time with the configured BobbyAxerol identity. Do not bundle unrelated `symbols.json`, local data, logs or caches. Open PRs into `dev`; promote to `main` only through release gates.
13. **New debt is governed.** Fix in-scope bugs during the phase. Stop and request direction only for a material architecture, infrastructure-cost, licensing, source-authority or public-contract decision.
14. **Provider guides refine, not fork, the platform.** OKX `P0-P4` work follows the seven-phase mapping in its guide. Public V2 remains provider-neutral; provider routes are authenticated diagnostics/control-plane only. Provider docs and changelog are re-verified for every touched endpoint/channel and the verification date is recorded.
15. **Running consumers are protected by default.** Development, fixtures, load tests and shadow producers use isolated process/container names, ports, Redis prefixes, consumer groups, schemas and output paths. No phase may restart, reconfigure, flush, prune, overwrite or redirect the running producer/consumer path unless an approved cutover step explicitly names the blast radius and rollback.
16. **Source changes require a coordinated release plan.** A new producer/source remains shadow until contract, domain parity, freshness, recovery and capacity gates pass. Authority changes use immutable artifacts and one versioned deployment manifest so every owner for the selected feed slice changes consistently; partial mixed ownership is prohibited. Consumer migration remains per declared manifest and does not require a big-bang V1 sunset.
17. **Testing covers behavior, not only availability.** Each slice runs applicable unit, contract/golden, deterministic replay, domain-oracle, integration, failure/reconnect, compatibility, resource/capacity and bounded real-provider checks. Reports state cases run, exact results, untested cases and cleanup evidence. A healthy HTTP response alone is never phase acceptance.
18. **Correctness, stability and scalability are release gates.** No optimization is promoted if it changes identity, units, timestamps, ordering, bar closure, source authority or legacy behavior without an approved versioned contract. No benchmark is accepted without zero unexplained loss/duplicate/gap and bounded CPU, memory, disk, queue and lag under measured load plus headroom.
19. **Production data is provider-authentic.** Production and shadow ingestion
    paths may only publish bytes received from an approved real venue/provider or
    replay those previously durably captured bytes. They must never fabricate,
    seed, interpolate or silently substitute market events. Synthetic, generated
    and simulator payloads are restricted to isolated tests and are marked as
    test provenance. Bounded read-only provider smoke is mandatory before a feed
    implementation is frozen; fixtures remain the deterministic failure oracle,
    never evidence that a live source works.
20. **Public beta is not source-authority promotion.** V2 beta data-plane routes
    are versioned, authenticated, rate-limited and read-only. Control mutations
    remain on an internal network. V1 remains authoritative and available as the
    rollback path until an independently approved feed-slice cutover.
21. **Implementation language stays behind the contract.** Rust may replace a
    Python hot path only behind the existing canonical Protobuf, V2 API/SDK and
    compatibility boundaries. Rust implementation names must not leak into
    public schemas or force alpha/Trading System consumers to change.
22. **Rust replaces core paths by evidence, not aspiration.** Each feed moves
    `PYTHON_PRIMARY -> RUST_SHADOW -> RUST_CANARY -> RUST_PRIMARY` independently.
    Exact domain parity, real-provider shadow evidence, replay determinism,
    reconnect/gap recovery, bounded resources and tested rollback are mandatory.
    An unexplained mismatch blocks promotion regardless of throughput gain.
23. **Python remains the outer platform layer.** Python continues to own REST and
    gRPC query/control surfaces, SDK/facades, consumer requirements, historical
    orchestration, reconciliation, operational tooling and low-rate adapters
    unless profiling plus a separate approval demonstrates a material benefit.
    Rust targets venue ingestion, native decoding, canonicalization,
    ordering/dedup/gap state, realtime books/bars, durable publishing and other
    measured hot paths.
24. **Every venue converges on one Rust core.** Binance is the first real
    vertical slice, not a separate core or a Binance-only target. Binance, OKX,
    DNSE/VN, future Deribit and other adapters must implement the same capability
    and canonical-core traits. A venue whose network/SDK edge remains Python
    publishes an authenticated raw provider envelope into the Rust core rather
    than maintaining a second Python canonical/quality implementation.

25. **Data-plane identity is application-enforced and consumer-bound.** Gateway
    authentication is not the sole trust boundary. REST dependencies and gRPC
    interceptors verify short-lived workload identity, audience, issuer,
    environment and scopes inside the V2 application. The authenticated subject
    resolves to one registered consumer manifest; a caller-supplied
    `consumer_id`, purpose, grade or execution flag cannot elevate entitlement.
26. **Public V2 payloads are typed and closed.** A public beta response or SDK
    model must use feed-discriminated payload types with `extra = forbid`,
    generated enums and exact decimal semantics. `dict[str, Any]`, unversioned
    provider payloads and ambiguous string enums are restricted to authenticated
    diagnostics. `UNSPECIFIED` enum values fail validation rather than selecting
    a production default.
27. **Delivery policy follows event lifecycle, not only feed name.** Trade,
    order-book delta/snapshot/reset, final bar, bar revision/correction,
    source-authority transition and quality-state transition are lossless
    canonical events. BBO, ticker and explicitly marked in-progress bar updates
    may be coalesced only by a deterministic lifecycle-aware key. A final or
    revised bar may never be overwritten by an in-progress update.
28. **Readiness is measured, not declared.** Liveness, process readiness,
    dependency readiness, data readiness, authority readiness and per-consumer
    eligibility are separate states. A route or runtime role cannot return
    `ready` from a phase constant or static manifest when its broker, query
    store, cursor signer, catalog, source policy, auth state or projector is
    unavailable or outside the approved lag/freshness bound.
29. **Cursor claims bind the complete recovery contract.** Signed cursors bind
    environment, authenticated consumer, requirement digest, stream/partition,
    snapshot watermark, schema major, partition epoch, source-policy revision,
    instrument-catalog revision and expiry. A cursor from a previous
    repartition, policy revision, environment or consumer is rejected
    deterministically.
30. **Snapshots and checkpoints are immutable facts.** SDKs and services must
    not fabricate a placeholder snapshot ID, cursor or watermark. Missing
    immutable snapshot identity or a signed resume cursor is a fail-closed
    contract error. Execution-grade consumers acknowledge only a contiguous
    applied range and persist checkpoints through a consumer-owned durable or
    transactional adapter.
31. **Raw lineage preserves exact provider evidence.** The raw-provider envelope
    stores the exact received frame bytes, the declared transport transform,
    source session and connection generation. `raw_frame_hash` covers exact
    bytes at the declared capture boundary; `canonical_payload_hash` covers the
    deterministic canonical representation. Re-serialized JSON is not evidence
    of byte-for-byte source fidelity.
32. **Replicated durability precedes authority.** A local SQLite WAL, local file
    or single-node Redis Stream may support bounded shadow certification, but no
    canonical feed becomes the sole production authority until replicated
    durable transport, acknowledgements, retention, failover, restore, quotas,
    ACLs and cursor recovery are proven on the real deployment topology.
33. **Authority is persistent, compare-and-swap and sink-fenced.** Every
    venue/market/product/feed/partition slice has one durable authority record,
    monotonically increasing revision and lease epoch. Producers include owner,
    slice, authority revision and lease epoch in publication metadata. The
    authoritative sink/projector rejects stale or non-owner writes; producer-side
    self-checks alone are insufficient against zombie writers.
34. **Partition ownership is stable and versioned.** Canary selection and
    subscription sharding operate on stable instrument or durable-partition
    identity, never per event. Rendezvous/consistent hashing or a persisted
    assignment table limits churn. Any partition-count or hash-function change
    creates a new partition-plan epoch with an explicit handoff watermark.
35. **Corrections and revisions are append-only domain events.** Trade busts,
    trade corrections, bar revisions, instrument metadata revisions and source
    authority revisions reference the superseded event or revision. Historical
    state is rebuilt into a new snapshot; canonical history is not silently
    mutated in place.
36. **Capacity gates are machine-evaluable.** A report is `PASS` only when every
    configured throughput, bytes/s, latency, loss, duplicate, gap, queue, spool,
    CPU, RSS, disk and recovery criterion passes exactly or an explicit approved
    tolerance is recorded. Tools may not label a result `PASS` when the measured
    target is missed implicitly.
37. **An in-process lock is not a distributed handoff proof.** A local lock may
    protect one replica, but multi-replica replay-to-live continuity requires a
    broker-native cursor/barrier or one active fenced gateway per partition.
    Remote durable I/O is not held under one global event-loop lock.
38. **Execution eligibility is derived server-side.** Data grade, authority,
    source policy, freshness, completeness, open-gap state and consumer
    entitlement jointly determine execution eligibility. A request header or
    payload field cannot assert that a response is execution-grade.
39. **Disaster recovery precedes execution dependency.** A critical alpha or
    execution service may not depend solely on V2 until broker failover,
    PostgreSQL control-state restore, cursor-key rotation, object-store/PITR,
    Redis/projector rebuild and authority reconstruction from the audit log have
    passed the approved recovery objectives.
40. **The main plan is a transactional implementation journal.** Before code,
    record the approved phase/scope, guide links, invariants, test gates and
    rollback here. After every coherent tested slice, record exact completion,
    verification, cleanup, decision gates and debt here in the same commit. A
    phase cannot be reported complete while this tracker or its governing guide
    disagrees with code/evidence.
41. **Scope and approval are explicit.** A discussion/evaluation request causes
    no mutation. Newly discovered work outside the approved scope is reported
    with impact and recommendation before implementation. Restart, cutover,
    authority change, destructive cleanup, push and merge require the user's
    explicit approval for that action.
42. **Final reporting is evidence-bound.** Every completion report names the
    plan status, tests actually run with pass/fail/skip counts, untested/external
    gates, runtime impact, cleanup, commit/branch and push/merge state. Local or
    same-host proof is never upgraded linguistically into production evidence.

## 3. Phase Summary

| Phase | Name | Primary outcome | Status |
|---:|---|---|---|
| 0 | Containment, inventory and measurable baseline | Freeze compatibility, stop unused cost and establish reproducible truth | `COMPLETE` |
| 1 | Canonical contracts, identity and runtime boundaries | Stable venue-neutral domain plus separately scalable Python roles | `COMPLETE (DARK)` |
| 2 | Durability contract, bridge and Rust foundation | Replayable transport boundary and deterministic cross-language core without premature broker cutover | `COMPLETE (DARK)` |
| 3 | Scalable ingestion and compatibility projection | Demand-driven Rust hot path with legacy V1/Redis parity | `COMPLETE (FROZEN SHADOW)` |
| 4 | Quality, history, replay and gap-free handoff | Certified data products from warmup through live recovery | `COMPLETE (FROZEN SHADOW)` |
| 4.5 | V2 readiness and debt closure | Freeze query semantics and remove correctness/security ambiguity before endpoint work | `COMPLETE (FROZEN DARK)` |
| 5 | V2 API/SDK and controlled consumer migration | Stable snapshot/cursor interface without breaking existing consumers | `COMPLETE (FROZEN SHADOW)` |
| 6 | Production certification and multi-venue readiness | HA/security/SLO gates, controlled authority cutover and adapter scalability | `BLOCKED (SHADOW PASS; PRIMARY NO-GO)` |
| 7 | V2 public beta and consumer canary | Publish a protected read-only V2 surface and validate real consumer behavior without changing authority | `COMPLETE (BETA-GO READ-ONLY)` |
| 8 | Multi-venue Rust realtime core and reference slice | Build one provider-neutral Rust core for all venues and prove it with cross-venue conformance plus a Binance USD-M reference shadow | `COMPLETE (8.0-8.3; RUST_SHADOW only)` |
| 9 | Rust core canary and progressive replacement | Promote certified Rust feed slices while Python remains the outer platform and rollback boundary | `PLANNED` |

## 4. Phase 0 - Containment, Inventory And Measurable Baseline

**Status:** `COMPLETE`

### Goal

Create a trustworthy, reproducible baseline before changing transport or schemas; freeze V1 behavior; identify every real consumer; and stop broad Spot streaming only when no active/declared consumer requires it.

### Guide Index

- [Phase 0 detailed guide index: Sections 2, 4, 24, 27, 30-31 and Epic E0](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-0)
- [OKX Phase 0 workstream: compatibility inventory, fixtures, profiles and known pagination defect](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-0)

### To Do

- Inventory all REST routes, request/response examples, SDK public methods, Redis keys/channels, source/market semantics and known consumers in alpha, Trading System and diagnostics.
- Snapshot V1 OpenAPI and legacy Redis payloads as golden compatibility artifacts. Record owner, criticality, freshness, warmup and fallback requirements per consumer.
- Measure current host/runtime baseline: enabled feeds, shard count, messages/s, queue depth/drop delta, reconnects, Redis commands/memory/network, API latency, CPU/RSS, parquet coverage and provider REST pressure.
- Add a deterministic baseline corpus for Binance USD-M, Binance Spot, OKX and VN payloads, including malformed, duplicate, out-of-order, reconnect and market-closed cases.
- For OKX, freeze `/v1/crypto/ohlcv/okx/...`/SDK output, capture exact native `instId` and REST/WS profile fixtures, and characterize the current `after`/`before` defect before correcting it in an approved implementation slice.
- Separate `liveness`, service `readiness`, feed readiness and execution eligibility in reports without changing V1 response shape.
- Add explicit source/market feature flags and validate configuration at startup. Remove duplicate/hardcoded universe overrides from runtime ownership.
- Prove Spot demand from declaration plus telemetry. If demand is zero, set Spot ingestion disabled by default while preserving Spot REST wrappers, adapter code and a tested re-enable path.
- Establish test-resource namespaces and cleanup commands for broker topics, Redis, PostgreSQL and object storage before those dependencies are introduced.
- Record baseline budgets and acceptance thresholds used by later phases. Thresholds must reflect measured load plus agreed headroom, not arbitrary aspirational numbers.

### Verification And Exit Gate

- Golden `/v1`, SDK and Redis compatibility tests pass against the current implementation.
- Consumer inventory covers every observed and declared caller; unknown callers are listed and block destructive contract changes.
- Two bounded runtime windows show demand telemetry, feed freshness, queue/drop deltas, reconnect behavior and resource usage.
- Spot-off configuration starts without Spot WebSocket shards, leaves USD-M/VN demanded feeds healthy, reduces resource use measurably and can be rolled back with one configuration change.
- Existing unit suite, Docker integration suite and read-only sampled provider smoke pass; generated test state is cleaned.
- Baseline report and machine-readable artifacts are committed without credentials or raw unbounded logs.

### Completed

- Added validated source ownership configuration. The new artifact defaults to Binance USD-M trade+kline; Spot, DNSE, vnstock and preload ownership remain independently configurable. The running service was not restarted.
- Frozen V1 OpenAPI, route/method/name inventory, SDK signatures and Redis payload shapes under [`contracts/v1`](contracts/v1).
- Added a read-only audit tool, bounded provider smoke and deterministic Binance/OKX/VN/malformed fixture corpus.
- Inventoried the full workspace, Trading System and active migrated alpha tree without reading generated logs/state/data.
- Captured two bounded runtime windows and a resource/topology baseline. Details and exact artifacts are in the [Phase 0 baseline report](upgrade/evidence/PHASE0_BASELINE_REPORT.md).
- Detected and fixed provider-scoped kline demand health correlation without changing V1 Redis payloads (`965275e`). Runtime verification remains part of the coordinated immutable-image deployment because Phase 0 did not restart the live process.
- Measured current source topology: 44 full Binance shards versus 16 USD-M-only shards, a projected 63.636% connection reduction. No active demand lease required Spot during observation.

### Technical Debt / Decision Gate

- Phase 1 must turn measured load into explicit SLO/headroom budgets. Baseline observed approximately 4.3k-4.5k Redis commands/s and 1.98-2.04 MB/s input during the two ten-second windows.
- The running image still has four Binance sources because no cutover was allowed. Deploying the USD-M-only default requires immutable-image V1/Redis shadow parity and coordinated recreation with the documented source-list rollback.
- Legacy `stream:trade:{symbol}` source authority must be frozen to USD-M (or explicitly versioned) before Spot producer removal. Active alphas had no direct-provider usage, but workspace-wide legacy/reference files remain and are tracked in inventory evidence.
- Existing queue code has 3,790,249 cumulative drops. Recent drops were zero in final windows, but Phases 2-3 must replace feed-agnostic drop/coalesce behavior before trade/book delta certification.
- OKX `after`/`before` pagination remains a characterized defect in the compatibility facade; it is corrected under the async adapter/history implementation with V1 golden protection, not silently in Phase 0.
- Existing `websockets.legacy` and `InvalidStatusCode` deprecation warnings must be removed during the scalable adapter implementation.

### Rollback

- Restore the previous feed-enable configuration and immutable image. No schema, topic or consumer cutover occurs in Phase 0.

## 5. Phase 1 - Canonical Contracts, Identity And Runtime Boundaries

**Status:** `COMPLETE (DARK / NO V1 CUTOVER)`

### Goal

Define one precise, venue-neutral data domain and split the combined process into independently scalable roles while preserving all V1 behavior.

### Guide Index

- [Phase 1 detailed guide index: Sections 8-10, 12, 20-23, P1 and Epics E1-E3](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-1)
- [OKX Phase 1 workstream: canonical identity, authoritative registry and capability profiles](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-1)

### To Do

- Create versioned Protobuf packages and Buf configuration for common envelope, instrument, trade, BBO/quote, bar, order-book snapshot/delta, quality and feed-state events.
- Preserve price/quantity as exact decimal coefficient/scale or venue-native string; prohibit canonical binary float.
- Implement canonical `instrument_uid`, aliases, venue, source/provider, market/product type, contract expiry, strike, option type, multiplier, tick/lot metadata and trading calendar/session model.
- Define capability descriptors for REST history, trades, BBO, bars, L2 snapshots/deltas, sequence/checksum, resubscribe/resnapshot and source authority. Ensure options and dated contracts need no core schema redesign.
- Model OKX Spot, Swap, dated Futures, Options and Event contracts from `/public/instruments`; never derive a derivatives `instId` with string heuristics. Region/entity/tier availability is explicit capability metadata.
- Build PostgreSQL migrations for instrument master, aliases, source policy, subscription registry, config revisions, leases/fencing and job state. Do not store the tick stream in PostgreSQL.
- Extract deployable Python roles (`api`, `control`, `history`, compatibility facade) from the current combined lifespan. A role flag must have one owner and fail startup on contradictory ownership.
- Keep legacy imports and application entrypoints through explicit facades. API replicas must not create venue subscriptions.
- Write ADRs for contract representation, instrument identity, role boundaries, transport decision inputs and V1 compatibility ownership.

### Verification And Exit Gate

- Buf lint/generation and breaking checks pass; generated Python and Rust types encode/decode the same golden bytes.
- Exact decimal, timestamp, event-ID and identity collision suites pass for spot, perpetual, dated futures, VN derivative and option fixtures.
- Role-topology integration test proves scaling API replicas does not increase provider connections or duplicate publication.
- V1 OpenAPI/Redis/SDK golden artifacts are byte/semantic compatible with Phase 0.
- Schema migrations are forward-only, idempotent in clean and existing DB cases, and have backup/rollback evidence.
- Static dependency tests prevent API/control/history modules from importing and starting ingestion ownership accidentally.

### Completed

- Added canonical Protobuf packages, pinned Buf code generation and a frozen
  Phase 1 breaking baseline. Generated Python/Rust models share one contract;
  exact decimal, nanosecond timestamp, large native ID and deterministic golden
  binary tests pass.
- Added UUIDv5 canonical instrument identity, temporal aliases, metadata
  revisions, source/venue separation, session calendars and capability profiles.
  OKX Spot/Swap/Futures/Option/Event records are parsed from authoritative
  `/public/instruments` fields without symbol heuristics.
- Added forward-only PostgreSQL migrations for instrument/control metadata,
  source policies, subscriptions, revisions/audit, leases/fencing and jobs. Clean
  and legacy-seeded disposable databases pass second-apply idempotence with
  identical QDL schema hashes; no tick-event table was introduced.
- Added dark `api`, `control` and `history` entrypoints with fail-closed role
  ownership. Three API replicas instantiate no venue-loop owner. Existing
  `app.main:app` remains the sole V1 combined ingestion/projection authority.
- Added ADRs `0001`-`0005`, contract CI, Compose role profile and Phase 1 evidence.
- Verification: Buf format/lint/build/generate/breaking PASS; Python/Rust golden
  parity PASS; migration smoke PASS; full application regression `125 passed`;
  both environment-gated Redis integration tests separately PASS on disposable
  Redis; frozen V1 OpenAPI/Redis/SDK artifacts PASS.
- Evidence: [Phase 1 report](upgrade/evidence/PHASE1_IMPLEMENTATION_REPORT.md),
  [contract gate](upgrade/evidence/phase1-contract-gate.json), and
  [migration smoke](upgrade/evidence/phase1-migration-smoke.json). The unchanged
  live V1 path also passed [7/7 bounded read-only checks](upgrade/evidence/phase1-live-v1-smoke.json)
  with both running containers still at restart count zero.

### Technical Debt / Decision Gate

- Contract naming or semantics that affect public V2 behavior require explicit approval before schema freeze. Pure implementation details do not.
- Phase 1 schema bootstrap uses a checked-in binary breaking baseline. Once this
  branch lands on the protected base branch, add Git-ref breaking comparison as
  a second gate; do not replace the immutable initial baseline.
- Control tables and separated roles remain dark. Connecting them to authority,
  adding durable transport, or starting role services belongs to later approved
  phases and requires a coordinated immutable-image deployment.

### Rollback

- New roles and schemas remain dark. Existing combined runtime stays authoritative until a later per-feed cutover.

## 6. Phase 2 - Durability Contract, Bridge And Rust Foundation

**Status:** `COMPLETE (DARK / NO V1 CUTOVER)`

### Goal

Introduce a transport-neutral replay contract, a bounded durable bridge and a deterministic Rust data-plane foundation without making Rust or a Kafka-compatible broker authoritative prematurely.

### Guide Index

- [Phase 2 detailed guide index: Sections 6-7, 11, 28-29 and Epics E4-E5](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-2)
- [OKX Phase 2 workstream: durable fixtures, simulator and cross-language deterministic parity](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-2)

### To Do

- Define transport-neutral `EventSink`, `EventSource`, cursor/checkpoint, event-ID, retry and replay contracts before selecting infrastructure. Application and public contracts must not expose Redis- or Kafka-specific identifiers.
- Implement a bounded bridge for the first isolated feed slice using a dedicated durable Redis Streams instance or local WAL/spool. It must have persistence, `noeviction`, strict memory/disk bounds, trimming, cursor-expiry behavior, monitoring and a tested cleanup/sunset path; the existing ephemeral `redis_marketdata` is forbidden for this role.
- Measure bridge throughput, replay horizon, consumer-group count, lag, memory/disk amplification and operational recovery. Provision a Kafka-compatible broker only when the promotion gate demonstrates a real need; keep producer/consumer contracts Kafka-compatible from the start.
- When the Kafka gate is approved, provision isolated raw, canonical, quality and DLQ/quarantine topics with explicit partition keys, retention, replication, quotas and ACLs. A single-node broker is a replay/durability step, not an HA claim.
- Implement idempotent publication, deterministic event IDs, retry classification, bounded local spool and feed-state transition when durable commit is unavailable.
- Create a Cargo workspace for contract types, decimal/time utilities, instrument identity, event IDs, adapter traits, broker client, telemetry and replay test tools.
- Implement cross-language golden codecs/checksums and a deterministic venue simulator reusable by Python and Rust.
- Include OKX REST envelopes and WS book snapshot/update/gap/keepalive/maintenance-reset/connection-generation frames in the same durable simulator; no separate OKX event backbone is introduced.
- Build a shadow raw-to-canonical pipeline for one small, demand-backed slice. The provisional slice is selected Binance USD-M trade symbols, not broad Spot.
- Keep Redis as latest-state/legacy projection only; prove it can be rebuilt from canonical events.
- Add CI gates for formatting, lint, tests, unsafe-code policy, dependency/license/security audits, Buf compatibility and reproducible container artifacts.

### Verification And Exit Gate

- Restart/recovery tests for the selected bridge lose no acknowledged canonical events and do not expose non-idempotent duplicate state. If Kafka is promoted in this phase, broker restart/failover tests are additionally mandatory.
- Same raw fixtures and config/normalizer revision produce identical canonical checksums across repeated replay and across Python/Rust reference implementations.
- Slow/down broker tests prove spool bounds, backpressure and `DEGRADED/BLOCKED` semantics; no silent queue drop is allowed.
- Redis flush/restart followed by replay rebuilds the same latest-state checksum and legacy projection fixture.
- Performance baseline reports p50/p95/p99/p99.9, throughput, CPU, RSS, allocation and disk/network amplification using semantically identical parsing.
- All ephemeral topics, groups, Redis prefixes and volumes created by tests are removed after evidence capture.

### Completed

- Defined portable event sink/source, batch append, logical cursor/checkpoint,
  deterministic event-ID, retry and partition contracts without exposing a
  Redis/Kafka offset in application contracts.
- Implemented a bounded local SQLite WAL bridge for the selected BTCUSDT and
  ETHUSDT Binance USD-M trade shadow slice. It uses `synchronous=FULL`, atomic
  batches, raw-first durable acceptance, payload checksums, corruption checks,
  monotonic offsets, consumer TTL and strict logical/physical metadata bounds.
- Implemented restartable raw-to-canonical processing, idempotent compatibility
  projection and Redis flush/restart/replay rebuild. All Redis writes use an
  isolated `shadow:qdl:v2` namespace; legacy production publication is disabled.
- Added Python/Rust exact-decimal golden parity for Binance and OKX plus a
  deterministic OKX protocol simulator covering sequence gaps, stale connection
  generations, keepalive and maintenance reset.
- Added a Rust workspace and immutable replay tool with pinned compiler/base
  image digests, `unsafe` forbidden, deterministic backoff/rate-limit/fencing
  primitives and CI format/clippy/test/dependency policy gates.
- Measured 10,000 durable events over 10 partitions and 8 consumer groups:
  1,470.85 events/s append, 8,211.74 events/s replay, p99 62.28 ms per fsynced
  batch, 33,004 KiB max RSS and 2.072x disk amplification. All configured gates
  passed for the small shadow slice.
- Verification: full Python regression 146 run (143 pass, 3 environment-gated skips);
  Phase 2 focused tests 20 PASS; Rust 9 PASS; Buf compatibility PASS; isolated
  Redis recovery PASS; Rust dependency/license/advisory policy PASS; two
  immutable builds produced the same image ID.
- Read-only live smoke loaded BTCUSDT/ETHUSDT through unchanged V1, produced two
  raw and two canonical local records, and made zero production writes. Running
  Data Layer/Redis restart counts remained zero and all Phase 2 resources were
  cleaned.
- Evidence: [Phase 2 implementation report](upgrade/evidence/PHASE2_IMPLEMENTATION_REPORT.md),
  [verification summary](upgrade/evidence/phase2-verification.json),
  [performance report](upgrade/evidence/phase2-performance.json), and
  [live shadow smoke](upgrade/evidence/phase2-live-shadow-smoke.json).

### Technical Debt / Decision Gate

- Kafka promotion requires explicit approval after measured evidence shows at least one material trigger: multiple independent replay consumers, replay horizon beyond the bounded bridge, raw trade/book volume exceeding its safe budget, multi-node HA requirement, or unacceptable bridge lag/recovery time.
- Until that gate passes, the dedicated bridge/local spool is transitional infrastructure with a declared limit and sunset path, not the canonical long-term target.
- The bridge is a single-host, non-HA shadow mechanism and is not approved for
  broad-universe trades or order-book deltas. Phase 3 must produce sustained
  demand-backed parity/capacity evidence before any feed authority changes.
- Existing Python V1/build dependencies have advisory findings in the current
  image. Phase 2 added no Python dependency; a compatibility-tested dependency
  refresh remains mandatory before production promotion. Rust Phase 2
  dependencies pass advisory, license, ban and source policy checks.
- OpenTelemetry export and multi-node durable-broker failover are later
  certification work; Phase 2 supplies the stable interfaces, not an HA claim.

### Rollback

- Stop shadow Rust/broker services and remove their isolated resources. Existing Python-to-Redis path remains authoritative.

## 7. Phase 3 - Scalable Ingestion And Compatibility Projection

**Status:** `COMPLETE (FROZEN SHADOW)`

### Goal

Run high-throughput ingestion and canonical projection with explicit shard ownership, demand-driven subscriptions and V1-compatible outputs, initially in shadow and then per-feed authority slices.

### Guide Index

- [Phase 3 detailed guide index: Sections 12-14, 20, 23, 37, P2 and Epics E6-E8](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-3)
- [OKX Phase 3 workstream: async ingestion, public/business WS and order-book state machine](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-3)

### To Do

- Implement Rust Binance USD-M adapter for demanded trade/BBO/bar feeds using capability-based interfaces, validated instrument discovery and exact native sequence/time/decimal extraction.
- Replace configured broad-universe ownership with declared baseline plus TTL demand leases; implement lease/fencing epochs so only one active owner publishes a shard.
- Keep Spot disabled when registry refcount is zero. Add conformance tests showing Spot can be enabled without changing code or namespaces.
- Implement bounded queues by feed class: lossless backpressure/spool for trade/book delta; explicit coalescing only for latest-state-safe projections.
- Add reconnect/resubscribe, rate-limit budget, jittered backoff, heartbeat, gap detection and REST snapshot/resync appropriate to each feed.
- Implement OKX endpoint-bucketed async REST and public/business WS supervisors. True book-sequence gaps invalidate executable state and require a fresh WS snapshot; REST `/books` must not be used as a fictitious delta bridge.
- Implement canonical Redis projector plus V1 compatibility projector with checkpointed idempotence, versioned keys and legacy payload snapshots.
- Shadow-compare Python and Rust on event count, IDs, price, quantity, side, timestamps, sequence, bar closure and quality flags before authority changes.
- Provide adapter extension fixtures for OKX and a synthetic Deribit-style option/order-book source to prove capability boundaries without claiming those venues production-ready.

### Verification And Exit Gate

- Adapter conformance suite passes normal, malformed, duplicate, out-of-order, gap, reconnect storm, delist and graceful shutdown cases.
- Two-owner fencing test proves stale owner cannot publish after lease loss.
- Burst and sustained load at measured universe size plus headroom produces zero silent loss, bounded memory and controlled broker/Redis lag.
- Python-versus-Rust shadow reports meet exact-field parity; every allowed divergence is versioned and approved.
- Legacy V1/Redis consumers observe no shape, namespace or source-authority regression.
- One low-risk feed slice can switch authority and roll back without restarting unrelated venue/feed shards.

### Completed

- Added deterministic demand registry, TTL leases, no-truncation shard planning,
  PostgreSQL lease ownership and monotonically increasing fencing epochs.
- Added feed-class queue policy: trade/book are lossless and block under
  pressure; BBO/bar may coalesce only the same pending latest-state key. Spot
  with zero declared demand creates zero shards but uses the same contract when
  enabled later.
- Added a provider-authentic Binance USD-M adapter with exchange-info
  validation, exact tick/step metadata, demanded-only WebSocket streams,
  reconnect/backoff and interruptible graceful shutdown. Added a bounded Rust
  Binance trade hot path that writes raw provider frames before canonical bytes
  to an fsynced shadow WAL.
- Added an OKX V5 async REST client with endpoint buckets and retry, separate
  public/business WebSocket supervisors with acknowledgement correlation,
  heartbeat/reconnect/resubscribe and an executable book state machine. A true
  `prevSeqId/seqId` gap clears the book and requires a fresh WebSocket snapshot;
  REST `/books` is explicitly forbidden as a delta-continuity bridge.
- Added exact canonical BBO, bar and book mappings; raw-first market events;
  canonical latest-state and frozen V1 bar compatibility projection; atomic
  Redis checkpoint/lease-epoch fencing; and a per-feed authority router that
  switches `SHADOW/CANONICAL/LEGACY` without process restart.
- Added adapter capability-boundary tests for an option/order-book source.
  Deribit-shaped data is explicitly test-synthetic and cannot be certified as a
  production source.
- Added reconnect-storm and stop-interruption tests for Binance and OKX so a
  requested shutdown does not wait for the heartbeat timeout.
- Added bounded real-provider, burst/restart, sustained-load and Rust live-frame
  parity evidence. Production/shadow implementations do not import fixtures or
  simulator modules.
- Froze the Phase 3 implementation in shadow mode. Evidence and immutable
  artifact identity are recorded in
  [`PHASE3_IMPLEMENTATION_REPORT.md`](upgrade/evidence/PHASE3_IMPLEMENTATION_REPORT.md)
  and [`phase3-freeze.json`](upgrade/evidence/phase3-freeze.json). Reopening this
  phase requires a reviewed contract/ADR change and new certification evidence.

### Verification

- Full Python/V1 regression: `177` tests run, `172` passed and `5` expected
  environment-gated skips.
- Phase 3 focused adapter/control/projection/provenance suite: `29/29` passed,
  including malformed input, reconnect storm, delist/inactive symbol,
  sequence gap and graceful shutdown.
- Rust `1.82.0`: format, Clippy with warnings denied and `11/11` tests passed;
  Python/Rust trade/BBO/bar golden bytes are exact.
- Buf format/lint/breaking/codegen-diff gates passed against the frozen Phase 1
  descriptor.
- PostgreSQL disposable integration passed exclusive owner, renew, release,
  expiry takeover and stale-epoch fencing.
- Redis disposable integration passed AOF restart, deterministic flush/rebuild,
  stale-epoch rejection and live authority switch/rollback across isolated
  targets with no process restart.
- Provider-authentic read-only smoke passed Binance USD-M trade/BBO/closed bar
  and OKX trade/book snapshot/deltas, with `20` durable records and zero
  production writes.
- Rust read-only smoke consumed `3` real Binance events; every canonical byte
  matched Python and was written to isolated fsynced WAL only.
- Burst/restart gate accepted and replayed `20,000/20,000` events across `80`
  partitions at `1,343.03 events/s`, with queue rejection `0` and peak traced
  Python memory `1,965,064` bytes.
- Sustained gate held `500.70 events/s` for `5,000` events across `80`
  partitions; durable p95/p99 were `157.09/163.60 ms`, queue rejection `0`,
  records survived restart and peak traced memory was `335,239` bytes.
- Running `data_layer_service` and `redis_marketdata` stayed on their existing
  images with restart count `0`; `/v1/health` remained `ok`. No production
  Redis, PostgreSQL, Parquet, route, namespace or authority flag changed.
- Cleanup left no Phase 3 test container/network, removed the disposable Python
  test image and `709 MB` Cargo target cache, and retained only the frozen Rust
  evidence image. Existing logs, volumes and running services were untouched.

### Technical Debt / Decision Gate

- Phase 3 is certified only as a dark/shadow implementation. SQLite remains a
  single-host transitional spool, not an HA broad-universe authority. The
  Kafka-compatible promotion trigger and production authority cutover remain
  governed Phase 6 decisions.
- Binance individual trade and BBO WebSockets plus closed-bar REST were
  provider-certified from this host. The Binance USD-M kline WebSocket emitted
  no frame during bounded probes, so its parser has deterministic golden parity
  but is not falsely marked live-certified.
- OKX public `books` is certified with `seqId/prevSeqId` continuity. The current
  V5 `checksum` value is deprecated/fixed and is not represented as a valid CRC
  capability. VIP/deep-book channels remain separately uncertified.
- Historical completeness, raw retention governance, quarantine, source
  failover and gap-free warmup/live cursor handoff are Phase 4 scope. No
  generated bar, REST order-book bridge or synthetic source is used to conceal
  those pending capabilities.
- Production promotion remains deliberately unperformed under Rules 15-16.
  The authority mechanism is integration-certified, but V1 stays authoritative
  until the Phase 6 release/cutover gate is explicitly approved.

### Rollback

- No live rollback was needed because no authority changed. For an isolated
  shadow deployment, stop only its shard owner, return its per-feed authority
  to `LEGACY`, verify the fencing epoch, and delete only its namespaced shadow
  checkpoints/spool after checksum capture. Existing Python/V1 publication
  continues uninterrupted.

## 8. Phase 4 - Quality, History, Replay And Gap-Free Handoff

**Status:** `COMPLETE (FROZEN SHADOW)`

### Goal

Produce auditable, replayable and revision-aware data from raw ingestion through historical warmup and live continuation, with no undetected gap or duplicate at the handoff.

### Guide Index

- [Phase 4 detailed guide index: Sections 13-16, 38 and Epics E7/E9](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-4)
- [OKX Phase 4 workstream: historical pagination, reference data coverage and handoff](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-4)

### To Do

- Implement canonical validation levels, duplicate/out-of-order handling, sequence/gap ledger, clock-discipline metrics and failover/source-authority state machine.
- Preserve raw source payload and lineage for governed retention; quarantine malformed/unknown-instrument events instead of coercing invalid values.
- Add S3-compatible object storage and Iceberg/Parquet materialization with immutable data files, atomic snapshots, schema evolution and compaction governance.
- Migrate existing VN canonical 1m and derived intervals without fabrication; preserve session calendars, origin, revision and sparse-market semantics.
- Materialize crypto history/replay only where demand/cost evidence requires it; retain direct bounded REST wrappers for ordinary warmup.
- Implement snapshot plus durable cursor/watermark protocol, cursor persistence and reconnect replay so warmup transitions to live without gap or duplicate.
- Add historical/live reconciliation, bar revision behavior and replay determinism reports keyed by source/config/normalizer versions.
- For OKX, correct `after`/`before` traversal with exact window filtering/dedup/no-progress guards; expose explicit funding/mark/index/OI provenance and coverage. Current OI snapshots never imply pre-ingestion historical coverage.

### Verification And Exit Gate

- OHLCV oracle tests verify first/max/min/last/sum rules, market-session boundaries, DST/time zones, daily close and late/revised bars.
- Crash tests at file upload/metadata commit boundaries expose either old or new Iceberg snapshot, never partial state.
- Snapshot-cursor tests cover cold start, reconnect, cursor expiration, compaction boundary, late event and consumer restart.
- Historical/live overlap checks produce zero unexplained gaps/duplicates for certified feeds.
- Raw replay produces deterministic canonical checksums and traceable lineage to historical rows and Redis latest state.
- VN real-provider bounded checks distinguish market closed/sparse/late data from outage without synthetic seeding.

### Completed

- Phase 4 verification matrix frozen before implementation: canonical quality
  and source authority; immutable/revision-aware history; deterministic replay
  and signed cursor handoff; OKX pagination/reference coverage; VN sparse
  session migration; bounded real-provider checks; crash/restart/cleanup gates.
- Production V1, Redis, PostgreSQL and existing VN Parquet remain read-only for
  this phase. All new catalog/object-store resources use isolated shadow paths;
  no object-store or historical authority cutover is approved here.
- History shadow slice implemented: exact-decimal revision-aware bars,
  session/DST-aware OHLCV aggregation, immutable ZSTD Parquet snapshots,
  conditional atomic catalog heads, S3-compatible and PyIceberg boundaries,
  plus crash/concurrent-writer tests. The real VN migration canonicalizes the
  mixed legacy UTC/VN-naive 1m file, deduplicates only exact OHLCV duplicates
  with full source lineage, fails closed on conflicting revisions, and derives
  all larger intervals from canonical 1m without fabricating bars.
- Real read-only VN evidence: 28,196 legacy rows -> 27,955 canonical 1m rows;
  241 exact duplicate groups, zero conflicting revisions, zero fabricated rows;
  all seven interval snapshots round-trip from isolated shadow storage. See
  `upgrade/evidence/phase4-vn-shadow-migration.json`.
- Gap-free handoff/replay slice implemented with HMAC-signed, rotation-aware,
  consumer/stream/partition/snapshot-scoped cursors; snapshot watermarks,
  durable consumer checkpoints and offset-contiguity checks. Cursor tampering,
  wrong scope, expiry, compaction loss and unexplained gaps fail closed; late
  arrivals appended after the watermark remain replayable by durable offset.
- Deterministic raw replay now reports raw/canonical/lineage checksums keyed by
  source revision, normalizer version and config revision. History/live overlap
  reconciliation supports explicit higher revisions and checks only the open
  times supplied by a session calendar, preserving legitimate sparse feeds.
- OKX V5 shadow history/reference client implemented without changing the V1
  route: documented `after=oldest_ts` backward traversal, inclusive exact-window
  filtering, overlap dedup, confirmed-candle revision preference, no-progress
  failure, page/record budgets and explicit coverage. Trade, mark and index
  candles remain distinct contracts; funding retains formula/method/raw lineage;
  OI is explicitly `SNAPSHOT_ONLY` and never claims historical coverage.
- Bounded real OKX public-API evidence passed with 30 trade, 30 mark and 30 index
  1m bars, six funding records and one OI snapshot; all requested historical
  windows reported full coverage, with zero production writes. See
  `upgrade/evidence/phase4-okx-real-history.json`.
- Calendar quality policy now distinguishes closed sessions/holidays, legitimate
  sparse feeds, late bars and missing continuous bars. A real read-only DNSE
  probe for completed date 2026-08-12 returned exactly 241 VN30F1M bars over
  provider bar sessions 09:00-11:29, 13:00-14:29 and close 14:45, with zero
  missing/out-of-session/fabricated rows. The 08:45 market pre-open is retained
  as market-calendar context and is not fabricated as provider OHLCV. See
  `upgrade/evidence/phase4-dnse-provider-coverage.json`.
- Historical catalog governance now enforces additive-compatible schemas,
  records compaction operations, identifies uncommitted orphan files and
  requires exact dataset confirmation before deletion. S3 CAS uses the ETag
  from the same read (preventing a concurrent-head race) and paginates listings;
  dedicated race/immutability tests pass.
- Full-suite certification found and fixed a handwritten-package collision with
  generated protobuf namespace `qdl.quality.v1`; the implementation now lives
  under `qdl.data_quality`, leaving the stable generated contract untouched.
  Durable replay performance over 10,000 events passed all gates: 1,547.52
  append events/s, 8,538.58 replay events/s, 60.39 ms append p99 and 2.07x
  disk amplification. See `upgrade/evidence/phase4-replay-performance.json`.
- Final verification passed 36/36 focused Phase 4 tests and the full Python/V1
  regression (213 run, five expected environment skips). PostgreSQL migrations
  passed on a clean database, an existing database and a second idempotent
  apply while preserving legacy data (16 `qdl_*` tables and three lease
  functions). Rust fmt/Clippy `-D warnings`/11 tests, Buf format/lint/breaking/
  generated-code diff, and Redis AOF restart/rebuild checks also passed.
- Production compatibility remained read-only: the running V1 container kept
  restart count zero and health, VN preload and Binance USD-M OHLCV endpoints
  returned HTTP 200. No production Redis, PostgreSQL, Parquet or authority
  state was modified. The implementation report and machine-readable freeze
  are `upgrade/evidence/PHASE4_IMPLEMENTATION_REPORT.md` and
  `upgrade/evidence/phase4-freeze.json`.

### Technical Debt / Decision Gate

- Object-store/catalog deployment and retention cost require approval before production provisioning; local MinIO/catalog remains a test implementation only.
- Provider licensing constraints must be recorded before raw retention is enabled for a new source.
- Production HMAC key custody/rotation, externally exposed handoff endpoints and
  per-dataset authority promotion remain Phase 5/6 gates; test keys and local
  catalog boundaries are not production credentials or a cutover claim.
- OKX open interest remains a truthful point-in-time snapshot. Historical OI
  coverage requires a separately certified provider capability and must never
  be inferred from the current endpoint.

### Rollback

- Existing VN Parquet/read path remains available until shadow snapshot reconciliation passes. Historical authority switches per dataset, never globally.

## 8A. Phase 4.5 - V2 Readiness And Debt Closure

**Status:** `COMPLETE (FROZEN DARK)`

### Goal

Close cross-phase correctness, security and compatibility ambiguity before any
public V2 route or gRPC service is implemented. Freeze provider-neutral query
semantics over transport/storage interfaces so Phase 5 adds delivery surfaces,
not new domain behavior.

### Guide Index

- [Pre-Phase 5 readiness guide](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-4-5)
- [Stable API, SDK and consumer semantics: Sections 17-19](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#17-stable-api-design)
- [Failure semantics: Section 38](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#38-failure-semantics-exposed-to-consumers)

### To Do

- Reconcile the conflicting Section 17/38 error names into one stable internal
  taxonomy and freeze `DataRequirement`, consumer grade, completeness/partial,
  freshness, bar revision, source-policy and recovery semantics.
- Define bounded provider-neutral query/result contracts before HTTP/gRPC
  serialization. An execution-grade all-instruments requirement must fail
  closed on partial, stale, gapped, non-authoritative or unentitled data.
- Replace handoff's concrete SQLite dependency with durable transport/catalog
  protocols. Bind immutable historical snapshot cursor end to the captured live
  watermark before issuing a signed token; mismatch must fail closed and retry.
- Introduce a rotation-aware signing-key provider boundary. Unsigned logical
  cursor tokens remain internal only and must never be accepted on a public V2
  boundary.
- Add source entitlement/licensing policy with default-deny external
  redistribution and raw-history access. Capability and provenance cannot imply
  entitlement.
- Close safe legacy compatibility debt needed by certification (notably the
  deprecated WebSocket client/exception API) without changing V1 routes, SDK or
  Redis payloads.
- Resolve stale debt notes from Phases 0-4 as `CLOSED`, `SUPERSEDED` or an
  explicit Phase 6 infrastructure/authority decision. Do not hide an in-scope
  defect as a decision gate.

### Verification And Exit Gate

- Pure-domain tests cover invalid/bounded requirements, strict versus partial
  batches, all consumer grades, freshness/gap/authority decisions and licensing
  denial without provider calls.
- Handoff tests cover snapshot/watermark match, mismatch, concurrent head move,
  key rotation, unknown/retired key, tamper, wrong scope, expiration, compaction,
  restart and transport substitution without SQLite-specific API assumptions.
- Existing Phase 1-4 focused suites, full Python/V1 regression, Buf breaking and
  generated-code gates, Rust fmt/Clippy/tests, Redis rebuild and PostgreSQL
  migration smoke pass.
- A Python dependency/advisory report is recorded. Unfixed exploitable runtime
  findings block Phase 5; accepted non-runtime/tooling findings require an owner
  and expiry.
- Read-only V1 health, VN preload and Binance USD-M history smoke return the
  frozen behavior while the running service restart count remains unchanged.
- Test containers, images, databases, Redis state, object-store paths and build
  caches are isolated and removed. User-owned `symbols.json` remains untouched.

### Completed

- Cross-phase audit identified four endpoint blockers: inconsistent failure
  names, absent query/requirement domain contracts, SQLite-coupled handoff and
  in-memory-only signing/entitlement boundaries. Provider certification and HA
  authority gates were separated from endpoint semantics instead of being
  pulled prematurely into Phase 5.
- Added provider-neutral query contracts for one canonical error vocabulary,
  bounded `DataRequirement`/batch semantics, consumer grades, coverage,
  freshness/gap/recovery and bar-revision policies. Execution-grade requests
  cannot relax full coverage, authoritative source, gap or freshness gates.
- Added source licensing/entitlement contracts with explicit purpose and data
  product. Missing, expired, raw-history and external-redistribution grants
  fail closed independently of provider capability.
- Refactored handoff to portable durable-store, catalog and signing-key-provider
  protocols. `SnapshotHandoffCoordinator` issues a signed grant only when the
  immutable snapshot cursor end exactly equals the captured durable watermark;
  live events after that capture remain replayable. Static keys are test/local
  only, rotation overlap works and retired/unknown/tampered/unsigned cursors
  fail closed.
- Reconciled Section 17/38 error names and added legacy alias mapping without
  creating extra public values. CI now checks Buf against both the immutable
  Phase 1 baseline and the pull-request base branch.
- Replaced deprecated legacy WebSocket APIs with `websockets.asyncio` and
  verified one real Binance USD-M trade frame read-only. V1 route, payload,
  Redis and reconnect semantics remain unchanged.
- Closed Python runtime dependency debt: multi-stage image excludes Poetry and
  build dependencies; patched runtime dependencies audit at 0 findings across
  61 packages. Full regression exposed and fixed undeclared DNSE `msgpack`
  ownership. The tested image content size was 163,007,213 bytes versus
  394,348,787 bytes for the running image (58.66% smaller).
- Certification passed 11/11 Phase 4.5 tests, 47/47 combined Phase 4/4.5 tests
  and 224 full Python tests with five expected environment skips. Redis rebuild
  3/3, PostgreSQL clean/existing/idempotent migration, Buf gates and Rust
  fmt/Clippy/11 tests passed. The 10,000-event durability benchmark passed at
  1,432.70 append/s, 8,266.96 replay/s, 59.70 ms p99 and 2.072x disk
  amplification.
- Read-only production smoke returned HTTP 200 for health, VN preload and
  Binance USD-M history; the running V1 restart count stayed zero. No endpoint,
  producer, storage or source authority was activated. Test image, containers,
  networks, temporary reports and 520.3 MiB Cargo artifacts were removed.
- Cross-phase closure matrix, implementation details and machine-readable
  evidence are in `upgrade/evidence/PHASE45_V2_READINESS_REPORT.md` and
  `upgrade/evidence/phase45-freeze.json`.

### Technical Debt / Decision Gate

- Kafka-compatible HA promotion, production object-store/Iceberg provisioning,
  production secret backend, raw-data licensing approval and per-feed authority
  cutover remain Phase 6 decisions. Phase 4.5 must provide stable interfaces and
  fail-closed readiness for them; it must not fake infrastructure approval.
- OKX historical OI and VIP/deep-book capability remain unavailable until a
  separately licensed/certified provider source exists. Public contracts expose
  this honestly as capability/coverage state.

### Rollback

- Phase 4.5 adds dark pure-domain contracts and compatibility-safe internals.
  Revert its commits if required; V1 remains authoritative and no runtime,
  storage, source or consumer manifest is changed.

## 9. Phase 5 - V2 API, SDK And Controlled Consumer Migration

**Status:** `COMPLETE (FROZEN SHADOW)`

### Goal

Expose provider-neutral V2 snapshot/query/stream contracts and migrate consumers one `DataRequirement` at a time while V1 remains fully operational.

### Guide Index

- [Phase 5 detailed guide index: Sections 17-19, 24-25, 32, P2/P3 and Epics E10-E13](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-5)
- [OKX Phase 5 workstream: provider-neutral V2, internal diagnostics and controlled migration](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-5)

### To Do

- Implement provider-neutral V2 REST snapshot/batch/history endpoints, typed errors, explicit quality/provenance and partial-batch semantics.
- Serve OKX through those provider-neutral contracts using canonical `instrument_uid`; keep capability/status/subscription reconcile routes internal rather than making alpha code depend on `/providers/okx` paths.
- Implement gRPC server-streaming with snapshot cursor, durable replay, backpressure status and consumer telemetry.
- Generate Python SDK V2 from canonical contracts; provide sync/async wrappers, cursor persistence, freshness/source validators and V1 compatibility facade.
- Make `DataRequirement` manifests the audited source for subscription, startup readiness, fallback, revision and warmup needs.
- Integrate one reference alpha-grade and one execution-grade consumer in shadow. Trading System must not need Kafka knowledge.
- Validate current alpha runtime and Trading System V1 behavior unchanged; migrate only explicitly selected consumers after parity evidence.
- Publish deprecation telemetry and owner notifications, but do not remove V1 or legacy Redis in this phase.

### Verification And Exit Gate

- OpenAPI/Buf breaking gates and generated-client tests pass in CI.
- End-to-end tests cover venue simulator -> durable log -> canonicalizer -> Redis/V1 and gRPC/V2 -> reference consumers.
- Consumer restart, slow consumer, cursor expiration, fallback activation and revised-bar cases produce documented deterministic behavior.
- V1 and V2 shadow responses match canonical values and differ only in declared metadata/versioning.
- Selected alpha and Trading System consumers pass warmup, latest, stream, reconnect and freshness tests without direct venue connections.
- Load tests prove API replicas scale independently and do not multiply ingestion connections.

### Completed

- Phase 5 implementation started on `feat/fund-grade-data-layer-v2` after the
  Phase 4.5 dark freeze. Delivery is split into three independently tested
  slices: provider-neutral REST/OpenAPI, cursor-backed stream plus SDK V2, and
  controlled shadow-consumer migration/certification.
- V1 remains authoritative throughout this phase. No existing route, Redis
  payload, venue subscription, source authority or running consumer is changed
  implicitly; `symbols.json` remains user-owned and excluded from phase commits.
- REST/governance slice implemented provider-neutral instrument, snapshot,
  warmup, history, batch, feed-status, gap and readiness contracts with stable
  RFC 9457-style problem details. Binance and OKX are addressed only through
  canonical instrument identity; provider remains response provenance.
- Added strict consumer manifest parsing, aggregate deprecation telemetry and a
  governed `REGISTERED -> SHADOW -> ACCEPTED -> ACTIVE` migration state machine
  with explicit V1 rollback. PostgreSQL migration `0004` stores manifests,
  requirements, migration audit and hourly usage aggregates, never tick data.
- REST/query/consumer focused certification passed `20/20`; disposable
  PostgreSQL clean/existing/idempotent migration passed with `20` QDL tables,
  three lease functions and preserved legacy rows. No production DB was used.
- Added signed opaque cursor scope resolution so SDK consumers never provide or
  learn internal stream/partition topology. Removed topology fields reserve their
  Protobuf numbers/names, preserving wire safety rather than reusing tags.
- Added gRPC `REPLAYING`, `LIVE` and `RATE_LIMITED` controls, durable-first
  fanout, duplicate suppression, bounded buffers and isolated slow-consumer
  disconnect. SDK recovery covers explicit acknowledge, restored-state resume,
  cursor expiry snapshot replacement, transient reconnect and monotonic offsets.
- Completed async/sync SDK query wrappers and strict source-policy, entitlement,
  freshness, coverage, final-bar, revision, gap and execution-authority checks
  on both server and client boundaries. Direct REST and SDK clients now preserve
  all declared `DataRequirement` policy fields.
- Certified an OKX reference alpha and Binance execution-grade Trading System
  shadow consumer without direct venue connections. Reference fallback is
  visible to alpha-grade policy and rejected for execution-grade use.
- Froze CI/Make targets for OpenAPI/Buf/codegen, Phase 5 tests, dependency audit,
  migrations, Redis rebuild, load and bounded real-provider smoke. Full results
  are in
  [`PHASE5_V2_API_SDK_MIGRATION_REPORT.md`](upgrade/evidence/PHASE5_V2_API_SDK_MIGRATION_REPORT.md).

### Verification

- Phase 5 focused suite: `27/27` pass. Full Python/V1 regression: `251`
  executed, `246` pass and `5` expected environment skips.
- Buf format/lint/breaking/codegen and frozen OpenAPI gates pass. Rust format,
  Clippy `-D warnings` and `11/11` tests pass. Dependency audit reports zero
  known vulnerabilities.
- PostgreSQL clean/existing/idempotent migration preserves legacy data; Redis
  AOF restart/rebuild passes `3/3` with identical checksum and test DB cleanup.
- Eight independent API replicas served 2,000 requests at concurrency 100:
  `317.45 req/s`, p50 `266.942 ms`, p99 `444.088 ms`, zero venue connections and
  zero ingestion owners. An earlier p99 failure exposed sync thread-pool cost;
  async query handlers fixed it before freeze.
- Read-only real-provider smoke passed Binance USD-M canonical value parity and
  returned five authentic OKX swap bars with `production_writes=0`. Running V1
  health remained `ok`; no service restart, authority switch or consumer
  activation occurred.

### Technical Debt / Decision Gate

- No unresolved Phase 5 P0/P1 defect remains. No consumer is forced to migrate
  without owner acceptance; sunset dates remain a governed telemetry decision.
- Production HA broker/object store, external AuthN/AuthZ/TLS, secret-manager
  custody, OpenTelemetry SLO operations and per-feed authority promotion are
  Phase 6 certification/activation gates. Phase 5 does not represent local
  shadow durability or test credentials as production infrastructure.

### Rollback

- Move the selected consumer manifest back to V1/legacy projection. V2 and durable state remain available for diagnosis; no shared contract reversion is needed.

## 10. Phase 6 - Production Certification And Multi-Venue Readiness

**Status:** `BLOCKED` - implementation and shadow certification are complete;
production authority is intentionally `NO-GO` until the infrastructure gates in
the Phase 6 report pass.

### Goal

Certify production reliability, security, resource efficiency and operational recovery; complete controlled authority cutover for approved feeds; and prove the architecture can add OKX, DNSE/VN and future Deribit options without core redesign.

### Guide Index

- [Phase 6 detailed guide index: Sections 25-29, P3, 34-35, 37-41 and Appendix B](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-6)
- [OKX Phase 6 workstream: profile-aware certification, P4 products and optional SBE/Rust promotion](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-6)

### To Do

- Add OpenTelemetry traces/metrics/log correlation, role/feed dashboards, error budgets and actionable alerts for connection, lag, gaps, quality, projection, history and consumers.
- Enforce network zones, control-plane AuthN/AuthZ, secret handling, egress allowlists, SSRF/payload limits, audit logs and supply-chain/container policies.
- Run chaos matrix: process kill, broker failover, Redis loss/rebuild, projector checkpoint boundaries, DB/object-store outage, network partition, malformed frames, reconnect storm and slow consumers.
- Run sustained and burst performance/soak against measured production load plus headroom; verify CPU/RSS/disk/network and no monotonic memory/lag growth.
- Certify Binance USD-M first; certify OKX and Python VN adapters independently using the common conformance suite and explicit source-authority policies.
- Certify OKX JSON core feeds before any SBE promotion. Tier/profile products fail independently; SBE requires pinned schema/version, JSON shadow parity, unknown-schema fail-closed behavior and tested JSON rollback.
- Prove option readiness with instrument discovery, expiry/strike/call-put identity and order-book snapshot/delta/checksum fixtures representative of Deribit. Actual Deribit production activation remains a separate adapter certification, not a core rewrite.
- Cut over authority per venue/market/feed/hash range only after shadow parity and rollback rehearsal. Keep V1 compatibility projector until registered consumer sunset criteria are met.
- Remove obsolete combined-runtime producers and unused broad Spot runtime only after ownership, consumer and rollback gates pass; retain reusable adapter capability.
- Produce immutable artifacts, SBOM/provenance, release notes, runbooks and DR evidence; clean all test infrastructure and generated state.

### Verification And Exit Gate

- Every item in [Section 41 production acceptance checklist](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#41-production-acceptance-checklist) and Appendix B has evidence or an explicitly approved non-applicable rationale.
- Certified feeds meet correctness, freshness, gap, durability, replay, latency and recovery SLOs under normal, burst and failure tests.
- Security scans and AuthN/AuthZ/egress/secret-redaction tests meet policy with no unresolved critical/high issue.
- Capacity report demonstrates safe headroom on the current host or supplies a measured scale-out requirement before cutover.
- Rollback rehearsal restores the previous authoritative producer without data ambiguity.
- Test topics, consumer groups, schemas, Redis prefixes, buckets, volumes and containers are removed; production state remains untouched.

### Completed

- Added bounded OpenTelemetry-compatible correlation/metrics primitives, SLO
  evaluation and low-cardinality rules. Canonical drops and completeness breach
  fail as SEV-1 conditions.
- Added fail-closed JWT/RBAC control identity, environment/venue scope, exact
  egress allowlists, SSRF/private-address blocking, payload/decompression
  bounds, secret redaction and hash-chained mutation audit records.
- Added deterministic chaos/recovery tests for restart, transient durable sink,
  slow consumer, Redis rebuild, projector replay, object-store commit failure,
  lease/fencing failover, malformed/duplicate/gap inputs and OKX
  make-before-break reconnect.
- Added capability-scoped adapter certification. Binance USD-M selected TRADE,
  OKX V5 JSON reference/history and DNSE BAR scopes passed their bounded gates;
  OKX SBE and actual Deribit activation remain fail-closed capabilities.
- Added capacity certification with normal/burst windows, p50/p95/p99/p99.9,
  restart replay, queue rejection and memory-growth evidence.
- Added deterministic SPDX release manifests, immutable image enforcement,
  checksums, signature verification rehearsal, pinned CI actions, Python/Rust
  dependency gates and Trivy image/repository scans.
- Hardened the runtime image to fixed non-root UID/GID `10001`; upgraded final
  runtime `setuptools`; verified no unresolved HIGH/CRITICAL image finding,
  leaked secret or HIGH/CRITICAL repository misconfiguration.
- Ran the full final-image suite: 274 Python tests passed with five conditional
  integration skips whose Docker/Buf/Redis equivalents passed separately; 11
  Rust tests passed; Buf lint/breaking/generation, isolated Redis rebuild and
  PostgreSQL migration gates passed.
- Captured authentic read-only evidence from Binance USD-M, OKX V5 and DNSE.
  No synthetic/provider-fabricated production evidence and no production write
  were used.
- Preserved the running V1 authority and production state. At certification
  close it remained healthy, with restart count zero and no OOM.
- Frozen evidence:
  [Phase 6 production certification report](upgrade/evidence/PHASE6_PRODUCTION_CERTIFICATION_REPORT.md),
  [machine-readable decision](upgrade/evidence/phase6-certification-freeze.json),
  [capacity](upgrade/evidence/phase6-capacity.json),
  [service/provider smoke](upgrade/evidence/phase6-real-provider-service-smoke.json),
  [OKX](upgrade/evidence/phase6-okx-real-provider.json) and
  [DNSE](upgrade/evidence/phase6-dnse-real-provider.json).

### Technical Debt / Decision Gate

- `BLOCKED`: deploy a replicated Kafka-compatible durable broker and prove
  producer acknowledgements, replication, broker failover and restore. The
  certified SQLite implementation remains a bounded bridge only.
- `BLOCKED`: deploy OTel collector/dashboards/alert routing and approve the
  production SLO/error budget.
- `BLOCKED`: apply production workload identity, RBAC/network policy, external
  secret rotation, registry signature admission and entitlement/retention
  governance.
- `BLOCKED`: rehearse object-store/PITR and regional DR on independent
  infrastructure. A same-host test cannot certify regional recovery.
- `BLOCKED`: register and migrate every critical consumer, then run an
  operator-approved `SHADOW -> CANARY -> PRIMARY` cutover for one exact feed
  slice. Combined V1 and broad Spot producers are not removed automatically.
- Actual Deribit, additional options vendors or regional HA are separate production activations requiring credentials, licensing, capacity and source-semantics approval. The core architecture must already support them.
- V1/legacy Redis removal is not part of automatic Phase 6 closure; it requires zero-consumer telemetry and an approved sunset release.

### Rollback

- Authority flags roll back per feed/partition to the last certified producer. Durable cursors and canonical data remain available for reconciliation.

## 11. Phase 7 - V2 Public Beta And Consumer Canary

**Status:** `COMPLETE` (`7.0-7.3 COMPLETE`; `BETA-GO READ-ONLY`; V1 authoritative)

### Goal

Publish the provider-neutral V2 data plane as a protected, read-only beta and
prove that real monitoring and paper-alpha consumers can use typed snapshot,
warmup, history, signed cursor, replay and live stream contracts without an
undetected handoff gap. Phase 7 must harden the public contract, application-level
identity, readiness and SDK checkpoint behavior before exposure. V1 remains the
unchanged production authority and rollback path throughout this phase.

### Non-Goals

- Phase 7 does not change venue subscription ownership.
- Phase 7 does not grant Rust, V2 or the beta gateway authority to write legacy
  production keys/channels.
- Phase 7 does not allow a live execution service to depend solely on V2.
- Phase 7 does not use beta traffic as evidence for replicated durability or
  production source-authority promotion.
- Phase 7 does not sunset `/v1`, SDK v1 or direct legacy Redis consumers.
- Phase 7 does not make provider-native payloads part of the public contract.

### Architecture Boundary

```text
Approved venue/provider bytes
            |
            v
V1 production authority ---------------------> existing V1/Redis consumers
            |
            +-> canonical shadow state/log
                        |
                        +-> V2 query/snapshot/warmup
                        |
                        +-> V2 signed cursor/replay/live
                                    |
                                    +-> monitoring consumer
                                    +-> disposable paper alpha

Control mutation, authority mutation, diagnostics and provider-native raw
payload routes remain private and use separate permissions.
```

“Public beta” means a documented and reachable contract behind approved workload
identity, entitlement, quotas and rate limits. It is not an anonymous endpoint,
a source-authority claim or approval for execution dependency.

### Phase Decomposition

| Subphase | Outcome | Authority impact |
|---|---|---|
| 7.0 | Contract, bar lifecycle, data-plane identity, readiness and SDK hardening | None |
| 7.1 | Isolated beta deployment with real dependency health and protected routes | None |
| 7.2 | Monitoring consumer and disposable paper-alpha canary | None |
| 7.3 | Multi-session evidence freeze, cleanup and beta release decision | None |

Subphase 7.1 cannot begin until 7.0 contract/security gates pass. Subphase 7.2
cannot begin until isolated deployment, cursor recovery and rollback topology
tests pass. The phase remains `PLANNED` or `IN_PROGRESS` until all 7.3 evidence
is frozen.

### Required V2 Contract Shape

The public data-plane contract uses a discriminated payload union. The concrete
generated/Pydantic names may vary, but the semantic boundary is mandatory:

```python
MarketDataView = Annotated[
    TradeView
    | BboView
    | BarView
    | BookSnapshotView
    | BookDeltaView
    | FundingView
    | OpenInterestView
    | MarkPriceView
    | IndexPriceView
    | TickerView,
    Field(discriminator="feed"),
]
```

Every public model uses closed-field validation. A `TRADE` envelope cannot carry
bar fields, a `BAR` cannot omit its lifecycle/finality semantics, and unknown
provider fields do not leak into the contract accidentally.

Required response metadata:

```text
contract_schema
contract_version
schema_digest
normalizer_version
adapter_version
instrument_revision
source_policy_revision
authority_revision
request_id / correlation_id
snapshot_id
watermark_offset
signed stream cursor
```

Canonical decimal values remain coefficient/scale or a contractually equivalent
exact representation. Public and SDK models must not convert price or quantity
to binary float.

### Data-Plane Identity And Consumer Manifest

Every beta consumer has a durable manifest resolved from the authenticated
workload subject. The manifest, not request-controlled fields, owns entitlement:

```yaml
consumer_id: paper-alpha-momentum-01
subject: spiffe-or-jwt-subject
owner: alpha-team
environment: beta
criticality: paper
allowed_purposes:
  - research
allowed_contracts:
  - snapshot
  - warmup
  - stream
allowed_feeds:
  - TRADE
  - BAR
allowed_instrument_patterns:
  - "BINANCE:USDM:PERPETUAL:*"
allowed_source_policy_ids:
  - "binance-usdm-primary-v1"
max_request_rate_per_second: 20
max_concurrent_streams: 4
max_stream_buffer_events: 1000
max_warmup_rows: 5000
cursor_retention_requirement_seconds: 86400
sdk_min_version: "2.0.0b1"
sdk_max_major: 2
direct_venue_access: forbidden
direct_legacy_redis_access: forbidden
execution_dependency: forbidden
```

The exact persistence schema may be PostgreSQL-backed, but it must support
revisioned manifests, owner, environment, effective dates, audit identity and a
fail-closed disabled state.

Required data-plane permissions:

```text
market_data.query
market_data.snapshot
market_data.history
market_data.stream
instrument.read
diagnostics.read       # private, never implied by query
control.mutate         # private and separate from data-plane scopes
```

### Bar Lifecycle And Delivery Semantics

The delivery decision is event-lifecycle aware:

| Canonical event | Delivery policy |
|---|---|
| Trade | `LOSSLESS` |
| Book delta | `LOSSLESS` |
| Book snapshot/reset | `LOSSLESS` |
| Final bar | `LOSSLESS` |
| Bar revision/correction | `LOSSLESS` |
| Quality-state transition | `LOSSLESS` |
| Source-authority transition | `LOSSLESS` |
| BBO | `LATEST_STATE` |
| Ticker | `LATEST_STATE` |
| In-progress bar | `COALESCE_BY_BAR_KEY` |

A canonical bar must expose or derive the following semantics without ambiguity:

```text
bar_state = IN_PROGRESS | FINAL | REVISED | CANCELLED
interval
open_time_ns
close_time_ns
revision
origin
supersedes_event_id
aggregation_version
source_role
```

The coalescing key for an in-progress bar is at least:

```text
instrument_uid + interval + open_time_ns + lifecycle_class
```

A final bar is never removed by coalescing, and a correction creates a new
lineage-bearing event rather than mutating the prior canonical event silently.

### Readiness Model

Phase 7 replaces static/scaffold readiness with measured components:

```text
/health/live
    process is alive; no dependency claim

/health/ready
    this runtime role can serve its declared contract

/v2/system/dependencies
    auth/JWK, query store, durable source, Redis projector, catalog,
    source policy, cursor signer and audit dependencies

/v2/system/data-readiness
    feed freshness, completeness, open gaps, replay lag and projector lag

/v2/system/authority-readiness
    current authority revision, owner, lease epoch and compatibility projection

/v2/consumers/{consumer_id}/eligibility
    manifest entitlement plus current data quality and dependency state
```

Examples of role readiness:

- `query_v2` is ready only when the canonical query store, active cursor signing
  key, instrument catalog revision, source-policy revision and workload identity
  verifier are valid.
- `stream_v2` is ready only when replay cursors resolve, the durable source/live
  tail is reachable, per-partition handoff is available and subscriber limits
  are loaded.
- `compat_projector` is ready only when canonical lag is inside threshold,
  Redis is reachable and its authority/fencing revision is current.
- `history_query` may remain ready for immutable historical snapshots while a
  live venue is degraded, but its response explicitly reports the data
  `as_of`/coverage revision.
- A process can remain live while data readiness is degraded; orchestrator
  restart behavior must not confuse venue outage with process death.

### Snapshot, Cursor And Multi-Replica Handoff

The signed cursor claim set binds:

```text
environment
consumer_subject
consumer_id
requirement_digest
stream
partition_key
snapshot_id
snapshot_watermark
logical_offset
schema_major
partition_plan_epoch
instrument_catalog_revision
source_policy_revision
authority_revision
issued_at
expires_at
key_id
```

Phase 7 must select and document one beta topology:

1. **Active/passive beta gateway:** one active gateway owns each partition behind
   a distributed lease and sink-visible fencing epoch; passive replicas do not
   independently create a second live barrier.
2. **Partition-affine gateways:** a stable partition plan routes cursor replay
   and live tail to one owner, using broker-native offsets and a per-partition
   replay/live barrier.

A process-local global lock is insufficient as multi-replica evidence. Remote
replay/durable I/O must be asynchronous and bounded; unrelated partitions must
not serialize behind one global event-loop lock.

### SDK Continuity And Checkpoint Contract

The V2 SDK must:

- Return generated or typed public models instead of unvalidated dictionaries.
- Accept a `CredentialProvider` capable of short-lived token refresh and key
  rotation; REST and gRPC transports apply the same workload identity.
- Reject a response without immutable `snapshot_id`, signed cursor or required
  watermark. The SDK must never synthesize `"latest-snapshot"`.
- Enforce sequential observation and contiguous acknowledgement.
- Distinguish `observed`, `applied` and `checkpointed` offsets.
- Expose a consumer-owned transactional/checkpoint adapter for critical clients.
- Mark memory/file cursor stores as development, monitoring or paper-only unless
  their durability boundary is explicitly approved.
- Fail closed on cursor/requirement/environment/consumer mismatch.
- Resnapshot only through an explicit `SNAPSHOT_REPLACED` control event that
  requires the consumer to rebuild local state.
- Emit SDK version, contract version, cursor offset and requirement digest
  telemetry without high-cardinality raw symbols where prohibited.

Minimum acknowledgement invariant:

```text
acknowledged_offset == previous_acknowledged_offset + 1
acknowledged_offset <= highest_applied_offset
```

A batch acknowledgement may advance a contiguous range only when its start is
the previous checkpoint plus one.

Recommended extension boundary:

```python
class CheckpointTransaction(Protocol):
    async def save_applied_cursor(
        self,
        *,
        consumer_id: str,
        requirement_digest: str,
        cursor_token: str,
        offset: int,
    ) -> None: ...

class CredentialProvider(Protocol):
    async def get_access_token(self) -> str: ...
```

### Runtime Roles

Phase 7 adds explicit role names instead of extending the Phase 1 dark-role
manifest indefinitely:

```text
api_v1
query_v2
stream_v2
control
history_query
history_materializer
compat_projector
reconciliation
```

A role owns only the dependencies and lifecycle required by that role. API/query
replicas never open venue sockets. The beta deployment uses separate image
digests, ports, state paths, credentials, Redis prefixes, consumer groups,
PostgreSQL schemas/rows and network policy from the V1 production authority.

### Guide Index

- [V2 API, SDK and migration design: Sections 17-19, 24-25 and 32](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-5)
- [Security, operations and cutover boundaries: Sections 25-29 and 34-41](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-6)
- [Phase 6 certification decision](upgrade/evidence/PHASE6_PRODUCTION_CERTIFICATION_REPORT.md)
- [Canonical contract and runtime boundaries](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-1)
- [Gap-free handoff and consumer migration](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-4)

### To Do

#### 7.0 Contract And Security Hardening

- Add a data-plane security configuration and REST guard separate from the
  control-plane permission. Apply it to every V2 snapshot, warmup, history,
  instrument and stream bootstrap route.
- Add a gRPC server interceptor and client call credentials with the same issuer,
  audience, environment, token-expiry and scope semantics as REST.
- Resolve `principal.subject -> consumer manifest`; reject a request when
  authenticated subject, requested `consumer_id`, environment or manifest
  revision does not match.
- Derive allowed purpose, data grade, source policy, feed, instrument scope,
  quotas and execution eligibility server-side.
- Add token/JWK rotation tests, expired/not-yet-valid token tests, wrong
  audience/environment tests, scope escalation tests and credential redaction.
- Introduce typed feed-discriminated REST responses and typed SDK models. Forbid
  unknown public fields and provider-native payload fallthrough.
- Replace contract-critical free-form strings with generated enums containing an
  explicit `UNSPECIFIED = 0`; reject unspecified values at service boundaries.
- Add `schema_digest`, contract version, normalizer/adapter version,
  instrument/source-policy/authority revision metadata.
- Add bar lifecycle/finality/revision fields and replace feed-only delivery
  policy with lifecycle-aware policy.
- Add compatibility fixtures proving that typed V2 changes do not affect V1
  OpenAPI, SDK v1 signatures or Redis payload bytes.
- Add Git-reference plus immutable-baseline Buf breaking checks and OpenAPI
  semantic diff. A beta contract freeze candidate is an immutable artifact.

#### 7.1 Isolated Beta Runtime

- Build immutable non-root beta images for `query_v2`, `stream_v2` and required
  supporting roles. Pin image digests and record SBOM/provenance.
- Replace hardcoded readiness with dependency probes, lag/freshness evaluation
  and authority/manifest revision checks.
- Choose and implement the active/passive or partition-affine stream topology.
  Add distributed lease/fencing for the selected beta gateway ownership.
- Replace blocking replay/durable calls under a global async lock with an async
  repository and per-partition handoff barrier.
- Configure dedicated beta ports, DNS/hostname, JWT audience, credentials,
  network policy, Redis prefix, consumer group, cursor keyring, state path,
  quotas and audit sink.
- Enforce request-size, decompression-size, deadline, concurrency, stream-count,
  warmup-row and replay-limit bounds at both gateway and application layers.
- Run a topology protection test:
  1. capture V1 processes, connections, Redis namespaces and restart counters;
  2. deploy V2 beta;
  3. execute beta traffic;
  4. stop V2 beta;
  5. prove exact V1 ownership/topology remains unchanged.
- Ensure no V2 beta component writes legacy production keys/channels or shares a
  writable local history/cursor path with V1.
- Publish beta OpenAPI, Protobuf descriptor set, schema digest, SDK package,
  compatibility statement, cursor TTL and rate-limit documentation.

#### 7.2 Consumer Canary

- Register one monitoring/reference consumer with read-only query/stream scopes.
- Register one disposable paper alpha after the monitoring consumer passes.
- Require both consumers to use the V2 SDK and prohibit direct venue access,
  direct beta broker access and reuse of a production durable consumer group.
- Exercise snapshot/warmup, signed cursor, replay/live handoff, client restart,
  credential rotation, cursor expiry, stale data, open gap, slow consumer,
  gateway failover and V1 fallback.
- Compare V1 and V2 on exact instrument identity, decimal coefficient/scale,
  source/event/receive times, source role, authority revision, event count,
  final-bar lifecycle, freshness, coverage and quality flags.
- Repeat closed-bar/live handoff across multiple complete bar intervals and, for
  applicable VN feeds, across market-session open/close boundaries.
- Record consumer-applied and checkpointed offsets. Verify a paper alpha restart
  reconstructs the same local signal state from snapshot plus replay.
- Keep `execution_dependency = forbidden` in both consumer manifests.

#### 7.3 Evidence Freeze And Beta Decision

- Run normal and burst beta traffic with multiple fan-out consumers, including
  one intentionally slow consumer.
- Measure request rate, events/s, bytes/s, p50/p95/p99/p99.9, cursor lag, replay
  lag, end-to-end freshness, CPU/core, RSS, network, durable-store/Redis growth,
  subscriber count, disconnect/replay count and error-budget consumption.
- Run malformed/oversized request, auth failure, token rotation, rate-limit,
  cursor tamper, cursor expiry, partition-epoch mismatch and dependency outage
  tests.
- Freeze compact checksummed evidence, immutable manifests and exact commands.
- Revoke disposable credentials and remove temporary consumers, keys, cursor
  files, containers, networks and isolated state after certification.
- Record an explicit `BETA-GO` or `BETA-NO-GO`. `BETA-GO` does not change source
  authority or allow execution-only dependency.

### Verification Matrix

| Area | Required cases | Pass condition |
|---|---|---|
| V1 compatibility | OpenAPI, SDK signatures, Redis keys/channels/payloads | Byte/behavior compatible; V1 restart count unchanged |
| REST identity | valid, expired, wrong audience, wrong environment, wrong scope, consumer mismatch | Fail closed with typed errors and no data leak |
| gRPC identity | same cases as REST plus reconnect/token rotation | Same principal/manifest decision as REST |
| Typed contract | each feed payload, unknown field, invalid discriminator, unspecified enum | Invalid states are unrepresentable or rejected |
| Bar lifecycle | in-progress, final, revised, reconnect boundary | Final/revision never coalesced or lost |
| Cursor integrity | tamper, expiry, policy/catalog/partition revision mismatch | Deterministic rejection or explicit resnapshot |
| Handoff | snapshot/replay/live during concurrent publication | No duplicate or missing logical offset |
| Gateway HA | active failure/passive takeover or partition-owner restart | One owner, fenced takeover, bounded reconnect |
| SDK checkpoint | sequential apply, skipped ACK, duplicate ACK, crash before/after checkpoint | No forward checkpoint over unapplied data |
| Slow consumer | outbound buffer exhaustion | Explicit disconnect/control error; replay from confirmed cursor |
| Dependency health | auth, query store, durable source, Redis, catalog, signer outage | Readiness/data eligibility degrade accurately |
| Consumer parity | monitoring and paper alpha | Zero unexplained value/count/finality/authority mismatch |
| Capacity | normal and burst profiles | Every machine-readable threshold passes |
| Rollback | remove beta route/roles | Exact pre-beta V1 topology remains |

### Required Evidence Artifacts

At minimum, Phase 7 produces:

```text
upgrade/evidence/phase7-contract-freeze.json
upgrade/evidence/phase7-openapi-diff.json
upgrade/evidence/phase7-buf-breaking.json
upgrade/evidence/phase7-auth-matrix.json
upgrade/evidence/phase7-readiness-matrix.json
upgrade/evidence/phase7-cursor-handoff.json
upgrade/evidence/phase7-sdk-checkpoint.json
upgrade/evidence/phase7-consumer-parity.json
upgrade/evidence/phase7-capacity.json
upgrade/evidence/phase7-security-adversarial.json
upgrade/evidence/phase7-evidence-freeze.json
upgrade/evidence/phase7-topology-rollback.json
upgrade/evidence/PHASE7_PUBLIC_BETA_REPORT.md
```

Reports record exact artifact/image/schema/config revisions, test cases run, skipped
cases, cleanup evidence and the remaining authority restrictions.

### Verification And Exit Gate

Phase 7 is `COMPLETE` only when all conditions below pass:

- Application-level REST and gRPC workload identity, principal-to-consumer
  binding, entitlement and rate-limit tests pass.
- The public beta contract has no generic provider payload dictionary and
  critical semantic fields use typed closed models/enums.
- Final bars, bar revisions, quality transitions and authority transitions use
  lossless delivery semantics.
- Role and data readiness are dependency-derived; no beta route reports ready
  from a static phase note.
- Snapshot/cursor/replay/live continuity passes in the selected multi-replica or
  active/passive fenced topology.
- The SDK does not fabricate snapshot/cursor state and enforces contiguous
  applied/checkpointed offsets.
- V1 golden API/Redis/SDK compatibility remains byte/behavior compatible; V1
  authority is not restarted or reconfigured by beta deployment.
- Real-provider read-only smoke passes; no generated market event is admitted to
  beta evidence.
- Monitoring and paper-alpha consumers complete multiple handoffs with zero
  unexplained identity/value/count/finality/source-authority mismatch and zero
  undetected gap.
- Capacity evidence passes every configured threshold exactly, with no
  unexplained loss, duplicate, monotonic queue/spool growth or unbounded resource
  growth.
- Stopping beta routes/containers, revoking beta credentials and deleting
  isolated state restores the exact pre-beta topology.
- The release decision states explicitly that V2 remains read-only beta,
  non-authoritative and forbidden as a sole execution dependency.

### Completed

- `7.0 COMPLETE` on 2026-08-14. Added application-level REST and gRPC workload
  identity, immutable consumer-manifest binding, server-derived entitlement and
  execution eligibility, closed feed-discriminated models, generated enums,
  contract lineage, lifecycle-aware lossless final/revised bars and typed SDK
  responses/checkpoint behavior.
- Full Python regression passed 285 tests with five conditional skips covered by
  separate Docker/Buf/migration integration gates. Rust fmt, clippy with warnings
  denied and all 11 Rust tests passed. PostgreSQL clean/existing/idempotent
  migration passed with legacy rows retained and 21 QDL tables.
- Buf format/lint and breaking checks passed against both the immutable Phase 1
  baseline and the Phase 7 beta freeze candidate. OpenAPI semantic diff found no
  removed operation, response, schema or enum value. V1 OpenAPI, SDK surface and
  Redis payload golden hashes remain unchanged.
- Evidence: [Phase 7.0 report](upgrade/evidence/PHASE7_CONTRACT_SECURITY_HARDENING_REPORT.md),
  [contract freeze](upgrade/evidence/phase7-contract-freeze.json),
  [OpenAPI diff](upgrade/evidence/phase7-openapi-diff.json),
  [Buf gates](upgrade/evidence/phase7-buf-breaking.json) and
  [auth matrix](upgrade/evidence/phase7-auth-matrix.json).
- No V1 runtime, source authority, provider connection or production state was
  restarted or mutated. This completion does not authorize beta deployment.
- `7.1 COMPLETE` on 2026-08-14. Added isolated non-root `query_v2` and
  active/passive `stream_v2` roles, dependency-derived readiness, a monotonic
  Redis fencing lease, atomic shared Redis quotas, per-partition gap-free
  replay/live barriers and bounded HTTP/gRPC/resource controls.
- The real topology gate authenticated a beta query, fenced one stream owner,
  promoted the passive owner from epoch `1` to `2`, then removed every beta
  container/network/volume. Canonical V1 container IDs, images, restart counts,
  networks and mounts were unchanged; production Redis contained zero beta keys
  before and after.
- Full Python regression passed 296 tests with five existing conditional skips.
  Rust fmt/clippy with warnings denied and 11 Rust tests passed. Both Buf
  breaking baselines and the frozen OpenAPI digest passed unchanged. The tested
  non-root image, SBOM and SHADOW release manifest are immutable and verified.
- Evidence: [Phase 7.1 report](upgrade/evidence/PHASE71_ISOLATED_BETA_RUNTIME_REPORT.md),
  [readiness matrix](upgrade/evidence/phase7-readiness-matrix.json),
  [cursor handoff](upgrade/evidence/phase7-cursor-handoff.json),
  [topology rollback](upgrade/evidence/phase7-topology-rollback.json) and
  [release bundle](upgrade/evidence/phase71-release-bundle/release-manifest.json).
- Phase 7.1 activates no consumer data source and does not authorize execution
  dependency or beta authority. Real monitoring and paper-alpha activation are
  exclusively Phase 7.2 work.
- `7.2 COMPLETE` on 2026-08-14. Added a strict canonical source catalog, bounded
  V1 read-only bridge, shared durable query/stream watermark, per-consumer
  signed handoff cursors, monitoring consumer and disposable paper-alpha
  consumer. Both consumers use only the V2 SDK and explicitly forbid execution
  dependency.
- The real-provider topology canary seeded 117 closed BTCUSDT 1m bars, started
  monitoring before paper, observed/checkpointed offsets `118/119`, promoted the
  passive gateway from epoch `1` to `2`, then resumed the paper consumer at
  offset `120` on the next real closed bar. V1/V2 mismatch count and restarted
  signal-state mismatch were both zero.
- Stale data, missing closed-bar intervals, cursor scope/expiry, credential
  rotation, bounded slow-consumer recovery, final-only authenticated ingest,
  duplicate ingest, V1 fallback and exact topology rollback passed. No generated
  market event entered real evidence.
- Full Python regression passed 305 tests with five existing conditional skips;
  Rust fmt/clippy with warnings denied and 11 tests passed. Both Buf breaking
  baselines and the frozen OpenAPI digest remained unchanged.
- Evidence: [Phase 7.2 report](upgrade/evidence/PHASE72_CONSUMER_CANARY_REPORT.md),
  [consumer parity](upgrade/evidence/phase7-consumer-parity.json),
  [SDK checkpoint](upgrade/evidence/phase7-sdk-checkpoint.json) and
  [topology canary](upgrade/evidence/phase72-topology-canary.json).
- V1 remains authoritative and was not restarted or reconfigured. Phase 7.2
  does not authorize V2 execution dependency, production durable groups or
  public authority.
- `7.3 COMPLETE` on 2026-08-15. Added per-consumer concurrent-stream quota
  enforcement and one bounded read-only capacity manifest, then ran normal and
  burst REST traffic plus four-way replay/live fan-out against real V1/provider
  closed bars. No generated market event entered beta evidence.
- Normal traffic reached 39.120 requests/s at p99.9 211.058 ms; burst traffic
  reached 36.362 requests/s at p99.9 628.694 ms, both with zero errors. Three
  fast stream consumers drained 64 offsets to zero lag at 652.316 events/s; one
  slow consumer was explicitly disconnected without blocking peers or losing
  durable data.
- Missing/invalid identity, two-key rotation, malformed/oversized request,
  rate limit, cursor tamper/expiry/scope, Redis outage and active/passive
  failover gates passed. The Redis restart test found and closed an ephemeral
  fencing-epoch defect by adding an isolated beta-only AOF volume; epoch then
  persisted and advanced from `1` to `2`.
- Peak application RSS was about 68.2 MiB, peak CPU 75.21% of one core, durable
  growth 73728 bytes and all machine thresholds passed. Exact rollback removed
  every beta container, network, volume, key and cursor file while preserving
  byte-equal V1 topology and HTTP 200 fallback.
- Final regression passed 309 Python tests with five conditional skips covered
  by dedicated integration gates; targeted Phase 7 regression passed 35 tests.
  Rust fmt/clippy and all 11 tests passed, as did both immutable Buf breaking
  baselines.
- Final decision: `BETA-GO` for protected read-only V2 only. V1 remains source
  authority and the sole approved execution fallback. Evidence: [final report](upgrade/evidence/PHASE7_PUBLIC_BETA_REPORT.md),
  [capacity](upgrade/evidence/phase7-capacity.json), [security/adversarial](upgrade/evidence/phase7-security-adversarial.json)
  and [runbook](docs/runbooks/phase73-public-beta-decision.md).

### Technical Debt / Decision Gate

- A certified bounded bridge may support Phase 7 while V1 remains authoritative,
  but it cannot satisfy Phase 8 authority-capable or Phase 9 primary gates.
- The beta stream topology must choose active/passive or partition-affine
  ownership explicitly; “multiple stateless replicas” is not a valid handoff
  design by itself.
- File cursor storage is acceptable only for disposable paper/monitoring use.
  Any critical consumer requires an approved durable/transactional adapter.
- Any contract change after the beta freeze candidate requires a new schema
  digest, compatibility report and SDK support decision.

### Rollback

- Remove the V2 beta gateway route and stop only dedicated V2 roles.
- Revoke beta credentials and disable/delete beta consumer manifests.
- Delete isolated beta cursor/checkpoint state, Redis prefixes, consumer groups,
  containers and networks after evidence capture.
- V1 requires no replay, schema rollback, venue reconnection or service restart.
- If a beta contract defect is found, revoke the affected schema/SDK release,
  keep V1 authoritative and issue a new beta contract revision rather than
  silently changing semantics in place.

## 12. Phase 8 - Multi-Venue Rust Realtime Core And Reference Slice

**Status:** `COMPLETE (8.0-8.3; RUST_SHADOW only; V1 authoritative)`

### Goal

Build one production-shaped, provider-neutral Rust realtime core and a
replicated shadow durability substrate; prove the same identity, decimal,
timestamp, session, ordering, gap, quality, replay and durable-publish semantics
across Binance, OKX, DNSE/VN and Deribit-style capability inputs; then certify a
demanded Binance USD-M TRADE reference shadow using the exact same authentic
provider frames as the Python primary. No Rust output receives public or legacy
write authority in this phase.

### Non-Goals

- Phase 8 does not change public source authority.
- Phase 8 does not certify BBO, L2 or BAR merely because TRADE passes.
- Phase 8 does not move Python query/control/history/SDK ownership into Rust.
- Phase 8 does not embed the production realtime loop in FastAPI or call Rust
  once per event through PyO3.
- Phase 8 does not treat a local file/SQLite bridge as the production broker.
- Phase 8 does not certify every venue edge from Binance evidence.
- Phase 8 does not fabricate real-provider evidence from fixtures or simulators.

### Target Ownership

```text
Binance Rust edge --------\
OKX Rust edge -------------+--> versioned raw-provider envelope
DNSE Python/SDK edge ------+             |
Deribit future Rust edge --/             v
                                Rust canonical realtime core
                                - instrument resolution
                                - exact decimal/time
                                - source session/generation
                                - event identity
                                - ordering/dedup/gap
                                - quality/book/bar lifecycle
                                - backpressure/spool
                                - DurableSink
                                      |
                 +--------------------+-------------------+
                 |                                        |
                 v                                        v
        replicated shadow raw log                replicated shadow
                                                  canonical/quality log
                                                           |
                                                           v
                                            shadow query/projector/replay

Python remains:
- V1 production authority
- public V1 compatibility writer
- V2 outer API/SDK/control/history/reconciliation
- low-rate/proprietary acquisition edge where justified
```

The shared core is a set of provider-neutral crates and contracts, not one giant
multi-venue process. Deployment shards remain separated by venue/market/feed
blast radius.

### Phase Decomposition

| Subphase | Outcome | Authority impact |
|---|---|---|
| 8.0 | Replicated shadow broker, topics, security, observability and recovery baseline | None |
| 8.1 | Versioned raw envelope, Rust session/core traits and full failure semantics | None |
| 8.2 | Exact-frame Python/Rust tee, cross-venue conformance and long shadow soak | None |
| 8.3 | Immutable authority-capable Rust artifact, rollback manifest and evidence bundle, still fenced | None |

The production-shaped broker begins in 8.0 so Rust backpressure, ACK latency,
retention, replay, partitioning and recovery are measured against the real
transport before any Phase 9 decision.

### Replicated Durable Transport Contract

A Kafka-compatible deployment is the default target selected by the architecture
guide. Another implementation requires an ADR proving equivalent semantics.

Minimum shadow topics/streams:

```text
qdl.<env>.raw.<venue>.<market>.<feed>.v1
qdl.<env>.canonical.<feed>.v2
qdl.<env>.quality.v2
qdl.<env>.control.authority.v1
qdl.<env>.quarantine.<venue>.<feed>.v1
qdl.<env>.audit.v1
```

Topic names are infrastructure details and do not appear in public cursors or API
contracts.

Required broker properties:

```text
replication factor >= approved failure-domain requirement
acks = all
min in-sync replicas enforced
idempotent producer enabled
unclean leader election disabled
retention >= maximum cursor TTL + recovery/incident margin
bounded message size and batch size
per-service ACLs and quotas
encryption in transit
disk/partition monitoring
tested node loss, leader change, restore and retention expiry
```

The system targets loss-detected, replayable and effectively-once canonical
projection. It does not make a vague end-to-end exactly-once claim across an
external venue.

### Versioned Raw Provider Envelope

The raw envelope is a first-class contract shared by native Rust edges and
approved thin Python acquisition edges:

```text
raw_schema_name
raw_schema_major/minor
capture_id
provider
venue
market
product_type
native_symbol
native_channel
subscription_id
source_session_id
connection_generation
lease_epoch
authority_revision
partition_plan_epoch
received_at_ns
transport_protocol
transport_compression
capture_boundary
raw_frame_bytes
raw_frame_sha256
adapter_version
config_revision
instrument_catalog_revision
correlation_id
test_provenance
```

Rules:

- `raw_frame_bytes` are exact bytes at the declared capture boundary.
- If WebSocket compression is used, both the transport codec and whether the
  capture is pre- or post-decompression are explicit.
- `raw_frame_sha256` hashes those exact bytes.
- `canonical_payload_hash` is computed separately after deterministic canonical
  serialization.
- Required provider fields never default to plausible market values such as
  `0`, `false` or empty string. Missing/invalid required data is quarantined and
  drives an observable quality transition.
- Raw records remain immutable; a corrected normalizer creates a new canonical
  revision/version rather than rewriting raw evidence.
- Synthetic fixtures set `test_provenance = true` and cannot enter production or
  public-beta evidence namespaces.

### Source Session, Sequence And Offset Semantics

The core distinguishes:

```text
native_sequence
    venue/provider sequence or trade ID within its documented scope

source_session_id
    epoch in which native sequence continuity semantics apply

connection_generation
    monotonically increasing reconnect/resubscription generation

event_id
    deterministic source event identity

logical_partition_offset
    durable-log ordering position exposed through a transport-neutral cursor

lease_epoch
    ingestion owner generation

authority_revision
    control-plane authority record revision

partition_plan_epoch
    version of the stable shard/partition assignment
```

A process-local accepted-event counter is not a production partition sequence.
Old-generation frames arriving after reconnect are rejected/quarantined.
Provider-specific sequence reset rules are declared in the capability manifest.

### Rust Core And Crate Boundaries

Recommended workspace boundaries:

```text
rust/crates/qdl-domain
rust/crates/qdl-contracts
rust/crates/qdl-provider-envelope
rust/crates/qdl-venue-core
rust/crates/qdl-quality
rust/crates/qdl-ordering
rust/crates/qdl-orderbook
rust/crates/qdl-bars
rust/crates/qdl-durable-sink
rust/crates/qdl-kafka
rust/crates/qdl-replay
rust/crates/qdl-telemetry
rust/crates/qdl-binance
rust/crates/qdl-okx
```

Core crates must not branch on venue for canonical identity, exact decimal,
quality-state, ordering, replay or durable publication. Provider modules own
wire-protocol parsing and documented native semantics through capability traits.

Rust runs as separate process/container roles such as:

```text
rust-ingestor-binance-usdm-shard-*
rust-ingestor-okx-swap-shard-*
rust-canonicalizer-*
rust-quality-projector-*
rust-replay-worker-*
```

A thin DNSE/proprietary Python edge may publish authenticated raw envelopes but
does not retain a separate Python canonical/quality engine.

### Full-Duplex Venue Session Engine

Each production-shaped venue edge implements:

- Connection state machine and connection-generation increment.
- Reader and writer halves.
- Ping/Pong and provider heartbeat semantics.
- Subscription command, acknowledgement and rejection tracking.
- Desired-versus-actual subscription reconciliation.
- Provider rate-limit budget and endpoint/channel bucket.
- Read/freshness deadlines.
- Exponential backoff with jitter and a retry budget.
- Make-before-break only where provider semantics permit it.
- Old-generation frame rejection.
- Graceful drain and terminal durable watermark.
- Lease renewal and immediate fail-closed publication on lease loss.
- Bounded in-memory queue and local spool with explicit disk quota.
- Degraded, blocked, disconnected and recovering feed states.
- Quarantine path for malformed, schema-unknown and semantic-invalid frames.

Queue saturation behavior:

```text
lossless canonical lifecycle:
    apply backpressure
    -> bounded durable/local spool
    -> degrade or disconnect before unacknowledged loss
    -> replay/reconcile

latest-state lifecycle:
    deterministic lifecycle-aware coalescing
    -> increment coalescing metrics
    -> preserve freshness/quality semantics
```

No canonical event may be silently dropped while the feed remains healthy.

### Stable Subscription And Partition Planning

Replace sort/chunk reshuffling for production ownership with rendezvous hashing,
consistent hashing or a persisted assignment table.

Each assignment plan records:

```text
partition_plan_epoch
hash_algorithm/version
partition_count
assignment_revision
instrument_uid
old_owner
new_owner
handoff_watermark
created_at
approved_by
```

Adding one instrument must not reshuffle most existing subscriptions. Canary and
primary authority use the same stable partition key. An order book, bar
aggregator or sequence scope cannot be split across owners.

### Exact-Frame Tee And Oracle Comparison

The first real reference slice uses one received provider frame:

```text
provider frame
    -> immutable capture_id + raw hash
        -> Python primary oracle
        -> Rust shadow core
```

Do not compare two independent WebSocket connections as the primary correctness
oracle because connection timing, provider batching and subscription boundaries
can create false mismatches.

Comparison dimensions:

```text
capture_id
event_id
instrument_uid/revision
venue/market/product
exact price/quantity decimals
side and provider flags
source event time
received time policy
native sequence
source session/generation
bar lifecycle/finality/revision
quality transitions
output count
quarantine decision
canonical payload hash
```

Allowed divergences require an explicit versioned contract decision; unexplained
divergence blocks Phase 8.

### Machine-Readable Venue Capability Manifest

Each venue/market/feed capability publishes a manifest, for example:

```yaml
venue: BINANCE
market: USDM
product_type: PERPETUAL
feed: TRADE
adapter_version: "..."
native_sequence_field: aggregate_trade_id
sequence_scope: instrument
source_timestamp_precision: millisecond
heartbeat: websocket_ping_pong
subscription_ack: provider_defined
snapshot_required: false
duplicate_identity: native_trade_id
reconnect_sequence_continuity: provider_defined
rate_limit_profile: binance-usdm-v1
supports_raw_exact_frame: true
authority_eligible: false
```

For order book capabilities the manifest additionally defines:

```text
snapshot source
first valid update rule
delta range/sequence rule
checksum algorithm
gap detection rule
resnapshot rule
maximum recovery window
```

Unsupported capabilities fail independently. Binance TRADE success does not
certify Binance L2, OKX SBE, DNSE bars or Deribit options.

### Guide Index

- [Python/Rust role model and canonical hot path: Sections 8-14 and 20-23](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-1)
- [Durable transport and Rust foundation: Sections 6-7, 11 and 28-29](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-2)
- [Ingestion, fencing and compatibility projection: Sections 12-14, 23 and 37](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-3)
- [Production certification and failure testing](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-6)
- [OKX V5 capability and sequence guide](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md)

### To Do

#### 8.0 Replicated Shadow Substrate

- Select/provision the approved Kafka-compatible broker on isolated shadow
  namespaces and real failure domains.
- Define raw, canonical, quality, authority, quarantine and audit topic
  configurations: partition key, retention, replication, min-ISR, compression,
  maximum record size, quota, ACL and owner.
- Implement broker-backed `DurableSink`/`EventSource` with idempotent producer,
  bounded retry, explicit ACK classification and transport-neutral cursor
  mapping.
- Keep the existing bounded bridge as an evidence/fallback tool, not hidden
  authority. Add a documented sunset path.
- Deploy OTel collector, dashboards and alert routing for broker ACK latency,
  produce failure, partition lag, spool growth, replay throughput, consumer lag,
  disk pressure and leader changes.
- Run broker node loss, leader election, network partition, disk pressure,
  retention expiry, producer restart and restore tests.
- Prove Redis latest/legacy shadow projection can be rebuilt from the replicated
  canonical log.

#### 8.1 Raw Envelope And Rust Session/Core

- Add generated raw-provider-envelope Protobuf/schema and cross-language golden
  bytes.
- Capture exact frame bytes and separate raw-frame/canonical hashes.
- Add source session, connection generation, authority revision and partition
  plan epoch to raw and canonical metadata where required.
- Remove plausible defaults for required provider fields; quarantine malformed or
  incomplete frames with reason codes and bounded payload evidence.
- Implement provider-neutral connection, subscription, rate-limit, heartbeat,
  lease/fencing, decoder and capability traits.
- Implement full-duplex Binance USD-M session handling and deterministic
  reconnect/resubscription state.
- Implement deterministic ordering, deduplication, gap detection, quarantine and
  quality transitions.
- Implement lifecycle-aware queue/backpressure/spool policy including lossless
  final bars/corrections and coalescible in-progress state.
- Replace production shard planning with stable versioned assignment.
- Ensure Rust binaries use independent lifecycle/resources and do not share the
  FastAPI event loop.

#### 8.2 Reference Shadow And Cross-Venue Conformance

- Tee exact authentic Binance USD-M TRADE frames to Python primary and Rust
  shadow.
- Feed bounded authentic OKX and DNSE captures through approved native/raw
  envelope edges and compare against the existing Python canonical oracle.
- Run Deribit-style deterministic option/book fixtures to prove identity and
  capability boundaries without claiming a live adapter.
- Run long deterministic raw-frame replay repeatedly and across clean/restarted
  processes; compare event IDs, hashes, sequence/session, quality transitions and
  counts.
- Exercise controlled reconnect, stale-generation arrival, subscription reject,
  lease loss, durable-sink outage, spool bound, malformed frame, slow projector,
  Redis loss and restart/replay.
- Run normal, burst and replay-concurrent-with-live profiles with production-like
  payload mix and consumer fan-out.
- Measure events/s, bytes/s, p50/p95/p99/p99.9, CPU/core, RSS, allocation,
  network, broker ACK, queue/spool depth, projector lag and recovery time.
- Soak across an approved duration or complete market sessions; justify the
  chosen window by venue/feed behavior.

#### 8.3 Authority-Capable Artifact, Still Fenced

- Produce immutable Rust image digest, SBOM, signature/provenance,
  capability-manifest digest, contract/schema digest and configuration revision.
- Produce an immutable Python rollback manifest for the exact candidate slice.
- Implement/read the persistent authority record and sink-side fencing contract,
  but keep the Rust slice in `RUST_SHADOW`.
- Rehearse `SHADOW -> CANARY -> SHADOW` without public write authority.
- Freeze the exact production candidate slice and partition plan for Phase 9
  approval.
- Retain only compact checksummed evidence and approved replay captures; clean
  disposable topics/groups/prefixes/containers.

### Verification Matrix

| Area | Required cases | Pass condition |
|---|---|---|
| Broker durability | producer restart, broker node loss, leader change, min-ISR failure, restore | No acknowledged canonical loss inside certified boundary |
| Broker security | ACL, quota, TLS, unauthorized topic access | Least privilege; fail closed |
| Raw fidelity | exact capture bytes, compression boundary, hash | Stable exact raw hash and separate canonical hash |
| Decoder | malformed, missing required field, unknown schema | Quarantine; no plausible default |
| Session engine | ping/pong, ACK reject, timeout, reconnect storm, stale generation | Deterministic state and bounded reconnect |
| Lease/fencing | owner loss, zombie producer, epoch change | Stale publication rejected |
| Ordering | duplicate, out-of-order, sequence reset, reconnect | Deterministic dedup/gap/session behavior |
| Backpressure | broker slow/down, memory queue full, spool full | Explicit degrade/block/disconnect; zero silent loss |
| Stable sharding | add/remove instruments, owner change | Bounded churn and explicit plan epoch |
| Cross-language | long replay Python versus Rust | Exact canonical parity |
| Exact-frame shadow | shared capture into Python/Rust | Zero unexplained mismatch |
| Cross-venue core | Binance, OKX, DNSE/VN, Deribit-style fixtures | Shared core; capability-specific failures isolated |
| Capacity | mixed live, burst, replay+live | Every machine threshold passes with headroom |
| Cleanup | topics, groups, prefixes, state | No test artifact affects V1 or future authority |

### Required Evidence Artifacts

```text
upgrade/evidence/phase8-broker-topology.json
upgrade/evidence/phase8-broker-failover.json
upgrade/evidence/phase8-broker-security.json
upgrade/evidence/phase8-raw-envelope-golden.json
upgrade/evidence/phase8-rust-session-chaos.json
upgrade/evidence/phase8-stable-sharding.json
upgrade/evidence/phase8-cross-venue-conformance.json
upgrade/evidence/phase8-python-rust-parity.json
upgrade/evidence/phase8-real-provider-shadow.json
upgrade/evidence/phase8-capacity.json
upgrade/evidence/phase8-release-capacity.json
upgrade/evidence/phase8-soak.json
upgrade/evidence/phase8-authority-rehearsal.json
upgrade/evidence/phase8-release/
upgrade/evidence/PHASE8_RUST_REALTIME_CORE_REPORT.md
```

### Verification And Exit Gate

Phase 8 is `COMPLETE` only when:

- Replicated shadow transport, ACK, retention, ACL/quota, node-failure and restore
  evidence pass.
- Exact raw provider bytes, source session/generation and separate raw/canonical
  hashes are implemented and golden-tested.
- Required provider fields fail closed to quarantine rather than defaulting to
  plausible market values.
- The Rust session engine handles full-duplex lifecycle, subscription state,
  reconnect, stale generation, lease loss and bounded backpressure.
- Stable versioned shard/partition ownership passes bounded-churn tests.
- Shared core conformance passes Binance, OKX, DNSE/VN and Deribit-style
  capability inputs without venue branches in canonical identity, decimal,
  ordering, quality, replay or durable-publish modules.
- Exact-frame Binance Python/Rust shadow and authentic bounded OKX/DNSE captures
  have zero unexplained semantic/count/quality mismatch.
- Process kill, broker outage, spool exhaustion, Redis loss, slow projector and
  restart/replay tests recover without acknowledged canonical loss or ambiguous
  owner.
- Normal, burst, soak and replay-concurrent-with-live evidence passes every
  configured machine threshold with bounded resources and lag.
- The candidate Rust artifact, capability manifest, partition plan, evidence
  bundle and Python rollback manifest are immutable and signed/verified.
- All Rust output remains isolated shadow data; no public endpoint, legacy key or
  production authority is changed.

### Completed

- `8.0 COMPLETE` on 2026-08-15 for the isolated shadow substrate. Added a
  digest-pinned Apache Kafka 4.2.0 three-replica KRaft topology with
  `acks=all`, idempotence, RF3/minISR2, unclean-election disabled, mTLS,
  fail-closed ACLs, bounded resources and six separately owned raw/canonical/
  quality/authority/quarantine/audit topics.
- Added the async Rust `KafkaDurableSink`/`KafkaEventSource`. Producer cursors
  come only from broker ACK partition/offset; consumer checkpoints remain
  explicit after local offset storage. The release smoke used the Rust client,
  PEM mTLS identities and actual broker records rather than Kafka CLI alone.
- Added bounded OTel collector and alert contracts for ACK latency, produce
  failure, consumer lag, spool/disk pressure, leader churn and stalled replay;
  high-cardinality instrument/event labels are forbidden.
- Certified 65/65 acknowledged records through full restart and one replica
  volume loss. One-node loss remained writable; below-minISR writes did not
  advance durable offsets; unauthorized writes did not advance offsets; Redis
  shadow projection rebuilt byte-equivalent from replay. Full ISR returned
  after restore.
- Cleanup removed all Phase 8.0 containers, networks and volumes. V1 stayed
  HTTP 200 and its inspected topology was unchanged. Evidence:
  [broker topology](upgrade/evidence/phase8-broker-topology.json),
  [failover](upgrade/evidence/phase8-broker-failover.json),
  [security](upgrade/evidence/phase8-broker-security.json) and
  [implementation report](upgrade/evidence/PHASE80_REPLICATED_SHADOW_SUBSTRATE_REPORT.md).
- This same-host three-broker test certifies protocol, replication, fencing and
  recovery behavior for shadow development. It does not claim independent
  rack/region failure domains or authorize Phase 9 production cutover.
- `8.1 COMPLETE` on 2026-08-15. Added a generated `qdl.provider.v1`
  raw-envelope/quarantine contract with explicit capture boundary, transport
  codec, exact raw bytes/hash, session/generation, lease/authority/partition
  epochs and fixture provenance. Added wire-compatible EventEnvelope fields for
  session/generation/authority/plan and canonical payload hash.
- Added provider-neutral `qdl-provider-envelope` and `qdl-venue-core` crates.
  The core owns full session lifecycle, subscription ACK/reject state,
  heartbeat/read deadlines, lease/generation fencing, sequence/dedup/gap
  decisions, lifecycle-aware bounded backpressure and stable SHA-256 rendezvous
  assignment. Binance-specific command/ACK JSON stays in its adapter module.
- Removed plausible Binance defaults for buyer-maker, final-bar flag, last trade
  ID and trade count in both Python and Rust canonicalizers. Missing or invalid
  values now fail to quarantine upstream rather than silently becoming
  `false`/`0`.
- Added shadow-only capability manifests for Binance USD-M TRADE, OKX SWAP
  TRADE, DNSE/VN BAR and a fixture-only Deribit option BOOK boundary. None is
  authority-eligible.
- Contract format/lint and breaking checks passed against both frozen Phase 1
  and Phase 7 beta baselines. Targeted Python regression passed 17 tests; Rust
  fmt/clippy and 24 tests passed. A 10,000-instrument rendezvous test moved
  33.6% of assignments when adding a third owner and moved zero existing
  assignments when only one instrument was added. Evidence:
  [raw golden](upgrade/evidence/phase8-raw-envelope-golden.json),
  [session chaos](upgrade/evidence/phase8-rust-session-chaos.json),
  [stable sharding](upgrade/evidence/phase8-stable-sharding.json) and
  [implementation report](upgrade/evidence/PHASE81_RAW_ENVELOPE_RUST_CORE_REPORT.md).
- `8.2 COMPLETE` on 2026-08-15. Added an atomic exact-frame tee to Binance
  USD-M and OKX V5 without changing existing callback consumers, plus an
  append-only canonical `raw_capture_id` and exact raw-frame hash linkage in
  both Python and Rust. Missing linkage fails closed in shadow mode.
- Certified 189.03 seconds of concurrent authentic Binance/OKX trade traffic:
  1,855 Binance and 510 OKX events observed, with a bounded 128 captures per
  venue retained. A credential-owning DNSE acquisition edge delivered a full
  authentic 241-row `VN30F1M` session for 2026-08-14. Deribit remains explicit
  fixture-only and cannot claim live provenance.
- Replayed 498 cross-venue records 200 times (99,600 events) through Python and
  three clean Rust processes. Deterministic Protobuf bytes, identities,
  decimals, timestamps, sequences, session/generation, quality flags and
  canonical hashes had zero mismatch and zero restart divergence. Python p99
  was 0.230 ms; debug Rust exceeded the 1,000 events/s Phase 8.2 floor. Release
  capacity is deliberately deferred to the immutable 8.3 artifact.
- No public endpoint, V1 key, Redis projection or production authority was
  written. Compact checksummed evidence and the implementation report are in
  [Phase 8.2 report](upgrade/evidence/PHASE82_REFERENCE_SHADOW_CONFORMANCE_REPORT.md).
- Operator note: host `.env` DNSE credentials were stale (`OA-401`) while the
  running workload identity acquired successfully. Secrets were not copied;
  operator secret rotation must reconcile these sources independently of the
  completed canonical parity gate.
- `8.3 COMPLETE` on 2026-08-15. Built immutable image
  `qdl-phase8-rust@sha256:46a7c3fa516c0035c3ce41add0ce77e9acb4d4dfd1b0ac74130c894ca7ad5280`
  from revision `053ec76`, with pinned builder/runtime bases, non-root
  `10001:10001`, SBOM, signed checksummed provenance, exact candidate partition
  plan and Python V1 rollback manifest.
- Authority is split correctly between compacted latest state and append-only
  audit history. A replicated full-broker restart restored state revision 3 and
  audit revisions `[1, 2, 3]`; stale, public, legacy and canary-after-rollback
  writes were rejected. Final authority remained `RUST_SHADOW`.
- Release-profile replay processed 139,500 cross-venue events with zero
  semantic/byte/restart mismatch across three clean Rust processes. Python p99
  was 0.224 ms and minimum Rust release throughput was 81,710 events/s.
- Buf format/lint and both frozen-baseline breaking gates passed; 45 targeted
  Python regressions and the full Rust fmt/clippy/workspace suite passed.
  Cleanup left zero Phase 8 containers, networks or volumes. V1 stayed HTTP 200
  and its inspected topology was unchanged. See the
  [Phase 8 report](upgrade/evidence/PHASE8_RUST_REALTIME_CORE_REPORT.md).
- Post-merge closure on 2026-08-16 fixed the Python V1 runtime image without
  changing the frozen Rust candidate or starting Phase 9. The builder now
  creates `/opt/venv` at its final path, so console-script shebangs do not retain
  the obsolete `/app/.venv` interpreter after the multi-stage copy. CI executes
  the real `uvicorn` binary and checks its shebang. Frozen candidate verification
  now verifies the signed manifest, bundled SBOM and immutable artifact metadata
  without incorrectly comparing an old candidate to mutable files at repository
  HEAD; newly generated bundles still verify current repository artifacts by
  default. The CI Compose overlay also resets fixed container names, host ports
  and bind volumes so local certification cannot replace production Redis or
  mutate production data/log paths. The non-root runtime also owns an explicit
  `/home/qdl` cache/config boundary for provider SDKs and plotting imports;
  runtime code no longer retries against an absent home directory. PR #3 checks
  passed and the Phase 8 head is contained in `dev`.
- Runtime venue discovery no longer mutates tracked source configuration.
  `/app/symbols.json` is a read-only bootstrap seed; refreshed Binance USD-M
  metadata is atomically replaced under writable `data/cache/`. A cache write
  failure returns the valid provider result and raises one warning instead of
  misclassifying it as a provider outage and retrying the REST request.

### Technical Debt / Decision Gate

- Broker product, topology, retention and failure-domain cost require explicit
  infrastructure approval before 8.0 deployment.
- TRADE, BBO, L2/book and BAR are separate certification units.
- OKX JSON and SBE are separate capabilities; SBE requires entitlement, pinned
  schema/version, unknown-schema fail-closed behavior and tested JSON rollback.
- Binance reference success certifies the shared core only for declared
  capability semantics. Each venue network edge still needs provider-specific
  reconnect, rate-limit, session and capacity evidence.
- A Python acquisition edge is allowed only when provider SDK/legal/operational
  constraints justify it and it forwards through the common raw-envelope/core
  boundary.
- Phase 8 may create an authority-capable artifact, but cannot move it beyond
  fenced shadow without an independently approved Phase 9 slice.

### Rollback

- Fence and stop Rust shadow roles.
- Stop shadow broker producers/consumers for the affected test namespace without
  deleting frozen evidence required for investigation.
- Continue Python V1 production authority unchanged.
- Rebuild/remove shadow projections from the replicated log as needed.
- No public endpoint, SDK v1, legacy key/channel or venue subscription owner
  changes in Phase 8.

## 13. Phase 9 - Rust Core Canary And Progressive Replacement

**Status:** `9.0-A COMPLETE_ISOLATED`; `9.0-B COMPLETE_ISOLATED`; `9.0-C COMPLETE_CONTROL_PLANE / NO_GO_EXTERNAL`; `9.1 COMPLETE_IMPLEMENTATION / CANARY_NOT_AUTHORIZED`; an actual production `RUST_CANARY` transition remains blocked on an exact `GO` bundle

### Goal

Promote Rust from shadow to authoritative realtime ownership one explicitly
approved venue/market/product/feed/partition slice at a time. Every promotion
uses persistent compare-and-swap authority, sink-side fencing, a formal terminal
watermark handoff, exact-frame parity evidence and a rehearsed rollback.
Python remains the stable API/SDK/control/history/reconciliation platform and V1
compatibility boundary.

### Non-Goals

- Phase 9 is not a multi-venue or all-feed cutover.
- Phase 9 does not authorize two unfenced public writers for safety.
- Phase 9 does not sunset V1 automatically.
- Phase 9 does not make a low-rate Python acquisition edge invalid.
- Phase 9 does not promote BBO, L2 or BAR from TRADE evidence.
- Phase 9 does not promote OKX SBE from OKX JSON evidence.
- Phase 9 does not allow a critical consumer to depend solely on V2 before DR
  and recovery gates pass.

### Promotion Sequence

```text
Shared Rust canonical/quality/durable core
    |
    +-> BINANCE / USD-M / TRADE -> BBO -> L2 -> BAR
    +-> OKX / SPOT+SWAP / JSON TRADE -> BBO -> L2 -> BAR
    +-> DNSE/VN Python acquisition edge -> Rust BAR/quality core
    +-> Deribit option TRADE/BOOK after independent activation
    +-> future venue capability adapters
```

Each arrow represents separate capability evidence and approval. A later
capability does not inherit certification from an earlier one.

### Phase Decomposition

| Subphase | Outcome |
|---|---|
| 9.0 | Close Phase 6 infrastructure/security/observability/DR blockers and approve exact slice |
| 9.1 | `RUST_CANARY`: dual-read/compare, one public authority remains Python |
| 9.2 | `RUST_PRIMARY`: terminal-watermark cutover for one bounded slice |
| 9.3 | Hold period, rollback-window closure and independent expansion decision |

### Mandatory Preconditions

Phase 9 cannot start until:

- Replicated durable transport and restore tests from Phase 8 pass.
- Production OTel collector, dashboards, alerts and SLO/error budget are active.
- Workload identity, RBAC, network policy, external secret rotation, artifact
  signature admission and retention/entitlement governance pass.
- PostgreSQL control-plane backup/PITR and object-store restore pass.
- Regional/failure-domain DR is rehearsed on independent infrastructure.
- Redis and compatibility projections rebuild from canonical log.
- Persistent authority records and sink-side fencing pass zombie-writer tests.
- Every affected consumer is registered with owner, criticality, contract,
  freshness, recovery, SDK and rollback requirements.
- Exact candidate slice, hash range/partition plan, artifact digest, blast radius
  and immutable Python rollback manifest receive explicit approval.

### Authority Slice Identity

The authority key is at least:

```text
environment
venue
market
product_type
feed
partition_plan_epoch
hash_range_or_partition_id
schema_major
```

An authority record contains:

```text
slice_id
state
authority_revision
owner_id
lease_epoch
partition_plan_epoch
terminal_watermark
artifact_image_digest
sbom_digest
signature_identity
contract_schema_digest
normalizer_version
adapter_version
config_revision
instrument_catalog_revision
source_policy_revision
evidence_bundle_id
rollback_manifest_digest
approved_by
approved_at
hold_until
```

### Authority State Machine

```text
PYTHON_PRIMARY
    -> RUST_SHADOW
    -> VALIDATING
    -> RUST_CANARY
    -> RUST_PRIMARY

VALIDATING | RUST_CANARY | RUST_PRIMARY
    -> BLOCKED
    -> ROLLBACK_PENDING
    -> PYTHON_PRIMARY
```

Every transition uses compare-and-swap:

```text
expected_current_state
expected_authority_revision
expected_owner_id
expected_lease_epoch
expected_partition_plan_epoch
```

No operator or deployment script writes `RUST_PRIMARY=true` directly. A
transition creates an immutable audit record with previous/new state, owner,
epoch, terminal watermark, artifacts, config/schema revisions, evidence,
operator and change ticket.

### Sink-Side Fencing

Every raw/canonical/projected write carries:

```text
slice_id
owner_id
authority_revision
lease_epoch
partition_plan_epoch
```

The durable sink or authoritative projector rejects:

```text
event.owner_id != active_owner
event.authority_revision != active_authority_revision
event.lease_epoch < active_lease_epoch
event.partition_plan_epoch != active_partition_plan_epoch
```

The V1 compatibility projector applies the same authority decision before
emitting legacy output. Producer-side lease checks alone are not sufficient.

One-authority invariant:

```text
For one authoritative slice and logical offset range:
exactly one owner may create canonical/public/legacy authoritative output.
Any number of shadow readers/comparators may observe it.
```

### Formal Cutover Watermark Protocol

A `RUST_CANARY -> RUST_PRIMARY` transition executes:

1. Freeze subscription/config/partition mutation for the exact slice.
2. Verify current authority record and acquire the cutover lock/lease through
   compare-and-swap.
3. Confirm Python primary and Rust canary consume the same authentic capture
   range and parity is clean through watermark `W`.
4. Instruct the Python owner to stop accepting new ownership after its terminal
   durable commit and drain to `W`.
5. Persist Python terminal owner checkpoint, source session/generation and
   terminal watermark `W`.
6. Increment authority revision and lease epoch; set Rust owner in one durable
   CAS transaction.
7. Make the final sink/projector reject all writes from the old owner/epoch.
8. Start/continue Rust authority from `W + 1` or the documented provider-specific
   handoff boundary. Reconnect/resnapshot if provider sequence semantics require
   it.
9. Reconcile an approved overlap/boundary range by event ID, source sequence,
   decimal value, timestamp, quality and output count.
10. Enable V1 compatibility projection from canonical Rust-authoritative events.
11. Disable only the exact Python venue subscription for the promoted slice.
12. Observe the hold period with enhanced alerts and keep the immutable Python
    rollback manifest available.
13. Close the transition only after consumer, SLO, lag and parity gates remain
    clean through the hold period.

Example transition:

```json
{
  "slice_id": "prod/binance/usdm/perpetual/trade/plan-7/partition-03",
  "old_owner": "python-ingestor-v1",
  "new_owner": "rust-ingestor-v2",
  "old_epoch": 19,
  "new_epoch": 20,
  "old_authority_revision": 42,
  "new_authority_revision": 43,
  "terminal_watermark": 913880123,
  "first_new_watermark": 913880124
}
```

### Formal Rollback Protocol

Rollback is not “start Python again”:

1. Trigger `BLOCKED` or `ROLLBACK_PENDING`; fence Rust at the final sink.
2. Persist the last accepted Rust watermark, source session/generation and
   incident reason.
3. Activate the immutable Python rollback artifact/config under a new authority
   revision and lease epoch.
4. Reconnect/resnapshot according to provider semantics.
5. Replay/reconcile from the last common durable cursor and deduplicate by
   deterministic event ID.
6. Resume V1 compatibility output only from the newly active owner.
7. Verify affected consumers observe no duplicate external publication and
   recover within the approved RTO.
8. Preserve incident evidence and do not re-enter canary until a hold-down period
   and new approval prevent flapping.

### Stable Canary Selection

Canary selection hashes stable `instrument_uid` or uses a durable partition ID.
It never hashes individual events. All events that share sequence/book/bar state
remain with one owner.

Canary configuration records:

```text
partition_plan_epoch
hash function/version
selected range or partition IDs
expected instruments
owner assignments
start watermark
maximum blast radius
hold duration
rollback trigger thresholds
```

Changing partition count/hash algorithm creates a new plan epoch and separate
handoff; it is not an in-place config edit.

### Automated Guardrails And Anti-Flapping

The control plane moves a canary/primary slice to `BLOCKED` or
`ROLLBACK_PENDING` when any approved trigger fires:

```text
unexplained canonical mismatch > 0 for correctness-critical fields
undetected/open gap beyond policy
final-bar or revision mismatch > 0
durable ACK timeout or replication failure
projector/consumer lag above threshold
freshness or completeness SLO breach
monotonic queue/spool growth
duplicate external publication
authority ambiguity or stale-owner write attempt
resource headroom breach
consumer error-rate breach
```

For execution-dependent data the immediate automated action is fence/degrade/
fail closed. Automatic owner rollback versus operator-approved rollback is
declared per slice. A hold-down period prevents repeated
`Python -> Rust -> Python -> Rust` transitions near a noisy threshold.

### V1 Compatibility And Consumer Migration Registry

The V1 compatibility projector remains the public legacy writer after Rust
promotion. Existing consumers do not learn whether Python or Rust produced the
canonical event.

Each consumer record includes:

```text
consumer_id
owner
criticality
execution_dependency
feeds/instruments
minimum_data_grade
maximum_freshness
maximum_replay_lag
cursor_retention_requirement
SDK/contract version
fallback contract
kill switch
migration state
last observed V1 demand
last observed V2 demand
owner sign-off
```

Migration states:

```text
V1_ONLY
V1_WITH_V2_SHADOW
V2_PAPER
V2_PRIMARY_WITH_V1_FALLBACK
V2_ONLY
DECOMMISSIONED
```

V1 sunset requires all of:

- no registered consumer for the contract/key/channel;
- zero observed demand for the approved observation period;
- consumer owner sign-off;
- replacement and rollback documentation;
- completed retention/audit requirement;
- approved sunset release.

Rust promotion alone never authorizes V1 removal.

### Correction And Revision Protocol

Canonical corrections are append-only:

```text
EVENT_CORRECTION
TRADE_BUST
TRADE_CORRECTION
BAR_REVISION
INSTRUMENT_METADATA_REVISION
SOURCE_AUTHORITY_REVISION
```

Correction metadata:

```text
revision
supersedes_event_id
reason_code
detected_at_ns
effective_at_ns
reconciler_or_operator
evidence_reference
source_policy_revision
```

Historical materializers create a new snapshot/manifest revision. Consumers may
apply, recompute, rewind or fail closed according to `DataRequirement`; canonical
history is never silently rewritten.

### Disaster Recovery And Reconstruction

Before a critical alpha or execution consumer may depend solely on V2, Phase 9
must prove:

- Broker node and approved failure-domain failover.
- Broker restore with event/order/cursor reconciliation.
- PostgreSQL authority/consumer/config restore and PITR.
- Object-store historical snapshot/manifest restore.
- Cursor signing-key rotation and retired-key verification window.
- Complete Redis/latest/V1 compatibility rebuild from canonical log.
- Projector rebuild from checkpoints.
- Authority-state reconstruction from immutable audit records.
- Clock-skew detection and timestamp-quality degradation.
- Recovery without duplicate external publication.
- Recovery within approved RPO/RTO for each consumer grade.

### Guide Index

- [Authority ownership and no-big-bang migration: Sections 30-33](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#30-migration-strategy-no-big-bang-rewrite)
- [Production acceptance and adapter definition of done: Sections 37-41 and Appendix B](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#41-production-acceptance-checklist)
- [OKX JSON/SBE promotion boundary](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-6)
- [Phase 6 production certification decision](upgrade/evidence/PHASE6_PRODUCTION_CERTIFICATION_REPORT.md)
- [Phase 8 Rust realtime-core evidence](#12-phase-8---multi-venue-rust-realtime-core-and-reference-slice)

### To Do

#### 9.0 Production Prerequisites And Exact Slice Approval

##### 9.0-A Runtime Correctness Closure

**Status:** `COMPLETE_ISOLATED`

**Purpose:** Close correctness and deployment-boundary defects discovered after
the server migration before any Phase 9 authority implementation or Rust
canary. This slice changes no public V1 contract, source authority, canonical
writer or running production container until isolated evidence passes and a
separate operator-approved deployment is prepared.

**Observed baseline (2026-08-18):**

- The running service is the V1-only `data-layer:v0.1.0` image with host source
  bind-mounted read-write at `/app`; OpenAPI exposes 40 V1 routes and zero V2
  routes. No Phase 7 beta, Phase 8 Rust, Kafka or OTel role is active.
- Binance USD-M TRADE is producing authentic frames, but all eight configured
  USD-M KLINE shards have remained connected with `message_count=0`. All 734
  expected one-minute kline feeds are missing while `/v1/health` incorrectly
  reports `binance_kline_stream=true`.
- Independent read-only probes reproduce provider ACK/connected-without-data:
  raw trade and book-ticker produce frames, while kline, aggregate-trade and
  mark-price subscriptions time out after successful connection/subscription.
- Feed demand leases are zero while the runtime still opens the broad USD-M
  universe. Historical queue-drop count is 138,060; the measured recent
  five-minute delta is zero.
- The production container has no explicit CPU, memory or PID limit. Redis and
  V1 HTTP remain available; no production state was mutated during discovery.

**Invariants:**

- A WebSocket handshake or subscription ACK is transport state, never data
  readiness. Enabled feeds that do not produce a valid provider frame inside
  their declared first-frame/staleness deadline fail closed to a typed degraded
  state.
- Recovery uses provider-authentic Binance REST closed bars for active demand
  only. It never fabricates candles, marks the open candle final, substitutes
  OKX data as Binance authority or hides WebSocket degradation.
- Trade/book canonical events are not silently coalesced or dropped. Existing
  legacy latest-state projection behavior remains contract-compatible while
  queue loss and recovery state stay observable.
- Tests use isolated processes, Redis prefixes and Compose project names. They
  do not restart V1, flush shared Redis, mutate production Parquet or reuse live
  consumer groups. Disposable state is removed after evidence capture.

**Implementation tasks:**

1. Split stream transport, source and per-feed readiness. Report TRADE and
   KLINE independently; expose connected shard count, producing shard count,
   first-frame deadline, stale deadline, recovery source and active demand.
2. Add a bounded data-frame watchdog. A connected shard with no valid frame by
   deadline enters `DATA_UNAVAILABLE`, records the outage and reconnects with
   jittered backoff rather than remaining green forever.
3. Add one demand-scoped closed-kline recovery loop. It batches and rate-limits
   Binance REST requests, emits only fully closed rows with explicit
   `BINANCE_REST_GAP_FILL` provenance, deduplicates by symbol/interval/open time,
   and stops after demand leases expire.
4. Make demand registry ownership and TTL visible. REST/SDK demand renewal must
   not make reads fail, but missing registration cannot authorize broad source
   health or resource claims.
5. Correct `/v1/health` without changing its response keys: boolean TRADE/KLINE
   fields reflect their own data readiness; top-level status degrades for an
   enabled unavailable source while market-closed DNSE remains healthy by
   policy. Add detailed source readiness under the existing nested supervisor
   payload.
6. Add an immutable production Compose overlay with no host source bind and
   explicit CPU, memory, PID, read-only-root, tmpfs and writable data/log/cache
   boundaries. Keep the current deployment untouched; cutover requires a
   digest-pinned image, preflight and operator approval.
7. Freeze compact machine-readable evidence and an implementation report,
   including authentic probe results, unit/integration counts, V1 golden diff,
   resource limits, production-unchanged proof and cleanup.

**Implementation checkpoint (2026-08-18):**

- Implemented valid-frame watchdog, independent source readiness, bounded queue
  backpressure and demand-only Binance closed-kline REST recovery. Removed the
  reconnect-only recovery duplicate so one manager owns scheduling, dedup and
  backoff.
- Recovery preserves the provider interval from `k.i`, rejects open/invalid rows,
  retains explicit provenance and expires work with the final demand lease.
- V1 health keys remain unchanged; TRADE and KLINE booleans now represent their
  own data readiness. Added owner visibility to the existing demand snapshot.
- Added immutable isolated Compose candidate with non-root/read-only execution,
  no source bind, dedicated state, loopback-only ingress and CPU/RAM/PID limits.
- Deterministic verification passed: targeted runtime matrix 35/35; full repository
  suite ran 345 tests with 340 passes, 5 environment-gated skips and zero failures;
  compile/diff checks clean; and
  live-vs-candidate OpenAPI path diff 40/40 with zero additions or removals.
- Built and ran immutable candidate digest `sha256:4a2723ec39057c75a89889d955feac7acc6fb01bc126a579f8c74d384b9b6999` as UID 10001 with read-only root, no source bind and declared CPU/RAM/PID limits.
- Real-provider smoke proved 8/8 USD-M TRADE shards ready and 0/8 KLINE shards
  unavailable; health stayed degraded while demand-only REST recovery returned a
  final BTCUSDT bar exactly equal to Binance REST OHLCV. Lease expiry stopped
  further provider fetches. Queue pressure and drop deltas remained zero.
- The isolated smoke exposed and closed two candidate bugs before release: Redis
  UID with `cap_drop: ALL`, and data outage being cleared by transport reconnect.
- Candidate containers, networks, volumes and images were removed after evidence.
  Production V1 remained unchanged and running throughout. Evidence: [Phase 9.0-A
  report](upgrade/evidence/PHASE90A_RUNTIME_CORRECTNESS_REPORT.md) and [machine
  result](upgrade/evidence/phase90a-runtime-correctness.json).

**Verification cases:**

- Valid TRADE plus valid KLINE frames make only their matching source ready.
- Connected/ACKed KLINE with zero frames misses the first-frame deadline and is
  degraded; `binance_kline_stream` must be false.
- One dead kline shard cannot mark other feed types unavailable, and one healthy
  trade shard cannot make kline healthy.
- Stale, malformed and wrong-feed frames do not satisfy readiness.
- Active kline demand receives provider-authentic fully closed REST recovery;
  open rows, duplicates and non-demanded symbols are rejected.
- REST timeout, 429/5xx, partial batch, reconnect, Redis outage, queue pressure,
  lease expiry and process restart remain bounded and observable.
- V1 OpenAPI/golden payloads and legacy Redis keys/channels do not change.
- Isolated real-provider smoke proves the actual provider behavior and recovery
  path; generated/simulated data is limited to deterministic failure tests and
  is never counted as provider evidence.
- Immutable image runs as non-root without source bind, honors resource limits,
  passes liveness/readiness/data-readiness probes and leaves V1 unchanged.

**Exit gate:**

- Zero false-green source readiness in the verification matrix.
- Zero fabricated/open-as-final bars and zero unexplained duplicate recovery
  publication.
- Zero V1 contract/golden regression and zero production mutation.
- Bounded CPU, memory, queue, request rate and retry/backoff under normal,
  outage and recovery cases.
- All disposable containers, networks, volumes, Redis prefixes and captures are
  removed; compact checksummed evidence remains.
- Phase 9.1 remains blocked. Completing 9.0-A does not satisfy production OTel,
  independent failure-domain DR, workload identity, external secrets,
  signature admission, consumer registration or exact-slice approval.

**Rollback:**

- Do not deploy the candidate overlay; continue the unchanged V1 container.
- If a later approved deployment regresses, restore the immutable V1 image and
  source configuration, remove only the candidate namespace and verify V1
  OpenAPI/Redis compatibility plus provider data readiness.
- REST recovery can be disabled independently; source health must remain
  degraded rather than reverting to connected-is-ready semantics.

##### 9.0-B Isolated V2 Beta

**Status:** `COMPLETE_ISOLATED`

**Purpose:** Re-certify the existing provider-neutral V2 query/stream beta on
the migrated host using the Phase 9.0-A runtime-correctness baseline. The beta
is a read-only, non-authoritative consumer of one explicitly bounded V1 source
slice; it is not a Rust canary, a public-internet deployment or a source
authority transition.

**Guide index:**

- [Phase 9.0-B isolated beta boundary](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#appendix-e--phase-90-b-isolated-v2-beta-boundary)
- [V2 API/SDK and consumer migration](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#18-sdk-v2-architecture)
- [No-big-bang migration](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#30-migration-strategy-no-big-bang-rewrite)
- [Production acceptance checklist](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#41-production-acceptance-checklist)
- [Phase 7 isolated beta runbook](docs/runbooks/phase7-isolated-beta-runtime.md)
- [Phase 9.0-B certification runbook](docs/runbooks/phase90b-isolated-v2-beta.md)

**Invariants:**

- V1 remains the only production/source authority and keeps its existing
  container, networks, mounts, Redis namespaces and public contracts.
- Beta reads only the approved internal V1 endpoint and publishes only into its
  dedicated canonical spool, Redis prefix, consumer group and loopback ports.
- The beta image is content-addressed, non-root, read-only, bounded and contains
  no host source bind. JWT, cursor and bridge secrets are beta-only.
- Beta output uses provider-authentic, final closed bars. Fixtures/synthetic
  events are permitted only in isolated deterministic tests and cannot satisfy
  the real-provider gate.
- Process readiness, dependency readiness and data availability are tested
  separately. Missing/stale source data must produce typed unavailability, not
  a false-green data claim.

**Implementation tasks:**

1. Reuse the Phase 7 V2 query, active/passive stream, dedicated AOF Redis and
   read-only V1 bridge topology; do not fork or rename stable contracts merely
   because this is a new certification phase.
2. Pin the candidate and helper images by digest, stamp the application artifact
   with source revision, and use a Phase 9.0-B-specific config revision, Redis
   prefix, consumer group, project name, leases, credentials and evidence paths.
3. Run the continuous bridge only with the `phase7-canary` profile and prove it
   cannot call a venue directly, write V1 state or publish non-final bars.
4. Validate authenticated V2 warmup/query and gRPC replay/live handoff for the
   approved BTCUSDT USD-M 1m BAR slice, including decimal/timestamp/finality,
   event identity, cursor continuity and V1-vs-V2 parity.
5. Exercise active/passive failover and fencing, Redis outage/recovery, process
   restart, stale/invalid cursor, malformed/auth abuse, rate/concurrency bounds,
   slow consumer and duplicate bridge polling.
6. Measure bounded CPU, memory, PIDs, Redis/durable-store growth, request and
   stream latency. Record untested infrastructure gates honestly.
7. Tear down all beta containers, networks, volumes, images, keys and temporary
   credentials; prove V1 topology/state and API remain unchanged. Freeze a
   checksummed human and machine-readable report.

**Verification and exit gate:**

- Existing V2 contract/security/unit suites and the Phase 9.0-A regression
  matrix pass with zero unexplained domain mismatch.
- Exactly one stream replica is active; failover increments the fencing epoch
  and stale-owner operations fail closed.
- Authentic V1 and V2 closed bars match exactly for identity, interval, OHLCV,
  timestamps and finality; replay/live offsets are contiguous with no duplicate
  external event.
- Missing credentials, wrong audience/environment/scope/consumer, malformed
  requests and expired/tampered cursors fail closed with typed errors.
- Dependency failure makes readiness unavailable while V1 fallback remains
  healthy. Recovery is bounded and does not require V1 restart.
- Resource limits hold, no beta state enters production Redis, V1 OpenAPI paths
  remain unchanged, and cleanup counters are all zero.
- Completion authorizes review of an isolated V2 beta only. Phase 9.1 and any
  Rust/source authority promotion remain blocked on the mandatory production
  infrastructure and exact-slice operator gates.

**Rollback:** Stop and remove only the isolated Compose project and its volumes,
revoke beta credentials, verify zero beta keys in production Redis and continue
the unchanged V1 path. V1 requires no replay, resubscription or restart.

**Completed:**

- Reused the frozen Phase 7 V2 query/stream/bridge topology with dedicated
  Phase 9.0-B projects, AOF Redis, stores, credentials, ports and namespaces.
- Built and certified immutable candidate revision `1c881389b4ee21a153903505822c61512b176044`
  as non-root UID/GID `10001`, read-only root, no host source bind and bounded
  resources.
- Added exact provider-bar parity, continuous bridge, immutable provenance,
  adversarial/security, capacity, V1 topology and deterministic cleanup gates.
- Fixed rootless evidence ownership and complete Compose profile activation in
  the certification harness; both defects now have regression coverage.

**Verification:**

- Full Python regression: `351` tests, `346` passed, `5` skipped, `0` failed.
- Real provider slice: `BINANCE / USDM / PERPETUAL / BTCUSDT / BAR / 1m`;
  canonical mismatches `0`, generated market events `0`, duplicate timestamps
  `0`, non-final bars `0`, execution-eligible events `0`.
- Query load: normal `110.204 req/s`, burst `65.012 req/s`; `0` errors;
  p99.9 `72.211 ms` and `486.649 ms`. Stream throughput was `1815.252`
  events/s and measured end-to-end freshness was `6695.751 ms`.
- Authentication, entitlement, cursor tamper/expiry/scope, malformed/oversized
  request, rate limit, active/passive fencing, Redis outage/recovery, slow
  consumer isolation and replay continuity all passed fail-closed gates.
- Production V1 container/image/start time and OpenAPI digest were unchanged;
  beta containers/networks/volumes/image tags and production beta keys after
  cleanup were all `0`.
- Frozen evidence: [human report](upgrade/evidence/PHASE90B_ISOLATED_V2_BETA_REPORT.md),
  [machine decision](upgrade/evidence/phase90b-isolated-v2-beta.json),
  [continuous parity](upgrade/evidence/phase90b-continuous-bridge.json),
  [capacity](upgrade/evidence/phase90b-capacity.json),
  [security](upgrade/evidence/phase90b-security-adversarial.json) and
  [checksums](upgrade/evidence/phase90b-evidence.sha256).

**Technical debt / decision gate:** No in-scope Phase 9.0-B defect remains.
This result authorizes only isolated read-only beta review. Phase 9.1 remains
blocked on replicated production transport, OTel/alert routing, workload
identity/RBAC, external secret rotation, signature admission, independent DR,
complete critical-consumer registration and explicit exact-slice approval.

##### 9.0-C Production Prerequisites

**Status:** `COMPLETE_CONTROL_PLANE / NO_GO_EXTERNAL` (2026-08-18)

**Purpose:** Turn every Phase 9 production prerequisite into an explicit,
machine-verifiable, fail-closed gate. Reuse valid Phase 6/8/9 evidence without
misrepresenting same-host rehearsal as independent production infrastructure.
This subphase does not deploy a public V2 endpoint, promote Rust, change V1
authority or approve an exact slice by implication.

**Guide index:**

- [Phase 9 production prerequisite boundary](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#appendix-f--phase-90-c-production-prerequisite-boundary)
- [Deployment architecture](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#29-deployment-architecture)
- [Migration and authority](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#30-migration-strategy-no-big-bang-rewrite)
- [Operational runbooks](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#34-operational-runbooks)
- [Performance policy](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#37-performance-engineering-policy)
- [Production acceptance checklist](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#41-production-acceptance-checklist)

**Invariants:**

- A local container, SQLite spool, in-process metric buffer, debug exporter,
  self-signed test key or same-host broker replica can prove code behavior but
  cannot satisfy a production/failure-domain gate.
- Evidence is immutable, checksummed, scoped, expiring where applicable and
  attributable to an operator or workload identity. Missing, malformed,
  expired, lower-scope or contradictory evidence blocks promotion.
- Authority state is persistent and transitions by compare-and-swap. Entering
  `RUST_CANARY` or `RUST_PRIMARY` requires a passing prerequisite bundle and an
  explicit exact-slice approval; config booleans cannot bypass this rule.
- Existing V1 containers, OpenAPI, Redis namespaces and venue subscriptions
  remain unchanged throughout isolated certification.

**Implementation tasks:**

1. Add a provider-neutral prerequisite policy covering replicated transport,
   production telemetry/alert acknowledgement, workload identity/RBAC/network
   policy, external secret rotation, signed-image admission, PostgreSQL PITR,
   object-store restore, independent DR, Redis/projector rebuild, consumer
   registration/rollback and exact-slice approval.
2. Add strict evidence and exact-slice schemas plus a deterministic evaluator
   that emits `GO` only when every required production gate passes. Preserve
   local rehearsal as `LOCAL_ONLY`, never silently upgrade its scope.
3. Add additive PostgreSQL authority/prerequisite/audit tables and a CAS
   transition function with immutable audit, stale revision/owner/lease/plan
   rejection and guarded canary/primary transitions.
4. Freeze a candidate manifest for the bounded Binance USD-M BTCUSDT TRADE
   slice in `RUST_SHADOW`; public and legacy writes remain forbidden.
5. Re-run unit/contract/migration tests and applicable isolated broker,
   security, recovery and V1 compatibility checks. Test malformed, missing,
   stale, expired, forged, lower-scope and conflicting evidence.
6. Produce a compact machine/human gate report, explicit blocker inventory,
   deployment/rollback runbook and portable checksums; remove all disposable
   resources and prove V1 unchanged.

**Exit gate:**

- Code/schema/migration/evaluator and local certification may close as
  `COMPLETE_CONTROL_PLANE` while the overall decision remains `NO_GO_EXTERNAL`.
- `PRODUCTION_PREREQUISITES_PASS` requires real replicated/failure-domain,
  observability/page acknowledgement, identity/secret/admission, restore/DR,
  consumer-owner and operator approval evidence. No test fixture can satisfy it.
- Phase 9.1 remains blocked unless the exact bundle decision is `GO`, its
  artifact/config/contract digests match the candidate, and the approval names
  the exact authority slice.

**Rollback:** Keep V1 authoritative, remove only Phase 9.0-C disposable test
resources, and retain additive control-plane/audit records. Revoking or expiring
any prerequisite evidence immediately restores `NO_GO`; it never starts or
restarts a producer.

**Implementation and verification (2026-08-18):**

- Added a provider-neutral 12-gate production policy, strict candidate/evidence
  models and deterministic evaluator. Unknown gates, nested secret fields,
  duplicate bindings, unsafe artifact paths, invalid hashes/timestamps, stale or
  lower-scope evidence, semantic threshold failures and candidate mismatches all
  fail closed.
- Froze candidate digest
  `72eb1500e19a7e738373c85442c6fc42331cebd15aba86a8b746f62c2fedc037`
  at `RUST_SHADOW`; public/legacy writes remain disabled. Release provenance
  includes image/SBOM/signature, contract, normalizer, adapter, config, catalog,
  source-policy, partition-plan and rollback revisions.
- Added additive prerequisite-bundle, authority-slice and immutable transition
  audit schema. The database CAS rejects stale state/revision/owner/lease/plan,
  stale owners, missing terminal watermark, missing/expired hold windows,
  `NO_GO`, expired or candidate-mismatched bundles and audit mutation.
- Isolated PostgreSQL migration applied twice successfully. Nine negative safety
  outcomes passed, two valid state transitions were audited, production
  mutations were zero and the disposable container was removed.
- Targeted control-plane suite passed `13/13`; focused cross-phase candidate
  suite passed `29/29`; full candidate-image suite ran `364` tests with `359`
  pass, `5` intentional skips and `0` failures.
- Machine evaluation correctly produced `NO_GO_EXTERNAL`: `0/12` production
  gates passed because the available proof is local, missing or explicitly
  blocked. V1 container identity, topology and `/v1/health` remained unchanged
  and healthy before/after evaluation; no production database or Redis was
  mutated.
- Workspace governance is canonical in `/home/bobby/AGENTS.md` and its
  repository-tracked [AGENTS.md](AGENTS.md) copy; Rules 40-42 make plan
  synchronization, explicit approval boundaries and evidence-bound final
  reporting mandatory for every later slice and cloned workspace.
- Runbook and portable evidence are frozen at
  [phase90c-production-prerequisites.md](docs/runbooks/phase90c-production-prerequisites.md),
  [phase90c-production-prerequisites.json](upgrade/evidence/phase90c-production-prerequisites.json),
  [phase90c-authority-migration.json](upgrade/evidence/phase90c-authority-migration.json)
  and [phase90c-evidence.sha256](upgrade/evidence/phase90c-evidence.sha256).

**Technical debt / decision gate:** No in-scope control-plane defect remains.
The 12 blockers are deliberately external production deployment/operator gates,
not evidence that can be fabricated in this repository. Phase 9.1 remains
blocked until a fresh exact-candidate bundle evaluates `GO`; completing this
subphase does not authorize public V2, Rust canary or any authority cutover.

**Phase 9.1 prerequisites:**

- Close every applicable Phase 6 `NO-GO` blocker with real infrastructure
  evidence.
- Deploy production durable transport, OTel collector/dashboards/alerts,
  workload identity, RBAC/network policy, secret rotation and signed artifact
  admission.
- Rehearse PostgreSQL/object-store/PITR and approved failure-domain DR.
- Implement persistent authority tables/state machine, immutable transition
  audit and sink-side fencing.
- Register all consumers affected by the exact candidate slice and record
  fallback/kill-switch behavior.
- Freeze the exact venue, market, product, feed, partition-plan epoch, hash range,
  artifact digest, source policy, blast radius, hold period and rollback
  manifest.
- Obtain explicit approval naming that exact slice. A general Phase 9 approval is
  not sufficient for later slices.

#### 9.1 Rust Canary

**Status:** `COMPLETE_IMPLEMENTATION / CANARY_NOT_AUTHORIZED`

**Purpose:** Implement and certify the exact-slice Rust canary path while Python
remains the sole authoritative public/V1 writer. Because Phase 9.0-C currently
returns `NO_GO_EXTERNAL`, this phase may run only isolated rehearsal and
fail-closed authorization tests. It must not persist a production
`RUST_CANARY`, publish public/legacy output or imply production readiness.

**Guide index:**

- [Phase 9.1 canary boundary](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#appendix-g--phase-91-rust-canary-boundary)
- [Migration and authority](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#30-migration-strategy-no-big-bang-rewrite)
- [Performance policy](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#37-performance-engineering-policy)
- [Operational runbooks](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#34-operational-runbooks)
- [Production acceptance checklist](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#41-production-acceptance-checklist)

**Invariants:**

- The exact candidate remains `BINANCE / USDM / PERPETUAL / TRADE / BTCUSDT /
  partition-plan epoch 1`; a changed image, contract, catalog, source policy,
  normalizer, adapter or partition plan creates a new candidate digest.
- Python V1 remains the only public and legacy writer. Rust canary output uses a
  dedicated isolated canonical namespace; two public writers are impossible.
- Production authorization consumes the strict Phase 9.0-C decision. Missing,
  stale, `NO_GO`, mismatched or unapproved evidence cannot be bypassed by an
  environment variable, test flag or direct state boolean.
- Every canary publication binds exact slice, owner, authority revision, lease
  epoch and partition-plan epoch. Sink-side fencing rejects stale/conflicting
  writers, not only producer-side self-checks.
- Parity uses the same authentic captured provider frames and compares identity,
  exact decimals, timestamps, sequence, quality, event ID, payload hash and
  deterministic bytes. Generated events are test-only and never parity proof.
- Any correctness mismatch, open gap, duplicate external output, stale-writer
  attempt, lag/freshness/resource breach or authority ambiguity blocks the
  canary. A hold-down prevents automatic re-entry/flapping.

**Implementation tasks:**

1. Add a provider-neutral Phase 9 canary manifest and strict authorizer bound to
   the Phase 9.0-C candidate/evidence decision. Separate production activation
   from isolated rehearsal at the type/API boundary.
2. Extend the Rust authority/sink core with a versioned Phase 9 record carrying
   owner, authority/lease/partition epochs, candidate and prerequisite bundle,
   start watermark, approval and hold window. Preserve the Phase 8 V1 internal
   record decoder for compatibility.
3. Enforce sink fencing for wrong slice, owner, revision, lease, partition plan,
   target, watermark and expired/blocked state. `RUST_CANARY` permits only
   isolated canary canonical output; public and legacy targets remain denied.
4. Add deterministic same-frame parity and guardrail evaluation with bounded
   lag/freshness/resource thresholds, first-failure reason, immutable
   observations and anti-flapping/hold-down behavior.
5. Build an isolated canary certification harness using the frozen authentic
   Binance capture and replicated test broker. Exercise normal/burst/replay,
   process restart, lease loss, stale owner, broker restart/min-ISR failure,
   slow consumer, guardrail block and rollback to shadow. It must verify zero
   public/legacy writes and unchanged V1 topology/health.
6. Produce strict machine/human evidence, checksums and an operator runbook.
   Clean all disposable topics/groups/containers/networks/volumes/images and
   record exact test counts and unresolved external gates.

**Verification and exit gate:**

- Unit/contract/golden tests cover malformed manifests, candidate mismatch,
  `NO_GO`, expiry, stale authority fields, forbidden targets, guardrail triggers
  and anti-flapping.
- Python/Rust replay over the same authentic frame range has zero unexplained
  mismatch across clean process restarts and burst repetition.
- Isolated broker/recovery tests preserve one canary owner, reject stale writes,
  keep public/legacy write counts at zero and clean all test resources.
- Full Data Layer suite passes and V1 container identity, OpenAPI/health, Redis
  namespaces and source ownership remain unchanged.
- With the current Phase 9.0-C decision, the maximum allowed closure is
  `COMPLETE_IMPLEMENTATION / CANARY_NOT_AUTHORIZED`. Only a fresh exact-candidate
  `GO` bundle plus explicit operator approval can advance the production state
  to `RUST_CANARY` and begin the approved hold window.

**Implementation journal (2026-08-18):**

- `COMPLETE` control-plane slice: strict production authorizer consumes the
  exact Phase 9.0-C decision/candidate/bundle and rejects `NO_GO`, stale,
  incomplete, mismatched or V1-mutating evidence. Isolated rehearsal is a
  separate non-production authorization mode.
- `COMPLETE` guardrail slice: immutable observations, zero-tolerance semantic
  checks, bounded lag/freshness/resource checks, first-failure capture and
  explicit reset only after hold-down. Targeted result: 9/9 tests pass in a
  disposable container; production mutations remain zero.
- `COMPLETE` Rust authority/sink v2: Phase 8 record/sink v1 remains unchanged;
  v2 binds slice/owner/revision/lease/partition-plan/candidate/bundle/watermark,
  denies public/V1 targets and advances watermark only after durable ACK.
  Rust result: clippy with `-D warnings` passes; 16/16 focused crate tests pass.
- `COMPLETE` authentic same-frame parity: 128 frozen real Binance USD-M trade
  frames repeated 200 times produced 25,600 canonical events. Python and three
  clean Rust process runs matched exact record and aggregate hashes with zero
  semantic mismatch. Measured throughput was 27,115.455 events/s for Python and
  at least 350,581.025 events/s for Rust on this host; this is certification
  evidence, not a capacity promise.
- `COMPLETE` replicated-broker rehearsal: exact authority transitions were
  `RUST_SHADOW -> RUST_CANARY -> BLOCKED -> RUST_SHADOW`; one-replica-loss ACK,
  below-min-ISR fail-closed, full broker restart, compacted authority recovery,
  immutable audit ordering and 64-record slow-consumer catch-up all passed.
  Public and legacy writes remained zero.
- `COMPLETE` verification: Rust format/clippy pass; Rust workspace 32/32; focused
  Phase 8-9 Python matrix 73/73; full Python suite 381/381 with 5 intentional
  skips. Certification cleanup left zero isolated containers, networks and
  volumes; V1 health stayed HTTP 200 and topology remained unchanged.
- `COMPLETE` evidence and operations: [machine evidence](upgrade/evidence/phase91-rust-canary-certification.json),
  [human report](upgrade/evidence/PHASE91_RUST_CANARY_REPORT.md),
  [checksums](upgrade/evidence/phase91-evidence.sha256) and
  [runbook](docs/runbooks/phase91-rust-canary.md). The harness now separates
  compacted authority partition reads from audit/consumer catch-up semantics and
  uses process-loss fault injection through surviving bootstrap nodes.
- `OPEN EXTERNAL GATE`: Phase 9.0-C production infrastructure/operator evidence
  remains `NO_GO_EXTERNAL`; same-host replicas are not an independent failure
  domain. No production canary authority or V1/public mutation was performed.

**Rollback:** In rehearsal, persist a higher-revision `RUST_SHADOW` record,
fence the canary owner, reconcile the bounded cursor range and remove only the
isolated namespace. In production, use the formal rollback protocol above; do
not restart Python or edit authority state outside the CAS/audit path.

#### 9.2 Bounded Rust Primary

**Status:** `COMPLETE_IMPLEMENTATION / PRIMARY_NOT_AUTHORIZED`

**Purpose:** Implement the terminal-watermark ownership protocol required to
promote one exact, already-certified `RUST_CANARY` slice to `RUST_PRIMARY` while
preserving one authoritative writer, V1 compatibility and V2 cursor/replay
continuity. Phase 9.0-C is still `NO_GO_EXTERNAL`; therefore this phase may
certify only an isolated bounded-primary rehearsal. It must not disable a real
Python subscription, mutate production authority or write production
canonical/public/legacy destinations.

**Guide index:**

- [Phase 9.2 bounded-primary boundary](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#appendix-h--phase-92-bounded-rust-primary-boundary)
- [Formal authority model](#authority-state-machine)
- [Terminal-watermark protocol](#formal-cutover-watermark-protocol)
- [Formal rollback protocol](#formal-rollback-protocol)
- [Operational runbooks](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#34-operational-runbooks)
- [Production acceptance checklist](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#41-production-acceptance-checklist)

**Exact scope and invariants:**

- The rehearsal remains bound to the frozen Phase 9.1 candidate slice and
  candidate/partition/config/schema digests. Any changed identity creates a new
  candidate and invalidates inherited evidence.
- A terminal checkpoint is immutable and identifies the old owner, authority
  revision, lease epoch, partition-plan epoch, source session/generation,
  terminal event and final durable watermark `W`.
- Primary authorization requires an accepted handoff whose parity range is
  gap-free and mismatch-free through `W`. Rust begins authoritative output at
  exactly `W + 1`; `<= W` is stale and `> W + 1` before the boundary commit is
  an open gap.
- Authority owner, revision and lease epoch change together in one persistent
  compare-and-swap transaction. Revision advances exactly by one and a changed
  owner requires a strictly newer lease epoch.
- Final canonical sink and V1 compatibility projector independently enforce the
  same authority record. Producer self-checks are insufficient. The old owner,
  stale lease/revision, wrong plan, wrong destination and duplicate watermark
  all fail closed.
- Only `RUST_PRIMARY` may emit authoritative canonical, public V2 and legacy V1
  compatibility output for the promoted range. Shadow/canary/blocked states
  retain their narrower Phase 9.1 permissions.
- Isolated rehearsal topics may model final/public/legacy projection but are
  explicitly test-only and must be unique, disposable and counted separately.
  Production write counts remain zero.
- The exact Python subscription is disabled only after durable authority CAS,
  sink/projector acceptance and boundary reconciliation. Under current
  `NO_GO_EXTERNAL`, this action is simulated only; the real V1 topology remains
  unchanged.
- Rollback records the last accepted Rust watermark, fences Rust first, then
  grants the immutable Python rollback owner a new revision/lease and resumes
  from the next reconciled watermark. Restarting Python without authority is
  forbidden.

**Implementation tasks:**

1. Add strict bounded-primary authorization that consumes a fresh exact
   Phase 9.0-C `GO`, completed production canary hold evidence, candidate/bundle
   identity and explicit slice approval. Keep isolated rehearsal a distinct
   non-production type that cannot be converted to production authority.
2. Add immutable terminal-checkpoint and accepted-handoff contracts plus a
   PostgreSQL migration. A database trigger must prevent direct or legacy
   transition paths from entering `RUST_PRIMARY` or rollback `PYTHON_PRIMARY`
   without accepted matching handoff evidence.
3. Extend the provider-neutral Rust authority core additively. Preserve Phase 8
   v1 and Phase 9.1 v2 decoders; add a v3 primary record/state machine with
   exact `revision + 1`, strict owner/lease CAS, terminal boundary and rollback
   transitions.
4. Add final-sink and compatibility-projector fencing with independent durable
   watermark tracking per target. Authority changes between ACK and watermark
   commit must fail closed and remain recoverable by deterministic replay.
5. Build an isolated replicated-broker certification over authentic frozen
   provider frames. Exercise `N-1/N/N+1`, duplicate/out-of-order/gap input,
   stale/zombie writer, CAS conflict, crash before/after CAS, sink/projector
   restart, one-replica loss, below-min-ISR, full broker restart, slow consumer
   and bounded rollback.
6. Verify exact Python/Rust canonical parity, output counts/order/digests, V1
   projected schema behavior and V2 snapshot/cursor/replay continuity. Measure
   cutover/rollback RTO without turning the measurement into a production SLO.
7. Freeze machine/human evidence, checksums and a runbook; remove only
   Phase 9.2 disposable resources. Record V1 topology/health before and after.

**Verification and exit gate:**

- Rust unit/contract tests cover every state transition, malformed checkpoint,
  boundary off-by-one, stale owner/revision/lease/plan, wrong target, duplicate,
  gap, ACK failure and rollback path.
- PostgreSQL migration tests prove transactionality, append-only evidence,
  direct-primary bypass rejection, CAS conflict rejection and both handoff
  directions.
- Replicated-broker rehearsal has one authoritative owner, zero unexplained
  semantic mismatch, zero external duplicate/gap, ordered compatibility output
  and recovery after process/broker failure.
- Full Python and Rust suites pass. V1 OpenAPI/health, container identity,
  Redis namespaces and live subscription ownership remain unchanged.
- While Phase 9.0-C remains `NO_GO_EXTERNAL`, maximum closure is
  `COMPLETE_IMPLEMENTATION / PRIMARY_NOT_AUTHORIZED`. Production promotion
  requires a fresh exact `GO`, successful real canary hold and explicit
  operator approval; repository tests cannot fabricate those gates.

**Implementation journal (2026-08-18):**

- `COMPLETE` control and evidence contracts: production primary authorization
  requires a fresh exact Phase 9.0-C `GO`, completed production canary hold,
  immutable rollback manifest and explicit bounded-slice approval. Isolated
  rehearsal remains a separate non-production authorization type.
- `COMPLETE` additive persistence boundary: migration `0007` adds immutable
  terminal checkpoints and accepted handoffs, a primary/rollback bypass guard
  and exact handoff-aware CAS. Disposable PostgreSQL smoke passed both
  Python-to-Rust primary and Rust-to-Python rollback, rejected stale/direct CAS
  and preserved evidence across idempotent migration replay.
- `COMPLETE` Rust authority/sink core: v1/v2 decoders remain intact; v3 binds
  terminal checkpoint, accepted handoff, exact revision/lease/plan and
  independently contiguous canonical/public/legacy target watermarks. Loading
  an already-primary authority is fail-closed until each target reconstructs
  its durable contiguous watermark. Crash-before-CAS reconstruction, duplicate
  W rejection and exact W+1 restart are covered. Rust fmt/clippy pass and the
  full workspace is 40/40; the focused Python matrix is 87/87 and the full
  Python suite is 395/395 with 5 intentional skips.
- `COMPLETE` isolated replicated-broker certification: 25,600 authentic Binance
  USD-M trade events across three clean Rust runs produced zero semantic
  mismatch. The authority path completed `RUST_CANARY -> RUST_PRIMARY ->
  BLOCKED -> ROLLBACK_PENDING -> PYTHON_PRIMARY`; canonical/public V2/legacy V1
  projections are identical and gap-free for watermarks 101..181. A fresh Rust
  process recovered watermark 180 independently from all three durable targets,
  rejected writes before restore and duplicate 180 after restore, then emitted
  exactly 181. One-replica-loss ACK and below-min-ISR fail-closed passed.
- `COMPLETE` measured isolated operations and cleanup: cutover 22.200 ms, formal
  rollback 533.237 ms and delayed-consumer catch-up 24.524 s. Production public
  and legacy writes remained zero; V1 health stayed 200/200 with unchanged
  topology; disposable containers/networks/volumes ended at 0/0/0. Migration
  smoke passed both handoff directions and checksum verification passed. The
  builder now carries pinned rustfmt/clippy, and the rebuilt runtime digest
  exactly matches the certified image digest.
- `OPEN EXTERNAL GATE`: Phase 9.0-C production prerequisites and real canary
  hold remain unavailable; V1 stays authoritative.

**Rollback:** Before production authorization, remove only isolated Phase 9.2
topics/groups/containers/networks/volumes and retain Phase 9.1 code/evidence. A
future production rollback must follow the formal protocol above and may never
use a direct owner flag or uncoordinated Python restart.

#### 9.3 Hold, Close And Expand Independently

**Status:** `COMPLETE_CONTROL_PLANE / PRODUCTION_HOLD_NOT_STARTED`

**Purpose:** Add the provider-neutral post-primary control plane that observes
one exact `RUST_PRIMARY` slice, decides whether its rollback window may close,
freezes consumer/authority evidence and creates independent expansion
candidates. Phase 9.0-C remains `NO_GO_EXTERNAL` and Phase 9.2 is not production
authoritative, so this phase implements and certifies the protocol only in
isolated scope. It must not manufacture a production hold, close a real
rollback window, mutate production authority or grant another feed/venue
transitive approval.

**Guide index:**

- [Phase 9.3 hold/closure/expansion boundary](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#appendix-i--phase-93-hold-closure-and-independent-expansion)
- [Authority state machine](#authority-state-machine)
- [Formal rollback protocol](#formal-rollback-protocol)
- [Verification matrix](#verification-matrix)
- [Production acceptance checklist](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#41-production-acceptance-checklist)

**Decision boundary and invariants:**

- Hold/closure is a control-plane lifecycle around an existing exact authority
  record. It does not add a new data-plane authority state and does not weaken
  `RUST_PRIMARY`, `BLOCKED`, `ROLLBACK_PENDING` or `PYTHON_PRIMARY` fencing.
- Every observation binds slice, candidate, owner, authority revision, lease,
  partition-plan epoch and monotonically increasing time/watermark. A changed
  identity ends the hold; evidence from another owner or epoch cannot be mixed.
- Correctness breaches have zero tolerance: semantic mismatch, open gap,
  duplicate external write, accepted stale writer, authority ambiguity, durable
  ACK failure, projection divergence, consumer checkpoint regression or
  unexplained source-quality failure blocks closure.
- Capacity/freshness/lag thresholds are explicit policy. Observations are
  append-only, ordered, bounded and sufficiently dense for the approved hold
  duration. Missing intervals fail closed; they are not interpolated.
- Closing the rollback window is an immutable registry decision, not deletion
  of the Python rollback manifest. It requires a production-authorized primary,
  a completed real hold, exact healthy consumer and authority registries, a
  fresh rollback rehearsal, explicit operator/change-ticket approval and an
  unchanged authority CAS identity at commit time.
- Expansion never inherits authority or certification. More instruments,
  `BBO`, `L2`, `BAR` and another venue/market each create a new candidate digest,
  required capability matrix and independent Phase 6/9 certification set.
- Runtime decommission requires zero ownership, zero active rollback dependency,
  a closed governed window and explicit repository cleanup approval. Shared
  contracts, adapters and compatibility knowledge are retained unless a
  separate approved removal proves no consumer dependency.
- With current external gates, the maximum valid result is
  `COMPLETE_CONTROL_PLANE / PRODUCTION_HOLD_NOT_STARTED`. V1 and the Python
  authority path remain unchanged.

**Implementation tasks:**

1. Add typed hold policy, observation, decision, registry snapshot, closure
   approval and expansion manifest contracts. Validate strict schemas, identity,
   time ordering, zero-tolerance correctness fields and bounded thresholds.
2. Add a stateful hold evaluator with monotonic observation ordering, required
   sample density, exact authority continuity and sticky fail-closed breach
   behavior. Recovery requires a new hold identifier, never an in-place reset.
3. Add a production closure authorizer that rejects the current Phase 9.0-C
   `NO_GO_EXTERNAL`/Phase 9.2 rehearsal evidence and accepts only exact real
   production primary, hold, consumer, authority, rollback and operator records.
4. Add append-only PostgreSQL hold observations/decisions, authority closure and
   expansion registries. A closure function must lock and recheck the exact
   current authority record; it records closure only and never changes owner,
   state, revision, lease or watermark.
5. Add independent expansion planning for more instruments/partitions, `BBO`,
   `L2`, `BAR` and another venue/market. Persist
   `INDEPENDENT_CERTIFICATION_REQUIRED`, forbid transitive evidence and bind a
   new candidate/partition-plan identity where applicable.
6. Build disposable PostgreSQL migration smoke plus isolated control-plane
   certification. Cover clean hold, sparse/out-of-order observations, every
   zero-tolerance breach, threshold breach, stale authority CAS, registry
   mismatch, duplicate closure, immutable evidence, all expansion classes and
   decommission refusal.
7. Freeze machine/human evidence, checksum and operator runbook. Record V1
   health/topology before and after, production mutations as zero and remove
   only Phase 9.3 disposable resources/images.

**Verification and exit gate:**

- Unit/contract tests cover valid and malformed hold observations, identity and
  epoch drift, timestamp/watermark regression, missing density, correctness and
  resource breaches, sticky blocking and deterministic decision digest.
- Closure authorization tests prove current evidence is denied, complete
  production-shaped test fixtures are accepted only in test scope, and every
  missing/stale/mismatched registry, rollback or approval field fails closed.
- PostgreSQL tests prove migration idempotency, append-only evidence, exact
  authority row locking, stale CAS rejection, duplicate closure rejection and
  zero authority-field mutation after closure.
- Expansion tests prove no transitive certification, capability-specific gate
  requirements, unique candidate identity and zero write authority.
- Full Python/Rust compatibility suites pass. V1 health, API/SDK/Redis contracts,
  running topology and authority ownership remain unchanged.
- While external gates remain unavailable, production hold duration, production
  closure and real expansion authority remain explicitly untested and blocked.

**Implementation journal (2026-08-19):**

- `COMPLETE` plan boundary: scope, invariants, test matrix and Appendix I were
  frozen before code changes on `feat/phase93-hold-close-expand`, stacked on
  certified Phase 9.2.
- `COMPLETE` typed control contracts: strict hold policy/identity/observation/
  decision, frozen consumer and authority snapshots, rollback rehearsal,
  operator closure approval, independent expansion manifests and decommission
  assessment are implemented. Focused domain and migration-contract tests pass
  16/16, including every correctness/resource breach and current no-go denial.
- `COMPLETE` additive persistence and actual PostgreSQL smoke: migration `0008`
  creates append-only hold, registry, rollback, approval, closure, expansion and
  decommission records. Two holds, three observations, two decisions, one
  closure, five expansion types and two decommission decisions passed. Closure
  left authority exactly `RUST_PRIMARY:4:rust-primary:2:100`; approval and
  closure UUIDs are distinct, the frozen closure digest binds every expansion,
  and stale CAS, dirty pass, incomplete gates and all tested mutations failed
  closed. Idempotent replay, stable-readiness startup and scoped container
  cleanup passed.
- `COMPLETE` isolated certification and operator tooling: parent Phase 9.2
  provenance remains 25,600 authentic events with zero semantic mismatch; the
  accelerated hold is explicitly `TEST_CONTROL_PLANE_FIXTURE` and has no
  production authority. Current no-go is rejected, five expansion manifests
  remain independently uncertified/write-disabled, Python decommission with a
  rollback dependency is denied, and production mutations remain zero. V1
  health stayed 200/200 with unchanged topology. Focused Phase 9.3 tests pass
  20/20 and evidence checksums pass.
- `COMPLETE` import/runtime boundary: control-plane imports no longer eagerly
  load the alpha SDK. Existing `PaperAlphaCanary` and `sdk_requirement` exports
  remain API-compatible through lazy loading and passed container smoke.
- `COMPLETE` final compatibility and cleanup gates: full Python is 415/415
  with 5 intentional skips; Rust fmt/clippy pass and the full workspace is
  40/40. Final migration and certification reruns pass, evidence checksums pass,
  V1 health remains 200 with unchanged topology, production mutations are zero,
  no Phase 9.3 container/network/volume remains and the temporary Rust builder
  image was removed without global prune.
- `COMPLETE` post-closure CI hotfix (2026-08-19): GitHub run
  `32210830176` showed that PR #7 full unit tests passed, while the additive
  PostgreSQL migration step failed after one transient ready probe immediately
  preceded an init restart. `phase5_migration_smoke.sh` now requires eight
  consecutive successful probes over two seconds before `createdb`. The exact
  CI-image unit command passes 415/415 with 5 intentional skips and the failing
  migration gate passes 3/3 consecutive runs. All disposable Compose/PostgreSQL
  resources were removed; no schema, authority or running service changed.
- `COMPLETE` post-closure security hotfix (2026-08-19): PR #7 run
  `32211508679` passed unit, migration, Redis, Rust artifact and performance
  gates, then Trivy rejected nine Debian util-linux packages for fixed HIGH
  `CVE-2026-53615`. The runtime stage now applies repository security upgrades,
  installing `2.41.5-0+deb13u1` instead of vulnerable `2.41-5`. A pinned Trivy
  0.74.0 tar scan reports 0 HIGH/0 CRITICAL and the rebuilt exact CI image still
  passes 415/415 unit tests with 5 intentional skips. No CVE was ignored or
  allowlisted.
- `OPEN EXTERNAL GATE`: Phase 9.0-C remains `NO_GO_EXTERNAL`; no real primary
  owner, production hold interval or operator closure approval exists.

**Rollback:** Before production authority exists, remove only Phase 9.3 test
schemas, fixtures, evidence runtime and images; retain append-only repository
evidence. A future production closure cannot be reversed by deleting its row.
An incident still uses the formal `BLOCKED -> ROLLBACK_PENDING ->
PYTHON_PRIMARY` authority protocol and a new audit decision.

### Verification Matrix

| Area | Required cases | Pass condition |
|---|---|---|
| Authority CAS | stale expected revision/state/epoch | Transition rejected |
| Sink fencing | zombie Python/Rust write, delayed buffered write | Stale/non-owner write rejected |
| Canary | exact-frame dual read, normal/burst/reconnect | Zero unexplained semantic mismatch |
| Cutover boundary | terminal `W`, first new offset, overlap reconciliation | No acknowledged loss or duplicate external output |
| Compatibility | V1 API/SDK/Redis consumers | No behavior/shape/source regression |
| Consumer continuity | V2 snapshot/cursor/replay during cutover | No undetected gap |
| Broker failure | node loss, leader change, min-ISR failure | Authority degrades/fences correctly and recovers |
| Projector failure | Redis loss, checkpoint rollback, rebuild | Deterministic rebuild and no duplicate public output |
| Owner failure | Python/Rust crash before/after CAS | One owner and recoverable watermark |
| Rollback | Rust fenced, Python reactivated | RTO met and cursor range reconciled |
| Auto guardrails | mismatch, lag, freshness, resource breach | Block/fence according to policy; no flapping |
| Correction | trade/bar/source revision | Append-only lineage and consumer policy enforced |
| DR | broker/DB/object store/key rotation/authority rebuild | Approved RPO/RTO |
| Capacity | normal/burst/soak after authority | Headroom and bounded resources/lag |
| Cleanup | canary/test state | Only governed production state/evidence remains |

### Required Evidence Artifacts

Per promoted slice:

```text
upgrade/evidence/phase9-<slice>-approval.json
upgrade/evidence/phase9-<slice>-authority-cas.json
upgrade/evidence/phase9-<slice>-sink-fencing.json
upgrade/evidence/phase9-<slice>-canary-parity.json
upgrade/evidence/phase9-<slice>-cutover-watermark.json
upgrade/evidence/phase9-<slice>-consumer-continuity.json
upgrade/evidence/phase9-<slice>-capacity.json
upgrade/evidence/phase9-<slice>-rollback.json
upgrade/evidence/phase9-<slice>-dr.json
upgrade/evidence/PHASE9_<SLICE>_PROMOTION_REPORT.md
```

### Verification And Exit Gate

A slice is `RUST_PRIMARY` only when:

- Every applicable Phase 6 production checklist and Appendix B item passes on
  real infrastructure with authentic provider events.
- Persistent authority CAS, immutable audit and sink-side fencing reject stale
  or zombie writers.
- Exact-frame Python/Rust parity remains exact through the canary and cutover
  boundary.
- The formal terminal-watermark protocol completes with one owner, no
  acknowledged loss and no duplicate external publication.
- The exact Python subscription is disabled only after Rust ownership is
  accepted; unrelated slices are unchanged.
- V1 compatibility tests and selected consumers remain unchanged.
- V2 beta consumers continue snapshot/cursor/replay through cutover and rollback
  tests.
- Broker, Redis/projector, process crash, reconnect, gap and DR cases recover
  inside approved objectives.
- Resource, latency, queue, spool and lag measurements meet approved headroom
  without monotonic growth.
- Rollback to Python passes within the approved RTO and reconciles the affected
  cursor range.
- The hold period completes without authority ambiguity, unexplained mismatch or
  SLO breach.
- Production state, consumer registrations, immutable manifests and evidence are
  governed; all disposable canary/test resources are removed.

Phase 9 as a program remains `IN_PROGRESS` while additional slices are planned.
Completing one Rust primary slice does not mark every venue/feed complete.

### Completed

- Phase 9.0-A/9.0-B runtime and isolated V2 beta closure, Phase 9.0-C strict
  prerequisite control plane, Phase 9.1 isolated canary, Phase 9.2 isolated
  bounded-primary protocol and Phase 9.3 hold/closure/expansion control plane
  are implemented and certified at their explicitly non-production scopes.
- No production authority slice, real hold closure, expansion or Python runtime
  decommission is approved. Each requires independent operator approval and
  production-scope evidence.

### Technical Debt / Decision Gate

- Python outer-layer replacement is not a Phase 9 objective. A future change
  requires profiling and a separate contract-preserving decision.
- V1 sunset remains owner- and telemetry-based; Rust promotion does not authorize
  removal.
- Automatic rollback versus automatic fence-plus-operator-approval is selected
  per consumer criticality and slice risk.
- Regional DR topology, RPO/RTO and cost require explicit production approval.
- Deribit/options and OKX SBE remain separately licensed, entitled and certified
  activations.
- A new partition count/hash algorithm requires a new partition-plan epoch and
  migration protocol.
- Any correction semantics not covered by the canonical contract block
  authoritative use for that feed until versioned.

### Rollback

- Set the slice to `BLOCKED/ROLLBACK_PENDING` and fence Rust at the final sink.
- Persist the last accepted Rust watermark and incident evidence.
- Activate the immutable Python rollback manifest under a new authority revision
  and lease epoch.
- Reconnect/resnapshot according to provider semantics.
- Replay/reconcile from the last common durable cursor and restore V1
  compatibility output from only the Python authority.
- Verify recovery within RTO and no duplicate external publication.
- Other venue/feed/partition slices remain untouched.

## 14. Approval Gates Before Further Implementation

The original foundation is implemented through Phase 6 shadow certification.
Phases 7-9 are a new governed program and require phase/subphase approval before
runtime deployment or source-authority mutation.

### Phase 7 Approval

Phase 7 approval authorizes:

- contract/security/readiness/SDK hardening;
- isolated protected read-only beta deployment;
- monitoring and disposable paper-alpha consumers;
- real-provider read-only comparison and bounded beta load.

It does not authorize:

- V1 restart or reconfiguration;
- venue/source ownership change;
- Rust public/legacy write authority;
- live execution dependency;
- V1/legacy Redis sunset.

The deployment manifest must name beta hostname, JWT issuer/audience, cursor
keyring/TTL, monitoring consumer, paper alpha, resource namespaces and rollback
topology.

### Phase 8 Approval

Phase 8 approval authorizes:

- isolated replicated shadow durable infrastructure after cost/topology review;
- shared provider-neutral Rust core and separate runtime roles;
- Binance USD-M TRADE exact-frame reference shadow;
- bounded authentic OKX/DNSE capture comparison;
- deterministic Deribit-style conformance;
- authority-capable artifact rehearsal that remains fenced shadow.

It does not authorize any Rust public, canonical-authoritative or legacy write
authority.

### Phase 9 Approval

Each Phase 9 approval must name:

```text
environment
venue
market
product_type
feed
partition plan epoch
hash range or partition IDs
artifact/image digest
schema/normalizer/adapter/config revisions
source policy
consumer blast radius
cutover window
hold period
automatic guardrails
rollback manifest
operator/change ticket
```

Approval for one slice does not authorize another feed, venue, market or
partition range.

### Historical Foundation Approval

The original implementation began after approval of these architecture choices:

1. Use a demand-backed Binance USD-M slice instead of blindly starting with
   broad Binance Spot.
2. Treat options/Deribit as a first-class capability test while deferring actual
   venue activation until its own certification.
3. Preserve Python as the outer platform and use Rust for measured realtime hot
   paths behind stable contracts.
4. Keep V1 compatibility until per-consumer telemetry and owner-governed sunset.

Later phases may refine measured thresholds but may not weaken compatibility,
correctness, no-silent-loss, source authenticity, identity binding, durability,
fencing, recovery or cleanup gates without explicit approval.

## 15. Phase 7-9 Critical Path And Parallel Work

### Critical Path

```text
7.0 typed contract + data-plane identity + bar lifecycle + real readiness
    -> 7.1 isolated beta topology + cursor/handoff certification
        -> 7.2 monitoring/paper consumers
            -> 7.3 beta evidence freeze
                -> 8.0 replicated shadow broker + production telemetry
                    -> 8.1 raw envelope + Rust session/core
                        -> 8.2 exact-frame shadow + conformance + soak
                            -> 8.3 authority-capable fenced artifact
                                -> 9.0 production blockers + exact slice approval
                                    -> 9.1 Rust canary
                                        -> 9.2 bounded Rust primary
                                            -> 9.3 hold/close/independent expansion
```

### Parallel Work Allowed

These workstreams may run in parallel when they do not bypass the critical path:

- Phase 7 typed REST/SDK work and application auth/interceptor work.
- Phase 7 consumer manifest persistence and dependency readiness probes.
- Phase 8 broker infrastructure and raw-envelope schema/golden fixtures.
- Phase 8 stable sharding and venue capability manifests.
- Phase 8 Rust Binance session engine and cross-venue deterministic fixtures.
- Phase 9 authority schema/audit tooling and DR runbook development before the
  first authority approval, provided no source mutation occurs.

### Work That Must Not Be Parallelized Across An Authority Boundary

- Two owners writing authoritative canonical or legacy output for the same
  logical slice.
- Repartitioning during canary/cutover.
- Source-policy mutation during terminal-watermark handoff.
- SDK/public contract semantic change during consumer canary without a new
  schema digest.
- Rust primary promotion while replicated broker, sink fencing or DR evidence is
  incomplete.

## 16. Suggested Implementation Slices

Commits remain coherent and independently testable. Suggested slices:

| Slice | Scope | Mandatory evidence before merge |
|---|---|---|
| F7-01 | Typed V2 REST/SDK payloads and enums | OpenAPI/Buf/golden diff |
| F7-02 | Bar lifecycle and lifecycle-aware delivery policy | Final/revision lossless tests |
| F7-03 | REST data-plane guard and consumer manifest | Auth/entitlement matrix |
| F7-04 | gRPC interceptor and SDK credential provider | REST/gRPC identity parity |
| F7-05 | Dependency/data/authority readiness | Outage/readiness matrix |
| F7-06 | Cursor claim expansion and multi-replica handoff | Cursor/HA continuity report |
| F7-07 | Contiguous SDK checkpoint adapters | Crash/apply/checkpoint tests |
| F7-08 | Isolated beta deployment and topology rollback | V1 unchanged proof |
| F7-09 | Monitoring and paper-alpha canary | Consumer parity report |
| F8-01 | Replicated broker substrate and telemetry | Failover/security/restore |
| F8-02 | Raw provider envelope and exact-byte lineage | Cross-language golden |
| F8-03 | Rust session/core traits | Unit/conformance/chaos |
| F8-04 | Stable partition planner | Churn/epoch tests |
| F8-05 | Binance exact-frame shadow | Python/Rust parity |
| F8-06 | OKX/DNSE/Deribit-style conformance | Capability matrix |
| F8-07 | Soak/capacity and authority rehearsal | Phase 8 report |
| F9-01 | Persistent authority CAS and audit | State-machine tests |
| F9-02 | Sink/projector fencing | Zombie-writer tests |
| F9-03 | Cutover/rollback orchestrator | Terminal watermark rehearsal |
| F9-04 | Consumer migration registry and guardrails | Manifest/alert tests |
| F9-05 | First exact Rust canary | Slice-specific canary report |
| F9-06 | First bounded Rust primary | Promotion/rollback/DR report |

A slice does not merge merely because code compiles. Its relevant contract,
correctness, failure, compatibility, capacity and cleanup evidence must pass.

## 17. Machine-Readable Evidence Rules

Every Phase 7-9 JSON evidence file contains:

```text
schema
phase
subphase
status
generated_at
git_commit
image_digests
contract_schema_digest
config_revision
instrument_catalog_revision
source_policy_revision
authority_revision
partition_plan_epoch
environment
scope
commands
cases_run
cases_passed
cases_failed
cases_skipped
thresholds
measurements
unexplained_mismatches
acknowledged_loss
duplicates
open_gaps
cleanup
decision
```

Rules:

- `status = PASS` only when all required machine conditions pass.
- A skipped required case makes the relevant gate `BLOCKED`, not `PASS`.
- Tolerances are explicit fields with owner/approval; they are not hidden in
  report code.
- Provider-authentic and synthetic evidence are counted separately.
- Raw unbounded production logs and secrets are not committed.
- Evidence references immutable image/schema/config revisions.
- Cleanup records exact deleted namespaces/resources and confirms production
  state was untouched.

## 18. Final Definition Of Done

The Phase 7-9 program achieves its target when:

1. V2 is a typed, authenticated, consumer-bound, observable and recoverable data
   contract.
2. Monitoring and approved alpha consumers use snapshot/cursor/replay/live
   without undetected gaps.
3. Rust provides one shared provider-neutral realtime core with exact raw
   lineage, stable session/sequence semantics and bounded backpressure.
4. Replicated durable transport is the canonical replay source; Redis is a
   rebuildable projection/compatibility layer.
5. At least one explicitly approved feed slice completes
   `PYTHON_PRIMARY -> RUST_SHADOW -> RUST_CANARY -> RUST_PRIMARY`.
6. Persistent authority CAS and sink-side fencing prove exactly one
   authoritative writer per slice.
7. Cutover and rollback meet approved RPO/RTO without acknowledged loss or
   duplicate external publication.
8. V1 behavior remains stable until every consumer completes a governed
   migration/sunset.
9. DR reconstructs broker, authority, cursor, historical and Redis/projector
   state within approved objectives.
10. Further venue/feed expansion uses the same contracts, capability manifests
    and evidence gates without redesigning the core.


## 19. V2 Stable Rust Core And Binance/OKX/VN Equal-Source Closure

**Status:** `PHASE A COMPLETE / PHASE B IN PROGRESS (ISOLATED STABLE) / RUNTIME CUTOVER NOT AUTHORIZED`

### Purpose

Close exactly two product gaps without creating another open-ended architecture
program:

1. make Rust the authoritative realtime data core while Python remains the
   stable REST/gRPC/SDK/history/control and compatibility edge;
2. make Binance, OKX and VN markets first-class sources under the same domain,
   quality, durability and authority rules instead of treating OKX/VN as
   reference-only or second-class paths.

Stable V2 consumer contracts are independent from implementation language.
Alphas and Trading System must use the same V2 API/SDK while individual
venue/feed slices can move between Python rollback and Rust authority without a
consumer rewrite.

### Frozen Product Scope

The equal-source GA baseline is capability-equivalent rather than falsely
feed-identical. A venue is first-class when every feed it actually exposes uses
the same identity, exact-decimal/unit, lineage, quality, durability, recovery,
authority and consumer-readiness gates:

| Venue/product | TRADE | BBO | BAR | L2 |
|---|---:|---:|---:|---:|
| Binance USD-M perpetual | required | required | required | capability-gated |
| Binance Spot | required | required | required | capability-gated |
| OKX SWAP perpetual | required | required | required | capability-gated |
| OKX Spot | required | required | required | capability-gated |

| VN venue/product | TRADE | QUOTE | BAR | Provider edge | Rust core |
|---|---:|---:|---:|---|---|
| VN derivatives (DNSE primary) | required | capability-gated | required | Python/vendor SDK | required |
| VN equities (DNSE/vnstock policy) | required | capability-gated | required | Python/vendor SDK | required |

`capability-gated` is not an optional quality class: the
capability registry records the provider's real product/feed support and each
advertised feed passes the same strict gates. The platform does not fabricate
crypto-style BBO or book semantics for a VN provider/feed that has not exposed
and certified them. DNSE and vnstock remain separate providers with immutable provenance and
audited source switching; `MARKET_CLOSED` is not stale or offline.

L2 remains in the common canonical model and Rust state machine, but is not
advertised stable for either crypto venue until both Binance snapshot+delta and
OKX snapshot+`seqId/prevSeqId` resync pass the same production gate. OKX SBE,
VIP-only 10 ms books and Deribit options remain independent capabilities.

### Non-Negotiable Invariants

- Rust owns the common realtime core: raw-envelope validation, exact
  canonicalization, quantity-unit enforcement, source session/generation,
  ordering, deduplication, gap detection, backpressure, deterministic event
  identity, quality transitions and durable publication. Rust also owns native
  realtime transport/decoding for high-volume Binance and OKX feeds.
- Python owns V2 REST/gRPC and SDK facades, historical/warmup orchestration,
  consumer manifests, workload identity, readiness, observability/control,
  compatibility projection and low-rate/vendor-SDK acquisition adapters. A
  Python DNSE/vnstock edge may emit only authenticated versioned raw provider
  envelopes; it may not bypass the Rust canonical/quality/durable core.
- Python may not reimplement Binance/OKX/VN realtime domain decisions after
  promotion. It may validate/project canonical events and provide a bounded
  rollback adapter.
- Kafka-compatible durable records are the handoff between Rust and Python.
  Redis remains latest-state/compatibility projection, not lossless authority.
- Binance, OKX and VN use one canonical identity/quantity-unit/quality/authority
  contract and one durable publication boundary. Provider-native channel names
  and provider-specific missing-value conventions never leak into alpha code.
- V1 remains available during migration. No running producer, Redis namespace,
  alpha or Trading System adapter changes before isolated parity and an explicit
  cutover approval naming the blast radius.
- Public V2 version `2.0.0` freezes URI, schema, units, decimal, timestamp,
  bar lifecycle, error and cursor semantics. Internal source promotion cannot
  change that contract.

### Phase A - Unified Rust Realtime Core And Binance/OKX/VN Parity

**Goal:** Deliver one Rust realtime core with first-class Binance, OKX and VN
sources, capability-truthful feeds and a durable canonical output contract.

**Implementation:**

1. Add provider-neutral Rust subscription/session contracts and dedicated
   Binance/OKX JSON adapters behind the existing canonical envelope.
2. Implement OKX public `trades`, `bbo-tbt` and business candle channels,
   strict ACK correlation, public/business socket separation, less-than-30-
   second ping/pong, 480 control requests/hour guard, maintenance-notice
   reconnect and bounded jittered backoff.
3. Implement OKX trade aggregation identity, BBO replace-only semantics and
   candle `confirm=0/1` lifecycle with exact decimals/timestamps.
4. Add canonical quantity-unit semantics before the V2 stable freeze. Preserve
   venue-native raw quantity plus explicit unit; BAR distinguishes base, quote
   and contract volume where supplied. Never map OKX derivative contracts,
   Binance base quantity or VN shares/contracts into an ambiguous unitless
   field. Unknown/missing unit fails closed at execution-grade boundaries.
5. Route DNSE/vnstock authenticated raw envelopes through the same Rust
   canonical/quality/durable core. Preserve VN timezone/calendar, sparse/no-
   trade semantics, derivative multiplier/unit, market-closed state and provider
   switch provenance. No synthetic bar or zero-default repair is permitted.
6. Keep L2 sequencing in the shared core but fail capability activation unless
   snapshot/delta continuity and entitlement are certified.
7. Publish raw/canonical records through the existing TLS/ACL,
   idempotent-ACK Kafka client under authority fencing. Final sink and projector
   advance watermarks only after durable ACK.
8. Add Python/Rust golden parity for Binance and OKX TRADE/BBO/BAR and VN
   TRADE/BAR plus capability-gated QUOTE, including units, session/calendar
   state, malformed and missing
   fields, duplicate/out-of-order input, reconnect, stale generation, queue
   saturation and broker interruption.
9. Run bounded authentic Binance/OKX WebSocket and DNSE/VN-provider smoke with
   production writes zero; freeze provenance, counts, hashes, latency and
   resource evidence. Market closure is tested separately and never replaced by
   generated provider data.

**Exit gate:**

- zero correctness-critical Python/Rust mismatch;
- no unexplained loss, duplicate or gap;
- capability-truthful first-class matrices for Binance, OKX and VN;
- every canonical quantity has an unambiguous unit and native lineage;
- VN calendar/session/provider-fallback semantics pass strict tests;
- Rust fmt/clippy/workspace and full Python compatibility pass;
- no V1 runtime change and all disposable resources removed.

### Phase B - Stable Python Edge, Consumer Migration And Release

**Goal:** Publish Data Layer `2.0.0 Internal Stable` with Rust realtime core
behind stable Python V2 endpoints and migrate real consumers without a public
contract change.

### Phase B Execution Boundary

**Approved scope:** implement, deploy and certify an isolated `2.0.0` stable
candidate. This approval does not change V1/Rust production authority, restart
port `8100`, write current Redis namespaces, or authorize production cutover.
Cutover remains a separate transaction after the exact topology, service/image
digests, ports, credentials, volumes, affected consumers and rollback command
are presented to and approved by the operator.

**Stable topology:**

```text
Rust/Python acquisition edges
  -> Kafka raw (RF3/minISR2, mTLS/ACL)
  -> transactional Rust canonical core
  -> Kafka canonical/quarantine (replay authority)
  -> Python stable projector
       -> active fenced stream_v2 gateway -> shared bounded SQLite query cache
       -> dedicated Redis V2 latest + V1 compatibility projection
       -> Kafka consumer checkpoint only after cache + Redis success
  -> query_v2 replicas read the bounded cache
  -> stream_v2 active/passive serves snapshot + signed cursor + replay + live
```

- The stable projector never calls a venue and never creates canonical market
  semantics. It validates generated Protobuf and projects only Rust output.
- SQLite is a rebuildable bounded cache, never the replay authority. Kafka raw
  and canonical topics remain authoritative for recovery.
- Active stream gateway commits a canonical event before fan-out. Projector
  writes Redis and advances Kafka offsets only after that commit. Duplicate
  replay is idempotent across gateway, SQLite and Redis checkpoints.
- Binance, OKX and VN consumers use the same V2 API/SDK and stable catalog.
  Provider/feed capability remains truthful; unavailable feeds fail readiness.
- Dedicated stable Redis may contain root-shaped V1 compatibility keys only
  inside the isolated deployment. Current `redis_marketdata` is never touched.
- Stable runtime roles, health and data-readiness are separate. API replicas do
  not open provider sockets and one slow consumer cannot block projector or
  unrelated stream partitions.

**Phase B acceptance gates:**

1. Stable catalog/query models cover TRADE/QUOTE/BAR units, source role,
   lifecycle/finality, quality/freshness and market-session semantics.
2. Canonical-before-raw, duplicate replay, crash before/after cache/Redis/Kafka
   checkpoint, active gateway failover, Redis loss/rebuild and Kafka restart
   recover without acknowledged loss or duplicate external publication.
3. V1 OpenAPI/SDK/Redis golden compatibility remains unchanged; V2 OpenAPI and
   SDK freeze as `2.0.0` with generated artifacts current.
4. Registered monitoring, Binance alpha, OKX alpha, VN alpha and Trading System
   paper manifests complete warmup -> cursor -> replay -> live and restart with
   no direct venue connection or domain mismatch.
5. Real-provider lineage from Phase A is replayed through stable query/stream/
   compatibility projection; generated data is limited to explicit failure and
   capacity tests.
6. Full Python/Rust/Buf/OpenAPI/security/capacity suites pass, immutable images
   and compact evidence are frozen, all disposable resources are removed, and
   V1 health/topology remain unchanged.

**Rollback before cutover:** stop only the isolated stable project, revoke its
credentials and remove its dedicated topics/groups/Redis/cache volumes. V1
continues unchanged. Production rollback after a later approved cutover must
use a newer authority revision and the frozen V1 manifest; it is not exercised
by implication in Phase B implementation.

**Implementation:**

1. Replace beta/shadow product labels with stable `2.0.0` labels while
   retaining authority state as runtime metadata.
2. Add a canonical V2 deployment manifest containing Rust Binance/OKX ingestion,
   Python DNSE/vnstock acquisition edges, the shared Rust core, durable broker,
   Python projector/query/stream roles, dedicated Redis and immutable
   volumes/images.
3. Add the Python durable-broker-to-query/projector adapter. SQLite remains only
   a bounded query cache; Kafka is the replay authority.
4. Package `qdl_sdk` V2 with a durable checkpoint adapter and V1 facade. Update
   the service access guide to V2-first with a capability-truthful matrix.
5. Register and test at least one real alpha per Binance, OKX and VN, the
   Trading System market-data adapter and monitoring consumer. Paper consumers
   move one manifest at a time and retain V1 rollback.
6. Certify warmup, snapshot, stream, reconnect, replay, cursor restore,
   freshness/gap/session blocking, Redis rebuild, process/broker restart and
   bounded load on the new server.
7. Build immutable `2.0.0` artifacts and release evidence. Runtime cutover is a
   separate operator action after exact topology/rollback approval.

**Exit gate:**

- V2 API/SDK contract and generated artifacts are frozen and CI-green;
- Binance, OKX and VN real consumer cycles pass with no direct venue connection;
- Rust is the selected canonical realtime core for approved slices and Python
  remains the outer/vendor-acquisition edge;
- V1 rollback remains tested;
- V2 runtime health/readiness is dependency-derived and host-visible;
- production cutover remains blocked until the operator approves exact services,
  ports, volumes, credentials and rollback.

### Phase B Subphase Control Board

Phase B is executed as five independently reviewable subphases. A defect found
inside one subphase is recorded as a bounded repair slice under that subphase;
it does not silently open a new architecture scope. No later subphase may be
declared complete while an earlier required gate remains open.

| Subphase | Scope and exit gate | Current status | Frozen evidence/conclusion |
|---|---|---|---|
| `B.0 Contract And Stable Edge` | Catalog identity, V2 query/stream/projector contracts, consumer manifests, isolated topology and V1 compatibility | `COMPLETE` | B1-B4: 6, 32, 26 and 34 targeted tests passed; the one conditional Redis case was run separately against disposable Redis and passed. Public V1 and production runtime were unchanged. |
| `B.1 Runtime Correctness And Capacity` | Authentic acquisition, Rust canonical core, bounded projector/cache, final BAR lifecycle, lossless-vs-latest delivery and resource convergence | `COMPLETE` | B5-B8: final full Python discovery passed 478 tests with 6 explicit skips; Rust fmt/Clippy/workspace passed. A clean candidate loaded 2,000 authentic closed BARs, converged core/projector lag to 50/29, retained canonical-only bounded cache, zero quarantine and bounded Redis/app memory. Intermediate failed candidates are diagnostic evidence, not accepted releases. |
| `B.2 Controlled Consumer Acceptance` | Registered Binance, OKX, VN, Trading System and monitoring warmup -> signed cursor -> replay -> live, including session/freshness semantics | `PARTIAL_EXTERNAL` | B9-B12: crypto alpha, monitoring and Trading System paper consumers passed on immutable `df88de0`; 500 rows per crypto binding, replica-equal results and 129-779 ms live freshness. B.2-D now passes local contract/recovery closure, but DNSE production bootstrap remains blocked by official REST TCP/443 egress and the current host credential is rejected by WebSocket authentication. Synthetic or lineage-incomplete V1 data is not accepted as a substitute. |
| `B.3 Durability And Recovery` | Process generation, active/passive handoff, broker quorum loss, Redis/projection-cache rebuild, exact cursor continuity and fail-closed recovery | `COMPLETE` | B13-B19 plus the approved clean-log closure passed. Fresh RF3/minISR2 Kafka accepted real Binance/OKX data; atomic cache rebuild converged at lag 19 with observed bound 46; 12 retained partitions had zero gaps/duplicates/quarantine; signed SDK consumers reached `REPLAYING -> LIVE`; Trading System paper snapshots were fresh/execution-eligible; V1 was unchanged. Conclusion: `PASS`. |
| `B.4 Release Certification And Cleanup` | Full Python/Rust/Buf/OpenAPI/security/capacity suites, immutable one-SHA images, compact evidence, docs/runbook, exact candidate cleanup and V1 invariant | `COMPLETE` | Final code SHA `2412572` produced the non-root Python/Rust image pair; full source, contract, security, capacity, real-provider restart and signed consumer gates passed. Exact candidate resources were removed and V1 stayed unchanged. Conclusion: `PASS`; Phase B overall remains `PARTIAL_EXTERNAL` only for the pre-existing DNSE provider gate. |

#### B.2-D DNSE Production Provider Closure (`PASS_LOCAL_EXTERNAL_GATE`)

**Goal:** close the remaining Vietnam-market provider gap without weakening the
first-class Binance/OKX/VN contract. DNSE remains a Python vendor acquisition
edge, while every authenticated raw TRADE/BAR envelope continues through the
shared Rust canonical, quality and durable-publication core.

**Approved references:** the provider-neutral rules in
`upgrade/quant-data-layer-fund-grade-upgrade-architecture.md` sections 4, 14.4,
Phase A item 5 and Phase B, with operator procedure in
`docs/runbooks/dnse-production-provider-edge.md`; plus the operator-supplied local DNSE OpenAPI SDK
snapshot under `dnse_provider/`. The snapshot is protocol reference only: it has
no discovered redistribution license and contains credential-bearing examples,
so no source, secret or generated artifact from that directory may be committed.

**Scope and invariants:**

1. Add a versioned, TLS-verifying DNSE REST history transport with explicit
   proxy policy, bounded thread-safe provider quota, bounded retry/backoff with
   `Retry-After`, strict response/pagination validation and secret-redacted
   errors. The default API contract revision is `2026-07-23`, configurable by
   `DNSE_API_VERSION`; insecure SDK `CERT_NONE` behavior is forbidden.
2. Keep REST exclusively for cold bootstrap and bounded gap repair. Use the
   provider-native authenticated `ohlc_closed.1` WebSocket for live final BARs;
   TRADE and BAR enter one bounded lossless queue and are acknowledged only
   after Kafka durable publication. No synthetic/no-trade BAR is fabricated.
3. Persist an atomic, mode/catalog/acquisition/authority-bound DNSE BAR
   watermark only after complete Kafka ACK. A matching complete checkpoint
   avoids repeated cold REST bootstrap; unreadable, conflicting, partial or
   stale-authority state fails closed. Reconnect may replay equal final BARs but
   changed values at one timestamp are quarantined by the existing core.
4. Preserve VN identity, `Asia/Ho_Chi_Minh` calendar/session semantics,
   derivative contract/share units and immutable `DNSE_DIRECT` lineage. Market
   closure is healthy non-execution state, not provider outage.
5. V1 port `8100`, current Redis/Kafka namespaces, production authority and all
   running consumers remain unchanged. No service restart, runtime cutover,
   provider relabeling, production write or broad cleanup is approved here.

**Test and acceptance gates:**

- unit/contract tests for version/signature headers, TLS/proxy policy,
  timeout/429/5xx retry, malformed JSON, unequal OHLC arrays, timestamp bounds,
  duplicate/conflicting rows, non-monotonic pagination and quota concurrency;
- closed-BAR WebSocket mapping tests for exact decimal text, final 1m timestamps,
  wrong symbol/resolution, queue saturation, batched durable ACK and ACK failure;
- restart/checkpoint tests for atomic persistence, exact restore, partial ACK,
  corruption, authority/catalog/acquisition mismatch and duplicate replay;
- existing Python/Rust VN golden parity, calendar/session, stable-edge,
  deployment and compatibility suites remain green;
- bounded real-provider smoke must use authentic DNSE bytes and zero public V1
  writes. If this host still cannot reach official REST TCP/443, implementation
  can become `PASS_LOCAL_EXTERNAL_GATE`, but DNSE production bootstrap remains
  fail-closed until either DNSE permits this host egress or an approved isolated
  DNSE acquisition edge publishes authenticated raw envelopes over mTLS/ACL.

**Implementation journal:**

- `2026-08-20 DNSE REST TRANSPORT SLICE PASSED`: added a dedicated provider
  wrapper with API revision `2026-07-23`, HMAC nonce/signature, mandatory TLS and
  hostname verification, explicit opt-in environment proxy, thread-safe bounded
  quota, redacted status errors, bounded timeout/retry/`Retry-After`, response
  byte/page/row limits, strict parallel-array/OHLC/timestamp validation and
  monotonic pagination with exact duplicate/conflict handling. The legacy
  `_fetch_ohlc_raw` signature now delegates to this wrapper; its date chunk loop
  no longer skips one day between exclusive boundaries. Eight isolated,
  network-disabled unit cases passed. No service, provider, Redis, Kafka or V1
  runtime was contacted or changed.
- `2026-08-20 DNSE DURABLE CLOSED-BAR SLICE PASSED LOCALLY`: the stable VN edge
  now uses REST only for cold history and native authenticated
  `ohlc_closed.1` for live final 1m bars. TRADE and BAR share one bounded
  lossless queue; queue pressure, malformed/future/wrong-resolution BARs and
  missing durable ACK fence the source. A complete binding-wide watermark is
  persisted by fsync plus atomic rename only after all Kafka acknowledgements,
  and is bound to slice, authority, catalog and acquisition revisions. Matching
  state skips repeated bootstrap; corrupt, partial or mismatched state fails
  closed. REST and WebSocket captures keep distinct transport provenance while
  resolving to one canonical BAR binding and semantic identity in Rust.
- Compatibility remained explicit: the public V1 fallback signature and
  derivative-symbol export were preserved, its historical chunks are now
  contiguous and it raises rather than returning partial rows after terminal
  failure. Vendor SDK dispatch queues remain unbounded by default for V1; only
  the isolated V2 edge opts into bounded backpressure. Catalog/acquisition/
  capability revisions and adapter version were advanced together, and the
  compose topology mounts the existing stable-state volume without changing
  public authority.
- Verification on the exact source passed 513 Python tests with 6 declared
  skips; focused DNSE suites passed 28/28, the broader compatibility slice
  passed 78 with one conditional skip, and Rust fmt, Clippy `-D warnings` and
  the full workspace passed 63 tests with zero failures/skips. The new Rust
  oracle proves REST history and a repeated native closed-BAR callback produce
  one deterministic canonical final BAR. Final compose rendering passed with
  isolated dummy secrets and no container start. One attempted targeted rerun
  in the immutable runtime image stopped before test collection because release
  images deliberately exclude pytest; it performed no provider or data write.
- Authentic read-only preflight remains externally blocked. At
  `2026-08-20T05:25:35Z`, direct official REST TCP/443 timed out after 5.002
  seconds before TLS. WebSocket TCP/TLS succeeded, but the current host key was
  rejected as invalid before subscription. The local official SDK snapshot and
  tracked SDK use the same HMAC authentication protocol, so no insecure or
  synthetic workaround was introduced. V1 remained running and unchanged,
  reporting `MARKET_CLOSED`, a previously authenticated healthy session and
  zero queue drops; restarting it before credential rotation is unsafe.
- Cleanup removed the disposable Rust test image and temporary pytest data.
  Release/rollback images, V1, production data, Redis/Kafka and volumes were
  retained. Shared BuildKit cache was not broadly pruned because the host is
  shared; exact disposable runtime artifacts are gone. Conclusion:
  `PASS_LOCAL_EXTERNAL_GATE`, not DNSE production-authoritative. Promotion
  requires a valid dedicated key plus official REST reachability, or an
  operator-approved isolated egress edge, followed by the bounded 500-row,
  closed-BAR, restart/checkpoint and signed-consumer gates in the runbook.
  Implementation commits: `583ae82` (strict versioned history) and `5d37976`
  (durable closed-BAR acquisition, checkpoint, compatibility and runbook).

**Rollback:** revert only this bounded provider/edge commit and use the retained
`2.0.0-2412572` artifacts. Existing V1 remains the authority throughout. The
decision to deploy an egress-capable DNSE edge or promote the revised V2 slice
requires a separate operator-approved topology and blast radius.

Every subphase closure records: approved boundary, invariant, exact commands and
pass/fail/skip counts, real-provider or test provenance, resource/latency data,
runtime mutations, cleanup, V1 impact, commit SHA, remaining external gate and
one explicit conclusion (`PASS`, `FAIL`, `PARTIAL_EXTERNAL` or `NOT_STARTED`).
A health endpoint or process-up result alone cannot close a subphase.

**Phase B artifact hygiene:** after each coherent tested slice, remove its
disposable containers/networks and exact unreferenced image tags. Retain only
the running V1 artifacts, the active isolated candidate and one explicitly
named rollback generation. Build cache is cleaned at subphase boundaries using
the Data Layer builder/cache scope; broad host-wide prune is forbidden because
other repositories share this host. Every cleanup records the exact removed
objects and reclaimed bytes. B.4 performs the final one-SHA image replacement
and removes the superseded candidate/rollback artifacts after verification.

### Implementation Journal

- `2026-08-19 PLAN COMPLETE`: the original two-phase scope was frozen on
  `feat/v2-stable-rust-binance-okx`. Official OKX V5 docs were rechecked:
  public/business sockets are separate, connections with no subscription/data
  over 30 seconds need ping/pong/reconnect, control requests are bounded to 480
  per connection/hour, `trades-all` is atomic, `bbo-tbt` is replace-only, and
  stateful books rely on `seqId/prevSeqId`.
- `2026-08-19 SCOPE REFINEMENT APPROVED`: Phase A includes Binance, OKX and VN
  as equal first-class sources. High-volume crypto transport is native Rust;
  DNSE/vnstock may remain Python acquisition edges but must publish authenticated
  raw envelopes into the same Rust canonical/quality/durable core. Feed support
  is capability-truthful, not fabricated for visual symmetry.
- `2026-08-19 DOMAIN CORRECTION`: the current DNSE runtime subscribes native
  trades even though its legacy Redis channel is named `stream:vn:*` and often
  described as a quote. V2 records these as TRADE. DNSE/vnstock QUOTE is a
  separate capability and cannot be advertised until a real bid/ask feed is
  certified; legacy naming does not define the canonical domain.
- `2026-08-19 PHASE A SLICE A1 COMPLETE`: froze additive quantity-unit and
  trade-identity semantics before V2 stable release. TRADE/QUOTE/BAR/BOOK/OI/
  TICKER quantities now carry explicit base/quote/contract/share units; BAR can
  preserve native, base, quote and contract volume simultaneously. DNSE trades
  without native IDs use `DERIVED_RAW_CAPTURE`, unknown aggressor side and
  explicit missing-field/source-time quality flags. Canonical source role is no
  longer hardcoded primary, so vnstock secondary provenance remains visible.
- `2026-08-19 SLICE A1 EVIDENCE`: Buf format/lint and breaking checks passed
  against both Phase 1 and Phase 7 beta baselines. The OpenAPI semantic diff
  passed with 10 operations, 42 schemas and zero hard break. Seventeen
  Binance-USD-M/Spot, OKX-SWAP/Spot, DNSE-derivative/equity and vnstock-equity
  fixtures match exact Python/Rust golden bytes; 56 cross-phase Python tests,
  9 frozen-contract tests and the full 43-test Rust workspace passed with
  Clippy warnings denied. V1 runtime/topology was not changed.
- `2026-08-19 PHASE A SLICE A2 COMPLETE`: added the provider-neutral
  `qdl-realtime-core`, native Rust Binance/OKX raw ingestion, Python DNSE/vnstock
  and Binance REST BAR raw edges, fenced Kafka raw publication, atomic Kafka
  consume-transform-produce transactions and separate canonical/quarantine
  topics. Rust is now the only canonical realtime semantics core; Python is the
  external/vendor-acquisition edge.
- `2026-08-19 PROVIDER DEFECT CLOSED`: Binance Kline WebSocket produced zero
  frames through legacy combined, official public combined and official public
  raw probes, matching the running V1 zero-message telemetry. Binance BAR now
  uses the existing low-rate REST history boundary to fetch the latest closed
  native row, then passes through Rust. It is never fabricated or silently
  relabeled. Binance Spot BBO missing provider time uses receive time with an
  explicit `SOURCE_TIME_MISSING` flag.
- `2026-08-19 PHASE A FINAL EVIDENCE`: real-provider certification committed 26
  raw -> 26 canonical records with zero quarantine across Binance USDM/Spot,
  OKX SWAP/Spot and VN equity/derivative products. Transactional certification
  committed under one Kafka replica loss, suppressed one duplicate and routed
  one intentional sequence gap to quarantine with exact read-committed counts.
  The release core processed 100,000 events at approximately 154,139 events/s
  with p99 about 10.8 microseconds and zero loss/duplicate/quarantine. Full
  Python regression passed 433 tests with 5 conditional skips; Rust fmt/Clippy
  and all 50 tests passed, including atomic rollback for partially invalid
  multi-row provider frames; Buf format/lint/breaking and code generation passed.
  Every disposable container/network/volume/image was removed; final scoped
  cleanup also removed 2.8 GiB of Cargo target artifacts and the 2.91 GB
  `qdl-v2-rust-builder:test` image. V1 health/topology remained unchanged. See
  [Phase A report](upgrade/evidence/PHASE_A_RUST_MULTIVENUE_CORE_REPORT.md).
- `2026-08-19 PHASE B STARTED`: operator approved isolated stable
  implementation, consumer-manifest migration and immutable `2.0.0` packaging.
  Production authority/cutover was not approved. Phase B follows the execution
  boundary and acceptance gates above; every tested slice is journaled here.
- `2026-08-19 PHASE B SLICE B1 COMPLETE`: introduced the strict stable source
  catalog and provider-neutral TRADE/QUOTE/BAR query backend. Instrument
  metadata is declared once and feed bindings reference deterministic UIDs,
  preventing per-feed metadata drift. Stable admission now requires exact raw
  capture lineage, source session/generation, monotonic receive/normalize/
  publish timestamps, quantity units, trade identity, source/adapter/
  normalizer/authority revisions and final/revised BAR lifecycle where required.
  The catalog baseline contains 16 bindings across Binance USD-M/Spot, OKX
  SWAP/Spot, HNX VN derivatives and HOSE equity with truthful feed capability.
- `2026-08-19 PHASE B IDENTITY DEFECT CLOSED`: Phase A golden fixtures retained
  older arbitrary crypto UIDs and therefore cannot define the stable registry.
  Stable B1 keeps the Phase 1 deterministic `InstrumentIdentity` authority and
  refuses those legacy IDs. New Phase B provider evidence must be captured with
  the stable catalog identities before release; validation was not weakened.
  VN30F1M is modeled explicitly as a continuous FUTURE series with roll-policy
  metadata, while dated futures still fail closed without an expiry.
- `2026-08-19 PHASE B SLICE B1 VERIFICATION`: six isolated, network-disabled
  tests passed for deterministic identity, unknown-field/provenance rejection,
  continuous-vs-dated future semantics, exact TRADE/QUOTE/BAR units, crypto gap
  blocking, sparse VN/market-closed behavior and consumer-bound signed replay
  cursors. The test found and closed a query defect where `latest()` could miss
  a gap immediately before its one-row window; latest/history now evaluate gap
  state across all retained cache rows. No runtime container, Redis namespace,
  consumer or production data was changed; the disposable test container was
  removed by `--rm`.
- `2026-08-19 PHASE B SLICE B2 COMPLETE`: implemented the Kafka-authoritative
  Python stable edge around the Rust core. The read-committed consumer disables
  auto commit/store; the projector durably stores raw envelopes, holds bounded
  canonical-before-raw records per broker partition, commits canonical data to
  the active fenced stream gateway/shared cache, atomically projects dedicated
  Redis latest/V1 compatibility state and Pub/Sub, and only then advances the
  Kafka checkpoint. Assignment changes discard only local uncheckpointed queues
  for broker replay. Stale projection epochs raise a hard fence rather than
  masquerading as an idempotent duplicate.
- `2026-08-19 PHASE B COMPATIBILITY DECISION`: V1 Redis aliases are an explicit
  per-binding policy. Binance USD-M owns the generic `BTCUSDT` trade/bar aliases;
  Binance Spot gets market-qualified trade aliases only, so the stable candidate
  cannot recreate the former mixed-writer race. OKX has no fabricated V1 Redis
  alias and migrates through V2. DNSE TRADE may project the frozen `vn:quote:*`
  compatibility shape only in the dedicated stable Redis; canonical semantics
  remain TRADE. Current production Redis is never addressed.
- `2026-08-19 PHASE B SLICE B2 VERIFICATION`: 32 isolated tests passed with one
  conditional Redis test skipped in the network-disabled run, covering B1/B2,
  V2 API compatibility and beta-runtime regression. A separately named,
  disposable Redis 7.2 container passed the real Lua/TTL/PubSub/idempotency/
  fencing test; exact test keys were removed, then the container and network
  were deleted. Failure injection passed canonical-before-raw, Kafka checkpoint
  failure/replay, Redis loss/rebuild, source alias isolation, HMAC rejection,
  duplicate HTTP ingest and stale-writer fencing. No test used provider data as
  generated production evidence and no live V1 process/state was mutated.
- `2026-08-19 PHASE B SLICE B3 STARTED`: freeze the real-consumer migration
  registry and public package metadata for `2.0.0`. The bounded slice covers
  monitoring, one Binance paper alpha, one OKX paper alpha, one VN paper alpha
  and the Trading System paper adapter. Every manifest must resolve only
  catalogued instrument/feed pairs, use SDK major 2, retain an explicit V1
  rollback contract and remain `SHADOW` with `cutover_authorized=false`.
  Package, SDK and generated OpenAPI versions must agree exactly; V1 OpenAPI,
  SDK and Redis compatibility goldens must remain byte/semantic compatible.
  This slice does not deploy a runtime, migrate a consumer, change authority,
  restart port `8100` or write any current Redis namespace.
- `2026-08-19 PHASE B SLICE B3 COMPLETE`: added a strict migration-plan
  loader and five governed paper manifests for monitoring, Binance alpha, OKX
  alpha, VN alpha and Trading System. Every data requirement resolves to the
  deterministic stable catalog; only Trading System may declare
  `PAPER_ONLY` execution dependency and all other consumers remain
  `FORBIDDEN`. Unknown fields, unknown instrument/feed bindings, active
  routes, weak rollback policy and implicit cutover fail closed. Package,
  `qdl_sdk` and generated OpenAPI metadata now agree on `2.0.0`.
- `2026-08-19 PHASE B SLICE B3 VERIFICATION`: 26 isolated contract,
  consumer-state, stable-edge and V1/V2 golden tests passed; one real-Redis
  integration case was intentionally skipped in this network-disabled suite
  because the same Lua/TTL/PubSub/fencing path passed against disposable Redis
  in Slice B2. The source was mounted read-only with a tmpfs log path. No V1
  service, authority, consumer route, Redis state or provider connection was
  changed, and no disposable file/container remained.
- `2026-08-19 PHASE B SLICE B4 STARTED`: build the isolated stable deployment
  manifest and immutable runtime configuration around the committed B1-B3
  contracts. Broker topology reuses the certified RF3/minISR2 mTLS/ACL
  substrate; Rust native acquisition and transactional canonical core consume
  deterministic stable catalog identities; Python query, active/passive stream
  and projector remain provider-free outer roles. Native channel mapping is a
  separate, strictly validated acquisition contract and cannot redefine
  instrument identity or canonical semantics. All names, ports, topics,
  credentials, volumes, Redis prefixes and consumer groups are isolated.
  Readiness must expose Kafka, dedicated Redis, cache, lease, catalog and
  manifest dependencies. Deployment/certification may create only disposable
  `qdl_v2_stable_candidate` resources; current V1 port `8100`, Redis and
  consumers remain outside the topology and production cutover is not approved.
- `2026-08-19 PHASE B SLICE B4 COMPLETE`: added the self-contained isolated
  stable topology, deterministic acquisition-to-core deployment contract,
  immutable image-ID candidate bundle generator and idempotent RF3/minISR2
  TLS/ACL broker bootstrap. Sixteen catalog bindings map one-to-one across
  Rust-native Binance/OKX, Python latest-closed Binance BAR and Python DNSE
  SDK/REST acquisition; only Rust creates canonical semantics. Python roles are
  non-root/read-only/bounded, state volume ownership has an isolated preflight,
  query has two replicas, stream is active/passive, projector health is
  dependency-derived and V1/current Redis names are absent.
- `2026-08-19 PHASE B B4 DEFECTS CLOSED`: stable Redis quota namespaces were
  incorrectly rejected by the beta-only prefix guard and are now admitted only
  under explicit `qdl:stable:v2:` isolation. Multi-replica projector recovery
  could also stall when one replica persisted raw while another held canonical;
  bounded idle polling now rechecks the shared cache without advancing Kafka
  before downstream ACK. DNSE acquisition never evicts queue entries: pressure
  or Kafka ACK failure fences the source for supervised recovery.
- `2026-08-19 PHASE B SLICE B4 VERIFICATION`: 34 isolated acquisition,
  identity, authority, topology, failure, V1/V2 edge and release contract tests
  passed with one separately-certified Redis integration skip. Docker Compose
  rendered successfully with required secrets supplied only as disposable
  validation values. Tests covered one-to-one source mapping, deterministic
  bundle hashes, no secret disclosure, closed-BAR dedup, zero-volume VN BAR,
  queue-pressure fencing, cross-replica wake-up and rejection of primary/public
  authority. No runtime was deployed and no disposable resource remained.
- `2026-08-19 PHASE B SLICE B5 STARTED`: certify the committed `467bbf3`
  candidate with immutable Python/Rust image IDs on the migrated 8-core host.
  The isolated run must exercise RF3/minISR2 broker policy, transactional Rust
  raw-to-canonical processing, Python projector/cache/dedicated Redis, active/
  passive stream handoff, both query replicas and all five SHADOW manifests.
  Real-provider records or replay of Phase A durable provider captures are
  mandatory for market-data evidence; generated fixtures are restricted to
  failure/capacity injection. Broker loss, Redis rebuild, process restart,
  cursor recovery and rollback cleanup must leave V1 topology/health unchanged.
  Any runtime defect is fixed and retested before release evidence; cutover
  remains explicitly unauthorized.
- `2026-08-19 PHASE B B5 RUNTIME DEFECT SLICE VERIFIED`: the first isolated
  immutable-candidate boot exposed three fail-closed deployment defects before
  any V1 or current-Redis write. Kafka brokers mounted `/tmp` with `noexec`, so
  Zstd JNI could not load and raw producers received opaque `UNKNOWN` delivery
  failures; only Kafka now receives a bounded `exec,nosuid,nodev` tmpfs. The
  projector still mounted its old host identity path while its contract pointed
  at the role-scoped `stable_tls` volume; all runtime identities now use the
  root-copied, UID/GID `10001`, mode `0440`, read-only role volume. Finally, the
  internal sink allowlist knew the retired single `stream_v2` name but not the
  fixed active/passive roles; it now admits exactly `stream_v2_active` and
  `stream_v2_passive` while continuing to reject HTTPS, redirects and external
  hosts. Targeted deployment/edge verification passed 22 tests with one
  separately gated disposable-Redis integration skip. A new immutable candidate
  must be built from the committed fix SHA before runtime certification resumes.
- `2026-08-19 PHASE B B5 FINAL-BAR DOMAIN DEFECT CLOSED`: real OKX
  `candle1m` traffic carries both provisional `confirm=0` and closed `confirm=1`
  rows. The catalog already required final bars, but this policy was not included
  in the generated Rust binding, so provisional canonical output reached and
  correctly failed the Python projector gate. `require_final_bar` is now carried
  catalog-to-core; Rust transactionally consumes raw provisional updates, counts
  them as `filtered`, publishes no canonical/public event and does not mislabel
  expected venue lifecycle as quarantine. Only final/revised BARs proceed; a
  final-only policy attached to a non-BAR payload remains quarantined. Rust fmt
  and warnings-as-errors Clippy passed, the full Rust workspace passed 51 tests,
  and 22/22 targeted Python deployment/edge tests passed with one separately
  gated disposable-Redis integration skip. Runtime retest requires images built
  from the next committed SHA.
- `2026-08-19 PHASE B B5 INGRESS DEFECT CLOSED`: application readiness was
  `READY` container-locally, but services attached only to Docker
  `stable_internal`; Docker retained declared bindings without creating host
  forwarding. The approved Phase 7 separation is restored: query and active/
  passive stream roles join a dedicated `stable_ingress` bridge and publish only
  on `127.0.0.1`; Kafka, Redis, Rust core and projector remain unexposed, with
  projector internal-only. Provider roles alone retain `stable_egress`. Targeted
  topology/deployment/edge verification again passed 22 tests with the one
  separately gated Redis integration skip.
- `2026-08-19 PHASE B B5 ACQUISITION RELIABILITY DEFECT FIXED, RUNTIME
  RETEST PENDING`: authenticated V2 smoke correctly returned `DATA_STALE` for
  Binance USD-M after the Rust provider process had exited on a real WebSocket
  `Connection reset by peer`. Native Binance/OKX sessions now reconnect with
  capped exponential jittered backoff, reset failure streak only after durable
  raw ACK, and a top-level supervisor restarts transient decode/subscription/
  transport failures without weakening configuration or authority fail-closed
  gates. OKX public/business futures are fail-fast joined so one dead channel
  cannot be masked by the other. Every raw Kafka append retains identical
  provider bytes, capture/event identity and receive time across retry; retryable
  and capacity failures backpressure indefinitely until ACK or stop, while
  fencing/configuration failures remain non-retryable. Bounded-test reservations
  are released on non-retryable append failure. Rust fmt, warnings-as-errors
  Clippy and all 51 workspace tests passed. A forced disconnect/reconnect smoke
  on an immutable image is still required before this defect gate closes.
- `2026-08-19 PHASE B B5 PROJECTOR THROUGHPUT DEFECT IDENTIFIED, FIX IN
  PROGRESS`: the immutable `5f32238` candidate keeps all four native providers,
  the transactional Rust core and the Python projector alive, and both raw and
  canonical Kafka offsets advance from real provider traffic. Authenticated
  Binance USD-M V2 snapshot nevertheless fails closed as `DATA_STALE` because
  `stable-projector-v1` accumulates thousands of records of lag. The root cause
  is one synchronous Kafka commit round-trip after every individually durable
  raw and canonical ACK. The correctness invariant remains unchanged: no Kafka
  offset may be eligible for commit before local raw durability or complete
  canonical stream/cache/Redis projection. The bounded fix is confined to the
  Confluent adapter: coalesce only already-ACKed offsets per topic-partition,
  submit asynchronous commits at a bounded count/time interval, surface commit
  callback errors fail-closed, discard uncommitted batches on rebalance for
  idempotent replay, and synchronously flush on orderly close. Unit failure
  gates and a real-provider lag-delta retest are required before this slice can
  close. V1, current Redis, public contracts and authority remain untouched.
- `2026-08-19 PHASE B B5 PROJECTOR CHECKPOINT FIX UNIT-VERIFIED, RUNTIME
  RETEST PENDING`: the Confluent adapter now batches at most 128 downstream-ACKed
  offsets or 100 milliseconds per topic-partition. Async callback failures stop
  subsequent poll/checkpoint operations, assignment changes discard only the
  uncommitted local batch for idempotent broker replay, and orderly close uses a
  synchronous final flush. The engine still calls `checkpoint` only after raw
  spool durability or complete canonical stream/cache/Redis projection. Thirty
  deployment, stable-edge and release tests passed with one separately gated
  real-Redis integration skip, including no premature checkpoint, coalesced
  offsets, rebalance replay and callback fail-closed. An immutable Python image
  plus real-provider lag-delta smoke remains required before closure.
- `2026-08-19 PHASE B B5 TRANSACTIONAL CORE RECOVERY DEFECT IDENTIFIED, FIX
  IN PROGRESS`: a deliberately isolated broker-loss gate exposed a second
  recovery boundary after the projector throughput fix. RF3/minISR2 preserved
  the topics and Rust acquisition continued appending raw provider bytes, but
  the transactional core stopped advancing canonical/quarantine offsets after
  its transaction coordinator connection was interrupted. Public V2 correctly
  returned `DATA_STALE`; there was no fabricated freshness, data loss or V1
  effect. The accidental Kafka1 exit code 137 was caused by running a separate
  Java admin CLI inside the broker 512 MiB cgroup, not by broker steady-state
  usage; all subsequent admin probes must use the isolated `stable_admin` role.
  The bounded runtime fix must reconstruct the complete transactional bridge and
  in-memory normalization state under the same authority/transactional identity
  only for retryable Kafka transport errors, with capped jittered backoff and
  explicit generation telemetry. Configuration, fencing, schema, decode and
  domain errors remain non-retryable. Kafka exactly-once offsets then replay
  only the uncommitted batch. A real one-broker loss/rejoin gate must prove raw,
  canonical and projector offsets resume and authenticated snapshots return
  `LIVE` before closure.
- `2026-08-19 PHASE B B5 TRANSACTIONAL CORE SUPERVISOR UNIT-VERIFIED, RUNTIME
  RETEST PENDING`: `qdl-realtime-core` now owns a generation supervisor around
  the complete transactional bridge and normalization state. Retryable/capacity
  Kafka errors rebuild the producer, consumer and core under the unchanged
  authority record and transactional ID after capped jittered backoff; Kafka
  transaction recovery resolves any indeterminate batch before the consumer
  resumes from committed offsets. Non-retryable transport and all non-Kafka
  domain/schema/decode failures still terminate. Generation is emitted in
  bounded progress telemetry. The pinned Rust build, rustfmt, Clippy with all
  warnings denied and the retry-class unit test passed. Immutable-image broker
  loss/rejoin evidence remains the closure gate.
- `2026-08-19 PHASE B B5 TRANSACTION OUTPUT THROUGHPUT DEFECT IDENTIFIED, FIX
  IN PROGRESS`: after transaction-state recovery, the shared core resumed from
  the committed watermark but could not catch live raw traffic. Measurement
  showed the cause inside the transaction boundary: a batch of up to 256
  canonical/quarantine outputs was delivered with one awaited Kafka network
  future at a time. The fix retains the same bounded batch, idempotent producer,
  per-partition order, output validation, atomic offset commit and all-or-nothing
  abort, but enqueues the batch delivery futures concurrently before awaiting
  them. No new dependency or public behavior is introduced. Atomic rollback,
  ordering, duplicate and real-provider lag-delta gates must pass before close.
- `2026-08-19 PHASE B B5 CONCURRENT TRANSACTION DELIVERY UNIT-VERIFIED,
  RUNTIME RETEST PENDING`: the Rust bridge now polls all bounded delivery futures
  for one transaction concurrently, while Kafka idempotence and max in-flight
  preserve partition ordering and the existing transaction still atomically
  commits output plus source offsets or aborts the whole batch. Pinned release
  compile, rustfmt, all-target Clippy with warnings denied and all six qdl-kafka
  tests passed. No dependency, topic, schema or authority changed. A new
  immutable image must prove catch-up under real provider traffic and repeat
  broker-loss recovery before this gate closes.
- `2026-08-19 PHASE B B5 OKX BBO DOMAIN DEFECT IDENTIFIED, FIX IN PROGRESS`:
  bounded quarantine inspection found repeated `SEQUENCE_GAP` only on OKX
  `bbo-tbt`. The approved OKX V5 guide defines this channel as a replace-only
  best-bid/offer snapshot and explicitly forbids requiring `prevSeqId`; strict
  continuity belongs to incremental book channels. Stable acquisition had
  incorrectly assigned `CONTIGUOUS`, effectively requiring `seqId + 1` and
  creating false gaps. Both OKX SWAP and Spot BBO bindings will use provider-
  neutral partition/receive ordering (`NONE`) while retaining native `seqId` in
  canonical provenance and deterministic event identity. Incremental book
  policies and generic contiguous-order tests remain strict. Real OKX BBO must
  then show canonical output without sequence-gap quarantine.
- `2026-08-19 PHASE B B5 OKX BBO POLICY UNIT-VERIFIED, RUNTIME RETEST
  PENDING`: both stable OKX SWAP/Spot `bbo-tbt` bindings now use replace-only
  partition ordering, and acquisition validation rejects any future attempt to
  attach `MONOTONIC` or `CONTIGUOUS` native-sequence policy to `okx_bbo`.
  Generic contiguous-order behavior remains unchanged for incremental channels.
  Thirty deployment, stable-edge and release tests passed with one separately
  gated real-Redis integration skip. Real-provider quarantine/canonical deltas
  remain required after rebuilding the runtime bundle.
- `2026-08-19 PHASE B B5 PROJECTOR DURABILITY THROUGHPUT DEFECT IDENTIFIED,
  FIX IN PROGRESS`: the clean immutable `3dc58cf` candidate proves real Binance
  and OKX snapshots with correct canonical provenance and zero quarantine, but
  `stable-projector-v1` lag is not bounded under the subscribed real-provider
  burst. Measurement shows the per-event path performs one fsync-backed raw
  cache transaction, one fsync-backed canonical cache transaction through the
  active stream gateway, one signed HTTP request and one Redis Lua projection
  before checkpoint eligibility. Kafka remains the replay authority and SQLite
  durability must not be weakened. The bounded fix therefore batches existing
  domain operations rather than changing semantics: poll a small time/count
  window, append raw and canonical records with `append_many`, preserve order
  per partition, send one signed internal ingest batch, project atomically per
  event, and coalesce only fully downstream-ACKed Kafka offsets. Any partial
  sink/projection/checkpoint failure leaves the batch replayable and idempotent;
  stale leases, lineage mismatch, collision, capacity and unknown schema still
  fail closed. Unit ordering/failure/replay tests plus real-provider lag,
  quarantine, resource and recovery gates are mandatory before closure.
- `2026-08-19 PHASE B B5 PROJECTOR DURABLE MICRO-BATCH UNIT-VERIFIED,
  RUNTIME RETEST PENDING`: the projector now polls at most 128 records over a
  25 ms bounded window, persists raw records with one `append_many` transaction,
  joins canonical records without changing per-partition order, sends one
  signed internal ingest batch to the fenced active gateway, persists canonical
  records with one fsync-backed transaction and pipelines the unchanged atomic
  Redis Lua projection. Kafka checkpoints remain after the complete downstream
  ACK chain. Assignment changes discard only uncheckpointed memory; collision,
  stale lease, unknown lineage and capacity gates remain fail-closed. Existing
  single-event methods delegate to the batch path, preserving callers. Focused
  tests prove two raw plus two canonical records use exactly two durable writes,
  one projection batch and ordered raw-before-canonical checkpoints; injected
  batch projection failure produces no canonical checkpoint and succeeds on
  idempotent replay. Thirty-two Phase B edge/deployment/release tests passed with
  one separately gated real-Redis integration skip; Python compile and
  `git diff --check` passed. Immutable-image real-provider lag/I/O, Redis,
  failover and broker recovery evidence remains mandatory.
- `2026-08-19 PHASE B B5 RUST CORE CAPACITY DEFECT IDENTIFIED, FIX IN
  PROGRESS`: after projector batching, real raw ingress measured about 392
  events/second while one ordered transactional core sustained about 150
  events/second. Its final batches were near the 256-event bound and the process
  saturated one CPU; raising its cgroup from 0.75 to 1.5 CPUs did not remove lag,
  proving a single-loop topology bottleneck rather than insufficient host
  headroom. Increasing batch size enough to hide the gap would add seconds of
  market-data latency and is rejected. The stable topology will instead run
  three explicit Rust core workers over the six Kafka partitions. Workers share
  only the consumer group and authority, while each has a deterministic client,
  shard and transactional ID so Kafka preserves one owner per partition and
  exactly-once output/source-offset transactions without fencing peers. Runtime
  bundle and Compose tests must reject duplicate identities or a worker count
  exceeding topic partitions. Real traffic must show bounded/decreasing total
  group lag, zero quarantine and no duplicate canonical/public writes before
  this gate closes.
- `2026-08-19 PHASE B B5 THREE-CORE TOPOLOGY UNIT-VERIFIED, RUNTIME RETEST
  PENDING`: the stable bundle now emits three explicit core configs over six
  Kafka partitions. Worker `001` preserves `core.json`; workers `002/003` use
  dedicated files. All share `qdl-v2-stable-core-v1` but have unique static
  client, shard and transactional IDs, the same pinned authority and identical
  provider-neutral normalization bindings. Compose defines three non-root,
  read-only, bounded Rust services without public ports. Contract tests reject
  worker indices outside the topology and verify identity uniqueness, common
  group/authority and worker-count-to-partition bounds. The rendered Compose is
  valid and 32 Phase B deployment/edge/release tests passed with one separately
  gated real-Redis integration skip. An immutable runtime must still prove lag
  convergence, exactly-once broker recovery and no quarantine/duplicates.
- `2026-08-19 PHASE B B5 RUST CORE HOT-PATH DEFECT IDENTIFIED, FIX IN
  PROGRESS`: partition-level evidence showed one hot ordered raw partition can
  still miss the 2-3 second execution freshness policy. Code inspection found
  an algorithmic defect: every raw event clones the complete global dedup set,
  dedup eviction queue, ordering tracker and partition-sequence map solely to
  roll back a multi-row provider frame. With a one-million-event dedup bound,
  processing cost grows with history and eventually becomes quadratic. The fix
  will stage only mutations for the current provider frame: scalar partition
  sequence, per-partition ordering transition and new event IDs. Those stages
  commit only after every row passes canonicalization/finality/ordering; a row
  failure discards them and emits the same atomic quarantine. Existing global
  dedup, bounded eviction, session reset, duplicate, monotonic/contiguous gap
  and stale-generation semantics must remain byte/domain equivalent. Rust unit,
  atomic multi-row rollback, parity, warnings-as-errors, benchmark and real hot-
  partition freshness gates are required before closure.
- `2026-08-19 PHASE B B5 RUST CORE STAGED HOT PATH UNIT/CAPACITY VERIFIED,
  IMMUTABLE RUNTIME RETEST PENDING`: realtime-core no longer clones global
  partition sequences, ordering partitions or the bounded dedup set/queue for
  every provider event. It stages only current-frame partition sequences,
  ordering transitions and new event IDs, then commits after every expanded row
  passes; a failed row discards the stage and preserves atomic quarantine/replay
  behavior. Two direct staged-ordering tests prove discarded state is invisible
  and committed batches preserve duplicate/gap semantics. All 30 targeted
  `qdl-venue-core`/`qdl-realtime-core` tests passed, including aggregated-frame
  rollback, reconnect dedup, stale generation, sequence-gap, final-bar and
  provider identity cases. The first unchanged 100,000-event/50,000 events/s
  benchmark gate correctly failed at 6,177 events/s and exposed one residual
  clone of the partition recent-ID set inside `stage()`. Removing that clone,
  without lowering the gate, produced 100,000 canonical events, zero duplicate,
  zero quarantine, 115,107 events/s, p50 6.19 microseconds and p99 18.921
  microseconds. Targeted Clippy, full workspace tests and an immutable three-core
  real-provider freshness/recovery run remain mandatory before this gate closes.
- `2026-08-19 PHASE B B5 STABLE QUERY TAIL DEFECT IDENTIFIED, FIX IN
  PROGRESS`: the clean `c177bb0` three-core candidate drained core lag to 11
  records and projector lag to 57 records with zero quarantine, and authenticated
  Binance/OKX monitoring, alpha trade/bar and OKX execution snapshots passed.
  Binance execution QUOTE nevertheless failed strict 2-second freshness 20/20.
  Its Redis canonical envelope was current (receive age about 148 ms), while the
  query backend reported about 230 seconds stale. The shared spool held more than
  36,000 events for that partition; `StableSpoolQueryBackend` called the replay
  API `read(limit=10000)`, whose intentionally ascending semantics return the
  oldest retained page, then treated the page tail as latest. The additive fix
  must introduce an ordered `read_tail` primitive for latest-state/query use and
  leave cursor/replay `read` semantics untouched. Transport ordering, tail bound,
  stable query beyond 10,000 records, strict authenticated freshness and replay
  regression tests are mandatory before runtime recovery certification resumes.
- `2026-08-19 PHASE B B5 STABLE QUERY TAIL UNIT-VERIFIED, IMMUTABLE
  RUNTIME RETEST PENDING`: `SQLiteDurableSpool.read_tail` is an additive bounded
  latest-window primitive that selects newest offsets efficiently and returns
  them in ascending logical order. Existing `read(after=...)` replay/cursor
  semantics are unchanged. Stable latest/history/status/gap views now use the
  tail window. Regression tests prove replay limit two still returns offsets
  1-2 while tail limit two returns 4-5, and a real SQLite partition with 10,001
  canonical trade records returns offset 10,001 as LIVE instead of the end of
  the oldest page. The two transport/stable-edge suites passed 32 tests with one
  separately gated real-Redis skip. The immutable candidate must still prove the
  Binance execution QUOTE 2-second policy, lag, replay and failure recovery.
- `2026-08-19 PHASE B B5 QUERY TAIL CORRECTNESS PASSED, READ
  AMPLIFICATION FIX IN PROGRESS`: on the immutable `f120173` candidate, the
  Binance USDM quote canonical partition exceeded 10,000 records and strict
  2-second authenticated execution snapshots passed 20/20, proving newest-tail
  correctness. But latest/status latency remained 729-1,115 ms (813 ms average)
  because every request decoded the full 10,000-row tail. Feed-aware bounds will
  read one event for TRADE/QUOTE latest/status, retain the bounded BAR continuity
  window needed for missing-candle detection, and read only the requested history
  window for warmup/replay snapshot creation. Public contracts, durability, gap
  policy and replay cursor semantics remain unchanged. Unit call-bound tests and
  immutable post-10k latency/freshness evidence are required.
- `2026-08-19 PHASE B B5 QUERY READ AMPLIFICATION UNIT-VERIFIED,
  IMMUTABLE LATENCY RETEST PENDING`: stable latest/status now reads one newest
  event for TRADE/QUOTE; BAR latest retains the 10,000-event continuity window,
  and history/snapshot reads exactly the requested warmup bound. A call-bound
  test locks limits `1, 1, 10000` for trade latest, one-row history and bar latest
  respectively. The transport/stable-edge suites now pass 33 tests with one
  separately gated real-Redis skip, including the 10,001-record newest-tail
  regression. The public API, freshness thresholds, gap policy and replay reads
  were not changed. Immutable post-10k p95/p99 and strict snapshot evidence still
  gates acceptance.
- `2026-08-19 PHASE B B5 POST-10K QUERY LATENCY VERIFIED, CLEAN
  CANDIDATE/RECOVERY GATES PENDING`: two isolated query replicas were rolling-
  recreated with immutable `d08c424` while preserving the `f120173` test spool
  and credentials solely to measure the >10,000-record boundary. After a brief
  real-provider catch-up interval correctly failed closed because the source
  timestamp was about 16 seconds old, the feed recovered without mutation. One
  hundred consecutive Binance execution QUOTE requests then passed the unchanged
  2-second policy: p50 2.94 ms, p95 3.74 ms, p99 32.44 ms, max 89.8 ms and 4.31
  ms average, versus 813 ms average before feed-aware bounds. This mixed rolling
  rehearsal is not final certification; a clean all-`d08c424` candidate and
  broker/provider/Redis/process recovery gates remain mandatory.
- `2026-08-19 PHASE B B5 TWO-BROKER FAIL-CLOSED PASSED, PROJECTOR
  RECOVERY DEFECT IDENTIFIED`: with one isolated RF3 broker stopped, the durable
  spool advanced from 5,063 to 12,423 events and quarantine stayed zero. With two
  brokers stopped under minISR=2, queued work drained and the spool then remained
  exactly constant at 27,403 for five seconds, proving no false acceptance. After
  both brokers returned healthy, Rust producers/cores remained alive but the
  Python projector did not recover because an asynchronous checkpoint error
  terminated its process and Compose intentionally had `restart: no`. The fix is
  a bounded in-process projector supervisor: recreate the poisoned Kafka consumer,
  keep readiness NOT_READY while disconnected, retain the durable spool/Redis
  target, replay only uncheckpointed Kafka records through idempotent sinks, and
  use capped exponential backoff. Closing a poisoned generation must be logged but
  must not mask the original error or prevent a new generation. Unit retry/close
  tests and the same real two-broker outage/recovery gate are mandatory.
- `2026-08-19 PHASE B B5 PROJECTOR SUPERVISOR UNIT-VERIFIED, REAL
  OUTAGE RETEST PENDING`: the stable projector now recreates a failed Kafka
  consumer/engine generation in process with capped 250 ms-to-5 s exponential
  backoff. Readiness reports Kafka NOT_READY while no generation is active. Shared
  durable spool, HTTP sink and Redis target survive generation replacement, so
  only uncheckpointed broker records replay through existing idempotency gates. A
  poisoned generation close is bounded/logged and cannot mask the original fault
  or prevent recovery; cancellation still propagates. Stable edge/release suites
  passed 28 tests with one separately gated real-Redis skip, including injected
  async-checkpoint failure, poisoned close, exact one-backoff retry and successful
  second generation. The real two-broker recovery test must now be repeated.
- `2026-08-19 PHASE B B5 TWO-BROKER RECOVERY PARTIAL PASS, JOIN
  BACKPRESSURE DEFECT IDENTIFIED`: after Kafka1/Kafka2 returned healthy, the same
  projector process (`restart=0`) resumed the durable spool from 101,585 to
  104,614 records in five seconds with zero quarantine. This proves the new
  supervisor reconnects autonomously. However, independent Kafka raw/canonical
  topic scheduling let the canonical backlog outrun its correlated raw backlog;
  the 10,000-record in-memory join bound then failed closed and repeatedly
  recreated the consumer generation. No canonical record was checkpointed
  before raw durability and no bad public data escaped, but repeated rebalances
  leave lag unbounded and are not acceptable for stable release. The bounded
  fix is Kafka-native high/low-water backpressure: pause only assigned canonical
  partitions before the hard join limit, continue consuming and durably ACKing
  raw partitions, drain correlated canonical records in partition order, then
  resume canonical partitions below the low watermark. Increasing the RAM bound,
  checkpointing canonical early or weakening lineage validation is forbidden.
  Unit pause/resume, assignment, overflow, ordering and replay tests plus the
  same real two-broker recovery/lag/quarantine gate are mandatory.
- `2026-08-19 PHASE B B5 PROJECTOR JOIN BACKPRESSURE UNIT-VERIFIED,
  IMMUTABLE OUTAGE RETEST PENDING`: the Confluent adapter now pauses/resumes only
  assigned canonical partitions and reapplies requested flow control after an
  assignment epoch changes. The engine reserves one bounded poll batch before
  its hard record limit, also applies byte high/low watermarks, continues raw
  durability/checkpoint progress while canonical is paused, and resumes only
  after both pending records and bytes drain below low water. The original hard
  record/byte fence remains active if a broker violates pause. Thirty targeted
  projector tests passed with one separately gated Redis integration skip; the
  broader stable edge/release/deployment, V2 multi-venue contract and Phase 8.3
  release regression passed 50 tests with the same one conditional skip. Source
  was mounted read-only, network was disabled and no V1/runtime state changed.
  A fresh immutable image and repeated real two-broker recovery gate still block
  candidate certification.
- `2026-08-19 PHASE B B5 CANONICAL REPLAY DETERMINISM DEFECT IDENTIFIED`:
  the immutable `9afc21b` projector applied backpressure, reached previously
  committed canonical replay and correctly stopped on `EventIdCollision` rather
  than overwrite immutable data. A separately authorized read-only diagnostic
  group scanned the isolated canonical log without joining/rebalancing the
  projector. It found repeated records with the same raw capture, native source
  sequence, business payload, partition key and event ID, but a different
  `normalized_at_ns` generated from the Rust process wall clock. Thus a retry of
  identical raw bytes was not byte-deterministic. The runtime core also rebuilt
  partition sequence from process-local zero, which would regress continuity
  when a process resumes from a committed Kafka offset. The fix must materialize
  immutable normalized/published/accepted timestamps from the durable raw
  capture time; processing latency remains operational telemetry. Runtime
  `partition_sequence` must derive monotonically and deterministically from the
  raw Kafka transport offset plus bounded expanded-row index under the existing
  partition-plan epoch. Exact replay across fresh core instances, mid-log restart
  continuity, multi-row ordering, overflow, quarantine and transactional tests
  plus a clean isolated candidate are mandatory. Event identity, collision
  detection, public contracts and V1 remain unchanged.
- `2026-08-19 PHASE B B5 CANONICAL REPLAY DETERMINISM UNIT/CAPACITY
  VERIFIED, CLEAN RUNTIME RETEST PENDING`: Rust canonical and quarantine output
  timestamps are now materialized from the immutable raw receive boundary;
  wall-clock processing latency remains runtime telemetry and cannot alter event
  bytes. The Kafka runtime derives `partition_sequence` from raw transport offset
  with a fixed one-million-row stride and checked overflow, preserving monotonic
  order across fresh processes and bounded expanded frames without changing the
  public event-ID contract. Tests prove identical raw/offset input yields exact
  bytes across fresh cores despite different processing clocks; quarantine is
  likewise deterministic; a mid-log restart advances sequence; two rows retain
  order; and offset overflow fails closed. Rust fmt and full workspace tests
  passed 57 tests; warnings-as-errors Clippy passed. The unchanged release gate
  processed 100,000 canonical events with zero duplicate/quarantine at 127,732
  events/s, p50 5.7 microseconds and p99 15.2 microseconds against the 50,000/s
  minimum. No V1/current-Redis state changed. A clean immutable all-one-revision
  candidate, duplicate-log scan and repeated recovery gates remain mandatory.
- `2026-08-19 PHASE B B5 REAL-CONSUMER GATE DEFECT IDENTIFIED`: authenticated
  reads through both isolated query replicas proved OKX SWAP TRADE/QUOTE and
  Binance/OKX final BAR projections, but Binance USD-M TRADE/QUOTE correctly
  failed closed as `DATA_STALE`. A bounded provider probe proved both documented
  Binance combined-stream paths were delivering authentic frames on this host.
  The Rust Binance session had split the socket and discarded its write half,
  so it could not explicitly answer provider WebSocket `PING` control frames;
  repeated protocol resets eventually made the projected feed stale. The
  approved in-scope repair is to keep the session full duplex, answer `PING`
  with matching `PONG`, preserve bounded reconnect/backoff, then rebuild the
  immutable candidate and repeat authenticated manifest, reconnect, lag,
  replay and resource gates. V1 and current Redis remain untouched.
- `2026-08-19 PHASE B B5 BINANCE CONTROL-FRAME REPAIR UNIT-VERIFIED,
  REAL-PROVIDER HOLD PENDING`: the native Binance loop now retains the full-duplex
  socket and replies to every provider `PING` with the matching `PONG`; data-frame
  parsing, authority fencing, durable ACK, bounded exponential backoff and event
  identity are unchanged. The current source passed Rust format, workspace
  Clippy with warnings denied and all 57 workspace tests. The first test run
  failed only because the builder image intentionally omitted repository golden
  fixtures; after copying the exact `contracts/` and `tests/` fixture trees into
  the disposable test container, the unchanged source passed. A newly labeled
  immutable image must now remain fresh beyond the Binance heartbeat boundary
  and pass both-replica manifest smoke before this defect is closed.
- `2026-08-19 PHASE B B5 NATIVE-INGEST RESTART/THROUGHPUT DEFECTS
  IDENTIFIED`: the c1701c1 real-manifest retest still found Binance USD-M stale.
  A unique admin read-only consumer sampled 11,016 committed quarantine records;
  every record was `STALE_GENERATION`, primarily Binance BBO/trade frames whose
  process-local generation reset after container recreation. The raw ingestor
  also waits for one RF3/all-ISR Kafka delivery ACK before reading the next frame;
  authentic BTCUSDT USD-M burst traffic can therefore outrun the reader and reset
  the provider socket even when Ping/Pong is correct. The in-scope closure is a
  durable, fsync-backed per-ingestor/per-service connection-generation counter
  mounted only in isolated stable state, plus a strictly bounded concurrent
  durable-publish window that preserves per-key enqueue order, backpressures at
  its configured limit and never drops/fabricates frames. Restart must advance
  generation, stale old generations must still quarantine, and broker failure
  must fail closed. Unit, clean restart, authentic provider, lag and quarantine
  gates are mandatory before immutable candidate acceptance.
- `2026-08-19 PHASE B B5 NATIVE-INGEST CLOSURE UNIT-VERIFIED, CLEAN
  RUNTIME PENDING`: every Rust-native config now owns a distinct absolute
  generation-state path and a 512-record hard in-flight bound. Generation is
  atomically persisted and fsynced before each connect/reconnect; OKX public and
  business services use independent counters. Binance reads control/data frames
  while Kafka `acks=all` deliveries are in flight, preserves enqueue order per
  Kafka key, stops reading at the hard bound, retries retryable delivery failures
  and drains every already-enqueued delivery before failing closed. Four native
  services mount only the isolated stable-state volume; current V1 state is not
  reachable. Rust format, full workspace Clippy and 59 workspace tests passed; a
  final drain-safety edit additionally passed targeted Clippy and both ingestor
  tests. Seven deployment tests and 62 broader Phase B/V2/security/release tests
  passed with one separately gated Redis skip. A clean immutable candidate must
  still prove restart generation advance, zero new quarantine, authentic Binance
  freshness beyond heartbeat, bounded lag and broker-outage recovery.
- `2026-08-19 PHASE B B5 CLEAN 0C97E29 CANDIDATE CAPACITY DEFECT
  IDENTIFIED, FIX IN PROGRESS`: both immutable images were rebuilt and verified
  at revision `0c97e29`; a fresh RF3/minISR2 Kafka, dedicated Redis, stable
  state and query cache were bootstrapped under the isolated
  `qdl_v2_stable_candidate` project. Broker topic/ACL bootstrap passed and all
  app roles used the same image revision. Authentic Binance/OKX traffic produced
  zero quarantine/collision/startup errors, but authenticated real-consumer
  readiness correctly failed closed because the single Python projector lag grew
  monotonically. Within a few minutes its rebuildable SQLite cache reached
  751,783 records / 392,154,317 payload bytes (486,651 raw plus 265,132
  canonical) against the one-million-record hard bound, while projector I/O
  reached about 5.18 GB and Kafka canonical/raw lag continued increasing.
  Therefore process-up/healthy is explicitly rejected as acceptance and the
  isolated acquisition/core/projector roles were stopped before capacity
  exhaustion; V1 remained untouched.

  Root cause is the compatibility path, not provider correctness or Rust
  normalization: Kafka is already the replay authority, yet the Python
  projector serially performs per-record raw lookup, post-ACK canonical lookup
  and checkpoint calls around otherwise batched fsync/HTTP/Redis operations, and
  the transitional cache retains every high-frequency raw and canonical event
  for 24 hours despite a capacity that cannot hold that horizon. The bounded
  repair may not sample, drop, fabricate or weaken Kafka durability. It must:
  (1) add bounded batch lookup/checkpoint primitives and remove per-event SQLite
  rereads/thread hops while preserving downstream-before-checkpoint ordering;
  (2) make the SQLite cache explicitly replay-window bounded per partition, with
  deterministic cursor-expiry/snapshot recovery while Kafka remains the durable
  replay source; (3) preserve sufficient raw lineage for every uncheckpointed
  canonical record and fail closed under overflow; and (4) prove sustained
  real-provider lag convergence, bounded cache/disk/I/O, exact replay/cursor
  behavior, zero quarantine/collision and broker/restart/Redis recovery before
  Phase B can close. Public V1/V2 contracts, event identity, freshness policy and
  authority remain unchanged.
- `2026-08-19 PHASE B B5 BOUNDED PROJECTOR CACHE/HOT-PATH
  UNIT-VERIFIED, IMMUTABLE RUNTIME RETEST PENDING`: the stable SQLite bridge now
  supports a disabled-by-default per-partition replay window and the stable role
  explicitly selects 10,000 records per partition, matching the public bounded
  replay limit. Logical offsets remain monotonic; a cursor older than the retained
  window deterministically raises `CursorExpired` and requires the existing
  snapshot-and-replay recovery. Kafka remains the only replay authority. SQLite
  keeps `synchronous=FULL` but checkpoints its WAL at 1,000 pages instead of
  100 to reduce checkpoint write amplification without weakening transaction
  durability.

  The projector now resolves raw lineage and signed-ingest ACKs through bounded
  batch queries, checkpoints each fully ACKed raw/canonical micro-batch in one
  broker call, and uses a 512-record/10 ms hard micro-batch in the stable role.
  Internal HMAC schema, canonical bytes, event IDs, per-partition ordering,
  Redis atomic Lua projection and downstream-before-Kafka-checkpoint ordering
  are unchanged. The generic spool default remains untrimmed, so existing V1
  and non-stable callers do not inherit the new window implicitly. Targeted
  transport/projector/release tests passed 44 tests with one separately gated
  Redis integration skip. The broader V2 API/SDK, security, Phase 8 and Phase B
  regression passed 102 tests with the same one conditional skip, using the
  pinned dependency image, read-only source and no network. A new immutable
  image and clean authentic-provider lag/cache/I/O/restart/outage test still
  gate acceptance.
- `2026-08-19 PHASE B B5 FIRST BOUNDED-CACHE RUNTIME RETEST FAILED,
  TRANSPORT-LINEAGE REPAIR IN PROGRESS`: the clean all-`3d0ff9c` candidate
  confirmed that batch lookup/checkpoint and partition windows prevent unbounded
  cache growth, but they cannot make the prior topology sustainable. The
  projector still consumed and fsync-persisted every raw frame solely to recover
  raw bytes needed by the compatibility projection. After one minute, canonical
  lag remained increasing (for example 65,351 records on hot canonical partition
  0), projector CPU was about 78%, block writes were already about 1.47 GB and
  three raw partitions were continuously trimming at 10,000 rows. This also
  risks evicting lineage before a lagged canonical record arrives. The isolated
  producer/core/projector roles were stopped; V1 remained unchanged.

  The corrected internal boundary is additive and provider-neutral: the
  transactional Rust core must copy the already authenticated
  `RawProviderEnvelope` bytes into a private Kafka canonical-record header in
  the same exactly-once transaction. The Python projector consumes only
  read-committed canonical records, validates embedded capture ID/hash/provider/
  session/authority against the canonical envelope, and passes those bytes only
  to the internal HMAC compatibility projector. SQLite then stores canonical
  replay/query cache rows only; Kafka raw/canonical remain the authorities.
  Public Protobuf, REST/gRPC, event ID, partition key, freshness, Redis V1 shape
  and venue semantics may not change. The prior split raw-topic join remains a
  test/rolling-compatibility path but is not selected by stable `2.0.0`.
  Mandatory gates are exact Rust transaction-header replay, missing/tampered
  lineage fail-closed, no raw SQLite rows in stable runtime, V1 compatibility
  golden parity, sustained provider lag convergence, bounded canonical cache,
  process/broker/Redis recovery and zero quarantine/collision.
- `2026-08-19 PHASE B B5 PRIVATE TRANSPORT LINEAGE UNIT-VERIFIED,
  IMMUTABLE RUNTIME RETEST PENDING`: the Rust transactional canonical output
  now carries the exact consumed `RawProviderEnvelope` in a private
  `qdl-raw-provider-envelope` Kafka header in the same output/source-offset
  transaction. The stable Python Kafka adapter selects only the read-committed
  canonical topic, requires that header, validates capture ID and all raw
  provenance again at the projector and signed-ingest boundary, and persists
  only canonical events in the bounded SQLite cache. The previous raw-topic
  join remains available only when explicitly configured for rolling/test
  compatibility; missing inline lineage with canonical-only topology fails
  closed.

  The internal HMAC endpoint accepts the lineage field additively, maps malformed
  protobuf to bounded 422, and never writes the private raw bytes into cache
  headers or public payloads. Public Protobuf/OpenAPI/SDK/event identity and
  exact Redis compatibility projection remain unchanged. Rust fmt, full
  workspace Clippy with warnings denied and all 60 workspace tests passed,
  including exact present/absent private-header assertions. Python compileall
  passed; targeted tests passed 46 with one conditional Redis skip and the broad
  V2/security/Phase 8/Phase B suite passed 104 with the same skip. Tests cover
  canonical-only subscription, valid embedded lineage without raw cache rows,
  missing lineage, signed inline ingest, malformed lineage, legacy fallback,
  replay/idempotency and checkpoint ordering. Clean immutable real-provider
  lag/cache/I/O and recovery evidence remains mandatory.
- `2026-08-19 PHASE B B6 AUTHENTIC BAR WARMUP CLOSURE IN PROGRESS`: the clean
  all-`2b38f95` candidate proved the private-lineage topology is sustainable:
  projector lag converged from 1,112 to 62 while provider traffic continued,
  SQLite contained canonical rows only, every partition stayed at or below
  10,000 rows, and no quarantine/collision was observed. Authenticated Binance
  and OKX TRADE snapshots returned the typed V2 contract and a mismatched
  consumer identity failed closed. The same real-runtime test exposed a release
  blocker: both registered alpha manifests require 500 final 1m BARs, while the
  clean latest-only BAR edges returned `409 PARTIAL_RESULT`.

  The approved repair remains inside the isolated Phase B topology. Add bounded
  real-provider historical BAR bootstrap for Binance Spot/USD-M and OKX
  Spot/SWAP, publish every native row through raw Kafka and the transactional
  Rust core, and never write history directly into SQLite or Redis. Final BAR
  identity must be idempotent across REST bootstrap, WebSocket delivery and
  process restart; conflicting content for the same immutable provider bar must
  fail closed. No synthetic/generated runtime market data is allowed. Required
  gates are exact 500-row closed/contiguous coverage per crypto alpha manifest,
  Python/Rust canonical-byte parity, duplicate/restart idempotency, malformed/
  incomplete provider fail-closed behavior, bounded resource/lag evidence and
  warmup -> signed cursor -> replay -> live through the released SDK. Rollback
  stops/removes only `qdl_v2_stable_candidate`; V1 port 8100, current Redis and
  current consumers remain untouched.
- `2026-08-19 PHASE B B6 AUTHENTIC BAR WARMUP IMPLEMENTED AND UNIT-VERIFIED,
  IMMUTABLE RUNTIME ACCEPTANCE PENDING`: Binance Spot/USD-M and OKX Spot/SWAP
  now fetch a bounded 500-row final 1m history from the approved public REST
  APIs before the live loop. Strict adapters reject partial coverage, malformed
  native rows, provisional bars, time gaps and conflicting duplicate timestamps.
  Every accepted row is wrapped with HTTP/provider provenance and enters raw
  Kafka; the Python edge never writes SQLite or Redis directly. OKX final BAR
  source/event identity no longer includes transport partition sequence, so the
  same provider bar is idempotent across REST bootstrap, WebSocket delivery and
  restart while a same-identity payload conflict remains fail-closed. The
  provider-owning DNSE stable edge moved under `qdl.adapters.vn`; its old runtime
  import remains a compatibility facade, restoring the static role boundary
  without changing vendor behavior.

  Targeted BAR/deployment/golden tests passed 17/17. The official 19-case
  multi-venue golden generator/check passed; only the intended OKX Spot/SWAP BAR
  bytes and manifest hashes changed. Full Python discovery passed 478 tests with
  six explicit dependency/infrastructure skips. Full Rust fmt, workspace Clippy
  with warnings denied and workspace tests passed. A bounded real-provider probe
  loaded exactly 500 contiguous closed rows for Binance USD-M, Binance Spot, OKX
  SWAP and OKX Spot over the same window, with test provenance false. No V1
  service, current Redis namespace or consumer was mutated. A new all-one-SHA
  immutable candidate must still prove the registered warmup -> cursor -> replay
  -> live and recovery/resource gates before Phase B closes.
- `2026-08-19 PHASE B B7 NATIVE INGESTOR STARTUP-ORDER DEFECT CLOSED,
  IMMUTABLE REHEARSAL PENDING`: the fresh all-`ad4338d` isolated candidate
  bootstrapped exactly 500 contiguous final provider BARs for Binance USD-M,
  Binance Spot, OKX SWAP and OKX Spot (2,000 raw records total). Runtime
  inspection then found that the Spot native ingestors raced `stable_tls_init`
  and exited before `/stable-certs/producer/ca.crt` existed; USD-M/SWAP happened
  to win the same race. The compose contract now requires completed TLS and
  state initialization plus healthy Kafka1/Kafka2/Kafka3 for all four native
  ingestors. Compose validation passed and all seven stable deployment contract
  tests passed, including exact dependency regression assertions for every
  native ingestor. The next all-one-SHA immutable candidate must demonstrate all
  four processes start cleanly before consumer/recovery certification continues.
  Rollback remains removal of only project `qdl_v2_stable_candidate`; V1 is
  unchanged.
- `2026-08-19 PHASE B B8 LATEST-STATE CAPACITY DEFECT CONFIRMED, FIX IN
  PROGRESS`: after B7, all four native ingestors started cleanly from immutable
  `6a9a19e` images and authentic 500-row BAR bootstrap again completed for all
  four crypto bindings. Under current provider traffic the single Python
  projector backlog nevertheless grew from about 35.8k to 244.6k records in
  60 seconds. Raising its quota from 0.75 to 2 CPUs still grew backlog by about
  63k/minute, and three projector replicas still could not drain the hottest
  ordered partition. A read-committed one-record tail inspection identified
  partition 0 as Binance USD-M `bookTicker`, receiving roughly 3.0-3.5k BBO
  updates/second. Trade/final-BAR partitions were not the dominant source.

  CPU scaling and batch-boundary canonical coalescing are rejected: one cannot
  parallelize a single ordered key, while transaction-batch coalescing would
  make replay output depend on non-deterministic batch boundaries. The bounded
  repair follows the approved lifecycle policy: acquisition bindings declare
  `LOSSLESS` or `LATEST_STATE`; only `LATEST_STATE` BBO frames may replace the
  same pending key inside a fixed bounded flush window before raw Kafka ACK.
  The last authentic provider frame in each window is durably published with
  unchanged bytes/provenance, a pending value is flushed on timer and orderly
  disconnect, and coalescing counters are explicit. TRADE, final/revised BAR,
  book and quality/authority transitions remain lossless and backpressured.
  Rust unit/replay/ordering/failure tests plus clean real-provider lag,
  freshness, resource and recovery evidence gate closure. The overloaded
  isolated candidate and its volumes were removed before disk bounds; V1 was
  not touched.
- `2026-08-19 PHASE B B8 LIFECYCLE-AWARE ACQUISITION COALESCING
  UNIT-VERIFIED, IMMUTABLE RUNTIME RETEST PENDING`: generated native acquisition
  configs now carry provider-neutral `feed` and `delivery_class` fields plus a
  bounded 50 ms latest-state flush window. Rust rejects every mismatched policy:
  only QUOTE may be `LATEST_STATE`, while TRADE and BAR must be `LOSSLESS`.
  Binance and OKX keep at most one pending authentic raw frame per quote binding,
  replace it only before Kafka acceptance, flush the last frame on the bounded
  timer and orderly session exit, retain the existing ACK/retry/backpressure
  path, and expose accepted/coalesced counts. Lossless frames bypass this buffer.

  Eleven targeted Python deployment/history tests passed. The pinned Rust
  release build completed; four native-ingestor tests passed, targeted Clippy
  passed with warnings denied, and rustfmt is clean. Unit evidence covers policy
  mismatch rejection, last-frame byte/timestamp retention and durable generation
  behavior. A new one-SHA candidate must still prove authentic quote freshness,
  trade losslessness, decreasing/near-zero core and projector lag, bounded
  cache/resource use, restart/failover and no quarantine/collision before B8 or
  Phase B can close.
- `2026-08-19 PHASE B B8 IMMUTABLE REAL-PROVIDER CAPACITY GATE PASSED,
  CONSUMER/RECOVERY CERTIFICATION IN PROGRESS`: a clean all-`0a25407` candidate
  started all four Binance USD-M/Spot and OKX SWAP/Spot native ingestors only
  after TLS/state initialization and healthy RF3/minISR2 brokers. The bounded
  REST bootstrap published exactly 500 contiguous closed 1m provider bars per
  binding (2,000 total, `test_provenance=false`) through raw Kafka and the Rust
  core. Under continuing authentic provider traffic, Rust-core lag was 50
  records and projector lag was 29 records at the bounded observation point;
  the prior projector growth reversed and an earlier 60-second sample decreased
  from 265 to 84. SQLite contained only `md.canonical.v2`, zero quarantine rows,
  76,795 records, and no partition exceeded the configured 10,000-record replay
  window. Query cache utilization was about 6.9%; all query dependencies were
  READY; exactly one stream gateway held epoch-1 lease while its peer reported
  STANDBY. The isolated Redis used about 3.3 MiB and app-role memory remained
  bounded; no application warning/error was observed in the sampled startup and
  runtime window.

  Current V1 remained container `0e0eb56c78ba`, image `8f2a5a3f1ff9`, Up and
  HTTP 200 on port 8100. No current Redis namespace, V1 consumer or authority was
  changed. B8 code/capacity is accepted, but Phase B is not closed: authenticated
  Binance/OKX/VN/Trading-System consumer flows, active/passive restart,
  generation restart, broker outage, Redis rebuild, full regression/release
  checks, compact evidence and exact candidate cleanup remain mandatory. DNSE
  testing must use the configured real provider credentials/session and fail
  closed if 500-bar coverage is unavailable; generated VN data is forbidden.
- `2026-08-19 PHASE B B9 CONSUMER ACCEPTANCE DEFECTS CONFIRMED, FIX IN
  PROGRESS`: authenticated SDK acceptance correctly failed closed with
  `DATA_STALE`. Bounded inspection proved Binance BARs continued to append every
  minute, but OKX Spot/SWAP BARs stopped at the one-time bootstrap because the
  stable BAR cycle polled only Binance bindings. The real DNSE WebSocket
  authenticated and subscribed successfully with configured credentials, while
  its REST BAR poll timed out; the VN edge also has no 500-row historical
  bootstrap and therefore cannot satisfy its registered alpha manifest after a
  clean start. Finally, both query service and SDK compare wall-clock freshness
  before honoring `MARKET_CLOSED`, contradicting the approved rule that a closed
  session is neither stale nor offline.

  The in-scope repair is bounded and contract-preserving: poll the latest closed
  OKX BAR through the existing strict history adapter; bootstrap exactly 500
  authentic closed DNSE 1m rows through raw Kafka/Rust with bounded retry and
  duplicate/conflict validation; centralize VN session-calendar lookup and skip
  live BAR polling while the session is closed; treat `MARKET_CLOSED` as
  available historical state for ALPHA/RESEARCH while preserving
  `execution_eligible=false` and execution-grade SDK fail-closed behavior. Tests
  must cover incomplete/conflicting history, retry exhaustion, session closure,
  OKX append, server/SDK policy and the real registered consumer flow. No public
  schema, V1 route, current Redis or authority mode may change.
- `2026-08-19 PHASE B B9 MULTI-VENUE BAR/SESSION REPAIR IMPLEMENTED AND
  UNIT-VERIFIED, IMMUTABLE RUNTIME RETEST PENDING`: the stable crypto BAR cycle
  now appends the latest strictly closed native BAR for Binance USD-M/Spot and
  OKX SWAP/Spot through one lossless raw-Kafka batch; OKX uses its existing
  strict V5 history adapter with `limit=1`, and all Kafka ACKs are cardinality
  checked. DNSE now bootstraps exactly 500 closed provider 1m rows per configured
  FPT/VN30F1M binding over a bounded 30-day lookback, retries at most four times,
  deduplicates identical native timestamps, rejects conflicting duplicates or
  partial coverage, and publishes only authentic rows through raw Kafka/Rust.
  Live DNSE BAR polling is retry-bounded, ACK-checked and skipped outside the
  governed calendar session.

  The VN calendar moved to a shared domain resolver keyed by canonical
  `session_calendar_id`. Query and SDK semantics now preserve the approved rule:
  `MARKET_CLOSED` history is readable by ALPHA/RESEARCH despite wall-clock age,
  while execution-grade access fails closed as `DATA_NOT_READY` and no item is
  execution eligible. Public schema, route and event identity are unchanged.
  Targeted tests passed 31/31; the broader stable projector/query/SDK/security/
  release regression passed 85/85 with one explicit infrastructure-gated skip;
  compose validation including `stable-vn` passed. Test cases include OKX live
  append/idempotence, DNSE transient retry, partial/conflicting history, exact
  units/provenance, closed-session no-REST behavior, server/SDK policy and
  execution blocking. The next gate is a clean immutable all-one-SHA candidate
  with real Binance/OKX/DNSE consumer and recovery evidence.
- `2026-08-19 PHASE B B9 OKX PROVISIONAL-CLOSE RETRY CLOSED`: the first
  immutable `0228036` runtime attempt showed OKX could still report the just-ended
  candle as provisional at boundary `+1s`; the outer cycle recovered at `+3.5s`
  and ACKed all four crypto BARs, but emitted a misleading ERROR. The OKX adapter
  now retries this provider transition internally at bounded 0.5/1/2-second
  delays (four attempts maximum) and raises only after exhaustion. No candle is
  accepted before `confirm=1`, and event identity remains native-time based.
  Fourteen focused BAR/deployment tests passed, including provisional recovery
  and exhaustion. A final immutable image/runtime replay remains required.
- `2026-08-19 PHASE B B10 WARMUP FRESHNESS DEFECT CONFIRMED, FIX IN
  PROGRESS`: real registered alpha warmup still failed `DATA_STALE` even though
  Binance and OKX latest closed BARs were aligned and only 43 seconds old. The
  SDK was applying realtime freshness/state and execution eligibility to every
  row in a 500-row historical warmup. Historical context is necessarily older
  than the realtime freshness threshold, so this made any non-trivial warmup
  impossible. The correction keeps identity, source policy, coverage,
  completeness, ordering and BAR finality checks on every row, but evaluates
  stale/gap/realtime execution eligibility only on the tail watermark row. A
  stale or non-authoritative tail must still fail closed. Public schema and
  server query semantics remain unchanged; targeted SDK tests and the authentic
  warmup -> cursor -> replay -> live flow gate closure.
- `2026-08-19 PHASE B B10 SDK WARMUP QUALITY REPAIR UNIT-VERIFIED,
  AUTHENTIC FLOW RETEST PENDING`: SDK warmup validation now checks instrument,
  feed, interval, source policy, completeness, history gap and final BAR semantics
  for every row, while applying wall-clock freshness/state and execution
  eligibility only to the tail watermark. A gap anywhere in the requested history
  still blocks. A stale, unavailable or non-authoritative tail still blocks. This
  preserves strict execution safety without rejecting valid historical context.
  Twenty-two SDK/API/end-to-end tests passed, including an execution-grade
  two-row warmup with an old non-executable context row and a fresh executable
  tail, plus stale-tail rejection. Public V2 schema and server routes are
  unchanged.
- `2026-08-19 PHASE B B11 UNIFIED-STREAM CURSOR SCOPE DEFECT CONFIRMED,
  FIX IN PROGRESS`: the authenticated stable SDK flow passed query/history
  validation after B10, then gRPC subscribe failed closed with
  `CURSOR_INVALID: cursor stream does not match the data requirement`. The
  signed cursor is correct: stable topology intentionally uses one canonical
  transport stream, `md.canonical.v2`, and carries instrument/feed/source scope
  in the partition key. The generic gRPC service still hardcodes the earlier
  beta convention `md.canonical.v2.<feed>`, so it rejects its own stable
  snapshot cursor.

  The bounded repair must not weaken cursor authorization or change any public
  Protobuf/API/token. Introduce an injectable, transport-neutral cursor-scope
  validator: the existing feed-scoped validator remains the default for beta
  and existing tests, while stable runtime validates the exact canonical stream
  and exact catalog binding partition for the requested instrument, feed,
  interval and source policy. Wrong consumer, stream, instrument, feed, source
  or policy must continue to fail closed. Required gates are targeted positive
  and adversarial contract tests followed by the authentic registered
  warmup -> signed cursor -> replay -> live flow. Rollback is code-only inside
  the isolated candidate; V1 port 8100, current Redis and current consumers
  remain unchanged.
- `2026-08-19 PHASE B B11 CATALOG-AUTHORITATIVE CURSOR SCOPE UNIT-VERIFIED,
  AUTHENTIC FLOW RETEST PENDING`: gRPC stream scope validation is now an
  injectable transport-neutral contract. The default validator preserves the
  existing feed-specific beta convention without changing its call sites. The
  stable runtime explicitly installs a catalog-backed validator that resolves
  the full requirement, including interval and source policy, then requires the
  exact catalog canonical stream and instrument/feed/source partition. This
  permits the intentional unified `md.canonical.v2` stream without accepting an
  arbitrary same-prefix stream or partition. Public protobuf, signed cursor
  format, SDK and replay gateway are unchanged.

  The targeted Phase B edge and Phase 5 stream/SDK suites passed 39 tests with
  one pre-existing dependency-gated skip. Positive unified-stream scope and
  wrong stream, partition, feed and source-policy failures are covered; the
  existing signed consumer/scope mismatch regression remains green. The final
  immutable candidate still must pass the authenticated real-provider consumer
  and recovery gates before Phase B can close.
- `2026-08-19 PHASE B B12 IMMUTABLE CRYPTO CONSUMER ACCEPTANCE PASSED,
  RECOVERY CERTIFICATION IN PROGRESS`: immutable Python and Rust images were
  built from `df88de0` with OCI revision/version labels and pinned into a fresh
  secret bundle; the manifest records image SHA-256 IDs and no secret values. A
  clean isolated RF3/minISR2 Kafka topology with mTLS/ACLs started four native
  Binance USD-M/Spot and OKX SWAP/Spot ingestors, three Rust core workers, one
  Python projector, two query replicas and one active plus one fenced standby
  stream role. The real-provider BAR edge ACKed exactly 500 contiguous final 1m
  rows for each of four crypto bindings, 2,000 total, through raw Kafka/Rust.

  Authenticated registered consumers then passed without source mounts or test
  data. Binance and OKX alpha manifests each returned 500 rows with `FULL`
  coverage and completed signed cursor `REPLAYING -> LIVE` handoff to the next
  correct TRADE event. Both query replicas returned identical canonical 500-row
  data and watermark. Monitoring received live Binance and OKX TRADE. The
  Trading System paper manifest received live TRADE and QUOTE for both venues;
  all four snapshots were `LIVE`, execution eligible and measured 129-779 ms
  fresh against their strict limits. No V1/current Redis/consumer was changed.

  VN cannot be certified on this host yet: the official DNSE WebSocket endpoint
  is reachable and authenticated in the prior bounded attempt, but the official
  `openapi.dnse.com.vn` REST history endpoint is not reachable at TCP 443 from
  this new host. Existing V1 Parquet lacks the exact raw provider lineage needed
  to claim `DNSE_DIRECT`, so it is deliberately not relabeled or injected. This
  is an external provider/egress gate, not permission to fabricate runtime data.
  Phase B still requires restart/failover, two-broker outage, Redis rebuild,
  full regression/release evidence and exact candidate cleanup.
- `2026-08-19 PHASE B B13 RECOVERY DEFECT CONFIRMED, FIX IN PROGRESS`:
  restarting only the Binance USD-M native ingestor advanced its persisted
  connection generation from 1 to 2 while all other sources remained at 1; the
  three Rust workers continued processing with zero quarantine, so generation
  restart/fencing behavior passed. Active/passive testing then stopped the exact
  lease owner. The standby acquired epoch 2 and its readiness changed from 503
  `STANDBY` to 200 `READY`, but the first resume attempt overlapped takeover and
  received retryable `GATEWAY_FENCED`; the continuity gate must be rerun after a
  stable owner is observed.

  Bounded logs also exposed a real interceptor defect whenever an SDK stream is
  closed: the unary-stream wrapper keeps a `ContextVar` token across `yield`, so
  async-generator finalization may reset that token from a different context and
  emit `Task exception was never retrieved`. The repair must scope authorization
  around each awaited iterator step and explicit iterator close, never across a
  yielded response. Authentication/authorization semantics, public gRPC schema
  and cursor behavior cannot change. Tests must prove clean client cancellation
  with no unhandled loop exception, retain all authorization failures, then rerun
  checkpoint -> owner stop -> epoch takeover -> contiguous replay/live. V1 and
  all non-candidate state remain untouched.
- `2026-08-19 PHASE B B13 STREAM FINALIZER REPAIR UNIT-VERIFIED, FAILOVER
  RETEST PENDING`: the gRPC authorization interceptor now establishes and resets
  request identity around each awaited source-iterator step. It holds no
  `ContextVar` token across a yielded response, and explicit iterator close is
  also authorized/reset within one context. Authentication, entitlement, quota,
  cursor and protobuf behavior are unchanged. A new real-gRPC cancellation test
  captures the event-loop exception channel and proves client `aclose()` emits no
  cross-context finalizer error. Phase 5 stream/SDK, Phase 7 security and Phase B
  edge suites passed 51 tests with one pre-existing dependency-gated skip. A new
  immutable Python image and stable-owner checkpoint resume retest remain
  mandatory before B13 closes.
- `2026-08-19 PHASE B B13 IMMUTABLE FAILOVER GATE PASSED`: a clean all-`cfc0246`
  candidate repeated the owner transition with readiness gating. The SDK ACKed
  offset 2,271 on epoch-1 owner `stable-stream-active`; after that exact owner
  stopped, `stable-stream-passive` acquired epoch 2 and the SDK resumed from the
  persisted signed checkpoint at exactly offset 2,272. The event was supplied by
  durable replay before the `LIVE` control, as required for a gap-free handoff.
  No cross-context finalizer, unhandled task, cursor or authorization error was
  emitted. B13 is closed.
- `2026-08-19 PHASE B B14 KAFKA RECOVERY CAPACITY DEFECT CONFIRMED, FIX IN
  PROGRESS`: the two-broker outage correctly failed closed: native producers
  emitted `MessageTimedOut` retry events and no successful ACK, while Rust core
  workers restarted their generations after broker transport failure. During
  quorum restoration, however, Kafka3 was cgroup OOM-killed at its 512 MiB hard
  limit (exit 137, `OOMKilled=true`) while reloading coordinator/transaction
  metadata. Host memory remained healthy with about 9 GiB available, proving the
  failure is an undersized broker cgroup rather than host exhaustion.

  Raise only the stable candidate broker limit to 768 MiB while retaining a 256
  MiB JVM heap, pinned image, bounded CPU and separate volumes. Add a deployment
  regression for the measured headroom, roll the three candidate brokers one at
  a time, and repeat two-broker loss/restore. Acceptance requires all 3/3 healthy,
  no OOM, no false ACK, automatic producer/core/projector recovery, increasing
  durable offsets and fresh query/stream service. This does not change V1 or the
  older Phase 8 rehearsal compose contract.
- `2026-08-19 PHASE B B14 KAFKA RECOVERY HEADROOM UNIT-VERIFIED, RUNTIME
  RETEST PENDING`: `docker-compose.v2-stable.yml` now bounds each broker at 768
  MiB while keeping `-Xms256m -Xmx256m`, 0.75 CPU, pinned image, RF3/minISR2 and
  independent state unchanged. This supplies measured coordinator/native-buffer
  recovery headroom rather than increasing the Java workload. Compose validation
  passed and 16 stable deployment/release contract tests passed, including exact
  resource assertions. A rolling 3/3 broker recreation and repeated two-broker
  outage/restore still gate runtime acceptance.
- `2026-08-19 PHASE B B14 TWO-BROKER OUTAGE/RECOVERY GATE PASSED`: all three
  candidate brokers were rolling-recreated with preserved independent volumes
  and the new 768 MiB bound. The repeated loss of Kafka1 and Kafka2 left Kafka3
  running without OOM; native OKX/Binance publishers emitted retryable delivery
  timeouts and no success ACK under minISR failure. After restoring the two
  brokers, all six raw partitions reported ISR `1,2,3`, all brokers remained
  `OOMKilled=false`, and durable raw offsets advanced from
  `[27775,3876,26500,0,30715,14959]` to
  `[33157,4851,29594,0,34752,16487]`. Rust generations recovered, all workers
  resumed progress with quarantine zero, and a strict Trading System QUOTE
  snapshot/live handoff returned `LIVE`, execution eligible and 239 ms fresh.
  B14 is closed.
- `2026-08-19 PHASE B B15 REDIS REBUILD CERTIFICATION IN PROGRESS`: Redis is a
  rebuildable stable projection and lease dependency, while Kafka plus canonical
  SQLite remain durable authorities. Before recreating only the isolated stable
  Redis, stop the projector and both stream processes so an ephemeral lease epoch
  reset cannot overlap a locally unexpired old owner. Reset only the inactive
  `stable-projector-v1` canonical Kafka group to earliest, restart one active plus
  one standby stream, then restart the projector and require idempotent replay,
  repopulated bounded Redis keys, query readiness, signed cursor replay/live, no
  collision/quarantine and unchanged V1. Production promotion still requires a
  governed external HA lease store or the same all-owner fencing runbook; this
  local rebuild does not claim an independent failure domain.
- `2026-08-19 PHASE B B16 PROJECTION CACHE GENERATION DEFECT CONFIRMED, REPAIR
  APPROVED`: the B15 rehearsal rebuilt Redis while retaining a bounded SQLite
  cache whose oldest event rows had already been trimmed. Resetting the Kafka
  projector group to earliest therefore reintroduced older canonical event IDs
  after their SQLite dedup rows had expired; the cache assigned new logical
  offsets and correctly failed strict BAR continuity with
  `OPEN_SEQUENCE_GAP`. Kafka/canonical records were not lost and V1 was not
  touched, but B15 is not accepted.

  Redis latest state and the SQLite query/stream spool are now one rebuildable
  **projection cache unit** behind Kafka authority. Persist a random cache
  identity in SQLite, bind the dedicated Redis namespace atomically to that
  identity, and verify the binding in every Redis projection transaction.
  Missing or mismatched identity with a non-empty spool fails closed before
  consuming Kafka; a Redis flush during operation also fences the next write.
  Do not retain an unbounded event-ID tombstone table merely to make a bounded
  cache mimic the durable log. Recovery stops projector, query and every stream
  lease owner, recreates only isolated stable Redis plus SQLite cache files,
  resets only `stable-projector-v1` to earliest, then starts stream, projector
  and query in that order. Existing signed cursors expire by design and clients
  must perform a fresh warmup/handoff.

  Acceptance requires unit/real-Redis tests for stable identity across restart,
  mismatch/missing/flush fencing and atomic empty-cache initialization; a full
  canonical replay into a fresh bounded cache; zero gap/collision/quarantine;
  replica-equal query results; SDK replay/live; fresh Trading System paper
  data; and unchanged V1. Rollback removes only the isolated candidate and
  restores the previous immutable image; no production authority or consumer
  route changes are authorized.
- `2026-08-19 PHASE B B16 CACHE-GENERATION FENCING TARGETED TEST PASSED,
  REAL-REDIS/REPLAY GATE PENDING`: the SQLite spool now persists one random
  128-bit lowercase-hex cache identity across process restart and generates a
  new identity only when the cache file is rebuilt. The stable Redis target
  atomically binds its isolated namespace to that identity; every projection
  Lua transaction verifies the binding, so a missing/mismatched identity or a
  Redis flush fences writes rather than creating a partial latest-state view.
  Projector startup permits first binding only when the spool is empty and
  readiness exposes a hashed cache identity, never the raw identifier.

  `python -m unittest -v tests.test_fund_phase2_transport
  tests.test_phaseb_stable_edge` ran inside immutable Python image
  `qdl-v2-python:2.0.0-cfc0246` with read-only source, no network and bounded
  tmpfs: 42 cases ran, 41 passed and only the explicitly environment-gated
  real-Redis integration case skipped. Python compile and `git diff --check`
  passed. An interrupted attempt created only disposable network
  `qdl-phaseb-cache-test`; it was verified and removed before continuing.
  B16 remains `IN_PROGRESS` until the named real-Redis test and fresh atomic
  Kafka replay acceptance pass.
- `2026-08-19 PHASE B B16 REAL-REDIS GENERATION GATE PASSED`: a named
  disposable Redis 7.2 instance with persistence disabled, 16 MiB no-eviction
  bound and isolated Docker network passed the previously skipped integration
  test in 0.210 seconds. The test proved first atomic bind, TTL/non-TTL latest
  writes, one Pub/Sub publication, duplicate suppression, stale lease fencing,
  conflicting cache-ID rejection, live identity loss detection and projection
  rejection after deleting the identity key. Combined with the preceding
  network-disabled run, the B16 targeted gate is 42/42 passed with zero skips.
  Container and network were removed immediately; no candidate, V1 or current
  Redis state was addressed. B16 code is ready for a coherent commit, while
  B.3 still requires the fresh full Kafka -> cache-unit replay runtime gate.
- `2026-08-19 PHASE B B17 ATOMIC CACHE-UNIT REBUILD STARTED`: add one guarded
  operator command for the exact isolated stable project. It must stop the
  projector, both query replicas and every stream lease owner; delete only the
  three SQLite cache files in `stable_state`; flush only `stable_redis`; reset
  only consumer group `stable-projector-v1` on `md.canonical.v2` to earliest;
  start stream owners, then projector; wait for zero bounded lag and bound
  cache identity; finally start query replicas. Kafka brokers/topics/raw data,
  Rust core/acquisition, TLS, manifests and V1 remain untouched.

  The command defaults to plan-only and requires both an explicit apply flag
  and exact confirmation token. A failure leaves readers/projector stopped or
  NOT_READY and is safely rerunnable from Kafka; it never rolls forward a
  partial cache as healthy. Tests must cover the destructive guard, exact
  service/file/topic/group allowlist, lag parsing and fail-closed command
  failure before the command is used on `qdl_v2_stable_candidate`.
- `2026-08-19 PHASE B B17 GUARDED REBUILD COMMAND UNIT-VERIFIED, RUNTIME
  REHEARSAL PENDING`: `scripts/rebuild_v2_stable_projection_cache.py` is
  plan-only by default and requires exact token
  `REBUILD_QDL_V2_STABLE_PROJECTION_CACHE` for apply. Its constants pin the
  stable Compose manifest/project, five cache users, three SQLite files,
  `stable_redis`, `stable-projector-v1` and `md.canonical.v2`; callers cannot
  inject an arbitrary service, volume, topic or consumer group. It starts
  stream -> projector -> query, requires two consecutive zero-lag samples,
  projector/query readiness and non-empty rebuilt Redis.

  Six command-policy tests passed for exact allowlists, confirmation, canonical
  lag parsing, wrong-project rejection and abort-before-delete/flush while a
  cache user remains running. The combined B16/B17 targeted suite ran 48
  cases: 47 passed and only the separately passed real-Redis case skipped in
  the network-disabled invocation. Python compile and diff checks remain clean.
  No runtime was mutated by these tests. B17 now needs a new immutable Python
  image and one execution against the isolated cfc0246 candidate state.
- `2026-08-19 PHASE B B17 FIRST RUNTIME REHEARSAL FAILED CLOSED AT AN
  OVER-STRICT LAG GATE`: the guarded command stopped all five cache users,
  recreated only SQLite cache files plus isolated Redis, reset only the
  canonical projector group and replayed the authentic Kafka log with image
  `e002da6`. Lag converged from about 448,000 to 32 records within the
  900-second bound, but continuous live provider input prevented two exact-zero
  samples. The command raised `TimeoutError` and left both query replicas
  stopped; no partial cache was served and V1 remained unchanged.

  Exact zero is not a valid steady-state requirement for an actively written
  topic. The bounded correction requires three consecutive samples at or below
  250 total records across exactly six canonical partitions, then projector
  cache-generation readiness. Query starts only afterward, and strict
  warmup/gap/freshness plus SDK/Trading-System checks remain the authoritative
  data acceptance. Increasing the bound at runtime, stopping acquisition to
  manufacture zero, or weakening freshness/gap policy is forbidden.
- `2026-08-19 PHASE B B17 CORRECTED ATOMIC REBUILD PASSED`: the same guarded
  command completed against `qdl_v2_stable_candidate` after the fixed bounded
  live-lag policy was applied. It observed all six canonical partitions, three
  consecutive samples at or below the immutable 250-record bound, a maximum
  accepted sample of 232 and final observed lag of 63. It rebuilt the isolated
  Redis namespace to 47 keys, started stream -> projector -> query in the
  declared order, and did not address V1, Kafka data, Rust acquisition/core or
  production consumers.
- `2026-08-19 PHASE B B18 STRICT CONSUMER ACCEPTANCE FAILED CLOSED ON A REAL
  BAR GAP`: immediately after the fresh Redis-plus-SQLite rebuild, the
  authenticated SDK warmup for the registered Binance/OKX BAR consumers raised
  `required feed has an unresolved sequence gap`. This proves the earlier
  retained-cache hypothesis was incomplete: the gap is reproducible from the
  canonical Kafka log into a new cache. The acceptance stopped before Trading
  System execution data was exposed. B.3 remains `IN_PROGRESS`; the next
  bounded repair must identify the exact venue/partition/open-time discontinuity
  and fix provider bootstrap/canonical ordering or revision handling from real
  data. Synthetic gap filling and policy relaxation are forbidden.
- `2026-08-19 PHASE B B18 ROOT CAUSE CONFIRMED FROM REAL PROVIDER DATA`:
  read-only inspection of all four continuous BAR partitions found exactly one
  discontinuity in each Binance Spot/USD-M partition: `16:52 -> 16:54 UTC`; both
  OKX partitions were continuous. At 16:54 the BAR edge fetched four closed
  provider bars but Kafka durable ACK failed. `run_cycle()` advanced its
  in-memory `_last_open_ms` before receiving the ACK and, on recovery, requested
  only the newest closed bar. It therefore skipped the unacknowledged Binance
  16:53 bar permanently. No explicit provider gap flag, duplicate open time or
  generated row was present.

  The approved B18 repair is provider-neutral BAR catch-up inside the existing
  edge: derive the missing interval count from the last ACKed open time, fetch a
  bounded closed-history window when more than one bar is pending, verify exact
  continuity/finality, publish the complete ordered batch, and advance each
  watermark only after every Kafka ACK. Publish failure must preserve the old
  watermark so the next cycle retries the same range. Tests must cover one-bar
  fast path, multi-bar catch-up, publish failure/retry, incomplete provider
  history fail-closed and Binance/OKX parity. Then rebuild the isolated cache
  and rerun strict authenticated consumer acceptance.
- `2026-08-19 PHASE B B18 ACK-AUTHORITATIVE BAR CATCH-UP IMPLEMENTED AND
  UNIT-VERIFIED`: the shared Binance/OKX BAR edge now freezes one observation
  boundary per cycle, uses the one-bar latest path normally, and fetches up to
  1,000 real closed provider bars only when the last ACKed watermark proves a
  backlog. It validates exact interval boundaries and complete ordered coverage,
  publishes the whole batch, and advances per-binding watermarks only after all
  Kafka acknowledgements. Provider incompleteness, non-integral boundaries,
  excessive backlog and partial/failed ACKs fail closed without watermark
  mutation.

  Focused deployment/history tests ran 16/16 passing with network disabled. New
  cases prove an eight-row Binance/OKX catch-up is retried identically after an
  injected Kafka ACK failure, and incomplete history reaches neither publisher
  nor watermark. Python compile and diff checks passed. B18 is not runtime-
  accepted until an immutable image heals the authentic 16:53 gaps via provider
  history, a fresh cache rebuild reports zero open gaps, and strict SDK/Trading
  System acceptance passes.
- `2026-08-19 PHASE B B18 RUNTIME HEAL EXPOSED CANONICAL DUPLICATE COLLISION`:
  immutable image `2041f18` bootstrapped 2,000 real provider BAR rows with full
  Kafka ACK, but the projector rejected repeated historical BARs because their
  semantic `event_id`/`canonical_payload_hash` matched existing rows while
  capture/session/timing provenance correctly differed. Generic SQLite collision
  enforcement is therefore not weakened: the repair belongs in the stable
  projector, which understands canonical semantics. It may classify only an
  identical 32-byte canonical payload hash under the same event ID and partition
  as a semantic duplicate; a changed market payload remains a hard collision.

  The same repair must keep late historical BARs in the bounded query cache
  without replacing a newer Redis/latest projection. Exact Kafka replay after a
  crash still reapplies an existing record before checkpoint; a later semantic
  duplicate is checkpointed without fan-out. Tests must prove changed semantics
  fail closed, provenance-only duplicate recovery is idempotent, late BAR repair
  closes history without latest regression, and full fresh replay remains
  deterministic. Runtime quarantine inspection found 417 records, all old OKX
  stale-generation rows and none for the missing Binance 16:53 bars; no fake or
  provider-invalid row is being accepted.
- `2026-08-19 PHASE B B18 SEMANTIC DUPLICATE AND LATE-BAR REPAIR UNIT-PASSED`:
  the stable projector now keeps generic SQLite byte-collision fencing strict
  while handling one narrower canonical case: the same event ID, partition and
  independently recomputed 32-byte market-payload hash may be checkpointed as
  an idempotent provenance-only duplicate without a second Redis/stream fan-out.
  A changed payload, missing/invalid hash or changed partition remains a hard
  `EventIdCollision`. Late historical BARs are durably admitted to the bounded
  query history but are not projected over a newer latest BAR.

  The focused stable-edge/deployment/history run executed 46 cases in the
  immutable `2041f18` Python test image with network disabled: 45 passed and the
  separately proven real-Redis integration case was the sole conditional skip.
  Tests explicitly prove provenance-only idempotence, changed-semantics
  rejection, late-history repair without latest regression, ACK retry,
  incomplete-history fencing, Binance/OKX BAR parity and ordered checkpointing.
  No runtime, provider, Kafka, Redis, V1 route or durable volume was mutated by
  this unit gate. B.3 remains `IN_PROGRESS` until the committed image passes the
  isolated atomic rebuild and strict real-provider consumer acceptance.
- `2026-08-19 PHASE B B18 RUNTIME RETEST FAILED CLOSED AND B19 BOUNDED
  REPAIR STARTED`: immutable projector image `8851166` correctly passed the
  provenance-only duplicates but the guarded earliest replay exceeded its
  900-second operator timeout at 733,158 records of lag and then exposed real
  changed-semantics BAR collisions. Query replicas remained stopped, projector
  was stopped after diagnosis, V1 port 8100 remained healthy and no consumer
  received candidate data.

  Read-only Kafka/SQLite inspection at the frozen offsets found two distinct
  causes. OKX REST/WS BARs were numerically identical but differed only in exact
  decimal spelling/scale and acquisition origin; byte/hash equality is therefore
  stricter than market-semantic equality. Binance REST BARs differed in actual
  close, volume, quote volume and trade count: the earlier `VENUE_NATIVE` row was
  observed too close to the minute boundary and a later settled backfill changed
  it. The next bounded B19 repair may normalize only BAR decimal values and
  acquisition origin for semantic duplicate comparison; every non-BAR payload,
  identity, timestamp, interval, lifecycle, unit and actual numeric value remains
  strict. The provider edge also gains an aligned close-settlement grace so it
  never labels a just-closed mutable row as accepted final data. Changed numeric
  BARs continue to fail closed; no revision is fabricated in the projector.
  Unit/parity tests precede any new runtime image. Existing isolated Kafka/data
  volumes are retained pending an explicit clean-candidate decision.
- `2026-08-19 PHASE B B19 BAR SEMANTICS AND SETTLEMENT UNIT-PASSED`:
  the projector now validates each exact Decimal audit spelling against its
  coefficient/scale, compares BAR numbers with exact `Decimal` arithmetic and
  ignores only acquisition origin plus equivalent trailing-zero spelling for
  duplicate classification. Interval, times, OHLCV/base/quote/contract values,
  trade count, finality, revision, lifecycle, supersession and quantity unit
  remain strict; all non-BAR feeds still require equal canonical payload hashes.
  The generic durable spool remains byte-immutable.

  The shared real-provider BAR edge now uses an explicit two-second settlement
  grace for bootstrap and latest/catch-up observation and aligns its minute loop
  to that delay. The value is frozen in the isolated compose and bounded to
  1-10 seconds. The stable edge/deployment/history suite ran 48 network-disabled
  cases: 47 passed and the separately proven real-Redis conditional case was
  the sole skip. New cases prove trailing-zero/origin equivalence, changed BAR
  numeric rejection and equal settled observation for Binance/OKX. No runtime
  was restarted by this gate. A clean isolated runtime data-log decision remains
  required because the retained Kafka test log contains previously captured
  materially wrong early-final Binance rows and must not be accepted by policy
  relaxation.
- `2026-08-19 PHASE B B19 REAL-CAPTURE CLASSIFIER GATE PASSED, RUNTIME
  REMAINS FAIL-CLOSED`: immutable image `c61fa39` classified two retained Kafka
  records against the read-only SQLite cache exactly as required: the OKX
  REST/WS BAR with equivalent numeric Decimals returned `same_market_semantics=true`;
  the Binance BAR with changed close/volume/trade count returned `false`. The
  diagnostic used the authorized projector identity with manual assignment and
  no offset commit. It validates the classifier against durable provider bytes,
  not generated market data.

  The old isolated Kafka log is not releasable evidence because it already
  contains early-final Binance values from the superseded +1-second edge. No
  policy was weakened to ingest them. Python candidate roles were consolidated
  onto immutable image ID `b0560895...` and left stopped; Kafka/Rust/Redis and
  all volumes were retained for audit. V1 health on port 8100 remained OK.
- `2026-08-19 PHASE B B19 INCREMENTAL ARTIFACT CLEANUP PASSED`: after exact
  container-reference checks, cleanup removed only unreferenced Python tags
  `e002da6`, `2041f18` and `8851166`. It then removed four QDL BuildKit records
  older than one hour (608.7 MB). Image storage fell from 11.1 GB to 9.278 GB;
  build-cache storage fell from 9.097 GB to 8.488 GB, for about 2.43 GB total
  recovery. The retained artifacts are V1, active candidate `c61fa39`, one
  Python/Rust rollback `cfc0246`, Kafka/Redis and all 17 volumes. No broad prune,
  volume deletion, topic reset or production mutation occurred.
- `2026-08-20 PHASE B.3 CLEAN RUNTIME CLOSURE RESUMED`: the operator resumed
  completion of the remaining B.3 durability/recovery gates before any B.4
  work. The operator explicitly approved stopping only
  `qdl_v2_stable_candidate` and deleting its exact four test volumes
  `kafka1_data`, `kafka2_data`, `kafka3_data` and `stable_state`; `stable_tls`,
  every V1/production container and every production volume remain protected. The rehearsal reuses only the isolated Docker project
  `qdl_v2_stable_candidate`; it does not create another release topology or
  address V1. The exact reset scope is limited to candidate volumes
  `kafka1_data`, `kafka2_data`, `kafka3_data` and `stable_state`, whose retained
  records are already classified as invalid release evidence because they
  contain superseded early-final Binance BARs. Candidate TLS/credentials are
  preserved. V1 containers, Redis, Parquet, provider state, ports, routes and
  consumer authority are immutable.

  The closure gate is: fresh real-provider bootstrap under immutable
  `c61fa39`; ACK-authoritative BAR catch-up with the fixed two-second settlement
  boundary; zero canonical gap/collision/quarantine; atomic Redis-plus-SQLite
  rebuild; replica-equal query watermarks; signed SDK warmup -> replay -> LIVE
  for registered Binance/OKX bindings; fresh Trading System paper snapshots;
  bounded lag/resources; and unchanged V1 health. Any failure stops candidate
  readers and leaves V1 authoritative. B.3 remains `IN_PROGRESS` until all
  gates and exact cleanup evidence are recorded; B.4 remains forbidden.

  The first reset command was rejected by the safety gate before execution
  because exact deletion approval was not yet explicit. Candidate containers,
  all five candidate volumes and V1 remain unchanged; no workaround was used.
- `2026-08-20 PHASE B.3 CLEAN BOOTSTRAP PASSED, RECOVERY SAFETY REPAIR IN
  PROGRESS`: after exact approval, only the candidate project was stopped and
  the three Kafka plus one state volume were deleted; `stable_tls` and V1 were
  verified preserved. Fresh RF3/minISR2 Kafka, Redis and state started; broker
  bootstrap passed three topics, six partitions, mTLS and ACLs. Read-only TLS
  validation found all nine expected files. Real-provider acquisition ACKed 500
  settled BARs for each Binance USD-M, Binance Spot, OKX SWAP and OKX Spot
  binding (2,000 total), then ACKed the next closed cycle; Rust workers reported
  zero quarantine/collision/error and canonical offsets advanced.

  Before the atomic rebuild, the safety gate identified that Compose dependency
  traversal could rerun `stable_tls_init` when starting stream/query roles. It
  was blocked before TLS mutation. The bounded repair makes recovery role start
  explicit with `up --no-deps` after the project, Kafka, Redis, state and TLS
  invariants are validated, and adds a unit test for the exact command. No
  public contract, provider semantics, V1 route or authority changes.
- `2026-08-20 PHASE B.3 CLEAN RUNTIME CLOSURE PASSED`: the safety repair passed
  8/8 focused unit tests. The guarded atomic rebuild then deleted only the three
  candidate SQLite cache files, flushed only candidate Redis and reset only
  `stable-projector-v1` on `md.canonical.v2`. It observed all six partitions
  for three consecutive samples within the fixed 250-record gate, with observed
  bound 46 and final lag 19, then opened query replicas only after projector and
  Redis readiness; Redis rebuilt to 47 keys.

  Signed public SDK acceptance used real-provider data with
  `test_provenance=false`. Binance and OKX each returned 500 final 1-minute BARs
  with full coverage, no open gap and replica-equal market semantics/watermark
  512. The only per-request replica difference was the expected clock-derived
  `quality.freshness_ms`; all identity, timestamps, payload, source, contract
  and other quality fields were equal. Both alpha streams emitted
  `REPLAYING -> LIVE`, then two strictly contiguous events
  (Binance 35752-35753; OKX 13155-13156) were ACKed. Trading System paper read
  Binance/OKX TRADE and QUOTE snapshots at 146-316 ms freshness; all four were
  execution eligible.

  Read-only cache verification found 73,456 canonical records across 12 bounded
  partitions, maximum 10,000 per partition, zero retained offset gaps, zero
  duplicate event IDs and zero quarantine rows. Kafka quarantine offsets were
  zero on all six partitions; observed projector/core lag totals were 34/12.
  Exactly one stream owner was READY and its peer STANDBY. Candidate TLS matched
  the preserved source bundle SHA-256, V1 health remained `ok`, Redis used
  3.23 MiB/160 MiB and the largest app role used 69.87 MiB/512 MiB; Kafka
  brokers stayed within 461.4 MiB/768 MiB. Logs contained no application
  collision, unresolved gap, panic or quarantine; Kafka startup emitted only
  benign internal-topic-already-exists warnings.

  The final five-module Phase B regression ran 63 cases: 62 passed and one
  separately proven real-Redis conditional case skipped in network-disabled
  mode. The acceptance harness itself failed closed before the final pass on
  least-privilege env access, an invalid test-only `event_id` assumption,
  request-time freshness comparison and expected control-event handling; none
  mutated provider/Kafka/Redis data or exposed candidate output to production.
  B.3 conclusion is `PASS`/`COMPLETE`. B.4 remains `NOT_STARTED`; no
  production cutover or consumer authority migration is authorized.
- `2026-08-20 PHASE B.4 RELEASE CERTIFICATION STARTED`: B.3 is complete and
  the operator approved B.4 only. Commit `5054e1e` is the provisional common
  source SHA. Required gates are full Python discovery; Rust format, locked
  workspace Clippy and tests; Buf format/lint/two-baseline breaking/generation;
  OpenAPI semantic compatibility; package/release/security/capacity suites;
  immutable Python and Rust images carrying the same source SHA; isolated
  candidate rolling recreation; compact evidence; exact unreferenced
  image/cache cleanup; and unchanged V1 health/OpenAPI/topology.

  Rollback is to stop only recreated candidate roles and restore pinned
  `c61fa39` Python plus `cfc0246` Rust images against preserved candidate
  Kafka/state/TLS. Public V1, provider data, production Redis/Parquet, routes and
  consumer authority are immutable. A failed gate leaves B.4 in progress and
  cannot be relabeled as technical debt. Push, merge, release publication and
  production cutover remain outside scope.
- `2026-08-20 PHASE B.4 PYTHON CERTIFICATION PASSED`: immutable
  `c61fa39` dependencies with source bind at `5054e1e` ran full unittest
  discovery in a network-disabled, read-only, cap-dropped container bounded to
  768 MiB, 1.5 CPU and 256 PIDs. It executed 503 tests: 497 passed and six
  explicit conditional/infrastructure cases skipped. Failure-path ERROR and
  CRITICAL logs were injected assertions and their tests passed. No candidate,
  V1, provider, Kafka, Redis or volume state was addressed. Rust and contract
  gates remain pending; B.4 stays `IN_PROGRESS`.
- `2026-08-20 PHASE B.4 RUST PACKAGING DEFECT FOUND, BOUNDED FIX STARTED`:
  the exact-SHA Rust builder compiled all release binaries, but full locked
  workspace tests failed at compile time because `Dockerfile.phase8-rust`
  copied Rust sources/generated bindings without the immutable
  `contracts/golden` and `tests/fixtures/phase2` oracle files referenced by
  Rust tests. No runtime or domain assertion failed, and no candidate was
  recreated. The in-scope fix copies only those two bounded test inputs into
  the builder stage and adds release-packaging assertions. Runtime stage,
  binaries, contracts and provider behavior remain unchanged. Rust fmt/Clippy/
  tests must be rerun from a rebuilt builder before this gate can pass.
- `2026-08-20 PHASE B.4 RUST CERTIFICATION PASSED`: the rebuilt bounded
  builder included only the two missing immutable oracle directories.
  `cargo fmt --all -- --check`, locked workspace Clippy with
  `-D warnings`, and the full workspace test gate passed under no-network,
  cap-dropped, no-new-privileges execution bounded to 3 CPU, 3 GiB and 512
  PIDs. All 62 Rust tests passed with zero failures/skips, covering exact
  Python/Rust golden bytes, Binance/OKX provider parsing, quantity/decimal
  identity, generation fencing, replay/dedup/gap/quarantine, final BAR
  semantics, Kafka TLS/transaction headers, authority handoff/rollback,
  backpressure classes and VN source semantics. The targeted Python packaging
  regression also passed 6/6 and `git diff --check` was clean. No candidate
  or V1 runtime was mutated. Contract/security/package/capacity gates remain.
- `2026-08-20 PHASE B.4 CONTRACT CERTIFICATION PASSED`: Buf 1.50.0
  format, lint, breaking checks against both frozen Phase 1 and Phase 7
  baselines, and generation all passed; regenerated Python/Rust artifacts had
  zero Git diff. Seven generated-contract/golden tests passed in a read-only,
  no-network Python container. OpenAPI semantic comparison against `dev`
  reported `PASS_PRE_BETA_FREEZE`, 10 operations, 42 schemas and zero removed
  operation/response/schema/enum or security/required-parameter change.
  Candidate/V1 runtime and durable state were not addressed. Security,
  package, capacity and final one-SHA image gates remain.
- `2026-08-20 PHASE B.4 CAPACITY DIAGNOSTIC RECORDED`: the approved
  Phase 2 durability gate passed at 3,042 append events/s, p99 14.4 ms,
  22,688 replay events/s and 6.03x disk amplification; the eight-replica V2
  API gate passed at 706 requests/s and p99 181 ms with zero venue connection.
  A separate exploratory Phase 6 run requested 10,000 normal / 40,000 burst
  events/s while cgroup-limited to 2 CPU and failed closed at 5,656 events/s.
  Those rates are not the script's approved 500/1,500 certification defaults,
  so this is retained as non-release diagnostic evidence rather than a code
  defect or a lowered threshold. The approved bounded profile and final Rust
  release benchmark must still pass. No runtime/data was mutated.
- `2026-08-20 PHASE B.4 RUST SUPPLY-CHAIN POLICY DEFECT FOUND`:
  checksum-verified cargo-deny 0.20.2 fetched the current RustSec database and
  reported advisories, bans and sources clean, but license validation failed
  because the Rust builder did not copy the repository's existing `deny.toml`.
  Cargo-deny inside that artifact therefore used its default deny-all license
  policy and rejected normal MIT/Apache/BSD dependencies; host CI still had
  the tracked policy. The bounded repair copies the reviewed policy into the
  builder, makes its Linux target/advisory/license/source boundaries explicit,
  and adds release regression assertions. No dependency, runtime or market
  semantics change.
- `2026-08-20 PHASE B.4 CAPACITY AND SECURITY GATES PASSED`: the
  approved 80-partition Phase 6 profile passed two normal windows at
  503.62/503.70 events/s and burst at 1,503.07 events/s, p99.9 128.055 ms,
  zero queue rejection/replay mismatch and negative measured memory growth.
  The reviewed `deny.toml` targets Linux production, has no advisory/license
  exception, denies wildcard dependencies and unknown registry/Git sources,
  and permits only the encountered permissive licenses. Checksum-pinned
  cargo-deny 0.20.2 passed advisories, bans, licenses and sources; duplicate
  transitive versions remained non-blocking graph warnings. Pip-audit reported
  no known Python vulnerability. Pinned Trivy 0.73.0 source/config scanning,
  excluding only runtime data/logs and compiled target artifacts, found zero
  HIGH/CRITICAL misconfiguration and zero secret across 15 analyzed targets.
  Seven release-policy/package tests passed and `git diff --check` remained
  clean. Final full regression, one-SHA images, image scans and candidate
  recreation remain.
- `2026-08-20 PHASE B.4 FINAL SOURCE REGRESSION PASSED`: an initial
  read-only rerun omitted the required `/app/logs` tmpfs and stopped four
  import modules at `RotatingFileHandler`; 495 other tests ran and no domain
  assertion failed. With the documented non-root writable log tmpfs restored,
  full network-disabled/read-only discovery passed 504 tests: 498 passed and
  six explicit conditional/infrastructure cases skipped. Test-injected timeout,
  stale-source, queue-fence and recovery logs remained expected assertions.
  This closes source-level regression after the Docker packaging and supply-
  chain policy repairs. The next gate is freezing the source commit and building
  both immutable images from that exact SHA.
- `2026-08-20 PHASE B.4 IMMUTABLE ARTIFACT GATE PASSED`: source
  commit `ea84a21be71572674cc5b160788d8edd0f870738` produced Python image
  `sha256:00ffbd5b...` and Rust image `sha256:b464f342...`; both carry
  exact OCI revision/version and run non-root. Trivy found zero
  HIGH/CRITICAL vulnerability and zero embedded secret in either image.
  Final-SHA Rust processed 100,000 events at 129,256 events/s, p99 12.906
  microseconds, zero duplicate and zero quarantine against the 50,000/s gate.
  The final builder contained its policy and cargo-deny again passed all four
  checks.
- `2026-08-20 PHASE B.4 CANDIDATE ROLLING GATE FAILED CLOSED`: only 13
  isolated application roles were recreated one at a time with `--no-deps`;
  Kafka, Redis, TLS/state volumes and V1 were preserved. Both query replicas
  became READY and exactly one stream peer was READY while the other was
  STANDBY, but the projector then repeatedly rejected
  `canonical event ID maps to different market semantics`. Two Rust workers
  reported 497 quarantines after restart. This violates the zero-collision/
  zero-quarantine release invariant even though V1 remained `ok`. B.4 stays
  `IN_PROGRESS`; candidate readers/workers must be stopped fail-closed,
  durable evidence inspected and the root cause fixed or the pinned
  `c61fa39`/`cfc0246` rollback restored before any acceptance claim.
- `2026-08-20 PHASE B.4 REAL-PROVIDER ROOT CAUSE AND REPAIR BOUNDARY`:
  read-only Kafka/projector inspection decoded 994 committed quarantine records
  as OKX `candle1m` `STALE_GENERATION`; the REST BAR edge reused generation
  1 while the native WebSocket owner had advanced the same canonical partition.
  At the projector checkpoint, repeated Binance REST warmup also reused an
  immutable revision-0 event ID for materially changed close/volume/trade-count
  values. The projector and Rust fencing behaved correctly and remain strict.
  The bounded repair makes closed 1m BAR acquisition single-owner REST for both
  Binance and OKX while Rust remains the sole canonical core, persists an
  authority/catalog-bound last-ACKed BAR watermark atomically in
  `stable_state`, skips overlapping bootstrap after restart, and uses the
  approved 10-second settlement ceiling. Corrupt/mismatched state, incomplete
  history, changed immutable BAR semantics, partial Kafka ACK and stale
  authority all continue to fail closed. Public V1/V2 contracts, event identity,
  query semantics, V1 port 8100 and production authority do not change. Gates:
  restart/corrupt-state/ACK-loss tests, native subscription manifest proof,
  Python/Rust full regression, then a clean isolated real-provider rehearsal
  with zero gap/collision/quarantine and restart continuity.
- `2026-08-20 PHASE B.4 BAR OWNERSHIP/CHECKPOINT REPAIR UNIT-PASSED`:
  acquisition manifest revision 2 assigns all four Binance/OKX final 1m BAR
  bindings to the single Python REST edge; the Rust native ingestors retain
  eight lossless/latest-state TRADE/QUOTE bindings and Rust remains the only
  canonical core. The edge now persists each last fully Kafka-ACKed open time
  by atomic write/fsync/rename in isolated `stable_state`, restores only an
  exact slice/authority/catalog/acquisition match, resumes partial bootstrap,
  rejects corrupt/future watermarks and never advances on partial ACK. Compose
  mounts the state volume, waits for its initializer and uses the bounded
  10-second settlement ceiling. Public APIs/event IDs/projector fencing are
  unchanged. Syntax and compose validation passed; 18 targeted tests passed,
  then all five Phase B modules ran 65 cases: 64 passed and the separately
  proven real-Redis integration case was the sole skip. No app role, Kafka,
  Redis, volume, V1 route or provider state was mutated by this test slice.
  Full Python/Rust regression, immutable same-SHA artifacts and clean isolated
  real-provider restart acceptance remain.
- `2026-08-20 PHASE B.4 REPAIR FULL SOURCE GATE PASSED`: two initial full
  Python invocations reached 497 tests but four import modules could not open
  `/app/logs/app.log` because the child tmpfs was root-owned; no domain
  assertion failed. With the same source and a bounded log tmpfs owned by
  non-root UID/GID 10001, full discovery ran 506 tests: 500 passed and six
  explicit conditional/infrastructure cases skipped. Rust format, locked
  workspace Clippy with warnings denied and all 62 workspace tests passed in
  the exact builder with network disabled. Compose rendering and
  `git diff --check` also passed. Test-injected timeout, queue-fence,
  checkpoint and stale-source logs were expected assertions. No candidate/V1
  process or durable state was mutated. Freeze a final journal commit, build
  both images from that one SHA, rescan and run isolated real-provider restart
  acceptance next.
- `2026-08-20 PHASE B.4 FINAL ARTIFACT/RUNTIME GATES PASSED`: final code
  commit `2412572eaa89864ce74910b0f2e5f8b50833fb15` produced Python image
  `sha256:fec269ec555624baa68ee15fdd0281d72996e55f847b7347856be6b2fa51ea25`
  and Rust image
  `sha256:fbff0ed3c4390831a2aebf12f57c266eb6f01dde258b3cffacacbcbaa30d6c97`.
  Both are non-root and carry the exact OCI revision/version. Pinned Trivy
  0.73.0 under the repository CI policy found zero fixable HIGH/CRITICAL
  vulnerability and zero secret in both. The stricter no-ignore diagnostic
  exposed only currently unfixed Debian findings and was retained as diagnostic
  rather than hidden. The final Rust benchmark processed 100,000 events at
  133,477.5 events/s, p99 14,124 ns, zero duplicate/quarantine against the
  50,000/s gate; 25/25 final package/deployment tests passed.

  A fresh isolated RF3/minISR2 candidate bootstrapped 500 authentic closed 1m
  BARs for each Binance USD-M, Binance Spot, OKX SWAP and OKX Spot binding.
  Restart restored the revision-2 ACK checkpoint, skipped overlapping history
  and caught up the exact closed-bar backlog. Cache inspection found 75,187
  canonical records across 12 bounded partitions, maximum 10,000 each, zero
  offset gap, duplicate event ID or quarantine; Kafka quarantine offsets were
  zero and projector lag was 35 under the 250 gate. Redis used 1.22 MiB/128
  MiB with 51 keys; app and broker roles stayed inside all configured bounds;
  application logs had no warning/error/collision/gap during acceptance.
- `2026-08-20 PHASE B.4 FINAL CONSUMER ACCEPTANCE PASSED`: signed released
  SDK clients read 500 final real-provider Binance and OKX BARs with replica-
  equal market semantics. Both alpha streams reached `REPLAYING -> LIVE`,
  ACKed contiguous events and resumed from durable cursor at exactly the prior
  offset + 1. Trading System paper read authoritative/execution-eligible
  Binance and OKX TRADE/QUOTE snapshots at 132-158 ms freshness; monitoring
  reads were authoritative at 100-180 ms. No order, synthetic data or
  production mutation occurred. DNSE remains the already recorded official-
  provider external gate, so Phase B overall is `PARTIAL_EXTERNAL`; it does not
  invalidate B.4 artifact certification or permit cutover.
- `2026-08-20 PHASE B.4 CLEANUP AND CLOSURE PASSED`: removed the fresh
  `qdl_v2_b4_candidate` project and all five disposable volumes; removed the
  stopped old candidate containers/networks and only its four approved Kafka/
  state test volumes while preserving `qdl_v2_stable_candidate_stable_tls`.
  Removed three builder tags, superseded `ea84a21` Python/Rust tags and the
  unused Python `cfc0246` tag. Retained final `2412572`, V1 and the tested
  Python `c61fa39`/Rust `cfc0246` rollback pair. Exact-ID pruning of 41 B.4
  BuildKit records reduced cache from 168/12.94 GB to 154/10.94 GB; no broad
  prune ran. The exact `/tmp` secret bundle, scan output and SDK harness were
  deleted after bounded evidence was recorded. V1 was never restarted and remained `status=ok`, Redis true,
  recent queue drops zero and DNSE `OPEN_HEALTHY`. Full evidence is frozen in
  `upgrade/evidence/PHASE_B4_RELEASE_CERTIFICATION_REPORT.md`. B.4 conclusion
  is `PASS`/`COMPLETE`; push, merge, release publication and authority/consumer
  cutover remain unapproved.
- `2026-08-19 PHASE B ARTIFACT CLEANUP POLICY RECORDED`: Phase B ends at B.4;
  B17/B18 are repair slices inside B.3, not new subphases. Exact cleanup retains
  V1, active `e002da6`, active/rollback `cfc0246`, Kafka/Redis and all durable
  volumes. Obsolete unreferenced QDL image tags and the two named test-builder
  containers are removed after reference validation; shared host-wide prune is
  forbidden. Final BuildKit cleanup is scoped to Data Layer build records and
  recorded with before/after bytes.
- `2026-08-19 PHASE B INCREMENTAL ARTIFACT CLEANUP PASSED`: reference-aware
  cleanup removed exactly two disposable test-builder containers and 47 obsolete
  unreferenced QDL image tags. It retained `data-layer:v0.1.0`, Python candidate
  `2.0.0-e002da6`, Python/Rust runtime rollback `2.0.0-cfc0246`, every running
  Kafka/Redis image and all 17 volumes. Image cleanup reduced root filesystem
  use from 91 GiB to 73 GiB. BuildKit cleanup was limited to records matching
  `description~=qdl` and older than one hour; cache fell from 50.2 GB to 6.484
  GB and root filesystem use fell again to 47 GiB. Total host space recovered
  was about 44 GiB. No host-wide prune ran, candidate/V1 containers remained
  running and no data volume was deleted. Post-cleanup B16/B17 regression
  ran 49 tests in the immutable `e002da6` Python image with network disabled:
  48 passed and the separately proven real-Redis conditional case was the sole
  skip; Python compile and `git diff --check` passed.
- `RUNTIME UNCHANGED`: port 8100 still serves V1 from the existing container;
  no restart, authority mutation or consumer migration has occurred.

### Phase C - Production V2 And Rust Authority Cutover

**Status:** `C.0 RELEASE/MERGE PREPARATION IN PROGRESS / PRODUCTION CUTOVER NOT AUTHORIZED`

**Purpose:** move approved Binance and OKX feed slices from the current V1
authority to the stable V2 contract with Rust as the actual canonical realtime
authority and Python retained as the API/SDK/history/control/projector edge.
DNSE remains a declared external debt and is disabled from initial production
promotion until its provider gates pass.

**2026-08-20 pre-cutover audit facts:**

- public port `8100` currently serves `data-layer:v0.1.0`; OpenAPI exposes 40
  V1 paths and zero V2 paths;
- no V2 stable container is running; only the retained
  `qdl_v2_stable_candidate_stable_tls` volume remains;
- the feature branch is more than 80 commits ahead of `dev`; the latest
  released `2.0.0-2412572` images predate the bounded DNSE closure commits;
- the stable compose and realtime binaries deliberately accept only
  `RUST_SHADOW`; Phase 9.2 proves the CAS/handoff/fencing behavior in an
  isolated rehearsal but is not wired into the long-running stable runtime;
- therefore changing an environment variable or routing consumers directly to
  the current candidate would be an invalid cutover and could create ambiguous
  writer authority.

The operator procedure and merge/cutover commands are frozen in
`docs/runbooks/v2-production-rust-authority-cutover.md`.

#### C.0 Release Branch Closure And Production-Authority Design

**Goal:** merge the already-certified V2 implementation through `dev`, then
implement the missing long-running authority wiring on a new feature branch
without mutating V1.

**Required work:**

1. Fix plan/document drift, run CI-equivalent source/contract/security suites,
   push the current feature branch and merge it to `dev` only after CI passes.
   Do not merge `main` and do not deploy from an unmerged worktree.
2. Create `feat/v2-production-authority-cutover` from updated `dev`.
3. Connect the Phase 9 persistent PostgreSQL CAS and immutable audit/handoff
   records to a transactional authority outbox and compacted Kafka authority
   topic. A database transition and its outbox record are one transaction;
   retries are idempotent and stale revision/owner/lease/plan fail closed.
4. Make the long-running Rust canonical sink consume and reconstruct that
   authority stream, use the existing Phase 9.2 fence at every durable target,
   and refuse canonical/public/compatibility writes until its exact authority
   and target watermarks are reconstructed.
5. Keep acquisition separate from publication authority. Binance/OKX native
   acquisition may publish authenticated raw events only under the current
   slice lease; Python adapters may never bypass the Rust canonical core.
6. Add an operator CLI for preflight, transition, fence, rollback and status.
   The CLI prints identities/revisions/watermarks/digests only, never secrets.
7. Build one immutable Python/Rust image pair from the merged SHA, generate
   SBOM/provenance, and retain exactly one tested V1 rollback generation.

**C.0 gates:** migration idempotency, transactional outbox replay, compacted
authority recovery, stale-writer rejection, restart recovery, exact Python/Rust
parity, public V1 contract compatibility, full source/Clippy/security tests and
zero production mutation. Conclusion must be either `PASS` or `FAIL`; missing
authority wiring cannot be deferred as operational debt.

**C.0 implementation journal:**

- `2026-08-20 RELEASE/CUTOVER PREPARATION RECORDED`: corrected the malformed
  `RUNTIME UNCHANGED` journal line and added the production cutover boundary
  plus `docs/runbooks/v2-production-rust-authority-cutover.md`. Read-only
  runtime inspection proved V1 `0.1.0` still owns port `8100` with 40 V1
  paths and zero V2 paths, no V2 containers are running, and the current stable
  binaries/config are intentionally shadow-only. The branch is more than 80
  commits ahead of `dev`; it must merge through CI before a new authority
  feature branch is created.
- Documentation whitespace and secret scans passed; stable compose rendered
  successfully with isolated dummy values and no container start. Host
  preflight observed 11 GiB available RAM, 108 GiB free disk and eight CPUs.
  No image build, provider call, service restart, authority mutation, consumer
  route change, topic/Redis write, volume deletion, push or merge occurred.
  C.0 remains `IN_PROGRESS` until the current PR is CI-green and merged to
  `dev`; production authority wiring starts only on the new branch named in
  this phase. Preparation commit: `130da39`.
- `2026-08-20 OPERATOR CUTOVER SIMPLIFICATION RECORDED`: the operator reports
  all alpha consumers are stopped and Trading System is the sole active
  consumer. Phase C therefore removes staged alpha/monitoring migrations and
  uses one bounded Trading System parity-and-route switch followed by a
  preapproved Binance/OKX maintenance window. This reduces operations, not
  correctness: persistent authority CAS/outbox, sink fencing, W/W+1 handoff,
  durable audit and per-slice rollback remain mandatory. V1 stays hot on port
  `8100`; DNSE stays V1-only.

#### C.1 Isolated Stable V2 Deployment

Deploy the immutable pair under project `qdl_v2_stable_candidate`, loopback
ports `18201/18202/18210/18211/18220/18221`, dedicated RF3/minISR2 Kafka,
dedicated Redis prefix/cache/state and unique credentials. Start in
`RUST_SHADOW`; exclude the `stable-vn` profile. V1 port `8100`, current
Redis, provider sockets and consumer routes remain unchanged.

Require real Binance/OKX warmup and stream data, zero unexplained gap/duplicate/
quarantine, bounded lag/resources, broker and process restart recovery, exact
cursor continuation, V1 health unchanged and exact disposable cleanup on
failure.

#### C.2 Single-Consumer Trading System Cutover

The operator confirms all alpha consumers are currently stopped and Trading
System is the only active Data Layer consumer. Do not create artificial alpha or
monitoring migration stages. Built-in V2 health/lag/authority telemetry remains
mandatory, but it is not a separate cutover consumer.

Run one bounded Trading System dual-read parity window for Binance and OKX:
V1 remains the decision source while the same requested instruments, timestamps,
decimals, units, final BAR lifecycle and freshness are compared against V2.
After zero correctness mismatch and healthy cursor/replay evidence, switch the
Trading System market-data adapter to V2 in one controlled restart. Configure a
venue-aware rollback route: Binance/OKX primary V2 with explicit V1 fallback;
DNSE remains V1-only. Never splice providers silently--every route transition
records source, reason, watermark and operator/audit identity.

A stale, gapped, non-authoritative, wrong-session or unit-mismatched V2 read
fails closed. Fallback to V1 is allowed only when V1 passes the same
freshness/session/contract checks and the source-switch audit is durable.

#### C.3 Fast-Track Rust Authority Promotion

Promote all approved Binance and OKX feed slices in one maintenance window, but
execute the CAS internally one slice at a time so a failure is isolated. One
operator packet may list the complete slice set, image IDs, old/new owners,
authority/lease/plan revisions, terminal watermarks, topics/groups, ports,
volumes, secret references, Trading System route and rollback command.

Each slice still follows:

`PYTHON_PRIMARY -> RUST_SHADOW -> RUST_CANARY -> RUST_PRIMARY`.

The canary is bounded by accepted real events and continuity evidence rather
than a long calendar wait. Fence the old writer at `W`, persist its terminal
checkpoint, accept the handoff, execute CAS/outbox, reconstruct every target
through `W`, and publish first as Rust at `W+1`. When one slice passes, the
same preapproved window proceeds to the next. Any ambiguity, missing ACK,
parity mismatch, lag/gap, stale CAS or Trading System failure enters `BLOCKED`
for that slice and restores V1 under a newer revision; unrelated promoted slices
remain governed independently.

#### C.4 Close With V1 Hot Fallback

After all approved Binance/OKX slices are `RUST_PRIMARY`, Trading System reads
V2 as its normal source and V1 stays running on port `8100` as the tested hot
fallback. There is no alpha-by-alpha migration while those alphas remain down
and no V1 sunset is part of this cutover. Publish the V2 release only after the
Trading System cycle, Rust authority audit, cursor/replay continuity and an
exercised V1 fallback/return-to-V2 drill pass. DNSE remains V1-only until its
separate provider gate passes.

**Decision boundary:** C.0 code/release preparation and C.1 isolated deployment
are non-production-authority work. C.2 changes only the Trading System
market-data route. C.3 requires one explicit operator packet for the approved
Binance/OKX slice set. No command implicitly authorizes deleting volumes,
stopping V1 or promoting DNSE.

### Rollback

Before runtime cutover, remove only isolated Rust/Kafka/V2 test resources.
After an approved cutover, fence the selected Rust slice, restore the matching
Python rollback manifest under a newer authority revision/lease, replay from the
last common durable watermark and leave all unrelated venue/feed slices
untouched.
