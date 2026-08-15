# Phase 7.0 Contract And Security Hardening Report

## Decision

Phase 7.0 is `PASS` on branch `feat/fund-grade-data-layer-v2`. Phase 7 remains
`IN_PROGRESS`: this report does not authorize Phase 7.1 deployment, consumer
canary, execution dependency, source-authority promotion or V1 retirement.

No running V1 service, provider socket, production Redis key, PostgreSQL row,
Parquet object, cursor, consumer group, volume or credential was changed.

## Implemented

- Added a dedicated V2 data-plane identity boundary shared by REST and gRPC:
  signed workload JWT, pinned algorithm/key ID, issuer/audience/environment,
  expiry/lifetime, token-to-manifest subject and immutable revision binding.
- Registered server-side permissions, purposes, feed/instrument/source policy,
  grade, execution dependency and bounded request/warmup/batch/stream quotas.
  Token roles and manifest permissions are intersected; neither can escalate the
  other.
- Protected all ten current V2 REST operations and added a gRPC interceptor plus
  SDK call credentials with equivalent decisions and typed failure statuses.
- Replaced provider-generic public responses with closed, feed-discriminated
  models for TRADE, QUOTE, BAR, BOOK_SNAPSHOT, BOOK_DELTA, FUNDING_RATE,
  OPEN_INTEREST, MARK_INDEX_PRICE and TICKER.
- Added generated enums with `UNSPECIFIED = 0` rejection, exact decimal
  coefficient/scale, contract lineage and explicit bar lifecycle/revision.
- Made only `IN_PROGRESS` bars coalescible. `FINAL`, `REVISED` and `CANCELLED`
  bar events are lossless and retain revision semantics across Python/Rust.
- Made execution eligibility a server-side derivation from entitlement,
  authority, live/completeness/gap/freshness state; backend payload values cannot
  grant eligibility.
- Made SDK snapshot/warmup responses typed and fail closed when the server does
  not provide immutable snapshot/cursor state.
- Added additive manifest-access persistence and idempotent migration coverage.

## Verification

| Gate | Result |
|---|---|
| Full Python regression | `PASS`, 285 tests, 5 conditional skips |
| Focused REST/gRPC security | `PASS`, 11/11 |
| Rust fmt/clippy/tests | `PASS`, warnings denied, 11/11 |
| Buf format/lint | `PASS` |
| Buf breaking vs Phase 1 and Phase 7 baselines | `PASS` |
| OpenAPI semantic diff | `PASS_PRE_BETA_FREEZE`, no removed operation/response/schema/enum |
| V1 OpenAPI/SDK/Redis golden compatibility | `PASS`, no V1 contract file changed |
| PostgreSQL clean/existing/idempotent migration | `PASS`, legacy retained, 21 tables |

The five Python skips are conditional integration variants; Docker application,
gRPC, Buf and migration gates covering those dependency boundaries passed
separately. No generated or simulated market event was admitted as provider
evidence; Phase 7.0 did not require a provider write or production-data smoke.

## Frozen Artifacts

- `contracts/v2/openapi.snapshot.json`:
  `bea44d3920db52f5893eb773aa195ae7f4abd2684d5ca65d904e995934fabcea`
- `contracts/baseline/qdl-v2-phase7-beta.binpb`:
  `16686bb63dd2633f5572dca98baef1d7e1c3d9aec249a259a9725a58ad1445ef`
- `contracts/golden/phase2/binance-usdm-bar.bin`:
  `247b9ffe1ba730b7861dca4bf241872be0a2604e0169752e30dac3b467814b8c`

Machine-readable details are in `phase7-contract-freeze.json`,
`phase7-openapi-diff.json`, `phase7-buf-breaking.json` and
`phase7-auth-matrix.json`.

## Remaining Decision Gate

Phase 7.1 must implement measured dependency readiness, isolated non-root beta
roles, selected fenced gateway ownership, asynchronous replay handoff, shared
quota state, dedicated credentials/namespaces/state and exact topology rollback.
The current in-process minute quota is intentionally bounded to 7.0 tests and is
not a multi-replica production quota backend. V1 remains authoritative.
