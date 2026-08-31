# Phase 11.1 Active-Demand Inventory Evidence

Date: 2026-08-26

## Scope and safety boundary

This evidence covers a source-only, dark control-plane compiler. It reads
declared Data Layer, Trading System, and alpha configuration, then produces a
versioned demand inventory, bounded admission plan, lease/topology plan, and
per-slice readiness. It does not start or recreate any service, connect a
consumer, subscribe a websocket, change a route/authority, write Redis/Kafka/
SQLite/PostgreSQL, modify alpha configuration, or submit an order.

The source registry is
`config/v2/active-demand-source-registry.yaml`. It makes every new
venue/market/feed budget explicit. `DataRequirement`, `DemandLeaseRegistry`,
`CapabilityRegistry`, and `DemandTopologyPlanner` remain the shared domain
primitives; no alpha or symbol receives its own container, image, or planner.

## Deterministic evidence

- Isolated, network-disabled compilation of the mounted declarations passed:
  83 requirements from 50 source documents, 10 explicit out-of-scope
  exclusions, manifest digest
  `c331a1dac94d84a69c88f65667e43fea34f765168a15de12cac9a895d87452a2`.
- Input digest:
  `e9bd5c29e0a8b561cc67ab7383c58c82f64ddca7f53cc93a6c6d0954284265b9`.
- The dedicated Phase 11.1 tests passed 10/10. They cover source ambiguity,
  explicit and continuous selector resolution, truthful missing capability,
  lease renewal/expiry, priority/budget exhaustion, dark readiness, no raw
  metadata persistence, and 1,025-symbol topology planning. The latter is
  above the 658 admitted physical slices and still produces one logical
  venue/market role rather than per-symbol infrastructure.
- The isolated V1/V2 regression command passed 81 tests; three Redis tests
  were skipped because no disposable `QDL_PHASE2_REDIS_URL` was provisioned.
  V1 OpenAPI/SDK/Redis golden tests passed. The test harness used only tmpfs
  logs and network was disabled.

## Bounded real-provider admission

One read-only public metadata request was made for each demanded market:
Binance Spot, Binance USD-M, OKX Spot, and OKX Swap. Raw provider bodies were
not persisted. Their response digests and every canonical row are recorded in
`phase111-active-demand-provider-admission.json`.

- 1,633 admission rows: 1,564 `ADMITTED`, 69 `MISSING_INSTRUMENT`.
- 658 unique admitted physical slices become 656 realtime subscriptions plus
  two reference/data-provisioning slices. The dark plan has four logical
  roles (`BINANCE/SPOT`, `BINANCE/USDM`, `OKX/SPOT`, `OKX/SWAP`) and 15 bounded
  connection shards at 200 subscriptions per connection. It creates zero
  runtime roles, images, or connections at this phase.
- Every selected budget is below its configured limit. All admitted entries
  are `WARMING` with `DARK_PLAN_NOT_APPLIED`; all have
  `execution_eligible=false`. No runtime status is fabricated as `LIVE`.

## Explicit runtime NO-GO

The 69 failed rows are not a Data Layer parser error and were not silently
replaced by spot, BTC, ETH, or stale cache. Binance USD-M metadata did not
contain these declared symbols:

`ACXUSDT`, `ATAUSDT`, `COSUSDT`, `DUSDT`, `HFTUSDT`, `HIGHUSDT`, `ICXUSDT`,
`IPUSDT`, `MBOXUSDT`, `MLNUSDT`, `NFPUSDT`, `PHBUSDT`, `SCRTUSDT`,
`STORJUSDT`, `SYSUSDT`, `TONUSDT`, `VANRYUSDT`, `VICUSDT`.

Owners are `deep_momentum_prod_yearly_monthly_1d`,
`regressionportfolioA001_1d`, and `rsiboundportfolioA001_1d`. Exact
owner/symbol/feed/interval entries are in the admission and convergence JSON.
The appropriate next decision is for the universe/alpha owner to publish a
new approved universe revision or explicitly disable those demands. Phase
11.1 must not change that strategy configuration itself.

## Result and next permitted step

Phase 11.1 source/control-plane work is **implemented and tested locally**;
it is deliberately **not runtime-applied, production-ready, or
production-authoritative**. The dark manifest proves membership, capability,
budget, lease, and topology behavior. It does not certify provider sessions,
final bars, websocket reconnect, resource capacity, or consumer routing.

After the three owner-visible missing-instrument declarations are resolved,
the next permitted change is the separately approved Phase 11.2 dynamic
realtime runtime handoff. Until then V1/V2 routes and all live consumers stay
unchanged.
