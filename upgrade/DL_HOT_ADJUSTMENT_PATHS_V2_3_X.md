# DL hot-adjustment paths, v2.3.x (what changes, what restarts, which TS roles follow)

Written 2026-11-16 (Claude, DL lane) for the v2.1.3 Phase 0 plan (owner: "the DL side needs a complete hot-adjustment
path", Paper blockers first). Status: **documentation of the existing v2.3.0 mechanism, read from code; the
operations are not newly exercised here** except where a receipt is cited. Line numbers are at `7d4333e`
(tree = main `f2ea1d7`) for DL and TS `34fc6192` (v2.1.2) for TS, and will drift.

## 0. The rules every row inherits

- **One tool.** `scripts/dl_declaration.py render DECLARATION --out NEW_DIR` then `gate NEW_DIR` (offline, no runtime,
  never overwrites; docstring lines 1-50). The render writes the coupled set (demand, catalogue, acquisition, scope,
  routing, manifests, Query runtime, ingestors, cores, kn/realm bundles, sealed TS binding, `execution-scope/`,
  `declaration-index.json`, `report.json`). The window mounts NEW dirs; deployed files are never edited in place.
- **Additive by construction.** Retained rows are kept byte for byte. The role-config refreshers refuse removals
  (`scripts/refresh_v2_core_runtime_bindings.py:85`, `scripts/refresh_v2_native_ingestor_runtime.py:211`) and the gate
  refuses removed/changed rows (`dl_declaration.py:1951-1991`). The only non-additive path that exists is the bar-edge
  checkpoint migration record (`qdl/runtime/bar_edge_checkpoint.py:107` `validate_migration_record`, `:160`
  `plan_crossing`).
- **Accept-set (JWT coupling).** A manifest successor that is additive keeps the previous floor
  (`qdl/consumer/manifest_compat.py:69-75` `next_min_token_revision`; additivity = no identity change, no removed
  purpose/permission/row, no lowered quota, `:41-62`). Query/stream accept any token revision in
  `[min_token_revision, revision]` (`qdl/security/data_plane.py:391`; Rust `auth.rs` / `authority.rs`, slice 2a).
  So an additive manifest bump needs **no** same-window JWT bump on the DL side.
- **TS sealed-binding coupling (the TS side, stricter than DL).** A TS role with `DATA_LAYER_V2_REQUIRE_SEALED_BINDING=true`
  refuses to start unless its binding's `consumer_manifest_revision` equals its `DATA_LAYER_V2_JWT_MANIFEST_REVISION`
  and its `universal_manifest_sha256` equals `DATA_LAYER_V2_RELEASE_MANIFEST_SHA256`
  (TS `alpha_sdk/data_layer_release_binding.py:219-244`, called by `adapters/market_data/data_layer_v2.py:3375-3384`
  and `:3415-3424`). The binding is read once at process start; nothing reloads it (the bridge `_reload` event is set
  only by `stop()`, `services/market_data/data_layer_bridge.py:1387`). Route selection for every V2 read comes from
  that binding (`data_layer_v2.py:3071-3087`). Therefore: **a TS role picks up new routes only by a recreate with a new
  binding + the matching JWT revision + release sha**, and a role that does not need new routes keeps its old
  binding file and JWT (the accept-set keeps it valid).
- **Which TS roles read DL routes.** The writer `market_data_service_p184w` subscribes per routed product. `ts-gateway`,
  `ts-risk`, `ts-paper` load the binding at start (`build_versioned_data_layer_client`) but price orders from the
  writer's market cache (`services/paper_execution/native_execution.py:335`, `services/risk_engine/main.py:463,1116`
  `build_market_cache_reader`). They need no change for added execution feeds as long as their binding file stays in
  place (`v2.2.6-stage-a-ts/trading-system.paper.stable-r25.json`; new bindings go to new paths).
- **Quota.** EXECUTION quotas are derived, never hand-picked (`qdl/consumer/quota_model.py`;
  `dl_declaration.py:852-877`; offline gate `scripts/check_consumer_quota_model.py`). SIGNAL (alpha) quotas stay
  declared, `MODEL_PENDING`.
- **CPU.** Owner cap 6.0 vCPU for the 17 DL roles, OKX before Binance (`capacity.venue_priority`, report
  `deferral_plan`). The render's capacity estimate is ingest-only (1.8 ms/record upper bound); Query cost per request
  is not modelled (`report.capacity.query_requests.basis`), so a window carries a measured CPU gate.
- **Order of restarts** (window A, 2026-11-13, GREEN): cores (all three at once) → projectors (one at a time) →
  stream → Query (one at a time) → writer stop → ingestor(s) → bar_edge (only when BAR rows change) → writer
  BOOTSTRAP → STRICT. Kafka, stable_redis and market_cache are never recreated by a declaration.

## 1. Matrix

| Operation | Tool / command | Additive? | DL roles restarted | TS roles | Quota / CPU checks | Gates | Rollback |
|---|---|---|---|---|---|---|---|
| **Add execution feeds** (QUOTE / MARK_INDEX_PRICE / BOOK / TRADE) for TS | declaration `consumers[].class: EXECUTION`, `instruments[]` (+ `instrument_captures` for an instrument the catalogue lacks); render + gate | yes; manifest floor kept (stage B: r14 `min_token_revision 13`) | cores x3, projectors x2 (kn bundle), stream x2 + Query x2 (realm bundle + manifest), the venue ingestor(s) whose file changed. Not bar_edge (no BAR change). | writer: recreate (same image) with new binding + `JWT_MANIFEST_REVISION` = new revision + `RELEASE_MANIFEST_SHA256` + scope env (`MARKET_DATA_<VENUE>_SYMBOLS` or `MARKET_DATA_DECLARED_SCOPE_FILE`); gateway/risk/paper: none | quota gate (model vs granted, `consumer_quota_model` in gate); measured W-B2 rate x products; ingest CPU estimate + measured Query slope; subscriptions per lane <= `max_subscriptions_per_connection` | gate PASS; writer W1/W2 VERIFIED; 0 RATE_LIMITED; 0 auth refusals; MARK/QUOTE freshness; TS coverage gate; DL CPU <= 6.0 over 15 min | coupled: writer back to old binding/JWT (BOOTSTRAP→STRICT, never `docker start`), then ingestor, readers, projectors, cores on the old dirs. Evidence: stage A (r13/r25, 2026-11-13, two rollbacks exercised), stage B packet |
| **Add signal BAR intervals (derived)** | declaration `universes[]` with `intervals` / `grant_intervals`; `DERIVED_FINAL` from the largest materialised finer parent | yes; alpha manifest floor kept (r15→r16 floor 15) | Query x2, stream x2, projectors x2, cores x3 (bar_edge-owned rows), **bar_edge** (its runtime pair; checkpoint crossing `plan_crossing`, migration record only if a legacy checkpoint is crossed) | none for TS execution; the alpha host follows its own manifest | `okx_rest_budget` (bar-edge CANDLES lane 150/min), bootstrap estimate (HISTORY_CANDLES 150/min), +~1.7 rec/s live | gate incl. `bar_edge_core_raw_lineage`, `derived_parents`; window: crossing `dropped=0`, D6b derived gaps healed, parity auditor 0 | forward declaration; projector rollback constraint 4'/6' (rc3 projector stays while derived records exist). Evidence: window A out6 (318 DERIVED_FINAL), D6b +6 h PASS |
| **Add funding** (v2.3.1) | declaration `universes[].live_funding.cadence_ms`; members without cadence typed BLOCKED `FUNDING_CADENCE_UNDECLARED` (branch `feat/v2.3.1-okx-funding-live`, plan ~77935) | yes | needs the **v2.3.1 images** (Rust core funding + ingestor lane) first, then the same restart set as an execution/signal feed addition | none unless a TS role consumes funding | capacity 0.03 rec/s/binding | v2.3.1 certificate + gate | image rollback = v2.3.0 digests; declaration rollback is forward. Status: implemented / tested locally / isolated shadow only, not released |
| **Change latest-state flush per feed** | declaration `ingestor.latest_state_flush_ms_by_feed: {QUOTE: 1..1000, MARK_INDEX: 1..1000}` (`dl_declaration.py:1160-1213`; Rust `qdl-native-raw-ingestor.rs:273-279,524-528,600-617`) | config change, no row change; **per ingestor file and per feed, not per instrument** | only the ingestor(s) whose file is in the declaration base (leave a venue's file out to leave it untouched) | none (writer sees a different MARK cadence) | ingest records/s per flush from a coalesce probe (`c150/okx_ws_coalesce_probe.py`) | MARK/QUOTE write-age p99 vs the agreed bound | re-render with the old map (or the old file) and recreate the ingestor. Not exercised in production yet (first use: stage B) |
| **Remove feeds** (Binance BOOK/TRADE case) | **no supported path in v2.3.x.** The refreshers and the gate refuse removals (refs above); the migration record covers bar-edge checkpoints only | no | would be: every reader of the removed grants, cores, ingestor, projectors | every consumer whose manifest loses rows gets a floor reset (`next_min_token_revision` returns the new revision when not additive) → **same-window JWT bump** for that consumer; TS writer also new binding | CPU saving measured first (C3) | needs a new tool slice: realtime retirement with an explicit retirement record, gate checks, and a census of who reads | forward declaration restoring the rows (+ JWT/binding back) |
| **Change consumer quotas** | EXECUTION: re-render (quotas follow the model; declared quotas below the model are refused, `dl_declaration.py:870-875`). SIGNAL: `consumers[].quotas` (raise only, `:813-820`) | raise = additive (floor kept); lower = not additive (floor reset) | Query x2 + stream x2 (realm bundle + manifest); not cores/ingestors/projectors unless the kn bundle `max_warmup_rows` changes (projector caps follow it) | raise: none (JWT/binding unchanged, the accept-set keeps them valid); lower: JWT bump for that consumer | `check_consumer_quota_model.py`; refusals burn quota (`HEADROOM` 1.5) | 0 RATE_LIMITED, quota counter samples (`dlcov/quota_rate.py`) | old realm dir, readers recreated |
| **Add a new consumer** | new `consumers/stable/<id>.yaml` + realm bundle entry (`scripts/kn_gateway_bundle.py`), JWT subject/key in the realm `jwt-config.json`; declaration tool handles grants of existing consumers, not identity creation | yes for others | Query x2 + stream x2 (realm bundle); projectors if the kn bundle changes | the new consumer's own roles (key + binding) | its own quota model / declared quota; CPU by its demand | auth from the new identity; 0 refusals for others | old realm dir. Identity/key material is an owner step (secrets), not rendered |
| **Change the JWT revision** | TS side: `DATA_LAYER_V2_JWT_MANIFEST_REVISION` + `RELEASE_MANIFEST_SHA256` + a binding sealed at that revision (`alpha_sdk/data_layer_release_binding.py:219-244`) | n/a | none, as long as the new revision is inside the deployed manifest's accept-set | each TS role that changes it must be recreated with the matching binding (start-time load) | none | role starts (binding loads), 0 auth refusals | recreate with the previous triple; the previous binding file stays in place |

## 2. Stage B applied to the matrix (2026-11-16 packet)

Minimal safe TS set: **writer only** (JWT 14, release sha of r14, binding r26, `MARKET_DATA_OKX_SYMBOLS` + 71), same image
`9dfef8a8`. ts-gateway / ts-risk / ts-paper keep JWT 13 + r25 (r14 accepts 13..14; they price from the market cache).
DL: cores x3, projectors x2, stream x2, Query x2, ingestor_okx_swap. Not: Binance ingestor, bar_edge, Kafka, Redis.
See `upgrade/evidence/releases/v2.3.0/drafts/STAGE_B_EXECUTION_71_PACKET.md`.

## 3. Gaps found while writing this (backlog, not fixed here)

1. Realtime feed removal has no tool path (Binance C3 needs a slice).
2. The core generator hard-codes `batch_wait_ms: 25` (`qdl/runtime/stable_deployment.py:1145`) and the core refresher
   refuses a changed non-binding field (`refresh_v2_core_runtime_bindings.py:73-75`): after W-A1 (live 50) a declaration
   render on the live cores is refused. Stage B works around it by rendering on the bw25 base and setting the one key
   (verified identical otherwise); the class fix is to make `batch_wait_ms` a declared/core-config input (v2.3.1).
3. The TS binding is start-time only; a TS route hot-reload (new binding without a recreate) does not exist.
4. Query CPU per request is not in the capacity model.

## Delta 2026-11-16: stage B and W-A1 applied; gaps 2 and 4 confirmed

Plan anchor: DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md, "DL runtime record: W-A1 permanent and stage B r7 APPLIED".
- The first production use of "Add execution feeds" and "Change latest-state flush per feed" is done: stage B r7
  APPLIED 12:24-12:38Z with the row's restart set (cores x3, projectors x2, stream x2, Query x2, OKX ingestor, writer
  only on the TS side). Rollback was not needed. Evidence `upgrade/evidence/releases/v2.3.0/runtime-record-20261116/`.
- Gap 2 (hard-coded `batch_wait_ms`) is confirmed in production: the running cores are the bw50 successors
  (byte-equal to `declaration-stage-b-r7/cores-bw50/`), not the generator output. v2.3.1 B1.
- Gap 4 (Query cost per request) has a measured term: the TS identity's requests are 57-61 % native QUOTE probes
  (2,958-3,319/min), not MARK reads, so a MARK-only lever cannot meet a request gate. v2.3.1 B4 proposes a batched
  probe read.
- The flush map moved the retained OKX MARK lanes too (per ingestor file, as the matrix says). v2.3.1 B3.

## Delta 2026-11-17: v2.3.1 closes gap 2, corrects its cause, and adds one-env-per-release

Plan anchor: DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md, "DL v2.3.1 release candidate prepared offline".

**Gap 2, correction.**
- The core refresher (`refresh_v2_core_runtime_bindings.successor`, also used by `dl_declaration.py build_cores`)
  does not refuse a bw50 base. It compares only the `core` sub-document and carries the active top-level
  `batch_wait_ms` over.
- Verified offline on `declaration-stage-b-r7/cores-bw50`: OK, 0 additions, byte-equal.
- The hard-coded value only affected full generator output. v2.3.1 declares it, `STABLE_CORE_BATCH_WAIT_MS = 50`,
  together with the previously omitted `book_resync_quarantine: true`.

**Env.** `deploy/compose/qdl-v2-stable-kn.v2.3.1.env` is the single env for the release. An overlay env file
(stage-b.env) is no longer part of the rollback recipe.

**Gap 4 (Query cost).** It has two measured terms:
- replica affinity of a single httpx pool: the fix is the SDK 2.6.0 per-replica client;
- an offline per-item cost of about 0.59 ms CPU per execution MARK item.

## Delta 2026-11-17 (later): Binance signal universes are declarable (stage C)

Plan anchor: DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md, "DL v2.3.1 stage C declaration r8".

"Add signal bars" now covers Binance USDM. You can declare a SIGNAL universe with venue BINANCE / market USDM and
native 1m plus derived intervals.

Constraints:
- The 1m base is always the certified native kline lane. `bar_1m_source` is not declarable for Binance.
- New Binance instruments enter through `instrument_captures` (venue BINANCE, exact `symbols`), appended verbatim.
- A signal consumer may take a second venue. Its own OKX BAR row is the demand template.

Restart set: `ingestor_binance_usdm`, cores, projectors, streams, Query and bar_edge. `ingestor_okx_swap` is
restarted only when OKX bindings change.
