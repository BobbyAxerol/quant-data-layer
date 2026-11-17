# DL stage B packet: execution QUOTE + MARK_INDEX_PRICE for the 71 unusable crossec OKX swaps (prepared offline, not applied)

Prepared 2026-11-16 by Claude (DL lane) for the coordinator. Status: **implemented (render) / tested locally (offline
gate, quota gate, unit tests)**. Not applied, not shadow-certified. No runtime action was taken: the only production
contact was read-only (`docker inspect`, `docker exec ... cat` of mounted documents, `docker logs`, cgroup `cpu.stat`,
Redis GET of the quota counter / heartbeat / PTTL). Run sheet: `STAGE_B_EXECUTION_71_RUNSHEET.md`.

Spec: `~/.local/state/trading-system/v213-goals/V213_SCOPE_DRAFT.md` §7, §8.6, §9.

## 1. Scope

- Consumer `trading-system.paper.stable` only, class EXECUTION, OKX SWAP, feeds QUOTE + MARK_INDEX_PRICE, for exactly
  the 71 instruments of `crossec-usable-instruments-20261116T1001Z.json` `unusable_no_quote_mark`
  (`declaration-stage-b-r7/inputs/symbols-stageB71.json`, sha256 `93525c14...`; equal to `dlcov/symbols-stageB71.json`).
- FET-USDT-SWAP and LUNA-USDT-SWAP were not in the catalogue. They are added from the verbatim public OKX captures of
  2026-11-13 04:26Z (`765827cf...` / `0614fec7...`, the PACKET Q3 decision "add them in stage B"). FET `listTime`
  2026-09-05 (re-listing); the alpha-side identity question (COVERAGE_150_STUDY §2) is unchanged and not answered here.
  The captures are 3 days old; a fresh capture before the window is optional (a read of a public endpoint, no runtime
  effect) and would change only the provenance entry if the rows are equal.
- No BOOK, TRADE or BAR (crossec sends MARKET only).

## 2. Tool path and why

`scripts/dl_declaration.py render` + `gate` (v2.3.0, origin/dev `7d4333e`, tree = main `f2ea1d7`). It is the supported
path for TS execution rows since v2.3.0: it calls the stage A renderer (`prepare_paper_read_extension` templates and
`_seal`) for the TS manifest and sealed binding, derives EXECUTION quotas from `qdl/consumer/quota_model.py`, and
renders the coupled catalogue / acquisition / scope / routing / Query runtime / ingestor / core / bundle set in one
revision. Running the stage A renderer alone would leave the other coupled documents unrendered.

Base = the live declaration (out6, declaration_revision 5), verified byte-equal to the mounted documents on
2026-11-16 ~10:58Z (section 3). The repo form is the committed evidence `declaration-out6/repo/` (the repo's own
`config/v2` stays at the pre-declaration test baseline, RELEASE_NOTES "Release tree = runtime"). Declaration revision
7 (r6 was a dry render on 2026-11-13, `outB6`, 69 symbols; not reused).

Declaration choices:
- `accept_set: true` → r14 carries `min_token_revision: 13`. JWT-13 holders stay valid, so only the writer moves to
  JWT 14; ts-gateway / ts-risk / ts-paper (JWT 13, binding r25 mounted) need no recreate.
- `ingestor.latest_state_flush_ms_by_feed: {QUOTE: 50, MARK_INDEX: 500}`. **Supported per feed, but per ingestor
  config, not per instrument** (`IngestorConfig.latest_state_flush_ms_by_feed`, rust
  `qdl-native-raw-ingestor.rs:273-279, 524-528`; the tool applies one map per file, `dl_declaration.py:1160-1213`).
  So MARK_INDEX 500 ms also applies to the 26 retained OKX MARK instruments. QUOTE 50 = today's default for all.
- The Binance ingestor is left out of the base on purpose: no Binance binding changes, and including it would write
  the flush map into the Binance file and force an `ingestor_binance_usdm` recreate.
- `capacity.rates_per_binding` = measured on exactly these 71 (section 5).

## 3. Render result

Render `outB7` (scratchpad `stageb/outB7`; committed copy without bundles:
`upgrade/evidence/releases/v2.3.0/declaration-stage-b-r7/`, `SHA256SUMS`). Command (capped `--rm`, `--network none`,
`--cpus 1 --cpu-shares 256`, image `qdl-v2-python:2.3.0-rc3-7cd4fc36` = `a235c0a1`, run 11:35Z after the W-A1 lock):
`python -B scripts/dl_declaration.py render /s/inputs/declaration-stage-b-r7.yaml --out /s/outB7` rc 0, status
`RENDERED_NOT_APPLIED`, 0 BLOCKED rows; `gate /s/outB7` rc 0, **PASS 17/17** (base unchanged, index, loaders, manifests
additive, consumer_quota_model, accept_set_floor_rule, release routing, config strictly additive, role configs additive +
lanes stable, bar_edge crossing retained 1047 / added 0 / dropped 0 / no migration record, retained lineage, live config
lineage 236 bindings, bar_edge raw lineage, derived chain 318, coverage chain, okx_rest_budget, capacity envelope).

Base verified equal to live (read-only, 2026-11-16 ~10:58Z): OKX ingestor `4cb59e29`, Binance `021dd5dc`, cores
`ff105182`/`348f2c70`/`dfab3186` (= out6 = bw25), kn bundle `924951cd`, realm bundle `c5e21bb7`, Query acquisition
`81d001aa`, catalogue `6f1699d4`, TS r13 `cfc20e29`, binding r25 `980123da`, authority `db00040f` (= the object embedded in
every core). After W-A1 (11:16Z) the live cores are `v2.3.0-rc3-a1-bw50-cores` (`4a676578`/`e9c457cb`/`38e130e2`).

| Document | Before → after | sha256 (prefix) |
|---|---|---|
| catalogue | r14 → r15, +2 instruments (FET, LUNA), +142 bindings (71 QUOTE, 71 MARK_INDEX_PRICE) | `87945896` |
| acquisition (repo + Query runtime) | r22 → r23, +142 RUST_NATIVE (71 `okx_bbo`, 71 `okx_mark_index`) | repo `da7240cd`, runtime `9256bd5a` |
| demand / scope / routing | r10 → r11 (+142) / r13 → r14 (+142) / r31 → r32 | `be1646f0` / `3d6613b0` / `8290128b` |
| TS manifest `trading-system.paper.stable` | **r13 → r14**, 105 → 247 requirements, `min_token_revision: 13`, quotas 10,000 rpm / 160 streams → **31,000 / 370** (model) | `fc51bbf2` |
| sealed binding | **r25 → r26**, 104 → 246 products, `binding_sha256` `ed81043f8fc758d3...` | file `7f5394b9743b9170...` |
| execution scope (`qdl.declared-execution-scope.v1`) | 102 instruments | `526c7bf2` |
| OKX ingestor | 252 → 465 subscriptions, `config_revision` 22 → 23, flush map `{MARK_INDEX: 500, QUOTE: 50}`, new lane `okx-public-005` | `14b28e29` |
| cores x3 | +213 bindings each (71 `okx_bbo`, 71 `okx_mark_price`, 71 `okx_index_price`) | bw25 render `80651283`/`dc58fee8`/`399c03c8`; **bw50 (use these)** `8fb1dc96`/`af533769`/`383c34cd` |
| kn / realm bundle | bundle hash `7bb5baee` / `67d8e86e` (file `12934c8e` / `54026d98`; not committed, scratchpad) | |
| Query runtime catalogue / bar-edge pair / index | runtime `87945896`; bar-edge `86d928f8` / `2b31669c` (bar_edge not recreated); index `9fbeaa85` | |

**Coupled TS consumer env** (report `consumer_environment`): `DATA_LAYER_V2_JWT_MANIFEST_REVISION=14`,
`DATA_LAYER_V2_RELEASE_MANIFEST_SHA256=0b51a0f5d1f90418e1909f5f710ddda2f581def4dbd831718fb3411a0f1887fc`, binding r26.

**Cores after W-A1.** The core generator hard-codes `batch_wait_ms: 25` (`qdl/runtime/stable_deployment.py:1145`) and the
core refresher refuses a changed non-binding field (`scripts/refresh_v2_core_runtime_bindings.py:73-75`), so the tool
cannot render on the bw50 cores. The render therefore uses the bw25 (out6) cores, and `cores-bw50/` is that output
with the one key changed (exactly one occurrence per file). Verified (`offline/cores-bw50-derivation.txt`): live bw50
== out6 except `batch_wait_ms`; the bw50 successor's first 1,166 bindings == live; its tail == the tool's 213 added
bindings; every non-binding field == live. Class fix: v2.3.1 (hot-adjustment doc, gap 2).

**Retained rows** (independent check `tools/retained_check.py`, `offline/retained-check.json`, PASS): every base row
present and JSON-equal in the render for catalogue instruments (523 → 525) and bindings (1163 → 1305), repo and
runtime acquisition, scope, demand (1291 → 1433), TS manifest requirements (105 → 247), binding products and request
policies (104 → 246), OKX ingestor subscriptions incl. `lane_shard` (252 → 465), core bindings (1166 → 1379). Non-binding
changes: OKX ingestor `config_revision`, `latest_state_flush_ms_by_feed`; cores none.

**TS-side acceptance (offline, real TS loaders).** See section 3a.

**Unit tests** (capped `--rm`, `--network none`, `--cpus 1 --cpu-shares 256`, image `a235c0a1`, tree `7d4333e`):
`python -B -m unittest tests.test_v230_declaration_tool tests.test_consumer_quota_model tests.test_v230_ingestor_lanes
tests.test_refresh_v2_core_runtime_bindings tests.test_refresh_v2_native_ingestor_runtime tests.test_paper_read_extension`
→ **Ran 67, OK (skipped 8)** (the 8 need `QDL_V230_STAGE_A_RUNTIME_DIR`; `offline/unit-tests.log`). No code changed in
this slice, so these confirm the tool at this tree, not new code.

## 3a. TS-side acceptance (offline)

Both checks ran in the g34 TS image `33818ebd` (label revision `34fc6192`, the image of the 10:01Z gate), `--rm
--network none --cpus 1 --cpu-shares 256`, against the actual render. Status: **tested locally** (no Redis, no DL).

- `tools/ts_accept_check.py` → `offline/ts-accept.json`, **PASS**, with the real TS loaders:
  - `DataLayerConsumerRouteBinding.load(r26, sha 0b51a0f5..., revision 14, require)`: OK, 246 products;
  - `DataLayerV2RouteManifest.load(r26, same)`: OK (the `ROUTE_MANIFEST_FILE` path);
  - r26 with JWT 13: **refused** "binding consumer manifest revision differs from JWT configuration" (the writer cannot
    keep 13);
  - r25 with JWT 13 + `9ba2264d`: OK (the Paper roles that stay as they are);
  - `parse_declared_scope` (IDR-288 writer parser, `qdl.declared-execution-scope.v1`, the check the 154-instrument dry
    render used, DL plan ~77638) on `execution-scope/trading-system.paper.stable.json` + r26 bytes, token revision 14:
    LOADED, revision 7, manifest_revision 14, 236 routes, `classify` with the binding's own route selection: **0 unbound**;
  - all 71 x {QUOTE, MARK_INDEX_PRICE} select V2 in r26: 142 / 142.
- TS `scripts/market_coverage_gate.py` (grant + binding cells, `--observe-seconds 0`, no market Redis; crossec bindings
  `a0528982...`, the same input as 10:01Z):
  - r13 / r25 (control): FAIL, 71 failing with `NOT_GRANTED` / `NOT_ROUTED` for QUOTE and MARK_INDEX_PRICE, the same 71
    as the 10:01Z receipt;
  - **r14 / r26: STATIC_PASS, 96 instruments, 0 failing** (grant/binding PASS 202 cells = 96 x QUOTE/MARK + 5 x BOOK;
    the remaining BOOK cells are L2/supply-only, not required for MARKET/LIMIT).
  - The cache/fresh/live cells need the running writer, so 96/96 PASS stays a window gate (G5).

## 3b. Minimal TS coupling (read from code)

- **The writer cannot keep JWT 13.** With `DATA_LAYER_V2_REQUIRE_SEALED_BINDING=true` the binding loader refuses unless the
  binding's `consumer_manifest_revision` equals the JWT revision and its `universal_manifest_sha256` equals
  `DATA_LAYER_V2_RELEASE_MANIFEST_SHA256` (TS `alpha_sdk/data_layer_release_binding.py:219-244`, called from
  `adapters/market_data/data_layer_v2.py:3375-3384` (binding) and `:3415-3424` (route manifest)). A binding that routes
  the r14 products is sealed at revision 14, so the writer needs JWT 14 + the r14 sha + r26. The DL accept-set (r14
  accepts 13..14) only spares the roles that do not need the new routes.
- **The writer needs r26.** Every V2 subscription goes through `route_selection` on the sealed binding
  (`data_layer_v2.py:3071-3087`). r25 has no rows for the 71, so they stay `NOT_ROUTED` (10:01Z gate). The declared-scope
  file cannot add routes either: `parse_declared_scope` refuses a scope cut from another binding
  (`services/market_data/declared_scope.py:127-131`, `DECLARED_SCOPE_BINDING_MISMATCH`), and `classify` marks unbound
  every route the binding does not admit (`:156-162`, `main.py` `declared_route_admits`).
- **No hot pick-up.** The binding and route manifest are loaded once in `build_versioned_data_layer_client`. The bridge
  `_reload` event is set only by `stop()` (`services/market_data/data_layer_bridge.py:320,1387`). So the writer must be
  recreated: same image `9dfef8a8`, env + binding mount only.
- **Other Paper roles: nothing.** `ts-gateway`, `ts-risk`, `ts-paper` mount r25 and carry JWT 13 + `9ba2264d`
  (`docker inspect`, 11:0xZ). They load the binding at start, but price orders from the writer's market cache
  (`services/paper_execution/native_execution.py:335`, `services/risk_engine/main.py:463,1116`
  `build_market_cache_reader`). r14 keeps accepting their JWT 13 (`min_token_revision: 13`, `qdl/security/data_plane.py:391`),
  and the r25 file stays in place (r26 goes to a new dir). So they are not recreated. If the sealed binding ever had to
  change for them, all three would need a recreate with JWT 14 + sha + r26 together.
- **Minimal safe set:** writer only, with `DATA_LAYER_V2_JWT_MANIFEST_REVISION=14`, `DATA_LAYER_V2_RELEASE_MANIFEST_SHA256=0b51a0f5...`,
  binding r26 mounted at the same container path, and `MARKET_DATA_OKX_SYMBOLS` = live + 71. Blast radius on the TS
  side: one container, Paper outage from writer stop to STRICT VERIFIED (window A: about 12 min).

## 4. Quota (RATE_LIMITED failure mode)

**Model** (`scripts/check_consumer_quota_model.py`, capped container, PASS; `offline/quota-gate.json`):

| Manifest | QUOTE / MARK / TRADE / BOOK_SNAP / BOOK_DELTA / BAR | steady rpm | opening | required = granted rpm | streams / max_streams |
|---|---|---|---|---|---|
| r13 (live) | 31 / 31 / 10 / 11 / 11 / 10 | 6,012 | | 10,000 | 104 / 160 |
| r14 | 102 / 102 / 10 / 11 / 11 / 10 | 19,502 | 1,004 | **31,000** | 246 / **370** |

**Measured W-B2 demand (read-only; whole `trading-system.paper.stable` identity, both Query replicas, writer + Paper
roles):** shared quota counter (stable_redis GET, `dlcov/quota_rate.py`) 10:59-11:04Z: 2,250 / 1,925 / 1,911 / 1,551 /
1,896 per minute (mean 1,907; `measure/quota-wb2-live.jsonl`) for 104 products, i.e. 32 % of the r13 model. Split from
the writer heartbeat (redis_service GET): native QUOTE probes 74,357 since worker-4 epoch start 06:51Z (about 9.3/min per
QUOTE route, 31 routes); TRADE 72 (about 0.3/min); window B (2026-11-13 21:5xZ) gave QUOTE 238/min the same way
(`p184-window-b-wb2-20261113T1732Z/b-probes-{a,b}.json`). The remainder, about 1,600/min, is MARK reference reads
(about 51/min per MARK slice, model 100) plus 22 BOOK_SNAPSHOT. Earlier point: old writer `7332e85a` on r13 measured
4,407-5,128/min (stage A v3 `quota-per-minute.jsonl`), so W-B2 smart reads removed about 60 %.

**r14 estimate** (inferred from the above, not measured): MARK 102 x 51 = 5,200; QUOTE 31 x 9.3 + 71 x [9.3 .. 90]
= 950 .. 6,700 (the 71 are less liquid; a quiet route probes at bound/3 = 90/min, `quota_model.py` docstring); TRADE +
BOOK about 30 → **about 6,200 (likely) to 12,000 (all 71 QUOTE routes quiet) per minute**, plus 1,004 opening reads in the
first minute after the writer start.

**Headroom vs RATE_LIMITED.** Granted 31,000 = 2.6x the worst estimate and about 5x the likely one. The failure mode of
the 2026-11-13 window (granted 4,500 < demand 6,000, refusals burning quota because the counter INCRements first,
`qdl/security/data_plane.py` `RedisMinuteQuota`) needs demand above the grant; here it would need 2.6x the worst case.
Streams: 246 of 370. The per-consumer read-lane pending bound (`qdl/query/lanes.py:271`) can still refuse a few reads in
the bootstrap burst (stage A: 3); recorded, not gated.

## 5. Capacity

All numbers in vCPU, cgroup `cpu.stat` medians. DL = the 17 `qdl_v2_stable_candidate-*` roles.

| Item | Value | Source / basis |
|---|---|---|
| DL now, after W-A1 (bw50) | **4.958** (brokers 3.167, raw-age p99 131.3 ms) | `~/.local/state/qdl-v2/wa1-perm-20261116T1110Z/RECEIPT.md` (coordinator, 18 min, 11:16Z on); 11-14 run 4.948 agrees |
| Ingest path, 71 new | +330.2 rec/s → +0.594 upper bound | render `capacity` with rates measured on exactly these 71 (`coalesce-50/500.json`, 10:3xZ): bbo-tbt @50 ms 125.5 + mark-price @500 ms 142.0 + index-tickers @500 ms 62.7 rec/s; 1.8 ms/record |
| Retained 25 OKX MARK at 500 ms instead of 50 ms | -84 rec/s → about -0.15 | same probes (mark 124.6→50.0, index 41.5→32.1 rec/s); BTC-USD-SWAP not in the probe |
| Query x2 | +0.5 likely, +1.1 worst | **estimate, not modelled by the tool**: linear fit of two live points, 1,907 rpm → 0.425 (11:01Z today) and about 4,800 rpm → 0.72-0.79 (stage A v3 post-cpu, 2026-11-13): about 0.11 vCPU per 1,000 rpm; × the section 4 range |
| stream_kn x2 | +0.1-0.2 | not measured; 104 → 246 subscriptions, +330 records/s fan-out to one consumer |
| **Projected DL** | **about 5.9-6.1 likely; 6.6-6.9 worst** | sum; the tool's 5.542 is ingest-only |
| Ceiling | 6.0 | owner |

So stage B is at the ceiling, not "about 5.4" as §8.6 estimated: §8.6 left out the Query cost of +142 execution products.
The window's G6 (15-min median <= 6.0) is the real arbiter and may fail; decision D3.

- **Writer** (`market_data_service_p184w`, not in the DL cap): 1.53-1.57 now (10:33Z, 11:01Z). MARK runs in the 4
  Python workers: about 2 vCPU per +543 MARK records/s (COVERAGE_150_STUDY §5.4, estimate) → +0.5-0.75 for 71 at 500 ms,
  less up to 0.3 from the retained 25 at 500 ms; native QUOTE slots +small → **about 2.0-2.3**. Worker p95 gate <= 0.6 per
  worker is recorded, not gated (not checked per worker here).
- **Kafka disk:** `/` 774 G, 401 G used, **374 G free (48 %)** (`df`, 11:0xZ), floor 14-15 % (never < 10 %): no risk.
  Brokers held about 23.3 GB each after stage A (`window-stageA-v3/post-kafka-du-*.txt`); +246 net rec/s on about 740 raw
  rec/s today scales to roughly +7-15 GB across 3 brokers at retention (estimate, from COVERAGE_150_STUDY §5.3).
- **Lanes / subscriptions:** OKX 252 → 465. Retained lanes keep name and feed: public-001 BOOK 10, 002 TRADE 5, 003 QUOTE
  26 → **97**, 004 MARK_INDEX → **100**, business-001 BAR 100, business-002 BAR 59; new **public-005 MARK_INDEX 94**. Every
  lane <= 100 (`max_subscriptions_per_connection` 100), one feed per lane, sticky pins (`lane_plan PINNED`). Binance 27,
  cap 200, untouched.
- **Per-venue raw Kafka rates now** (ingestor progress, two 10 s samples ~11:0xZ, `measure/progress2-*.jsonl`): OKX
  536-557 rec/s, 127-133 kB/s; Binance 185-229 rec/s, 71-88 kB/s.

## 6. Decisions for the owner

- **D1 GO** for the window in `STAGE_B_EXECUTION_71_RUNSHEET.md` with its blast radius (section 5 there) and coupled
  rollback (section 4).
- **D2 MARK flush.** `MARK_INDEX: 500` is per ingestor file, so the 25 retained OKX USDT swaps (+ BTC-USD-SWAP) move to
  500 ms too. Writer MARK write age p99 is 723 ms now (11:01-11:04Z, `measure/fresh-now-p99.json`; W-B2 window B 499-580
  ms) and will rise. Proposed G4: MARK write-age p99 <= 1,000 ms, max < 2,000 ms (Paper `market_max_age_seconds` 2,
  MARK bound 2,000 ms), instead of the W-B2 "+100 ms" rule. Alternative: `MARK_INDEX: 50` (no retained change) costs
  +546 instead of +330 rec/s (+0.39 vCPU more), which does not fit 6.0.
- **D3 CPU.** Projected DL about 5.9-6.1 (worst 6.9). Options: (a) run as is with G6 as an abort gate (recommended; the
  rollback is proven twice); (b) split into two declarations of about 35 each, measuring the Query slope between them
  (two windows, two revisions r14/r15); (c) ask the TS lane to lower the writer MARK reference read cadence
  (`DATA_LAYER_V2_MARK_REFRESH_SECONDS` 1 → 2, about -2,600 rpm at r14), which needs its own MARK-freshness evidence.
- **D4 FET / LUNA** captures are from 2026-11-13 04:26Z. Use them (recommended), or re-capture before the window.

## 7. Binance measurement plan input (read-only; nothing rendered)

Census `tools/binance_census.py` over the 12 mounted realm manifests (`v2.3.0-rc2-realm`), the out6 repo manifests,
the out6 demand and catalogue, the live Binance ingestor and the live Query acquisition (`offline/binance-census.json`).
Instruments: `BINANCE:USDM:{BNB,BTC,DOGE,ETH,SOL}USDT`, and `BINANCE:SPOT:BTCUSDT`, which has QUOTE/TRADE/BAR rows.

| Feed (USDM, 5 symbols) | Demand (drives acquisition) | Granted to (manifests) | Live supply |
|---|---|---|---|
| QUOTE | trading-system.paper.stable | alpha.binance.{live,paper,sandbox}, trading-system.{live,paper,sandbox} | 5 subs; 64-68 rec/s Kafka (coalesced from 700-1,100 frames/s) |
| MARK_INDEX_PRICE | trading-system.paper.stable | the same 6 + qdl.crypto.reference-l2.stable | 5 subs; 5 rec/s |
| TRADE | alpha.binance.paper.stable, trading-system.paper.stable | the same 6 + monitoring.multivenue.stable (BTC, ETH) | 5 subs; 55-91 rec/s |
| BOOK_SNAPSHOT / BOOK_DELTA | trading-system.paper.stable | the same 6 + qdl.crypto.reference-l2.stable (BTC, ETH) | 7 subs; 53-55 rec/s, **50-61 kB/s (the largest byte share)** |
| BAR | 1m: alpha.binance.paper.stable, trading-system.paper.stable; other intervals granted to alpha.binance.* only | 5 native 1m + 65 REST (bar_edge) | 8 rec/s |

- Dropping Binance BOOK + TRADE would remove about 108-146 raw rec/s (of about 740 raw in total) and 57-73 kB/s, at most
  about 0.2-0.26 vCPU at 1.8 ms/record (estimate). QUOTE parsing (700-1,100 frames/s) stays in the ingestor if QUOTE
  stays.
- **Non-additive: yes.** Removing the rows from any manifest above resets that consumer's accept-set floor
  (`qdl/consumer/manifest_compat.py:69-75`), so every affected consumer (alpha.binance.* x3, trading-system.* x3,
  reference-l2, monitoring) needs a same-window JWT bump, and the TS writer a new binding. Removing only the supply
  (demand/acquisition/ingestor) while keeping the grants leaves granted-but-unserved products.
- **No tool path in v2.3.x:** `refresh_v2_native_ingestor_runtime.py:211` and `refresh_v2_core_runtime_bindings.py:85`
  refuse removals, and the gate refuses removed rows (`dl_declaration.py:1951-1991`). The migration record exists only
  for bar-edge checkpoints. C3 needs a realtime-retirement slice first, plus a census of real readers (which alpha or
  Paper account trades Binance today: not checked).
- Not checked: Kafka bytes per topic on disk per venue (needs `du` inside the brokers; not run, to avoid broker exec).

## 8. Open risks

1. CPU at the 6.0 ceiling (section 5); Query cost is an estimate from two points.
2. MARK freshness for the retained 26 OKX MARK instruments at 500 ms (D2).
3. Writer convergence for 246 slices: `--min-execution-ready PRE+130` may need the convergence extension; the 71 less
   liquid QUOTE routes probe more (quota, section 4).
4. The TS coverage gate 96/96 was run offline with grant/binding only (section 3a); cache/live cells need the window.
5. FET history is short (re-listed 2026-09-05); execution data is fine, signals are the alpha owner's question.
6. Paper roles keep JWT 13 / r25 (accept-set). If anyone recreates them later, they must keep r25 + JWT 13 or move to r26
   + JWT 14 + the r14 sha together.
7. The writer's `MARKET_DATA_OKX_SYMBOLS` must be built from the live value plus the 71 by a script; a hand-typed list is
   the classic failure.
8. A rollback after the new OKX lane has run leaves 142 bindings' records on the raw topic. The old cores ignore them
   (`oos`), which is proven for stage A, not for stage B specifically.
