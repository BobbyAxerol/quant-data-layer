# Combined production window: DL v2.3.0 rc2 + TS writer W-B2 (DRAFT)

Status: **DRAFT for adversarial review. NOT APPROVED, NOT RUN.** Written 2026-11-13 by the DL integration agent;
section W is written by the TS writer agent. The owner decided on one early window (coordinator, 2026-11-13). Every
recreate below needs the owner's GO for this exact blast radius and rollback (CLAUDE.md section 3). Values marked
**TBD** are filled by the named producer before review closes.

## 0. Scope in one paragraph

- **DL v2.3.0 rc2** (`release/v2.3.0-rc2-integration`, code `832c02c4`) delivers:
  - DERIVED_FINAL bars for the 25 covered OKX instruments at 3m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H and 1D (1W, 2D and
    3D stay off until R-cert 2 passes the Monday 00:00Z boundary; they are enabled later by declaration);
  - per-binding contract lineage and per-binding cursor lineage;
  - the contract-line schema_digest (value unchanged: line 1 `100966435a30367d...`);
  - the declaration mechanism (mounted bar_edge declaration, checkpoint v5, accept-set, selectors, sticky lanes);
  - the consumer quota model;
  - the producer linger knob, which ships unset.
- **TS writer W-B2** delivers the IDR-289 lineage handoff, probe deferral and IDR-287 strict resume.
- **Writer release base (owner decision 2026-11-13).** W-B is the new writer release base. The window rolls exactly
  `sha256:9dfef8a8a548be2ee2d800fb62c006c8d11fcbcf3c66999779f6c97e15a71a6f`, built from a clean `git archive` of TS
  `4af62754`.
  - Labelled as an exception: "local build, CI pending".
  - Lane A runs a full CI on the same tree as the release receipt (CI6).
  - If CI goes red, roll back to `7332e85a` by the rehearsed path (BOOTSTRAP then STRICT, section W.5).
  - Reviewed delta: TS `upgrade/evidence/P184_WRITER_REVIEWED_DELTA_R1_TO_WB3.json`.
- Order: the DL roles go first, on the new contracts, before any derived bar is published. Then bar_edge. The writer
  goes last.

## 0a. Certification status at GO (what is and is not proven)

**This window rolls rc2 (`832c02c4`) to production as a release candidate.** It is not the DL v2.3.0 release. The
final DL v2.3.0 release and tag happen only after the R-cert 2 final verdict and S1-S6 pass.

| Item | Status | Evidence |
|---|---|---|
| R-cert 1 | PASS | 740,350 records compared, 0 mismatches; receipt `6814e25c` |
| R-cert 2 (shadow) | **Interim only** | 840/840 MATCH at 12:03Z on the **rc1** images (`7af599de`). S1-S6 run Monday 06:00Z; final verdict after 12:30Z |
| R-cert 4 (capacity envelope) | **Not run** | The window's measured substitute is the D0 median and the D6 + 30 min gate, DL vCPU <= 6.0 (section 7a) |
| rc2 full suite | PASS (environment errors only) | Run on `3f45a315`: 4296 tests, 4 errors, all `/app/logs/app.log` permission errors for the non-root user; those modules re-run as root: 15 OK |
| rc2 delta after the full suite | PASS | `3f45a315..832c02c4` changes only `qdl/runtime/lineage.py`, `scripts/dl_declaration.py`, 2 fixtures and 2 test files (no Rust, no runtime role code). Delta tests: `test_v230_lineage` 14 OK on `c7fe877`; tool + bar_source + lineage 47 OK (8 skipped) on `832c02c4` |
| Declaration render | Gate 18/18 PASS | Tool `832c02c4`, depth 1000 render (section 3) |

**rc2 vs rc1 (`7af599de..832c02c4`), by commit**, and how each is covered:
- `0f20d8f`, slice 2d: per-binding contract lineage, cursor lineage, contract-line schema_digest. Python + Rust
  (`cursor_v3.rs`, `gateway_bundle.rs`, stream-gateway, native ingestor). Covered by the full suite and the 2d cargo tests: contracts 36, stream-gateway lib 69 + native_stream
  56, ingestor 80, clippy `-D warnings` (DL plan, slice 2d entry).
  It is **not** covered by the R-cert 2 shadow, which runs rc1.
- `de9d766`: declarable final-BAR source (`bar_source` / `bar_1m_source`) and the OKX REST budget gate. Covered by
  the full suite and `test_v230_bar_source`.
- `c5c42dd`: the tool sets `bar_1m_source: native` for the 150 tier. Covered by the full suite.
- `5b18b86`, `c7fe877`: config lineage seeded from the live ingestor stamps, covering every binding of a shared
  source. Lineage helpers and the gate only. Covered by the delta tests.
- `832c02c`: tool support for the bar_edge runtime pair, live config lineage and the 6.0 vCPU OKX-first cap. Covered
  by the delta tests and the 18/18 gate.
- Not proven before GO, so the window gates are the check:
  - rc2's lineage code against live consumers: the D3/D4 lineage and cursor gates, plus the writer gates;
  - rc2's CPU cost: the section 7a gate.

**Shadow-tied rollback criteria.**
- The window's rollback path (section 5) back to v2.2.6 stage-A r13 applies if any of these fails:
  - Monday's S1-S6;
  - the R-cert 2 final verdict;
  - a re-run of R-cert 2 on the rc2 images, if the owner requires one.
- The section 5 order, the M4 offset guard and the writer BOOTSTRAP-then-STRICT path (W.5) apply unchanged.
- The DL roles go back to their section 1 images and recorded compose: stage-A r13 for query/stream/OKX ingestor,
  stage-A for cores/projectors, v2.2.5 for Binance ingestor/bar_edge.

## 1. Live state the packet starts from (read-only docker inspect, 2026-11-13 ~10:40Z; re-read at T-0)

| Role | Image | Created from (compose labels) |
|---|---|---|
| query_kn_1/2, stream_kn_1/2, ingestor_okx_swap | 753604f8 / 27c79b53 / 27c79b53 | `releases/v2.2.6-stage-a-compose-r13/` (stage A retry 09:39-09:40Z) |
| rust_core x3, market_projector_1/2 | 7c848aad / 60f83a51 | `releases/v2.2.6-stage-a-compose/` (08:06-08:09Z) |
| ingestor_binance_usdm, binance_bar_edge | 27c79b53 / 08f356a8 | `data-layer-dev` v2.2.5 compose (2026-10-20) |

- bar_edge reads its image config: catalog r11 / acquisition r19.
- Its checkpoint is v4: 640 binding_ids, 635 watermarks.
- T-0 check: every row above is re-read and compared. Any difference aborts.

## 2. Images (rc2; built by the derived-bars agent after the rc2 full suite passes)

| Variable | rc2 image | Roles |
|---|---|---|
| QDL_IMAGE_CORE | `qdl-v2-rust:2.3.0-rc2-832c02c4` = `sha256:58bdd28216139f0c1a40abeee3077834f1cf922cf6412e7fb1ce8845591a0512` (label revision 832c02c4) | rust_core x3 |
| QDL_IMAGE_PROJECTOR | same rust image `sha256:58bdd282…0512` | market_projector_1/2 |
| QDL_IMAGE_STREAM | same rust image `sha256:58bdd282…0512` | stream_kn_1/2 |
| QDL_IMAGE_INGESTOR | same rust image `sha256:58bdd282…0512` | ingestor_okx_swap, ingestor_binance_usdm |
| QDL_IMAGE_QUERY | `qdl-v2-python:2.3.0-rc2-832c02c4` = `sha256:03de529acc4f59ba2637f289aa196f544944f92fdcf719390cdf9aacbdfd441b` (label revision 832c02c4) | query_kn_1/2 |
| QDL_IMAGE_BAR_EDGE | same python image `sha256:03de529a…441b` | binance_bar_edge |

Build receipt: `~/.local/state/qdl-v2/v230-rcert2/images-832c02c4.json`. Both images come from a git-archive context of `832c02c4`; the Python dependency image is `qdl-v2-python@sha256:753604f8…`, and `verify_runtime_dependencies` PASS.

- Kafka and Redis images are unchanged.
- Digests come from `docker inspect` of the built images. No retag of an existing tag.

## 3. The first v2.3.0 declaration (`scripts/dl_declaration.py`, tool `832c02c4`; full crossec scope)

- **Render.** Output `scratchpad/dlv230tool/first/out4`, declaration `../declaration-first-v230-full.yaml`
  (declaration_revision 5), gate `../gate4.json`. Read-only against the live base; every live input is recorded with
  its sha256. Earlier renders `out`..`out3` are superseded.
- **Scope** (owner decision: the full crossec signal universe in this window):
  - **`okx-swap-crossec-94`** (crossec 96; FET and LUNA BLOCKED, NOT_IN_CATALOG):
    - native candle1m for 89 new instruments (RUST_NATIVE); the 5 majors already have it;
    - DERIVED_FINAL 1h and 12h from 1m;
    - alpha grants 1h and 12h (1m is a parent and is not granted);
    - FUNDING_RATE and CONTRACT_METADATA on their existing reference path (no live funding stream exists; that would
      be a code release).
  - **`okx-swap-covered-25`:** derived 3m..1D from native 1m, granted.
  - Not declared: 1W, 2D, 3D (after R-cert 2 passes the Monday boundary), and the 4-contract price_tick refresh
    (non-additive, needs its own migration record).
  - TS QUOTE/MARK for the other 69 is **stage B**, right after this window: about +1.40 vCPU, which with stage A puts
    it at the 6.0 cap.
- **Gate: 18/18 PASS:**
  - `live_config_lineage`: 181 live bindings, 34 at 19 (Binance, including the 7 book_delta), 147 at 21;
  - `retained_lineage_unchanged`;
  - `bar_edge_core_raw_lineage`: 1047 bindings x 3 cores;
  - `bar_edge_migration_record`: 11/19 -> 14/22 matches the live v4 checkpoint (640);
  - `okx_rest_budget`: the REST lane carries only the 1d polls, 0.2/min;
  - `capacity_envelope`;
  - plus base, index, loaders, manifests, accept-set, routing, additivity, lanes, chain checks.
- **Revisions:**
  - catalogue 13->14 (+407 BAR); acquisition 21->22 (318 DERIVED_FINAL + 89 RUST_NATIVE 1m); demand 9->10;
    scope 12->13; routing 30->31.
  - `alpha.okx.paper.stable` r15->r16, `min_token_revision` 15, 871 requirements. The BAR 1h/12h rows mirror the r15
    rows exactly (SNAPSHOT_AND_REPLAY, ALPHA). Quotas carried over from r15 (rpm 4950, streams 300, warmup 10000;
    the quota model covers EXECUTION only, alpha is MODEL_PENDING).
  - TS manifest r13 / binding r25 unchanged.
- **Ingestors:**
  - OKX 163 -> 252 subscriptions (+89 candle1m); business-001 fills to 100, new business-002 takes 59; retained lane
    names unchanged; no lane over 100.
  - Binance 27, pins only.
  - Both need the rc2 ingestor image (lane_shard).
- **Files** (index prefixes):

  | File | sha256 prefix |
  |---|---|
  | runtime catalogue | `6f1699d4` |
  | runtime acquisition | `81d001aa` |
  | bar-edge pair | `9154bf1a` / `ba7bd0f4` |
  | migration record | `cd15ee73` |
  | universes | `b40826c7`, `a131d737` |
  | realm r16 | `b998bdce` |
  | kn bundle | `1ae1d908` |
  | realm bundle | `aaf12392` |
  | cores | `ff105182` / `348f2c70` / `dfab3186` |
  | OKX ingestor | `4cb59e29` |
  | Binance ingestor | `021dd5dc` |

- **Bootstrap (one-off). Decision R-H2 = depth 1000 (coordinator, 2026-11-13; matches L6).**
  - The new selectors declare max_warmup_rows 1000, capped by the 1,095-day lookback. That covers crossec's need
    (180 x 12h plus 60 bars) with ample margin.
  - Estimate: BOOTSTRAP_ESTIMATE_PENDING (re-render out5). The 10000 render estimated 407 bindings, about 2.30M rows,
    about 23,058 requests and about 154 min on OKX/SWAP/HISTORY_CANDLES (150/min, a bucket separate from the live
    CANDLES lane), assuming 100 rows/request, one pass and no retries.
  - **Deeper history later.** Depth up to 10,000 is a later declaration-only step using the v2.3.0 mechanism: a
    config revision, the coverage gate and an ordered restart, with no image and no tag. It runs off-window and
    outside the 23:30-00:05Z band. The alpha's 1h depth beyond 1000 h follows that path if they ask.
  - Not verified: whether native gap repair shares the HISTORY_CANDLES bucket.

## 3c. Consumer manifest revisions and JWTs (verified read-only 2026-11-13 ~12:40Z)

- **(a) What changes.** The render changes one consumer manifest. The other five render byte-identical to the rc2
  repo.

  | Manifest | Live (query_kn_1 mount) | Render |
  |---|---|---|
  | `alpha.okx.paper.stable` | r15 `/consumer-realms/alpha.okx.paper.stable-r15.yaml` sha256 `33b9cfd4ff19fee2...` | **r16**, `metadata.min_token_revision: 15`; out4 sha256 `b998bdce7be0c1c7...`, replaced by the depth-1000 render (out5) |
  | `trading-system.paper.stable` | r13 `/consumer-realms/trading-system.paper.stable-r13.yaml` sha256 `cfc20e29b18d35c9...` | r13, byte-identical (`cfc20e29...`), no accept-set: exact 13 as today |
  | alpha-binance-paper r16, alpha-vn-paper r1, monitoring-multivenue r2, reference-l2-stable r7 | as today | byte-identical |
  | sandbox / live realm files | as today | not rendered, not touched |

- **Why r15 tokens keep working.**
  - r16 passes `additive_problems(r15, r16) == []`: same identity; purposes, permissions, requirement rows and
    quotas are a superset. So the renderer keeps the floor at 15 (`qdl/consumer/manifest_compat.py`
    `next_min_token_revision`).
  - rc2 Query (`qdl/security/data_plane.py:391`) and stream (`authority.rs:116`, `auth.rs:546`) accept a token
    revision in 15..16. Grants always come from the active manifest.
  - Before D3/D4 the old roles still compare exactly against r15, and after a rollback they compare exactly
    against r15 again. An r15 token is valid in every state of the window and of the rollback.
- **(b) Who holds a JWT for it now.**
  - Read-only `docker inspect` of every running container found no live consumer of `alpha.okx.paper.stable`.
    No container sets an `alpha.okx` consumer id or JWT revision, and no hosted-alpha container runs.
  - The only references are the server side: query_kn_1/2 `QDL_STABLE_CONSUMER_MANIFESTS` and the JWT key maps.
  - The `ts-sbx-executor-*` matches are `COMMAND_JOURNAL_CANARY_SCOPES`, not DL tokens.
  - Containers holding a DL consumer JWT revision, all `trading-system.paper.stable`:

    | Container | JWT revision |
    |---|---|
    | `market_data_service_p184w` | 13 |
    | `w5d-ts-gateway`, `w5d-ts-paper`, `w5d-ts-performance`, `w5d-ts-risk` | 11 |

  - The w5d rev-11 value is pre-existing and is not changed by this window, since the TS manifest is byte-identical.
    Whether those services call DL directly with it is **not verified** here.
- **(c) Consumer-side plan.**
  - No bump is needed in the window.
  - The hosted alpha can start on a JWT r15 for `alpha.okx.paper.stable`. It is accepted both before and after the
    window and after a rollback.
  - Issue a JWT r16 only after the DL v2.3.0 final (R-cert 2 final + S1-S6), because the rollback roles (v2.2.6
    stage-A r13) refuse r16.
  - Hosted-alpha start gate: `alpha.okx.paper.stable` token revision in {15, 16} on rc2, 15 while rollback is
    possible. This is separate from the TS writer's JWT 13 / binding r25 gate.
- **(d) TS writer.**
  - `trading-system.paper.stable` stays r13 (byte-identical) with an exact compare.
  - The render writes no TS file, so consumer binding r25 is untouched.
  - W-B2 keeps `DATA_LAYER_V2_JWT_MANIFEST_REVISION=13` and binding r25 (section W env row).

## 3a. Two catalogue lineages (verified read-only 2026-11-13) and how the declaration keeps both

- The live core rows carry a per-binding raw `instrument_catalog_revision`, the revision at which the binding was
  added. Live `rust_core.json`: 695 rows at 11 (all 642 BAR rows), 4 at 12, 60 at 13. The OKX ingestor: 99 / 4 / 60.
- The core quarantines a raw envelope whose value differs from its row (`qdl-realtime-core/src/lib.rs:698`).
- Query's served contract shows 13 for every product. Today bar_edge stamps 11 / config 19 from its image r11/r19.
- Declaration rule (gated):
  - Query keeps its own `/runtime` catalogue with served lineage 13 for every retained binding.
  - bar_edge gets its own pinned pair, `/runtime/bar-edge-source-bindings.yaml` and
    `/runtime/bar-edge-acquisition-bindings.yaml`. Every bar-edge-owned BAR binding is at catalogue lineage = its live
    core row value (11) and config lineage 19, so bar_edge stamps exactly what it stamps today. New derived rows carry
    the new revisions in both files.
- The gate checks that the bar-edge file lineage equals the live core rows across all 3 cores, and that the
  retained served lineage is unchanged.
- Superseded by the full render: the stage-A 20 (and all 89 new) get NATIVE candle1m on the OKX ingestor; no REST_FINAL 1m. The REST lane carries only the 1d polls.
- Manifest: `alpha.okx.paper.stable` r15 -> r16 (additive selectors for the derived intervals), `min_token_revision`
  15. JWT 15 stays valid on rc2 stream/query; the old roles never mount r16.
- Live input hashes (read-only `--rm` container, per-directory ro mounts):
  - Query runtime catalogue `f51ef5c4`, acquisition `bf9fbad4`;
  - kn bundle `9136beba`;
  - OKX ingestor `d62f36b4`, Binance ingestor `5833bc4e`;
  - runtime authority `db00040f`;
  - bar-edge checkpoint `d179ca89`.

## 3b. Data path (KN, kn3 market cache; not the spool)

- **Live 1m** comes from the ingestor's native candles on the OKX business lane. Then: raw Kafka, Rust core
  (canonical), projector, KN market cache (`market_cache`).
- **Derived bars** (`DERIVED_FINAL`):
  - bar_edge reads the parent's durable final bars from the KN market cache (`kn_bar_readback`);
  - it publishes one raw derived record per closed target bar;
  - the Rust core recomputes the aggregate and is its only canonical writer (`BAR_ORIGIN_AGGREGATED`);
  - the projectors write it to the KN cache.
- **History bootstrap** of each new binding (native or derived) is a bar_edge REST history read through the Rust
  provider admission (BATCH class). It publishes to raw Kafka, then core, then projector, then the KN cache. It is
  sized by the demand bundle (`/kn/bundle.json`) and declared depth.
- **Warmup reads.** Query (`QDL_STABLE_QUERY_BACKEND=kn3`) serves them from the KN cache, up to 10,000 rows per
  request (manifest `max_warmup_rows`).
  - Alpha r16 rows keep `FRESH_SNAPSHOT` recovery for deep warmup beyond the cached window.
  - Funding and metadata stay on their existing paths.
- **Not used:** the SQLite spool (`StableSpoolQueryBackend`) is not on this path in production.

## 4. Steps (each: recreate per role with `up -d --no-deps --force-recreate <svc>`, then its gate, then the next)

Compose: a durable copy of the rc2 `deploy/compose/qdl-v2-stable-kn.yml` plus a release env. The env is the stage-A
env with the rc2 image variables and the declaration paths. Same TLS and secret env files as each role's current
labels.

| # | Role(s) | Change | Gate before the next step |
|---|---|---|---|
| D-1 | none | After the rc2 image builds: `df /`; prune build cache and unreferenced images BY DIGEST if under the floor (M5). | Free >= 100 GiB AND >= 20% before D0 |
| D0 | none | Preflight. Host lock. Read PRE values: writer heartbeat demanded/ready/execution_ready/continuity, DL vCPU, `df`. Copy the 3 manifests. Install the declaration dir. Gate report PASS. | Every input sha256 matches the gate report. Disk free >= 100 GiB and >= 20% of `/` (M5). PRE values recorded |
| D1 | rust_core x3 | rc2 core image + declaration core configs (derived raw kind) | Simultaneous-replace method (rc22 runbook). 3/3 healthy. Group stable (0 rebalances after settle). Lag recovers. quarantines not growing. scope_quarantines 0 |
| D2 | market_projector_1, then _2 | rc2 image (Bar.aggregation field 19) + new bundle | writer heartbeat ready == demanded (PRE, 104). projector lag back to baseline. 0 ENVELOPE_NOT_CANONICAL |
| D3 | stream_kn_1, then _2 | rc2 image + new bundle (per-binding lineage) | Health OK. A retained-product cursor from before the window still verifies (writer heartbeat: no CURSOR_EXPIRED) |
| D4 | query_kn_1, then _2 | rc2 python image, mounted manifests, new catalogue | `/health/dependencies` declaration `image_files=0`. A sampled retained record's contract lineage equals its pre-window value. DR02/DR03 byte compare |
| W-pre | writer | Stop the writer (section W) before the ingestor step, as in stage A (review H1) | Writer stopped; outage bound per section W |
| D5 | ingestor_okx_swap, then ingestor_binance_usdm | rc2 image, lane pins = today's, linger unset | started event: `lane_plan: PINNED`, `producer.linger_ms: null`, timeout 30. Lane names in session-liveness unchanged. 0 subscription rejected. Book resync complete |
| D6 | binance_bar_edge | Stop bar_edge, THEN copy its checkpoint to `.pre-v230` (M3: a D0 copy would be stale), then rc2 python image, bar-edge pinned pair `/runtime/bar-edge-*-bindings.yaml`, migration record env | Log `checkpoint crosses catalog 11->R acquisition 19->R' crossed=True retained=640 added=<derived> dropped=0 migration_record=True`. Heartbeat `declaration` image_files=0. Derived bars for the 25 appear on md.bars.v2 and in the market cache. Parity auditor: 0 mismatches on the first closes |
| W | writer (section W) | W-B2 image | Section W gates |

- **Why this order.** Cores, projectors, stream and Query run the new contracts before bar_edge (D6) publishes any
  derived bar. An old projector drops field 19 and fails ENVELOPE_NOT_CANONICAL (derived-bars agent). Lineage is
  pinned, so D1-D5 do not change any record lineage the writer sees.
- **Writer gates** (all thresholds are PRE values read at D0, not constants; review H1).
  - D1-D4 (old writer 7332e85a running):
    - ready == demanded (PRE 104), execution_ready >= PRE (92 at review time);
    - no CURSOR_EXPIRED / EXECUTION_ORDERING_AUTHORITY_MISMATCH / RESUME_GENERATION_MISMATCH;
    - with pinned lineage, any of these is a DL lineage defect: stop and roll back.
  - After the W recreate (W-B image, section W):
    - continuity_valid == continuity_required (PRE 11/11) within N minutes (section W), then 0 gaps after settle;
    - ready == demanded, execution_ready >= PRE, 0 parked.
- **Stop rule.** Any gate FAIL stops the window. Roll back that step, then the coupled set (section 5).

## 4a. Window plan (estimates; T = start of D0)

- **Start.** Any time 14:00-21:00Z today (2026-11-13), with the owner's GO; proposed 19:00Z.
  - Latest start 21:00Z, so that D6, the bootstrap and the D6 + 30 min gate end before 23:30Z.
  - Otherwise start after 00:05Z.
  - The 23:30-00:05Z band is avoided: daily 1d close, HISTORY_CANDLES contention.
- **Duration.** About 2 h, from D0 to the end of the D6 + 30 min CPU gate. Timings are estimates, not measured.

  | Step | Time |
  |---|---|
  | D0 (preflight; the 10 min CPU median sets the headroom) | T+0 to T+15 |
  | D1 (cores, simultaneous replace) | T+15 to T+25 |
  | D2 (projectors) | T+25 to T+33 |
  | D3 (stream) | T+33 to T+38 |
  | D4 (Query) | T+38 to T+43 |
  | W-pre | T+43 |
  | D5 (ingestors) | T+43 to T+50 |
  | D6 (bar_edge; the bootstrap runs about 15 min at depth 1000) | T+50 to T+70 |
  | W (W1 + W2 + 10 min soak) | T+70 to T+90 |
  | D6 + 30 min CPU gate | until about T+100 to T+120 |

- **Writer outage.** Paper fails closed from W-pre to W2 VERIFIED, about 35 min. Bound: 30 min from W-pre to D5 + D6
  green, else roll back (section W).
- **Blast radius.** Recreated:
  - DL: rust_core x3, market_projector x2, stream_kn x2, query_kn x2, ingestor_okx_swap, ingestor_binance_usdm,
    binance_bar_edge (11 containers, `qdl_v2_stable_candidate`);
  - TS: `market_data_service_p184w` (writer, W-B `9dfef8a8...`).
  - Consumers interrupted briefly: Paper (fail-closed outage above), Sandbox G1 / W05 relays (section 6).
- **Rollback.** Section 5: reverse order, about 1 min per role, to the section 1 images. Writer back to `7332e85a`
  via BOOTSTRAP then STRICT. M4 offset guard; no offset reset or seek. Shadow-tied criteria in section 0a.
- **Not touched.** Section 6: Kafka brokers and config, topics and retention, market_cache and stable_redis, the
  cert mesh, CPU and memory caps, monitor, Portal, every TS service other than the writer, every consumer JWT (no reissue; the only manifest change is alpha.okx.paper.stable r15 -> r16 with accept-set 15..16, section 3c).

## 5. Rollback (coupled, reverse order; each about 1 min per role)

1. **W:** section W.
2. **D6:** stop bar_edge, restore `crypto-bar-checkpoint.json.pre-v230` (taken at D6 after the stop), recreate it with image 08f356a8 and the
   v2.2.5 compose (image config r11/r19). Derived bars stop. Already-published derived bars stay in Kafka/cache as
   final bars of products no consumer is entitled to after rollback.
3. **D5:** ingestors back to 27c79b53 with their recorded compose/env (lane names never changed).
4. **D4 to D1:** each role back to its recorded image and compose/env, in the order Query, stream, projectors, cores.
   - Cores use the simultaneous-replace method.
   - The old bundle and catalogue carry no lineage pins, so records go back to the global values. These equal the
     pinned ones, so lineage does not move.
5. **Never in rollback:** a new image build, Kafka/Redis changes, data deletion.
6. **No offset reset or seek of projector groups (M4).** After a rollback, the derived BAR records with field 19 stay
   in md.bars.v2 and the canonical topics for their retention period.
   - The old projector (60f83a51) stops its partition on an envelope it cannot encode (`stage_b.rs:1240`, `:144`
     "never skipped"). Restarted old projectors resume past those offsets, so this is safe as long as no group offset
     is reset or seeked and no rebuild crosses those records until retention passes the last derived offset.
   - If ONLY D6 fails: roll back bar_edge alone. Keep the projectors (and cores, stream, Query) on rc2; they are
     additive and safe.
7. **Offset guard (M4).** Before any rollback, and in the receipt and the DL plan entry: record the last derived
   offset per md.bars.v2 and md.canonical.v2 partition, plus the retention expiry time of that offset. "No reset or
   seek until retention passes" is checked against these values.
8. **Left behind but unmounted after a full rollback:** the r16 realm file, the r14 bundles, the declaration
   directories, the `.pre-v230` backup. They are harmless; list them in the receipt.

## 6. Not touched

- kafka1-3 and broker config;
- market_cache and stable_redis (ephemeral, CLAUDE.md 4.4);
- topics and retention;
- the cert mesh;
- Sandbox G1 and W05 are NOT untouched: `ts-sbx-market-data` reads both streams and both Queries, and W05 relays `redis_marketdata`. D1/D3/D4/D5 interrupt them briefly. Checks per section W (G1 health and W05 relay health before and after, as in the stage A v3 runbook);
- CPU and memory caps (unless section 7 says so);
- monitor, Portal, every TS service other than the writer (Sandbox G1 / W05: see above);
- every consumer manifest except `alpha.okx.paper.stable` (r15 -> r16, additive, accept-set 15..16; section 3c).
  No consumer JWT is changed or reissued in the window. The TS writer keeps JWT 13 and binding r25.

## 7. CPU / disk impact

- Steady: about +47 records/s (+89 native 1m, derived bars), about +0.085 vCPU (gate). bar_edge REST lane: 1d polls
  only.
- REST visibility: last 1m read +6.1 s after the boundary. At 00:00Z the 1m and 250 D48 1d reads queue together:
  last at +106 s (today about +98 s).
- Bootstrap: depth 1000 (R-H2, section 3), about 15 min, bounded by the 150/min HISTORY_CANDLES lane.
- DL vCPU today: ~4.75-5.16 after Kafka F1 (coordinator). Expected steady delta about 0 vCPU (+0.2 records/s); the bootstrap burst is bounded by the 150/min lane.
- Disk: the bootstrap adds about 200k bar rows once at depth 1000 (bytes not verified). Floor at D0 and D6: free >= 100 GiB AND >= 20% of `/`; 109 GiB / 22.5% at review time; 118 GiB / 24% free after the rc2 Rust build (12:08Z). Abort below.

## 7a. CPU budget (owner rule 2026-11-13)

- **Cap.** DL CPU budget is **6.0 vCPU**. If a window or declaration would exceed it, OKX coverage (all OKX bindings
  and endpoints) has priority: Binance additions are deferred or trimmed first, never OKX. The declaration gate
  encodes this (venue priority OKX > BINANCE, cap 6.0).
- **Headroom** = 6.0 minus the D0-measured median DL vCPU (all `qdl_v2_stable_candidate-*` roles, cgroup 60 s
  samples over 10 min).
  - Coordinator's current figure: about 4.75-5.16, so headroom is about 0.8-1.25.
  - The declaration's steady delta is about 0. rc2 is new code on every DL role, so the delta is only known by
    measurement.
- **Recorded gate:** D6 + 30 min median DL vCPU <= 6.0. If it is exceeded, apply the OKX-first rule: defer Binance
  additions by declaration. This window adds none, so exceeding means rc2 itself costs more, and the window rolls
  back.

## 8. Evidence to keep

- Per step: docker inspect before and after, the gate outputs, logs filtered to the gate lines.
- Window receipt into `upgrade/evidence/releases/v2.3.0/window-<ts>/` with `SHA256SUMS`. No secrets.

## W. Writer W-B2 (W-pre before D5, W after D6) - written by the TS writer agent

> Release base: W-B `9dfef8a8...` = writer release base, owner decision 2026-11-13 (local build, CI pending; a red CI means rollback to `7332e85a` per W.5). See section 0.


**Naming.** "W-B2" is the release name used in this packet. The image is the final W-B2 build:
- id `sha256:9dfef8a8a548be2ee2d800fb62c006c8d11fcbcf3c66999779f6c97e15a71a6f`;
- tag `local/trading-system:p184-writer-wb-4af62754`;
- label revision `4af62754a69f5930994283a58517de74cc1eaa2c`.

The working names "W-B3" and the `0b9843a2` / `6dc2a35c` builds are superseded and must not be used.

- Source: TS `fix/writer-strict-resume-class-20261113` @ `4af62754`, not pushed. Worktree: `/home/bobby/.worktrees/ts-writer-resume-class-20261113`.
- Tools: from a clean `git archive` of TS `2d2e18eb` with blob checks (W.9), never from the worktree.

**W.1 What changes.** One container, `market_data_service_p184w`:

| | Live (T-0 re-read) | After W |
|---|---|---|
| Image | `sha256:7332e85a7f125a89e587a263267ca187a8ec2ee62ebd9eb2608e546eb8835944` | `sha256:9dfef8a8a548be2ee2d800fb62c006c8d11fcbcf3c66999779f6c97e15a71a6f` |
| Env | stage-A env at T-0: JWT manifest revision 13, consumer binding file r25, `MARKET_DATA_NATIVE_REQUIRE_RESUME=1` | the same, plus `MARKET_DATA_NATIVE_GAP_BOOTSTRAP_FEEDS=QUOTE`. Nothing else changes; W1 is transiently REQUIRE_RESUME=0 |
| Mounts, networks, limits, restart policy | as live | unchanged; the tool copies them from `docker inspect` |

The image carries:
- IDR-287: QUOTE-only recorded gap bootstrap. TRADE parks with a CRITICAL alert.
- Native health reports flushed in order (the 09:53Z respawn class).
- IDR-288 reader: inactive, because `MARKET_DATA_DECLARED_SCOPE_FILE` is not set.
- IDR-289: revision-only handoff for BOOK/MARK ordering and native resume, in either direction.
- Smart reads.

Lineage is pinned in section 3, so the handoff is defence in depth here.

**W.2 PRE values (read at D0, read-only).**
- From the writer heartbeat `details`: `demanded_v2_slices`, `ready_v2_slices`, `execution_ready_v2_slices`, `continuity_valid` / `continuity_required`, and `native_continuity` gaps.
- At 11:27Z they read: 104 / 104 / 93 / 11 of 11 / 0. execution_ready moves between 92 and 93 at stage A.
- The gates below use these PRE values, never constants.

**W-pre. Stop the writer before D5 (review H1, as stage A).**
```
docker stop --time 30 market_data_service_p184w     # restart policy unless-stopped: stays stopped
docker inspect market_data_service_p184w --format '{{.State.Status}} {{.Image}}'   # exited sha256:7332e85a...
```
- **Outage.** From W-pre to W2 VERIFIED: D5 plus D6 plus about 8 min for W (2 x 180 s verify plus the starts).
  - Bound: if D5 plus D6 is not green within 30 min of W-pre, abort to rollback (W.5).
  - The outage is longer than the 120 s row TTL. This is why W starts BOOTSTRAP first (W1) and goes strict second (W2), as in stage A. A strict start after a stop longer than 120 s refuses: no marker, and TRADE parks.
- **Effect on Paper admission** (fail closed; source read, not exercised in this window):
  - About 120 s after W-pre, the execution QUOTE/TRADE/BOOK values expire.
  - Risk then refuses admission with typed reasons (`services/risk_engine/market_data.py:178` `MARKET_DATA_STALE`; `:236` `MARKET_DATA_FEED_MISSING_<FEED>` / `MARKET_DATA_STALE_<FEED>`) for every writer-served instrument. No order is admitted on stale data.
  - Orders already resting are not touched by the writer. Whether Paper fill simulation also pauses is **not checked**; the Paper owner has to answer that.
  - The W05 relay (`w5d-market-relay` -> `w5d-market`) copies `redis_marketdata`, so the W05 namespace sees the same expiry.
- **Stop before D1 instead? Not recommended.**
  - Lineage is pinned, so D1-D4 do not move any record lineage the writer sees.
  - The running old writer is the consumer-side signal for the D2/D3 gates: ready == demanded, no `CURSOR_EXPIRED`.
  - Stopping before D1 adds D1-D4 to Paper's fail-closed outage and removes those gates. It buys nothing: the writer does not read the cores directly.
  - What the writer must not see is the ingestor lane change during D5, and W-pre covers that.

**W.3 Commands** (TS worktree; the tool writes a 0600 env file, masks secrets in its output and deletes the file):
```
# W1 BOOTSTRAP on the new image, from the stopped writer
python3 -B scripts/p184_writer_recreate.py --expect-image sha256:7332e85a7f125a89e587a263267ca187a8ec2ee62ebd9eb2608e546eb8835944 --image sha256:9dfef8a8a548be2ee2d800fb62c006c8d11fcbcf3c66999779f6c97e15a71a6f \
  --allow-stopped --require-resume 0 --set-env MARKET_DATA_NATIVE_GAP_BOOTSTRAP_FEEDS=QUOTE \
  --min-execution-ready <PRE execution_ready> --deadline 180 --convergence-s 120 [--dry-run first]
# W2 STRICT (same image; the strict precheck refuses unless every QUOTE/TRADE marker + value exists and is <= 30 s old)
python3 -B scripts/p184_writer_recreate.py --expect-image sha256:9dfef8a8a548be2ee2d800fb62c006c8d11fcbcf3c66999779f6c97e15a71a6f --require-resume 1 \
  --min-execution-ready <PRE execution_ready> --deadline 180 --convergence-s 120 [--dry-run first]
```
- Review each dry-run diff:
  - W1: image, REQUIRE_RESUME 1 -> 0, and the GAP key added;
  - W2: REQUIRE_RESUME 0 -> 1 only.
- Each run: stop (W1 skips it, the writer is already stopped), rename to `.prev-<ts>` (kept), run, connect the extra networks. The W2 outage is 10-20 s.

**W.4 Gates.**
1. W1 and W2 each print `VERIFIED`. The checks:
   - READY; every worker READY;
   - `ready == demanded == PRE demanded` (104);
   - `execution_ready >= PRE execution_ready`, with one bounded extension of 120 s only while execution_ready alone is short and rising;
   - QUOTE keys < 2 s old, TRADE < 15 s;
   - 0 parked, 0 `native_continuity` gaps.
2. Continuity: within **N = 5 min** of W2 VERIFIED, `continuity_valid == continuity_required == PRE` (11 of 11 at 11:27Z), then 0 gaps through the soak.
   - N is the tool's verify bound (180 + 120 s) rounded up. It is not measured on live.
   - BOOK continuity needs a fresh snapshot plus deltas per instrument after a start.
3. Soak 10 min:
   - no native worker respawn;
   - 0 `EXECUTION_ORDERING_AUTHORITY_MISMATCH`, `RESUME_GENERATION_MISMATCH`, `NativeWorkerError`, `CURSOR_EXPIRED` in the writer log;
   - count `NATIVE_HEALTH_STALE_REPORT`: more than 0 is a finding, a respawn is the stop.
4. Probe rate, recorded and not a gate. Read-only heartbeat GET over 120 s.
   - Baseline on `7332e85a` at 10:30Z: QUOTE 2,743 probes/min (1,920 SkippedOffset), TRADE 39.8/min.
   - Writer CPU baseline: 1.65 vCPU.

**W.5 Rollback** (writer only):
- From W1 or W2, use the fast path the tool prints:
  ```
  docker stop --time 30 market_data_service_p184w && docker rename market_data_service_p184w market_data_service_p184w.failed-<ts>
  && docker rename market_data_service_p184w.prev-<ts> market_data_service_p184w && docker start market_data_service_p184w
  ```
  - After W2 this restores the W1 container. Repeat the rename with W1's `.prev` to reach the stage-A container on `7332e85a` (JWT 13, r25, REQUIRE_RESUME=1, no new key).
  - If the writer was down longer than 120 s (always the case after W-pre), do not rely on that strict start: recreate the old image BOOTSTRAP then STRICT with the same tool: `--expect-image <current> --allow-stopped --image sha256:7332e85a7f125a89e587a263267ca187a8ec2ee62ebd9eb2608e546eb8835944 --unset-env MARKET_DATA_NATIVE_GAP_BOOTSTRAP_FEEDS --require-resume 0`, then `--require-resume 1`.
- If W-pre is done and DL rolls back: run the DL rollback first. By then the writer has been down longer than the 120 s row TTL, so **never `docker start`** the stopped strict container (TRADE would park). Always BOOTSTRAP then STRICT on the old image: `--expect-image sha256:7332e85a7f125a89e587a263267ca187a8ec2ee62ebd9eb2608e546eb8835944 --allow-stopped --require-resume 0`, verify, then `--expect-image sha256:7332e85a... --require-resume 1`, verify.
- If the old image refuses with BOOK `EXECUTION_ORDERING_AUTHORITY_MISMATCH` or native `RESUME_GENERATION_MISMATCH`:
  - stop the writer;
  - run `python3 -B scripts/p184_writer_scope_reset.py --symbols-from market_data_service_p184w --out <window dir>` (dry run, then `--apply`; exact keys, never MARK, read-back 0, per-key receipt);
  - start the writer BOOTSTRAP.
- Rehearsal run 2 (W.7a) proves the W-B2 -> 7332e85a rollback strictly on the isolated stack.

**W.6 Not touched by W.**
- DL roles, Kafka, cursor files, Postgres.
- Redis data, except what the writer itself writes and the scope-reset fallback.
- Consumer manifests and JWT; every other TS service; `shared/symbols.json`.
- G1 Sandbox, as in the stage A v3 runbook:
  - `ts-sbx-market-data` is **not running** at 11:27Z (`docker ps`). Its absence is re-checked at T-0; if it runs, its StartedAt and image must be equal before and after W.
  - `ts-sbx-executor-binh` / `ts-sbx-executor-okxn`: StartedAt and image must be equal before and after W. They do not read `redis_marketdata` through the writer.
- W05:
  - `w5d-market-relay` and `w5d-market`: StartedAt and image must be equal before and after W.
  - After W2, the relayed `execution_quote` keys in `w5d-market` are fresh again, < 2 s old, read-only GET.
  - Do not schedule the window over a W05 FT08/FT10 drill: coordinate with the W05 agent.

**W.7 Evidence.**
- Both tool run dirs: `journal.jsonl`, with stopped (`was_running`), renamed, started, samples, `failing`, and VERIFIED or VERIFY_FAILED.
- `docker inspect` before and after, secrets masked; PRE values; gate log lines; probe rate; CPU.

**W.7a Qualification of this image** (isolated DR05 DL stack, own identity and quota):
- 438,532,677 bytes; native binary sha256 `ebefe4c4...`; import smoke OK.
- Tests are listed in TS plan `ts-idr287-health-order-recreate-converge-20261113`.
- Smart reads (`rate2`, OKX ETH-USDT-SWAP, 120 s windows, stream applied in both):
  - QUOTE 69.27 -> 0 probes/min/route, TRADE 2.99 -> 0, i.e. a 100% reduction for both;
  - measured on `0b9843a2`; the probe code is identical in `4af62754`, where only `runtime.rs` changed;
  - stage-A 104 slices cannot be produced on DR05 (1 instrument per scope).
- Rehearsal run 2: **PASS** at 11:47Z. Receipt `~/.local/state/trading-system/p184-wb-qualification-20261113/run2/receipt.json`; TS plan `ts-writer-wb2-rehearsal2-pass-20261113`.
  - Rolls, each strict and VERIFIED, with 0 parked and 0 gaps:
    - 7332e85a -> W-B2, 12.2 s;
    - **rollback W-B2 -> 7332e85a, 12.1 s, strict on W-B2's state**;
    - -> W-B2 again, 11.9 s.
  - Gap proof: a 135 s outage on W-B2. QUOTE recorded one RESUME_STATE_EXPIRED gap and served; TRADE parked HANDOFF_RESUME_REQUIRED; no TRADE gap.
  - FT08 dry-run was SKIPPED: its budget exceeded the lock window. An earlier attempt passed every FT08 preflight check except baseline convergence, which DR05 cannot meet because it has no BOOK producer.
  - Not covered by DR05: W1 `--allow-stopped` (unit-tested only), a DL revision bump, and the 104-slice set.

**W.8 Open before GO.**
- (a) Owner decision on the release base. The release-build rules refuse W-B on the Dockerfile rule and the runtime dependency lock rule. Record: `upgrade/evidence/P184_WRITER_REVIEWED_DELTA_R1_TO_WB3.json`.
- (b) The image is a local build, not CI-built. PR and CI belong to lane A.
- (c) An isolated rehearsal of a DL revision bump across the writer is not supported by DR05 tooling (DL agent). The handoff evidence is unit and integration tests only.

**W.9 Tool provenance (as stage A v3).**
- Every writer tool runs from a clean `git archive` of TS commit `2d2e18eb937c6e7f9fd47e2dd11abf69be61f8b1`, never from a worktree:
  - `scripts/p184_writer_recreate.py`
  - `scripts/p184_writer_scope_reset.py`
  - their imports
- Extract the archive into the window directory and check every blob before use:
  ```
  W=<window dir>/ts-tools-2d2e18eb; mkdir -p $W && git -C /home/bobby/.worktrees/ts-writer-resume-class-20261113 archive 2d2e18eb937c6e7f9fd47e2dd11abf69be61f8b1 | tar -x -C $W
  for f in scripts/p184_writer_recreate.py scripts/p184_writer_scope_reset.py services/market_data/atomic_projection.py \
           services/market_data/cache_keys.py domain/instrument_identity.py; do
    test "$(git hash-object $W/$f)" = "$(git -C /home/bobby/.worktrees/ts-writer-resume-class-20261113 rev-parse 2d2e18eb937c6e7f9fd47e2dd11abf69be61f8b1:$f)" || echo MISMATCH $f
  done
  ```
  Expected blob shas:
  - `scripts/p184_writer_recreate.py` `26b23d0db7a81cb6c84a128e879b1998c06e52be`
  - `scripts/p184_writer_scope_reset.py` `95a893ee45e948c6bea095704932161623138d74`
  - `services/market_data/atomic_projection.py` `64e41a70b911195e178ed2128934a418a876ccae`
  - `services/market_data/cache_keys.py` `e1f6ac7b4e79f8f99fdd7ef9e5a7015cb193cbd2`
  - `domain/instrument_identity.py` `4715ca1ef523f788d42623b6944d5b4a024eeca6`
  - `domain/exceptions.py` `7ad1d202be8226c9402d24bb486d27ecaf02e6e2` (imported by `domain/instrument_identity.py`)
- Run the tools as `cd $W && python3 -B scripts/<tool>.py ...`. A MISMATCH aborts W.
- What `2d2e18eb` adds over `4af62754`, the image source, is operator tooling only. It does not change the image.
  - `--allow-stopped`
  - image-label hygiene
  - the TEST-only scoped verify
  - the rehearsal scoring
