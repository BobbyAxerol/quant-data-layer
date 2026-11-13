# DL v2.3.0 rollout draft: declaration-only mechanism (slice 2) + derived bars (slice 1)

Status: **DRAFT, NOT APPROVED, NOT RUN.** Written 2026-11-13 from branch `feat/v2.3.0-declaration-mechanism`
(commits 1a `ccfe62a8`, 1b). The derived-bars slice (worktree `dl-v230-derived-bars`) and the declaration tool
(separate agent) are not merged yet. Image digests are unknown until the merged v2.3.0 tree is built. Every runtime
step needs owner approval of its exact blast radius and rollback (CLAUDE.md section 3).

## 1. Facts this plan depends on (read-only 2026-11-13: docker inspect and `--rm` reads)

| Role | Running image | Declaration it reads today |
|---|---|---|
| query_kn_1/2 | `753604f8` | `/runtime` catalog r12 / acquisition r20 (`releases/v2.2.0-02cd827/runtime`); 9 manifests from `/consumer-realms`, 3 from the image |
| stream_kn_1/2 | `27c79b53` | `/consumer-realms/bundle.json`, `jwt-config.json` |
| market_projector_1/2 | `60f83a51` | `/kn/bundle.json` |
| rust_core x3 | `7c848aad` | mounted core configs |
| ingestor_okx_swap, ingestor_binance_usdm | `27c79b53` | mounted ingestor configs; `QDL_KAFKA_REQUEST_TIMEOUT_SECONDS=30` set |
| binance_bar_edge | `08f356a8` | **image** `/app/config/v2`: catalog **r11** / acquisition **r19** |

- The live bar-edge checkpoint (`state/bar-edge/crypto-bar-checkpoint.json`) is schema **v4**: catalog 11,
  acquisition 19, 640 binding_ids, 635 watermarks, canonical_cache_id present.
- Offline classification of r11/r19 -> r12/r20 BAR bindings, using the runtime files and the certified loaders:
  640 -> 640, added 0, removed 0, changed 0 (scratch `v230/crossing_r11_r12.py`). Moving the bar edge to the mount is
  a pure legacy crossing. It needs a migration record with `previous_binding_digests` and no removed or changed set.
  All 635 watermarks are kept.
- The three image manifests Query reads (`monitoring-multivenue`, `alpha-vn-paper`, `reference-l2-stable`) are
  byte-identical to the repository copies (sha256 `304a610d...`, `be7331aa...`, `5b220aba...`). The realm dir does
  not contain them yet.

## 2. Images to build (from the merged v2.3.0 tree; record digests by `docker inspect`)

- **python** (query, bar_edge): declaration provenance, selectors, accept-set, checkpoint v5, plus derived bars.
- **rust ingestor**: sticky lanes, per-feed flush, producer knobs.
- **rust stream gateway**: accept-set.
- **rust projector**: bundle contract, plus proto `Bar.aggregation` (field 19, derived-bars slice).
- **rust core**: `qdl_derived_bar` raw kind -> `BAR_ORIGIN_AGGREGATED` (derived-bars slice).

## 3. Step 0: preflight (no runtime change)

1. Check the host measurement lock and other agents' windows. `python3 -B scripts/preflight_env.py` on the TS side
   only if the writer changes (it does not here).
2. Record the rollback set: current image id and compose env per role (table above).
3. Back up the bar-edge checkpoint (`crypto-bar-checkpoint.json` -> `.pre-v230`). The new image writes v5 and the old
   image cannot read v5, so rollback restores this backup.
4. Render declaration **r1 with no coverage change**: the current r12/r20 catalogue/acquisition, today's ingestor
   and core configs (unpinned, no flush map), the 12 manifests byte-identical, the bundles byte-identical,
   `declaration-index.json`, and the bar-edge migration record r11/r19 -> r12/r20 with previous digests from image
   `08f356a8`.
   - Gate: certified loaders, additivity, lane plan, index.
   - One variable: the mechanism.
5. Copy the three image manifests byte-identically into `QDL_CONSUMER_REALM_DIR`. This is an additive host-file
   change; the running roles do not read it.

## 4. Release window order (each role: recreate, health, then the next; per-role `up -d --no-deps` only)

1. **rust_core x3** with the new core image, by the approved simultaneous-replace method (rc22 runbook; never a
   rolling group change: incidents 2026-10-15). Config unchanged.
2. **market_projector_1/2**, one at a time. They must run the new contracts **before** the bar edge publishes any
   derived bar: an old projector drops field 19 and fails ENVELOPE_NOT_CANONICAL (derived-bars agent).
3. **stream_kn_1/2**, one at a time. Bundle unchanged, so existing tokens stay valid by exact match.
4. **query_kn_1/2**, one at a time, with the new python image and the compose change (3 manifests from
   `/consumer-realms`).
   - Verify `/health/dependencies` `declaration` shows `image_files=0`.
   - Run the DR02/DR03 byte compare as in v2.2.5.
5. **ingestor_okx_swap, ingestor_binance_usdm**, one at a time, with the new image and config unchanged.
   - Verify `started` reports `lane_plan: POSITIONAL`, `producer.linger_ms: null`, `request_timeout_seconds: 30`.
   - Lane names in the session-liveness dir must be unchanged.
6. **binance_bar_edge** last, with the new python image, `QDL_STABLE_SOURCE_BINDINGS` and
   `QDL_STABLE_ACQUISITION_BINDINGS` set to `/runtime/...`, and `QDL_STABLE_BAR_CHECKPOINT_MIGRATION` set to the
   record.
   - Verify the log line `checkpoint crosses catalog 11->12 acquisition 19->20 crossed=True retained=640 added=0
     dropped=0 migration_record=True`.
   - The heartbeat carries `declaration` with `image_files=0`.
   - The persisted checkpoint is v5.
   - The record env may be removed at the next restart; a matched record is never reused.

**Not touched:** kafka1-3, market_cache, stable_redis (ephemeral, CLAUDE.md 4.4), the TS writer and every TS service,
monitor, Portal. No manifest or JWT revision changes in this window.

**Rollback per role:** previous image digest + previous compose env.
- bar_edge: also stop it, restore `crypto-bar-checkpoint.json.pre-v230`, start the old image. It re-reads its image
  r11/r19 and the backup matches. Watermarks are older; bounded catch-up applies.
- stream/query: tokens are exact-revision, so no rollback hazard.

## 5. After the window: declarations only (each one gate + ordered restart + owner approval)

- **r2:** MARK/INDEX 500 ms (`latest_state_flush_ms_by_feed`) and lane pins equal to today's assignment.
  Restart the OKX ingestor; lane names unchanged by construction. Measure records/s and vCPU (one variable).
- **r3:** derived intervals for the covered instruments (`DERIVED_FINAL`), after R-cert 1-4.
- **Stage B/C coverage:** per the study §7 gates (disk headroom first).
- Restart order for a declaration: ingestor -> cores -> projector -> stream -> query -> bar_edge.
  Only the roles whose files changed restart.

## 6. What still needs a release after v2.3.0

New venue/market, new feed type, new derivation kind, parser/normaliser/proto change, flush or lane *semantics*,
exceeding the certified capacity envelope. Removals and source-mode changes are a declaration with an explicit
migration record, approved separately.
