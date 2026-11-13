# Quant Data Layer v2.3.0 (DRAFT)

Status: **DRAFT, not certified, not tagged.** Prepared 2026-11-13 on `release/v2.3.0-final-prep`, rebased onto the rc3 integration
tip `3d8ccd7` (rc3 code `7cd4fc36`, window A packet `60a97a18`, run-sheet rule `7e12b08`). The tree carries the
deployed compose and the out6 configuration (see "Release tree = runtime"). Every `TBD` is filled after the rc3
window A receipt and the R-cert 2 final verdict (list: [RELEASE_FILL_LIST.md](RELEASE_FILL_LIST.md)). When the certificate is PASS this file
moves to `upgrade/evidence/releases/v2.3.0/RELEASE_NOTES.md`, which the tag workflow requires (`release.yml`).

v2.3.0 makes coverage a **declaration**: after this release, adding instruments or intervals is "render a declaration,
pass the offline gate, ordered restart". It needs no certification campaign, no image and no tag. It also adds DL-side
derived bars, so higher intervals are aggregated from final 1m bars inside the standard DL path.

## Contents (since v2.2.5)

- **Derived bars (slice 1).** New acquisition mode `DERIVED_FINAL` with `derived_from {interval}`.
  - The bar edge builds a final bar from the finer final bars in the KN market cache.
  - The Rust core is still the only canonical writer: it recomputes the aggregate and emits `BAR_ORIGIN_AGGREGATED`.
  - Grouping is anchor-aware (`qdl.adapters.intervals.bar_bucket_open_ms`). OKX zero-volume minutes take OHLC only
    from traded constituents.
  - R-cert 1 (offline parity): PASS (see Certification).
  - Live bars are derived; history is bootstrapped from the venue's native interval.
- **Declaration mechanism (slices 2a / 2b).**
  - Consumer-manifest accept-set (`metadata.min_token_revision`, rule `qdl.consumer.manifest_compat`). An additive
    manifest no longer forces a same-window JWT bump.
  - Universe selectors for ALPHA/RESEARCH reads.
  - Effective-declaration provenance with opt-in guards (mounted-only, declaration index).
  - Bar-edge checkpoint v5 with per-binding series digests. An additive revision crossing keeps watermarks; a removal
    or a mode change needs an exact migration record.
  - Sticky ingestor lanes (`lane_shard`) and per-feed latest-state flush (`latest_state_flush_ms_by_feed`).
  - Mounted bar-edge and Query declarations in compose.
  - Producer knobs `QDL_KAFKA_PRODUCER_LINGER_MS` / `QDL_KAFKA_REQUEST_TIMEOUT_SECONDS`, shipped unset.
- **Declaration tool (slice 2c)**, `scripts/dl_declaration.py` with three commands:
  - `render`: declaration plus the TS / alpha adapters, producing one coupled, additive revision set with typed
    BLOCKED rows;
  - `gate`: offline checks of loaders, additivity, lanes, lineage, bar-edge crossing, capacity, OKX REST budget and
    quota model;
  - `checkpoint-migration`.
  - EXECUTION quotas come from `qdl.consumer.quota_model`.
  - A declarable final-BAR source (`bar_source` / `bar_1m_source`).
  - A bar-edge runtime file pair that keeps today's raw lineage.
  - The owner CPU cap of 6.0 vCPU, OKX before Binance.
- **Contract lineage (slice 2d).**
  - Per-binding catalogue lineage (`lineage_revision`) and config lineage (`lineage_config_revision`), seeded from the
    live ingestor stamps.
  - Per-binding cursor lineage and the contract-line `schema_digest`; the value is unchanged, line 1 `100966435a30367d...`.
- **v2.2.6 stage A configuration** (QUOTE + MARK for 20 more OKX linear swaps, catalogue r13, TS Paper r13 with
  model quotas 10,000 rpm / 160 streams): carried in this tree (`c29c216`).
- **v2.2.7 (ingestor producer linger hotfix): superseded by v2.3.0, not released.** Branch
  `release/v2.2.7-ingestor-linger` (`13f20d0`) never rolled and is not tagged. Its knob
  (`QDL_KAFKA_PRODUCER_LINGER_MS`, honoured `QDL_KAFKA_REQUEST_TIMEOUT_SECONDS`) is re-implemented in v2.3.0 slices
  2a/2b (coordinator decision 2026-11-13).
- **SDK 2.5.0** (additive, no SDK source change). The wheel bundles the generated contract, which gains
  `Bar.aggregation = 19` (`BarAggregation`). Its bytes therefore differ from the v2.2.5 asset `qdl_sdk-2.4.1`, so the
  version is bumped rather than republishing 2.4.1 with other bytes (release decision 2026-11-13, overnight package,
  recommended option). 2.4.1 keeps working against v2.3.0; older clients ignore field 19.

## Runtime (docker inspect after the window: TBD)

| Role | Image |
|---|---|
| rust_core x3, market_projector x2, stream_kn x2, ingestors x2 | `qdl-v2-rust:2.3.0-rc2-832c02c4` `sha256:58bdd28216139f0c1a40abeee3077834f1cf922cf6412e7fb1ce8845591a0512` (docker inspect 2026-11-13 ~16:10Z; reconfirm at fill) |
| query_kn x2, binance_bar_edge | `qdl-v2-python:2.3.0-rc3-7cd4fc36` `sha256:a235c0a18cdeb112ea0b49627eaf02f2a68ab878298ebba07884461ee5709d0b` (docker inspect 2026-11-13 ~16:10Z; reconfirm at fill) |
| kafka1-3, market_cache, stable_redis | unchanged (TBD digests) |
| TS writer | W-B2 `sha256:9dfef8a8a548be2ee2d800fb62c006c8d11fcbcf3c66999779f6c97e15a71a6f` (TS release, not part of this tag) |

v2.3.0 ships the rc3 runtime: the rc2 Rust image and the rc3 Python image (session liveness accepts a pinned event
revision at or below its session's; Rust diff against `832c02c4` is empty). The rc2 Python image `03de529a` is not
part of the release; it failed W1 of the 2026-11-13 combined window and was rolled back.
- CPU optimisations and live funding go into v2.3.1, so these digests do not change. The CPU optimisations are:
  projector commit coalescing, fetch batching, pipelined Redis reads, stream ListOffsets.
- Producer linger and broker fetch settings are runtime configuration only. They are recorded here only if they pass
  their measurement arms: TBD.

## Release tree = runtime

The release tree reproduces the deployed runtime (pre-flight F2, 2026-11-13):

- `deploy/compose/qdl-v2-stable-kn.yml` is byte-equal to the deployed compose
  `~/.local/state/qdl-v2/releases/v2.3.0-rc2-compose/qdl-v2-stable-kn.yml` (sha256 `5b1381e062d97a7a...`): alpha.okx
  r16 in both Query manifest lists, the bar_edge runtime pair, core backpressure stage 2000 and
  `QDL_STABLE_BAR_CHECKPOINT_MIGRATION`.
- `deploy/compose/qdl-v2-stable-kn.v2.3.0.env` is byte-equal to the deployed rc3 env (sha256 `2b1b4b8db699721f...`; image
  digests and host paths only, no secrets).
- The live declaration is carried as evidence in `upgrade/evidence/releases/v2.3.0/declaration-out6/` (with `SHA256SUMS`), byte-identical to render out6:
  - the repo-form set `repo/`: acquisition r22 with all 1,163 rows pinned (722 at 21, 407 at 22, 34 at 19),
    catalogue r14, demand r10, scope r13, routing r31, two universes, and `alpha-okx-paper.yaml` r16;
  - the mounted renders `runtime/`, `cores/`, `ingestor/`, `consumer-realms/`;
  - `render-inputs.json` and `report.json`.
  All six consumer manifests are equal as documents to the mounted realm files.
- It is not carried as `config/v2` / `consumers/stable`: those stay at the pre-declaration r21/r13 baseline that
  the test suite and the repo-default loaders use. With the out6 set in `config/v2`, 110 of 1,270 impacted tests
  fail (18 failures, 92 errors). The causes: selector manifests loaded without universe context (54);
  DERIVED_FINAL rows on SQLite readback (35); goldens pinned to r21/r13 (about 20). Making those loaders and tests
  selector- and DERIVED_FINAL-aware is a v2.3.1 slice.
- What stays different, and why:
  - the mounted Query/bar_edge documents (`runtime/*.yaml`) are rendered from the live runtime base, not from
    the repo form. They carry the live config-lineage stamps (H2, e.g. 19 on rows the repo form pins at 21), the live
    `:8443` OKX URLs on 110 rows, and lack `l2.materialized_snapshot_interval_ms` on 2 rows. They are recorded by
    sha256 in packet section 3 (acquisition `81d001aa`, bar-edge pair `9154bf1a` / `ba7bd0f4`, migration `cd15ee73`,
    kn bundle `924951cd`, ingestors `4cb59e29` / `021dd5dc`), and the copies are in `upgrade/evidence/releases/v2.3.0/declaration-out6/`.
  - `v2.3.0-rc2` and `v2.3.0-rc2-ingestor` are root-only (`0750 root:10001`), so the files there were not read back
    byte for byte without sudo. The cores (`rust_core*.json`) and the realm (`alpha.okx.paper.stable-r16.yaml`,
    `bundle.json`) were read back and are byte-equal to out6.
  - The Python image `a235c0a1` was built from a git archive of `7cd4fc36` (`COPY . /app`). A rebuild from this
    tree differs by docs and the files above, so the release ships the certified digests rather than a rebuild.

## First production declaration (window A 2026-11-13 14:44-17:31Z: GREEN)

The first production declaration is rendered by `scripts/dl_declaration.py` at tool `5a42ba01` and passes the offline
gate 18/18 (render out6, packet section 3).
- Revisions:
  - catalogue r13 -> r14, acquisition r21 -> r22, demand r9 -> r10, scope r12 -> r13, routing r30 -> r31;
  - `alpha.okx.paper.stable` r15 -> r16, accept-set floor 15.
- New bindings: 89 native candle1m, plus 318 DERIVED_FINAL (1h/12h for the 94 crossec swaps; 3m-1d for the 25
  covered).
- bar_edge moves to its mounted runtime pair. Checkpoint crossing r11/r19 -> r14/r22 with a migration record.
- **Result (window A, receipt `window-A-20261113T1444Z/`).**
  - Crossing: retained 640, added 407, dropped 0, migration_record=True.
  - Bootstrap: 14:52:47-17:00:54Z, about 2 h 08 min against the 72.7 min estimate. The core backpressure stage of
    2000 paused history repeatedly under host load.
  - Bootstrap outcome: 1047/1047 published, 681 provider-short. 5 bindings (Binance 3d BNB/BTC/DOGE/ETH/SOL) are
    suspended with a recent suffix (OVERLAP, daily retry, as before rc3).
  - 584 retained provider-short products re-bootstrapped at restore, as expected: defect DL-BL-2026-11-13-D,
    v2.3.1.
  - D6b at bootstrap end + 15 min: 0 derived gaps recorded, healed or kept; 76 derived ACK lines. The + 6 h read is
    at 23:01Z: FILL.

## Certification (TBD final)

| Item | Status |
|---|---|
| R-cert 1 offline derived-bar parity | **PASS**: 740,350 records compared, 0 mismatches; receipt `6814e25c` |
| R-cert 2 live shadow (>= 24 h + Monday 00:00Z) | TBD. Run 2 is a weekend run on the rc3 images (namespace `qdl-v230-rcert2-20261113t1551z`, started 2026-11-13 15:51Z). Run 1 interim 840/840 MATCH on rc1 images (12:03Z) does not count for the final verdict |
| R-cert 3 fault matrix | TBD |
| R-cert 4 capacity envelope | Not run. Window substitute, owner delta gate: median at bootstrap end + 30 min <= D0 median + 0.3. Window A: D0 6.97, gate median **6.54** over 11 samples, PASS. DL was above the 6.0 cap before the window (Kafka F1 leader balancing and stage A; the lever is the linger work) |
| Window R-gates | rc2 combined window 2026-11-13: D1-D6 PASS, W1 VERIFY_FAILED, rolled back (receipt `window-20261113T1237Z/`). rc3 window A (DL only, writer 7332e85a): **GREEN**. D1-D6, D5a (the W1 defect class) PASS on both Query instances, writer W1/W2 VERIFIED (46.7 s / 47.9 s), 10 min soak, D6b + 15 min and CPU gate PASS; Paper outage about 12 min (receipt `window-A-20261113T1444Z/`) |
| Full CI on the release tree (CI6) | TBD, one run (CI plan in [CI_PLAN.md](CI_PLAN.md)) |

## Limits (carried and new)

- Single host, not HA.
- A broker leadership move can restart a projector once, about 45 s, exactly-once kept (IDR-285, fix in v2.3.1).
- The IDR-128 supported-load envelope is still open.
- Live funding events and contract metadata are not bound. They are served as reference data through `FRESH_SNAPSHOT`
  (`qdl/reference/runtime.py:44-80`); the live OKX funding binding is in v2.3.1.
- Session acceptance on the successor path (DL-BL-2026-11-13-E) is hardened in v2.3.1; v2.3.0 window A changes no
  retained binding, so the gap is not exposed by this release's declaration.
- Bootstrap depth is manifest-wide (`max_warmup_rows`); per-row depth is backlog item DL-BL-2026-11-13-C.
- Bootstrap and native gap repair share the OKX HISTORY_CANDLES admission lane at BATCH priority.
- 1W / 2D / 3D derived bars stay off until a later declaration's R-cert run crosses their boundaries (Monday 00:00Z). v2.3.0 releases on the 2026-11-13 R-cert 2 run-2 certificate (owner, 2026-11-13: do not wait for Monday).

## Rollback

From the window packet section 5, as exercised on 2026-11-13 13:00-13:05Z (rc2 rollback, receipt
`window-20261113T1237Z/`): back to v2.2.6 stage-A r13 for query/stream/OKX ingestor, stage-A for cores/projectors
and v2.2.5 for the Binance ingestor/bar_edge. Capture logs and inspect before every recreate. The writer is
restored BOOTSTRAP then STRICT, never with `docker start`. The pre-rollout bar-edge checkpoint backup restores
the v4 checkpoint, because the old image cannot read v5. A declaration rollback is a forward declaration.

**Projector rollback constraint (amendments 4' / 6', RCA 2026-11-13, R-cert 2 S5).** Window A's D6 published
DERIVED_FINAL bars (`BAR_ORIGIN_AGGREGATED`, `Bar.aggregation`) on `md.bars.v2`. The rollback-target projector
`60f83a51` (= `qdl-v2-rust:2.2.4-c7ce7d3`) refuses them (`STATE_CODEC:ENVELOPE_NOT_CANONICAL`, c7ce7d3
`stage_b.rs:1240-1246`; S5 REFUSED 20:40:40Z). A restart resumes at its Redis checkpoint before the derived record,
so it crash-loops. Therefore:
- **4'.** Projectors are not rolled back while any derived record exists on any `md.bars.v2` partition (offset guard,
  packet section 5 item 7). `market_projector_1/2` stay on rc3 `58bdd282` with their rc3 bundle and catalogue; only
  Query, stream and cores roll back, in that order, then bar_edge and the ingestors. The rc3 projector reads both
  native and derived bars. Rolling back to `60f83a51` is allowed only when no derived record remains (D6 never
  published, or retention has expired past the last derived offset).
- **6'.** The old projector is never put in front of a bars topic holding derived records. No offset reset or seek of
  projector groups.
- **Rollback receipt gate:** after the rollback, both projectors' `errors.stage_b` contain no `ENVELOPE_NOT_CANONICAL`,
  `RestartCount` is unchanged for 10 min, and stage-B `bars_applied` rises.
- Open: the rc3 projector with the rolled-back r13 consumer bundle was not tested; 4' avoids that combination.
- Dated amendment: `drafts/COMBINED_WINDOW_V230_WB2_PACKET.md`, "Amendment 2026-11-13 (after window A)".
