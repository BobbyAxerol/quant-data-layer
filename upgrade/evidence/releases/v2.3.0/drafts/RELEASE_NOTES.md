# Quant Data Layer v2.3.0 (DRAFT)

Status: **DRAFT, not certified, not tagged.** Prepared 2026-11-13 on `release/v2.3.0-final-prep` (base: tip of
`release/v2.3.0-rc2-integration`, `e724712`). Every `TBD` is filled after the combined production window and the
R-cert 2 final verdict (list: [RELEASE_FILL_LIST.md](RELEASE_FILL_LIST.md)). When the certificate is PASS this file
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
- **SDK**: unchanged. `qdl-sdk 2.4.1`, no diff under `qdl_sdk/` since v2.2.5.

## Runtime (docker inspect after the window: TBD)

| Role | Image |
|---|---|
| rust_core x3, market_projector x2, stream_kn x2, ingestors x2 | `qdl-v2-rust:2.3.0-rc2-832c02c4` `sha256:58bdd28216139f0c1a40abeee3077834f1cf922cf6412e7fb1ce8845591a0512` (TBD confirm) |
| query_kn x2, binance_bar_edge | `qdl-v2-python:2.3.0-rc2-832c02c4` `sha256:03de529acc4f59ba2637f289aa196f544944f92fdcf719390cdf9aacbdfd441b` (TBD confirm) |
| kafka1-3, market_cache, stable_redis | unchanged (TBD digests) |
| TS writer | W-B2 `sha256:9dfef8a8a548be2ee2d800fb62c006c8d11fcbcf3c66999779f6c97e15a71a6f` (TS release, not part of this tag) |

The images are the rc2 images, unchanged, unless the R-cert 2 final verdict or the window forces an rc3. The linger
setting is config-only, so on its own it does not need an rc3.

## First production declaration (window 2026-11-13: TBD result)

The first production declaration is rendered by `scripts/dl_declaration.py` at tool `5a42ba01` and passes the offline
gate 18/18 (render out6, packet section 3).
- Revisions:
  - catalogue r13 -> r14, acquisition r21 -> r22, demand r9 -> r10, scope r12 -> r13, routing r30 -> r31;
  - `alpha.okx.paper.stable` r15 -> r16, accept-set floor 15.
- New bindings: 89 native candle1m, plus 318 DERIVED_FINAL (1h/12h for the 94 crossec swaps; 3m-1d for the 25
  covered).
- bar_edge moves to its mounted runtime pair. Checkpoint crossing r11/r19 -> r14/r22 with a migration record.

## Certification (TBD final)

| Item | Status |
|---|---|
| R-cert 1 offline derived-bar parity | **PASS**: 740,350 records compared, 0 mismatches; receipt `6814e25c` |
| R-cert 2 live shadow (>= 24 h + Monday 00:00Z) | TBD (interim 840/840 MATCH on rc1 images, 12:03Z 2026-11-13) |
| R-cert 3 fault matrix | TBD |
| R-cert 4 capacity envelope | TBD (window substitute: D0 median and bootstrap-end + 30 min, DL vCPU <= 6.0) |
| Window R-gates | TBD |
| Full CI on the release tree (CI6) | TBD, one run (CI plan in [CI_PLAN.md](CI_PLAN.md)) |

## Limits (carried and new)

- Single host, not HA.
- The IDR-128 supported-load envelope is still open.
- Live funding events and contract metadata are not bound. They are served as reference data through `FRESH_SNAPSHOT`
  (`qdl/reference/runtime.py:44-80`); a live funding binding is a v2.3.x candidate.
- Bootstrap depth is manifest-wide (`max_warmup_rows`); per-row depth is backlog item DL-BL-2026-11-13-C.
- Bootstrap and native gap repair share the OKX HISTORY_CANDLES admission lane at BATCH priority.
- 1W / 2D / 3D derived bars stay off until R-cert 2 passes a Monday boundary.

## Rollback

TBD from the window packet section 5: back to v2.2.6 stage-A r13 for query/stream/OKX ingestor, stage-A for
cores/projectors and v2.2.5 for the Binance ingestor/bar_edge. The pre-rollout bar-edge checkpoint backup restores
the v4 checkpoint, because the old image cannot read v5. A declaration rollback is a forward declaration.
