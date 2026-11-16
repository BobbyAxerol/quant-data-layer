# DL runtime record, 2026-11-16: W-A1 permanent and stage B r7 APPLIED (release tree = runtime)

Purpose: the deployed KN stack (`qdl_v2_stable_candidate`) now differs from DL `dev` 7d4333e (tree = main f2ea1d7,
tag v2.3.0) in two owner-approved ways. This record puts both in the repo with their receipts, so the release tree
names what runs. It is a record of applied runtime changes, not a release and not a certificate.

Recorded read-only at about 16:10Z on 2026-11-16 (`docker inspect`, `sha256sum` of the deployed files). No runtime
action was taken to write it.

## 1. Applied changes

| # | Change | Applied (Z) | Receipt | Status |
|---|---|---|---|---|
| 1 | **W-A1 permanent:** realtime core `batch_wait_ms` 25 -> 50 on rust_core x3 (Phase 0 P0-DL1) | 11:14:32-11:16:22 | `receipts/wa1-perm-20261116T1110Z.RECEIPT.md` | APPLIED, PASS. DL median 6.085 -> 4.958 vCPU, raw-age window-max p99 177.2 -> 131.3 ms |
| 2 | **Stage B declaration r7:** execution QUOTE + MARK_INDEX_PRICE for 71 crossec OKX swaps (catalogue r15, acquisition r23, demand r11, scope r14, routing r32, TS manifest r14, binding r26) | 12:24:51-12:38:02 | `receipts/stage-b-window-20261116T1200Z.RECEIPT.md` | APPLIED, kept under the owner's CPU condition (G6 failed against 6.5; all other gates PASS) |
| - | W-CAP65 (TS writer env `DATA_LAYER_V2_MARK_BATCH_WINDOW_MS` 20 -> 100, no DL change) | 14:06-15:13:39 | `receipts/w-cap65-window-20261116T1405Z.RECEIPT.md` | FAILED (writer requests 3,000-6,100/min vs gate 3,500), ROLLED BACK |

## 2. What runs (read-only inventory, about 16:05Z)

Images (unchanged by both changes):
- rust_core x3, market_projector x2, stream_kn x2, ingestor_okx_swap, ingestor_binance_usdm:
  `sha256:58bdd28216139f0c1a40abeee3077834f1cf922cf6412e7fb1ce8845591a0512`.
- query_kn x2: `sha256:a235c0a18cdeb112ea0b49627eaf02f2a68ab878298ebba07884461ee5709d0b`.
- All running, restart count 0. Started: cores 12:24:52, projectors 12:27:54 / 12:28:23, stream 12:29:14 / 12:30:20,
  Query 12:30:40 / 12:31:22, OKX ingestor 12:33:26. Binance ingestor untouched since 2026-11-13 14:51:12.

Compose and env (labels of the running cores, `com.docker.compose.project.environment_file`, in this order):
1. `releases/v2.3.0-rc2-compose/qdl-v2-stable-kn.v2.3.0-rc3.env` sha256 `949cf3f3e69e3e62e70581fd14960f64242ec8a5b26694c3082294351d8fdd05`.
   Line 15 `QDL_CORE_CONFIG_DIR=.../v2.3.0-rc3-a1-bw50-cores` (the W-A1 persistence; backup `.pre-wa1` in the W-A1 receipt dir).
2. `releases/v2.3.0-stage-b-compose/stage-b.env` sha256 `c2b3c2ca671405430fdb67cfc9bd578fe602a7840e9e4e5ff5616d9c13b81ba1`,
   5 keys, later in the list so it wins:
   `QDL_KN_RELEASE_DIR=.../v2.3.0-stage-b`, `QDL_CONSUMER_REALM_DIR=.../v2.3.0-stage-b-realm`,
   `QDL_PAPER_REALM_MANIFEST=trading-system.paper.stable-r14.yaml`, `QDL_CORE_CONFIG_DIR=.../v2.3.0-stage-b-cores`,
   `QDL_INGESTOR_CONFIG_DIR=.../v2.3.0-stage-b-ingestor`.
3. TLS generation env and the KN secrets env (not read, not recorded).
- Compose file `releases/v2.3.0-rc2-compose/qdl-v2-stable-kn.yml` sha256 `5b1381e062d97a7a8aef460c0cabfeeb5ae572f1cba0b3d47f36aff94cbf1e62`.

So the effective core config is `v2.3.0-stage-b-cores`, not the line-15 bw50 dir. Both carry `batch_wait_ms: 50`.

## 3. Deployed files vs this branch (byte equality, `sha256sum`)

| Deployed file | Repo file (`declaration-stage-b-r7/`) | Equal |
|---|---|---|
| `v2.3.0-stage-b-cores/rust_core.json` 8fb1dc96... | `cores-bw50/rust_core.json` | yes |
| `v2.3.0-stage-b-cores/rust_core_2.json` af533769... | `cores-bw50/rust_core_2.json` | yes |
| `v2.3.0-stage-b-cores/rust_core_3.json` 383c34cd... | `cores-bw50/rust_core_3.json` | yes |
| `v2.3.0-stage-b-realm/trading-system.paper.stable-r14.yaml` fc51bbf2... | `consumer-realms/trading-system.paper.stable-r14.yaml` | yes |
| `v2.3.0-stage-b-ts/trading-system.paper.stable-r26.json` 7f5394b9... (writer mount) | `bindings/trading-system.paper.stable-r26.json` | yes |
| `v2.3.0-stage-b-ingestor/ingestor_okx_swap.json` | `ingestor/ingestor-okx-swap.json` 14b28e29... | not checked directly: the deployed dir is root 0750, not readable by this user. The pinned set (`releases/stage-b-r7-20261116/pinned/SHA256SUMS`) lists 14b28e29 = repo, and the started log (stage B `s6-started.json`) shows 465 bindings and flush `{MARK_INDEX: 500, QUOTE: 50}` as rendered |

The repo `cores/` (bw25) files are the generator output. They are **not** what runs (section 5, B1).

## 4. Owner CPU decision (2026-11-16)

- Stage B G6: DL median 6.565 vCPU against the 6.5 ceiling (D3). Owner: keep stage B, temporary ceiling 7.0, 24 h to
  return to <= 6.5.
- W-CAP65 (the writer-side lever) failed its request gate and was rolled back at 15:13:39Z. At 15:08Z DL was
  7.09-7.23 with every role up (Binance 0.10 -> 0.20 too): US-hours market volume, not stage B alone.
- Owner at about 15:10Z: **keep stage B, ceiling 7.5 temporarily during US hours**, roll back stage B if DL goes above
  7.5. The root capacity fix moves to **DL v2.3.1** (cut unused Binance BOOK/TRADE feeds) together with **code-level
  read coalescing in the TS writer** (TS branch `feat/v213-writer-mark-read-coalescing`).

## 5. v2.3.1 backlog created by this record

- **B1 `batch_wait_ms` as a declared value.** `qdl/runtime/stable_deployment.py:1145` hard-codes 25 in `core_config`
  and the core refresher refuses a changed non-binding field (`scripts/refresh_v2_core_runtime_bindings.py:73-75`).
  Runtime is 50 since W-A1, so every render must be patched by hand (stage B did this; verified equal otherwise).
  Fix: a declared core value (declaration -> render -> refresher accepts a declared change), default 50; test: a
  render on the live base is byte-equal to the deployed `v2.3.0-stage-b-cores/*.json`.
- **B2 Binance BOOK/TRADE retirement slice.** No removal path in v2.3.x (refreshers and gate refuse removed rows). Needs
  a declaration `retirements[]` record, gate proof that no manifest still grants a retired row, the floor-reset list
  from `manifest_compat.next_min_token_revision`, a real-reader census first, and same-window JWT bumps (7-8 consumers,
  census `declaration-stage-b-r7/offline/binance-census.json`). Saving at most 0.20-0.26 vCPU (W-CAP65 packet §4).
- **B3 per-instrument flush.** `latest_state_flush_ms_by_feed` is per ingestor file and per feed, so stage B moved
  the retained OKX MARK lanes to 500 ms as well. Fix: a per-binding (or per-lane) override.
- **B4 (proposed, needs owner) batched QUOTE probe read.** Measured 15:56-16:00Z: the TS writer's native QUOTE probes
  are 2,958-3,319 requests/min, 57-61 % of the `trading-system.paper.stable` identity's 5,211-5,815/min (heartbeat
  `native_probe` counters vs the DL quota minute key; TS journal W-MRT). Query has only `reference:batch` for MARK, so a
  per-route QUOTE probe cannot be coalesced on the TS side. A `market-data/snapshot:batch` (or a per-session liveness
  frame on Stream) would let the native writer send one probe call per tick.
- **B5 env layering.** The base env still names the bw50 core dir on line 15 while `stage-b.env` overrides it. A
  rollback must drop `stage-b.env` from the env-file list first. v2.3.1 should render one versioned env per release.

## 6. Files

- `RUNTIME_RECORD.md` (this file), `SHA256SUMS`.
- `receipts/`: copies of the three receipts named in section 1 (no secrets; checked with a keyword scan).
