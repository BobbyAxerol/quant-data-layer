# Quant Data Layer v2.4.0

Status: **runtime accepted 2026-11-19; certificate PASS WITH NOTED LIMITS (`CERTIFICATE.md`); tag pending the CI6
receipt.**
- Release branch: `release/v2.4.0-acceptance` (worktree `/home/bobby/.worktrees/dl-v240-release`) from `dev` 7230d5c.
- Rollout and RCA: `drafts/ROLLOUT_PLAN.md` (sections 7-8), window gates `drafts/window/`.
- Runtime receipts: `runtime-windows-20261119/`.

## Images

| Role | Image |
|---|---|
| stream_kn x2 | `qdl-v2-rust@sha256:abafab2c98cbdd9d2fe86f2b26fd6fcf64800ccdd114a3dd5b92803de16a3e98` (`qdl-v2-rust:2.4.0-c17472c`) |
| query_kn x2, binance_bar_edge | `qdl-v2-python@sha256:18ada571c72856cefc2844c8cb2c3809c3e30ac77927bc4c73160e7b2787cd18` (`qdl-v2-python:2.4.0-acf42d7`) |
| rust_core x3, market_projector x2, ingestors | unchanged `58bdd282` (v2.3.0-rc2 binaries; cores/projectors on the stage C r8 config) |

## Contents (since v2.3.1)

1. **v2.3.2 content** (SessionAlive frames on the Stream, opt-in `DELIVERY_ON_CHANGE` on the 102 ON_CHANGE QUOTE
   bindings, ingestor session-liveness writes 250 ms, R0 release recipe with security gates).
2. **Shared-read capacity (S1-S8):** verified-token cache; advisory token revision (readers accept 1..manifest
   revision, so a consumer JWT never has to move in the same window as a manifest); quota charged only on admission
   (Rust refund guard); direct MARK/INDEX view call; shared latest view with singleflight; session-fair warmup queue
   and hot lanes, waits capped below client timeouts; per-replica token buckets (Redis optional, TS full rate on each
   replica); role claims ignored (manifest permissions gate). SDK 2.8.0 `session_id`.
3. **A-FUND:** FUNDING_RATE reference reads for the TS Paper writer on 102 perpetuals; historical 1m MARK/INDEX
   candles (bounded window, ALPHA grade, OKX SWAP 97); TS manifest r15, routing r34; SDK 2.9.0.
4. **Hot-read authority fallback (window 1 RCA):** Query tries the next Stream replica on a replica-scoped
   `HOT_AUTHORITY_MISMATCH`/`HOT_AUTHORITY_CHANGED`; each replica still enforces its exact match.
5. **Stage C completion on the v2.4.0 images:** Binance USDM signal data for crossec_ema (96 instruments, BAR 1m
   native, 1h/12h DERIVED_FINAL, FUNDING_RATE), alpha.okx r17, catalogue r16, acquisition r24, Binance ingestor rev 24.
6. **Release tooling:** AGENTS 34b correctness-class recipe, `--cpu-shares 256`, upgraded rust runtime base `b33ec0cb`;
   read-only window gate script and hot-read / session-liveness probes.

Interfaces: additive only (no proto change; SDK 2.7.0 → 2.9.0 minor versions; new optional request fields).

## Known limits

See `CERTIFICATE.md` section 7: shared100 (A6) is post-release acceptance; no prod TEST-consumer probe tool yet;
bar_edge re-proves provider-short history on every restart (fixed in v2.4.1).

## Release steps (coordinator runs them; nothing here was pushed, merged or tagged)

1. Push `release/v2.4.0-acceptance`, open the PR into `dev` (docs/evidence only: scoped CI), merge locally with
   `git merge --no-ff` (G7), push `dev`. The `dev` push run is a full run (CI5): it is the **CI6 receipt** on tree
   T = `git rev-parse dev^{tree}`. If only a known flaky job fails (`unit-tests (n)`), rerun that job once (CI7).
2. `dev` → `main` locally `--no-ff`; check `git rev-parse HEAD^{tree}` == T, so the `main` push inherits the run (CI1).
3. Annotated tag `v2.4.0` on that main merge commit, push the tag, then the GitHub release with the body below.
