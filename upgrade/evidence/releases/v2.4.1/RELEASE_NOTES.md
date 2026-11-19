# Quant Data Layer v2.4.1

Status: **runtime accepted 2026-11-19 (windows A, B, C); certificate PASS WITH NOTED LIMITS (`CERTIFICATE.md`);
code receipt run 44516122456 on tree `39690471…`; the tag receipt is the dev push run after this branch merges.**

## Images

| Role | Image |
|---|---|
| query_kn x2, binance_bar_edge | `qdl-v2-python@sha256:929eeac2091ecbe916cf1f19fba9f13056e621a1c2c1947427b97a8a7c002082` (`qdl-v2-python:2.4.1-985bd65`) |
| stream_kn x2 | unchanged `qdl-v2-rust@sha256:abafab2c98cbdd9d2fe86f2b26fd6fcf64800ccdd114a3dd5b92803de16a3e98` |
| cores, projectors, ingestors | unchanged `58bdd282` |

Runtime config: `releases/v2.4.1-compose/` (compose `7629a9a2…`, overlay b `2505d50b…`) and
`releases/v2.4.1-canonical/` (metadata `03065435…`, document sha256 `cacea7bb…`).

## Contents (since v2.4.0)

1. **Canonical instrument metadata v1.** Prices, quantities and bars stay venue-native.
   - A separate versioned runtime file states each instrument's underlying asset, price and quantity multipliers, and
     cross-venue aliases. Examples: Binance `1000PEPE`/`1000BONK`/`1000FLOKI`/`1000SHIB` are x1000; Binance `LUNA2` is
     OKX `LUNA`; OKX contract values come from ctVal x ctMult.
   - Every rule cites venue documentation. An unverifiable rule, or a cross-venue price disagreement, yields
     UNVERIFIED with no multipliers.
   - Served additively as `canonical` on `/v2/instruments`; proto `Instrument.canonical = 24`.
   - The file does not move the catalogue revision.
2. **SDK 2.10.0** `qdl_sdk.instruments`: exact Decimal helpers that refuse missing or UNVERIFIED metadata (typed
   `CanonicalMetadataUnavailable`), never 1:1.
3. **bar_edge**:
   - provider-short proofs persist in a sidecar next to the checkpoint, so a restart no longer re-proves every short
     binding;
   - a not-ready readback is a quiet retry.
4. **Runbook**: the bar_edge silent restore and its rollback triggers (`docs/runbooks/kn-maintenance-window-template.md`).

Interfaces: additive only.

## Release steps (coordinator)

1. PR `release/v2.4.1-acceptance` into `dev`, docs-only (scoped CI), then a local `--no-ff` merge.
2. Push `dev`. The `dev` push full run is the receipt on the final tree.
3. Merge `dev` into `main` locally (`--no-ff`) and check TREE_EQUAL.
4. Push the tag `v2.4.1` and create the GitHub release.
