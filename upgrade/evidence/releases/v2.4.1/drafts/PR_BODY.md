## Summary

DL v2.4.1 (`feat/dl-v241-canonical-instrument-metadata` -> `dev`, rebased on 2b3aa3e). Two owner-approved slices:

1. **Canonical instrument metadata v1** (`qdl.instrument.canonical-metadata.v1`). DL keeps every price, quantity and
   bar native and states, per instrument, how one venue unit relates to its underlying asset: `underlying_asset`,
   `underlying_instrument_uid`, `price_multiplier` (Binance `1000PEPEUSDT` = 1000), `quantity_multiplier` (OKX
   `PEPE-USDT-SWAP` = 10,000,000), cross-venue `aliases` (Binance `LUNA2USDT` = OKX `LUNA-USDT-SWAP`), `status`
   VERIFIED/UNVERIFIED and the `source` (endpoint, venue doc URL, capture time and sha256).
   - Rules with venue documentation quotes: `config/v2/canonical-underlying-rules.yaml`; unverifiable rules and
     cross-venue price disagreements fail closed (UNVERIFIED, no multipliers).
   - Tool `scripts/canonical_instrument_metadata.py` (capture / render / gate); a separate versioned runtime file, so
     `catalog_revision` (the Query/Stream hot-read authority) never moves.
   - Query serves it additively as `canonical` on `/v2/instruments` only when `QDL_CANONICAL_INSTRUMENT_METADATA`
     names the file (fail closed at start when malformed); OpenAPI additions only; proto `Instrument.canonical = 24`.
   - SDK 2.10.0 `qdl_sdk.instruments`: exact Decimal helpers that raise `CanonicalMetadataUnavailable` and never
     assume 1:1.
2. **S6 bar_edge**: provider-short proofs persist in a sidecar next to the checkpoint and bound the restore gap check
   (window 4 re-proved 680 short bindings / 5,575,631 pre-listing opens on one restart); checkpoint schema unchanged;
   a not-ready readback is a WARNING retry instead of an ERROR traceback.
3. Release recipe: RELEASE 2.4.1, SDK 2.10.0, python production reference `2.4.0-acf42d7`.

Interfaces: additive only (new optional REST field, proto field 24, SDK minor; `buf breaking` vs the frozen
baselines and the dev base PASS; `schema_line check` PASS, line digest unchanged).

## Validation

- Live render on the r16 catalogue (capture 2026-11-19T08:40:58Z, 264 files): 527 instruments, gate PASS; Binance
  USDM 203 VERIFIED / 58 UNVERIFIED, OKX SWAP 255 / 3; crossec 96 all VERIFIED; QNT fails the cross-venue price
  check (ratio 4.60) and is UNVERIFIED on both venues.
- Python: new tests 24 (canonical metadata 16, short proofs 8); impacted set 12 modules Ran 198 OK in
  `qdl-v2-python:2.4.0-acf42d7` (host slot, read-only tree); `tests.test_build_qdl_release_images` 46 OK.
- Proto/Rust: buf format/lint/breaking PASS; `buf generate` changes only the instrument pb2/rs;
  `cargo check -p qdl-contracts -p qdl-stream-gateway` OK.
- Image `qdl-v2-python:2.4.1-985bd65` = `sha256:929eeac2091ecbe916cf1f19fba9f13056e621a1c2c1947427b97a8a7c002082`
  (recipe receipt PASS, 15 expected file differences vs production 18ada571, pip freeze equal); trivy once: 0 fixable
  CRITICAL/HIGH.

## CI plan (CI2 / CI11)

`scripts/ci_change_scope.py --base 2b3aa3e --head <head> --event pull_request --base-ref dev`:
`scoped: python,rust,contracts`, 40 files, no CI-defining path.

| Job | Runs / inherited | Est. min |
|---|---|---|
| changes, preflight, repository-scan, ci-result | runs | 1.5 |
| contract-tests (`buf breaking`, generated diff) | runs | 0.5 |
| sdk-python310, release-gates | runs | 4.5 |
| unit-tests (4 shards) | runs | 48 |
| rust-workspace (generated/rust changed) | runs | 3 |
| kn-native-integration | runs | 6 |

About 64 job-minutes, GitHub-hosted (public repository), one push.

## Risk And Rollback

Code paths are opt-in or additive: without `QDL_CANONICAL_INSTRUMENT_METADATA` the API is unchanged; older SDKs
ignore `canonical`; the bar_edge sidecar is ignored by older images. Runtime rollout (separate coordinator windows):
Query x2 on the v2.4.1 image without the file, then with it, then bar_edge; rollback per role to python `18ada571`
with the v2.4.0-r2 compose.
