# DL v2.4.2 acceptance record and certificate

**Status: PASS (r10 runtime windows W1-W6 accepted 2026-11-19; bootstrap complete 1319/1319 at 17:11:07Z, D6b 0 outstanding; W7 ingestors accepted).**

Code receipt: full dev push run 44556512937 on tree `b69e27c4c71c654e4bd5e39b07ee07304f9a9e56` (dev 77d4d5a, PR #61
head 48137eb). The tag receipt (CI6) is the full dev push run after this acceptance branch merges, the same flow as
v2.4.1.

## 1. Release identity

| Item | Value |
|---|---|
| Source | `dev` 77d4d5a (merge of PR #61, head 48137eb), plus this acceptance branch (docs, evidence and runbook only) |
| python image | `qdl-v2-python:2.4.2-48137eb` = `sha256:5ea7a7a40ef2d1df920d0f7cf94f22431d53087f8002929e7dffdb4208f7dcc4`. Build receipt `~/.local/state/qdl-v2/build-v2.4.2-48137eb/receipt.json` PASS. It runs on query_kn x2 and binance_bar_edge. |
| rust image | `qdl-v2-rust:2.4.2-48137eb` = `sha256:e628fbe07afe94e8f716692633e77217a16223ea03abc7ea34edb959a48c72a7`, ingestors only (W7). The streams keep `abafab2c`; projectors and cores keep `58bdd282`. |
| R0 security | trivy 0.70.0 once per image: 0 fixable CRITICAL/HIGH on both |
| Declaration | r10 (`declaration-retire-r10/`, offline gate PASS 21/21): catalogue 16->17, acquisition 24->25, demand 12->13, scope 15->16, routing 34->35; manifests alpha.okx.paper r17->r18, alpha.okx.live and alpha.okx.sandbox realm r1->r2 (advisory floor 1) |
| Host prep | `releases/v2.4.2-r10` (7 files differ from `v2.3.1-stage-c`), `releases/v2.4.2-realm` (4 files differ from `v2.4.0-realm`), `releases/v2.4.2-compose` (compose with one delta, the Query manifest list). sha256 lists are in `drafts/window/SHA256SUMS.dirs`. The render-hash check passes: 16/16 live roles reproduce from their labels (`drafts/window/render-hash-check.txt`). |

## 2. Scope

- **Typed provider failure.** A provider's "instrument does not exist" answer becomes a per-binding typed failure.
  - Only documented codes count:
    - OKX 51001, 51021, 51022, 51027, cited from docs-v5 `#error-code-rest-api-public-trade-class`. The transient codes
      51028-51030 are excluded.
    - Binance -1121, -1122, -4141.
  - The binding gets a 6 h quarantine, a log event, and the heartbeat counter `instrument_unavailable`. The cycle
    continues.
- **Backoff.** The failing-cycle backoff is now `max()`, 2..30 s.
- **Live path.** An OKX `60018` per-argument subscribe refusal drops only that argument. The ingestor reports it in
  `subscriptions_rejected`. The fixture is the frames of one public, unauthenticated probe.
- **Declaration.** New venue-listing gate, required from r10. `retire_instruments` (`RetiredBase`) also retires
  deployed realm manifests.
- **Query.** A manifest-allowed requirement with no catalogue binding now returns a typed per-item refusal:
  `INSTRUMENT_NOT_FOUND` or `UNSUPPORTED_FEED`.

## 3. CI

| Run | Result |
|---|---|
| full local unit suite (4 shards, on 48137eb) | 1243 / 1168 / 950 / 1007 tests. Left after the run: 10 local-harness import errors (read-only `/app/logs`, unreadable `$HOME` probe path) and 1 timing flake. The 11 modules then passed when re-run CI-like: 164 OK, 23 skipped. |
| Rust (builder 1.82.0) | fmt, clippy -D warnings and workspace tests green |
| PR #61 | green, 13/13 |
| dev push 44556512937 (77d4d5a, tree b69e27c4) | **green: the code receipt** |

## 4. Runtime acceptance

Window dir: `~/.local/state/qdl-v2/v242-window-20261119T1553Z/`. The receipts are copied to `runtime-windows-20261119/`.

Gates come from `drafts/window/v242_window_gates.sh`: role config, mounts, bundle and manifest identity, the RVN
typed-refusal probe, the bar_edge crossing and heartbeat, G-hot, G-query-refusals and G-writer.

Order (corrected against the v2.4.0 window 1 RCA): stream_kn_2 first, so a Query on catalogue r17 always has a Stream
replica on r17.

| Window | Role | T0 (UTC) | Result |
|---|---|---|---|
| W1 | stream_kn_2 (realm bundle `016e68b4…`) | 15:53:56 | PASS 16/16. The r16 Queries fell through to stream_kn_1: 15 authority-fallback lines and 0 `HOT_AUTHORITY_OR_PROTOCOL_REFUSED`. |
| W2 | query_kn_2 -> python `5ea7a7a4`, r17, paper r18 / live r2 / sandbox r2 | 15:56:53 | PASS 20/20. RVN probe: snapshot `INSTRUMENT_NOT_FOUND`; mixed batch returns each item. |
| W3 | query_kn_1 | 15:59:39 | PASS 20/20; authority fallbacks 0 from here |
| W4 | stream_kn_1 | 16:02:41 | PASS 16/16 |
| W5a / W5b | market_projector_1 / _2 (kn bundle `1312c3bb…`) | 16:05:10 / 16:07:41 | PASS 13/13 each |
| W6 | binance_bar_edge -> python `5ea7a7a4`, r10 pair and migration record `0f98b8f7…` | 16:10:47 | PASS 20/20. First heartbeat 16:18:35 (+468 s silent restore). Crossing: 16->17 / 24->25, retained 1319, dropped 1, migration_record=True. Traceback 0; instrument_unavailable 0; published 1047 at gate time. |
| W7 | ingestor_binance_usdm, then ingestor_okx_swap -> rust `e628fbe0` (configs unchanged) | 16:20:29 / 16:24:23 | PASS. binance: 118 PINNED, subscriptions_rejected 0, liveness 4.0/s. The first gate read FAILed only on the `ingestor-rejects` regex false positive (section 5, item 5); the corrected re-run PASSed. okx: 465 PINNED, 7 lanes flowing, subscriptions_rejected 0, liveness 3.5/s. Writer 246/246. |

- TS writer: 246/246/234-235 in every window, worker_retries `{}`, refusals 0.
- W6 checkpoint backup: `.pre-v242` checkpoint `4c48bb7b…` and sidecar `a6ddec27…`, made before the recreate.
- Bootstrap after W6 (read-only tracker, `bar-edge-bootstrap-track.log`): published rose from 1047 (W6 gate) and 1068 (16:20:50Z) to **1319/1319 at 17:11:07Z**, with pending 0,
  history_failed 5 (the five Binance 3d bindings), instrument_unavailable 0 and declaration 17/25 on 2 consecutive
  samples, healthy, RestartCount 0, and 0 Traceback / bounded-bootstrap / instrument-unavailable lines. The v2.4.1
  head-of-line stop at 1118 is gone: published passed it at about 16:24Z without a stall. Short proofs in the sidecar:
  771.
- D6b at 17:26:07Z (15 min after the end, logs since 16:10:51Z): 0 derived-bar gaps recorded, 0 healed, 0 kept,
  0 outstanding. The edge logged no `stable derived BAR gap` line at all in the period
  (`d6b-20261119T1726Z.txt` is empty). bar_edge `5ea7a7a4`, rc 0, healthy.

## 5. Deviations and lessons

1. **Order correction.** The requested order was Query x2 before the Streams. Applied to a catalogue-revision change,
   that order would have repeated the v2.4.0 window 1 failure: `HOT_AUTHORITY_MISMATCH` on every replica
   (`hot_view.rs:111-119`). The windows ran stream_kn_2 -> Query x2 -> stream_kn_1, and the fallback counts above show
   the fall-through working as designed. Recorded in the runbook.
2. **Realm manifests outside the repository** (alpha.okx live/sandbox r1) still named RVN.
   - Loads were unaffected.
   - A snapshot batch that contained RVN failed with an untyped `KeyError`; the offline probe showed this.
   - Fixed in two places: the retirement now also retires realm manifests (slice 2a), and Query returns a typed
     per-item refusal (slice 2b).
3. **Local suite harness.** Mounting the worktree read-only as a different uid produced 10 harness-only import
   errors. The re-run uses a writable copy as the image user, as CI does.
4. **Live log window for the "missing binding" check** covered only the time since the v2.4.1 recreates (about 3 h),
   because the earlier logs did not survive the recreate.
5. **W7 gate false positive.**
   - The first `ingestor_binance_usdm` gate run FAILed on `ingestor-rejects` = 12.
   - Cause: the v2.4.1-era regex `subscription.*reject` also matches the new v2.4.2 progress-line field
     `subscriptions_rejected` (value 0). Since T0 there was 1 non-progress log line, with 0 errors or rejects.
   - The coordinator re-ran the gate with a corrected copy that excludes `qdl_native_raw_ingestor_progress` lines
     before counting: PASS (`gates-ingestor_binance_usdm-rerun-fixedregex.txt`).
   - The sha-pinned installed gate file was left unchanged and is kept as `drafts/window/v242_window_gates.as-installed.sh`.
   - The corrected gate for future windows is `drafts/window/v242_window_gates.sh`.
   - Real rejections stay gated by `ingestor-subscriptions-rejected` (the field sum) and `ingestor-rejection-events`.

## 6. Not changed

- kafka x3.
- rust_core x3. They keep one inert RVN row; the rendered r10 core files remove it at a later core restart.
- market_cache and stable_redis.
- The TS writer and the TS roles.
- The ingestor and core configs, canonical metadata v1, TLS and secrets.
- The rollback sources are kept: `v2.3.1-stage-c`, `v2.4.0-realm`, and images `929eeac2`, `abafab2c`, `58bdd282`.

## 7. Owner note: execution_alpha strategies that name RVN

These symbol lists name RVN, and none of them was running on the host on 2026-11-19:
- scalping_PSAR, scalping_sl_tp, taalpha, grid_long_only, vwma00315, fib_sl_tp_strength;
- regressionportfolioA001 and rsiboundportfolioA001 (symbols_300);
- signal_combine;
- deep_momentum (research snapshot).

After r10, an OKX RVN request from them gets a typed refusal: PERMISSION_DENIED from the manifest, or
INSTRUMENT_NOT_FOUND. Binance `RVNUSDT` is unchanged.
