# DL v2.4.1 acceptance record and certificate

**Status: PASS WITH NOTED LIMITS (runtime windows A, B, C accepted 2026-11-19).** Code receipt: full dev push run
44516122456 on tree `39690471c6c1aa8e010866fc976b9b0bbe5dc21a` (dev ca6a7de, PR #59). The tag receipt (CI6) is the
full dev push run after this acceptance branch merges (same flow as v2.4.0).

## 1. Release identity

| Item | Value |
|---|---|
| Source | `dev` ca6a7de (merge of PR #59, head 25c305a) plus this acceptance branch (docs, evidence, runbook only) |
| python image | `qdl-v2-python:2.4.1-985bd65` = `qdl-v2-python@sha256:929eeac2091ecbe916cf1f19fba9f13056e621a1c2c1947427b97a8a7c002082` (receipt `~/.local/state/qdl-v2/build-v2.4.1-985bd65/receipt.json` PASS; 15 expected file differences vs production `18ada571`, pip freeze equal) |
| rust image | unchanged: `qdl-v2-rust@sha256:abafab2c98cbdd9d2fe86f2b26fd6fcf64800ccdd114a3dd5b92803de16a3e98` |
| R0 security | trivy once on `929eeac2`: 0 fixable CRITICAL/HIGH |
| SDK | `qdl_sdk` 2.10.0 (additive) |
| Canonical metadata | `releases/v2.4.1-canonical/canonical-instrument-metadata.json`, revision 1, 527 instruments, document sha256 `cacea7bb212f9fdfdf4b4f5ccd32a7a0f56a2f3cfc46877ee5ce680d99be1130` (file sha256 `03065435…`); capture 2026-11-19T08:40:58Z, 262 files, `SHA256SUMS` `b73808f1…` |

## 2. Scope

- Canonical instrument metadata v1 (`qdl.instrument.canonical-metadata.v1`): rules with venue documentation
  (`config/v2/canonical-underlying-rules.yaml`), capture/render/gate tool, fail-closed loader, additive `canonical`
  on `/v2/instruments`, proto `Instrument.canonical = 24`, SDK 2.10.0 helpers that refuse missing or UNVERIFIED
  metadata. Render: Binance USDM 203 VERIFIED / 58 UNVERIFIED, OKX SWAP 255 / 3, crossec 96 all VERIFIED, QNT
  UNVERIFIED on both venues (cross-venue price ratio 4.60).
- S6 bar_edge: provider-short proofs in a sidecar; quiet retry on a not-ready readback.

## 3. CI

| Run | Result |
|---|---|
| PR #59 44503460886 (1feb744) | red, `unit-tests (2)`: SDK version pin `2.9.0` (my miss); then the schema count 74 -> 77 found locally |
| full local unit suite (4 shards, CI-equivalent) | 1462 / 885 / 744 / 1406 tests OK (skipped 23 / 9 / 15 / 87) |
| PR #59 44513526631 (25c305a) | green, 13 jobs, 65.7 job-min |
| dev push 44516122456 (ca6a7de) | **green, 13 jobs, 69.8 job-min: the CI6 receipt** |

## 4. Runtime acceptance (window dir `~/.local/state/qdl-v2/v241-window-20261119T1156Z/`; receipts copied to `runtime-windows-20261119/`)

Gates: `drafts/window/v241_window_gates.sh` (derived from the v2.4.0 window gates: role config, G-hot, G-query-refusals,
G-writer, plus `canonical-mount`, `canonical-disabled|enabled`, `canonical-index`, `query-start-clean`).

| Window | Role | T0 (UTC) | Result |
|---|---|---|---|
| A (overlay a, readers first: metadata mounted, not enabled) | query_kn_2, query_kn_1 -> python `929eeac2` | 11:56:54 / 11:59:38 | PASS both |
| B (overlay b, metadata enabled) | query_kn_2, query_kn_1 | 12:02:23 / 12:15:41 | PASS both; `canonical-index` 527 / revision 1 / `cacea7bb…` |
| C | binance_bar_edge -> python `929eeac2` | 12:29:44 | first gate read FAIL on `unhealthy` during the silent restore; PASS on re-run after the first heartbeat (12:38:45, 8 min 54 s after start); owner raised the load ceiling to 25 for C |

Bar_edge after C:
- The bootstrap re-proved and published up to 1118/1320 by 13:04Z, then **stopped advancing**. The cause is a known
  defect (section 5, item 5).
- State at 13:20:51Z: published 1118, pending 0, short 570, history_failed 5, healthy, RestartCount 0.
- The sidecar holds 570 short proofs.
- D6b at 13:20:51Z (since 12:29:44): 40 gaps recorded, 40 healed, 0 kept, 0 outstanding.
- The TS writer is at 246/246/234 (continuity 11/11).

## 5. Deviations and lessons

1. **Silent restore longer than in v2.4.0 W4** (8 min 54 s vs 5 min 38 s): the restore check reads 1320 restored
   bindings (checkpoint `.pre-v241`) against 1047 (`.pre-v240`), including 91 Binance 1m bindings at up to 10,000 rows;
   the S6 code adds one sidecar read and one dict lookup per binding before the heartbeat
   (`stable_bar_edge.py:804,827,879`). The healthcheck was `unhealthy` (expected; no auto-restart on this stack).
   Recorded in `docs/runbooks/kn-maintenance-window-template.md` with rollback triggers (exit/restart, Traceback or
   bounded bootstrap before the first heartbeat, no restore line within 20 min).
2. **First v2.4.1 start re-proves the short bindings** (repair set 772 bindings, missing_rows 6,086,136): no proofs
   existed yet. The sidecar holds the proofs written by this bootstrap; the S6 effect shows on the next restart.
3. PR #59 first run red on an un-grepped SDK version pin; the full local suite is now run before such pushes (CI10).
4. CI-DEBT-1 (lane A): no per-job inheritance across a red run (journal).
5. **Known defect, accepted (fix in DL v2.4.2).** A provider-delisted instrument head-of-line blocks the bootstrap
   repair, and the failing-cycle backoff is ineffective.
   - **Trigger.** OKX delisted `RVN-USDT-SWAP` between about 07:30Z (window 4 still ACKed it) and 08:40Z (absent from
     the instruments capture). The r16 catalogue still carries it.
   - **What happens.** From about 13:01Z every cycle fails with `OKX V5 error code=51001 ... doesn't exist`
     (`RuntimeError ... exhausted retries: /api/v5/market/history-candles`). `bootstrap_history` catches only
     `BarHistoryGapError`, and with `max_bindings=1` the same binding blocks the rest.
   - **Backoff.** `run_forever` takes `min(loop delay, 2**n capped at 30 s)`, so the backoff never applies. That gives
     about 28 cycles/min and 2.8 admitted requests/s against OKX history-candles (OKX limit: 20 per 2 s per IP; no
     429/50011 seen).
   - **Pre-existing.** v2.4.0 runs the same code, so a rollback would not help.
   - **Impact.**
     - About 202 bindings get no watermark or re-proof until v2.4.2: the OKX SWAP S-Z range after RVN, plus RVN.
     - Query still serves their cached bars, live and derived bars flow, and the writer is at 246/246.

## 6. Known limits

- UNVERIFIED instruments (Binance USDM without a `binance` spot constituent, OKX inverse/futures, spot, VN) carry no
  multipliers; consumers must refuse them (the SDK does).
- No prod TEST-consumer probe tool for a direct `/v2/instruments` read; Window B proves the loaded index in-process.
- shared100 (A6) remains post-release acceptance (v2.4.0).
- bar_edge bootstrap incomplete at 1118/1320 until DL v2.4.2 (item 5).
