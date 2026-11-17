# DL stage B window, 2026-11-16 12:24-13:15Z: APPLIED, kept under owner condition

Scope: execution QUOTE and MARK_INDEX_PRICE for 71 crossec OKX swaps. Declaration r7 = catalogue r15, acquisition r23, demand r11, scope r14, routing r32, TS manifest r14, sealed binding r26. Packet and runsheet: DL branch `feat/dl-stage-b-execution-71` @93cea019, `upgrade/evidence/releases/v2.3.0/drafts/STAGE_B_EXECUTION_71_*`. Pinned set: `~/.local/state/qdl-v2/releases/stage-b-r7-20261116/pinned` (SHA256SUMS).

Owner decisions, 2026-11-16:
- D1 GO.
- D2 MARK at 500 ms, with G4 at p99 ≤ 1000 ms.
- D3 temporary ceiling 6.5, raised to **7.0 after the G6 result**, with 24 h to return to ≤ 6.5.
- D4 compare FET/LUNA metadata before use: identical to the 13/11 captures.

## Steps
| Step | Time (Z) | Result |
|---|---|---|
| S1 cores x3 | 12:24:51 | running, rc=0, stage-b-cores mount, collect ~50 ms (bw50 kept), scope_q 0, oos 0, quarantines flat |
| S2 projectors x2 | ~12:27 | healthy, bundle_sha256 7bb5baee (= report), errors {} |
| S3/S4 stream x2, Query x2 | 12:29-12:32 | healthy; Query reads paper r14; realm bundle 67d8e86e (= report); writer and ts-gateway/risk/paper 0 auth refusals |
| S5 writer stop | 12:33:23 | Paper outage starts |
| S6 OKX ingestor | 12:33:25 | 465 bindings, lane plan PINNED, flush {MARK_INDEX:500, QUOTE:50}, 7 lanes incl. okx-public-005, 0 rejects; D5a PASS on both Query (rev 21/22/23 LIVE) |
| S7 W1 BOOTSTRAP | 12:34:52-12:36:27 | VERIFIED 246/246, execution-ready 235 (min 223), image 9dfef8a8 unchanged |
| S7 W2 STRICT | 12:36:54-12:38:02 | VERIFIED 246/246, 235, continuity 11/11 |

Paper market-data outage: 12:33:23 to 12:36:27Z (about 3 min).

## Gates (soak 12:38:02-12:53:18Z)
- **G1 PASS:** W1 and W2 VERIFIED; 246/246; continuity 11/11.
- **G2 PASS:** 0 `quota is exhausted`; per-minute max 7,781 against a grant of 31,000.
- **G3 PASS:** 0 auth refusals in the writer and in ts-gateway, ts-risk and ts-paper.
- **G4 PASS:**
  - MARK p99 558 ms, max 848 ms, 0 missing.
  - QUOTE p99 684 ms (pre 615 + 100 = 715), 0 missing. One QUOTE max sample of 14,902 ms is recorded; the gate covers p99 only.
- **G5 PASS:** `scripts/market_coverage_gate.py` in g34, 96/96 PASS, 0 failing, 0 runtime mutations. Receipt `coverage-gate-stageb.json`.
- **G6 FAIL against 6.5:** DL median 6.565 vCPU (15 samples, 6.29-6.77); post-soak 6.53, 6.74, 6.99. The increase is mostly Query (0.43 → ~1.4 vCPU, from writer reads). The writer itself went from 1.53 to ~2.3.
  - Owner decision: KEEP stage B, temporary ceiling 7.0. Bring it back to ≤ 6.5 within 24 h (Binance BOOK/TRADE cut, writer Query poll tuning). Roll back if it goes above 7.0 or if the 24 h deadline is missed.
- **G7 PASS:** 18 DL and writer roles rc=0. kafka, stable_redis, market_cache, bar_edge and the Binance ingestor are untouched. G1/W05/Portal containers are unchanged.

## Shared load during the window (disclosed)
- The P0-PM agent ran capped unit and parity runs between ~12:28 and 12:36Z (one container at a time; owner-approved exemption).
- The Edge session ran one Rust release build (--cpus 2, 12:29-12:36Z).
- Neither ran during the soak.

## Rollback (if needed)
Runsheet §4: a coupled rollback to r13/r25 in reverse order. The writer `.prev-20261116T123452Z` (JWT 13, r25 env) is kept.
