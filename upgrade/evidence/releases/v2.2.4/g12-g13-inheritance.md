# v2.2.4 - G12 / G13 evidence inheritance audit

Status: AUDIT, read-only, 2026-10-16 (Claude subagent). This is not an inheritance approval and not a gate result.
Machine-readable companion: [g12-g13-inheritance.json](g12-g13-inheritance.json) (receipt paths, sha256 seals, cell
lists, commands). DL head at audit `87db215`, TS head `bdf7685e`. No container, build, Redis/Kafka/Postgres access or
plan edit was made; live state was read with `docker inspect` / `docker image inspect` only, during the stage-100 #3
measurement lock (until 04:04:12Z).

Rules applied:
- G12 and G13 as frozen in TS `upgrade/p184-phase-d-gates.md`. G13 requires "every cell closed once on the final DL +
  writer digests".
- Codex index: historical receipts are not credited against final-image gates without an approved inheritance.
- INHERIT means the exercised path is unchanged and the cell can be offered to the owner for a recorded inheritance.
  It is not automatic.
- RERUN_AFFECTED: a changed role is on the path, but part of the receipt still describes an unchanged binary.
- RERUN_FULL: the system under test itself changed.
- CERTIFICATION_LEDGER.md has no G12/G13 rows, so no "Pinned at" column constrains these cells.

## Deployed set at audit time (docker inspect, ~03:00Z)

| Role | Image | Source |
|---|---|---|
| rust_core x3 | `7c848aad` rc22 (cpus 1.0 / 1.0 / 0.5) | DL a4e7b3b, config `v2.2.3-rc18-core-batch1024` (batch 1024, wait 25 ms) |
| stream_kn x2 | `baa72c96` | DL c934c1e (SR1/SR4), started 01:46-01:47Z |
| query_kn x2 | `b9ca6c36` rc19 | DL 4d3476c |
| projectors x2 | `ce629061` rc14 | DL a388537 (no IDR-107) |
| ingestors | `7fba2577` RUST18 | DL cdbf28b; binance cpus 0.75, okx 0.5 |
| writer | `d27db0cf` B16 | TS 5636e443; native TRADE,QUOTE; APPLY_MAX_ROWS 16; BOOK_SNAPSHOT wait 0; JWT rev 11 |
| kafka x3 | `9516fb76` | auto leader rebalance at the Kafka default (on) |

Query realm `reader-public-r16` was last modified 2026-10-12 09:46Z, before the G13 v4.2 window.

## Changes since the receipts

- W: writer B13 -> B14 -> B16. Covers G2 slices, G7 BOOK_SNAPSHOT wait 0 with latest-only projection, G8 sub-units
  and G9 health.
- S: Stream b095b18e -> baa72c96 (SR1/SR4), on top of the b095b18e live-reserve isolation.
- C: core rc22 by simultaneous replace.
- I: ingestor_binance_usdm cpus 0.75.
- K: md.latest.v2 delete.retention.ms 6 h.
- O: G10/G11 v2 oracles. These are campaign scorers and are not used by the G12 or G13 harnesses.
- Q: Query went rc12/rc16 -> rc19 (4d3476c BAR warmup fast render + per-source authorization). This happened after
  every G13 receipt, but not today.

## G12

| Scenario | Exercises | Newest receipt (pins) | Touched by | Verdict | Reason |
|---|---|---|---|---|---|
| Broker stop/start | Kafka RF3/ISR2, transactional core, projector, Stream, Query, writer clone -> TS cache; G1-G3 after catch-up | Last PASS `p184-g12-rc12-20261011` (20f29671; rust rc12 ebc7b794, py 3b585597, writer 64347a94; predicate v1). Newer `rc18-v2-control` (17673300) and `rc18-v2b-compat` (2f9e3464), predicate v2: FAIL, G2 max 1.68-2.47 s | C, S, W, Q | RERUN_FULL | Every consumer-side role changed. The v2 runs FAIL G2 max in both arms on the Kafka auto leader rebalance stall (TS plan 76388-76394), and the owner decision on handling it has not been recorded. The production projector is still rc14 without IDR-107, and a projector exit is a G12 stop condition. |
| Projector restart | projector stage A/B restart, seen at the TS cache | rc12 PASS 19.4 s; rc18-control2 PASS (4c8fef25, predicate v1) | S, W, Q, C | RERUN_AFFECTED | The projector binary is unchanged: `git diff --stat 5ee9463 a388537 -- rust/qdl-projector rust/qdl-kafka/src/lib.rs rust/qdl-contracts` is empty. The measured quantity crosses the changed Stream and writer. The fault runs in the same harness lifetime, so it adds no run time. |
| Stream restart | gateway restart; writer resume/replay through hub/handoff/live budget | rc12 PASS 138.4 s | S, W, Q, C | RERUN_FULL | Today's Stream change sits in the restarted role: `git diff --stat a388537 c934c1e -- rust/qdl-stream-gateway/src` shows 9 files, +2503/-219. The native writer reconnect is also new. |
| Core-group simultaneous replace + restart + SIGKILL (S2) | 3 cores x 6 raw partitions, kafka x3, publisher, monitor only | `p184-coregroup-20261015/S2-acceptance.json` (4626b44a), prereg 42b82e61 + addendum-5 2fa5dffb: run_S2 PASS 16/16, pause 10.6 s, kill caught up 10.2 s | none | INHERIT | Same core digest 7c848aad, same batch/wait config, same Kafka image. Stream, writer and ingestor are not started in this mode (provenance.jsonl). Production replace at 13:33Z re-formed in 11.2 s. Caveat: production rust_core_3 has 0.5 CPU, while the harness gave every core 1.0. Level: shadow-certified. |
| Core-group rolling (A', B3) | mixed-version rolling | B3 PASS at batch 25 (44d6fcb9); A' reproduced the storm | none | NOT_APPLICABLE | All cores already run rc22, and the procedure is simultaneous replace. B3 is kept as history and not credited. |

The G12 harness always runs the three faults in one lifetime, so the minimal G12 action is one full run.

## G13 (76 cells = 65 + 11 OKX_INVERSE)

Receipt pins:
- 41 cells come from `p184-g13-20261011T134532Z` (445e05c9) and `T140432Z` (f0b7b523). These ran on DL rc12
  (python 3b585597, rust ebc7b794) with writer 44baacc0.
- 35 cells come from `p184-g13-20261012T130309Z` (e3663d6e) and `20261013T170506Z` (9c285293). These ran on
  roles-v4: query 7d43121e, core f41ecb66, stream/projector/ingestors ce629061, writer 2beedc04, runner daa8841.
  verify-b 0bfaafe3 = 35/35.

| Step (cells) | Exercises | Touched by | Verdict | Reason |
|---|---|---|---|---|
| dr01-deployed (7: BINDING_COMPILE x2, WRONG_REALM x2, WRONG_REVISION x2, NO_GLOBAL_GRANT) | alpha creds -> Query snapshot route -> JWT/realm/revision/entitlement | none | INHERIT | `git diff --stat bcb0283 4d3476c -- qdl/security qdl/consumer consumers config/v2 qdl/reference` is empty. The router diff only adds the BAR warmup render and additive refusal logging. Realm and revisions are unchanged. |
| dr01-wrong-account (2) | TS Gateway test stack (2beedc04); no DL role | none | INHERIT | `services/gateway` and the probe script are unchanged in f1b52f63..5636e443. The runner (G13-R1) records the writer digest as provenance, so the cells name 2beedc04. |
| dr02-dr03-alpha (14) | alpha -> Query warmup/batch/history; Stream 1m append | Q, S, C | RERUN_AFFECTED | 4d3476c rewrote the DR02 BAR warmup render and its authorization. DR03 append crosses today's Stream and rc22. The alpha image and scripts are unchanged. |
| dr07-reference (12) | alpha_sdk.reference -> Query reference -> qdl/reference; FX proof (performance) | none | INHERIT | DL reference code and routes are unchanged. TS performance, alpha_sdk and the DR07 script are unchanged. `shared/config.py` only gained additive fields. Same G13-R1 provenance caveat as dr01-wrong-account. |
| dr04-dr06-views (22: DR04 14, DR06 6, DR05.QUIET 2) | Stream/Query -> production writer -> TS Redis -> MarketCacheReader + Risk | W, S, C, Q | RERUN_FULL | These cells read the writer apply path, which changed today (B16 native apply, 16-row sub-units, BOOK wait 0). Risk also changed (IDR-098, IDR-105/106). |
| dr05-upstream (6: GAP, GENERATION x 3 scopes) | raw -> core fence -> quarantine -> views -> writer clone BOOK | C, S, W | RERUN_FULL | rc22 changes the fence/rewind path: canonical.rs +295, qdl-realtime-core.rs +1983. The writer BOOK path changed today (e8cd7b7c). |
| dr05-transport (9: STALE, DISCONNECT, FALLBACK x 3 scopes) | writer clone transport | W, S | RERUN_FULL | The writer session and reconnect code changed (native), and so did the Stream. |
| dr08-isolated (4) | Kafka -> core -> Stream slow-consumer / starvation / restart / alert | S, C, Q | RERUN_FULL | SR1 and the reserve isolation rewrite the slow-consumer accounting these cells judge. The starvation predicate also needs an apparatus fix first (see OF-G13-1). |

Totals: INHERIT 21 cells (DR01 9, DR07 12); RERUN_AFFECTED 14; RERUN_FULL 41.

The DR01 and DR07 cells need about 40 s of step time inside the runtime window that dr02-dr03 needs anyway. Running
them there closes all 76 cells on the final digests without an inheritance amendment. This is the recommended option.

## Oracle loopholes (SR3/SR5 type)

- **OF-G12-1: window/predicate (SR5-like).**
  - Affects every G12 PASS under predicate v1: rc10-full3 c7b8ea95, rc12 20f29671 and rc18-control2 4c8fef25.
  - v1 measured publisher jitter instead of consumer recovery, so the reported catch-up times (138-484 s) were
    apparatus delay, and the G1-G3 window started that late.
  - Under v2, catch-up is 0.1-0.3 s, and G2 max FAILs on the leader-rebalance stall.
  - Whether each v1 window missed that stall is inferred, not proven per run. Do not credit any v1 PASS.
- **OF-G12-2: absent observations do not block (SR3-like).**
  - Location: `scripts/p184_g12_isolated_fault.py`, `md_sample`/`md_stats` (lines 1013-1055).
  - Uncovered events are counted as `pending` and are excluded from the verdict.
  - The oracle has no duplicate-applied check, no post-to-post rewind check and no native typed continuity check.
  - For the rerun, require pending == 0 and read the native `trade_integrity` counters, as the G10 v2 sampler does.
- **OF-G12-3: pre-35571a0 undersampling.** This can only produce false FAILs, not false PASSes. No credit change.
- **OF-G13-1: DR08.NO_CROSS_CONSUMER_STARVATION is timing-dependent.**
  - It failed identically on both arms on 2026-10-15 (DL plan line 73525).
  - SR1 changed how the slow subscription terminates.
  - Fix the window and the predicate before the qualification run.
- **OF-G13-2: DR05 verdict attribution (minor).**
  - `scripts/p184_dr05_verdict.py:39-40` accepts DATA_NOT_READY as `fault_typed`. The quarantine SEQUENCE_GAP +
    capture_id check still pins the fault.
  - Line 49 hard-codes generation 8. This can produce a false FAIL on rc22, not a false PASS.
- **OF-G13-3: DR05.DISCONNECT_RECONNECT.** The startup-control false positive was fixed before the receipts used here.
- **OF-G13-4: DR04.EXPLICIT_LAST is market-dependent.** It needs an active market or a retry rule fixed in advance.

## Minimal rerun list (serial, after the 04:04Z lock; none started)

0. Owner decision on the Kafka auto leader rebalance stall (preferred-election runbook vs broker config, TS plan 76394).
   This is required before a G12 rerun can PASS G2.
1. G12 full, one harness lifetime with all three faults: about 31-44 min, measured on earlier runs (rc12 30.6 min,
   rc18-control2 38.4 min, rc10-full3 43.8 min). Command:
   `python3 -B scripts/p184_g12_isolated_fault.py --run ~/.local/state/qdl-v2/p184-g12-v224-<UTC> --export ~/.local/state/qdl-v2/dr05-export-20261010/public-export.json --namespace p184-g12-dl-<hex16> --image-set <core 7c848aad, projector ce629061, stream baa72c96, python b9ca6c36, ts d27db0cf> --query-code image`
   Precheck before the run:
   - WriterClone has never started the native B16 writer.
   - The rc14 projector may exit on the broker stop (IDR-107 is not deployed).
2. G13 isolated phase, about 8 min: dr05-upstream (5.6 min measured) and dr08-isolated (2.2 min, after the
   OF-G13-1 fix). Command:
   `python3 -B scripts/p184_g13_final.py --phase isolated --role-images <roles-v224.json> --python-image-ref qdl-v2-python:2.2.3-rc19-4d3476c --writer-image sha256:d27db0cf... --ts-source-sha 5636e443... --dl-source-sha <release commit> --inputs <inputs-v224.json> --ts-root <TS run worktree>`
3. G13 runtime phase under an approved packet: about 20 min of step time (views 10.2, dr02-dr03 8.6, transport 0.7,
   dr01 + dr07 0.6). The whole window is about 25-30 min including the ts-p184 + alpha stack; the v4.2 window took
   18 min. Command: same runner with `--phase runtime --runtime-approval <packet>`.

Total is about 75-85 min of serial host time, plus packet approvals. The core-group S2 receipt is kept (INHERIT,
pending the owner record).
