# DL v2.4.0 acceptance record and certificate

**Status: PASS WITH NOTED LIMITS (runtime windows W1-W4 accepted 2026-11-19).** The release tag still needs the CI6
receipt: one full green CI run on the exact tagged tree (section 6). shared100 (A6) is post-release acceptance by owner
decision (section 7). This replaces `drafts/CERTIFICATE_DRAFT.md`.

## 1. Release identity

| Item | Value |
|---|---|
| Source | `dev` 7230d5c (merge of PR #56) plus this acceptance branch (`release/v2.4.0-acceptance`: window gate tooling and this record; no code) |
| rust image | `qdl-v2-rust:2.4.0-c17472c` = `qdl-v2-rust@sha256:abafab2c98cbdd9d2fe86f2b26fd6fcf64800ccdd114a3dd5b92803de16a3e98` (receipt `~/.local/state/qdl-v2/build-v2.4.0-c17472c/receipt.json`; binaries byte-identical to 3d4d84b) |
| python image | `qdl-v2-python:2.4.0-acf42d7` = `qdl-v2-python@sha256:18ada571c72856cefc2844c8cb2c3809c3e30ac77927bc4c73160e7b2787cd18` (receipt `~/.local/state/qdl-v2/build-v2.4.0-acf42d7/receipt.json`) |
| Runtime base | rust `qdl-v2-rust-base:bookworm-v240-20261118` `sha256:b33ec0cb…`; python `python:3.12-slim@sha256:2c941e86…` |
| R0 security | PASS: cargo-deny `ignore = []`, pip-audit, trivy 0 fixable CRITICAL/HIGH on both ids (`drafts/R0_SECURITY.md`) |
| SDK | `qdl_sdk` 2.9.0 (additive since 2.6) |
| Declaration | r8 stage C (catalogue r16, acquisition r24) + r9 A-FUND (TS manifest r15, routing r34) |

## 2. Scope (all `tested locally`, then accepted on the runtime in section 4)

- v2.3.2 content: Stream SessionAlive frames, opt-in DELIVERY_ON_CHANGE on 102 ON_CHANGE QUOTE bindings, R0 recipe.
- S1 verified-token cache; S2 advisory token revision (readers accept 1..manifest revision); S3 quota only on
  admission (Rust refund guard); S4 direct MARK/INDEX view call; S5 shared latest view + singleflight; S6
  session-fair warmup queue + singleflight, hot lanes session-fair, waits capped below client timeouts; S7 per-replica
  token buckets (Redis optional, TS full rate per replica); S8 role claims ignored; S9 shared100 prepared.
- A-FUND: FUNDING_RATE reference reads for the TS Paper writer (102 perpetuals) and historical 1m MARK/INDEX candles
  (97 OKX SWAP, bounded window, ALPHA grade); SDK 2.9.0.
- Window 1 RCA fix: Query hot read tries the next Stream replica on `HOT_AUTHORITY_MISMATCH`/`HOT_AUTHORITY_CHANGED`
  (`qdl/runtime/kn_hot_view.py`, PR #56).
- Stage C completion (S6-S11) on the v2.4.0 images: Binance signal data for crossec_ema (96 USDM BAR 1m native,
  1h/12h DERIVED_FINAL, FUNDING_RATE), Binance ingestor rev 24, bar_edge r8 pair; session-liveness writes 250 ms.

## 3. CI and local evidence

| Item | Result |
|---|---|
| PR #55 (A-FUND) | run 44444142309 green (scoped python; rust inherited from 44325107773) |
| PR #56 (hot-read fix) | run 44466904365 green on tree `36573ab3` (scoped python, 48.7 job-min; rust inherited from 44446296647) |
| dev 7230d5c push run | 44468533774 **red only on `unit-tests (4)`**: `test_fund_phase6_capacity…test_bounded_normal_and_burst_windows_preserve_replay`, a synthetic throughput gate (178.3 < 200 events/s on a hosted runner); unrelated to the change (rerun of that one job is the CI7 path) |
| dev 03d0aca push run | 44446296647 red only on `unit-tests (1)`: `test_v230_rcert2_shadow` wall-clock comparator; passed in 44466904365 |
| Local impacted sets | journal entries "DL v2.4.0 S1-S9", "A-FUND", "window 1 RCA" (Ran 212 OK for the hot-read fix) |

## 4. Runtime acceptance (2026-11-19; window dirs `~/.local/state/qdl-v2/v240-window1-20261119T0507Z/` and `v240-window-r2-20261119T0541Z/`; receipts copied to `runtime-windows-20261119/`)

Precondition W0 (TS lane): writer `d1addc04` (TS v2.2.0 W1) with backoff, per-replica Query pools, 246/246.

| Window | Role(s) | T0 (UTC) | Result |
|---|---|---|---|
| W1 (first attempt) | stream_kn_1 | 05:08:02 | **FAIL, rolled back 05:14:08** (writer 209-216/246 for 6 min). RCA below. |
| W1' | stream_kn_2 → rust `abafab2c`, realm `v2.4.0-realm` (bundle `577470a6…`) | 05:42:19 | PASS (coordinator receipt) |
| W2 | query_kn_2, query_kn_1 → python `18ada571`, r8 runtime, TS r15 + alpha r17, hot targets b,a | 05:56:10 / 05:57:46 | PASS (`gates-query_kn_1-reread.txt`; query_kn_2 probe-authority 2 → 0 on re-read, `NOTE_query_kn_2`) |
| W1'' | stream_kn_1 → rust `abafab2c` | 06:06:14 | PASS (`gates-stream_kn_1.txt`); writer 246/246 at T+10 min (06:16:26), 0 fallback lines after 06:06:14 |
| W3a | ingestor_binance_usdm → rev 24, 118 bindings PINNED, 250 ms | 06:18:14 | PASS, cadence 3.83 writes/s (`gates-ingestor_binance_usdm.txt`) |
| W3b | ingestor_okx_swap → 465 PINNED, 250 ms | 06:21:21 | PASS, cadence 3.5 writes/s (`gates-ingestor_okx_swap.txt`) |
| W4 | binance_bar_edge → python `18ada571`, r8 pair | 06:24:27 | first gate read FAIL on a stale heartbeat (held, no rollback); after the bootstrap **PASS** (`gates-binance_bar_edge-postbootstrap.txt`): crossing 14→16 / 22→24 retained 1047 added 273 dropped 0, heartbeat 16/24/0, published 1320, pending 0; D6b at 08:30:42 recorded 209 healed 209 kept 0 outstanding 0 (`d6b-20261119T0830Z.txt`) |

Window 5 (SessionAlive on the writer) is a TS-lane window and is not part of this certificate.

## 5. RCA and deviations

1. **W1 failure: catalog-revision mismatch on the private hot read** (ROLLOUT_PLAN section 8). Query on catalog 15
   was refused by the new Stream on catalog 16 (`hot_view.rs:115-119`) and did not try the other replica; quiet
   EXECUTION QUOTEs became `HOT_QUIET_PRIMARY_UNVERIFIED`. Query refusals 0 / 185 / 0 before / during / after.
   The stage C S6 incident of 2026-11-18 had the same trigger (30/35 refusals). Fixed in code (PR #56) and by the
   corrected order (stream_kn_2, Query x2, stream_kn_1).
2. **W1 gate defects:** the subscriber gate was invalid (the SDK keeps a failed-over subscription,
   `qdl_sdk/transport.py` 2.4.1:612-619) and the writer gate lacked the quiet-class allowance. Replaced by
   G-hot / G-query-refusals / G-writer (`drafts/window/v240_window_gates.sh`).
3. **W2 intermittent G-hot "1 not OK":** the first gate printed no code. Not reproduced in 24 later runs; production
   Query showed only the pre-existing `HOT_RECORD_MISSING` class. The gate now prints each failing read with its code
   and fails only on authority/auth codes.
4. **W4 stale heartbeat:** the edge rewrites its heartbeat only at the top of each loop turn
   (`stable_bar_edge.py:2333-2349`); the 5.5 min restore check ran first, so the gate read the old process's file
   (mtime 06:24:24, before the stop). Health showed `unhealthy` for that time; no role restarts on health.
5. **W4 restart repair larger than planned:** the restore check marked 784 retained bindings incomplete
   (missing_rows 6,082,736; 506 OKX SWAP + 278 Binance USDM) besides the 273 new bindings. Verified per binding from
   the edge's `history ACK` and `history is short` lines (`runtime-windows-20261119/bar-edge-repair-split.json`):
   - 680 bindings provider-short: 5,575,631 rows (91.7 %) are opens before the instrument's listing, re-proved
     because short proofs are in memory only (`stable_bar_edge.py:388,1622`); 668 of them published 0 rows, 12 filled
     a 1-2 row gap at the newest edge (14 rows in total, closes during the stop);
   - 104 bindings (99 OKX + 5 Binance, all 1m/3m) not short, all 10,000 rows present on refetch, 0 published:
     507,105 rows counted missing at restart (inferred: the rolling 1m/3m retention moved past the checkpoint
     watermark window; not verified further);
   - new bindings outside the repair set: 278 ACKed, 1,850,567 rows published.
   Bootstrap 06:30:14 → 08:14:25 (1 h 44 min) at about 5-40 bindings/min on the bounded provider lanes, 0 quota
   refusals. Defect candidate for DL v2.4.1 (S6): persist short proofs and bound the restore check by the earliest
   proven open.
6. **W4 transient readback race:** 29 `KnBarReadbackNotReady … (REBUILDING)` cycle failures, all on new crossec
   Binance 1h/12h products while the bars projector staged their first generation; every one has a later ACK. Logged
   as ERROR + traceback; v2.4.1 S6 logs it as a retry.
7. **Five Binance 3d bindings** (BNB/BTC/DOGE/ETH/SOL) stay `history_failed`/`history_suspended`
   (`OVERLAPPING_PROVIDER_WINDOWS` in Binance's 3d klines around 2023-08-14/16), as before the window. Not crossec.
8. My prediction during the bootstrap that `published` would plateau at 1315 was wrong: it reached 1320 (the five
   3d bindings kept their watermarks).

## 6. CI6 receipt (open)

No full green run exists yet on a tree that will be tagged: dev 7230d5c run 44468533774 is red on one shard, and this
acceptance branch changes the tree (docs/evidence only). The release receipt is one full CI run on the final dev tree
after this branch merges (see `RELEASE_NOTES.md` "Release steps"); main then inherits it (identical tree, CI1).

## 7. Known limits

- shared100 (A6) not run: post-release acceptance (driver shared100 mode, A6 bind, 100-session alpha fleet,
  measurement lock).
- No TEST-consumer tool for a direct per-replica stream subscription or the step 9.4 FUNDING_RATE / historical MARK
  read probes on prod.
- The hot lanes have no client-disconnect probe (wait capped at 7.5 s); Rust refunds per-method refusals only.
- bar_edge re-proves provider-short history on every restart (item 5), fix in v2.4.1.
