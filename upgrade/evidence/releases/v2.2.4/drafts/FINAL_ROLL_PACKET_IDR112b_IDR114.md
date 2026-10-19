# Final v2.2.4 roll packet: IDR-112 review (F1-F3) ingestors + IDR-114 Query (draft, NOT executed)

Status: prepared 2026-10-17 by Claude. Nothing in this packet has been run against production. Executing it needs
the owner's approval of exactly this blast radius and rollback. It supersedes, for execution, the two drafts
`IDR112_INGESTOR_ROLL_PACKET.md` (already executed 2026-10-17 16:46-17:06Z for `5e261809`) and
`IDR114_QUERY_ROLL_PACKET.md` (not executed); both stay as history.

## 1. What changes

Two fixes, two images, one window.

- Rust: IDR-112 Codex review F1-F3 (DL `83bef59`, `rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs` only; dispositions,
  tests and evidence in `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`, "P18.4 IDR-112 Codex review F1-F3"):
  - F1: one absolute 15 s OKX establish budget (connect + ACK), typed `Deadline` expiry; a standby that misses it is
    dropped with `qdl_native_standby_failed` `reason` `STANDBY_DEADLINE`, the active lane keeps running.
  - F2: comments only (what the ACK cutover guarantees).
  - F3: the recycle cooldown/cap slot is taken only after the OKX control budget admits the attempt.
  - Additive progress keys per lane: `staleness_suppressed`, `staleness_last_action`, `standby_failures_total`,
    `standby_deadline_total`. No Python consumer parses them. No config field added.
  - Image `qdl-v2-rust:2.2.4-83bef59` `sha256:a9679defdd970aaa961736d414cf8c443c2ead2f26ebf2e68eb481dda6725784`, label revision
    `83bef596a3cf0e21b423330fd58060afa3a9b1c1`, build receipt `~/.local/state/qdl-v2/build-v2.2.4-83bef59/receipt.json`
    (`7e630925...`), status PASS.
- Python: IDR-114 Query GC stall fix (DL `31a5155`), unchanged from the IDR-114 draft:
  `qdl-v2-python:2.2.4-31a5155` `sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff`, receipt
  `~/.local/state/qdl-v2/build-v2.2.4-31a5155/receipt.json` (`e1a9bcc5...`, PASS); vs the live `c21e164a` exactly the 6
  changed python files differ, pip freeze equal (`equivalence-vs-c21e164a.json`, `2ec87f34...`).

### 1.1 Rust equivalence (build receipt postchecks, whole-file sha256 in the image, sections where needed)

Build 2026-10-17 19:40:54-19:44:03Z under host lock `claude-dl-idr112b-rebuild` (taken 19:40:48Z, released after;
free disk 210.1 -> 210.0 GiB), `scripts/build_qdl_release_images.py --build --component rust --rev 83bef59`
(script sha256 `5d09d1ec...` = the committed blob at `052508d`; source archive `d8734935...`). Method unchanged:
offline cargo in builder `551428d9`, `--network none`, overlay on rc16 base `f41ecb66`. Postchecks: labels ok
(revision `83bef59`, version 2.2.4), image binaries = build context, ldd 0 missing, uid 10001, no TEST clock env, no
feature marker, `unexpected []`, `missing []`.

| Binary | vs `5e261809` (live ingestors, `6d61710`) | vs `60f83a51` (live stream/projector, `c7ce7d3`) | vs rc22 `7c848aad` (live core) |
| --- | --- | --- | --- |
| `qdl-native-raw-ingestor` | DIFFERENT (expected; `faec2c62...` vs `a7643973...`) | DIFFERENT (expected; vs `77ceeee1...`) | DIFFERENT (expected) |
| `qdl-stream-gateway` | EQUAL whole-file `c07bfdef...` | EQUAL whole-file | DIFFERENT (expected, rc22 predates SR1/SR4) |
| `qdl-projector` | EQUAL whole-file `52ad5e33...` | EQUAL whole-file | EQUAL_LOADED_SECTIONS expected; whole-file EQUAL observed |
| `qdl-realtime-core` | EQUAL whole-file `2d821673...` | EQUAL whole-file | EQUAL whole-file (the measured core) |
| 8 others (`qdl-production-core`, `qdl-kafka-smoke`, `qdl-parity-replay`, `qdl-binance-shadow`, `qdl-venue-core-certify`, 3 rehearsal) | EQUAL whole-file | EQUAL whole-file | EQUAL whole-file |

The expectation for the 11 unchanged binaries vs `5e261809` and `60f83a51` was EQUAL_LOADED_SECTIONS (whole-file
EQUAL satisfies it); every one is whole-file byte-identical, so no section analysis was needed (a stronger result).
Only `qdl-native-raw-ingestor` differs, matching `git diff --name-only 6d61710..83bef59 -- rust generated/rust
Cargo.lock Dockerfile.qdl-rust-runtime` = that one file. The new reference `v224_6d61710` was added to the
script's postchecks in `052508d` (tests 40 OK).

Config proof (release binary, `--rm --network none --read-only`, production configs mounted read-only, no Kafka env;
host lock `claude-dl-idr112b-proof` 19:44:49Z, released): both print `qdl_native_raw_ingestor_started` with
authority `RUST_PRIMARY`, OKX bindings 103 / Binance 27 and the default `latest_state_staleness`, then
`qdl_native_runtime_retry` on the missing `QDL_KAFKA_BOOTSTRAP_SERVERS` (no network possible), then a clean
`SIGTERM` shutdown (`..._stopped`). Evidence
`~/.local/state/qdl-v2/p184-idr112-okx-staleness-20261017/review-f1f3/release-binary-config-proof-83bef59.log`
(`f69f632d...`). No container left.

### 1.2 Python image decision: keep `3e9e4d3a`, no rebuild

- `git diff --name-only 31a5155..052508d -- qdl app generated config consumers qdl_sdk Dockerfile pyproject.toml
  poetry.lock` is empty (`052508d` = HEAD before this packet's commit; its rust tree equals `83bef59`).
- The whole `31a5155..052508d` diff is `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`,
  `rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs`, `scripts/build_qdl_release_images.py`,
  `tests/test_build_qdl_release_images.py` and `upgrade/evidence/...` (dockerignored).
- The Dockerfile does `COPY . /app`, so a rebuild at HEAD would differ only in those non-runtime files under
  `/app` (Rust source is not compiled in the python image, the two scripts/tests are not imported by Query, the plan is
  documentation). The hashed runtime roots and the dependency layer would be identical. A rebuild would add a
  release tag and a python roll with no runtime change, so the release keeps `3e9e4d3a` and records per-component
  revisions: rust `83bef59`, python `31a5155`.

## 2. Final image set per role (after this packet)

| Role | Now | After | Why |
| --- | --- | --- | --- |
| `ingestor_okx_swap` | `5e261809` | `a9679def` | F1-F3 (OKX path) |
| `ingestor_binance_usdm` | `5e261809` | `a9679def` | one ingestor binary in the release (F1-F3 change on Binance: 4 new progress keys, null/0) |
| `query_kn_1`, `query_kn_2` | `c21e164a` | `3e9e4d3a` | IDR-114 |
| `stream_kn_1/2`, `market_projector_1/2` | `60f83a51` | `60f83a51` (not touched) | `qdl-stream-gateway`, `qdl-projector` whole-file EQUAL in `60f83a51`, `5e261809` and `a9679def` (1.1) |
| `rust_core` x3 | `7c848aad` (rc22) | unchanged | `qdl-realtime-core` whole-file EQUAL rc22 = `a9679def` |
| `binance_bar_edge` | `08f356a8` | unchanged | own image |

Release record: the stream/projector roles run a different image digest but byte-identical binaries
(`qdl-stream-gateway`, `qdl-projector`, and every other binary except the ingestor). The release lists per-role
digests plus this equivalence statement from the build receipt; recreating four healthy roles for a digest label
would add four reconnects and two projector partition hand-overs with zero code change. Owner decision: accept the
equivalence statement (recommended) or add the four roles as an extra step later with `rollout_rc17_v2.py`.

## 3. Order and blast radius

| Step | Role | Tool | From -> To | Dry-run diff | Serves meanwhile | Watch |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `ingestor_okx_swap` (0.5 cpu) | rc17v2 | `5e261809` -> `a9679def` | `['image']` | none (5 OKX lanes reconnect break-before-make, ~0.3 s per lane, books re-snapshot) | 5 min |
| 2 | `ingestor_binance_usdm` (0.75 cpu) | rc17v2 | `5e261809` -> `a9679def` | `['image']` | none (all Binance lanes reconnect ~0.3-0.6 s, books re-bootstrap) | 5 min |
| 3 | `query_kn_1` (1.5 cpu) | rc15 | `c21e164a` -> `3e9e4d3a` | `['image']`, env/volume diff empty | `query_kn_2`; Stream read view and the TS writer fail over (~20 s + health) | 10 min |
| 4 | `query_kn_2` (1.5 cpu) | rc15 | `c21e164a` -> `3e9e4d3a` | `['image']`, env/volume diff empty | `query_kn_1` (new image) | 10 min |

One role at a time; each step only after the previous step's HOLD passes. Total about 35-40 min. Do it before the
G12/G13/final stage-100 rerun, never during it.

Not touched: `rust_core` x3, kafka1-3, `stable_redis`, `market_cache`, `stream_kn_1/2`, `market_projector_1/2`,
`binance_bar_edge`, the TS writer `market_data_service_p184w`, alphas, gates and scorer; every config, catalog,
consumer manifest, realm dir, JWT revision, env, mount and cpu cap; no Kafka offset, Redis key or spool row.

## 4. Preconditions (stop if any fails)

1. Owner approval of this packet.
2. Host lock (34a): `python3 -B scripts/host_measurement_lock.py check` shows no foreign job, then
   `python3 -B scripts/host_measurement_lock.py acquire --owner claude-dl-final-roll --purpose "v2.2.4 final roll IDR-112b ingestors + IDR-114 query" --minutes 90`
   (from `/home/bobby/.worktrees/data-layer-p184-closure`). Release it after step 4's HOLD or after a rollback.
3. The live containers still equal the saved inspects in `$PK/inspect/` (captured 2026-10-17T19:37:30Z): same container id,
   image and RestartCount 0. Otherwise re-run the dry runs (section 9) and re-read this packet.

   | Role | Container | Image | RestartCount | Health |
   | --- | --- | --- | --- | --- |
   | `ingestor_okx_swap` | `c91149e75ed9` | `5e261809` | 0 | healthy |
   | `ingestor_binance_usdm` | `904597f6da53` | `5e261809` | 0 | healthy |
   | `query_kn_1` | `ba1fbeb39314` | `c21e164a` | 0 | healthy |
   | `query_kn_2` | `066dd875e530` | `c21e164a` | 0 | healthy |
4. `docker image inspect qdl-v2-rust:2.2.4-83bef59 qdl-v2-python:2.2.4-31a5155 --format '{{.Id}}'` prints exactly
   `sha256:a9679defdd970aaa961736d414cf8c443c2ead2f26ebf2e68eb481dda6725784` and `sha256:3e9e4d3a...`.
5. No `ROLL_*` variable in the shell (`env | grep ^ROLL_` empty): rc15 reads `ROLL_REALM_DIR`/`ROLL_COLD_SLICE_MS`,
   which would change the Query env/mounts; rc17v2 reads `ROLL_DRY_RUN`/`ROLL_CPUS`.
6. Baseline HOLD: `S0=$(date -u +%s); python3 -B $P/hold_check.py $S0 $PK/hold-pre.json` shows writer `READY 64 64`
   and `violations []`.

## 5. Commands

```
cd /home/bobby/.worktrees/data-layer-p184-closure
T17=~/.local/state/trading-system/p184-20261005/runtime/p1/rc17/rollout_rc17_v2.py   # sha256 8de00c46...
T15=~/.local/state/trading-system/p184-20261005/runtime/p1/rc15/rollout_rc15.py      # sha256 811efd19...
P=~/.local/state/qdl-v2/releases/v2.2.4/rollout/postchecks
PK=~/.local/state/qdl-v2/releases/v2.2.4/rollout-idr112b-idr114
RUST=sha256:a9679defdd970aaa961736d414cf8c443c2ead2f26ebf2e68eb481dda6725784
PY=sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff
R15=~/.local/state/qdl-v2/p184-d3-capacity-20261009/rollout-rc15                   # rc15 receipt dir (fixed in the tool)
```

Postcheck tools (read-only; sha256): `hold_check.py`, `verify_service.py`, `ingestor_watch.py` (as in the 10-16/10-17
rolls); new `ingestor_lane_report.py` (`d46e971d...`) and `query_stall_report.py` (`812ae73f...`), both only
`docker logs` readers.

Steps 1 and 2 (rust ingestors; `R` = `ingestor_okx_swap`, then `ingestor_binance_usdm`):

```
S=$(date -u +%s); echo $S > $PK/$R.start
ROLL_ROOT=$PK python3 -B $T17 $RUST $R 2>&1 | tee $PK/roll-$R.out     # 0 accepted | 2 rolled back (verified) | 5 operator
python3 -B $P/verify_service.py qdl_v2_stable_candidate-$R-1 $PK/inspect/$R.json $RUST | tee $PK/verify-$R.json
python3 -B $P/ingestor_watch.py $R 300 > $PK/watch-$R.jsonl
python3 -B $P/ingestor_lane_report.py $R $S > $PK/lanes-$R.json
python3 -B $P/hold_check.py $S $PK/hold-after-$R.json $PK/hold-pre.json
```

Steps 3 and 4 (python Query; `Q` = `query_kn_1`, then `query_kn_2`):

```
S=$(date -u +%s); echo $S > $PK/$Q.start
env -u ROLL_REALM_DIR -u ROLL_COLD_SLICE_MS -u ROLL_EXPECT_BUNDLE_SHA python3 -B $T15 $PY $RUST $Q 2>&1 | tee $PK/roll-$Q.out
#   exit 0 accepted | 2 rolled back; the rust argument is ignored for Query roles; inspect/rollback/candidate/receipt
#   files land in $R15 as $Q-{inspect,rollback,rc15,receipt}-<stamp>.json
python3 -B $P/verify_service.py qdl_v2_stable_candidate-$Q-1 $PK/inspect/$Q.json $PY | tee $PK/verify-$Q.json
python3 -B $P/hold_check.py $S $PK/hold-after-$Q-t0.json $PK/hold-pre.json
# 10 min watch: at t+5 min and t+10 min
python3 -B $P/query_stall_report.py $Q $S > $PK/stall-$Q-t5.json       # and -t10.json
python3 -B $P/hold_check.py $S $PK/hold-after-$Q.json $PK/hold-pre.json  # at t+10 min
```

Then `cp $R15/$Q-*-<stamp>.json $PK/` for the run's stamps and `sha256sum` every new file into `$PK/SHA256SUMS`.

## 6. HOLD gates

All steps:
- `hold_check.py` against `hold-pre.json`: `violations []` - writer `READY 64 64`; RestartCount unchanged on every
  DL container and the writer (the recreated role restarts at 0); `rust_core` x3 0 retry/rebalance/started lines;
  stream gateway counter deltas 0; projector status errors none; 0 new error lines.
- `verify_service.py`: `image_ok` true, `mismatch []`, `restart_count` 0, `health` healthy.

Steps 1-2 (ingestors), additionally:
- rc17v2 accepted (exit 0): compose exit 0, image id = `RUST`, effective service = candidate (`['image']` only),
  settled, writer READY 64 64.
- `ingestor_watch.py` (5 samples): 5 OKX lanes / all Binance lanes in every sample; `core_raw_age_ws_max_ms` in the
  pre-roll range; no `qdl_native_runtime_retry` in `ingestor_other_lines`.
- `lanes-$R.json` (IDR-112 + F1-F3):
  - `f1_fields_present_on_every_lane` true (proves the new binary; the `5e261809` binary emits none of the 4 keys:
    checked on the live OKX ingestor 19:39Z, all `ABSENT`).
  - OKX: `armed` = exactly one entry, lane `okx-public-003`, `bindings` 6, default config
    (`window_ms` 2000, `excess_ms` 250, `cooldown_ms` 60000, `max_recycles_per_hour` 6, `min_bindings` 2, ...);
    `lanes_with_venue_lag` = `["okx-public-003"]`; `okx-public-003.venue_lag_ms.n > 0` and
    `staleness_baseline_ms` non-null (pre-roll: max 30, n 640, baseline 26 ms).
  - Binance: `armed` empty, `lanes_with_venue_lag` empty, the 4 new keys null/0, no `qdl_native_staleness*` event.
  - Reported, not gated: `qdl_native_staleness_triggered` actions, `staleness_suppressed`, recycles.
  - STOP and roll back: more than 2 `qdl_native_staleness_recycled` in the 5 min (false triggers on a healthy
    venue); any `qdl_native_standby_failed` (cause `STALENESS` or reason `STANDBY_DEADLINE`) together with a lane
    error; any `qdl_native_session_ready` after a recycle with `continued` false.

Steps 3-4 (Query), additionally:
- rc15 receipt: `ok` true, `writer_ready` true, `compose_exit` 0, `diff ["image"]`, `env_diff_keys []`,
  `volume_changed []`, `health` healthy.
- `stall-$Q-t5/t10.json` (IDR-114, from the new container's own log):
  - `stall_malformed` 0, `unknown_kinds []` (only gc / loop_lag / slow_request).
  - Reported, not gated: `gc.n`, `gc.ge_100ms`, `gc.max_ms`, `loop_lag.n/max_ms`, `slow_request.n/max_ms`,
    `qdl_refusal.n`. Expected: no gen-0 collection paced by MARK reads (the pre-fix baseline would show one >= 100 ms
    about every 76 s, i.e. ~8 in 10 min).
  - STOP and roll back: `traceback_lines` > 0, `asgi_disconnect_or_assertion_lines` > 0, `qdl_refusal.reference_route`
    rising above the pre-roll rate (pre-roll `query_kn_1` 10 min: 0), writer MARK `SOURCE_UNAVAILABLE` rising, or
    writer READY < 64.

## 7. Rollback (per role)

Ingestors (steps 1-2) -> `5e261809`:
- Automatic: rc17v2 rolls back once to the pre-roll service it captured and writes
  `$PK/$R-rollback-<stamp>.compose.json`; exit 2 = rolled back and verified.
- Manual (exit 5, or a HOLD failure after acceptance):
  `docker compose -f $PK/$R-rollback-<stamp>.compose.json up -d --no-deps --force-recreate $R`, then
  `verify_service.py qdl_v2_stable_candidate-$R-1 $PK/inspect/$R.json sha256:5e261809b5046c4ff9de098d5b57eb049287abaabd722ed145701be47d79114b`.
- Reference composes if the stamp file is missing (the composes that put `5e261809` live on 10-17; checked equal to
  the live services at 2026-10-17T19:37:30Z, mounts compared as a set, `$PK/rollback-reference-check.json`):
  `~/.local/state/qdl-v2/releases/v2.2.4/rollout-idr112/ingestor_okx_swap-rc17v2-20261017T164703Z.compose.json`
  (`4041a66a...`) and `.../ingestor_binance_usdm-rc17v2-20261017T165313Z.compose.json` (`465546ef...`).
- The `5e261809` binary accepts the unchanged configs (no config field added by F1-F3).

Query (steps 3-4) -> `c21e164a`:
- Automatic: rc15 recreates the role from `$R15/$Q-rollback-<stamp>.compose.json` when health, writer READY or the
  log check fails, and exits 2.
- Manual: `docker compose -f $R15/$Q-rollback-<stamp>.compose.json up -d --no-deps --force-recreate $Q`, then
  `verify_service.py qdl_v2_stable_candidate-$Q-1 $PK/inspect/$Q.json sha256:c21e164ac4db14af498b73104e344f003fa7117fdae2e465f50df8668cdca252`.
- Reference composes (put `c21e164a` live on 10-16; checked equal to live at 2026-10-17T19:37:30Z):
  `$R15/query_kn_1-rc15-20261016T171446Z.compose.json` (`209e38e8...`) and
  `$R15/query_kn_2-rc15-20261016T171801Z.compose.json` (`20367ebc...`).

A rollback of one role does not roll back earlier accepted steps: ingestors and Query are independent (no
interface change between them). After any rollback the run stops; release the lock and report.

## 8. After the roll

- Journal the receipts in the DL plan (one entry), update IDR-112 and insert IDR-114 in the TS
  `INTEGRATION_DEFECT_REGISTER.md` (row text in `IDR114_QUERY_ROLL_PACKET.md` section 8; deployed where).
- Release record v2.2.4: rust `a9679def` (ingestors) + `60f83a51` (stream/projector, equivalence 1.1) +
  rc22 `7c848aad` (core), python `3e9e4d3a` (Query), per-component revisions.
- Then the G12/G13/final stage-100 rerun once on this image set; the final stage-100 scores the Query stall from
  the production `qdl.stall` lines.

## 9. Dry runs (no docker mutation; outputs under `$PK`, sha256 in `$PK/SHA256SUMS`)

Captured 2026-10-17T19:37:30Z (`$PK/inspect/`, 8 roles incl. the untouched stream/projector for reference).

- `ROLL_DRY_RUN=1 ROLL_INSPECT_DIR=$PK/inspect python3 -B $T17 $RUST <role>` (saved inspects, the tool's own
  candidate and diff assertions, no docker call), exit 0 each:
  - `dry-run-ingestor_okx_swap.json` (`eac188cc...`): `diff ["image"]`, cpus 0.5 -> 0.5, `5e261809` -> `a9679def`,
    `volume_changed []`.
  - `dry-run-ingestor_binance_usdm.json` (`c1b9cac4...`): `diff ["image"]`, cpus 0.75 -> 0.75, `5e261809` ->
    `a9679def`, `volume_changed []`.
- rc15 Query: `~/.local/state/qdl-v2/p184-d3-capacity-20261009/rollout-query-rc19/dryrun_rc15_candidate.py`
  (`e094c87e...`, the 10-15/10-16 method: loads `rollout_rc15.py` unchanged, every `ROLL_*` cleared, applies its
  `candidate()` to the saved inspect), exit 0: `dry-run-query.jsonl` (`cd242a32...`): both replicas
  `diff_keys ["image"]`, `env_diff_keys []`, `volume_changed []`, `service_equal_except_image` true,
  `c21e164a` -> `3e9e4d3a`, restart unless-stopped, cpus 1.5, mem 1536 MiB.
- Rollback references: `rollback-reference-check.json` (`5b2b1d00...`): the four composes in section 7 equal the live
  services (mounts compared as a set; only docker's mount order differs).
- Postcheck tools validated read-only against the live containers 19:39Z: `ingestor_lane_report.py` on the OKX
  ingestor (`5e261809`): `okx-public-003` only lane with `venue_lag_ms` (max 30, n 640, baseline 26), the 4 F1 keys
  `ABSENT`, no staleness/standby event in 10 min; `query_stall_report.py` on `query_kn_1` (`c21e164a`): 0 `qdl.stall`
  lines (the old image has no telemetry), 0 tracebacks, 0 `qdl_refusal` in 10 min.
- `$PK/SHA256SUMS` lists every file above plus the build receipt.
