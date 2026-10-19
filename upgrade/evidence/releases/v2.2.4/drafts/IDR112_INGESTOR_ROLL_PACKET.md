# IDR-112 ingestor roll packet (draft, NOT executed)

Status: prepared 2026-10-17 by Claude. Nothing in this packet has been run against production.
Executing it needs the owner's approval of exactly this blast radius and rollback.

## 1. What changes

- Fix: IDR-112, venue-timestamp staleness recycle of OKX latest-state QUOTE lanes
  (`rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs`, commit `6d61710`; design, parameters and tests in
  `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`, section "P18.4 IDR-112").
- Image: `qdl-v2-rust:2.2.4-6d61710`
  `sha256:5e261809b5046c4ff9de098d5b57eb049287abaabd722ed145701be47d79114b`.
  - Label revision `6d61710807d2da389f3cb88ac34af0791852e408`.
  - Build receipt `~/.local/state/qdl-v2/build-v2.2.4-6d61710/receipt.json`, sha256 `6f004ce7...`, status PASS.
- Equivalence with the image running today (`60f83a51`, `qdl-v2-rust:2.2.4-c7ce7d3`), from that receipt:
  - The 11 other binaries are whole-file EQUAL: stream gateway, projector, realtime core, production core,
    kafka-smoke, parity-replay, binance-shadow, venue-core-certify and 3 rehearsal binaries.
  - `qdl-native-raw-ingestor` is DIFFERENT, as expected.
- Config: unchanged. The new field `latest_state_staleness` is absent from both production configs, so the
  documented defaults apply and OKX lane `okx-public-003` (bbo-tbt) arms.
  - Proof: the release binary was run in `--rm --network none --read-only` containers against both production
    config files. It printed the started line with the defaults, then stopped at the missing TLS files, before
    any network access.
  - Evidence: `~/.local/state/qdl-v2/p184-idr112-okx-staleness-20261017/release-binary-config-proof.log`.
- Python image: not rebuilt. `c21e164a` stays. `git diff c7ce7d3..6d61710` touches nothing under `qdl/`, `app/`,
  `generated/`, `config/`, `consumers/`, `qdl_sdk/`, `Dockerfile`, `pyproject.toml` or `poetry.lock`.

## 2. Roles

| Step | Role | From | To | Diff (dry run) | Required? |
| --- | --- | --- | --- | --- | --- |
| 1 | `ingestor_okx_swap` (0.5 cpu) | `60f83a51` | `5e261809` | `['image']` | Yes. This step carries the fix. |
| 2 | `ingestor_binance_usdm` (0.75 cpu) | `60f83a51` | `5e261809` | `['image']` | Owner decision (see 6). |

Dry runs (`ROLL_DRY_RUN=1`, saved inspects, no docker call) are under
`~/.local/state/qdl-v2/releases/v2.2.4/rollout-idr112/`:
- `dry-run-ingestor_okx_swap.json` (`4e07cb96...`)
- `dry-run-ingestor_binance_usdm.json` (`15037dd2...`)

Inspects captured 2026-10-17 ~16:00Z:
- `inspect/ingestor_okx_swap.json` (`9ecccc5f...`): container `dac2b6b00c5f`, RestartCount 0, healthy.
- `inspect/ingestor_binance_usdm.json` (`b7519e62...`): container `ddf54757c14a`, RestartCount 0, healthy.

Not touched:
- `rust_core` x3 (rc22 `7c848aad`), kafka1-3, `stable_redis`, `market_cache`
- `stream_kn_1/2` and `market_projector_1/2`. They stay on `60f83a51`, whose binaries are byte-identical to the
  new image.
- `query_kn_1/2`, `binance_bar_edge`
- the TS writer
- every config, catalog, consumer manifest and JWT revision

Blast radius of step 1:
- The container is recreated and all 5 OKX lanes reconnect break-before-make, like every earlier ingestor roll:
  about 0.3 s per lane, and the books re-snapshot.
- Roll time is about 40 s, plus a 5 min watch.

## 3. Preconditions (stop if any fails)

1. Owner approval of this packet.
2. Host lock taken:
   `python3 -B scripts/host_measurement_lock.py acquire --owner claude-dl-roll-idr112 --purpose "IDR-112 ingestor roll" --minutes 60`.
   Before taking it, `check` must show no foreign job.
3. The live containers still equal the saved inspects: same container ids, image `60f83a51` and RestartCount 0.
   Otherwise re-run the dry run and re-read this packet.
4. Baseline HOLD:
   `python3 -B ~/.local/state/qdl-v2/releases/v2.2.4/rollout/postchecks/hold_check.py $(date -u +%s) $PK/hold-pre.json`.
   It must show writer READY 64 64 and no new error lines.

## 4. Commands

`T=~/.local/state/trading-system/p184-20261005/runtime/p1/rc17/rollout_rc17_v2.py` (sha256 `8de00c46...`, the same
tool as the 2026-10-16 roll); `P=~/.local/state/qdl-v2/releases/v2.2.4/rollout/postchecks`;
`PK=~/.local/state/qdl-v2/releases/v2.2.4/rollout-idr112`; `IMG=sha256:5e261809b5046c4ff9de098d5b57eb049287abaabd722ed145701be47d79114b`.

```
S=$(date -u +%s)
ROLL_ROOT=$PK python3 -B $T $IMG ingestor_okx_swap        # 0 accepted | 2 rolled back (verified) | 5 operator
python3 -B $P/verify_service.py qdl_v2_stable_candidate-ingestor_okx_swap-1 $PK/inspect/ingestor_okx_swap.json $IMG
python3 -B $P/ingestor_watch.py ingestor_okx_swap 300 > $PK/watch-ingestor_okx_swap.jsonl
docker logs --since $S qdl_v2_stable_candidate-ingestor_okx_swap-1 2>&1 | grep -E 'qdl_native_staleness|STALENESS_HANDOVER|qdl_native_runtime_retry' > $PK/staleness-lines-okx.txt
python3 -B $P/hold_check.py $S $PK/hold-after-ingestor_okx_swap.json $PK/hold-pre.json
```

Step 2 (only if approved) runs the same block with `ingestor_binance_usdm`. Binance lanes never arm the detector,
so the `qdl_native_staleness*` grep must stay empty.

## 5. HOLD gates (as in the v2.2.4 roll, plus the IDR-112 checks)

- The roll tool accepts: compose exit 0, image id = `IMG`, effective service = candidate (`['image']` only),
  settled, writer READY 64 64.
- `verify_service.py`: `image_ok` true, mismatch `[]`, RestartCount 0, healthy.
- `hold_check.py` against `hold-pre.json`:
  - writer READY 64 64
  - RestartCount unchanged on every DL container and the writer
  - `rust_core` x3: 0 retry, rebalance or started lines
  - 0 new error lines
- 5 min watch (`ingestor_watch.py`): progress lines every 10 s with 5 lanes; `core_raw_age_ws_max_ms` in the
  pre-roll range; producer errors (`qdl_native_raw_enqueue_retry`, `qdl_raw_persisted_without_offset`) not
  above the pre-roll rate; no `qdl_native_runtime_retry`.
- IDR-112:
  - Exactly one `qdl_native_staleness_recycle_armed` line, for lane `okx-public-003`, with `bindings` 6 and the
    default config.
  - After 30 s, `okx-public-003` progress shows `venue_lag_ms.n > 0` and `staleness_baseline_ms` non-null.
  - No other lane reports `venue_lag_ms`.
- IDR-112 recycles during the watch are reported, not required.
  - STOP and roll back if there are more than 2 `qdl_native_staleness_recycled` in the 5 min, because that means
    false triggers on a healthy venue.
  - STOP and roll back if any `qdl_native_standby_failed` has cause `STALENESS` together with a lane error.
  - Each recycle must show `qdl_native_session_ready` with `continued: true`.

## 6. Decision: does `ingestor_binance_usdm` have to roll?

- For the fix: no.
  - Binance lanes never arm the detector. `staleness_applies` needs OKX QUOTE + LATEST_STATE lanes, and
    `run_binance_connection` has no staleness code.
  - The Binance code path changed only in the publisher type (a test-only in-memory variant) and in 4 new
    progress-line keys, which stay null or 0 on Binance.
- For the release image set: rolling it makes both ingestors run one binary.
  - The final stage-100 is scored "on release digests". If the release names `5e261809` for the ingestors, then
    Binance on `60f83a51` is a different binary from the one released.
- Recommendation: roll step 2 in the same window, after step 1's 5 min watch passes. It adds about 6 min and one
  Binance reconnect (all Binance lanes, about 0.3-0.6 s, books re-bootstrap). Do it before the gate rerun, not
  during it.
- Alternative: keep Binance on `60f83a51` and record per-role digests in the release.
- `stream_kn_*` and `market_projector_*`: do not roll. Their binaries are whole-file EQUAL to the new image. The
  release record should list per-role digests or carry an owner-approved equivalence statement from the build
  receipt.

## 7. Rollback

- Automatic: the roll tool rolls back once (T2/T3) to the pre-roll service it captured (image `60f83a51`, same
  mounts and cpus) and writes `ingestor_okx_swap-rollback-<stamp>.compose.json` under `$PK`.
- Manual, if the tool exits 5 or the HOLD fails after acceptance:
  `docker compose -f $PK/ingestor_okx_swap-rollback-<stamp>.compose.json up -d --no-deps --force-recreate ingestor_okx_swap`.
  - Then run `verify_service.py` with the old image id `sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e`.
  - If the stamp file is missing, use the reference compose
    `$PK/rollback-reference-ingestor_okx_swap-60f83a51.compose.json` (sha256 `e8f64ddb...`, the compose that put
    `60f83a51` live on 2026-10-16, same config mount).
  - Binance reference: `rollback-reference-ingestor_binance_usdm-60f83a51.compose.json` (`fc48b161...`).
- Configs are not changed, so the old binary accepts them unchanged. Its `deny_unknown_fields` matters only if a
  config gains `latest_state_staleness`.
- Kill switch without an image rollback: `latest_state_staleness: {"enabled": false}` in the role config plus a
  recreate. The blast radius is the same as a rollback, so a rollback is preferred.

## 8. After the roll

- Journal the receipts in the DL plan and update IDR-112 in the TS `INTEGRATION_DEFECT_REGISTER.md` (deployed
  where).
- Before the next final stage-100, run the report-only 5 min pre-check from the RCA: a parallel bbo-tbt probe
  against the lane's venue->DL, reporting lane max and probe max (criterion <= 200 ms each).
- The MARK half of the owner's 15:20Z decision is TS-side and is not in this packet.
