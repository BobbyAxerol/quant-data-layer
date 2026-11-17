# Run sheet: DL stage B, execution QUOTE + MARK_INDEX_PRICE for 71 crossec OKX swaps (declaration r7, TS r14 / r26)

Status: **prepared offline, not executed.** Packet: `STAGE_B_EXECUTION_71_PACKET.md` (same directory). The
coordinator runs this sheet only after the owner approves the packet's blast radius and rollback (CLAUDE.md §3).
Modelled on `dlcov/WINDOW_STAGE_A_V3.md` and `COMBINED_WINDOW_V230_WB2_RUNSHEET.md` (window A, GREEN 2026-11-13).

- Any gate FAIL means the section 4 coupled rollback, without asking again.
- No `compose up` of kafka*, stable_redis, market_cache or binance_bar_edge. No `docker start` of a stopped writer.
- Never `docker exec` a JVM tool into a broker (2026-11-13 rule).
- Lock: take the host measurement lock at T-0 for the window only and release it right after the CPU gate.

## 0. Variables (no secrets; source in every shell)

```bash
D=/tmp/claude-1001/-home-bobby/178e5b10-f053-4be7-9cc5-bcb710dcc977/scratchpad/stageb
A=$D/pinned-r7                                   # pinned render set (SHA256SUMS inside), copied from outB7 + verified
Q=/home/bobby/.local/state/qdl-v2; R=$Q/releases; P=qdl_v2_stable_candidate
CD=$R/v2.3.0-rc2-compose; CF=$CD/qdl-v2-stable-kn.yml           # compose unchanged (sha256 5b1381e0...)
BASE_ENV=$CD/qdl-v2-stable-kn.v2.3.0-rc3.env                     # deployed env AS IS; after W-A1 permanent (11:16Z) sha256 949cf3f3...,
                                                                 # QDL_CORE_CONFIG_DIR=.../v2.3.0-rc3-a1-bw50-cores (wa1-perm-20261116T1110Z/RECEIPT.md)
SB_ENV=$R/v2.3.0-stage-b-compose/stage-b.env                     # 5 lines, passed LAST (section 0b)
C="docker compose -p $P -f $CF"
GEN_TLS=$Q/tls/gen-20261019T084200Z/generation.env; READER_TLS=$Q/tls/consumer-ca-20261111T1304Z/deployed-tls.env
SECRETS=$R/v2.2.5/kn-secrets.env                                 # never cat
NEW="--env-file $BASE_ENV --env-file $SB_ENV"
OLD="--env-file $BASE_ENV"                                       # = the live state before the window (rollback); the cores' labels also
                                                                 # list wa1-perm/bw50.env, whose one key now equals BASE_ENV (check: config-hash == live label)
WIN=$Q/stage-b-window-$(date -u +%Y%m%dT%H%MZ); mkdir -p -m 700 $WIN; W=market_data_service_p184w
TT=/home/bobby/.local/state/trading-system/p184-window-b-wb2-20261113T1732Z/ts-tools-2d2e18eb   # writer tools, blob-checked (W.9)
IMG_W=sha256:9dfef8a8a548be2ee2d800fb62c006c8d11fcbcf3c66999779f6c97e15a71a6f                  # CURRENT writer image; not changed
# helpers hb / coreprog / health / authgrep / mark / capture: copy verbatim from
#   /tmp/claude-1001/-home-bobby/178e5b10-f053-4be7-9cc5-bcb710dcc977/scratchpad/v230/window-rc2/env.sh and
#   COMBINED_WINDOW_V230_WB2_RUNSHEET.md ("capture"); they read only.
```

### 0b. New host dirs (installed before T-30; nothing running reads them until a role is recreated)

| Dir | Content | Mode |
|---|---|---|
| `$R/v2.3.0-stage-b` | `sudo -n rsync -a $R/v2.3.0-rc2/ ` (keeps secrets, compose.json, tls exactly), then `bundle.json` = `$A/bundles/kn-bundle.json` and `runtime/{stable-source-bindings.yaml,stable-acquisition-bindings.yaml,bar-edge-source-bindings.yaml,bar-edge-acquisition-bindings.yaml,declaration-index.json}` = `$A/runtime/*` | as `v2.3.0-rc2` (root:10001 0750) |
| `$R/v2.3.0-stage-b-realm` | `sudo -n rsync -a $R/v2.3.0-rc2-realm/`, plus `trading-system.paper.stable-r14.yaml` and `bundle.json` = `$A/bundles/realm-bundle.json` | root 0755, files 0444 |
| `$R/v2.3.0-stage-b-cores` | `$A/cores-bw50/*` (`8fb1dc96` / `af533769` / `383c34cd`; section 1 step 2) | 0755, files 0444 |
| `$R/v2.3.0-stage-b-ingestor` | `$A/ingestor/ingestor-okx-swap.json` + a byte copy of the LIVE `ingestor_binance_usdm.json` (unchanged, not recreated) | root:10001 0750 |
| `$R/v2.3.0-stage-b-ts` | `$A/bindings/trading-system.paper.stable-r26.json` | bobby 0750, file 0444 |
| `$R/v2.3.0-stage-b-compose` | `stage-b.env` (below) | 0750 |

`stage-b.env` (exactly these 5 lines; no image digest changes):
```
QDL_KN_RELEASE_DIR=/home/bobby/.local/state/qdl-v2/releases/v2.3.0-stage-b
QDL_CONSUMER_REALM_DIR=/home/bobby/.local/state/qdl-v2/releases/v2.3.0-stage-b-realm
QDL_PAPER_REALM_MANIFEST=trading-system.paper.stable-r14.yaml
QDL_CORE_CONFIG_DIR=/home/bobby/.local/state/qdl-v2/releases/v2.3.0-stage-b-cores
QDL_INGESTOR_CONFIG_DIR=/home/bobby/.local/state/qdl-v2/releases/v2.3.0-stage-b-ingestor
```
Prep checks: every installed file `sudo -n cmp` equal to `$A`; the realm dir keeps `trading-system.paper.stable-r13.yaml`
(r13 tokens stay valid: r14 `min_token_revision: 13`); `$C $NEW --env-file $GEN_TLS --env-file $SECRETS config -q` OK and
the same with `$READER_TLS`; `diff <($C $OLD ... config <role>) <($C $NEW ... config <role>)` shows only bind sources
under the five dirs for every role this sheet recreates, and **no diff at all** for kafka1-3, stable_redis, market_cache,
binance_bar_edge and ingestor_binance_usdm is required only for the roles NOT recreated to be left alone (they are never
`up`-ed here).

## 1. T-30 checklist (read-only, except installing 0b)

1. `uptime` (1-min load < 24), `df -h /` (>= 15 % free; abort < 10 %), lock file absent or expired, `( cd $A && sha256sum -c --quiet SHA256SUMS )`.
2. **Core set.** `sha256sum` of the three live core files (`docker inspect` mount source of `/runtime/core*.json`) must
   equal `v2.3.0-rc3-a1-bw50-cores` (`4a676578` / `e9c457cb` / `38e130e2`, verified 11:35Z) → `$A/cores-bw50/*`. If W-A1
   was rolled back (`ff105182` / `348f2c70` / `dfab3186`) → `$A/cores/*` (bw25). Anything else → STOP and re-render.
   `$C $OLD --env-file $GEN_TLS --env-file $SECRETS config rust_core` config-hash must equal the live label.
3. Live inputs still equal the render base: OKX ingestor `4cb59e29`, Binance `021dd5dc`, kn bundle `924951cd`, realm
   bundle `c5e21bb7`, Query acquisition `81d001aa`, catalogue `6f1699d4`, TS r13 `cfc20e29`, binding r25 `980123da`.
   Any difference → STOP.
4. Roles: `docker inspect` image/StartedAt/RestartCount of all 17 DL roles and the writer into `$WIN/pre-roles.txt`;
   writer image = `$IMG_W`, `DATA_LAYER_V2_JWT_MANIFEST_REVISION=13`.
5. `hb | tee $WIN/pre-hb.json` (expect demanded 104, ready 104, exec 93, continuity 11/11);
   `PRE_EXEC=<execution_ready>`.
6. Baselines (lock not yet taken, 10 min, read-only): DL CPU median (cgroup sampler
   `v213-goals/dl-coverage-150-cpu-test-20261116T1033Z/cpu_window.py 60` x 10) → `D0`; writer freshness
   (`p184-window-b-wb2-20261113T1732Z/freshness.py`, 10 min) → `fresh-pre.json`; quota counter (`dlcov/quota_rate.py
   trading-system.paper.stable 5`) → `quota-pre.jsonl`.
7. Sandbox G1 / W05 / Portal containers: record StartedAt + image (must be unchanged at the end).

## 2. Steps (T-0 = START from the coordinator; lock taken)

**S1 cores, all three at once** (as window A D1; they must know the 213 new bindings before the ingestor emits):
```bash
capture rust_core rust_core_2 rust_core_3; mark S1
$C $NEW --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate rust_core rust_core_2 rust_core_3
sleep 90; coreprog 90s | tee $WIN/s1-cores.txt; sleep 60; coreprog 60s | tee -a $WIN/s1-cores.txt; hb
```
Pass: 3 running, RestartCount 0, mounts under `v2.3.0-stage-b-cores`, the started line shows the live `batch_wait_ms`
(50 after W-A1), scope_q 0, oos 0, quarantines not growing, ws_max < 250 ms within 10 min, writer ready == demanded.
Abort: `$C $OLD --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate rust_core rust_core_2 rust_core_3`.

**S2 projectors, one at a time** (rc3 image unchanged; amendment 4' holds: never the old projector image):
as window A D2 with `$NEW`. Pass: healthy, both on one new `bundle_sha256` (= `$A` kn bundle hash), errors {},
ownership_lost 0, rebuilds_started 0, latest_applied rising, 0 `ENVELOPE_NOT_CANONICAL`, hb ready == demanded.
Rollback per role: `$C $OLD --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate <role>`.

**S3 stream, S4 Query, one at a time (reader TLS), writer still RUNNING on JWT 13:**
as window A D3/D4 with `$NEW`. Pass: 4 healthy; both streams on the new realm bundle hash; both Query read
`trading-system.paper.stable-r14.yaml` (`grep -m1 -E '^\s+revision: 14'`); writer since S1: 0 auth refusals
(accept-set 13..14 proven live), 0 `quota is exhausted`, 0 `CURSOR_EXPIRED|AUTHORITY_MISMATCH|RESUME_GENERATION_MISMATCH`,
ready == demanded, exec >= PRE_EXEC. Paper roles `ts-gateway`, `ts-risk`, `ts-paper` (JWT 13, r25, not recreated): 0
auth refusals in their logs since S1.
Rollback per role: `$C $OLD --env-file $READER_TLS --env-file $SECRETS up -d --no-deps --force-recreate <role>`.

**S5 writer stop (declared Paper outage starts):**
```bash
docker stop --time 30 $W; mark Wpre; docker inspect $W --format '{{.State.Status}} {{.Image}}'   # exited, 9dfef8a8
```

**S6 OKX ingestor only (Binance is not recreated):**
```bash
capture ingestor_okx_swap; T=$(date -u +%FT%TZ)
$C $NEW --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate ingestor_okx_swap
sleep 60; docker logs --since $T $P-ingestor_okx_swap-1 2>&1 | grep -m1 qdl_native_raw_ingestor_started | tee $WIN/s6-started.json
docker logs --since $T $P-ingestor_okx_swap-1 2>&1 | grep qdl_native_raw_ingestor_progress | tail -1 | tee $WIN/s6-progress.json
docker logs --since $T $P-ingestor_okx_swap-1 2>&1 | grep -ci 'subscription.*reject'; coreprog 2m | tee $WIN/s6-cores.txt
for q in query_kn_1 query_kn_2; do docker exec -i $P-$q-1 python -B - --min-pinned 21 < /home/bobby/.worktrees/dl-stage-b-20261116/scripts/v230_session_revision_check.py | tee $WIN/s6-d5a-$q.json; done
```
Pass: bindings 465, lane_plan PINNED, `latest_state_flush_ms_by_feed` `{"MARK_INDEX":500,"QUOTE":50}`, lanes
okx-public-001..004 and okx-business-001/002 with the same names and feeds plus exactly `okx-public-005`
(MARK_INDEX), none over 100 (003 QUOTE 97, 004 MARK_INDEX 100, 005 MARK_INDEX 94), 0 rejected, cores scope_q 0 /
oos 0, D5a PASS on both Query (config_revision 23 records accepted with pinned 21/22).
Rollback: `$C $OLD --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate ingestor_okx_swap`.
Bound: S6 green within 15 min of `T_Wpre`, else section 4.

**S7 writer BOOTSTRAP then STRICT on its CURRENT image, coupled env (CLAUDE.md §4.1).**
The tool writes the full env (DSN included) to a 0600 env-file and never to argv or stdout (`p184_writer_recreate.py`
docstring, Boundary). `OKX_NEW` = the live `MARKET_DATA_OKX_SYMBOLS` value with the 71 symbols of
`$A/symbols-stageB71.json` appended in file order (97 entries; build it with python from the inspect, never by hand).
```bash
W_NEW=(--set-env DATA_LAYER_V2_JWT_MANIFEST_REVISION=14
       --set-env DATA_LAYER_V2_RELEASE_MANIFEST_SHA256=<consumer_environment sha from $A/report.json>
       --set-env MARKET_DATA_OKX_SYMBOLS=$OKX_NEW
       --mount-source /run/secrets/qdl-v2-binding/trading-system.paper.stable.json=$R/v2.3.0-stage-b-ts/trading-system.paper.stable-r26.json)
cd $TT && python3 -B scripts/p184_writer_recreate.py --expect-image $IMG_W --allow-stopped --require-resume 0 "${W_NEW[@]}" \
   --min-execution-ready $((PRE_EXEC + 130)) --deadline 300 --convergence-s 120 --dry-run
cd $TT && python3 -B scripts/p184_writer_recreate.py --expect-image $IMG_W --allow-stopped --require-resume 0 "${W_NEW[@]}" \
   --min-execution-ready $((PRE_EXEC + 130)) --deadline 300 --convergence-s 120 | tee $WIN/w1.txt
( setsid nohup timeout 1900 python3 -B /tmp/claude-1001/-home-bobby/178e5b10-f053-4be7-9cc5-bcb710dcc977/scratchpad/dlcov/quota_rate.py trading-system.paper.stable 30 > $WIN/quota-per-minute.jsonl 2>&1 < /dev/null & )
cd $TT && python3 -B scripts/p184_writer_recreate.py --expect-image $IMG_W --require-resume 1 \
   --min-execution-ready $((PRE_EXEC + 130)) --deadline 300 --convergence-s 120 | tee $WIN/w2.txt
mark W2; hb | tee $WIN/w2-hb.json
```
- `--min-execution-ready PRE+130`: 71 instruments x 2 feeds = 142 new slices; the 12-slice allowance is the same
  shortfall stage A accepted for MARK slices still converging. The coordinator may lower it to PRE+100 if convergence
  stalls, recorded.
- If W1 refuses with BOOK `EXECUTION_ORDERING_AUTHORITY_MISMATCH` or native `RESUME_GENERATION_MISMATCH`: stop the
  writer, run `cd $TT && python3 -B scripts/p184_writer_scope_reset.py --symbols-from $W --out $WIN/scope-reset-plan`
  (dry run), then with `--apply` (exact keys, never MARK, read-back 0), then W1 again. Retained BOOK lineage is pinned
  per binding (v2.3.0 2d), so this is the fallback, not the plan.

## 3. Gates (soak 15 min after W2; all must hold)

| Gate | Pass | Source |
|---|---|---|
| G1 bootstrap | W1 and W2 `VERIFIED`; demanded = ready = 246; continuity == PRE (11/11) within 5 min | tool journal, `hb` |
| G2 quota | 0 `consumer request quota is exhausted` in writer logs since S1; every per-minute sample < 31,000 (expected 6,000-12,000) | `docker logs`, `$WIN/quota-per-minute.jsonl` |
| G3 auth | 0 auth refusals in writer and in ts-gateway / ts-risk / ts-paper since S1 | `authgrep` |
| G4 MARK freshness | writer MARK write-age p99 <= 1,000 ms and max < 2,000 ms over the soak, 0 missing (pre 723 ms p99, 2026-11-16 11:0xZ); QUOTE p99 <= fresh-pre p99 + 100 ms | `freshness.py` + `freshness_p99.py` |
| G5 coverage | TS `scripts/market_coverage_gate.py` PASS 96/96 (command below) | receipt in `$WIN` |
| G6 DL CPU | 15-min median of the 17 DL roles <= 6.0 vCPU (cgroup sampler, Kafka included) | `cpu_window.py` x 15 |
| G7 roles | no RestartCount increase on any DL role; G1/W05/Portal StartedAt + image unchanged; ingestor_binance_usdm, bar_edge, kafka1-3, stable_redis, market_cache untouched | `docker inspect` |

G4 is not the W-B2 "+100 ms" rule: stage B moves the OKX MARK_INDEX flush from 50 to 500 ms for ALL OKX MARK lanes
(the flush map is per ingestor config and per feed, not per instrument), so MARK write age rises by design. The owner
must accept G4 as written (packet section 6, decision D2); if not, render with `MARK_INDEX: 50` instead (packet).

Coverage gate (G5), the same invocation shape as the 10:01Z run, inside the g34 image, read-only:
```bash
docker run --rm --network container:redis_marketdata --cpus 1 --cpu-shares 256 --memory 512m -e PYTHONDONTWRITEBYTECODE=1 \
  -v /home/bobby/.worktrees/ts-v212-sdk-release-20261116:/src:ro -v $A:/a:ro \
  -v /home/bobby/.local/state/trading-system/paper-hosting/bindings/hosting-paper-crossec-v1-1584bb76.json:/b/bindings.json:ro \
  -v $WIN:/out -w /src --entrypoint python sha256:33818ebd726884fa906d0647b5adc67f5c45688ebfb74ce3ad392b59375ab934 -B scripts/market_coverage_gate.py \
  --bindings /b/bindings.json --consumer-manifest /a/consumer-realms/trading-system.paper.stable-r14.yaml \
  --sealed-binding /a/bindings/trading-system.paper.stable-r26.json --market-redis-url redis://127.0.0.1:6379/1 \
  --observe-seconds 30 --source-commit 34fc619254a59599aea67719e8d71b0c70c2dd30 --output /out/coverage-gate-stageb.json
```
(Copy the exact flags of the 10:01Z receipt `coverage-gate-g34-20261116T1001Z.json` `inputs`/`runtime_pins` if they
differ; the redis URL form is not checked here.) Pass: `status PASS`, `passing_count 96`, `failing_count 0`.

Then release the lock (`rm -f ~/.local/state/host-measurement-window.lock`) and record `$WIN/*` with `SHA256SUMS`.

## 4. Coupled rollback to r13 / r25 (reverse order; the current state is the target)

1. Writer: `docker stop --time 30 $W`; BOOTSTRAP then STRICT on the same image with the old env:
   `--set-env DATA_LAYER_V2_JWT_MANIFEST_REVISION=13 --set-env DATA_LAYER_V2_RELEASE_MANIFEST_SHA256=9ba2264da2d5c2729376569c4ee2328cf7ad665f1aa209215f94892a5df47248
   --set-env MARKET_DATA_OKX_SYMBOLS=<the 26-symbol live value from pre-roles> --mount-source /run/secrets/qdl-v2-binding/trading-system.paper.stable.json=$R/v2.2.6-stage-a-ts/trading-system.paper.stable-r25.json`,
   `--allow-stopped --require-resume 0`, then `--require-resume 1`, `--min-execution-ready $PRE_EXEC`. Never `docker start`.
   On `AUTHORITY_MISMATCH` / `RESUME_GENERATION_MISMATCH`: scope reset (section 2 S7 note) with the r14 scope, then retry.
2. OKX ingestor: `$C $OLD --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate ingestor_okx_swap`
   (back to 252 bindings, config_revision 22; the old config has no `okx-public-005`).
3. Query x2, stream x2: `$C $OLD --env-file $READER_TLS ...` one at a time.
4. Projectors x2 (same rc3 image, old kn bundle `924951cd`): `$C $OLD --env-file $GEN_TLS ...` one at a time.
5. Cores, all three at once: `$C $OLD --env-file $GEN_TLS ...` (the live W-A1 core set, not the rc2 bw25 set).
6. Kafka records already written for the 142 new bindings stay on the topics; old cores ignore them as out of scope
   (counted in `oos`), old projectors have no product for them. No offset reset or seek.
7. Verify: writer 104/104, exec >= PRE_EXEC, continuity 11/11; coverage gate back to 25/96 (the pre-window state).

## 5. Blast radius

Recreated: rust_core x3, market_projector x2, stream_kn x2, query_kn x2, ingestor_okx_swap, the TS writer
`market_data_service_p184w` (same image `9dfef8a8`, env + binding mount only). Paper outage: from S5 to W2 VERIFIED
(window A: about 12 min).

Not touched: kafka1-3, stable_redis, market_cache, binance_bar_edge (its runtime pair is unchanged in meaning; it keeps
mounting `v2.3.0-rc2`), ingestor_binance_usdm, every other consumer manifest and JWT (alpha r16 etc.), ts-gateway /
ts-risk / ts-paper (JWT 13 / r25 stay valid under the accept-set), PM, Portal, Sandbox, `shared/symbols.json`, every DL
image digest, the compose file, Redis/Postgres data (except the writer's own keys and the scope-reset fallback).

## 6. Estimate

T-30 checks + baselines 30 min; S1-S4 20 min; outage S5-S7 10-15 min; soak + gates 20 min; total about 75-90 min with
the lock held about 60 min.
