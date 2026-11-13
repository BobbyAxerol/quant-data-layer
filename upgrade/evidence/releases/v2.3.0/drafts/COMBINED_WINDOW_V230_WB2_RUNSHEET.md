# Run sheet: combined window DL v2.3.0 rc2 + TS writer W-B2 (packet `6798ca0`)

This sheet executes `COMBINED_WINDOW_V230_WB2_PACKET.md`. The gates, the rollback and the not-touched list are the
packet's (sections 4, 4a, 5, 6, W). It adds only the exact commands.

- The owner gave GO on 2026-11-13: start as early as possible, same blast radius and rollback.
- Nothing starts without the coordinator's START.
- Any gate FAIL means the section 5 rollback, without asking again.
- Every shell: `source /tmp/claude-1001/-home-bobby/178e5b10-f053-4be7-9cc5-bcb710dcc977/scratchpad/v230/window-rc2/env.sh`.

## 0. Prepared before T-30 (done 2026-11-13 ~12:27Z; no running container reads any of it)

**Pinned set** `$A`: 21 files with `SHA256SUMS`.
- out6 runtime (Query pair, bar_edge pair, migration record, declaration index, universes), the kn and realm
  bundles, alpha r16, three realm copies of the image manifests (monitoring-multivenue, alpha-vn-paper,
  reference-l2-stable), cores x3 and ingestors x2.
- The window compose is the rc2 `deploy/compose/qdl-v2-stable-kn.yml` (`832c02c4`) plus 4 edits:
  1. Query `QDL_STABLE_CONSUMER_MANIFESTS` (both instances) names `alpha.okx.paper.stable-r16.yaml`.
  2. bar_edge `QDL_STABLE_SOURCE_BINDINGS` / `QDL_STABLE_ACQUISITION_BINDINGS` =
     `/runtime/bar-edge-{source,acquisition}-bindings.yaml` (the raw-lineage pair, section 3a).
  3. bar_edge `QDL_STABLE_BAR_CHECKPOINT_MIGRATION=/runtime/bar-edge-checkpoint-migration.json`.
  4. bar_edge core backpressure stage 20000 -> 2000 (M2).
- The declaration index guard stays OFF (tool agent: Query also loads non-indexed unchanged manifests).
- Env `qdl-v2-stable-kn.v2.3.0-rc2.env` is the r13 env with:
  - the 6 role images set to the rc2 digests (Rust `58bdd282`, Python `03de529a`);
  - the 4 dirs set to the new dirs below;
  - `QDL_KN_STATE_DIR` unchanged;
  - `QDL_PAPER_REALM_MANIFEST` unchanged (TS r13).

**New dirs** (sudo install, modes and owners as the stage-A dirs):

| Dir | Content |
|---|---|
| `$R/v2.3.0-rc2` | copy of `v2.2.6-stage-a`, with `bundle.json` = out6 kn bundle and `runtime/` = out6 |
| `$R/v2.3.0-rc2-realm` | copy of `v2.2.6-stage-a-realm-r13`, plus alpha r16, the out6 realm bundle and the 3 image manifests |
| `$R/v2.3.0-rc2-cores`, `$R/v2.3.0-rc2-ingestor` | out6 cores and ingestor configs |
| `$R/v2.3.0-rc2-compose` | window yml + env |
| `$R/v2.2.5-rollback-compose` | durable copy of `data-layer-dev` compose + v2.2.5 env, for the bar_edge / Binance ingestor rollback (`50c1f014` / `c6770d5d`) |

Checks done at prep time:
- every installed file `cmp`-equal to `$A`;
- TS r13 unchanged;
- `compose config -q` OK with both TLS env files;
- live inputs equal the render base (cores `22e139ff`/`26d6dad9`/`c962191b`, OKX ingestor `d62f36b4`, Binance
  `5833bc4e`, realm bundle `5180a904`, kn bundle `9136beba`);
- writer tools: clean `git archive` of TS `2d2e18eb` in `$TT`, all 6 blobs equal the packet W.9 list.

## 1. T-30 checklist (read-only, except the lock; all values into `$WIN/t30.txt`)

```bash
{ date -u +%FT%TZ; uptime; df -h /; } | tee $WIN/t30.txt                       # load: 1-min < 30 (record); disk >= 100 GiB and >= 20 %
( cd $A && sha256sum -c --quiet SHA256SUMS ) && echo "pins OK" | tee -a $WIN/t30.txt
cat ~/.local/state/host-measurement-window.lock 2>/dev/null | tee -a $WIN/t30.txt   # AGENTS.md 34a: absent, or until_utc in the past
docker ps --format '{{.Names}} {{.Status}}' | grep -E '^(ts-sbx-|w5d-|c2w05-)|rcert2' | tee -a $WIN/t30.txt   # G1/W05 state; R-cert 2 S-step not running (coordinator holds S2+)
for c in rust_core rust_core_2 rust_core_3 market_projector_1 market_projector_2 stream_kn_1 stream_kn_2 query_kn_1 query_kn_2 ingestor_okx_swap ingestor_binance_usdm binance_bar_edge; do
  echo "$c $(docker inspect $P-$c-1 --format '{{.Image}} {{.State.StartedAt}} {{.RestartCount}}')"; done | tee $WIN/pre-roles.txt   # = packet section 1 (any image difference aborts)
docker inspect $W --format '{{.Image}} {{.State.Status}}' | tee -a $WIN/t30.txt                   # 7332e85a running
hb | tee $WIN/pre-hb.json                                                                       # PRE: 104/104/93, continuity 11/11
for s in market_projector_1 market_projector_2 stream_kn_1 stream_kn_2; do echo "$s $(docker logs $P-$s-1 2>&1 | grep -o '"bundle_sha256":"[0-9a-f]*"' | tail -1)"; done | tee $WIN/pre-bundles.txt
coreprog 2m | tee $WIN/pre-cores.txt
sudo -n cp -p $STATE_CK $WIN/checkpoint-t30-reference.json                                       # reference only; the rollback copy is taken at D6 after the stop
```

- W05 r6 must be flat and idle (FT08 r4 finished or stopped safely); the coordinator confirms.
- **Measurement lock:** take it at T-0 for the window duration only, release it right after the CPU gate (AGENTS.md
  34a, CLAUDE.md 3b).
- **CPU baseline (packet 7a):** the D0 10-min median of all `qdl_v2_stable_candidate-*` roles. Run it at T-30 so it
  does not extend the window:
  `for i in $(seq 1 10); do docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' | grep qdl_v2_stable_candidate | awk '{gsub("%","",$2); s+=$2} END {print s/100}'; sleep 50; done | tee $WIN/cpu-d0.txt`
  The median is the headroom base (6.0 minus the median).

## 2. Steps (T-0 = START from the coordinator)

```bash
mark T0
printf '{"owner":"DL integration agent (combined window v2.3.0 rc2)","purpose":"production window, CPU gate","started_utc":"%s","until_utc":"%s"}\n' \
  "$(date -u +%FT%TZ)" "$(date -u -d '+3 hours' +%FT%TZ)" > ~/.local/state/host-measurement-window.lock   # released right after the CPU gate: rm -f ~/.local/state/host-measurement-window.lock
```

**D1 cores, all three at once (simultaneous replace).**
```bash
mark D1; $C --env-file $NEW_ENV --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate rust_core rust_core_2 rust_core_3
for c in rust_core:core.json rust_core_2:core-002.json rust_core_3:core-003.json; do r=${c%%:*}; m=${c##*:}
  docker inspect $P-$r-1 --format "{{.Name}} {{.Image}} {{.RestartCount}} {{range .Mounts}}{{if eq .Destination \"/runtime/$m\"}}{{.Source}}{{end}}{{end}}"; done | tee $WIN/d1-inspect.txt
sleep 90; coreprog 90s | tee $WIN/d1-cores.txt; sleep 60; coreprog 60s | tee -a $WIN/d1-cores.txt
```
- **Pass:**
  - 3 running, RestartCount 0, mounts under `v2.3.0-rc2-cores`;
  - all 3: scope_q 0, oos 0, quarantines not growing between the 2 samples;
  - ws_max < 250 ms within 10 min;
  - writer hb ready == demanded (PRE).
- **Abort:** all three at once:
  `$C13 --env-file $ENV13 --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate rust_core rust_core_2 rust_core_3`.

**D2 projectors, one at a time.**
```bash
for s in market_projector_1 market_projector_2; do T=$(date -u +%FT%TZ)
  $C --env-file $NEW_ENV --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate $s
  health $s || { echo "ABORT $s"; break; }
  docker logs --since $T $P-$s-1 2>&1 | grep -m1 '"event":"started"' | grep -o '"bundle_sha256":"[0-9a-f]*"'
  docker exec $P-$s-1 cat /status/status.json | python3 -c 'import sys,json;d=json.load(sys.stdin);b=d["stage_b"];print("'$s'","errors",d["errors"],"ownership_lost",b["ownership_lost"],"rebuilds_started",b["rebuilds_started"],"latest_applied",b["latest_applied"])'
done | tee $WIN/d2-projectors.txt
docker logs --since $(cat $WIN/T_D1) $P-market_projector_1-1 2>&1 | grep -c ENVELOPE_NOT_CANONICAL; hb
```
- **Pass:**
  - both healthy;
  - bundle_sha256 equal on both and different from pre-bundles;
  - errors {}, ownership_lost 0, rebuilds_started 0;
  - latest_applied rising 30 s later;
  - 0 ENVELOPE_NOT_CANONICAL;
  - hb ready == demanded.
- **Rollback per role:** `$C13 --env-file $ENV13 --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate <role>`.

**D3 stream, then D4 Query, one at a time (reader TLS).**
```bash
for s in stream_kn_1 stream_kn_2 query_kn_1 query_kn_2; do
  $C --env-file $NEW_ENV --env-file $READER_TLS --env-file $SECRETS up -d --no-deps --force-recreate $s
  health $s || { echo "ABORT $s"; break; }; done
sleep 35; for s in stream_kn_1 stream_kn_2; do echo "$s $(docker logs --since 1m $P-$s-1 2>&1 | grep -o '"bundle_sha256":"[0-9a-f]*"' | tail -1)"; done | tee $WIN/d3-streams.txt
for s in query_kn_1 query_kn_2; do docker exec $P-$s-1 sh -c 'grep -m1 -E "^\s+revision:" /consumer-realms/alpha.okx.paper.stable-r16.yaml'; done
docker logs --since $(cat $WIN/T_D1) $W 2>&1 | grep -cE 'CURSOR_EXPIRED|EXECUTION_ORDERING_AUTHORITY_MISMATCH|RESUME_GENERATION_MISMATCH'
docker logs --since $(cat $WIN/T_D1) $W 2>&1 | authgrep | wc -l; hb | tee $WIN/d4-hb.json
```
- **Pass:**
  - 4 healthy;
  - both streams on one new bundle_sha256;
  - both Query see r16;
  - writer: 0 CURSOR_EXPIRED / mismatch lines, 0 auth refusals, ready == demanded, execution_ready >= PRE;
  - Query `/health/dependencies` declaration `image_files=0` (read through `docker exec $P-query_kn_1-1`; the
    endpoint is as in the packet D4).
- **Rollback per role:** `$C13 --env-file $ENV13 --env-file $READER_TLS --env-file $SECRETS up -d --no-deps --force-recreate <role>`.

**W-pre writer stop.**
```bash
docker stop --time 30 $W; mark Wpre; docker inspect $W --format '{{.State.Status}} {{.Image}}'   # exited 7332e85a
```

**D5 ingestors (OKX first, then Binance).**
```bash
for s in ingestor_okx_swap ingestor_binance_usdm; do T=$(date -u +%FT%TZ)
  $C --env-file $NEW_ENV --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate $s
  sleep 60; docker logs --since $T $P-$s-1 2>&1 | grep -m1 qdl_native_raw_ingestor_started | tee $WIN/d5-$s-started.json \
   | python3 -c 'import sys,json;d=json.loads(sys.stdin.read());print("'$s'","bindings",d["bindings"],"lane_plan",d["lane_plan"],"linger",d["producer"].get("linger_ms"),"timeout",d["producer"].get("request_timeout_ms"))'
  docker logs --since $T $P-$s-1 2>&1 | grep qdl_native_raw_ingestor_progress | tail -1 \
   | python3 -c 'import sys,json;print(json.dumps([(l["lane"],l.get("subscriptions"),l.get("frames_per_s")) for l in json.loads(sys.stdin.read())["lanes"]]))' | tee $WIN/d5-$s-lanes.json
  docker logs --since $T $P-$s-1 2>&1 | grep -ci 'subscription.*reject'; done
coreprog 2m | tee $WIN/d5-cores.txt
```
- **Pass:**
  - lane_plan PINNED, linger null, timeout 30 s;
  - OKX lanes: okx-public-001..004 and okx-business-001 (same names) plus exactly `okx-business-002`, none over 100;
  - Binance lanes unchanged;
  - 0 rejected;
  - cores scope_q 0, oos 0;
  - book resync complete (writer is stopped; check `ingestor_okx_swap` BOOK lane frames > 0).
- **Rollback:**
  - OKX: `$C13 --env-file $ENV13 --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate ingestor_okx_swap`.
  - Binance: `$C225 --env-file $ENV225 --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate ingestor_binance_usdm`.
- **Bound:** D5 + D6 green within 30 min of `T_Wpre`, else roll back.

**D6 bar_edge.**
```bash
mark D6; docker stop --time 30 $P-binance_bar_edge-1
sudo -n cp -p $STATE_CK $STATE_CK.pre-v230 && sudo -n sha256sum $STATE_CK $STATE_CK.pre-v230 | tee $WIN/d6-checkpoint.txt
$C --env-file $NEW_ENV --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate binance_bar_edge
sleep 60; docker logs --since $(cat $WIN/T_D6) $P-binance_bar_edge-1 2>&1 | grep -E 'checkpoint crosses|cache generation changed|history incomplete' | tee $WIN/d6-crossing.txt
docker exec $P-binance_bar_edge-1 cat /var/lib/qdl-kn/heartbeat/bar-edge.json | tee $WIN/d6-heartbeat.json
```
- **Pass:**
  - `checkpoint crosses ... crossed=True retained=640 ... dropped=0 migration_record=True`;
  - heartbeat declaration `image_files=0`;
  - "history incomplete" lines only for retained bindings whose demand rose (record the count);
  - first derived closes on md.bars.v2 / market cache for 94 x {1h, 12h} and 25 x {3m..1D};
  - parity auditor 0 mismatches.
- **HOLD:** a `cache generation changed; bounded bootstrap required` line means W does not start. Decide with the
  coordinator: continue, or roll back bar_edge.
- **Rollback (bar_edge alone):**
  ```bash
  docker stop --time 30 $P-binance_bar_edge-1
  sudo -n cp -p $STATE_CK.pre-v230 $STATE_CK
  $C225 --env-file $ENV225 --env-file $GEN_TLS --env-file $SECRETS up -d --no-deps --force-recreate binance_bar_edge
  ```

**Bootstrap watch (from D6 to the bootstrap end; record, the gates decide).**
```bash
( setsid nohup timeout 7200 bash -c 'source '$D'/env.sh; while :; do date -u +%FT%TZ; coreprog 1m; \
   docker exec '$P'-binance_bar_edge-1 cat /var/lib/qdl-kn/heartbeat/bar-edge.json; \
   docker logs --since 1m '$P'-binance_bar_edge-1 2>&1 | grep -cE "derived BAR gap|native final BAR recovery|history paused"; sleep 60; done' \
   > $WIN/bootstrap-watch.log 2>&1 < /dev/null & )
```
- The bootstrap is done when the heartbeat `published + short == bindings` and no "history" retries remain.
- Holes older than 10 min are recorded (the packet section 3 hold rule).

**W-gate, then W.**
- W-gate: 5 consecutive 1-min samples where every core `ws_max` < 1000 ms (stricter than p99; the progress log has
  max/mean only), and the bar_edge history gate is open (heartbeat `gate_closed` 0, no "history paused" line).
- If it is not green, wait. The latest point is the bootstrap end + gap drain + 20 min, else roll back.

```bash
cd $TT && python3 -B scripts/p184_writer_recreate.py --expect-image $IMG_W_OLD --image $IMG_W_NEW --allow-stopped --require-resume 0 \
  --set-env MARKET_DATA_NATIVE_GAP_BOOTSTRAP_FEEDS=QUOTE --min-execution-ready <PRE exec> --deadline 180 --convergence-s 120 --dry-run
cd $TT && python3 -B scripts/p184_writer_recreate.py --expect-image $IMG_W_OLD --image $IMG_W_NEW --allow-stopped --require-resume 0 \
  --set-env MARKET_DATA_NATIVE_GAP_BOOTSTRAP_FEEDS=QUOTE --min-execution-ready <PRE exec> --deadline 180 --convergence-s 120 | tee $WIN/w1.txt
cd $TT && python3 -B scripts/p184_writer_recreate.py --expect-image $IMG_W_NEW --require-resume 1 --min-execution-ready <PRE exec> --deadline 180 --convergence-s 120 --dry-run
cd $TT && python3 -B scripts/p184_writer_recreate.py --expect-image $IMG_W_NEW --require-resume 1 --min-execution-ready <PRE exec> --deadline 180 --convergence-s 120 | tee $WIN/w2.txt
mark W2; hb | tee $WIN/w2-hb.json
```
- **Pass:** packet W.4.
  - W1 and W2 VERIFIED;
  - continuity == PRE within 5 min;
  - 10 min soak clean;
  - G1/W05 StartedAt and image unchanged.
- **Rollback:** packet W.5.

**D6b + CPU gate.**
- At bootstrap end + 15 min:
  - outstanding derived gaps (gap lines without a `gap healed`) = 0 → else FAIL;
  - record the recorded and healed counts;
  - the gap-repair backlog has drained.
- At bootstrap end + 30 min: CPU median (same sampler as D0, Kafka included) <= D0 median + 0.3 vCPU (owner delta gate, this window only), else roll back (section 7a).
- Then release the measurement lock.
- At + 6 h (~02:00Z, the coordinator's live-controller agent): any `derived BAR gap kept` line → FAIL → section 5
  rollback, outside 23:30-00:05Z.

## 3. Full coupled rollback (packet section 5, reverse order)

1. W.5.
2. D6 rollback above.
3. D5 rollbacks above.
4. Query, stream, projectors, cores, each with `$C13 --env-file $ENV13` plus its own TLS env.
   - Cores all three at once.
   - No offset reset or seek (M4). Record the last derived offsets per partition first.
5. The writer from a stopped state: BOOTSTRAP then STRICT on `7332e85a` (W.5); never `docker start` it.

## 4. Evidence

- `$WIN/*` and the tool run dirs into `upgrade/evidence/releases/v2.3.0/window-<ts>/` with `SHA256SUMS`.
- `README.CURRENT` in `$CD` after a PASS, pointing every DL role at this dir, as the r13 one did.
