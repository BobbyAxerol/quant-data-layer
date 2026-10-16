# v2.2.4 - final G12 / G13 reruns: ready-to-run commands and read-only pre-flight

Status: PREPARED, nothing run (2026-10-16, Claude subagent). Drafted while the host lock of
`claude-burst-b16-a1parity` was held: only file edits, host-python unit tests, `docker inspect`/`ps`/`image inspect`/
`history` and pure dry runs; no container, build, Kafka/Redis/Postgres access or plan edit.
Basis: [g12-g13-inheritance.md](g12-g13-inheritance.md) (minimal rerun list), oracle commits `d54264e` / `945b99f` (G12
v3, integrity report-only, WriterClone precheck, `--auto-leader-rebalance`), `18c888f` (internal-topic parity),
`2169c26` (DR08), `f7d6afa` (DR05), and `13ef59c` (this preparation: production parity from docker inspect).
Machine-readable companions: [g12-image-set-v224.json](g12-image-set-v224.json),
[g13-roles-v224.json](g13-roles-v224.json).

## Deployed set the reruns mirror (docker inspect 2026-10-16 ~11:10Z)

| Role | Image ID | Note |
| --- | --- | --- |
| writer `market_data_service_p184w` | `cb5216b8` B17 (`p184-writer-cand-b17-68f2572f`, TS `68f2572f`) | native TRADE,QUOTE; `MARKET_DATA_NATIVE_APPLY_MAX_ROWS=16`, `DATA_LAYER_V2_BOOK_SNAPSHOT_BATCH_MAX_WAIT_MS=0`; restarts 0 |
| rust_core x3 | `7c848aad` rc22 | |
| stream_kn x2 | `baa72c96` | |
| query_kn x2 | `b9ca6c36` = `qdl-v2-python:2.2.3-rc19-4d3476c` | also the harness python image |
| projectors x2 | `ce629061` rc14 | no IDR-107: a projector exit on the broker stop is a G12 stop condition |
| ingestors x2 / bar edge | `7fba2577` / `08f356a8` | G13 role map only |
| kafka x3 | `9516fb76` | no `KAFKA_AUTO_LEADER_REBALANCE_ENABLE` env -> Kafka default **true** (decision B pending) |
| Kafka topic configs | C: `__consumer_offsets` delete.retention.ms 600000 + segment.ms 60000, `__transaction_state` segment.ms 600000; A': md.latest.v2 delete.retention.ms 21600000 | from the committed plans; production effective values are confirmed by pre-flight 6 |

`deployed_mismatches(deployed_inventory(), roles-v224 + writer B17)` = `[]` [checked].

## Apparatus changes in 13ef59c (tested locally: host `python3 -B -m unittest`)

- G12 WriterClone precheck takes the writer from the clone's run-time `docker inspect` (image + `MARKET_DATA_NATIVE_*`
  + non-secret tuning keys); no B16 constant is left. A `--production-parity` snapshot pins the writer seen before the
  run; the run stops if the deployed writer changed.
- G12 mirrors production Kafka from `--parity-snapshot` (docker inspect + committed plans): brokers'
  `auto.leader.rebalance.enable` from the kafka1-3 env, decision C applied to the run's internal topics before the
  baseline (STOP unless MATCH), A' on the latest topic at creation; `broker_config.production_parity` recorded;
  `g12_receipt_creditable` requires MATCH and, with a current snapshot, equality with production (no final credit for a
  run on a superseded profile).
- WriterClone Redis CPU recorded and checked: uncapped or >= 1.0 CPU = parity (TS 3a09fe19). The G12 and G13 clones
  start Redis without `--cpus` (TS `scripts/p184_dr05_isolated.py:242`) = production `redis_marketdata` (no quota), so no
  cap needed changing; the DL quota Redis (stable_redis analogue, 0.1 CPU) is not a writer Redis.
- G13 runner: the writer-clone steps (`dr05-upstream`, `dr05-transport`) now require the deployed inventory check, so
  a clone never pairs `--writer-image` with another build's env.
- `scripts/host_measurement_lock.py` (check / acquire / release, AGENTS.md 34a) replaces the inline lock loops.
- Tests: G13 68 OK; lock 6 OK; G12 175 run: the 13 failures/errors are the same image-only tests as before the change
  (`redis`/`google` modules absent on the host). The in-image run is pre-flight 3. The in-container
  `--apply-internal-configs` path (confluent-kafka `incremental_alter_configs`) runs for the first time in the G12 run:
  it stops the run before the baseline if it does not reach MATCH.

## Common setup (inside each run's own lock; same pins for all three runs)

```
DL=/home/bobby/.worktrees/data-layer-p184-closure; TSW=/home/bobby/.worktrees/trading-system-next-upgrade
DLSHA=$(git -C $DL log -1 --format=%H -- upgrade/evidence/releases/v2.2.4/g12-g13-final-rerun.md)  # this file's commit
RUN_DL=/home/bobby/.worktrees/data-layer-p184-final-run-${DLSHA:0:8}
TSSHA=68f2572fbc1c64520803cbfc1dfd22bfc5470cff        # B17 label revision; G13 TS scripts unchanged since f1b52f63
RUN_TS=/home/bobby/.worktrees/trading-system-final-run-68f2572f
B17=sha256:cb5216b877f1659f2a66d9169c4d08f91c1d0b8caee2e0dbe24546774d3d0949
EXPORT=~/.local/state/qdl-v2/dr05-export-20261010/public-export.json
test ! -e $RUN_DL && git -C $DL worktree add --detach $RUN_DL $DLSHA
test ! -e $RUN_TS && git -C $TSW worktree add --detach $RUN_TS $TSSHA
test -z "$(git -C $RUN_DL status --porcelain --ignored --untracked-files=all)"
test -z "$(git -C $RUN_TS status --porcelain --ignored --untracked-files=all)"
```
Cleanup after the last run: `git -C $DL worktree remove $RUN_DL` and `git -C $TSW worktree remove $RUN_TS`, without
`--force` (a refusal = the worktree got dirty: stop and report).

## 1. G12 full (3 faults, one harness lifetime) - interim now, final after decision B

```
cd $RUN_DL
UTC=$(date -u +%Y%m%dT%H%M%SZ); PHASE=interim                     # final: PHASE=final, after B is rolled
BASE=~/.local/state/qdl-v2/p184-g12-v224-$PHASE-$UTC; NS=p184-g12-dl-$(python3 -c 'import secrets;print(secrets.token_hex(8))')
OWNER="claude P18.4 G12 v224 $PHASE $UTC"
python3 -B scripts/host_measurement_lock.py acquire --owner "$OWNER" --purpose "G12 full 3 faults isolated ns $NS" --minutes 75
mkdir -m 700 $BASE && python3 -B scripts/p184_g12_isolated_fault.py --parity-snapshot $BASE/production-parity.json \
  --lock-owner "$OWNER"
REB=$(python3 -c 'import json,sys;print(str(json.load(open(sys.argv[1]))["kafka"]["auto.leader.rebalance.enable"]).lower())' $BASE/production-parity.json)
# interim: REB must print true (production today); final: false (after B). Anything else = stop.
python3 -B scripts/p184_g12_isolated_fault.py --run $BASE --export $EXPORT --namespace $NS \
  --image-set upgrade/evidence/releases/v2.2.4/g12-image-set-v224.json --query-code image \
  --auto-leader-rebalance $REB --production-parity $BASE/production-parity.json --ts-root $RUN_TS \
  --lock-owner "$OWNER" > $BASE/run.log 2>&1
python3 -B scripts/p184_g12_isolated_fault.py --credit $BASE                       # interim: oracle + MATCH only
python3 -B scripts/host_measurement_lock.py release --owner "$OWNER"
```
- Lock (D1, Codex follow-up 2026-10-16): `--parity-snapshot` and `--run` describe production Kafka through admin
  containers; each launch checks the host-measurement lock and refuses under any valid lock other than `--lock-owner`
  (run them inside the owner's own lock; credit-time snapshots too). `--image-set` must name every role (a release set
  for the final run never falls back to older defaults).
- Image set: core `7c848aad`, projector `ce629061`, stream `baa72c96`, python `b9ca6c36`, ts B17 `cb5216b8`, kafka
  `9516fb76`, redis `dfa18828`, postgres `cf78e766`, holder alpine:3.20 `d9e853e8`. `--run` refuses when the image set's
  ts role is not the snapshot's deployed writer, or `--auto-leader-rebalance` differs from the snapshot.
- Expected: 31-44 min (rc12 30.6, rc18-control2 38.4, rc10-full3 43.8), plus about 1 min for the C apply; harness
  deadline 60 min; lock 75 min. Teardown runs in `finally`; `--down $BASE` is the manual fallback.
- Interim (rebalance true) is the pre-B qualification run: it measures the deployed config as it is, including the
  leader-rebalance stall that made the v2 runs fail G2 max. **It is not the final credit.**
- Final credit, after B is rolled (all three brokers `KAFKA_AUTO_LEADER_REBALANCE_ENABLE=false`): rerun the block with
  `PHASE=final`. The snapshot then yields `REB=false`. Credit at that time against a fresh snapshot:
  `python3 -B scripts/p184_g12_isolated_fault.py --parity-snapshot $BASE/credit-snapshot.json --lock-owner "$OWNER" && python3 -B
  scripts/p184_g12_isolated_fault.py --credit $BASE --production-parity $BASE/credit-snapshot.json` (exit 0 only when
  verdict v3 PASS, three faults, `production_parity` MATCH and the run profile equals production's).
- Receipt fields to read: `evidence/verdict.json` (`pass`, `faults[*].parts`, `broker_config.production_parity`,
  `broker_config.internal_topics.{applied,before,after}`, `writer_precheck`), `evidence/writer-precheck.json`
  (`clone_env` == `deployed_env`, `redis_cpu.parity`, `writer_clone_source.sha256`), `evidence/teardown.json`.
- Not mirrored (G12 harness shape unchanged since rc12, recorded and not claimed): 1 core member at 0.5 CPU
  (production 3 members, 1.0/1.0/0.5), kafka 0.75 CPU each (production 1.75), latest topic 3 partitions without the
  production `segment.ms` 3600000 / `segment.bytes` 128 MiB, synthetic TEST_ONLY publisher at 4 Hz BOOK.

## 2. G13 isolated phase (dr05-upstream + dr08-isolated, 10 cells)

```
cd $RUN_DL
OWNER="claude P18.4 G13 isolated v224 $(date -u +%Y%m%dT%H%M%SZ)"
python3 -B scripts/host_measurement_lock.py acquire --owner "$OWNER" --purpose "G13 isolated dr05-upstream + dr08" --minutes 70
python3 -B scripts/p184_g13_final.py --phase isolated \
  --role-images upgrade/evidence/releases/v2.2.4/g13-roles-v224.json \
  --python-image-ref qdl-v2-python:2.2.3-rc19-4d3476c --writer-image $B17 \
  --ts-source-sha $TSSHA --dl-source-sha $DLSHA \
  --inputs ~/.local/state/trading-system/p184-20261005/runtime/p1/g13v5/inputs-g13v5-draft-noci.json \
  --ts-root $RUN_TS --dry-run            # expect exit 0, blocked {}, 2 steps; then the same without --dry-run
python3 -B scripts/host_measurement_lock.py release --owner "$OWNER"
```
- Steps: `dr05-upstream` 6 cells (GAP_REFUSAL / GENERATION_CHANGE_FENCED x BINANCE, OKX, OKX_INVERSE; 5.6 min
  measured, timeout 30 min); `dr08-isolated` 4 cells (QUALIFICATION, >= 5 restart samples, OF-G13-1 fix `2169c26`; 2.2
  min measured, timeout 25 min). Expected about 8-10 min; lock 70 min.
- The non-dry run first checks: DL run worktree HEAD == `$DLSHA` with no tracked edit, the deployed inventory ==
  roles-v224 + writer B17 (DR08 copies the deployed core config; dr05-upstream clones the deployed writer env), and
  `qdl-v2-python:2.2.3-rc19-4d3476c` resolves to `b9ca6c36`. Receipts in a new `~/.local/state/qdl-v2/p184-g13-<UTC>/`.
- Dry run at drafting [checked]: exit 0, `blocked {}`, coverage 76/76, dr05-upstream 6 + dr08 4 cells, est 18 min,
  timeouts 55 min.

## 3. G13 runtime window (66 cells: dr01, dr02-dr03, dr04-dr06 views, dr05 transport, dr07)

Owner-approved packet required: `~/.local/state/trading-system/p184-20261005/runtime/p1/packet-g13-v5-20261016.md`
(DRAFT; v4.2/v4.3 procedure with the v5 substitutions: run worktrees, writer and 7 TS roles on B17, role map
roles-v224, run ids `g13r5wa20261016` / `g13r5fx01`, all 6 runtime steps). Files generated by
`p1/g13v5/make_g13v5.py` (exclusive create, 0600, prints hashes and paths only): `ts-p184-g13v5.compose.json`,
`fx-seed-plan-g13r5fx01.json`, `inputs-g13v5-draft-noci.json`. The generator first re-asserts that the tool-made v4 files equal the v3 files
with the ids and image substituted (byte-equal); this is why v5 needs no offline container.

Runner command (packet step 8; dry run first):
```
cd $RUN_DL && python3 -B scripts/p184_g13_final.py --phase runtime \
  --role-images upgrade/evidence/releases/v2.2.4/g13-roles-v224.json \
  --python-image-ref qdl-v2-python:2.2.3-rc19-4d3476c --writer-image $B17 \
  --ts-source-sha $TSSHA --dl-source-sha $DLSHA \
  --inputs ~/.local/state/trading-system/p184-20261005/runtime/p1/g13v5/inputs-g13v5-ci.json \
  --runtime-approval ~/.local/state/trading-system/p184-20261005/runtime/p1/packet-g13-v5-20261016.md \
  --ts-root $RUN_TS
```
- Dry run at drafting (draft inputs, draft packet) [checked]: exit 0, `blocked {}`, 6 steps / 66 cells (dr01-deployed 7,
  wrong-account 2, dr02-dr03 14, views 22, transport 9, dr07 12); writer scope = the 4 `g13r5wa20261016` accounts; DR07
  `--fx-account paper-p184-fx-g13r5fx01-binance`; est 39 min, timeouts 95 min.
- Expected: steps about 20 min measured (views 10.2, dr02-dr03 8.6, transport 0.7, dr01 + dr07 0.6), whole window
  30-40 min including the stack and the FX snapshot wait; lock 150 min.
- **BLOCKER (gate 0.6):** `p184_dr01_deployed.py record` requires `--ci-confirmed`. TS `68f2572f` is not on origin
  (origin `feat/v2-rust-first-okx-demo` = `af6227d9`), so there is no CI run for it. This needs an owner-approved push
  and a green CI run, then
  `python3 -B ~/.local/state/trading-system/p184-20261005/runtime/p1/g13v5/make_g13v5.py --wa g13r5wa20261016 --fx g13r5fx01 --ci-run <id> --out-suffix=-ci`.
  Without it the other 59 runtime cells still run, but the 7 dr01-deployed cells stay open.
- Verify (TS run worktree): `python3 -B scripts/p184_w05_verifier.py --case-evidence <the step jsonl files of the
  isolated and the runtime receipts> --expect-source-sha $TSSHA --expect-image-digest $B17`; target 76/76 DR cells with
  no provenance or drift issue.

## 4. Partition-43 cleaner pass and decision B roll (prepared 2026-10-16, nothing run)

Owner decisions 2026-10-16: B now, then G12 final; a temporary `min.cleanable.dirty.ratio=0.05` on
`__consumer_offsets` only (dynamic, rollback = delete the override). The force-trim was dropped (DeleteRecords on an
internal topic is refused by Kafka 4.2 `ReplicaManager`); the old markers are already past their deleteHorizon
(`hz_max` 2026-10-16T01:01:32Z in `p184-g12-coordinator-load-20261016/prod/__consumer_offsets-43.agg.json`), so one
cleaner pass removes them. Tools: `kafka_internal_topic_compaction.py --apply-temp/--remove-temp` (TEMP_OVERRIDES,
not drift while at its target; `--observe` prints `temp_removal` with the partition-43 size and `remove_recommended`
at <= 32 MiB per replica), `kafka_b_roll_runbook.py` (modular steps, receipts in
`~/.local/state/qdl-v2/p184-kafka-b-20261016/<step>-<UTC>/`). `--lock-owner claude-kafka-b` lets the tools run inside
the coordinator's own lock (any other valid lock still refuses).

```
DL=/home/bobby/.worktrees/data-layer-p184-closure; cd $DL
CERT=/home/bobby/.local/state/qdl-v2/655d2106d01f/cert-material-rotate-20260822T144323Z
C="python3 -B scripts/kafka_internal_topic_compaction.py"; B="python3 -B scripts/kafka_b_roll_runbook.py"
python3 -B scripts/host_measurement_lock.py acquire --owner claude-kafka-b --purpose "P18.4 partition-43 cleaner pass + decision B roll" --minutes 170
$B plan                                                            # pure: prints this sequence
$B check                                                           # read-only: health, quorum leader, order, C state, 43 size
$C --apply-temp --apply --cert-dir $CERT --lock-owner claude-kafka-b
$C --observe --cert-dir $CERT     # repeat (with the read-only pass watcher) until temp_removal.remove_recommended
$C --remove-temp --apply --cert-dir $CERT --lock-owner claude-kafka-b
$B check                                                           # re-read recommended_order (quorum leader last)
$B roll --broker <1st>        # each: exit 0 to continue; 1 = RESTORED_VERIFIED -> STOP; 3 = HARD STOP
$B roll --broker <2nd>        # refuses if it is now the quorum leader while another broker is still true
$B roll --broker <quorum leader>
$B elect --scope data         # coordinators stay where the rolls left them (no extra load stall)
$B final-check                # env false x3, parity snapshot auto.leader.rebalance.enable=false
python3 -B scripts/host_measurement_lock.py release --owner claude-kafka-b
```
- Per roll the runbook records: the roll tool report (healthy/ISR clocks, broker config), rust_core progress
  (`commit_wait_ms.max` / `commit_ms.max` per line, baseline 300 s before vs window, lines >= 1000 ms), every broker's
  `Finished loading` line (partition, records, bytes, ms: the first production coordinator-load measurement), restart
  counts of cores/projectors/stream/writer, and the writer heartbeat READY counts (`p184_g9_g10_writer_counters.py`,
  5 s samples). For the first two rolls the not-yet-rolled controller still has rebalance true; the runbook waits up to
  420 s for its automatic preferred election and measures that second move too.
- Election scope: `data`. The election tool has no per-partition exclusion; `--scope data` keeps every coordinator
  partition (43 and the core transaction partitions included) on its current leader. `elect --scope coordinators`
  refuses unless every measured partition-43 load in the receipt dir is <= 1000 ms (`--force-coordinators` overrides).
- Undo: a failed roll is rolled back by the tool (`RESTORED_VERIFIED`, exit 1). Already rolled brokers may stay false
  (the key acts only on the active controller); full undo, one broker at a time with the lock held:
  `python3 -B scripts/kafka_auto_leader_rebalance_roll.py --rollback <packet> --broker kafkaN --apply --cert-dir $CERT
  --lock-owner claude-kafka-b`. Temp key undo: `$C --remove-temp --apply ...`.
- G12 final follows in its own lock: section 1 with `PHASE=final`; the snapshot then yields `REB=false`, and the image
  set already names writer B17 (`ts` = `cb5216b8`). The temp key is removed before B, so production carries only C
  and the parity profile (C + A' + rebalance false) is complete.

## Serial schedule

All three take their own lock and never overlap each other or another measurement:
1. G12 interim, about 45 min.
2. G13 isolated, about 10 min.
3. G13 runtime window: after packet approval and CI, about 35 min.
4. Partition-43 cleaner pass (temp key) and decision B roll: section 4.
5. G12 final, about 45 min. The final G12 credit needs this run.

The G13 cells need no rerun after B: G13 exercises no broker restart. B changes only the static controller config,
and that is outside every G13 path.

## Pre-flight checklist (read-only; run in order, any FAIL = do not start)

1. Lock and foreign jobs: `python3 -B $DL/scripts/host_measurement_lock.py check` -> exit 0 (`lock` ABSENT/EXPIRED,
   `foreign []`). Also `ls ~/.local/state/host-measurement-window.lock`.
2. Pins: `git -C $DL cat-file -t $DLSHA` = commit and `git -C $DL merge-base --is-ancestor 13ef59c $DLSHA`;
   `git -C $TSW cat-file -t $TSSHA` = commit; the run-worktree paths do not exist yet (`test ! -e`).
3. Unit tests in the harness image. This starts a container, so run it inside the G12 lock. Use the network-none
   image run, as for the 221-test receipt: `docker run --rm --pull=never --network none --read-only --tmpfs /tmp -e
   HOME=/tmp -e PYTHONDONTWRITEBYTECODE=1 -v $RUN_DL:/src:ro -w /src --entrypoint python
   sha256:b9ca6c36fb0ad003b77e47bb3a777c3e565944d59cb3a2e9bc5dfb68e3448481 -B -m unittest
   tests.test_p184_g12_isolated_fault tests.test_p184_g13_final tests.test_host_measurement_lock
   tests.test_p184_dr08_isolated tests.test_p184_dr05_verdict`. Expected: OK.
4. Deployed set:
   `cd $DL && python3 -B -c 'import json,sys;sys.path.insert(0,".");from scripts import p184_g13_final as g;print(g.deployed_mismatches(g.deployed_inventory(),{"roles":json.load(open("upgrade/evidence/releases/v2.2.4/g13-roles-v224.json")),"writer":"'$B17'"}))'`
   -> `[]`; `docker inspect -f '{{.RestartCount}} {{.State.StartedAt}}' market_data_service_p184w` (restarts 0, start
   2026-10-16T09:27:01Z unless a later approved roll changed it -> then regenerate the image set, role map and packet).
5. Writer and Kafka parity:
   `python3 -B $DL/scripts/p184_g12_isolated_fault.py --parity-snapshot <scratch>/parity.json --lock-owner <own lock owner>`. Expect `writer.image` =
   B17 and `writer.env` with `MARKET_DATA_NATIVE_APPLY_MAX_ROWS` 16, `DATA_LAYER_V2_BOOK_SNAPSHOT_BATCH_MAX_WAIT_MS` 0
   and `MARKET_DATA_NATIVE_FEEDS` TRADE,QUOTE. Expect `kafka.auto.leader.rebalance.enable` true before B and false
   after B. A ValueError "disagree" means a B roll is in progress: stop.
6. Production C and A' effective values. This uses a throwaway `stable_admin`-equal container, so run it inside
   the lock; it is read-only on Kafka:
   `CERT=$(docker inspect qdl_v2_stable_candidate-kafka1-1 --format '{{range .Mounts}}{{if eq .Destination "/etc/kafka/secrets"}}{{.Source}}{{end}}{{end}}')`,
   then `python3 -B $DL/scripts/kafka_internal_topic_compaction.py --observe --cert-dir "$CERT"`. It must show
   exactly the C plan on both internal topics. The key material is mounted read-only and never read.
   For md.latest.v2 `delete.retention.ms` 21600000, use the A' apply receipt (DL plan, 2026-10-16 02:05:37Z, verified
   by the tool's after-describe), or the read-only `scripts/kn_state_topics_packet.py verify`, whose planned
   overrides come from the same budget.
   A mismatch means the snapshot profile is not production's: stop.
7. Images present: `docker image inspect -f '{{.Id}}'` on each of `7c848aad ce629061 baa72c96 b9ca6c36 cb5216b8 9516fb76
   dfa18828 cf78e766 d9e853e8 7fba2577 08f356a8 ca4548a1 05a97a47`, and `qdl-v2-python:2.2.3-rc19-4d3476c` -> `b9ca6c36`.
8. Inputs: `ls -l $EXPORT ~/.local/state/qdl-v2/p184-d1-rev11-20261009/base-catalog.yaml`; sha256 of the
   `p1/g13v5/*` files equals `p1/g13v5/make-g13v5-draft.out.json`.
9. Dry runs: the G13 isolated and runtime commands above with `--dry-run` -> exit 0, `blocked {}`. They are pure: no
   container and no receipts dir.
10. Leftovers: `docker ps -a --format '{{.Names}}' | grep -E '^(p184-g12-dl-|p184-dr05-|p184-dr08-|p184-g13-|ts-p184)'`
    is empty (the burst run's `p184-g12-dl-*` namespace must be gone); `docker network ls | grep -E 'ts-p184|p184-dr05'`
    is empty.
11. Host: `df -B1 --output=avail /` >= 20 GB, and `uptime` load1 < 16 before starting. At drafting: 182 GB free, load
    44 (burst running).
12. Codex index: re-read `P184_CODEX_CONTRIBUTIONS.md` for findings on `13ef59c` or this file. Record the disposition
    in the Unified Plan before the run.

## Gate status left open by this preparation

- Decision B is not rolled, so a G12 final credit is impossible until then. The interim run is qualification only.
- G13 runtime: owner approval of packet v5 (not given), and TS CI for `68f2572f` (not pushed).
- The first in-container use of `--apply-internal-configs` is inside the G12 run. It stops before the baseline on
  failure.
- The core-group S2 receipt stays INHERIT pending the owner record (inheritance audit).
