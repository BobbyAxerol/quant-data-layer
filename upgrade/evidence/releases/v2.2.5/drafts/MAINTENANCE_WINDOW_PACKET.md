# Data Layer v2.2.5 maintenance window packet (DRAFT, not executed)

Status: DRAFT for the owner's go/no-go. Nothing in this packet has been executed on production. The procedure was
rehearsed on an isolated stack (section 7); the final pre-window rehearsal with the window's exact inputs is item
2.5a. Target window: before 2026-11-06; hard deadline 2026-11-20 14:43Z (expiry of every deployed mesh certificate).

**This window is a full-mesh outage, not a config-only change.** The CA changes and every peer authenticates every
other peer, so the TS writer, every Data Layer mesh role and the three brokers stop together; the Trading System
receives no V2 market data from W1 until the writer is ready again after W12 (V1 is a stopped rollback container,
not a fallback). Expected outage and the deadlines: section 7b.

Owner approvals that cover this packet (2026-10-19): one window combining DL v2.2.5 (IDR-127 config, versioned KN
compose, healthchecks, IDR-129 Query fix) with the atomic mesh mTLS rotation; W12-A (writer image 2deb90b0 kept, TLS
identity mount switched only; `W12_DECISION.md`); rollback = old certificates (valid to 2026-11-20 14:43Z) + old
digests; K6 (observed no-op); F2 monitor restart on the TS release image; keep v2.2.5 compact (owner 09:00Z);
"Bạn cứ làm tiếp tôi duyệt hết rồi" (12:00Z). The coordinator launches the window; this packet is its script.

Related: `docs/runbooks/kn-host-reboot-recovery.md` (start order, market_cache rebuild 494 s), `W12_DECISION.md`,
`CERTIFICATE_SCOPE.md`, tools `scripts/kn_window_writer.py` (W12 composes, writer readiness, R4 soak),
`scripts/rotate_mesh_tls.py`, `scripts/kn_versioned_compose.py`, `scripts/kafka_internal_topic_compaction.py`.

## 1. What changes and what does not

| Area | Before (v2.2.4 runtime) | After (v2.2.5) | Recreated in the window |
|---|---|---|---|
| Mesh TLS | generation 20260822T144323Z (CA `qdl-phase8-certification-ca`, notAfter 2026-11-20 14:43Z) + KN client CA 2026-09-27 (leaves notAfter 2026-12-25) | generation 20261019T084200Z: one CA for the whole mesh, every leaf 365 days (notAfter 2027-10-19), same CN/SAN per identity, `ca.key` deleted, permissions hardened (private material 0400/0440 owned by its consumer, directories 0750) | every mesh peer |
| Container definitions | per-role inspect composes under ~/.local/state (pre-OPT caps in some files, drift R1/R4) | `deploy/compose/qdl-v2-stable-kn.yml` (committed), caps = live, parity PASS | all roles below |
| Healthchecks | none on stream_kn x2, market_projector x2, rust_core x3; 2 s / 5 s probes on stable_redis / Query / Kafka | liveness probes added (TCP 8210 / status-file freshness 30 s / TCP 8300); cadence 15 s on Kafka, Query, stable_redis (same tests) | same |
| Query image | qdl-v2-python:2.2.4-31a5155 (3e9e4d3a) | the v2.2.5 python image rebuilt from DL dev **with the IDR-129 fix** (`deploy/compose/qdl-v2-stable-kn.v2.2.5.env` `QDL_IMAGE_QUERY`; build receipt `upgrade/evidence/releases/v2.2.5/python-image-build-receipt.json`). The IDR-127-only candidate 2.2.5-601c3d8 (b7056141) is superseded | query_kn_1/2 |
| Rust images | 27c79b53 (ingestors, stream), 60f83a51 (projectors), 7c848aad (core) | **unchanged digests** | recreated for mounts/healthchecks only |
| bar edge | qdl-v2-python:2.2.3-rc14-a388537 (08f356a8), held | **unchanged image** (held: a new python image bakes catalog r12 / acquisition r20 and would refuse the r11/r19 checkpoint, section 6) | recreated for mounts only |
| Kafka topic config (K6) | temporary `min.cleanable.dirty.ratio` 0.05 on `__consumer_offsets` removed 2026-10-16 14:12Z; ABSENT at the 2026-10-19 10:35Z observation | unchanged (no-op; W0.4 records the exact pre-window state, W5 compares) | no |
| TS writer | `market_data_service_p184w`, image 2deb90b0 (TS 30f50a91), identity 2026-08-22 | W12-A: same image 2deb90b0, identity of the new generation; JWT key, binding, env unchanged (`DATA_LAYER_V2_JWT_MANIFEST_REVISION=11`) | yes (BOOTSTRAP, then STRICT) |
| TS monitor (F2) | `monitor_service` exited (image v1.2.3-f317fe3: alert storm, no DL platform checks) | TS v2.0.1 release image with TS PR #21 (cert expiry, Query memory, Kafka throttle), env `MONITOR_REQUIRED_SERVICES=market_data,monitor`, `MONITOR_COPY_DELIVERY_ENABLED=false` (section 5b). **Deferred** if TS v2.0.1 is not published by the window | monitor only |

Not touched: market_cache (projection cache; never recreated for a certificate change), Kafka data volumes, topics,
consumer-group offsets, KRaft metadata (the brokers' dynamic listener truststore path is kept; the new generation
provides that file), JWT signing keys and their public keys, cursor keys, consumer realm manifests (r16 dir), KN
release dir, Postgres, the V1 rollback container `data_layer_service` (no mesh identity, section 5c), every other TS
service, portal stacks.

## 1b. IDR-129 (Query RSS after warmup bursts) and the writer recreate

Production 2026-10-19: a writer recreate opens 64 concurrent `/warmup` requests (about 8 s each, 429
REPLICA_LANE_BUSY); query_kn_1 reached 2045/2048 MiB and restarted at 12:02:55Z, query_kn_2 was restarted under
control at 12:07:25Z (MALLOC_ARENA_MAX=2 already set). The window recreates the writer TWICE (W12 BOOTSTRAP, then
STRICT), i.e. two warmup bursts right after the Query replicas were recreated. Therefore:

- v2.2.5 ships the IDR-129 fix in the Query image; no window without it.
- W9 runs the fixed Query image before W12.
- During W12 sample `docker stats` of query_kn_1/2 every 5 s; if either exceeds 1.8 GiB, do not start the STRICT
  restart until it falls back below the post-W9 level (or restart that one replica, the other one serving the alias).
- Acceptance adds: Query memory 10 min after the STRICT start below 1.6 GiB and not growing (R4 soak, first 10 min).

## 2. Go/no-go checklist (T-1 day to T-0; no production mutation)

Window directory (create at T-0): `WIN=~/.local/state/qdl-v2/releases/v2.2.5/window-$(date -u +%Y%m%dT%H%MZ)`;
`mkdir -m 0700 -p $WIN/rollback`. Every receipt below goes there. Repository commands run from a DL checkout of the
dev commit that carries the window env (`/home/bobby/.worktrees/data-layer-dev` after `git checkout --detach
origin/dev`), never from a feature worktree.

| # | Check | Command / evidence | Go condition |
|---|---|---|---|
| 1 | Release inputs merged | DL dev contains the IDR-129 fix, the re-pinned `qdl-v2-stable-kn.v2.2.5.env` and the final rehearsal receipt; CI green on that commit | all three on origin/dev |
| 2 | Final rehearsal | `upgrade/evidence/releases/v2.2.5/rehearsal-final-*.json` (item 2.5a) | verdict PASS incl. burst check |
| 3 | TLS generation re-verified on the day | `sudo python3 -B scripts/rotate_mesh_tls.py verify --old-env ~/.local/state/qdl-v2/tls/gen-20260822T144323Z/generation.env --new-env ~/.local/state/qdl-v2/tls/gen-20261019T084200Z/generation.env --out $WIN/tls-verify.json` then `sudo python3 -B scripts/rotate_mesh_tls.py perms --gen-dir ~/.local/state/qdl-v2/tls/gen-20261019T084200Z` | both PASS |
| 4 | Compose parity against live | `python3 -B scripts/kn_versioned_compose.py parity --compose deploy/compose/qdl-v2-stable-kn.yml --env-file deploy/compose/qdl-v2-stable-kn.v2.2.4.env --env-file ~/.local/state/qdl-v2/tls/gen-20260822T144323Z/generation.env --env-file ~/.local/state/qdl-v2/releases/v2.2.5/kn-secrets.env > $WIN/pre-parity.json` | PASS; any other result = a role changed since 2026-10-19: stop and re-render, never apply over drift |
| 5 | Host measurement lock | `cat ~/.local/state/host-measurement-window.lock` absent/expired, then `python3 -B scripts/host_measurement_lock.py acquire --owner <window-owner> --purpose "DL v2.2.5 maintenance window" --minutes 180` (keep words like pytest/cargo out of `--purpose`: the acquire's foreign-process check matches its own shell, W12_DECISION side finding) | lock held by the window owner; never inside another owner's lock |
| 6 | Foreign activity | `docker ps --format '{{.Names}} {{.Image}} {{.Status}}'`: no `qdl-v225-reh-*`, `qdl-rss-*`, `knrd-*`, `p184-*`, `ts-p184*`, `idr*` test containers; no image build or campaign running (`pgrep -x cargo`, `pgrep -x dockerd` children are not a test) | none |
| 7 | Codex pause decision | owner/coordinator decides: Codex sessions paused for the window (as in the IDR-128 A/B arms) or allowed to continue read-only; record the decision and `ps -eo pid,etime,args | grep -i [c]odex` in `$WIN/foreign.txt` | decision recorded; no Codex test/build/container during W1-W13 |
| 8 | Portal stacks | IDR-128: the v2.2.4 certificate holds with unrelated heavy stacks paused; pausing the portal is the coordinator's choice for the soak, not required for correctness | decision recorded |
| 9 | Disk | `df -h /` and `docker system df` into `$WIN/disk-pre.txt` | >= 50 GB free on `/` |
| 10 | Old generation backup | `sudo tar --numeric-owner -cpf $WIN/rollback/tls-old-generation.tar -C / home/bobby/.local/state/qdl-v2/tls/gen-20260822T144323Z home/bobby/.local/state/qdl-v2/655d2106d01f/cert-material-rotate-20260822T144323Z home/bobby/.local/state/qdl-v2/655d2106d01f/bundle/identities-rotate-20260822T144323Z home/bobby/.local/state/qdl-v2/releases/v2.2.0-02cd827/tls home/bobby/.local/state/qdl-v2/p184-d3-capacity-20261009/reader-public-r16` then `sudo chmod 0400 $WIN/rollback/tls-old-generation.tar` and `sha256sum $WIN/rollback/tls-old-generation.tar > $WIN/rollback/tls-old-generation.sha256`. The window never writes these directories (the new generation is a separate tree); the tar protects the rollback against an accidental cleanup | tar written, sha256 recorded |
| 11 | Baseline | writer `python3 -B scripts/kn_window_writer.py ready --expect-demanded 64 --samples 2 --deadline-s 60 > $WIN/baseline-writer.json` (READY 64/64, execution-ready recorded); `docker stats --no-stream` of every KN role and the writer into `$WIN/baseline-stats.txt`; Query `ready_products` (runbook step 5) | writer READY 64/64, 0 unhealthy, no restarts |
| 12 | K6 pre-window state (exact) | W0.4 below | `__consumer_offsets` `temp_state` ABSENT, `c_state` APPLIED (`delete.retention.ms` 600000, `segment.ms` 60000), `__transaction_state` `c_state` APPLIED (`segment.ms` 600000), no drift |
| 13 | Writer composes rendered | W0.2 below | `render.json` deltas exactly as intended, `label_compose_deltas` [] |
| 14 | Rollback trigger and deadline agreed | section 7b | recorded in `$WIN/decision.txt` before W1 |
| 15 | TS v2.0.1 for the monitor | published tag + GHCR digest, or "monitor deferred" | either recorded |
| 16 | No open order path | `docker ps` shows no running TS order service (executor, risk, paper_execution, order_group, listener: exited since 2026-10-06); stopping the writer does not cancel orders, so any running order path needs its own TS plan | none running, or a TS plan |

5a. **Final isolated rehearsal** (one run, before the window, with the window's exact inputs: the IDR-129 Query
image, the hardened generation gen-20261019T084200Z, the window env, writer clone of 2deb90b0):
`scripts/kn_mesh_rotation_rehearsal.py --run <base> --old-gen <old generation.env> --new-gen <new generation.env>
--old-release deploy/compose/qdl-v2-stable-kn.v2.2.4.env --new-release deploy/compose/qdl-v2-stable-kn.v2.2.5.env
--secrets ~/.local/state/qdl-v2/releases/v2.2.5/kn-secrets.env --lock-owner <owner> --expect-query-image <digest>
--burst` (A -> B -> C -> D, then the 64-warmup burst check on the rehearsal Query), receipt next to
`rehearsal-20261019.json`.

## 3. Images

| Role | v2.2.4 (rollback) | v2.2.5 (target) |
|---|---|---|
| kafka1-3 | apache/kafka@sha256:9516fb76... | same |
| rust_core x3 | qdl-v2-rust@sha256:7c848aad... | same |
| ingestors, stream_kn x2 | qdl-v2-rust@sha256:27c79b53... | same |
| market_projector x2 | qdl-v2-rust@sha256:60f83a51... | same |
| query_kn x2 | qdl-v2-python@sha256:3e9e4d3a... | the IDR-129 rebuild pinned in `qdl-v2-stable-kn.v2.2.5.env` |
| binance_bar_edge | qdl-v2-python@sha256:08f356a8... | same (held) |
| market_cache, stable_redis | redis@sha256:dfa18828... | same |
| TS writer | 2deb90b0 (old identity) | 2deb90b0 (new identity) |
| TS monitor | exited, tradingsystem-image:v1.2.3-f317fe3 (5cd22b4e) | TS v2.0.1 GHCR image by digest, or deferred |

The env files `deploy/compose/qdl-v2-stable-kn.v2.2.4.env` and `.v2.2.5.env` pin every DL digest.

## 4. Window procedure

Common command prefix (`$C`), run from the DL dev checkout:

```
C="docker compose -p qdl_v2_stable_candidate -f deploy/compose/qdl-v2-stable-kn.yml \
   --env-file deploy/compose/qdl-v2-stable-kn.v2.2.5.env \
   --env-file ~/.local/state/qdl-v2/tls/gen-20261019T084200Z/generation.env \
   --env-file ~/.local/state/qdl-v2/releases/v2.2.5/kn-secrets.env"
NEW=~/.local/state/qdl-v2/tls/gen-20261019T084200Z
OLD_TS=~/.local/state/qdl-v2/655d2106d01f/bundle/identities-rotate-20260822T144323Z/trading-system
```

Never `$C up` without `--no-deps <services>`; never `--remove-orphans`; never name market_cache.

Pre-window steps (T-0, writer and mesh still serving):

| Step | Action | Pass condition |
|---|---|---|
| W0.1 | Save rollback composes of every KN role from docker inspect: `python3 -B -c "import json,os,subprocess,sys; os.umask(0o077); sys.path.insert(0,'.'); from scripts.kn_production_packet import service_from_inspect, external_compose; roles=[l for l in subprocess.check_output(['docker','ps','-a','--filter','label=com.docker.compose.project=qdl_v2_stable_candidate','--format','{{.Label \"com.docker.compose.service\"}}'],text=True).split() if not l.endswith('_init')]; svc={r: service_from_inspect(json.loads(subprocess.check_output(['docker','inspect','qdl_v2_stable_candidate-'+r+'-1']))[0]) for r in roles}; open('$WIN/rollback/kn-roles.inspect-compose.json','w').write(json.dumps(external_compose(svc),indent=1))"` (0600: the env-only secrets are in it; verified read-only 2026-10-19: 17 roles, `docker compose config` valid) | 17 roles in the file |
| W0.2 | Writer composes: `python3 -B scripts/kn_window_writer.py render --new-identity-dir $NEW/identities/trading-system --out-dir $WIN/writer` | exit 0; `render.json` deltas: bootstrap = identity source + REQUIRE_RESUME, strict = identity source, rollback-bootstrap = REQUIRE_RESUME, rollback = none; `label_compose_deltas` [] (verified read-only 2026-10-19 15:14Z) |
| W0.3 | Writer log offsets for the R1 counts: `for f in ~/.local/state/trading-system/p184-20261005/runtime/mark-cutover/logs/market_data*.log; do echo "$f $(stat -c %s $f)"; done > $WIN/writer-log-offsets-pre.txt` | file written |
| W0.4 | K6 exact pre-window state: `python3 -B scripts/kafka_internal_topic_compaction.py --observe --cert-dir ~/.local/state/qdl-v2/655d2106d01f/cert-material-rotate-20260822T144323Z > $WIN/k6-pre.json` | checklist item 12 |

Outage (W1 = T_W1; the deadlines of section 7b run from here):

| Step | Action | Pass condition before the next step |
|---|---|---|
| W1 | `docker stop -t 30 market_data_service_p184w` | exit code 0 (clean stop, cursors committed): `docker inspect -f '{{.State.ExitCode}}' market_data_service_p184w` |
| W2 | `docker stop -t 45` stream_kn_1/2, query_kn_1/2, binance_bar_edge, market_projector_1/2, ingestor_okx_swap, ingestor_binance_usdm, rust_core, rust_core_2, rust_core_3 (one command, container names `qdl_v2_stable_candidate-<role>-1`) | all exited |
| W3 | `docker stop -t 10 qdl_v2_stable_candidate-stable_redis-1`; `docker stop -t 60` kafka1-3 (one command) | all exited; from here the mesh is down |
| W4 | `$C up -d --no-deps --force-recreate kafka1 kafka2 kafka3 stable_redis` | R1 Kafka commands (section 8) with `$NEW/kafka`: brokers 1,2,3 answer; URP 0 and under-min-ISR 0 after ISR catch-up |
| W5 | K6 compare: `python3 -B scripts/kafka_internal_topic_compaction.py --observe --cert-dir $NEW/kafka > $WIN/k6-post.json` | `topics` section equal to `k6-pre.json` (topic configs live in KRaft metadata, which the broker recreate keeps). Only on a difference: restore the exact pre-window value with `--remove-temp --apply` (pre ABSENT) or `--apply-temp --apply` (pre PRESENT), `--cert-dir $NEW/kafka --lock-owner <window owner>`, then observe again |
| W6 | `$C up -d --no-deps --force-recreate rust_core rust_core_2 rust_core_3` (simultaneous: same image, no mixed-version group) | 3 members in `qdl-v2-realtime-core-v2`, healthy, no retry loop |
| W7 | `$C up -d --no-deps --force-recreate ingestor_binance_usdm ingestor_okx_swap` | healthy (session-liveness fresh) |
| W8 | `$C up -d --no-deps --force-recreate market_projector_1 market_projector_2` (market_cache is never stopped; stable_redis came back in W4: the projector exits when its cache is unreachable) | healthy (status fresh), `errors` empty, lag falling |
| W9 | `$C up -d --no-deps --force-recreate query_kn_1 query_kn_2` | healthy; R1 Query probe READY over mTLS with the NEW TS identity on 18301 and 18302; `ready_products` back to the baseline; record `docker stats` memory (post-W9 level) |
| W10 | `$C up -d --no-deps --force-recreate stream_kn_1 stream_kn_2` | healthy; R1 Stream handshake with the NEW TS identity verifies on 18320 and 18321 |
| W11 | `$C up -d --no-deps --force-recreate binance_bar_edge` (image unchanged) | healthy; log shows checkpoint restore, no bootstrap |
| W12a | Writer BOOTSTRAP (the outage exceeds the writer's 120 s execution-surface TTL; a STRICT start now parks every native TRADE/QUOTE slice in HANDOFF_RESUME_REQUIRED, IDR-101): start the Query memory watch `while sleep 5; do docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' qdl_v2_stable_candidate-query_kn_1-1 qdl_v2_stable_candidate-query_kn_2-1; done > $WIN/query-mem-w12.txt &` (stop it with `kill %1` after R1), then the `bootstrap` command of `$WIN/writer/render.json` (`docker compose -p trading-system-market-data -f $WIN/writer/bootstrap.compose.json up -d --no-deps --force-recreate market_data_service_p184w`), then `python3 -B scripts/kn_window_writer.py ready --expect-demanded 64 --deadline-s 900 > $WIN/w12a-ready.json` | exit 0: three consecutive heartbeats of THIS container (after its StartedAt) READY, V2_PRIMARY, demanded = ready = 64, unhealthy 0, V1 fallback 0, RestartCount 0 |
| W12 burst | Expected on Query during W12a and W12b: one warmup burst per writer start (64 concurrent `/warmup`: 10 BAR 1m x 1,000 rows + 54 latest-only slices of the TS manifest r11), ~8 s per large warmup, 429 `REPLICA_LANE_BUSY` redirects between the two replicas (SDK replica redirect), memory step per burst | neither replica above 1.8 GiB; otherwise hold W12b (section 1b) |
| W12b | Writer STRICT: `docker stop -t 30 market_data_service_p184w` (clean stop first; compose's own stop timeout is 10 s), note `T_S=$(date -u +%Y-%m-%dT%H:%M:%SZ)` and new log offsets (`... > $WIN/writer-log-offsets-strict.txt`, as W0.3), then the `strict` command of `render.json`, then `python3 -B scripts/kn_window_writer.py ready --expect-demanded 64 --deadline-s 900 > $WIN/w12b-ready.json` | exit 0 as W12a; 0 lines of 401 / UNAUTHENTICATED / CERTIFICATE_VERIFY_FAILED / STRICT_RESUME_REFUSAL / HANDOFF_RESUME_REQUIRED since `T_S` (R1 item 3) |
| W13 | Acceptance R1 (section 8) | all PASS -> consumer-visible outage ends at W12b ready |
| W14 | TS monitor (section 5b), only when TS v2.0.1 is published | monitor running, no alert storm |
| W15 | R2 parity, R4 60-min passive soak (section 8) | PASS; then release the host lock |

Writer continuity across BOOTSTRAP (Codex final-phase review section 3): BOOTSTRAP does not delete or rewrite
cursors; it lets the native TRADE/QUOTE slices start without a resumable execution surface (the surfaces expired after
120 s), so they reseed the latest state from Query. It makes no lossless-history claim for the outage interval: the
writer's realtime views have a gap from W1 to W12a ready, and TRADE history for that interval stays in the Data
Layer (Kafka canonical retention), not in the writer. Record per feed: the cursor files under
`~/.local/state/trading-system/p184-20261005/runtime/mark-cutover/state` before W1 and after W12b (`ls -l --time-style
=+%s` of the cursor directory into `$WIN/cursors-pre.txt` / `cursors-post.txt`), the first READY heartbeat of
W12a/W12b and the final STRICT mode (`MARKET_DATA_NATIVE_REQUIRE_RESUME=1` in `docker inspect`).

## 5. TS writer, monitor and old-identity holders

### 5a. Writer slot: W12-A (decided)

`W12_DECISION.md`: W12-A for this window (writer 2deb90b0, identity mount switch only). W12-B (TS release image with
group-commit and V1-lease changes) is not taken: its GHCR image closure, PR CI and full-writer A/B are incomplete.
The DL side needs only the new identity mount, JWT key unchanged, `DATA_LAYER_V2_JWT_MANIFEST_REVISION=11` and the
BOOTSTRAP -> STRICT start of W12. The writer runs as root inside the container, so the hardened `identities/`
directory (root 0750) is readable by it; the rehearsal probes run as root for the same reason.

### 5b. TS monitor_service (F2, TS PR #21 merged into TS dev f7d19f47)

Image: the TS v2.0.1 GHCR image (main after PR #21 and the phase E slices; another agent is releasing it). If v2.0.1
is not published by the window, W14 is **deferred** (monitor stays exited; the window does not depend on it). TS
v2.0.0 (9a251870) does not contain PR #21. Read-only facts checked 2026-10-19: the current `monitor_service` was
created from `/home/bobby/trading_system` (project `trading_system`, compose + release override) on 5cd22b4e; the
canonical TS checkout is on a feature branch with uncommitted compose edits, so the window takes the compose files
from the release tag instead.

1. Compose and scripts of the release: `TSC=$WIN/ts-v2.0.1; mkdir -p $TSC; git -C /home/bobby/trading_system
   fetch -q origin --tags; git -C /home/bobby/trading_system archive v2.0.1 docker-compose.yml
   docker-compose.release.yml scripts/monitor_inventory_refresh.py scripts/preflight_env.py | tar -x -C $TSC`.
2. Certificate sources. `monitor_inventory_refresh.py` copies only certificates directly inside a source
   directory; its default glob (`~/.local/state/qdl-v2/*/cert-material-rotate-*`) does not match the new layout,
   the hardened directories are not readable by the host user, and the client CA bundles also carry the inactive
   phase105b / consumer-realms CAs (notAfter 2026-11-24 .. 2026-12-26, kept on purpose, `CERTIFICATE_SCOPE.md`
   section 4) that would raise MESH_CERT_EXPIRING from 2026-11-03. So stage the mesh public certificates once:
   `MS=~/.local/state/trading-system/monitor-cert-sources/gen-20261019T084200Z; mkdir -m 0755 -p $MS; for d in kafka
   kn-tls identities/trading-system stable-certs/authority-dispatcher stable-certs/core stable-certs/producer
   stable-certs/projector stable-certs/query stable-certs/stream; do for f in $(sudo find $NEW/$d -maxdepth 1 -name
   '*.crt' ! -name '*bundle*'); do sudo install -m 0444 -o bobby -g bobby "$f" "$MS/$(echo $d | tr / _)__$(basename
   $f)"; done; done` (public certificates only; keys are never copied).
3. Inventory after the rotation (and after every later DL recreate): `python3 -B $TSC/scripts/monitor_inventory_refresh.py
   --cert-glob '' --cert-source $MS` -> `WRITTEN`, 5 cgroup targets (Kafka x3 throttle, Query x2 memory + throttle),
   soonest certificate 2027-10-19.
4. Env: `cp -p /home/bobby/trading_system/.env $WIN/rollback/ts.env.pre-window`; add, only if absent,
   `MONITOR_REQUIRED_SERVICES=market_data,monitor` and `MONITOR_COPY_DELIVERY_ENABLED=false` to
   `/home/bobby/trading_system/.env`; `python3 -B $TSC/scripts/preflight_env.py --env /home/bobby/trading_system/.env
   --compose $TSC/docker-compose.yml $TSC/docker-compose.release.yml` -> PASS (CLAUDE.md section 4.9).
5. Recreate the monitor only: `TRADING_SYSTEM_RELEASE_IMAGE=ghcr.io/bobbyaxerol/executorbroker@sha256:<v2.0.1
   digest> docker compose -p trading_system --project-directory /home/bobby/trading_system -f
   $TSC/docker-compose.yml -f $TSC/docker-compose.release.yml up -d --no-deps --force-recreate monitor`
   (`TRADING_SYSTEM_RELEASE_IMAGE` set for this service only, CLAUDE.md section 4.8).
6. Check after 10 min: `docker inspect -f '{{.Image}} {{.RestartCount}} {{.State.Status}}' monitor_service`;
   `docker logs --since 10m monitor_service 2>&1 | grep -c -E 'SERVICE_HEARTBEAT_STALE|COPY_CONSUMER_GROUP_MISSING'`
   = 0; no MESH_CERT_* / CGROUP_TARGET_MISSING alert.
7. Rollback: `docker stop monitor_service` (the pre-window state is "monitor exited"); restore the `.env` copy only if
   another service is affected (only the monitor reads the two keys).

### 5c. Containers that still carry the 2026-08-22 identity after the rotation

Checked with `docker inspect` (2026-10-19):

| Container | State | Mesh identity | After the window |
|---|---|---|---|
| `data_layer_service` (V1, `qdl-v1-fallback:v1.2.4-2b0dcf7`) | exited (0) since 08:58Z, restart unless-stopped | **none**: binds are `.env`, `data`, `logs`; no TLS/CERT/KAFKA/V2 key in its env or `.env` | unaffected by the rotation; stays the V1 rollback image (owner decision); not a V2 fallback |
| `market_data_service` (TS legacy writer, c1f713a8) | exited (137), unless-stopped | `655d2106d01f/bundle/identities-rotate-20260822T144323Z/trading-system` + JWT key | cannot connect to the rotated mesh (old CA); usable only with a full rollback to the old generation, and only until 2026-11-20 14:43Z |
| `market_data_service_p184w-dr07-rollback` (9150385e) | exited (137), unless-stopped | same old identity | same |
| `market_data_service_p184w-r49-rollback` (b4e7a6c0) | exited (137), restart no | same old identity | same |

Cleanup step (after R1-R4 PASS and the v2.2.5 tag, at the latest on 2026-11-20 when the old generation expires;
rule 34: removing a stopped operational container needs the owner's explicit approval): remove the three TS
containers above (`docker rm <name>`, no `-v`), then their images by id after the pin grep
(`~/.local/state/**/profile*.json`, `config/v2/*amendment*.json`, campaign/proof scripts); delete the old TLS
directories of checklist item 10 **except** `identities-rotate-20260822T144323Z/*-jwt` (the JWT signing keys stay in
use: the window does not rotate JWT), keeping `$WIN/rollback/tls-old-generation.tar` until the owner closes the
rollback period. Test apparatus that mounts 2026-08-22 identities stops working against production after the window
(verified for `scripts/idr129_query_rss_isolated.py`, `IDENTITIES` = the r135 run directory; others not inventoried)
and must be repointed to the new generation before its next production-facing run.

## 6. Rollback

Rollback restores the old certificates and the old digests; the old material is untouched by the window (the new
generation is a separate directory), so rollback is the same transaction in reverse, valid until 2026-11-20 14:43Z.

1. Stop the writer, then all DL mesh roles and the brokers (W1-W3).
2. Recreate with the v2.2.4 env and the deployed generation:
   `C_OLD="docker compose -p qdl_v2_stable_candidate -f deploy/compose/qdl-v2-stable-kn.yml --env-file deploy/compose/qdl-v2-stable-kn.v2.2.4.env --env-file ~/.local/state/qdl-v2/tls/gen-20260822T144323Z/generation.env --env-file ~/.local/state/qdl-v2/releases/v2.2.5/kn-secrets.env"`, same order W4, W6-W11.
3. Writer: `rollback-bootstrap` command of `$WIN/writer/render.json`, `ready`, `docker stop -t 30`, then the
   `rollback` command (= the pre-window writer exactly), `ready` again.
4. K6: compare `--observe` with the old cert dir against `k6-pre.json`; restore as in W5 only on a difference.
5. Monitor: `docker stop monitor_service` if W14 ran.
6. Last resort: `$WIN/rollback/kn-roles.inspect-compose.json` (W0.1) recreates the exact pre-window definitions per
   role (`docker compose -f <file> up -d --no-deps --force-recreate <role>`).

bar edge note: the held image a388537 bakes catalog r11 / acquisition r19, matching its checkpoint (catalog 11,
acquisition 19). A python image from this release bakes r12 / r20, and `stable_bar_edge._restore_state`
(`qdl/runtime/stable_bar_edge.py:560-567`) would refuse that checkpoint ("catalog_revision differs from runtime
authority"); `scripts/migrate_stable_bar_edge_checkpoint.py` cannot migrate a catalog revision. Rolling bar edge is
therefore not part of this window.

## 7. Rehearsal (isolated, 2026-10-19)

Receipt: `upgrade/evidence/releases/v2.2.5/rehearsal-20261019.json` (run 10:34-11:07Z, tool and compose at
3915a76, host lock claude-dl-v225, no foreign job). ISOLATED rehearsal on the same host: compose project
qdl-v225-reh from the committed compose with an overlay (own networks/volumes, no ports, CPU caps 2.99 vCPU in total,
Kafka super.users by CN instead of the production ACL list); kafka1-3, rust_core, both ingestors (live public
venue WebSockets, bindings narrowed to ETH perpetuals), market_projector_1, market_cache, stable_redis, query_kn_1,
stream_kn_1/2, and a TS writer clone (deployed image 2deb90b0 and env, ETH on both venues, 12 demanded slices).
Not production evidence; timings are under the caps and the narrowed data.

| Phase | Generation / images | Mesh stop | Brokers answer | DL roles healthy | Query READY (mTLS) | Writer BOOTSTRAP ready | **Consumer-visible outage** |
|---|---|---|---|---|---|---|---|
| A (baseline) | deployed 20260822T144323Z / v2.2.4 | - | - | - | READY | 12/12 | - |
| B rotate | new 20261019T084200Z / v2.2.5 | 18.7 s | 59.2 s | 159.0 s | 161.4 s | +47.1 s | **251.3 s** |
| C rollback | deployed / v2.2.4 | 20.9 s | 57.8 s | 137.2 s | 139.6 s | +82.8 s | **267.8 s** |
| D re-roll | new / v2.2.5 | 15.5 s | 59.5 s | 139.3 s | 141.8 s | +108.6 s | **290.7 s** |

(Times after the first writer stop; "DL roles healthy" and "Query READY" count from the recreate of the brokers.)

Per phase B, C, D:
- Trust anchor moved both ways: the Query and both Stream gateways refused the other generation's TS identity
  (CERTIFICATE_VERIFY_FAILED / unable to get local issuer), Kafka refused the other generation's admin identity and
  accepted the current one; the Kafka dynamic listener truststore override (truststore-kn-v220.jks) carried over.
- Offsets: every consumer group (core, projector stage A/B, both book-resync groups) present after the switch with no
  committed offset behind its pre-switch value; URP 0.
- Writer: clean stop exit 0; durable cursor items unchanged (TRADE 2 / QUOTE 2); BOOTSTRAP until 12/12 READY, then the
  deployed STRICT start: 12/12 READY at the steady check, 0 STRICT_RESUME_REFUSAL / HANDOFF_RESUME_REQUIRED; auth and
  protocol failures 0 (401, UNAUTHENTICATED, TLS verify) on the writer, Query and Stream; data-policy refusals
  counted apart (SOURCE_UNAVAILABLE MARK reference reads during start-up only).
- stable_redis recreated with the mesh; the writer was admitted again (quota state restarts by design).
- Teardown: 13 containers, 4 networks, 4 volumes, 4 work dirs removed; 0 left.

Findings that changed the window procedure:
1. The writer's execution surfaces in its Redis expire 120 s after the last market row. A STRICT start after a longer
   outage parks every TRADE/QUOTE slice in HANDOFF_RESUME_REQUIRED (terminal for the process, IDR-101). W12 therefore
   starts the writer once in BOOTSTRAP and then in the deployed STRICT mode.
2. In this run the STRICT-restart timer accepted the stopped process' last heartbeat (false positive); STRICT readiness
   is shown by the steady sample about 60 s later. The tool was fixed (a heartbeat newer than the restart is required),
   and `kn_window_writer.py ready` applies the same rule in production.

Final pre-window rehearsal (item 2.5a): the 2026-10-19 run used the IDR-127-only Query image and the generation
before the permission hardening (bytes unchanged). The final run uses the IDR-129 image, the hardened generation and
the window env, runs the probes as root (the hardened `identities/` is root 0750, like the writer), gives the
rehearsal Query the production memory cap (2 GiB), and adds the 64-warmup burst check after phase D: 3 bursts of 64
concurrent TS-manifest warmups (the 12 ETH requirements cycled; 2 BAR 1m x 1,000 rows each, the rest latest-only)
against the rehearsal Query with the new TS identity; predeclared gate: no Query restart or OOM kill, memory after the
settle <= baseline + 128 MiB and < 1.6 GiB, no step > 32 MiB between the last two bursts. Data volume is the
rehearsal's (minutes of ETH history), so this checks the window configuration, not the production-volume retention
(that is the IDR-129 isolated A/B, `CERTIFICATE_SCOPE.md` R3). Result: see the receipt `rehearsal-final-*.json`.

## 7b. Outage, recovery and deadlines

| Quantity | Value | Source |
|---|---|---|
| Expected consumer-visible outage (W1 -> W12b ready) | **about 4-5 min** | rehearsal B/C/D 251-291 s (ETH-only data, 2.99 vCPU caps) |
| Planning budget | 10 min | production has 64 writer slices, 3 cores, 2 projectors, 2 Query replicas (more start-up work than the rehearsal; not measured) |
| market_cache rebuild if the cache is lost (not part of the plan: the window never stops market_cache) | +494 s (bars READY at 389 s, latest at 494 s) plus Query warm-up and the writer start | F3 drill 2026-10-19 (isolated, one projector at 0.75 CPU; production splits the topics over two projectors) |
| Rollback outage | about 4.5 min (rehearsal C 267.8 s) | rehearsal |

One total deadline (Codex final-phase review section 3, agreed before W1 and never extended after a failure is
observed): **if the writer is not STRICT READY 64/64 by T_W1 + 25 min, start the rollback** (section 6). A step that
fails its pass condition starts the rollback at once; per-step waits consume the same 25 min. Reserve 20 min after
the trigger for the rollback (4.5 min rehearsed + BOOTSTRAP/STRICT + margin); if market_cache was lost, add 494 s.
Book the window as 15 min pre-window checks + 25 min forward + 20 min rollback reserve + 60 min soak, inside one 3 h
host lock.

## 8. Acceptance R1-R4 (exact commands)

Shell helpers (host, read-only; the hardened identity files need root to read):

```
NEW_TS=$NEW/identities/trading-system
qry() { sudo curl -sS --noproxy '*' --max-time 5 --resolve qdl-v2-query:$2:127.0.0.1 --cacert $1/ca.crt \
  --cert $1/client.crt --key $1/client.key https://qdl-v2-query:$2/health/ready; echo " rc=$?"; }
stm() { echo | sudo openssl s_client -connect 127.0.0.1:$2 -servername $3 -cert $1/client.crt -key $1/client.key \
  -CAfile $1/ca.crt -verify_return_error -brief 2>&1 | grep -E -m2 'Verification|error'; }
KA() { docker run --rm --network qdl_v2_stable_candidate_stable_internal --read-only --security-opt no-new-privileges:true \
  -v $1:/etc/kafka/secrets:ro --entrypoint /opt/kafka/bin/$2 \
  apache/kafka@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9 \
  --bootstrap-server kafka1:9092,kafka2:9092,kafka3:9092 --command-config /etc/kafka/secrets/admin.properties "${@:3}"; }
OLD_KAFKA=~/.local/state/qdl-v2/655d2106d01f/cert-material-rotate-20260822T144323Z
```

**R1 window acceptance** (receipt `$WIN/r1.json`, written by the operator from the outputs below):

1. Roles: `docker ps -a --filter label=com.docker.compose.project=qdl_v2_stable_candidate --format '{{.Names}}
   {{.Status}}'` -> 17 roles Up (healthy where a healthcheck exists), the two `*_init` one-shots exited;
   `docker inspect -f '{{.Name}} {{.Image}} {{.RestartCount}}' $(docker ps -q --filter
   label=com.docker.compose.project=qdl_v2_stable_candidate)` -> images = `qdl-v2-stable-kn.v2.2.5.env`,
   RestartCount 0.
2. Trust anchor moved: `qry $NEW_TS 18301; qry $NEW_TS 18302` -> READY, rc=0; `qry $OLD_TS 18301` -> TLS failure
   (rc 35/60); `stm $NEW_TS 18320 qdl-v2-stream-a; stm $NEW_TS 18321 qdl-v2-stream-b` -> `Verification: OK`;
   `stm $OLD_TS 18320 qdl-v2-stream-a` -> verify error; `KA $NEW/kafka kafka-broker-api-versions.sh | grep -c '(id: '`
   -> 3; `KA $OLD_KAFKA kafka-broker-api-versions.sh` -> fails (SSL handshake).
3. Writer: `$WIN/w12b-ready.json` ready true (READY slices and execution-ready slices reported as two numbers);
   auth/protocol lines since the STRICT start = 0:
   `while read f off; do tail -c +$((off+1)) "$f"; done < $WIN/writer-log-offsets-strict.txt | grep -c -E
   '\b(401|UNAUTHENTICATED|CERTIFICATE_VERIFY_FAILED|SSLError|STRICT_RESUME_REFUSAL|HANDOFF_RESUME_REQUIRED)\b'` and
   `docker logs --since $T_S market_data_service_p184w 2>&1 | grep -c -E '<same pattern>'` -> 0 and 0; data-policy
   refusals counted apart, per code and feed: `while read f off; do tail -c +$((off+1)) "$f"; done <
   $WIN/writer-log-offsets-strict.txt | grep 'slice disconnected' | sed -E 's/.*venue=([^ ]+) symbol=([^ ]+)
   feed=([^ ]+).* code=([^ ]+).*/\4 \1 \2 \3/' | sort | uniq -c` (quiet-venue staleness is legitimate; never
   converted into a fresh price).
4. Kafka: `KA $NEW/kafka kafka-topics.sh --describe --under-replicated-partitions | wc -l` -> 0; same with
   `--under-min-isr-partitions` -> 0; `KA $NEW/kafka kafka-consumer-groups.sh --describe --group
   qdl-v2-realtime-core-v2 --members | grep -c qdl` -> 3 members; `KA $NEW/kafka kafka-consumer-groups.sh --describe
   --group kn-projector-v3-a` and `kn-projector-v3-b` -> lag back in the pre-window range (`$WIN/baseline-*`).
5. Certificate expiry: served certificates `for p in 18301:qdl-v2-query 18302:qdl-v2-query 18320:qdl-v2-stream-a
   18321:qdl-v2-stream-b; do echo | sudo openssl s_client -connect 127.0.0.1:${p%%:*} -servername ${p#*:} -cert
   $NEW_TS/client.crt -key $NEW_TS/client.key -CAfile $NEW_TS/ca.crt 2>/dev/null | openssl x509 -noout -enddate
   -issuer; done` -> notAfter Oct 19 2027, issuer `qdl-phase8-certification-ca`; mounted files `sudo find $NEW -name
   '*.crt' ! -name '*bundle*' -exec openssl x509 -noout -enddate -in {} \; | sort | uniq -c` -> every notAfter
   >= Oct 19 2027 (the client CA bundles keep the inactive CAs listed in `CERTIFICATE_SCOPE.md` section 4).
6. Query memory: `$WIN/query-mem-w12.txt` never above 1.8 GiB.

**R2 compose parity** after the window: `python3 -B scripts/kn_versioned_compose.py parity --compose
deploy/compose/qdl-v2-stable-kn.yml --env-file deploy/compose/qdl-v2-stable-kn.v2.2.5.env --env-file
$NEW/generation.env --env-file ~/.local/state/qdl-v2/releases/v2.2.5/kn-secrets.env > $WIN/r2-parity.json` -> PASS,
intended deltas only.

**R3 Query image**: `upgrade/evidence/releases/v2.2.5/python-image-build-receipt.json` (changed files = git diff
from 31a5155, unexpected 0, pip freeze equal) + the IDR-129 isolated A/B receipt (`CERTIFICATE_SCOPE.md` D3).

**R4 passive soak**, 60 min, no load added, started right after R1:
`python3 -B scripts/kn_window_writer.py soak --out $WIN/r4-soak.jsonl --minutes 60 --interval-s 60
--expect-demanded 64`. It reports, separately: writer READY samples (demanded = ready = 64, unhealthy 0) and the
execution-ready min/max (two numbers); request latency = writer `native_probe` histograms differenced over the soak
(basis QUERY_REQUEST_START_TO_REDIS_APPLY_DECISION_MONOTONIC, bucket upper bounds 10..2000 ms); served event age
NOT_MEASURED (no passive counter); Query memory first/last/max and growth over the last 10 min; restarts of every KN
role and the writer; auth/protocol failures (writer files + stdout, Query and Stream logs) and data-policy refusals
per code/venue/symbol/feed. PASS = writer READY in every sample, 0 auth/protocol failures, 0 restarts, Query memory
< 1.6 GiB and not growing over the first 10 min after the STRICT start, latency within the v2.2.4 envelope.

## 9. Owner go / no-go

- [ ] Date/time of the window (before 2026-11-06) and the window owner.
- [ ] v2.2.5 merged in dev, CI green; IDR-129 fix merged, python image rebuilt from dev, digest recorded and pinned.
- [ ] Final rehearsal (2.5a) PASS including the burst check.
- [ ] W12-A (decided, `W12_DECISION.md`).
- [ ] Section 2 checklist rows 3-16 PASS on the day.
- [ ] Total deadline T_W1 + 25 min and the rollback reserve (section 7b) agreed.
- [ ] TS v2.0.1 digest for W14, or the monitor deferred.
- [ ] After acceptance: certificate scope per `CERTIFICATE_SCOPE.md` (same folder), then tag v2.2.5; old-identity
      cleanup (section 5c) as a separate owner approval.
