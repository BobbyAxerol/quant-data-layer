# KN host reboot recovery - boot order, checks, cache rebuild, rollback

Status: operator runbook for the production KN topology of release v2.2.4 (project `qdl_v2_stable_candidate`),
written 2026-10-19 (owner approval F3). It replaces the legacy boot unit `qdl-v2-stable-boot-recovery.service`,
which was retired the same day (section 7). A real host reboot of the KN topology has **not** been exercised; the
only drilled part is the market_cache rebuild (section 5), measured on an isolated stack. Nothing here claims high
availability: every role shares one host (see `kn-single-points-of-failure.md`).

## 1. Rules

1. **`docker start` only. Never `docker compose up`, never `docker compose -f ... up -d`.** Every KN role was created
   from a per-role packet compose under `~/.local/state/qdl-v2/...` (rollouts rc15, rc17v2, rc22, g12 b-packet),
   and `stable_redis` from a compose in a worktree that no longer exists. A compose up would recreate containers
   with whatever that file says today (pre-OPT caps, pre-KN roles on main), i.e. leave the certified set.
2. **No FLUSH, no offset reset, no `rebuild_v2_stable_projection_cache.py`, no `v2_stable_boot_recovery.py`.**
   Both scripts target the retired SQLite-spool topology (roles `projector_v2*`, `query_v2_*`, `stream_v2_*`;
   group `stable-projector-v1`, which no longer exists in Kafka). The KN projector rebuilds the cache by itself.
3. **Never recreate a role during recovery.** A recreate with another digest leaves the v2.2.4 certificate and
   needs a new DL release (TS plan, entry "P18.4 Data Layer v2.2.4 published").
4. Do not stop or restart a role that is already running and healthy; start only roles that are `exited`.
5. Host has `HTTP_PROXY`; use `curl --noproxy '*'` for local probes.

## 2. What happens on a reboot without an operator

Every DL role, the TS writer and the TS infrastructure (`redis_service`, `redis_marketdata`, `live_data_executor`)
run with restart policy `unless-stopped` (`docker inspect`, 2026-10-19), and `docker.service` is enabled, so the
daemon starts them all at boot **at the same time, in no order**. Roles whose dependency is not up yet exit and
are restarted by Docker with its doubling back-off (capped at one minute). Containers that were `exited` before the
reboot (the legacy TS stack, rollback containers, one-shot inits) stay exited.

Consequences, by design reading of the code (not exercised on this topology):

- `market_cache` and `stable_redis` come back **empty** (`--save "" --appendonly no`).
- `market_projector_1/2` stage B finds no cache checkpoint and builds a fresh generation of every owned state
  partition from the partition start; products become READY only at the build boundary
  (`rust/qdl-projector/src/stage_b.rs:8-13`). A projector that cannot reach `market_cache` at start exits
  (`main.rs` `run_stage_b`: `Cache::connect` -> `fatal`, exit 2) and Docker restarts it.
- `query_kn_*` report products NOT_READY until the projector publishes them; the TS writer sees them as not
  execution-eligible and fails closed. V1 is not a fallback today: all 64 TS products are `fallback: BLOCKED`.
- `stable_redis` loss resets leases and per-consumer quota windows; admission fails closed with 503 while it is
  down (`qdl/security/data_plane.py` RedisMinuteQuota).

So an unattended reboot should converge without a human, but the outage length is dominated by the cache rebuild
(section 5) plus Kafka log recovery after an unclean shutdown (not measured). Use this runbook to verify each
stage and to act when a stage does not converge.

## 3. Inventory (2026-10-19, v2.2.4 certified set)

| Stage | Container (`qdl_v2_stable_candidate-<role>-1`) | Image (short) | Docker healthcheck | Needs at start |
|---|---|---|---|---|
| 1 | `kafka1`, `kafka2`, `kafka3` (KRaft broker+controller, quorum 1/2/3) | `9516fb76` | TCP 9092 | disk |
| 2 | `market_cache`, `stable_redis` | `dfa18828` | `redis-cli ping` | - |
| 3 | `rust_core`, `rust_core_2`, `rust_core_3` | `7c848aad` | none | Kafka |
| 4 | `market_projector_1`, `market_projector_2` | `60f83a51` | none | Kafka, market_cache |
| 5 | `query_kn_1`, `query_kn_2` | `3e9e4d3a` | `/health/ready` (mTLS) | market_cache, stable_redis, rust_core admission |
| 5 | `stream_kn_1`, `stream_kn_2` | `27c79b53` | none | Kafka, stable_redis, Query read view |
| 6 | `ingestor_binance_usdm`, `ingestor_okx_swap` | `27c79b53` | session-liveness LIVE | Kafka, venue network |
| 6 | `binance_bar_edge` | `08f356a8` | heartbeat file < 180 s | Kafka, venue network |
| 7 | `market_data_service_p184w` (TS writer) | `2deb90b0` | none | Query, Stream, TS Redis/Postgres |
| - | `data_layer_service` (V1, rollback image only) | `qdl-v1-fallback:v1.2.4-2b0dcf7` | none | - |

One-shot inits `stable_tls_init` / `stable_state_init` stay `exited (0)`; do not start them.

## 4. Boot order and checks

The owner's order is kafka -> rust_core -> projector -> caches -> stream/query -> ingestors -> TS writer. The caches
are moved before the projector here because the projector exits when `market_cache` is unreachable (section 2);
the two Redis instances have no dependencies, so starting them early costs nothing.

Set once per shell:

```bash
P=qdl_v2_stable_candidate
st() { docker ps -a --filter name=^/${P}-$1-1$ --format '{{.Names}} {{.Status}}'; }
startx() { for r in "$@"; do s=$(docker inspect -f '{{.State.Status}}' ${P}-$r-1); \
  [ "$s" = exited ] && docker start ${P}-$r-1 || echo "$r: $s (left alone)"; done; }
```

| Step | Action | Check (all must hold before the next step) | Expected | If it does not converge |
|---|---|---|---|---|
| 0 | Host: `systemctl is-active docker`; `df -h /` | Docker active; root disk > 50 GB free (Kafka, TS state and logs share it) | - | Free disk first (2026-10-08 ENOSPC took Kafka down); never delete Kafka data |
| 1 | `startx kafka1 kafka2 kafka3` | All three `healthy`; from inside a broker with `admin.properties`: `kafka-topics.sh --describe --under-replicated-partitions` and `--under-min-isr-partitions` both empty | ~20 s per broker, ISR ~3.7-3.9 s after a clean roll (plan 2026-10-16 B roll); unclean-shutdown log recovery not measured | Broker log: `docker logs --since 10m ${P}-kafka1-1`; a broker stuck in recovery is waited out, not recreated; quorum needs 2 of 3 |
| 2 | `startx market_cache stable_redis` | `redis-cli ping` = PONG on both; `market_cache` `INFO keyspace` may be empty (expected after a reboot) | seconds | Check memory (`docker stats`); no persistence to restore, nothing else to do |
| 3 | `startx rust_core rust_core_2 rust_core_3` | Running, `RestartCount` not climbing; `kafka-consumer-groups.sh --describe --group qdl-v2-realtime-core-v2` shows 3 members and falling lag | < 1 min | Logs per core; core refuses duplicate `source_id` bindings by design |
| 4 | `startx market_projector_1 market_projector_2` | `docker exec ${P}-market_projector_1-1 cat /status/status.json`: `errors` empty, `stage_b.owned` non-empty; on an empty cache `stage_b.builds` rises to the number of owned partitions and `published` rises when each build ends | rebuild: section 5 | `integrity stop` in `errors` = a record that does not decode: **stop and escalate**, do not flush or reset offsets; `memory pressure` = market_cache full (`noeviction`), check its memory |
| 5 | `startx query_kn_1 query_kn_2 stream_kn_1 stream_kn_2` | Query `healthy`; `curl --noproxy '*' --resolve qdl-v2-query:18301:127.0.0.1 --cacert $I/ca.crt --cert $I/client.crt --key $I/client.key https://qdl-v2-query:18301/health/ready` (`I` = the TS writer identity dir mounted at `/run/secrets/qdl-v2`) returns `READY`, same on 18302; `/health/dependencies` lists `query_cache ready_products` close to the pre-reboot value (706 of 716 on 2026-10-19) | ready products follow the projector publishes | NOT_READY products while step 4 still builds is expected; a dependency `false` names the missing role |
| 6 | `startx ingestor_binance_usdm ingestor_okx_swap binance_bar_edge` | All three `healthy` (session-liveness LIVE / bar heartbeat fresh) | ingestor LIVE within its establish budget (OKX quote make-before-break, IDR-112) | Venue reachability first (proxy, DNS); one ingestor per venue, no second copy (SPOF runbook) |
| 7 | `docker start market_data_service_p184w` (only if exited) | Postgres `service_heartbeats` row `market_data`: `status READY`, `consumer_mode V2_PRIMARY`, `demanded_v2_slices = 64`, `unhealthy_v2_slices = 0`, `v1_fallback_count = 0`, `last_seen_at` fresh | after step 5 is READY | The writer stays fail-closed (not eligible) until Query publishes; do not switch it to V1 (BLOCKED) |

Step 7 heartbeat query (read-only):

```bash
docker exec live_data_executor sh -c 'psql -U "$POSTGRES_USER" -d live_data_executor -At -c "select status, details->>'"'"'consumer_mode'"'"', details->>'"'"'demanded_v2_slices'"'"', details->>'"'"'unhealthy_v2_slices'"'"', last_seen_at from service_heartbeats where service_name='"'"'market_data'"'"'"'
```

Recovery is complete when step 7 holds for 5 consecutive minutes. Record the start and end times of each step in
the plan journal.

## 5. market_cache rebuild (stage B) - measured on an isolated stack

Mechanism: no checkpoint in an empty cache -> each owned partition of `md.latest.v2` and `md.bars.v2` (6 + 6) is
read from its start, applied into a staged generation, and published at the end offset read at assignment
(`stage_b.rs:8-13`). On 2026-10-19 `market_projector_1` owned all six `md.latest.v2` partitions (status file), so
the latest topic, which carries almost all of the data, is rebuilt by one projector at its 0.75 CPU cap.

Measurement: **pending**. The isolated drill (state topics copied read-only into a drill broker, one projector with
stage A off and the cleaner off, a drill Redis with the production arguments, <= 1.5 vCPU) is queued behind the
host measurement lock; its result and receipts go into this section and the plan entry "F3 KN reboot runbook".
Until then the rebuild time is unmeasured. Size for orientation (broker 1, 2026-10-19): `md.latest.v2` 4.35 GiB in
6 partitions (395-1,131 MiB each), `md.bars.v2` 0.93 GiB; production `market_cache` holds 47,751 keys / 1.16 GiB.
Production stage B cannot be pointed at a separate consumer group: `run_stage_b` hard-codes the group
`kn-projector-v3-b` (`main.rs`), so a second projector against production Kafka would join the production group.

What the number does not include: Kafka log recovery after an unclean shutdown, the Docker restart back-off of
roles that start before their dependencies, Query/Stream warm-up and the writer's own session establishment.

## 6. Rollback and fallback

- There is no data fallback: V1 (`data_layer_service`) is a rollback image only and every TS product is
  `fallback: BLOCKED`. The writer stays fail-closed until V2 is READY. This is the intended behaviour.
- A role that will not start: inspect its log and the dependency named by its error; do not recreate. If the
  container itself is damaged, the only rollback is the certified image by digest from
  `upgrade/evidence/releases/v2.2.4/certificate.json` through an approved recreate packet built from
  `docker inspect` (not from a compose file), which needs owner approval and a DL certificate re-issue.
- Cache rebuild stuck on an integrity stop: escalate; the record and partition are in the projector status.
  Flushing the cache only restarts the same build and loses nothing, but it also proves nothing; an offset reset
  is never part of this runbook.
- mTLS certificates (CA and leaves) expire 2026-11-20 14:43Z; after that date a reboot cannot recover anything
  until the mesh is rotated (owner-approved maintenance window).

## 7. Legacy boot unit retirement and the proposed KN boot unit

Retired 2026-10-19: `/etc/systemd/system/qdl-v2-stable-boot-recovery.service` (disabled since installation, never
ran on the KN topology) ran `/home/bobby/data_layer/scripts/v2_stable_boot_recovery.py`, the spool-era recovery.
The unit file was moved out of `/etc/systemd/system` to
`~/.local/state/qdl-v2/boot-recovery/retired/qdl-v2-stable-boot-recovery.service.20261019` and systemd reloaded.
Rollback: move it back and `sudo systemctl daemon-reload` (it stays disabled). The repository copy
`deploy/systemd/qdl-v2-stable-boot-recovery.service` and the two spool scripts remain for history; do not install
them again.

Proposed replacement (packet only, **not installed**; installing it changes host boot and needs owner approval):
a one-shot unit that observes and records, and starts nothing by itself. Docker already starts every role
(section 2); what is missing after a reboot is a receipt and a timed verdict, not another process starting
containers.

```ini
[Unit]
Description=QDL KN post-boot recovery observation (receipt only)
After=docker.service network-online.target
Requires=docker.service

[Service]
Type=oneshot
User=bobby
WorkingDirectory=/home/bobby/data_layer
# The runner opens --out with O_EXCL (a receipt is never overwritten), so each boot needs its own name;
# "%%" and "$$" are systemd escapes for "%" and "$".
ExecStart=/bin/sh -c 'exec /usr/bin/python3 -B /home/bobby/data_layer/scripts/kn_recovery_runner.py \
  --packet /home/bobby/.local/state/qdl-v2/boot-recovery/kn-boot-packet.json \
  --out /home/bobby/.local/state/qdl-v2/boot-recovery/kn-boot-$$(date -u +%%Y%%m%%dT%%H%%M%%SZ).json'
# policy.deadline_seconds is capped at 3600 by the runner.
TimeoutStartSec=3900

[Install]
WantedBy=multi-user.target
```

The runner (`scripts/kn_recovery_runner.py`) is observe/propose only without `--approve-start-id`, never
stops, recreates, flushes or resets offsets, and exits on SERVING or BLOCKED within one deadline. Prerequisites
before it can be installed, all still open:

1. A production probe executable for the packet's `probe_command` that runs
   `scripts/kn_recovery_observe.py --config ...` inside a `--rm` container of the certified python image on
   `qdl_v2_stable_candidate_stable_internal` with the projector identity mounted read-only (the observer needs
   Kafka AdminClient access, the TS writer heartbeat and `docker inspect`).
2. The packet: exact role ids/images (regenerated after every recreate), gate policy with the deadline from
   section 5 plus margin, disk thresholds.
3. `/home/bobby/data_layer` brought to the released line (it is `main` 5bf5cd5 from 2026-10-02); the unit runs
   scripts from there.
4. One real reboot drill in a maintenance window to measure the whole sequence end to end.

The TS `monitor_service` Data Layer watchdog (`DATA_LAYER_V2_*` alerts from the writer heartbeat) is the
alerting side of the same picture; it is being restored separately (TS PR #21, F2).
