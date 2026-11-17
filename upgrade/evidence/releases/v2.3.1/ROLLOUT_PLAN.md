# DL v2.3.1 rollout plan: Query x2 recreate

**Status: prepared offline, not executed.**
- The coordinator runs it after the owner approves this blast radius and rollback (CLAUDE.md section 3). The owner
  approved the v2.3.1 release flow on 2026-11-17, and chose option A that day: this window recreates Query x2 only.
  The Rust roles and `binance_bar_edge` move later, in the follow-up window (section 7).
- The TS writer replicated transport (W-QRR) is already live and KEPT: writer `sha256:cf88571c...`, receipt
  `~/.local/state/qdl-v2/wqrr-window-20261117T0310Z/RECEIPT.md`. This window does not touch the writer.

**Rules:**
- Any gate FAIL rolls back that step without asking again.
- Recreate one Query at a time. The writer's replicated transport sends the other replica the traffic, and fails over
  to it on a transport error (`qdl_sdk.transport.ReplicatedRestQueryTransport`).
- Never `compose up` kafka*, stable_redis, market_cache, rust_core*, market_projector*, stream_kn*, ingestor_* or
  binance_bar_edge. Never run `scripts/phaseb_prepare_stable_candidate.py`.
- Lock: take the host measurement lock for the R6 latency run only, and release it right after.

## 1. Blast radius

**Recreated: exactly two roles.**

| Role | From | To |
|---|---|---|
| `query_kn_2`, then `query_kn_1` | `qdl-v2-python@sha256:a235c0a18cdeb112ea0b49627eaf02f2a68ab878298ebba07884461ee5709d0b` | `qdl-v2-python@sha256:<R0 digest>` (built once from `aa752182`, see `R0_SECURITY.md`) |

Config is unchanged. `NEW_ENV` differs from the deployed env list only in `QDL_IMAGE_QUERY` (R0 gate). That covers
compose, catalogue, manifests, realm, TLS and secrets.

**Not touched:**
- Kafka x3, stable_redis, market_cache;
- rust_core x3, market_projector x2, stream_kn x2, both ingestors: all stay on rust `58bdd282`;
- binance_bar_edge: stays on python `a235c0a1`, because `QDL_IMAGE_BAR_EDGE` is unchanged;
- the TS writer (`cf88571c`) and all Paper/PM/portal roles;
- the declarations (catalogue r15, acquisition r23, demand r11, scope r14, routing r32), TS manifest r14, sealed
  binding r26, and JWT manifest revision 14;
- certificates, the cursor key and the secrets.

## 2. Variables (no secrets)

```bash
Q=/home/bobby/.local/state/qdl-v2; R=$Q/releases; P=qdl_v2_stable_candidate
CD=$R/v2.3.0-rc2-compose; CF=$CD/qdl-v2-stable-kn.yml                       # sha256 5b1381e0... == deploy/compose/qdl-v2-stable-kn.yml
OLD="--env-file $CD/qdl-v2-stable-kn.v2.3.0-rc3.env --env-file $R/v2.3.0-stage-b-compose/stage-b.env"   # live list (labels)
NEW_ENV=$R/v2.3.1-compose/qdl-v2-stable-kn.v2.3.1.env                         # repo env + QDL_IMAGE_QUERY=<R0 digest>
READER_TLS=$Q/tls/consumer-ca-20261111T1304Z/deployed-tls.env; SECRETS=$R/v2.2.5/kn-secrets.env   # never cat
C="docker compose -p $P -f $CF"; W=market_data_service_p184w
OLD_IMG=sha256:a235c0a18cdeb112ea0b49627eaf02f2a68ab878298ebba07884461ee5709d0b
WIN=$Q/v231-query-window-$(date -u +%Y%m%dT%H%MZ); mkdir -p -m 700 $WIN
```

## 3. R0: image and security (offline, done before the window)

- Built once from `aa752182`, using the `build_qdl_release_images.py` python recipe. The result is recorded in
  `R0_SECURITY.md`: full digest, pip-audit, trivy, dependency versions.
- Write `NEW_ENV` = `deploy/compose/qdl-v2-stable-kn.v2.3.1.env` with only `QDL_IMAGE_QUERY=qdl-v2-python@sha256:<R0>`.
- Gate: `diff <(grep -v ^QDL_IMAGE_QUERY= $NEW_ENV) <(grep -v ^QDL_IMAGE_QUERY= deploy/compose/qdl-v2-stable-kn.v2.3.1.env)` is empty.
- Gate: rendering `$C $OLD ... config` and `$C --env-file $NEW_ENV ... config` differs in the Query image only.
- Gate: pip-audit reports no findings with an empty ignore list. trivy 0.70.0 (ignore-unfixed, CRITICAL/HIGH,
  `.trivyignore` holding the 2 BuildKit entries) exits 0.

## 4. R1: baseline, 10 minutes, read only (`$WIN/r1-*`)

- Per-replica CPU (`docker stats`, 6 samples).
- `qdl.stall slow_request` per minute per replica.
- Writer heartbeat: slices ready/demanded, MARK and QUOTE p99, auth refusals, RATE_LIMITED.
- DL total vCPU median.
- Expected from the W-QRR soak:
  - Query 0.66-0.75 / 0.67-0.72 vCPU;
  - slow_request 84-158/min;
  - MARK p99 575 ms, QUOTE p99 760 ms;
  - 246/246 slices.

## 5. R2 `query_kn_2`, then R3 `query_kn_1`

```bash
for s in query_kn_2 query_kn_1; do
  docker inspect $P-$s-1 --format '{{.Image}} {{.State.StartedAt}}' | tee $WIN/$s.pre
  $C --env-file $NEW_ENV --env-file $READER_TLS --env-file $SECRETS up -d --no-deps --force-recreate $s
  health $s || { echo "ABORT $s"; break; }        # helper from the v2.3.0 window env.sh (reads only)
  docker inspect $P-$s-1 --format '{{.Image}} {{.RestartCount}}' | tee $WIN/$s.post
  docker exec $P-$s-1 sh -c 'grep -m1 -E "^\s+revision: 14" /consumer-realms/trading-system.paper.stable-r14.yaml'
  sleep 120
done
```

Gates for each replica, all must PASS before the next step:

| Gate | Pass |
|---|---|
| Image | `.Image` == the R0 digest, RestartCount 0, healthy |
| Manifest | the Paper realm r14 is loaded |
| B4 route | `snapshot:batch` answers **200** `qdl.marketdata.snapshot-batch.v1` on the recreated replica. Run the endpoint benchmark with `operations: ["snapshot_batch"]`, rounds 1, TS root-client profile, under the R6 lock or as one 2-item call: `run_q12.sh` with a snapshot-batch-only profile. Before v2.3.1 the same call is a 404, which the SDK reports as `INTERNAL_ERROR` |
| Writer | since R2: 0 auth refusals, 0 `quota is exhausted`, 0 `CURSOR_EXPIRED\|AUTHORITY_MISMATCH\|RESUME_GENERATION_MISMATCH`; slices **246/246**; exec-ready at or above R1 |
| Failover | during the recreate the writer reports no unrecovered Query transport error (replica cooldown and retry on the other replica) |

**Rollback, per role, by digest:**
- Run `$C $OLD --env-file $READER_TLS --env-file $SECRETS up -d --no-deps --force-recreate <role>`. The deployed env list
  pins `QDL_IMAGE_QUERY=qdl-v2-python@sha256:a235c0a1...`.
- Check `.Image` == `$OLD_IMG`.
- Rollback order is reverse: kn_1, then kn_2.

## 6. R4 soak (30 min), then R5 latency re-measure under lock

**R4 gates**, against R1 and the owner's gates:

| Gate | Pass |
|---|---|
| Query CPU per replica | each <= 0.9 vCPU, split within 60/40. A per-item saving is not expected in v2.3.1; the JWT parse cache is about -0.004 vCPU |
| slow_request (both) | <= the R1 rate +10 % |
| MARK p99 / QUOTE p99 (writer heartbeat) | <= 1,000 ms each (owner gate 11-16) |
| Slices | 246/246 throughout |
| DL total vCPU median | <= 7.5 (temporary ceiling); target <= 6.5 |
| Errors | no `qdl.stall` above 2 s that R1 did not have; no new refusal codes |

**R5: four-quantity latency re-measure.**
- Take the host lock (coordinator), then run in sequence, one container at a time:
  1. `~/.local/state/qdl-v2/v231-latency-q12/run_q12.sh after-v231`. Quantities 1 and 2. TS r14 root client, 2 rps.
     Replicas are now **interleaved per case**, the first replica alternates, and wall time is recorded per sample
     (tool at `aa75218`). `snapshot_batch` must PASS.
  2. The 300 s canonical capture plus `report_stable_endpoints.py` over the 6 manifests (quantity 3), as in
     `LATENCY_REPORT.md` section 1.
  3. 3 read-only Redis sweeps of db1 (quantity 4).
- Write the results into `LATENCY_REPORT.md` section 4 and compare them with "before" and "after W-QRR".
- Release the lock.

R4 FAIL means rolling back R3, then R2 (section 5).

## 7. Follow-up window, not in this rollout: Rust roles and binance_bar_edge (owner option A)

The tree carries the refreshed locks. Until that window, the Rust roles (on rust `58bdd282`) and bar_edge (on python
`a235c0a1`) keep running images that carry the advisories the expired v2.3.0 waiver named. Exposure is as assessed in
v2.3.0.

The window does the following:
- Rebuild rust at the release head.
  - The pinned Rust reference images that `build_qdl_release_images.py` checks have been removed.
  - Re-pin them, or update the builder, first.
  - The offline cargo cache needs the new crates.
- Roll in this order:
  - cores, one at a time (bw50 configs unchanged);
  - projectors, one at a time (no `stable_redis` recreate);
  - streams, one at a time;
  - ingestors;
  - bar_edge last, onto the v2.3.1 python digest.
- Gates: as in v2.3.0 window A (D1-D6, D6b).
- Rollback: per role, by the v2.3.0 digest.
