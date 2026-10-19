# IDR-114 Query roll packet (draft, NOT executed)

Status: prepared 2026-10-17 by Claude. Nothing in this packet has been run against production.
Executing it needs the owner's approval of exactly this blast radius and rollback.

## 1. What changes

- Fix: IDR-114, per-request reference cycles in the Query process that paced 0.2-0.5 s GIL-holding generation-0
  collections (commit `31a5155`; mechanism, measurement and tests in `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`,
  section "P18.4 IDR-114").
  - `qdl/reference/local_mark_index.py`: in-process MARK/INDEX reads use `InProcessAsgiClient` instead of
    `httpx.ASGITransport` (same endpoint, same ASGI messages, no cycle per read).
  - `qdl/api_v2/router.py`, `qdl/runtime/kn_read_view.py`, `qdl/query/results.py`: a rendered refusal releases
    its traceback chain. Responses unchanged.
  - `qdl/runtime/stall_telemetry.py` + `qdl/runtime/stable.py`: `qdl.stall` stderr lines (kind gc >= 10 ms,
    loop_lag >= 50 ms, slow_request >= 250 ms). Observation only. Kept in production for the final stage-100.
  - GC thresholds `(100_000, 50, 100)`, `gc.freeze`, the GIL switch interval, public API, SDK, manifests: unchanged.
- Image: `qdl-v2-python:2.2.4-31a5155` `sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff`.
  - Label revision `31a515522aff3f6e5e7791aa21500cfa570ccb41`, version 2.2.4.
  - Build receipt `~/.local/state/qdl-v2/build-v2.2.4-31a5155/receipt.json` (`e1a9bcc5...`), status PASS (files vs rc19 ok, unexpected_different [], pip freeze equal, labels ok).
- Equivalence with the image running today (`c21e164a`, `qdl-v2-python:2.2.4-c7ce7d3`): `~/.local/state/qdl-v2/build-v2.2.4-31a5155/equivalence-vs-c21e164a.json` (`2ec87f34...`): of 439 hashed
  files under /app (`qdl`, `app`, `generated`, `config`, `consumers`, `qdl_sdk`) exactly 6 differ, equal to
  `git diff --name-only c7ce7d3 31a5155` on those roots: `qdl/api_v2/router.py`, `qdl/query/results.py`,
  `qdl/reference/local_mark_index.py`, `qdl/runtime/kn_read_view.py`, `qdl/runtime/stable.py`, new
  `qdl/runtime/stall_telemetry.py`. `pip freeze` equal. The 17 new/changed tests pass against the image's own
  `/app` code (`--network none`, tests mounted only).
- Rust image: unchanged. `5e261809` (`qdl-v2-rust:2.2.4-6d61710`) stays on both ingestors and `60f83a51` on
  `stream_kn_1/2` and `market_projector_1/2` (binaries whole-file equal except the ingestor, IDR-112 receipt).
  `31a5155` touches nothing under `rust/`.
- Measured effect (isolated two-replica stage-100 replay, 30 min, shadow scope; not gate evidence):
  | per replica | baseline A2 r1 / r2 | fix B r1 / r2 |
  | --- | --- | --- |
  | GC pauses >= 100 ms | 17 / 15 | 0 / 0 |
  | read stall clusters >= 250 ms | 12 / 9 | 1 / 0 |
  | max stalled read ms | 352 / 339 | 305 / - |
  | loop lag records >= 50 ms | 17 / 15 | 0 / 0 |
  Evidence `~/.local/state/qdl-v2/p184-idr114-query-gc-20261017/`.

## 2. Roles

| Step | Role | From | To | Expected diff | Serves meanwhile |
| --- | --- | --- | --- | --- | --- |
| 1 | `query_kn_1` | `c21e164a` | `3e9e4d3a` | `['image']` | `query_kn_2` |
| 2 | `query_kn_2` | `c21e164a` | `3e9e4d3a` | `['image']` | `query_kn_1` (new image) |

One at a time; step 2 only after step 1's HOLD passes. Each recreate takes about 20 s plus health (the 10-16 roll:
query_kn_1 20.4 s); the Stream read view and the TS writer fail over to the other replica meanwhile.

Not touched:
- `stream_kn_1/2`, `market_projector_1/2`, both ingestors, `rust_core` x3, kafka1-3, `stable_redis`,
  `market_cache`, `binance_bar_edge` (python `08f356a8`, its own image)
- the TS writer `market_data_service_p184w`, alphas, gates, scorer
- every config, catalog, consumer manifest, realm dir, JWT revision, Query env and mount

## 3. Preconditions (stop if any fails)

1. Owner approval of this packet.
2. Host lock taken by the executor (`scripts/host_measurement_lock.py acquire --owner <roll owner> --purpose
   "IDR-114 Query roll" --minutes 60`); `check` shows no foreign job.
3. `docker inspect` of both Query containers: image `c21e164a`, RestartCount 0, healthy, env/mounts equal to
   `~/.local/state/qdl-v2/p184-d3-capacity-20261009/rollout-rc15/query_kn_{1,2}-inspect-20261016T17*.json` except
   runtime-only fields (the tool asserts the diff is `['image']`).
4. Baseline HOLD:
   `S0=$(date -u +%s); python3 -B $P/hold_check.py $S0 $PK/hold-pre.json` - writer READY 64 64, no new error
   lines.

## 4. Commands

`T=~/.local/state/trading-system/p184-20261005/runtime/p1/rc15/rollout_rc15.py` (sha256 `811efd19...`, the tool of
the 2026-10-16 Query roll; Query roles only take the python image, the rust argument is ignored for them);
`P=~/.local/state/qdl-v2/releases/v2.2.4/rollout/postchecks`; `PK=~/.local/state/qdl-v2/releases/v2.2.4/rollout-idr114`
(new dir); `PY=sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff`; `RUST=sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e`.

```
mkdir -p $PK
S=$(date -u +%s)
python3 -B $T $PY $RUST query_kn_1        # receipts in .../p184-d3-capacity-20261009/rollout-rc15/; exit 2 = rolled back
python3 -B $P/verify_service.py qdl_v2_stable_candidate-query_kn_1-1 <query_kn_1-inspect-<stamp>.json from this run> $PY
docker logs --since $S qdl_v2_stable_candidate-query_kn_1-1 2>&1 | grep -c '"kind":"gc"'
python3 -B $P/hold_check.py $S $PK/hold-after-query_kn_1.json $PK/hold-pre.json
# 10 min watch (section 5), then step 2 with query_kn_2
```

## 5. HOLD gates (as in the v2.2.4 roll, plus IDR-114 checks)

- Roll tool: compose exit 0, image id = `PY`, diff `['image']`, env/volume diff empty, healthy, writer READY 64 64.
- `verify_service.py`: `image_ok` true, mismatch `[]`, RestartCount 0, healthy.
- `hold_check.py` against `hold-pre.json`: writer READY 64 64; RestartCount unchanged on every DL container and the
  writer; `rust_core` x3 0 retry/rebalance/started lines; 0 new error lines; stream refused/overflow deltas
  reported (the 10-16 roll recorded pre-existing writer-side overflow, scored by the final stage-100).
- IDR-114 (10 min after each step, from the new container's log):
  - `qdl.stall` lines present only as JSON with kinds gc / loop_lag / slow_request (the writer thread runs).
  - Report, do not gate: count and max of `kind=gc` >= 100 ms and `kind=loop_lag`, and
    `qdl_refusal` lines. Expected with TS writer + alpha MARK load: no gen0 collection paced by MARK reads
    (baseline would show one every ~76 s).
  - STOP and roll back: any traceback in the log, an `http.disconnect`/ASGI assertion error, MARK/INDEX reads
    failing (`qdl_refusal` with route `/reference` or writer MARK `SOURCE_UNAVAILABLE` rising above the pre-roll
    rate), or writer READY < 64.

## 6. Rollback

- Automatic: `rollout_rc15.py` recreates the role from its saved pre-roll compose (`query_kn_N-rollback-<stamp>`)
  when health, writer READY or the log check fails, and exits 2.
- Manual: `docker compose -f <query_kn_N-rollback-<stamp>.compose.json> up -d --no-deps --force-recreate query_kn_N`,
  then `verify_service.py` with `sha256:c21e164ac4db14af498b73104e344f003fa7117fdae2e465f50df8668cdca252`.
- Reference composes that put `c21e164a` live on 2026-10-16:
  `p184-d3-capacity-20261009/rollout-rc15/query_kn_1-rc15-20261016T171446Z.compose.json` and
  `query_kn_2-rc15-20261016T171801Z.compose.json`.
- No data, cache, Kafka offset or Redis key is touched by the roll or its rollback.

## 7. After the roll

- Journal the receipts in the DL plan; insert or update IDR-114 in the TS `INTEGRATION_DEFECT_REGISTER.md`
  (row text below; the register had another agent's uncommitted edit when this packet was written).
- The final stage-100 scores the stall with the `qdl.stall` lines in production: per replica count of
  `kind=gc` >= 100 ms and their alignment with the receipt's read outliers (`scripts/p184_query_gc_isolated.py
  analyse` logic).
- Stream: the shared `configure_query_interpreter` installs the gc hook in Stream too, but Stream runs the rust
  image `60f83a51`; no python Stream role is affected.

## 8. Register row to insert (TS `INTEGRATION_DEFECT_REGISTER.md`)

```
| IDR-114 | DATA_LAYER (Python Query process; image c21e164a) | Each Query replica stalled 0.3-0.5 s about every 76 s in production (isolated replay: 0.2-0.26 s every ~104 s, verified in process): CPython generation-0 collections whose whole 100,000 threshold was cyclic garbage, holding the GIL. The rarer 0.5-1.1 s stalls with a ~500 MiB RSS drop were not reproduced in 30 min (inferred same source, unverified). Two per-request cycle sources: (1) the in-process MARK/INDEX reads (`qdl/reference/local_mark_index.py` `reader_for_view`) went through `httpx.AsyncClient(transport=httpx.ASGITransport(app))`; every httpx request/response pair is a reference cycle (25-27 objects per read); (2) a typed refusal's traceback held every frame of the hot thread, lane task and ASGI stack (~190-280 objects per refused request). The RCA candidates `kn_query_backend.py:389/:703` were not confirmed (0 objects). | RCA DL `507b446` (135719Z: 27/26 stall clusters per replica, max 2486/694 ms, MARK DOGEUSDT 1097.7 ms inside a query_kn_2 stall). Isolated two-replica stage-100 replay with in-process GC telemetry and DEBUG_SAVEALL, DL plan section "P18.4 IDR-114"; evidence `~/.local/state/qdl-v2/p184-idr114-query-gc-20261017`. | FIXED in DL `31a5155`, tested locally, isolated-replay measured: per replica GC pauses >= 100 ms 17/15 -> 0/0, read stall clusters >= 250 ms 12/9 -> 1/0, loop-lag records 17/15 -> 0/0 (30 min each; GC thresholds unchanged). `InProcessAsgiClient` (same endpoint and ASGI messages), `release_exception_frames` after a refusal is rendered (responses unchanged), `qdl.stall` telemetry (gc / loop_lag / slow_request). Image `qdl-v2-python:2.2.4-31a5155` `3e9e4d3a`: vs `c21e164a` exactly the 6 changed python files differ, pip freeze equal. Roll packet `upgrade/evidence/releases/v2.2.4/drafts/IDR114_QUERY_ROLL_PACKET.md` (DL). | - (packet prepared: query_kn_1 then query_kn_2) | DL: yes (v2.2.4 python image 3e9e4d3a); TS: none |
```
