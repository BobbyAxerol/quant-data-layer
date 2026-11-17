# DL v2.3.1 rollout plan: Query x2 recreate (plus the coupled TS writer recreate)

Status: **prepared offline, not executed.** The coordinator runs it only after the owner approves this blast radius
and rollback (CLAUDE.md section 3). Owner decision 2026-11-17 (option A): v2.3.1 rolls Query x2 now; the Rust roles
and `binance_bar_edge` move to the new images in a separate follow-up window (section 6).

Rules carried from the v2.3.0 windows:
- Any gate FAIL triggers that step's rollback without asking again.
- Never `compose up` kafka*, stable_redis, market_cache, the cores, projectors, streams, ingestors or
  binance_bar_edge in this window.
- Recreate one Query at a time. The other replica serves, and the writer's replicated client (section 4) or its
  alias fails over to it.
- Lock: take the host measurement lock only for the latency "after" run (section 5). Release it right after.

## 1. Blast radius

| Role | Action | Image | Config |
|---|---|---|---|
| `query_kn_1`, `query_kn_2` | recreate, one at a time | `qdl-v2-python:2.3.1-<sha7>` (built at R0, digest recorded) replaces `a235c0a1` | unchanged: the same compose, and the v2.3.1 env renders the same values as the deployed env list (`tests/test_kn_stable_compose.py::test_v231_env_is_the_one_versioned_runtime_env`) |
| `market_data_service_p184w` (TS writer) | recreate on a TS image with the replicated Query client and `snapshot:batch` probes (TS-owned, separate TS rollout) | TS image from `feat/v213-writer-batched-quote-probe` + replica balancing (TS writer agent) | `DATA_LAYER_V2_QUERY_URL=https://query_v2_1:8200,https://query_v2_2:8200` (TS convention, as `DATA_LAYER_V2_GRPC_TARGET` already is) |

**Not touched:**
- kafka1-3, stable_redis, market_cache;
- rust_core x3, market_projector x2, stream_kn x2, ingestor_okx_swap, ingestor_binance_usdm (all stay on rust `58bdd282`);
- binance_bar_edge (stays on python `a235c0a1`);
- catalogue, acquisition, demand, scope, routing and consumer manifests (r15/r23/r11/r14/r32, TS r14), the sealed binding r26
  and the JWT manifest revision (14);
- certificates, the cursor key and the secrets;
- `scripts/phaseb_prepare_stable_candidate.py`, which is never run.

**Coupling (recorded):**
- DL and TS ship separately, in either order.
- B4 `snapshot:batch` is additive. A Query without it answers 404 and the TS writer falls back to per-route GET
  (TS `feat/v213-writer-batched-quote-probe` 3438bc90).
- Replica balancing is client-side (SDK 2.6.0 `build_replicated_rest_query`). The per-replica names `query_v2_1` and
  `query_v2_2` are already Query certificate SANs and aliases on `executor_network`.
- The JWT key cache is server-internal.
- The writer must stay on the TS image it runs at the time of the window, unless the TS rollout is the step being run.
  Pass `TRADING_SYSTEM_RELEASE_IMAGE` = the writer's current image (CLAUDE.md section 4.8).

## 2. Variables (no secrets)

```bash
Q=/home/bobby/.local/state/qdl-v2; R=$Q/releases; P=qdl_v2_stable_candidate
CD=$R/v2.3.0-rc2-compose; CF=$CD/qdl-v2-stable-kn.yml                 # compose sha256 5b1381e0... == deploy/compose/qdl-v2-stable-kn.yml
OLD="--env-file $CD/qdl-v2-stable-kn.v2.3.0-rc3.env --env-file $R/v2.3.0-stage-b-compose/stage-b.env"   # the live list (labels)
NEW_ENV=$R/v2.3.1-compose/qdl-v2-stable-kn.v2.3.1.env                   # = repo deploy/compose/...v2.3.1.env with QDL_IMAGE_QUERY set at R0
READER_TLS=$Q/tls/consumer-ca-20261111T1304Z/deployed-tls.env; SECRETS=$R/v2.2.5/kn-secrets.env   # never cat
C="docker compose -p $P -f $CF"
WIN=$Q/v231-window-$(date -u +%Y%m%dT%H%MZ); mkdir -p -m 700 $WIN; W=market_data_service_p184w
```

## 3. Steps and gates

**R0 build (offline, no runtime change).**
- Build: `python3 -B scripts/build_qdl_release_images.py --rev <release head> --component python --build --lock-owner <owner>`.
  The script needs the host lock held by `--lock-owner`. Hold it for the build only.
- Record the digest, then write `NEW_ENV` = the repo v2.3.1 env with only `QDL_IMAGE_QUERY` changed (`QDL_IMAGE_BAR_EDGE`
  stays a235c0a1).
- Gates:
  - the post-checks PASS;
  - `pip-audit` on the new image: no finding with an empty ignore list;
  - trivy 0.70.0 (ignore-unfixed, CRITICAL,HIGH) with the two-entry `.trivyignore`: exit 0;
  - `diff <(sed /QDL_IMAGE_QUERY/d $NEW_ENV) <(sed /QDL_IMAGE_QUERY/d deploy/compose/qdl-v2-stable-kn.v2.3.1.env)` is empty.

**R1 pre-window baseline (read only).**
- Capture 10 minutes of:
  - per-replica CPU (`docker stats`);
  - `qdl.stall slow_request` per minute per replica;
  - the writer heartbeat (ready == demanded, exec count);
  - the DL total vCPU median.
- The "before" latency report is `LATENCY_REPORT.md` section 2.

**R2 query_kn_2 then R3 query_kn_1, one at a time.**
```bash
for s in query_kn_2 query_kn_1; do
  docker inspect $P-$s-1 --format '{{.Image}} {{.State.StartedAt}}' | tee $WIN/$s.pre
  $C --env-file $NEW_ENV --env-file $READER_TLS --env-file $SECRETS up -d --no-deps --force-recreate $s
  health $s || { echo "ABORT $s"; break; }                       # helper from the v2.3.0 window env.sh (reads only)
  docker exec $P-$s-1 sh -c 'grep -m1 -E "^\s+revision: 14" /consumer-realms/trading-system.paper.stable-r14.yaml'
  sleep 120; done
```
- Pass, for each Query:
  - healthy, RestartCount 0, and image == the R0 digest;
  - Paper manifest revision 14 loaded;
  - the B4 smoke `POST /v2/market-data/snapshot:batch` with 2 items under the writer identity returns 200
    `qdl.marketdata.snapshot-batch.v1`. Use the endpoint benchmark `operations: ["snapshot_batch"]`, rounds 1;
  - since R2 the writer shows:
    - 0 auth refusals;
    - 0 `quota is exhausted`;
    - 0 `CURSOR_EXPIRED|AUTHORITY_MISMATCH|RESUME_GENERATION_MISMATCH`;
    - ready == demanded, and exec >= the R1 value.
- Rollback per role: `$C $OLD --env-file $READER_TLS --env-file $SECRETS up -d --no-deps --force-recreate <role>`, back to
  image a235c0a1. Both env lists render the same values except `QDL_IMAGE_QUERY`.

**R4 TS writer (TS rollout, run by the TS writer agent under its own packet).**
- The DL gates the coordinator checks after it:
  - writer Query requests/min <= 3,500;
  - `qdl.stall slow_request` (reference:batch) on either replica down from about 213/min to <= 20/min;
  - per-replica Query CPU within a 60/40 split: the expectation is about 0.6 / 0.6 against 0.94 / 0.26;
  - MARK p99 <= 1,000 ms and QUOTE p99 <= 1,000 ms (writer heartbeat, as W-MRT).
- Rollback: the TS packet's writer rollback (its previous image and env). The DL side needs no change, because the
  per-route GET and the alias still work.

**R5 soak and CPU gate.**
- 30 minutes.
- Pass:
  - DL median <= 6.5 vCPU, the owner target (D3);
  - the gates above hold;
  - no `qdl.stall` above 1 s.
- FAIL: roll back R4 first (TS), then R3/R2 (Query) if the failure is on the DL side.

## 4. Writer client change (TS-owned, for reference)

- Use `qdl_sdk.build_replicated_rest_query(("https://query_v2_1:8200", "https://query_v2_2:8200"), tls=...,
  credential_provider=..., timeout_seconds=..., max_connections_per_replica=<N>)` in place of one `RestQueryTransport` on
  `qdl-v2-query`.
- Keep-alive equals N, so idle connections are not closed. This also stops the about 130 TIME_WAIT sockets and the
  about 2 TLS handshakes per second.
- Each request round-robins over the healthy replicas. A transport error retries once on the other replica.
- A typed error is never retried on the other replica.
- The vendored SDK in the TS image must be 2.6.0 or later.

## 5. Latency "after" run

The same command set as the "before" run (`LATENCY_REPORT.md` section 1), under a coordinator-held host lock, within 2 h after R5.

## 6. Follow-up window (not in this rollout): Rust roles and binance_bar_edge

- The v2.3.1 tree carries the refreshed `Cargo.lock` (tokio 1.51.6, futures-util 0.3.34 without pin-utils, libz-sys 1.1.33,
  rustls-webpki 0.103.17, webpki-roots 1.0.12, zstd-sys 2.1.3) and Python lock (certifi 2026.10.23, msgpack 1.2.4; pillow
  and soupsieve removed).
- Until that window, these roles run the v2.3.0 images, which carry the advisories the v2.3.0 waiver named:
  - rust_core x3, market_projector x2, stream_kn x2 and both ingestors on rust 58bdd282;
  - binance_bar_edge on python a235c0a1.
- The follow-up window rebuilds rust at the release head. The offline cargo cache of `build_qdl_release_images.py`
  (`CARGO_WORK`) must first gain the new crates.
- It rolls:
  - the cores one at a time (the W-A1 `batch_wait_ms` 50 configs are unchanged);
  - the projectors one at a time (no `stable_redis` recreate);
  - the streams one at a time;
  - the ingestors one at a time;
  - bar_edge last.
- Its gates are those of the v2.3.0 window A (D1-D6, D6b).
- Rollback per role: the v2.3.0 digest.
- The v2.3.1 RELEASE_NOTES list this tree-vs-runtime gap.
