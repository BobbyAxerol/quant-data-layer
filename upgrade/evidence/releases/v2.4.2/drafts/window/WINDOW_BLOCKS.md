# DL v2.4.2 r10 rollout: window blocks (prepared 2026-11-19, nothing applied)

The coordinator runs every block. The agent only prepared files and runs read-only gates. Always
`up -d --no-deps --pull never <one service>`. Never a project-wide `up`, never `--remove-orphans`, never
`stable_redis` or `market_cache`.

```
P=qdl_v2_stable_candidate; R=/home/bobby/.local/state/qdl-v2/releases; C=$R/v2.4.2-compose; WG=$C/window/v242_window_gates.sh
BASE="--env-file $R/v2.3.1-compose/qdl-v2-stable-kn.v2.3.1.env --env-file $R/v2.3.1-stage-c-compose/stage-c.env"
SEC="--env-file $R/v2.2.5/kn-secrets.env"
TLS_C="--env-file /home/bobby/.local/state/qdl-v2/tls/consumer-ca-20261111T1304Z/deployed-tls.env"   # query, stream
TLS_G="--env-file /home/bobby/.local/state/qdl-v2/tls/gen-20261019T084200Z/generation.env"           # projector, bar_edge, ingestor
NEW="-f $C/qdl-v2-stable-kn.v2.4.2.yml $BASE --env-file $C/v2.4.2.env"
# rollback layerings = exactly what each role's live labels name today (proved by v242_render_hash_check.sh)
OLD_Q="-f $R/v2.4.1-compose/qdl-v2-stable-kn.v2.4.1.yml $BASE --env-file $R/v2.4.1-compose/v2.4.1-b.env"
OLD_S="-f $R/v2.4.0-r2-compose/qdl-v2-stable-kn.v2.4.0-r2.yml $BASE --env-file $R/v2.4.0-r2-compose/v2.4.0-r2.env"
OLD_P="-f $R/v2.3.1-stage-c-compose/qdl-v2-stable-kn.stage-c.yml $BASE"
OLD_B="$OLD_Q"                                   # bar_edge: v2.4.1 compose + v2.4.1-b.env (with TLS_G)
OLD_I="$OLD_S"                                   # ingestors: v2.4.0-r2 compose + env (with TLS_G)
```

## Prepared files

The full list with sha256 is in `$C/SHA256SUMS.dirs` and `$C/SHA256SUMS.compose`.

- `$R/v2.4.2-r10`: a `cp -a` of `v2.3.1-stage-c`. Only the 7 r10 files differ:
  - `bundle.json`: r10 kn bundle, field `1312c3bb…`, file `5ea1bc0f…`;
  - `runtime/stable-source-bindings.yaml`: catalogue r17, `635237bd…`;
  - `runtime/stable-acquisition-bindings.yaml`: `91ac93f7…`;
  - `runtime/bar-edge-source-bindings.yaml`: `98949326…`;
  - `runtime/bar-edge-acquisition-bindings.yaml`: `760e51f2…`;
  - `runtime/bar-edge-checkpoint-migration.json`: `0f98b8f7…`, 16/24 to 17/25, retained 1319, dropped 1;
  - `runtime/declaration-index.json`.
- `$R/v2.4.2-realm`: a `cp -a` of `v2.4.0-realm`, plus:
  - `bundle.json`: r10 realm bundle, field `016e68b4…`, file `8f57d0a8…`;
  - `alpha.okx.paper.stable-r18.yaml` (`226fb409…`);
  - `alpha.okx.live.stable-r2.yaml` (`f0903078…`);
  - `alpha.okx.sandbox.stable-r2.yaml` (`04c74043…`).
- Both dirs were diffed against their sources: only those files differ (the sha256 lists match the committed r10
  render, `upgrade/evidence/releases/v2.4.2/declaration-retire-r10/RENDER_SHA256SUMS`).
- `$C/qdl-v2-stable-kn.v2.4.2.yml`: the v2.4.1 compose with one delta, the Query x2 `QDL_STABLE_CONSUMER_MANIFESTS`:
  paper r17 to r18, live and sandbox to `-r2`.
- `$C/v2.4.2.env`:
  - images: python `5ea7a7a4…` on Query and bar_edge, stream `abafab2c` unchanged;
  - `QDL_KN_RELEASE_DIR=v2.4.2-r10`, `QDL_CONSUMER_REALM_DIR=v2.4.2-realm`;
  - canonical enabled, as in v2.4.1-b.
- `$C/v2.4.2-ingestor.env`: rust `e628fbe0…` (W6 only).
- Render-hash check (`$C/window/v242_render_hash_check.sh`, output in
  `~/.local/state/qdl-v2/v242-retire-r10-render/render-hash-check.txt`):
  - Every live role except `market_cache` is reproduced from its own labels (16/16 PASS).
  - `market_cache` was created from `v2.2.0-02cd827/compose.json` and is untouched.
  - Every window role's target hash differs, with the intended image.

## Order: corrected against the v2.4.0 window 1 RCA

You asked for Query x2 first, then Streams. That order would repeat the v2.4.0 window 1 failure:
- Query on catalogue r17 hot-reads a Stream whose bundle is catalogue r16.
- `hot_view.rs:111-119` returns `HOT_AUTHORITY_MISMATCH` on every replica.
- Quiet EXECUTION QUOTE then goes `DATA_NOT_READY`, and the writer falls below 246.

The proven safe order (v2.4.0 RCA section 8) is:

**W1 stream_kn_2 → W2 query_kn_2 → W3 query_kn_1 → W4 stream_kn_1 → W5a/b market_projector_1/2 → W6 binance_bar_edge
→ (W7 optional ingestors, separate GO).**

At each step Query always has a Stream replica on its own catalogue revision. The fall-through is in v2.4.0 and later
(`kn_hot_view.py`).

## Global preflight (once, before W1)

1. The v2.4.2 CI6 receipt is green: the dev full run on tree `b69e27c4…` (merge `77d4d5a`). Record RUN_ID.
2. Load: `uptime`. Run windows only under the owner's ceiling (25 to 30).
3. Baseline: `$WG stream_kn_2 $(date -u -d '-10 min' +%FT%TZ)`. Expected FAILs are the not-yet-applied deltas; the
   writer gates must PASS now (246/246).
4. `$C/window/v242_render_hash_check.sh` must end `RENDER-HASH CHECK PASS`. This proves the rollback layerings
   reproduce the live roles.
5. Images are present: `docker image inspect qdl-v2-python:2.4.2-48137eb qdl-v2-rust:2.4.2-48137eb`.

## W1 stream_kn_2 (r10 realm bundle; image unchanged)

- Preflight: writer 246/246; stream_kn_1 healthy (it carries all subscriptions during W1).
- Apply:
  `T0=$(date -u +%FT%TZ); echo $T0 > ~/.local/state/qdl-v2/v242-T_W1; docker compose -p $P $NEW $TLS_C $SEC up -d --no-deps --pull never stream_kn_2`
- Wait for healthy (up to 240 s), then `$WG stream_kn_2 $T0`. Gates:
  - image `abafab2c`, rc 0, healthy;
  - realm mount `v2.4.2-realm`, kn mount `v2.4.2-r10`;
  - `bundle_sha256=016e68b4…`;
  - hub decode/reader errors 0, refused 0;
  - G-hot: no authority refusal through each Query's configured client;
  - Query `HOT_AUTHORITY_OR_PROTOCOL_REFUSED` = 0 since T0. Authority-fallback lines are expected and INFO only: the
    r16 Queries fall through to stream_kn_1.
  - writer 246/246 (or only the quiet class) within 120 s; retries `{}`; refusals 0.
- Rollback: `docker compose -p $P $OLD_S $TLS_C $SEC up -d --no-deps --pull never stream_kn_2`

## W2 query_kn_2, then W3 query_kn_1 (python 2.4.2, catalogue r17, manifests paper r18 / live r2 / sandbox r2)

- Preflight: W1 PASS; writer 246/246.
- Apply, one role at a time:
  `T0=$(date -u +%FT%TZ); docker compose -p $P $NEW $TLS_C $SEC up -d --no-deps --pull never query_kn_2`.
  After its gates PASS, do the same for `query_kn_1`.
- Gate: `$WG query_kn_2 $T0` (and `query_kn_1`):
  - image `5ea7a7a4`, rc 0, healthy;
  - mounts: realm `v2.4.2-realm`, `/kn` and `/runtime` on `v2.4.2-r10`, `/canonical`;
  - manifests are paper r18, live r2, sandbox r2 and TS r15; hot targets b,a; canonical enabled, index 527 / 1 /
    `cacea7bb…`; no Traceback.
  - RVN typed refusal (`v242_rvn_refusal_probe.py`, run in the container on the deployed image and files):
    - catalogue r17 holds 0 RVN bindings;
    - no loaded manifest names RVN;
    - an RVN snapshot returns `INSTRUMENT_NOT_FOUND`;
    - a mixed snapshot batch returns per-item `DATA_NOT_READY` / `INSTRUMENT_NOT_FOUND`.

    Tested before install on `5ea7a7a4` (PASS). On the old `929eeac2` it raises an untyped `KeyError` (FAIL).
  - G-hot and query refusals as in W1. Writer 246/246, retries `{}`, refusals 0, no `HOT_AUTHORITY` on the native
    probe routes.
- Rollback (per role): `docker compose -p $P $OLD_Q $TLS_C $SEC up -d --no-deps --pull never query_kn_2` (or
  `query_kn_1`).

## W4 stream_kn_1 (r10 realm bundle)

- Preflight: W2 and W3 PASS. Both Queries are now on r17, so stream_kn_2 serves their hot reads.
- Apply: `T0=$(date -u +%FT%TZ); docker compose -p $P $NEW $TLS_C $SEC up -d --no-deps --pull never stream_kn_1`
- Gate: `$WG stream_kn_1 $T0`, with the same gates as W1. After W4, authority-fallback lines must stop.
- Rollback: `docker compose -p $P $OLD_S $TLS_C $SEC up -d --no-deps --pull never stream_kn_1`

## W5a market_projector_1, then W5b market_projector_2 (r10 kn bundle; image 58bdd282 unchanged)

- Preflight: W4 PASS.
- Apply, one at a time:
  `T0=$(date -u +%FT%TZ); docker compose -p $P $NEW $TLS_G $SEC up -d --no-deps --pull never market_projector_1`
- Gate: `$WG market_projector_1 $T0`:
  - image `58bdd282`, rc 0, healthy; kn mount `v2.4.2-r10`;
  - started event `bundle_sha256=1312c3bb…`, stage_a and stage_b true;
  - status errors `{}`, poll_errors 0, last_rebuild_error None;
  - writer 246/246.
- Rollback: `docker compose -p $P $OLD_P $TLS_G $SEC up -d --no-deps --pull never market_projector_1` (and the same
  for `_2`).

## W6 binance_bar_edge (python 2.4.2, r10 bar-edge pair and migration record)

- Preflight:
  - W5 PASS.
  - Note the restart-count baseline.
  - Back up the live checkpoint and short-proof sidecar while the old edge is running. This only reads the old files
    and writes `.pre-v242` copies:

    `docker exec $P-binance_bar_edge-1 sh -c 'cp -p /var/lib/qdl-kn/crypto-bar-checkpoint.json /var/lib/qdl-kn/crypto-bar-checkpoint.json.pre-v242 && cp -p /var/lib/qdl-kn/crypto-bar-checkpoint.json.history-short.json /var/lib/qdl-kn/crypto-bar-checkpoint.json.history-short.json.pre-v242 && sha256sum /var/lib/qdl-kn/crypto-bar-checkpoint.json*'`

    The live checkpoint has 1320 binding_ids at 16/24. The r10 record's `from` and `previous_binding_digests` match
    it (gate `bar_edge_migration_record`).
- Apply: `T0=$(date -u +%FT%TZ); docker compose -p $P $NEW $TLS_G $SEC up -d --no-deps --pull never binance_bar_edge`
- Gates: `$WG binance_bar_edge $T0`. Repeat until there is no HOLD. A silent restore of 5 to 9 min is expected, and
  "unhealthy" during it is expected (runbook).
  - Image `5ea7a7a4`, rc 0.
  - `/runtime` on `v2.4.2-r10/runtime`; `QDL_STABLE_BAR_CHECKPOINT_MIGRATION=/runtime/bar-edge-checkpoint-migration.json`
    with mounted sha256 `0f98b8f7…`.
  - Log line `checkpoint crosses catalog 16->17 acquisition 24->25 crossed=True retained=1319 added=0 dropped=1
    migration_record=True`.
  - Heartbeat after T0: revisions 17/25, image_files 0, bindings=1319, **instrument_unavailable=0** (the new
    heartbeat counter; RVN is retired).
  - Traceback lines since T0 = 0. Today the r16 edge logs about 550 per 10 min, the RVN 51001 failure.
  - running and healthy at the end; writer 246/246.
  - Follow-up for INFO, not a gate: `published` should rise past 1118 toward 1319 over the next cycles, because the
    head-of-line block is gone. Track it like v2.4.1 C, for example with `be-track.sh`.
- HOLD:
  - Any `cache generation changed; bounded bootstrap required`: the coordinator decides.
  - No heartbeat after T0 within 20 min is a rollback trigger (runbook).
- Rollback, in order:
  1. `docker stop $P-binance_bar_edge-1`
  2. Restore the checkpoint as the edge uid (state dir `$R/v2.2.0-02cd827/state/bar-edge`):
     `docker run --rm --network none --user 10001:10001 -v $R/v2.2.0-02cd827/state/bar-edge:/s alpine:3.20 sh -c 'cp -p /s/crypto-bar-checkpoint.json.pre-v242 /s/crypto-bar-checkpoint.json && cp -p /s/crypto-bar-checkpoint.json.history-short.json.pre-v242 /s/crypto-bar-checkpoint.json.history-short.json'`
  3. `docker compose -p $P $OLD_B $TLS_G $SEC up -d --no-deps --pull never binance_bar_edge` (back to r16 runtime,
     old record `cd15ee73…`, image `929eeac2`).
  4. Gate with `v241_window_gates.sh binance_bar_edge $T0 bar` from `$R/v2.4.1-compose/window`.

## W7 (optional, separate GO): ingestor_binance_usdm, then ingestor_okx_swap (rust 2.4.2: 60018 per-argument rejection)

- Purpose: ship the live-path fix and the `subscriptions_rejected` telemetry. The configs (`v2.4.0-ingestor`) are
  unchanged.
- Apply: `T0=$(date -u +%FT%TZ); docker compose -p $P $NEW --env-file $C/v2.4.2-ingestor.env $TLS_G $SEC up -d --no-deps --pull never ingestor_binance_usdm`
- Gate: `$WG ingestor_binance_usdm $T0`:
  - image `e628fbe0`;
  - config mount unchanged; started bindings 118 / 465 (OKX) PINNED;
  - every lane flowing;
  - **subscriptions_rejected=0** summed over lanes, and 0 `qdl_native_subscription_rejected` lines;
  - cores scope_quarantines 0; liveness cadence >= 2.5/s; writer 246/246.
- Rollback: `docker compose -p $P $OLD_I $TLS_G $SEC up -d --no-deps --pull never ingestor_binance_usdm` (image
  `58bdd282`).

## Untouched

- kafka1/2/3.
- rust_core, rust_core_2, rust_core_3. They keep one inert RVN 1d row each; the r10-rendered core files remove it at a
  later core restart, not in this window.
- The ingestors, unless W7 gets a GO.
- market_cache and stable_redis (never).
- The TS writer `market_data_service_p184w` (`d1addc04`) and every TS role.
- The configs in `v2.4.0-ingestor` and `v2.3.1-stage-c-cores`.
- `v2.4.1-canonical`, the TLS generations and the secrets.
- The rollback sources, left in place and read-only: `v2.3.1-stage-c`, `v2.4.0-realm`, the v2.4.1 / v2.4.0-r2 /
  stage-c compose and env files.
- Images `929eeac2`, `abafab2c` and `58bdd282`, kept for rollback.

## Consumer notes

- alpha.okx.paper r18, live r2 and sandbox r2 drop the OKX RVN 1d row. Existing tokens stay valid (advisory floor 1).
- An RVN request now gets a typed refusal: PERMISSION_DENIED from the manifest, or INSTRUMENT_NOT_FOUND.
- Binance RVNUSDT is unchanged.
- These `execution_alpha` lists name RVN; none is running: scalping_PSAR, scalping_sl_tp, taalpha, grid_long_only,
  vwma00315, fib_sl_tp_strength, regressionportfolioA001, rsiboundportfolioA001, signal_combine, deep_momentum.
