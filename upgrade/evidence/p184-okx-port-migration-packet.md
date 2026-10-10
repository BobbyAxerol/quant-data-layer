# P18.4 Packet: OKX WebSocket Port 8443 Migration

Status: `IMPLEMENTED / TESTED LOCALLY / LIVE ROLLOUT PENDING APPROVAL`.
Deadline: **before 2026-10-31**. After that date, OKX refuses connections on port 8443.

## 1. Venue fact

The OKX API changelog has an entry under "Upcoming Changes" titled
"WebSocket port 8443 discontinuation", last updated 2026-09-30
(https://www.okx.com/docs-v5/log_en/). It says:

- "OKX will stop accepting WebSocket connections on port 8443 on October 31, 2026".
  Port 443 already works on every affected domain, and both ports work until the cutover.
- Under "Breaking change": "After October 31, 2026, connections to port 8443 will fail."
  The fix is to remove `:8443`. Host and path stay the same, and `wss://` defaults to 443.
- The rule applies to `/ws/v5/public`, `/ws/v5/private` and `/ws/v5/business`, plus
  `/ws/v5/public-sbe` on the Global site. REST and Colo are not affected.
- Example: `wss://ws.okx.com:8443/ws/v5/public` becomes `wss://ws.okx.com/ws/v5/public`.
  For demo: `wss://wspap.okx.com:8443/...` becomes `wss://wspap.okx.com/...`.

The "Production Trading Services" section of the main API docs (https://www.okx.com/docs-v5/en/)
already lists `wss://ws.okx.com/ws/v5/{public,private,business}` with no port.

Live probe on 2026-10-10. It ran in `qdl-v2-python:2.2.3-rc6-10f9f5c` with `--cpus 1`.
Both port-less URLs connected on remote port 443 and delivered data:

| URL | open | first data | n |
|---|---:|---:|---:|
| `wss://ws.okx.com/ws/v5/public` (`tickers` BTC-USDT-SWAP) | 217.6 ms | 273.8 ms | 1 |
| `wss://ws.okx.com/ws/v5/business` (`candle1m` BTC-USDT-SWAP) | 182.1 ms | 234.7 ms | 1 |

This was a connectivity check, not an endpoint benchmark. It does not report the four latency quantities.

The deployed Rust ingestor (`qdl-v2-rust:2.2.1-855c914`) uses tokio-tungstenite 0.24.0
(per `Cargo.lock` at `855c914`). In `connect.rs:84`, `"wss" => 443`, so a URL with no port
connects on 443. The ingestor only checks that the URL starts with `wss://`
(`qdl-native-raw-ingestor.rs:380,425` at `855c914`). **No Rust rebuild is needed.**

## 2. Where port 8443 was used (inventory)

| Location | Kind | Action |
|---|---|---|
| `qdl/adapters/okx/client.py:54-55` | Python supervisor constants (used by scripts/tests only) | re-exported from the new owner |
| `qdl/runtime/production_catalog.py:76,818` | catalog generator | uses the owner |
| `qdl/runtime/l2_demand.py:44` | L2 plan projection (report field only) | uses the owner |
| `config/v2/stable-acquisition-bindings.yaml` | 106 public + 106 business URLs | port removed, **revision stays 19** |
| `scripts/phasea_real_provider_certification.py`, `phase114_l2_real_provider_capture.py`, `execution_view_diagnostics.py` | harness literals | port removed |
| 7 test files | fixtures and assertions | port removed |
| `upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md` | guide (including `wseea`) | port removed, note added |
| Live `/home/bobby/.local/state/qdl-v2/releases/v2.2.1-0e00c81-recovery/ingestor_okx_swap.json` | **the only live consumer**; mounted by `qdl_v2_stable_candidate-ingestor_okx_swap-1` | migrate with the script (section 4) |
| V1 `app/providers/okx/rest.py` | REST only (`https://www.okx.com`) | none |
| Rust sources | no OKX URL defaults | none |
| `CERTIFICATION_LEDGER.md:605`, the plan, `upgrade/evidence/*.json` | historical records | left as recorded |

Of the generated runtime bundle files, only `ingestor-okx-swap.json` carries an OKX URL.
The other ten (`core*.json`, `production-core-*.json`, the manifest, `authority.json`,
`ingestor-binance-usdm.json`) do not. This was checked by generating the bundle from the
committed plan.

Outside this repo (not changed here; the owning teams must act):
`/home/bobby/.worktrees/trading-system-next-upgrade/services/venues/okx/endpoint_profiles.py:31-40`
still uses `ws.okx.com:8443` and `wspap.okx.com:8443`, including **private** and **demo**.

## 3. What an identity change would cost, and why this move has none

| Identity | Derived from | Effect of this change |
|---|---|---|
| `catalog_revision` (`stable-source-bindings.yaml`) | the source catalog, which has no URLs | unchanged (11) |
| acquisition `revision` | a hand-maintained integer in the YAML | **unchanged (19)** |
| Bar edge checkpoint (`stable_bar_edge.py:468-482`, checked at `:560-567`) | slice, authority revision, catalog revision, acquisition revision, warmup rows, binding IDs | unchanged, so no checkpoint is stranded |
| Ingestor `config_revision` | `plan.revision` (`stable_deployment.py:1086`) | unchanged |
| `authority.partition_plan_digest` | sha256 of the acquisition YAML bytes (`stable_deployment.py:1247`) | changes only if authority is **re-issued**; runtime only checks its shape (`stable_deployment.py:282-321`, Rust `authority.rs:57`). Live authority is not re-issued (section 4) |
| Production-catalog `acquisition_plan_sha256` | generator provenance (`production_catalog.py:489`) | changes; report only |
| Consumer manifest / JWT revision, Query schema, contract digests | `consumers/`, proto, capabilities | unchanged (no URLs) |

Old and new YAML were compared with the three transport fields removed. They are identical:
712 bindings, revision 19 in both. File sha256 moved from `8ed0de04…5950a` to `1b586549…3656a`.

The bar edge running image `qdl-v2-python:2.2.2-d6d2637` pins acquisition 19 and catalog 11,
the same as this branch. So a later Python release built from this commit does not strand
its checkpoint.

**Design.** A WebSocket endpoint is transport configuration, not data identity.

- The single owner is `qdl/adapters/okx_endpoints.py`. It is dependency-free, so importing it
  from `stable_deployment` cannot create an import cycle.
- `StableAcquisitionBinding.validate` refuses an OKX URL on port 8443 at load time.
- `StableAcquisitionPlan.TRANSPORT_FIELDS` and `identity_digest()` define identity as the
  plan without endpoints. Tests prove that moving an endpoint keeps the digest, while channel,
  `enabled` or revision changes move it.

Making `partition_plan_digest` use `identity_digest` was considered and **deferred**. It is
written by 6 producers and compared by the rollover and cutover tooling. Changing its meaning
before the deadline would mix two definitions across live authority records. It is not needed
here because the authority is preserved.

A checkpoint migration script is **not needed**, because no revision moves.

## 4. Live rollout (one role)

Recreate: **none.** Restart: `qdl_v2_stable_candidate-ingestor_okx_swap-1` only, with
`docker restart`. Its image stays `qdl-v2-rust:2.2.1-855c914`
(`sha256:0f6876e16e51600419e1f172aae596bed9c2d369d1afcef93d1da30c18f262ec`).

Do not use compose: the container's compose working directory is
`kafka-client-recovery-20261001`. The bind mount is the single file above. A local test
confirmed that a running container keeps the old inode, and that `docker restart` rebinds
the path to the new file.

0. Check that no stage-100 run is active: `ps -eo args | grep -E '/run_stage100.sh [a-z] ' | grep -v grep` must be empty.
1. Dry run (read-only mount). Expected: `sha256_before 0c5d181c…ce47`, `sha256_after d4bd4bae…7ba7`,
   `identity_sha256 056207ce…79c3`, `config_revision 20`:
   ```
   docker run --rm --cpus 1 --network none -e PYTHONDONTWRITEBYTECODE=1 \
     -v <repo-at-commit>:/src:ro \
     -v /home/bobby/.local/state/qdl-v2/releases/v2.2.1-0e00c81-recovery:/live \
     --entrypoint python3 qdl-v2-python:2.2.3-rc6-10f9f5c -B \
     /src/scripts/migrate_okx_ws_port.py --config /live/ingestor_okx_swap.json
   ```
2. Apply: run the same command with `--apply`. This writes the backup
   `ingestor_okx_swap.json.pre-okx-port443-<ns>` next to the file, does an atomic replace that
   keeps the owner and mode, then reads the file back to verify.
3. `docker restart qdl_v2_stable_candidate-ingestor_okx_swap-1`.
4. Verify within 5 minutes:
   - the container is healthy and its log shows OKX public and business sessions connected;
   - `ss -tnp`/conntrack shows the remote port is 443, not 8443;
   - OKX TRADE, QUOTE, BOOK and MARK_INDEX projection keys advance in the V2 Redis across two
     samples about 10 s apart;
   - the OKX Swap 1m BAR arrives at the next boundary;
   - quarantine counters on `rust_core*` stay flat;
   - the TS market cache for BTC-USDT-SWAP advances.

Expected outage: one OKX reconnect, a few seconds. This is the same as the routine
reconnects already logged (plan, Astra R1-R7).

**Rollback, only on failure:** run the script with
`--config /live/ingestor_okx_swap.json --rollback-from /live/ingestor_okx_swap.json.pre-okx-port443-<ns>`,
then `docker restart` the same container. The script refuses a backup whose identity differs.
Rollback is only valid until 2026-10-31.

**Not touched:**
- Binance ingestor, all three `rust_core`, the projectors, query/stream KN, `binance_bar_edge`
  and its checkpoint, Kafka, `stable_redis`, `market_cache`
- `authority.json`, `stable.env`, identities, consumer manifests, V1 `data_layer_service`
- the Trading System

**Coupling:** `scripts/refresh_v2_native_ingestor_runtime.py` compares non-binding fields,
URLs included. Before this migration, it would refuse a refresh from this commit
("changes a non-binding runtime field"). Run the port migration first.

## 5. Evidence

- `tests/test_okx_ws_port_migration.py`: 17 tests. They cover the owner, the guard,
  `identity_digest` invariance, generated configs, and dry run / apply / no-op / rollback /
  refusal on a live-shaped config.
- 24 related modules: 325 tests passed, 1 environment skip, run network-off at `--cpus 1` in
  `qdl-v2-python:2.2.3-rc6-10f9f5c`. They include `test_phaseb_stable_edge`,
  `test_phase105_identity_acceptance`, `test_stable_runtime_refresh`,
  `test_refresh_v2_native_ingestor_runtime`, `test_catalog_regeneration`,
  `test_phasec40_authority_bootstrap`/`_cutover` and the 7 modules whose URL assertions were updated.
- Live dry run (read-only mount, no write) on the live OKX ingestor config:
  2 fields change, `config_revision` 20 is kept, identity `056207ce…79c3` is unchanged.
- Status: `tested locally`. The live rollout is not done.
