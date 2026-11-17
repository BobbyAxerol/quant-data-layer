# Quant Data Layer v2.3.1

Status: **release candidate, prepared offline; not certified, not tagged, not pushed.**
- Release branch: `release/v2.3.1-query-b4-stage-b` (worktree `/home/bobby/.worktrees/dl-v231-release-20261117`),
  from `origin/dev` 7d4333e (= v2.3.0 main tree).
- Certificate: `CERTIFICATE_DRAFT.md`.
- Rollout: `ROLLOUT_PLAN.md` (Query x2, plus the coupled TS writer rollout).
- Latency: `LATENCY_REPORT.md` ("before" measured; "after" follows the rollout).

v2.3.1 does three things:
- records the runtime that already runs (W-A1 and stage B r7);
- adds the B4 batched snapshot read and the Query-side optimisations found while explaining the replica imbalance;
- removes the v2.3.0 advisory waiver by refreshing the dependencies.

## Contents (since v2.3.0)

1. **Runtime record, already live (merge of `feat/dl-stage-b-execution-71` b3a3670b).**
   - W-A1: realtime-core `batch_wait_ms` 25 -> 50, permanent since 2026-11-16 11:16Z.
   - Stage B declaration r7, APPLIED 12:24-12:38Z:
     - catalogue r15, acquisition r23, demand r11, scope r14, routing r32;
     - TS manifest r14, sealed binding r26.
   - Receipts are in `upgrade/evidence/releases/v2.3.0/runtime-record-20261116/`.
   - The owner kept the stage-B CPU condition. Current status: W-MRT on the TS side, DL 6.41 vCPU on 2026-11-16 16:40Z.
2. **B1 core role fields as declared values.**
   - `qdl/runtime/stable_deployment.py` declares `STABLE_CORE_BATCH_WAIT_MS = 50` (it was a hard-coded 25) and
     `STABLE_CORE_BOOK_RESYNC_QUARANTINE = True`.
   - The generator omitted `book_resync_quarantine`. Every deployed core carries `true`, and the Rust core defaults it
     to `false` when it is absent. This was a second, unrecorded tree-vs-runtime difference, found by the new test.
   - Every generated core config now carries the production values.
   - New test: every non-binding role field the generator writes equals the deployed stage-B cores
     (`tests/test_phaseb_stable_deployment.py::test_generated_core_role_fields_equal_the_deployed_stage_b_cores`).
   - Correction to the runtime record section 5 B1 and to hot-adjustment guide gap 2: the core refresher does NOT
     refuse a bw50 base.
     - Verified offline: `successor()` on `declaration-stage-b-r7/cores-bw50/*` with the r7 repo configs returns
       OK, 0 additions, `batch_wait_ms` 50 kept, byte-equal output. The same holds for the bw25 `cores/`.
     - Reason: `scripts/refresh_v2_core_runtime_bindings.py` compares only the `core` sub-document, and
       `batch_wait_ms` is a top-level role field that the refresher carries over from the active file.
     - The hard-coded value mattered only for full generator output (the bundle and shadow paths).
3. **B4 `POST /v2/market-data/snapshot:batch`** (`qdl.marketdata.snapshot-batch.v1`; merge of
   `feat/dl-v231-b4-batched-quote-probe` faad7c3d).
   - Additive: one call (one quota unit) for up to the manifest's `max_batch_items` latest snapshots.
   - Every item is authorized before any read, and each item runs the unchanged single-snapshot policy.
   - OpenAPI changes by +1 path and +3 schemas.
   - Coupling: the TS writer branch `feat/v213-writer-batched-quote-probe` 3438bc90 consumes it in a separate TS
     rollout. A Query without the route answers 404, and the writer falls back to per-route GET.
4. **SDK 2.6.0 `build_replicated_rest_query`** (A.1, Query replica imbalance).
   - Production on 2026-11-17:
     - `query_kn_1` ran at 0.94 vCPU with about 213 `qdl.stall slow_request`/min on `reference:batch`;
     - `query_kn_2` ran at 0.26 vCPU with none;
     - TCP connections were balanced: the writer had 44 to kn_1 and 40 to kn_2.
   - Root cause:
     - the writer uses one httpx pool on the DNS alias `qdl-v2-query`, which names both replicas;
     - httpcore 1.0.9 always assigns a request to the oldest free connection;
     - so the sequential coalesced `reference_batch` stream stays on whichever replica its first connection reached.
   - The same pool also closes idle connections above keep-alive 20. At any time there are about 130 TIME_WAIT
     sockets.
   - Simulation with the same library versions:
     - single pool: the heavy stream landed 100 % on one replica in 4 of 4 seeds;
     - per-replica pools with per-request round-robin: balanced.
   - The helper builds the existing `ReplicatedRestQueryTransport` over one pool per replica URL, with keep-alive
     equal to max connections. `RestQueryTransport` gains `max_connections` / `max_keepalive_connections`.
   - The defaults are unchanged.
   - The writer change is TS-owned.
5. **Query JWT key parse cache** (A.2).
   - `qdl/security/policy.py` parses each verification key once per (kid, algorithm), not on every request.
   - Measured offline: RS256 decode 171 -> 132 us, ES256 214 -> 175 us. About 0.004 vCPU at the current about
     100 requests/s.
   - A key that does not fit the algorithm still fails closed, and nothing is cached for it.
6. **Dependency refresh; the v2.3.0 advisory waiver is removed (it expired at v2.3.1).**
   - Rust: `Cargo.toml` tokio `=1.51.6` (LTS line, fixes RUSTSEC-2026-0386) and futures-util `=0.3.34` (drops
     pin-utils, RUSTSEC-2026-0441).
   - `Cargo.lock`: libz-sys 1.1.33 (0418), rustls-webpki 0.103.17 (0382), webpki-roots 1.0.12 (0381), zstd-sys
     2.1.3 (0410). zstd-sys and libz-sys no longer pull bindgen.
   - `deny.toml` `ignore = []`. cargo-deny 0.20.2: advisories ok, bans ok, licenses ok, sources ok.
   - Python: `poetry.lock` certifi 2026.10.23 and msgpack 1.2.4; pyproject floor `msgpack >=1.2.3`.
   - pillow and soupsieve are removed from the runtime dependencies. No module in the repository imports them (grep
     of `import PIL`, `from PIL`, `soupsieve`, `bs4`), nothing depends on them in `poetry.lock`, and pillow 13 (the
     fixed line) needs Python >= 3.11 while the SDK supports 3.10.
   - setuptools comes from the Dockerfile step (`>=78.1.1`, latest at build).
   - `.github/accepted-advisories.json` now has empty `rust`, `python` and `trivy` lists. `.trivyignore` is back to the
     two BuildKit SBOM false positives.
   - The phase-6 and phase-83 policy tests require empty waivers again.
7. **One versioned env per release** (B5).
   - `deploy/compose/qdl-v2-stable-kn.v2.3.1.env` is the deployed rc3 env with the stage-B overlay applied (last wins).
   - A rollback no longer depends on env-file order.
   - The image digests stay the v2.3.0 ones until the rollout records the new Query digest.

8. **Stage C declaration r8, Binance signal data for crossec_ema** (owner, 2026-11-17). Rendered and gated offline;
   it runs in its own window after Query x2 (`ROLLOUT_PLAN.md` section 7).
   - Coverage: the 96 crossec instruments crossec_ema trades on OKX get Binance USDM BAR 1m on the native kline lane
     (91 new), DERIVED_FINAL 1h and 12h (182), and FUNDING_RATE on the reference path. All of it is granted to
     `alpha.okx.paper.stable` r17 (accept-set floor 15).
   - Revisions: catalogue r16 (adds FETUSDT and LUNA2USDT from the verbatim exchangeInfo), acquisition r24, demand
     r12, routing r33.
   - Gate 17/17. The bar-edge crossing from the live out6 pair is additive: 1,047 retained, 273 added, 0 dropped.
   - Tool changes in `scripts/dl_declaration.py`:
     - Binance USDM signal universes;
     - a cross-venue demand template;
     - Binance-anchored derived parents;
     - verbatim Binance capture rows appended for listed symbols only;
     - a complete runtime universe mount.
     - Tests: `tests/test_v231_stage_c_binance_signal.py`.
   - Evidence: `declaration-stage-c-r8/`, including the 96-symbol OKX -> Binance map (4 symbols at ×1000, 1 rename,
     0 missing) and the measured kline rate.

## Release tree = runtime

- Compose: `deploy/compose/qdl-v2-stable-kn.yml` is byte-equal to the deployed compose (sha256 `5b1381e062d97a7a...`).
- Env: the deployed env list is the rc3 env (sha256 `949cf3f3...`) plus `stage-b.env` (sha256 `c2b3c2ca...`).
  `deploy/compose/qdl-v2-stable-kn.v2.3.1.env` renders the same values.
- Declaration: stage B r7 is carried as evidence in `upgrade/evidence/releases/v2.3.0/declaration-stage-b-r7/` with
  `SHA256SUMS`. As in v2.3.0, `config/v2` and `consumers/stable` stay at the repo-default baseline the test suite
  loads.
- Deployed cores: they are byte-equal to `declaration-stage-b-r7/cores-bw50/`, and the generator's role fields now
  equal them (B1).
- **Known tree-vs-runtime gap, owner option A (2026-11-17).**
  - After the v2.3.1 rollout, `query_kn_1` and `query_kn_2` run the v2.3.1 Python image.
  - The Rust roles keep rust `58bdd282` (the v2.3.0 digest, built from the old Cargo.lock): rust_core x3,
    market_projector x2, stream_kn x2 and both ingestors.
  - `binance_bar_edge` keeps python `a235c0a1`.
  - Those images carry the advisories the v2.3.0 waiver named. Their exposure is as assessed in v2.3.0: build-time
    only, unmaintained notice, private-CA mesh, no AsyncFd from foreign fds, x86_64 only.
  - A follow-up window rolls them to images built from this tree (`ROLLOUT_PLAN.md` section 6).

## Rollout

`ROLLOUT_PLAN.md`:
- R0 build of the Python image;
- R1 baseline;
- R2/R3 Query one at a time;
- R4 the TS writer (its own packet);
- R5 30-minute soak and CPU gate.

Not touched: Kafka, stable_redis, market_cache, the cores, projectors, streams, ingestors, bar_edge, the declarations,
the manifests, the JWT revision, the certificates and the secrets.

## Limits

- Single host, not HA.
- The latency "before" quantities 1 and 2 are NOT MEASURED. The current-mesh identities are root-only for this user.
  Credentials for alpha.okx, alpha.vn, monitoring and reference-l2 were not located. Quantity 4 is an upper bound,
  because the cache stores no write timestamp (`LATENCY_REPORT.md`).
- The `reference:batch` per-item cost is profiled offline only (`CERTIFICATE_DRAFT.md`), not on a live replica.
- The SDK helper is `tested locally`. The balance gain is a simulation plus the code path, not yet a production
  measurement (`ROLLOUT_PLAN.md` R4 gates).
- Carried from v2.3.0: DL-BL-2026-11-13-D provider-short re-bootstrap (branch `fix/v2.3.1-bar-edge-provider-short`).
  It is **not in this release**, together with the other v2.3.1-named branches listed in `CERTIFICATE_DRAFT.md`
  section 4.
