# DL v2.3.1 certificate (DRAFT, not issued)

Draft status: `tested locally`. It is not shadow-certified and not production-ready until the rollout gates and the
full CI run pass.

The certificate is issued by the coordinator after three things:
- one full green CI run on the release PR head tree (CI6). CI5 applies: `ci.yml`, `Cargo.lock`, `poetry.lock` and
  `pyproject.toml` changed, so the run cannot be inherited;
- the rollout R2-R5 gates (`ROLLOUT_PLAN.md`);
- the latency "after" run (`LATENCY_REPORT.md` section 3).

## 1. Scope

| Item | Change | Evidence | Status |
|---|---|---|---|
| Runtime record (W-A1 bw50, stage B r7) | evidence only, already live | `../v2.3.0/runtime-record-20261116/` | APPLIED 2026-11-16 (receipts) |
| B1 `STABLE_CORE_BATCH_WAIT_MS = 50` | generator constant | `tests/test_phaseb_stable_deployment.py` (role fields == deployed cores) | tested locally |
| B4 `snapshot:batch` | additive route | `tests/test_v231_snapshot_batch.py` (11) and contract counts | tested locally |
| B5 one versioned env | `deploy/compose/qdl-v2-stable-kn.v2.3.1.env` | `tests/test_kn_stable_compose.py::test_v231_env_is_the_one_versioned_runtime_env` | tested locally |
| SDK 2.6.0 replica-balanced Query client | additive helper | `tests/test_qdl_sdk_replica_read_transport.py::ReplicatedRestQueryBuilderTests` (3); simulation (section 3) | tested locally |
| Query JWT parsed-key cache | server-internal | `tests/test_phasec2_transport_security.py::test_verifier_parses_each_key_once_and_still_fails_closed` | tested locally |
| Dependency refresh, waiver removed | locks, `deny.toml`, `accepted-advisories.json`, `.trivyignore`, policy tests | section 2 | tested locally (Rust); Python image audit at R0 |

## 2. Local preflight (CI-shaped, capped, one container at a time)

Filled from the run receipts. Logs are in the session scratchpad, so they are not durable. The results are copied here.

| Job (CI) | Local reproduction | Result |
|---|---|---|
| rust-workspace: fmt, clippy -D warnings, test | `rust:1.82-slim`, `--cpus 1 --memory 6g`, the job's apt set, `cargo fmt --check && cargo clippy --workspace --all-targets --locked -D warnings && cargo test --workspace --locked -- --test-threads=1` | **PASS**: fmt OK, clippy OK, 45 test binaries, 533 passed / 0 failed / 71 ignored |
| rust-workspace: cargo-deny | cargo-deny 0.20.2 (sha256-verified, the CI version), `rust:1.82-slim` + git | **PASS** with `ignore = []`: advisories ok, bans ok, licenses ok, sources ok |
| unit-tests (impacted modules) | 16 impacted modules in `qdl-v2-python:2.3.0-rc3-7cd4fc36`, tree mounted read-only, `--network none --cpus 1` | 216 ran: 214 OK, 12 skipped. Two errors were then fixed or explained: the new round-robin test lacked a credential provider (fixed), and the phase-0 golden is environment-only (`/app/logs` is not writable for uid 1001). Rerun as the image user 10001: 80/80 OK |
| unit-tests (4 full shards) | `scripts.ci_test_shard` 1..4 on the committed head | pending (running after commit) |
| release-gates: pip-audit, trivy | at R0, on the built v2.3.1 Python image (empty ignore lists) | pending (R0) |

## 3. Measurements behind the optimisations

- **A.1 replica affinity.** Read-only, production, 2026-11-17 02:27-02:30Z.
  - Writer `/proc/net/tcp`: 44 ESTABLISHED to kn_1 and 40 to kn_2. Server side: 41 and 38. The writer is the only
    client.
  - 400 samples: about 58-71 TIME_WAIT sockets per replica at any time.
  - CPU: kn_1 99.8 % and kn_2 27.9 % (`docker stats`).
  - In 60 s, kn_1 had 235 log lines and kn_2 had 5. They are `qdl.stall slow_request` on reference:batch, 250-485 ms.
- **Simulation** (scratchpad `sim/pool_affinity_sim.py`; `qdl-v2-python:2.3.0-rc3-7cd4fc36`, httpx 0.28.1 /
  httpcore 1.0.9, `--network none --cpus 1`).
  - Setup: 1 sequential heavy caller at 0.3 s per request and 12 light callers at 0.01 s, for 15 s.
  - Single pool on one alias: the heavy stream was 100 % on one replica in 4 of 4 seeds, and light traffic was
    skewed up to 80/20.
  - Per-replica pools with per-request round-robin: heavy 4.5-6.6 s vs 8.4-10.5 s, and light traffic 50/50.
- **A.2 JWT** (scratchpad `sim/jwt_bench.py`, same image, n = 3,000 each):
  - RS256: PEM 171.4 us vs parsed 131.7 us;
  - ES256: 213.7 us vs 174.5 us.
- **A.2 `reference:batch` per-item cost.** OFFLINE profile only, not production evidence (scratchpad
  `sim/ref_item_profile.py`).
  - Fixtures: the unit fixtures of `tests/test_execution_mark_index_live_view.py`, through `reader_for_view` (the kn3
    wiring minus the market-cache Redis read).
  - Cost: about 586 us CPU and 596 us wall per execution MARK item (n = 2,000, 1 CPU). That is about 29 ms CPU per
    50-item batch.
  - About 55 % of it is the in-process FastAPI/ASGI hop (routing, dependency solving, response serialization).
    `_result_from_response` takes about 0.31 ms and includes the JSON/base64/protobuf decode.
  - `binance_usdm_capabilities` is rebuilt twice per item, about 0.14 ms.
  - The 250-470 ms production batches therefore point to event-loop queueing on the hot replica, which A.1 removes,
    rather than to item CPU.
  - Ranked follow-ups, not implemented in v2.3.1:
    1. call the view directly instead of the in-process ASGI hop: about -0.35 ms per item, medium risk (the endpoint
       contract and its checks must be kept);
    2. cache the capability resolver per binding: about -0.14 ms per item, low risk.
  - At about 200 MARK items/s, that is about 0.07 + 0.03 vCPU in total.

## 4. Not in this release

Each of the following has its own branch and is not merged here:
- `fix/v2.3.1-bar-edge-provider-short` (DL-BL-2026-11-13-D, checkpoint v6);
- `feat/v2.3.1-okx-funding-live`;
- `integration/dl-v231-candidate` (IDR-285 projector in-process rebuild, arch-opt A2/A4/A6 prep, funding shadow
  receipt FAIL);
- `perf/v2.3.x-canonical-drop-raw-header`;
- `config/v2.3.x-a2-rf2-prep`;
- `config/v2.3.x-a6-dead-topics-prep`.

Also not in this release:
- B2 (Binance BOOK/TRADE retirement) and B3 (per-binding flush);
- the Rust and bar_edge image roll (follow-up window, owner option A).

## 5. Open risks

1. The Rust roles and bar_edge stay on the v2.3.0 images until the follow-up window. The release tree carries the
   refreshed locks, but those running images still carry the advisories that the expired waiver named.
2. The tokio 1.48 -> 1.51.6 and futures-util 0.3.31 -> 0.3.34 bumps touch every Rust role. Today they are covered by
   cargo test only. The live gates come in the follow-up window.
3. The balance gain depends on the TS writer adopting SDK 2.6.0. Until it does, kn_1 stays hot.
4. The latency quantities 1 and 2 are not measured. Credentials for this user are the blocker.
