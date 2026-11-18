# DL v2.4.0 rollout plan (DRAFT): writer first, stage C S6-S11 on the v2.4.0 images, then shared100

**Status: DRAFT, prepared offline, not executed.** The coordinator runs it only after the owner approves exactly this
blast radius and rollback (CLAUDE.md section 3). Any gate FAIL rolls that step back without asking again.

**What v2.4.0 is.** One DL release: the v2.3.2 content (SessionAlive frames, opt-in DELIVERY_ON_CHANGE, R0 recipe),
stage C r8 completion, and S1-S9 (journal entry "DL v2.4.0 S1-S9"). It supersedes the v2.3.2 window plan
(`../../v2.3.2/ROLLOUT_PLAN.md`, A-WR `ea4fc48`): the same steps, with the v2.4.0 images in place of the v2.3.2 R0
images. Section and gate names below refer to that plan.

**Images (candidates; digests from the v2.4.0 R0 build receipt, filled by the build step):**
- rust `qdl-v2-rust:2.4.0-<sha7>` = `<RUST_DIGEST>` (stream_kn; cores/projectors only in section 6);
- python `qdl-v2-python:2.4.0-<sha7>` = `<PYTHON_DIGEST>` (query_kn x2, binance_bar_edge).
- Security scans (cargo-deny ignore = [], trivy, pip-audit) are OPEN and must PASS on these two ids before the window.

**Rules (unchanged):** never `compose up` kafka*, stable_redis or market_cache; never run
`scripts/phaseb_prepare_stable_candidate.py`; one role at a time; stage C runsheet helpers (`bbbc4446`) for the stage
C gates.

## 0. W0: the TS writer first (precondition, TS window)

Exactly the v2.3.2 plan section 0: the TS v2.2.0 writer (backoff up to 30 s, never charging upstream exits; per-replica
Query pools `DATA_LAYER_V2_QUERY_URLS`; DELIVERY_ON_CHANGE from the Stream) is live before any DL recreate.
Gates W0.1-W0.5 (writer image id; `worker_retries` + `native_probe`; both replicas carry traffic; 246/246 READY,
continuity 11/11; `MARKET_DATA_NATIVE_SESSION_ALIVE_MS` unset until step 7).

v2.4.0 needs nothing new from the writer: it sends no warmups (writer `query.rs:91-92`), its Query timeout is
`query_deadline_ms` 10 000 ms and the v2.4.0 hot-lane wait ends at 7.5 s; its identity keeps its full quota rate on
each replica. No JWT manifest revision change (S2: revision is advisory).

## 1. Blast radius

| Step | Role | From (live) | To |
|---|---|---|---|
| P2 | realm dir | `v2.3.1-stage-c-realm/` (bundle `15bc88ff…`) | `v2.3.2-realm/` = byte copy + successor `76672095…` (only `delivery_semantics` on 102 QUOTE; manifests byte-equal) |
| 3a, 3b | `stream_kn_1`, then `stream_kn_2` | rust `58bdd282`, stage-b r7 realm | rust v2.4.0 `<RUST_DIGEST>`, realm `v2.3.2-realm/`, RO `/session-liveness` + `QDL_KN_SESSION_LIVENESS_DIR`; `QDL_KN_SHARED_QUOTA` unset (= off) |
| 4a, 4b | `query_kn_1`, then `query_kn_2` | python `26c3b051`, r7 config | python v2.4.0 `<PYTHON_DIGEST>`, r8 config per the stage C runsheet; new env defaults only (`QDL_V2_SHARED_QUOTA` off, `QDL_V2_SHARED_VIEW_TTL_MS` 10, `QDL_V2_QUOTA_REPLICA_SHARE` 0.5) |
| 5a | `ingestor_binance_usdm` | rust `58bdd282`, config rev 19, 1000 ms | same image, config rev 24 (r8 bar lane 96) + 250 ms |
| 5b | `ingestor_okx_swap` | rust `58bdd282`, 1000 ms | same image, 250 ms only |
| 5c | `binance_bar_edge` | python `a235c0a1`, out6 pair | python v2.4.0 `<PYTHON_DIGEST>`, r8 pair |
| 7 | TS writer env (TS window W3) | `MARKET_DATA_NATIVE_SESSION_ALIVE_MS` unset | 500 (SessionAlive on) |
| 8 | shared100 acceptance run | - | section 4 |

**Not touched:** Kafka x3, stable_redis, market_cache; rust_core x3 and market_projector x2 (already r8; image only in
section 6); the kn bundle `5c272b5c`; any declaration beyond r8; the TS manifest r14 / binding r26 and the JWT manifest
revision; the consumer manifests and the release routing (S2 changes code, not files); certificates, cursor key,
secrets; Paper, PM and portal roles.

## 2. Window steps (v2.3.2 plan section 4 order, gates added for v2.4.0)

**Step 1 baseline (read only, 10 min):** writer heartbeat (246/246, `worker_retries`, probes/min), DL CPU per role,
session file cadence, stage C T-30 checks. Also record per Query replica: requests/min, 429 count, Redis quota EVAL
rate (`INFO commandstats` read only on stable_redis) as the S7 "before".

**Steps 3a/3b `stream_kn_1` then `stream_kn_2`:** command as the v2.3.2 plan with `QDL_IMAGE_STREAM=<RUST_DIGEST>`.
Gates: v2.3.2 plan 3a/3b gates (bundle `76672095…`, decode 0, subscribers back, `/session-liveness` RO, writer 246/246
within 120 s with no RESTART_EXHAUSTED, a Stream-written ON_CHANGE QUOTE carries DELIVERY_ON_CHANGE) plus v2.4.0:
`stream.refused` not above baseline; no `UNAUTHENTICATED` / `PERMISSION_DENIED` in the writer's stream codes (role
claims ignored, revision advisory). Rollback: `QDL_IMAGE_STREAM=qdl-v2-rust@sha256:58bdd282…` + stage-b realm.

**Steps 4a/4b `query_kn_1` then `query_kn_2`:** stage C runsheet commands with `QDL_IMAGE_QUERY=<PYTHON_DIGEST>`.
Gates: alpha.okx r17 loaded; tokens r1..r17 accepted, r18 refused (one read probe each, TEST consumer only); TS
246/246; MARK p99 and QUOTE p99 not above baseline + 10 %; 429 count not above baseline; the Redis quota EVAL rate
falls to 0 on the recreated replica (S7); `dedicated_redis` readiness says "request quota is local". Rollback: image
`26c3b051` + r7 config.

**Step 5a/5b/5c:** exactly the v2.3.2 plan (5c on `<PYTHON_DIGEST>`). Rollbacks unchanged.

**Step 7 (TS window W3) SessionAlive on:** v2.3.2 plan step 7 gates (30 min, 96 crossec OKX QUOTE keys: proof age >
2 s in 0 samples while LIVE; QUOTE probes/min drop; no `EXECUTION_CONTEXT_STALE` on QUOTE).

## 3. Rollback summary

| Step | Rollback |
|---|---|
| 3 | rust `58bdd282` + stage-b realm |
| 4 | python `26c3b051` + r7 config |
| 5a / 5b | previous config dirs (1000 ms) |
| 5c | `a235c0a1` + out6 pair |
| 7 | unset `MARKET_DATA_NATIVE_SESSION_ALIVE_MS` |

## 4. Step 8: the shared100 acceptance run (after steps 1-7 are green)

Profile `config/v2/p184-shared100-profile.json` (sha256 `720e6d98…`), pre-registered A6
`config/v2/p184-stage100-a6-shared100-preregistered.json`, scorer
`qdl.certification.phase3_consumer_load.evaluate_shared100_gates`, candidate `scripts/p184_capacity100.py
--profile shared100`. Bind A6 to the run's runtime identity (query_kn/stream_kn digests, manifest/realm revisions)
before load. Measurement class: take the host measurement lock (AGENTS 34a/34b). Gates: restart storm (100 sessions
within 10 s, every warmup served, 0 refusals, TS lane p95 <= 6265.1 ms); noisy neighbour (one session >= 50 rps
throttled alone); G14 baseline-subtracted (idle, writer only, writer + 100 alphas; alpha delta <= 0.5 vCPU,
OWNER-APPROVED 2026-11-18). A FAIL is recorded, never rescored.

## 5. Section 6 follow-up: cores and projectors

As the v2.3.2 plan section 6, onto `<RUST_DIGEST>`, after the scans PASS. Separate window.
