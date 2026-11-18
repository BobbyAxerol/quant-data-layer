# DL v2.4.0 rollout plan (DRAFT): writer first, stage C S6-S11 on the v2.4.0 images, then shared100

**Status: DRAFT, prepared offline, not executed.** The coordinator runs it only after the owner approves exactly this
blast radius and rollback (CLAUDE.md section 3). Any gate FAIL rolls that step back without asking again.

**What v2.4.0 is.** One DL release: the v2.3.2 content (SessionAlive frames, opt-in DELIVERY_ON_CHANGE, R0 recipe),
stage C r8 completion, and S1-S9 (journal entry "DL v2.4.0 S1-S9"). It supersedes the v2.3.2 window plan
(`../../v2.3.2/ROLLOUT_PLAN.md`, A-WR `ea4fc48`): the same steps, with the v2.4.0 images in place of the v2.3.2 R0
images. Section and gate names below refer to that plan.

**Images (candidates; digests from the v2.4.0 R0 build receipt, filled by the build step):**
- rust `qdl-v2-rust:2.4.0-3d4d84b` = `qdl-v2-rust@sha256:aef35edf6cfa63f7ea9d8de69bce0a8e4354cb1008250be885344d6a534dfe5e` (stream_kn; cores/projectors only in section 6);
- python `qdl-v2-python:2.4.0-c95476c` = `qdl-v2-python@sha256:75717b3c44364fe708769083b6b8268b913bb6ed270b20b162e1c0eaeab7f2d9` (query_kn x2, binance_bar_edge).
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
| 3a, 3b | `stream_kn_1`, then `stream_kn_2` | rust `58bdd282`, stage-b r7 realm | rust v2.4.0 `qdl-v2-rust@sha256:aef35edf6cfa63f7ea9d8de69bce0a8e4354cb1008250be885344d6a534dfe5e`, realm `v2.3.2-realm/`, RO `/session-liveness` + `QDL_KN_SESSION_LIVENESS_DIR`; `QDL_KN_SHARED_QUOTA` unset (= off) |
| 4a, 4b | `query_kn_1`, then `query_kn_2` | python `26c3b051`, r7 config | python v2.4.0 `qdl-v2-python@sha256:75717b3c44364fe708769083b6b8268b913bb6ed270b20b162e1c0eaeab7f2d9`, r8 config per the stage C runsheet; new env defaults only (`QDL_V2_SHARED_QUOTA` off, `QDL_V2_SHARED_VIEW_TTL_MS` 10, `QDL_V2_QUOTA_REPLICA_SHARE` 0.5) |
| 5a | `ingestor_binance_usdm` | rust `58bdd282`, config rev 19, 1000 ms | same image, config rev 24 (r8 bar lane 96) + 250 ms |
| 5b | `ingestor_okx_swap` | rust `58bdd282`, 1000 ms | same image, 250 ms only |
| 5c | `binance_bar_edge` | python `a235c0a1`, out6 pair | python v2.4.0 `qdl-v2-python@sha256:75717b3c44364fe708769083b6b8268b913bb6ed270b20b162e1c0eaeab7f2d9`, r8 pair |
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

**Steps 3a/3b `stream_kn_1` then `stream_kn_2`:** command as the v2.3.2 plan with `QDL_IMAGE_STREAM=qdl-v2-rust@sha256:aef35edf6cfa63f7ea9d8de69bce0a8e4354cb1008250be885344d6a534dfe5e`.
Gates: v2.3.2 plan 3a/3b gates (bundle `76672095…`, decode 0, subscribers back, `/session-liveness` RO, writer 246/246
within 120 s with no RESTART_EXHAUSTED, a Stream-written ON_CHANGE QUOTE carries DELIVERY_ON_CHANGE) plus v2.4.0:
`stream.refused` not above baseline; no `UNAUTHENTICATED` / `PERMISSION_DENIED` in the writer's stream codes (role
claims ignored, revision advisory). Rollback: `QDL_IMAGE_STREAM=qdl-v2-rust@sha256:58bdd282…` + stage-b realm.

**Steps 4a/4b `query_kn_1` then `query_kn_2`:** stage C runsheet commands with `QDL_IMAGE_QUERY=qdl-v2-python@sha256:75717b3c44364fe708769083b6b8268b913bb6ed270b20b162e1c0eaeab7f2d9`.
Gates: alpha.okx r17 loaded; tokens r1..r17 accepted, r18 refused (one read probe each, TEST consumer only); TS
246/246; MARK p99 and QUOTE p99 not above baseline + 10 %; 429 count not above baseline; the Redis quota EVAL rate
falls to 0 on the recreated replica (S7); `dedicated_redis` readiness says "request quota is local". Rollback: image
`26c3b051` + r7 config.

**Step 5a/5b/5c:** exactly the v2.3.2 plan (5c on `qdl-v2-python@sha256:75717b3c44364fe708769083b6b8268b913bb6ed270b20b162e1c0eaeab7f2d9`). Rollbacks unchanged.

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

As the v2.3.2 plan section 6, onto `qdl-v2-rust@sha256:aef35edf6cfa63f7ea9d8de69bce0a8e4354cb1008250be885344d6a534dfe5e`, after the scans PASS. Separate window.

## 6. Step 9 (A-FUND): TS FUNDING_RATE entitlement, declaration r9 (DL first, TS JWT last)

Package: `upgrade/evidence/releases/v2.4.0/declaration-ts-funding-r9/` (SHA256SUMS; gate 15/15 PASS). TS manifest
r14 -> r15 (+102 ALPHA-grade FUNDING_RATE rows, rpm 31,000 -> 31,500), release routing r33 -> r34. No image change:
the TS manifest is realm-mounted (`QDL_STABLE_CONSUMER_MANIFESTS`, `QDL_PAPER_REALM_MANIFEST`) and only offline tools
changed. Run after the Query/Stream steps above are green (or fold 9.4 into those recreates).

**9.1 Realm install.** Copy the realm dir as a new versioned dir (same owner and modes) and add
`trading-system.paper.stable-r15.yaml` (sha256 `6a191147…`, 0444). Set `QDL_PAPER_REALM_MANIFEST=
trading-system.paper.stable-r15.yaml` in the new env file. Gate: `diff -r` against the live realm dir shows only the
new file (and the bundle of 9.2); the r14 file stays in place.

**9.2 Realm and kn bundle successor (live base).** Re-render the bundles with `scripts/dl_declaration.py render` on
the r9 declaration with `base.runtime.bundles` pointing at the LIVE realm and kn bundles (the offline package has no
bundles: they need the live base). Gates: the base bundle hashes verify; the only difference is the
`trading-system.paper.stable` manifest entry (revision 15, `min_token_revision` 1, the 102 FUNDING_RATE requirement
rows); catalogue entries byte-equal (incl. `delivery_semantics`); the kn bundle changes only if its BAR rows change
(they do not: expect it unchanged).

**9.3 Routing r34.** Install `repo/config/v2/stable-v2-release-routing.yaml` r34 where the Query replicas read the
release routing. Gate: it loads; the TS consumer entry is revision 15 with manifest sha256 `47ea4a9e…`; the only
other change is the 102 FUNDING_RATE products (V2_PRIMARY / BLOCKED).

**9.4 Recreate `query_kn_1`, `query_kn_2`, then `stream_kn_1`, `stream_kn_2`** (one at a time, same images, new
realm dir / env). Gates per role: manifests loaded with TS r15; a TS token minted for r14 is still accepted (S2:
1..15); TS writer 246/246 READY, no `UNAUTHENTICATED` / `PERMISSION_DENIED`; one read-only probe (TEST consumer or
the TS identity, Grade.ALPHA FUNDING_RATE reference_batch, bounded window, limit 400) returns OK for one OKX and one
Binance perpetual; Stream: bundle hash = the 9.2 successor, subscribers back.

**9.5 TS JWT to r15 (TS window, last).** Only after 9.4 is green on every replica: set
`DATA_LAYER_V2_JWT_MANIFEST_REVISION=15` on the TS writer and A-FUND. Gate: 246/246 READY, FUNDING_RATE reads OK,
no 401 for revision.

**Rollback (reverse order, each step alone):**

| Step | Rollback |
|---|---|
| 9.5 | `DATA_LAYER_V2_JWT_MANIFEST_REVISION=14` on the TS writer (r14 tokens are valid under r15 and r14) |
| 9.4 | recreate the same roles with the previous realm dir / env (`QDL_PAPER_REALM_MANIFEST` r14, previous bundles) |
| 9.3 | routing r33 |
| 9.2 | the previous realm / kn bundle files (byte copies kept in the previous realm dir) |
| 9.1 | point back to the previous realm dir; the r15 file is inert while unreferenced |

Never move the TS JWT to r15 before every Query and Stream replica serves r15: a r15 token against a r14 manifest is
refused (S2 keeps the upper bound).
