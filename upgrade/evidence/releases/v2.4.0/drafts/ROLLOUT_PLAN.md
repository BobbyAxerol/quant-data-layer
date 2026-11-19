# DL v2.4.0 rollout plan (DRAFT): writer first, stage C S6-S11 on the v2.4.0 images, then shared100

**Status: DRAFT, prepared offline, not executed.** The coordinator runs it only after the owner approves exactly this
blast radius and rollback (CLAUDE.md section 3). Any gate FAIL rolls that step back without asking again.

**What v2.4.0 is.** One DL release: the v2.3.2 content (SessionAlive frames, opt-in DELIVERY_ON_CHANGE, R0 recipe),
stage C r8 completion, and S1-S9 (journal entry "DL v2.4.0 S1-S9"). It supersedes the v2.3.2 window plan
(`../../v2.3.2/ROLLOUT_PLAN.md`, A-WR `ea4fc48`): the same steps, with the v2.4.0 images in place of the v2.3.2 R0
images. Section and gate names below refer to that plan.

**Images (candidates; digests from the v2.4.0 R0 build receipt, filled by the build step):**
- rust `qdl-v2-rust:2.4.0-c17472c` = `qdl-v2-rust@sha256:abafab2c98cbdd9d2fe86f2b26fd6fcf64800ccdd114a3dd5b92803de16a3e98` (stream_kn; cores/projectors only in section 6);
- python `qdl-v2-python:2.4.0-acf42d7` = `qdl-v2-python@sha256:18ada571c72856cefc2844c8cb2c3809c3e30ac77927bc4c73160e7b2787cd18` (query_kn x2, binance_bar_edge).
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
| 3a, 3b | `stream_kn_1`, then `stream_kn_2` | rust `58bdd282`, stage-b r7 realm | rust v2.4.0 `qdl-v2-rust@sha256:abafab2c98cbdd9d2fe86f2b26fd6fcf64800ccdd114a3dd5b92803de16a3e98`, realm `v2.3.2-realm/`, RO `/session-liveness` + `QDL_KN_SESSION_LIVENESS_DIR`; `QDL_KN_SHARED_QUOTA` unset (= off) |
| 4a, 4b | `query_kn_1`, then `query_kn_2` | python `26c3b051`, r7 config | python v2.4.0 `qdl-v2-python@sha256:18ada571c72856cefc2844c8cb2c3809c3e30ac77927bc4c73160e7b2787cd18`, r8 config per the stage C runsheet; new env defaults only (`QDL_V2_SHARED_QUOTA` off, `QDL_V2_SHARED_VIEW_TTL_MS` 10, `QDL_V2_QUOTA_REPLICA_SHARE` 0.5) |
| 5a | `ingestor_binance_usdm` | rust `58bdd282`, config rev 19, 1000 ms | same image, config rev 24 (r8 bar lane 96) + 250 ms |
| 5b | `ingestor_okx_swap` | rust `58bdd282`, 1000 ms | same image, 250 ms only |
| 5c | `binance_bar_edge` | python `a235c0a1`, out6 pair | python v2.4.0 `qdl-v2-python@sha256:18ada571c72856cefc2844c8cb2c3809c3e30ac77927bc4c73160e7b2787cd18`, r8 pair |
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

**Steps 3a/3b `stream_kn_1` then `stream_kn_2`:** command as the v2.3.2 plan with `QDL_IMAGE_STREAM=qdl-v2-rust@sha256:abafab2c98cbdd9d2fe86f2b26fd6fcf64800ccdd114a3dd5b92803de16a3e98`.
Gates: v2.3.2 plan 3a/3b gates (bundle `76672095…`, decode 0, subscribers back, `/session-liveness` RO, writer 246/246
within 120 s with no RESTART_EXHAUSTED, a Stream-written ON_CHANGE QUOTE carries DELIVERY_ON_CHANGE) plus v2.4.0:
`stream.refused` not above baseline; no `UNAUTHENTICATED` / `PERMISSION_DENIED` in the writer's stream codes (role
claims ignored, revision advisory). Rollback: `QDL_IMAGE_STREAM=qdl-v2-rust@sha256:58bdd282…` + stage-b realm.

**Steps 4a/4b `query_kn_1` then `query_kn_2`:** stage C runsheet commands with `QDL_IMAGE_QUERY=qdl-v2-python@sha256:18ada571c72856cefc2844c8cb2c3809c3e30ac77927bc4c73160e7b2787cd18`.
Gates: alpha.okx r17 loaded; tokens r1..r17 accepted, r18 refused (one read probe each, TEST consumer only); TS
246/246; MARK p99 and QUOTE p99 not above baseline + 10 %; 429 count not above baseline; the Redis quota EVAL rate
falls to 0 on the recreated replica (S7); `dedicated_redis` readiness says "request quota is local". Rollback: image
`26c3b051` + r7 config.

**Step 5a/5b/5c:** exactly the v2.3.2 plan (5c on `qdl-v2-python@sha256:18ada571c72856cefc2844c8cb2c3809c3e30ac77927bc4c73160e7b2787cd18`). Rollbacks unchanged.

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

As the v2.3.2 plan section 6, onto `qdl-v2-rust@sha256:abafab2c98cbdd9d2fe86f2b26fd6fcf64800ccdd114a3dd5b92803de16a3e98`, after the scans PASS. Separate window.

## 6. Step 9 (A-FUND): TS FUNDING_RATE entitlement, declaration r9 (DL first, TS JWT last)

Package: `upgrade/evidence/releases/v2.4.0/declaration-ts-funding-r9/` (SHA256SUMS; gate 15/15 PASS). TS manifest
r14 -> r15 (+102 ALPHA-grade FUNDING_RATE rows, +97 ALPHA-grade historical 1m MARK rows on the OKX SWAP perpetuals,
rpm 31,000 -> 31,500, 446 requirements), release routing r33 -> r34 (manifest sha256 `725c8ca2…`), realm file sha256
`e9f4536a…`. The historical MARK read (MARK_INDEX_PRICE with a bounded window, A-FUND's mark at a past funding
time) is Query code: it needs the python image `qdl-v2-python:2.4.0-c17472c` (above), so 9.4 is the Query/Stream
recreate of steps 3-4 on the c17472c images (the Rust binaries are byte-identical to 3d4d84b; only labels differ).

**9.1 Realm install.** Copy the realm dir as a new versioned dir (same owner and modes) and add
`trading-system.paper.stable-r15.yaml` (sha256 `e9f4536a…`, 0444). Set `QDL_PAPER_REALM_MANIFEST=
trading-system.paper.stable-r15.yaml` in the new env file. Gate: `diff -r` against the live realm dir shows only the
new file (and the bundle of 9.2); the r14 file stays in place.

**9.2 Realm and kn bundle successor (live base).** Re-render the bundles with `scripts/dl_declaration.py render` on
the r9 declaration with `base.runtime.bundles` pointing at the LIVE realm and kn bundles (the offline package has no
bundles: they need the live base). Gates: the base bundle hashes verify; the only difference is the
`trading-system.paper.stable` manifest entry (revision 15, `min_token_revision` 1, the 102 FUNDING_RATE requirement
rows and the 97 historical 1m MARK rows); catalogue entries byte-equal (incl. `delivery_semantics`); the kn bundle changes only if its BAR rows change
(they do not: expect it unchanged).

**9.3 Routing r34.** Install `repo/config/v2/stable-v2-release-routing.yaml` r34 where the Query replicas read the
release routing. Gate: it loads; the TS consumer entry is revision 15 with manifest sha256 `725c8ca2…`; the only
other change is the 102 FUNDING_RATE products (V2_PRIMARY / BLOCKED).

**9.4 Recreate `query_kn_1`, `query_kn_2`, then `stream_kn_1`, `stream_kn_2`** (one at a time, same images, new
realm dir / env). Gates per role: manifests loaded with TS r15; a TS token minted for r14 is still accepted (S2:
1..15); TS writer 246/246 READY, no `UNAUTHENTICATED` / `PERMISSION_DENIED`; one read-only probe (TEST consumer or
the TS identity, Grade.ALPHA FUNDING_RATE reference_batch, bounded window, limit 400) returns OK for one OKX and one
Binance perpetual; one historical MARK probe (Grade.ALPHA MARK_INDEX_PRICE, interval 1m, mark_index_kind MARK,
window [floor_1m(T), +60 s), limit 1, a T one day back) returns one closed candle (`confirm` 1) for one OKX SWAP; Stream: bundle hash = the 9.2 successor, subscribers back.

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

## 7. Prep record P1-P3 (2026-11-19, offline; nothing running reads these dirs)

Coordinator GO for P1-P3 only. Created three new host dirs; no live mount, env or container changed (stream_kn_1
StartedAt 2026-11-18T05:40:37Z, query_kn_1 2026-11-17T07:45:12Z, images 58bdd282 / 26c3b051, unchanged).
`R=/home/bobby/.local/state/qdl-v2/releases`.

| Step | Path | sha256 (file) | Owner / mode |
|---|---|---|---|
| P1 | `$R/v2.4.0-compose/qdl-v2-stable-kn.v2.4.0.yml` | `262a15148bc6b010acbfa8264ed3e826157a16dfd825d98637d78655ee02d463` | bobby 0444 (dir 0750) |
| P1 | `$R/v2.4.0-compose/v2.4.0.env` | `310da854746617c26ea1a416dc18079833f342edb204d63558d2730f35113d83` | bobby 0444 |
| P2 | `$R/v2.4.0-realm/bundle.json` (body sha256 field `577470a63180bd8b7ac45e4beff7399bd8cf604fa3f3efdb8a855ea4f5191192`) | `f3f2994b8dec6adaa85e194eadc0bee132b871363758fed9ab8a2fe545f964be` | root:root 0444 (dir 0755) |
| P2 | `$R/v2.4.0-realm/trading-system.paper.stable-r15.yaml` | `e9f4536a085d23a501d6d4d472b36f9974e24ad8c6e6f260ec09d1b635e657e4` | root:root 0444 |
| P3 | `$R/v2.4.0-ingestor/ingestor_binance_usdm.json` | `23610605f09b2932b6040f0e6f96b02f327b8342e7108d7ee0092a3673784277` | root:10001 0640 (dir 0750) |
| P3 | `$R/v2.4.0-ingestor/ingestor_okx_swap.json` | `1ca07ba6d40b4eae69422699a9a8d332ec5f135772122e53583f2c3c6de9e258` | root:10001 0640 |

- **P1.** Compose = `deploy/compose/qdl-v2-stable-kn.yml` at dev `03d0aca` with the stage C Query edit
  (`alpha.okx.paper.stable-r16.yaml` -> `-r17.yaml`, 2 lines). `diff` against the live stage C compose = exactly
  the stream `QDL_KN_SESSION_LIVENESS_DIR` + RO `stable_state:runtime/session-liveness` lines (2 services). Env
  overlay (after the stage C list): `QDL_IMAGE_STREAM` abafab2c, `QDL_IMAGE_QUERY` and `QDL_IMAGE_BAR_EDGE` 9cffe89b,
  `QDL_CONSUMER_REALM_DIR` v2.4.0-realm, `QDL_PAPER_REALM_MANIFEST` r15, `QDL_INGESTOR_CONFIG_DIR` v2.4.0-ingestor.
  Render gate (`compose config`, stage C list vs stage C list + overlay, with the same TLS/generation/secret env
  files, rendered to private scratch files then shredded): top level equal; services that differ are exactly
  stream_kn x2 (image, realm, session-liveness env + mount), query_kn x2 (image, realm, manifest list r14 -> r15),
  ingestor x2 (config source), binance_bar_edge (image). Kafka, stable_redis, market_cache, cores, projectors equal.
- **P2.** `v232_realm_bundle_successor.py` on the stage C realm bundle (`15bc88ff…`) with the pinned r8 catalogue
  (`e5a670de…`): PASS, `76672095…`, 1578 bindings, 102 QUOTE marked ON_CHANGE (OKX SWAP 97, BINANCE USDM 5),
  manifests unchanged. Then the TS entry compiled from the r15 file with the release compiler replaces the r14 entry
  (scratch tool mirroring `build_bundles`): compiled catalogue byte-equal to the base, other 11 entries equal, TS
  r14 -> r15, `min_token_revision` 13 -> 1, 247 -> 446 requirements (0 removed; +102 FUNDING_RATE ALPHA, +97
  MARK_INDEX_PRICE 1m ALPHA), rpm 31,000 -> 31,500, manifest sha256 `725c8ca2…`; bundle sha256 `577470a6…`. Both run
  in `qdl-v2-python@sha256:9cffe89b…` (`--network none`, read-only inputs, host slot). No BAR rows added, so the kn
  bundle stays `5c272b5c`. Dir = `sudo rsync -a` of `v2.3.1-stage-c-realm`; `diff -rq` = `bundle.json` + the new r15
  file only.
- **P3.** Inputs: the pinned r8 Binance config (`19688bc5…`, rev 24, 118 bindings; = stage C ingestor dir) and the
  live OKX config (`14b28e29…`, rev 23, 465 bindings). `refresh_v2_native_ingestor_runtime.py` (dry run, r8
  catalogue) refuses both files for a pre-existing reason unrelated to the cadence: it does not emit the r8
  `lane_shard` pin (Binance 22 bindings, OKX 306) nor `book_resync` / `latest_state_flush_ms_by_feed`, so it reads
  them as contract changes. It does emit `session_liveness_write_interval_ms` 250. The configs were therefore made by
  setting only that field (1000 -> 250) and re-serialising in the tool's canonical format (input files are already
  canonical): `diff` = exactly that one line in each file. The live ingestor image (`58bdd282`, source 832c02c4)
  accepts 250..5000 (`qdl-native-raw-ingestor.rs:595`). Follow-up: the refresh tool should learn `lane_shard`.

## 8. Window 1 RCA (2026-11-19) and the corrected order and gates (supersedes the stream-first order of section 2)

**Incident.** Window 1 recreated `stream_kn_1` on rust `abafab2c` with the v2.4.0 realm at 05:08:02Z. Stream gates
passed (bundle `577470a6…`, decode 0, refusals 0), but the TS writer dropped to 209-216/246 ready for 6 min; the
coordinator rolled kn_1 back at 05:14:08Z and the writer was 246/246 at 05:14:47Z.

**Root cause.** A catalog-revision mismatch between Query and one Stream replica on the private hot-read path:
- Query (r7 runtime) sends `catalog_revision` 15 in every signed hot read (`qdl/runtime/kn_hot_view.py`
  `hot_client_from_environment`); the new kn_1 bundle is catalog 16 (stage C r8). The gateway requires an exact match
  and refuses with `FAILED_PRECONDITION("HOT_AUTHORITY_MISMATCH")` (`rust/qdl-stream-gateway/src/hot_view.rs:115-119`).
- Query walks `QDL_KN_HOT_READ_TARGETS=qdl-v2-stream-a,qdl-v2-stream-b` in order and, before this fix, raised
  `HOT_AUTHORITY_OR_PROTOCOL_REFUSED` on any non-transient code without trying the next replica; for a quiet
  EXECUTION QUOTE that becomes `DATA_NOT_READY: HOT_QUIET_PRIMARY_UNVERIFIED` (`qdl/runtime/kn_query_backend.py:508`).
- Evidence: Query `qdl_refusal` with `HOT_AUTHORITY_OR_PROTOCOL_REFUSED` = 0 in 04:50-05:08Z, **185 on each replica**
  in 05:08-05:14:10Z, 0 after; the writer's native probe routes carried that detail; kn_2 absorbed all 144
  subscriptions with 0 refused/lagging/overflow; writer refusals 0.
- **Re-attribution: the stage C S6 incident of 2026-11-18 05:36Z** shows the same refusal (Query logs 05:30-05:50Z:
  30 on query_kn_1, 35 on query_kn_2). It was attributed to the writer's missing backoff; the trigger was this
  mismatch (the old writer then restart-stormed). The stage C order "streams before Query" (runsheet D1) has the
  defect; any catalog-revision change broke whichever side moved first.
- The subscriber gate was invalid: the SDK keeps a failed-over subscription on the other replica (writer
  `qdl_sdk/transport.py` 2.4.1:612-619), so a recreated replica shows 0 subscribers even when healthy.

**Fix (in v2.4.0).** `kn_hot_view.py`: on `FAILED_PRECONDITION` with `HOT_AUTHORITY_MISMATCH` or
`HOT_AUTHORITY_CHANGED` the read tries the next replica; each replica still enforces its exact match (gateway
unchanged); only when every replica refuses is the result `HOT_AUTHORITY_OR_PROTOCOL_REFUSED`; a transient refusal
on another replica keeps its retryable reason. Record-integrity refusals (`HOT_RECORD_GAPPED`, ...), auth
(`UNAUTHENTICATED`/`PERMISSION_DENIED`), `HOT_REQUEST_EXPIRED`, data loss and invalid argument never fall through.
Observability: `CanonicalHotClient.authority_fallbacks` per (target, detail) and one bounded stderr line per key and
10 s window (`{"event":"qdl_hot_read_authority_fallback",...}`). Python image `qdl-v2-python:2.4.0-acf42d7`
`sha256:18ada571c72856cefc2844c8cb2c3809c3e30ac77927bc4c73160e7b2787cd18` supersedes `9cffe89b`; rust stays
`abafab2c`.

**Corrected order (also safe for the live Query `26c3b051`, which has no fallback):**

| Window | Role | Why this order |
|---|---|---|
| 1' | `stream_kn_2` (rust `abafab2c`, realm `v2.4.0-realm`) | the old Queries ask stream-a (old, catalog 15) first: match |
| 2 | `query_kn_2`, then `query_kn_1` (python `18ada571`, r8 runtime catalog 16, `QDL_KN_HOT_READ_TARGETS=qdl-v2-stream-b:8210,qdl-v2-stream-a:8210`) | the new Query asks the new kn_2 first; the fallback covers a restart of kn_2 |
| 1'' | `stream_kn_1` | both Queries already on catalog 16 and ask kn_2 first |

Compose and env: `$R/v2.4.0-r2-compose/qdl-v2-stable-kn.v2.4.0-r2.yml` (= the P1 compose with the two Query
`QDL_KN_HOT_READ_TARGETS` lines reversed) and `v2.4.0-r2.env` (= `v2.4.0.env` with `QDL_IMAGE_QUERY` and
`QDL_IMAGE_BAR_EDGE` = `18ada571`). Render hashes: stream_kn_1 `1edb56ac…`, stream_kn_2 `2df541c5…` (unchanged from P1),
query_kn_1 `90683238…`, query_kn_2 `6cda1d40…`; the rollback lists render the live labels exactly (stream
`2003b67d…` / `ef4ea8ab…`, query `d4858008…` / `21da15ba…`).

**Gates per recreate** (`drafts/window/v240_window_gates.sh ROLE T0`, read only; any FAIL rolls that role back):
- **G-stream / G-query:** image, RestartCount 0, healthy; realm `v2.4.0-realm` RO. Stream: `/session-liveness` RO,
  bundle `577470a6…`, decode/reader errors 0, refused counters 0. Query: manifests list alpha.okx r17 and TS r15,
  hot targets b,a, `/runtime` = the stage C runtime.
- **G-hot (decides):** `drafts/window/v240_hot_read_probe.py` inside each Query container reads 3 OKX SWAP QUOTE
  bindings through that Query's own configured client: all OK on both Queries. Per-target refusals are printed
  (expected while revisions are mixed).
- **G-query-refusals:** 0 `qdl_refusal` with `HOT_AUTHORITY_OR_PROTOCOL_REFUSED` on both Queries since T0.
- **G-writer:** within 120 s 246/246, or not-ready only `EXECUTION_CONTEXT_STALE`/`TRADE_EVENT_STALE`;
  `worker_retries` `{}`; 0 native probe routes with a `HOT_AUTHORITY` unavailable since T0; writer refusals 0; writer
  image `d1addc04` RestartCount 0.
- Stream subscriber counts are informational only.

**Follow-up (not a gate):** a direct test subscription per Stream replica needs a TEST-consumer JWT tool for prod
(none exists); the FUNDING_RATE and historical MARK read probes of step 9.4 still need that TEST identity.
