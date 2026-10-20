# Quant Data Layer v2.2.5 - scoped release certificate

Status: **PASS under P1 (POST_HOC_PACKAGING_DECISION, see section 4), which needs the owner's confirmation before the
tag.** Without P1, G5 is NOT_MEASURED under the item that `CERTIFICATE_SCOPE.md` fixed before the window, and the
certificate would not be PASS. Both readings are kept side by side (`certificate.json` `gates` /
`gates_without_p1`), as v2.2.4 did for A5. NOT TAGGED, NOT PUBLISHED.

Issued 2026-10-20 after the production maintenance window of 2026-10-20 01:28-02:54Z (coordinator-run, owner GO).
v2.2.5 is a compact release: configuration, container definitions, mesh certificates, and the Query python image with
the IDR-129 fix. **No Rust binary changed.** Every image digest below was read with `docker inspect` /
`docker image inspect` (read-only, 2026-10-20 ~03:00Z). Every sha256 was computed with `sha256sum` or Python `hashlib`.
Machine form: [certificate.json](certificate.json) (schema `qdl.scoped-release-certificate.v2`).

Companion files: [RELEASE_NOTES.md](RELEASE_NOTES.md), [endpoint-report.json](endpoint-report.json),
[CERTIFICATE_SCOPE.md](CERTIFICATE_SCOPE.md) (the per-gate matrix fixed before the window),
[MAINTENANCE_WINDOW_PACKET.md](MAINTENANCE_WINDOW_PACKET.md) (procedure and section 10 result),
[W12_DECISION.md](W12_DECISION.md), window receipts [window-20261020T0128Z/](window-20261020T0128Z/) (`SHA256SUMS`).

## 1. Decision rule

- Fixed before the window (`CERTIFICATE_SCOPE.md` sections 2-3): a gate whose measured source is unchanged is
  inherited from v2.2.4 with its named delta proof; G5/G6 are never inherited (Query code changed, IDR-129) and rest
  on delta proof D3 (a)-(d); "a missing item of (b) is NOT_MEASURED and blocks the G5 claim". R1, R2, R3 and R4 must
  PASS.
- Carried from v2.2.4 section 1: no gate FAIL; a NOT_MEASURED item is never upgraded; required CI green on the PR head.
- `status` is `PASS` only when all hold. Section 4 shows that one predeclared item (D3 (b) concurrent hot-read latency)
  is missing; PASS therefore rests on P1. That is a post-measurement change of how G5 is scoped, made at packaging,
  and is labelled as such everywhere.

## 2. Identity

| Item | Value | Source |
| --- | --- | --- |
| Previous published release | `v2.2.4` (tag on main `b5ab3c2`) | git |
| Release head carrying this certificate | the packaging commit on `docs/v225-release-package` (SHA reported in the PR) | git |
| Query image (query_kn_1/2, **changed**) | `qdl-v2-python:2.2.5-9b6747f` `sha256:753604f8abaa866e2de30c7ccaf36168986bc6991550ee326d53e51b83d1550a`, label `9b6747fdcefbc3ebd9107776411db4ba9528d5e5`; build receipt PASS: 20 files differ from 3e9e4d3a = exactly `git diff 31a5155..9b6747f`, unexpected 0, pip freeze equal ([python-image-build-receipt.json](python-image-build-receipt.json) `2bacdc16...`) | `docker image inspect`, `sha256sum` |
| Rust images (**unchanged digests**) | ingestors x2, stream x2 `27c79b53...` (label `9dab224`); projectors x2 `60f83a51...` (`c7ce7d3`); rust_core x3 `7c848aad...` (`a4e7b3b`, rc22) | `docker image inspect` |
| Unchanged non-DL-built images | kafka1-3 `apache/kafka@sha256:9516fb76...`; market_cache, stable_redis `redis@sha256:dfa18828...`; binance_bar_edge `08f356a8...` (rc14 `a388537`, held, scoped out) | `docker inspect` |
| Container definitions | `deploy/compose/qdl-v2-stable-kn.yml` `50c1f014...` + `qdl-v2-stable-kn.v2.2.5.env` `c6770d5d...` (every image by digest) | `sha256sum` |
| Mesh TLS | generation `20261019T084200Z`, one CA `610c0c6f...` (notAfter 2027-10-19T08:42:01Z), every leaf notAfter 2027-10-19, same CN/SAN per identity; replaces `20260822T144323Z` (CA `7b374a30...`, notAfter 2026-11-20T14:43:23Z); `rotate_mesh_tls.py verify` 118/118 PASS on the day | `window-20261020T0128Z/tls-verify.json` `0a650aa5...` |
| TS writer (W12-A) | `market_data_service_p184w` `sha256:2deb90b0cc52eae7610fa44692f90df077e9db100ca259e38fd652a2dc1c5dc5` (TS `30f50a91`), unchanged; identity mount = new generation; JWT key and `DATA_LAYER_V2_JWT_MANIFEST_REVISION=11` unchanged; `MARKET_DATA_NATIVE_REQUIRE_RESUME=1` | `docker inspect` |
| SDK | `qdl-sdk 2.4.1`, wheel `9f79e032604bf79db5ca9070c7f8a99b242e7cf22e1659b395b307dc798c28e7` = v2.2.4; rebuilt 2026-10-20 from this branch: wheel, `.cdx.json` `6bda0014...`, `.release.json` `273e5326...` byte-identical; `git diff b5ab3c2 -- qdl_sdk generated contracts scripts/build_qdl_sdk_release.py` empty | `sha256sum`, `git diff` |

### Runtime at issue (`docker inspect`, 2026-10-20 ~03:00Z)

| Role | Image | Caps (vCPU / memory) | RestartCount | Health | Started |
| --- | --- | --- | --- | --- | --- |
| query_kn_1, _2 | `753604f8` | 2.0 / 2 GiB | 0, 0 | healthy | 01:32:45Z |
| ingestor_okx_swap, ingestor_binance_usdm | `27c79b53` | 1.0 / 0.75 | 0, 0 | healthy | 01:32:13Z |
| stream_kn_1, _2 | `27c79b53` | 0.75 | 0, 0 | healthy | 01:33:12Z |
| market_projector_1, _2 | `60f83a51` | 0.75 | 0, 0 | healthy | 01:32:14Z |
| rust_core, _2, _3 | `7c848aad` | 1.0 / 1.0 / 0.5 | 0, 0, 0 | healthy | 01:32:12Z |
| kafka1-3 | `9516fb76` | 2.25 (NanoCpus 2249999872) | 0, 0, 0 | healthy | 01:31:26Z |
| stable_redis | `dfa18828` | 0.5 | 0 | healthy | 01:31:26Z |
| market_cache | `dfa18828` | 1.0 | 0 | healthy | 2026-09-27 (never recreated) |
| binance_bar_edge | `08f356a8` (held) | 0.75 | 0 | healthy | 01:35:14Z |
| market_data_service_p184w (TS) | `2deb90b0` | - | 0 | (no healthcheck) | 01:38:32Z |
| monitor_service (TS) | `5cd22b4e`, **exited** (W14 deferred) | - | - | - | - |

17 DL roles, none OOM-killed. 2 of them (query x2) run a v2.2.5 image; the other 15 keep their v2.2.4-certified
digests.

## 3. Evidence index

| Evidence | Path | sha256 |
| --- | --- | --- |
| Window receipts (non-secret copies) | [window-20261020T0128Z/](window-20261020T0128Z/) | `SHA256SUMS` lists all 19 files; r1 `eb47296d...`, r4-soak.jsonl `82a4e9aa...`, r4-soak.out `d4301a18...`, r2-parity-rerun `15415306...` |
| Pre-window compose parity | `window-20261020T0128Z/pre-parity.json` | `184e7a13...` (PASS, 847 checks) |
| R2 after the tool fix | `window-20261020T0128Z/r2-parity-rerun.json` | `15415306...` (PASS, 863 checks, 0 problems, 6 intended deltas) |
| IDR-129 isolated A/B arm A (3e9e4d3a) | `~/.local/state/qdl-v2/idr129-query-rss-20261019b/A-stage.out` | `d758dbff687a7446d5267bce4715014406fbbbc3f8609dfacf0315238a92c3e2` |
| IDR-129 isolated A/B arm H (84512ab built image) | `~/.local/state/qdl-v2/idr129-query-rss-20261019c/H.out` | `643dcc60d45a48dffbc4cc7bf6979a560315f12d8b794e2f227c208385faabbd` |
| Query build receipt (window image) | `~/.local/state/qdl-v2/build-v2.2.5-9b6747f/receipt.json` = [python-image-build-receipt.json](python-image-build-receipt.json) | `2bacdc168e07878cff6062b14f26ad4837b8aaca3200ef63e30c61e17406ae0e` |
| Final isolated rehearsal incl. burst | [rehearsal-final-20261019T175600Z.json](rehearsal-final-20261019T175600Z.json) | `410e0c37229e877a6019099b9b3f37322273667b45f7568f4033355eb10f6096` |
| First rehearsal | [rehearsal-20261019.json](rehearsal-20261019.json) | `9d6586dcd547d4822a062613991dcd9078e2806f45facb5cdf995641bff1a1b0` |
| IDR-127 semantic parity | [semantic-parity.json](semantic-parity.json) | `0d8e3a3bbc0096691b10cfc0a172779968f5dc68bdc7f301cc00492b536b4861` |
| Inherited certificate / endpoint report / scorecard | `../v2.2.4/certificate.json`, `endpoint-report.json`, `scorecard.json` | `42412548...`, `013b66e6...`, `2dc2f8d1...` |
| Inherited G12/G13 | `../v2.2.4/g12-g13-inheritance.json` | `95088c9c...` |
| Endpoint report | [endpoint-report.json](endpoint-report.json) | in certificate.json `endpoint_report_sha256` |

## 4. Gates

Inheritance holds only for the W12-A branch that was taken (writer unchanged) and K6 no-op (k6-post `topics` ==
k6-pre). INHERITED = the v2.2.4 receipt of the counted run `p184-final-campaign-20261019T040311Z` (or the v2.2.4
G12/G13 receipts), pinned by sha256 in `certificate.json` `inherited`.

| Gate | v2.2.5 verdict | Basis |
| --- | --- | --- |
| G1-G3 transit | INHERITED PASS | rust binaries unchanged; regression R1 + R4 PASS; the Query-served parts are G5/G6 |
| G4 per-row delivery | INHERITED PASS | same; under G4-A1, G4-A2, A3 as in v2.2.4 |
| G5 request latency | **PASS under P1; NOT_MEASURED without P1** | below |
| G6 execution eligibility | PASS (delta proof) | D3 (a): the IDR-129 diff (`qdl/api_v2/router.py` warmup response slicing, `qdl/query/service.py` refused-item frame release + `large_warmup_lane_busy`, `qdl/runtime/memory_maintenance.py`, `qdl/runtime/stable.py` install) touches no eligibility, freshness or policy code; IDR-129 tests 12 OK on its PR; production execution-ready 53 after STRICT = pre-window baseline 53 (R4 range 48-53 over 59 samples, READY 64/64 in all) |
| G7-G10 integrity, lossless TRADE, cursor, ordering | INHERITED PASS | rehearsal offset/cursor continuity in every phase; R1 URP 0, under-min-ISR 0, core 3 members, projector lag back (277 / 56) |
| G11 recovery | INHERITED: PASS under A5, **FAIL without A5** (as v2.2.4) | not re-run; the window measured recovery: outage 545 s, STRICT READY 64/64 |
| G12 isolated fault | INHERITED PASS | binaries unchanged |
| G13 writer acceptance | INHERITED PASS (76/76 on 2deb90b0) | W12-A: same writer image; BOOTSTRAP -> STRICT READY 64/64 in the window |
| G14 CPU | INHERITED PASS | healthchecks only added cheap probes; no R4 CPU sample was recorded (predeclared as "not a gate") |

**G5 in detail.** Predeclared delta proof D3 and what exists:

| D3 item | Result |
| --- | --- |
| (a) IDR-129 tests incl. failed/cancelled requests; rows, limits, order unchanged | PASS (PR #46: `tests/test_query_idr129_memory.py` 12 OK, negative control fails on the unfixed service; affected suites 552 tests, 1 environment failure explained in the plan) |
| (b) isolated A/B on a production-volume private cache copy, 3e9e4d3a vs fixed image, 12 x 64-warmup bursts, no manual gc/trim in the scored arms | **RSS**: 472 -> 683 MiB (max 743.7) before vs 361 -> 445 MiB after (plateau). **Warmup latency** (200s, p50 per burst): 6.4-8.7 s vs 6.6-8.3 s. **429 per burst**: 992-1135 vs 991-1118. **Failed slices per burst** (harness at 1 vCPU, queue-dominated, both arms): 27-37 vs 26-35. **Concurrent hot-read latency: NOT_MEASURED** (the harness drives warmups only). Arm H ran image 84512ab; its four IDR-129 files equal the released 9b6747f (`git diff` empty) |
| (c) final rehearsal 64-warmup burst on the window configuration | PASS (3 bursts, baseline 133.4, peaks 160-172, settled 156.8 MiB; near-zero row volume) |
| (d) build receipt | PASS (above) |
| Window | Query max 188.3 MiB during both writer starts; R4 plateau 260-277 MiB, 0 restarts; R4 request latency QUOTE n=7261 p50 <=10 / p95 <=25 / p99 <=25 ms, TRADE n=1800 p50 <=25 / p95 <=25 / p99 <=50 ms (writer native_probe, basis `QUERY_REQUEST_START_TO_REDIS_APPLY_DECISION_MONOTONIC`; a different clock from the v2.2.4 G5 classes, so it is a regression sample, not a G5 re-measurement) |

**P1 (POST_HOC_PACKAGING_DECISION, 2026-10-20, Claude; requires owner confirmation).** G5 is split by code path:
- G5-hot (the v2.2.4 stage-50 per-class p95/p99 of hot reads): the request path of hot reads is unchanged by the
  IDR-129 diff (it changes warmup responses, refused batch items and adds a maintenance hook). The only new code
  that can run on the event loop beside a hot read is the memory maintenance, which runs only when RSS >= 448 MiB
  (`QDL_QUERY_MEMORY_SOFT_LIMIT_MIB`), the large-warmup lane is idle >= 3 s and >= 120 s passed since the last run.
  In production after the window it ran **0 times** (`docker logs --since 2026-10-20T01:33:00Z` of query_kn_1/2,
  grep `kind=memory_maintenance`, 0 and 0; RSS 228 / 234 MiB at 03:00Z). G5-hot is therefore INHERITED PASS.
- G5-cold (warmup shapes): PASS by D3 (b) RSS, latency and 429 rows above.
- G5-maintenance (hot-read latency while a maintenance run executes): **NOT_MEASURED**, stated as a limit. What is
  measured: each run blocks the replica's event loop for 136-160 ms (arm H at 1 vCPU: gc 128-152 ms + trim 8-12 ms),
  at most once per 120 s per replica. A hot read on that replica that overlaps a run waits up to that long. This is a
  bound from measured durations, not a latency measurement under load.

Without P1, the predeclared rule makes G5 NOT_MEASURED and the status not PASS. To remove P1: run the isolated
A/B with a concurrent hot-read driver (both images, under the host lock), or have the owner accept P1.

## 5. Limits stated by this certificate

1. **P1 is post hoc** (section 4): G5 is PASS only with the G5-maintenance item recorded as NOT_MEASURED.
2. **Supported load (IDR-128 OPEN, owner decision 2026-10-19 ~18:00Z).** The inherited latency envelope was measured
   with unrelated heavy stacks paused (15 portal containers, Codex tree stopped); a mixed portal workload on the same
   host is outside the certified scope. The window and the R4 soak ran with the portal **running** and Codex
   read-only, passive load only; R4 numbers are a regression sample under that condition, not a re-certification.
3. Inherited from v2.2.4 and still true: G11 PASS only under the post-hoc A5 (FAIL without it); G14 DL mean CPU 5.75
   vCPU above the 5 vCPU budget (not a gate); IDR-108 open; G12 harness shape is not production shape; G13 cells are
   shadow-certified; the writer vendors SDK 2.4.0.
4. Not measured passively in the window: served event age, delivery lag venue -> canonical, end-to-end venue -> TS
   cache (inherited from v2.2.4 G5/G6, G4, G2).
5. **W14 deferred**: `monitor_service` stays exited (GHCR pull denied, the token lacks `read:packages`), so no
   DATA_LAYER / cert-expiry / Query-memory watchdog runs.
6. Single host, not HA.
7. **Old-identity containers** (packet section 5c): `market_data_service`, `market_data_service_p184w-dr07-rollback`,
   `market_data_service_p184w-r49-rollback` still mount the 2026-08-22 identity; they cannot join the rotated mesh;
   removal waits for the owner's approval (rule 34). Test apparatus pinned to 2026-08-22 identities (e.g.
   `scripts/idr129_query_rss_isolated.py` `IDENTITIES`) must be repointed before its next production-facing run.
8. bar edge held on rc14 (catalog r11 / acquisition r19 checkpoint); inactive client CAs in the consumer bundles
   expire 2026-11-24 .. 2026-12-26 and are used by no running consumer (`CERTIFICATE_SCOPE.md` section 4).
9. The consumer-visible outage of the window was 545 s, including ~2 min of a coordinator wait-loop bug at W10.
10. R2 at the window FAILed on a tool defect (float32 `cpus`); the PASS is the re-run after the fix
    (`CPUS_TOLERANCE` 1e-6 vCPU, unit tests in `tests/test_kn_stable_compose.py`).
11. R4 Query memory rose over the first 10 min (+30 MiB in 5 min) before its plateau; the packet's literal "first 10
    min not growing" wording is not met, the soak tool's last-10-min check is (packet section 10).

## 6. Rollback

Until 2026-11-20 14:43Z: the packet section 6 (old generation + v2.2.4 env, rehearsed twice: 267.8 s / 519.0 s).
After that date the old certificates are expired; an image-only rollback (v2.2.4 env, Query `3e9e4d3a`, with the new
generation env) is the remaining path and is **not rehearsed**. Writer: unchanged image; only its identity mount moves
with the generation. `QDL_QUERY_MEMORY_SOFT_LIMIT_MIB=0` disables the maintenance hook without an image change.

Not touched by this release: rust images, Kafka data/topics/offsets/KRaft metadata, market_cache, JWT keys, cursor
keys, consumer realm dir r16, bar edge image and checkpoint, V1 rollback container.

## 7. Publication (STOP before tagging)

- `certificate.json` `status` is `PASS`; the endpoint report sha256 in it equals the file; RELEASE_NOTES.md exists;
  the SDK wheel is the byte-identical 2.4.1 build. `release.yml` is unchanged.
- Still required before a tag: the owner's confirmation of P1 (or the hot-read A/B); required CI green on the PR
  heads; the release head on `origin/main`. The coordinator creates the tag.
