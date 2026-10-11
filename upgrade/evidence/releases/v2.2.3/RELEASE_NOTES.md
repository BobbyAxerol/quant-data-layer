# Quant Data Layer v2.2.3

Status: `PREPARED_NOT_PUBLISHED`. This is not a published release, a Git tag or an owner publication decision.
Publication needs the owner to push and tag. Remote CI must also run on the final head. The published stable
release is still v2.2.2.

Scorecard: [scorecard.json](scorecard.json). Inventory, hashes, the SDK build and CI: [release-preparation.json](release-preparation.json).

## Scope

This release closes the P18.4 phase D Data Layer work on the Kafka-native stack. It contains:
- one-commit images for every application role (IDR-039, closed on 413c5b9);
- execution-view correctness fixes (IDR-062, IDR-040, IDR-084, Astra R1/R3);
- hot-path contention fixes (Astra R2/R3), the OKX session fixes and the ingestor liveness knob;
- the G5 warmup fixes;
- the Stream replica byte budget fix (a shared decoded record is charged once) and the provider admission
  INFLIGHT_CAPACITY wait fix;
- the funding settlement publication allowance (60 s, now backed by a measurement);
- additive public fields: `event_id` (IDR-082) and `proof_observed_at_ns` (IDR-040 fix 2), in SDK 2.2.1.

Public endpoints keep their versions. Every change is additive, and readers on SDK 2.1.0 ignore the new fields.

This is not a Trading System order, accounting or alpha certification. It is not HA or independent-failure-domain
evidence, because every run used the same host.

## Source And Images

- Release source: `413c5b914f8c6fd0f67429fdddb39451589e9745` on `fix/p184-dl-closure`.
- The only later commit, 2f489b1, changes scripts, tests and the plan journal (`git diff --stat 413c5b9 2f489b1`:
  `scripts/p184_g6_ts_execution_probe.py`, `scripts/phase3_consumer_load_acceptance.py`,
  `tests/test_p184_g6_view_quality.py`, the plan). It is not in an image.
- Commits since the previous evidence source bbfb3be (`git log --oneline bbfb3be..2f489b1`):
  - runtime code: 2e0de95 (Query funding allowance re-applied with measurement), dadb9c8 (Stream charge-once
    budget), 70bef7d (SDK 2.2.1 random stream replica start), 413c5b9 (admission INFLIGHT_CAPACITY poll);
  - tooling: 259aeb9 (local CI runner), 627f19d, 7188b8b, 8bbcaae (G12 harness), 848d5d2, 65ada37 (campaign tooling
    and G1-G14 scorer), ba496ff (G6 scorer and TS EXECUTION probe), 2f489b1 (G6 anchor correction);
  - journal and evidence: 163191c, e7b2028, 59e725a (previous release evidence), 2d11a87, 6d5da7f.

Images were checked with `docker image inspect`. Both revision labels equal 413c5b9, which closes IDR-039. The
version label reads `2.2.3-rc10`, because a tag never changes a digest.

| Image | Digest | Tag | Roles |
| --- | --- | --- | --- |
| Python | `sha256:a0c3649d9a47cf59300746cb521fd020b190fbeefb3ff0bda6851ea8033119eb` | `qdl-v2-python:2.2.3-rc10-413c5b9` | query_kn_1/2, binance_bar_edge |
| Rust | `sha256:96a2c9120ef42892f56278df5d6c6dc5371cc83aa713b7b692902d657f74c3d5` | `qdl-v2-rust:2.2.3-rc10-413c5b9` | stream_kn_1/2, market_projector_1/2, rust_core x3, ingestor_okx_swap, ingestor_binance_usdm |

All 12 application roles run these two digests: restart count 0, OOM false (`docker inspect`, 2026-10-11). They were
rolled one role at a time between 04:30:01Z and 04:35:15Z on 2026-10-11, with the writer READY 64/64 after every
step (receipts `p184-d3-capacity-20261009/rollout-rc6/<role>-receipt-20261011T*.json`). The bar edge already ran the
rc10 Python image since 2026-10-10T18:46Z.

The following were not changed by this release:
- kafka1-3 (`sha256:9516fb7634ba`), apart from the dynamic broker config below;
- market_cache and stable_redis (`sha256:dfa18828cbc0`);
- the one-shots stable_state_init and stable_tls_init (`sha256:bd0163fd76b0`).

Ingestor configs are unchanged from the previous record: OKX revision 20, Binance revision 19, liveness 1500/4000 ms,
no `book_liveness_proof_ms`, so IDR-040 fix 2 is OFF.

TS writer `market_data_service_p184w` (TS repo `trading-system-next-upgrade`, pushed):
- image `sha256:bae2509fac12c2ba00fc5815a9d89924d8adfe3ad482bbccc7d55ea6b1f6510c`, TS `8b567eb`, SDK 2.1.0;
- env during campaign 4: `DATA_LAYER_V2_STREAM_PIPELINE_DEPTH=1`, `MARKET_DATA_FIXED_WORKER_PINS=OKX:BTCUSDT=2,OKX:ETHUSDT=0`,
  `DATA_LAYER_V2_MARK_WAKE_READ_DELAY_BY_VENUE=BINANCE=100` (from the writer inspect taken before the post-campaign
  roll, `trading-system/p184-rev11-window/campaign-roll/inspect-20261011T042442Z.json`, sha256 `ee6c5ff4...`);
- since 2026-10-11T04:24:53Z the writer runs depth 0 with the same pins and MARK delay (`docker inspect`). Depth 0
  was the coordinator's choice from two comparisons; that comparison is not re-verified in this record.

## Runtime Config Change: Kafka Log-Cleaner Throttle

- Change: cluster-wide dynamic default `log.cleaner.io.max.bytes.per.second=8388608` (8 MiB/s), applied
  2026-10-11T03:40:56Z without a broker restart. The owner approved it through a question at about 03:40Z.
- Cause: compaction of `md.latest.v2` on kafka1 (CPU limit 1.25 vCPU, `docker inspect` NanoCpus) pinned the
  broker. While it ran, every rust_core transaction commit stalled about 2.06 s, at 14:12, 15:29 and 19:09Z on
  2026-10-10. The 19:09Z stall is behind the G1/G2 max of campaign 20261010T185053Z.
- Receipt: `~/.local/state/qdl-v2/kafka-cleaner-throttle-20261010/` (README.md `72cbcf87...`, alter.out
  `a59f5deb...`, after.out `8a635a9b...`, applied_at.txt `5f94630c...`).
- Rollback, failure only: `kafka-configs.sh --alter --delete-config log.cleaner.io.max.bytes.per.second`.

## Gate Scorecard (G1-G14)

Gates come from the frozen TS table `upgrade/p184-phase-d-gates.md` (TS 31cf663, sha256 `c67bf69b...`) and the
frozen budget `config/v2/p184-stage100-budget.json` (sha256 `ff85335b...`).

Scoring rule: a PASS needs the table's measurement rule, which is steady state for at least 30 min at the stage-100
workload plus a burst window. A measurement outside that rule can FAIL a gate but cannot PASS it. A NOT_MEASURED is
never upgraded. No frozen gate was widened.

The gate scorecard is final campaign 4, `~/.local/state/qdl-v2/p184-final-campaign-20261011T034233Z/` (scorecard
sha256 `01336708...`, `protocol_met: true`, no protocol problems, load 03:44:05-04:14:05Z, 64 demanded slices, writer
restarts 0). G12 and G13 are scored from their own receipts on the release digests.

Summary:
- PASS: G1, G3, G5, G6, G7, G9, G10, G11, G14.
- FAIL: G2, G8.
- NOT_MEASURED: G4, G12.
- DEFERRED: G13.

Paths under `~/.local/state/qdl-v2/`; sha256 prefixes, full hashes in scorecard.json.

| Gate | Rule | Measured | Source (sha256) | Verdict |
| --- | --- | --- | --- | --- |
| G1 BOOK e2e to TS cache | p95 <= 500, p99 <= 1000, max < 2000 ms | DELTA B/O p95 177.4/191.5, p99 334.1/380.8, max 1736.7/1844.2 (n 83994/97565); SNAPSHOT B/O p95 245.6/236.7, max 1211.4/1144.5 | campaign `trace/transit-load.json` (d1376db1), `trace/mark-load.json` (7ac870af) | **PASS** |
| G2 TRADE/QUOTE/MARK new-event transit | TRADE p95 <= 500, max < 1500; QUOTE/MARK p95 <= 500, max < 1000 ms | **TRADE BINANCE p95 1562.1, max 5338.8 (n 160328); TRADE OKX p95 2084.2, max 4301.8 (n 39292)**; QUOTE B/O p95 188.0/207.1, **max 1439.2/1340.6**; MARK B/O p95 396.7/348.5, **max 1125.4/1227.8** | same two files | **FAIL** |
| G3 BAR e2e from close | p95 <= 5 s, max < 60 s | pooled n 320, p95 1535, max 3007 ms; 0 unseen, 0 read errors | `g3/g3-summary.json` (a15a368f) | **PASS** |
| G4 venue -> canonical | candidate p95 <= baseline p95 x 1.2 per row | 17/17 baselined rows PASS in every window they were sampled (e.g. trade B/O 108.2-133.2 / 110.8-136.6 vs limits 269.3/257.4). 10 observed rows have no frozen baseline: bar BINANCE 2h/4h/5m, bar OKX 15m/4h/5m, OKX_INVERSE book_delta/book_snapshot/mark/quote | `g4-{1..5}-summary.json` (02a554c5, 42aa78a1, 013e6929, 86494b65, 0111526b) | NOT_MEASURED: rows without baseline |
| G5 request latency | certified p95/p99 per class and venue; frozen cold limits | All 10 class/venue rows PASS (worst p95 95.0 OKX L2, worst p99 293.8 BINANCE L2); cold 2500/5000 overlap 4 runs, 0 bad | `stage100/gates.json` (63f12e8c), `receipt.json` (a79c9c9d) | **PASS** |
| G6 durable-view event age + quality | per-binding D3 policy of the TS EXECUTION consumer; zero unsafe | TS EXECUTION probe 19121 reads over 32 targets: every D3 binding measured, 0 unsafe (e.g. MARK OKX 3563 ELIGIBLE + 25 ELIGIBLE_QUIET; QUOTE OKX 3515 + 68; TRADE NOT_ELIGIBLE B/O 49/463, typed). Alpha reads: 0 unsafe under their own requirement | `g6-ts/summary.json` (229e0380) | **PASS** |
| G7 TS consumer path | SDK -> writer p95 <= 100; writer -> Redis p95 <= 50 ms per feed | SDK -> writer p95: BOOK_DELTA 51.0, BOOK_SNAPSHOT 79.3, QUOTE 40.7, TRADE 83.0, MARK 0.6. Writer -> Redis p95: 18.4, 41.2, 10.5, 46.7, 20.2 | `trace/transit-load.json` (d1376db1) | **PASS** |
| G8 TS execution Redis | commandstats <= 60 % core per 10 min; SLOWLOG 0 in steady state; PING p99 <= 5 ms | commandstats <= 0.0868 core; PING p99 <= 1.675 ms per window (n 3850, 0 failures); **SLOWLOG 6 new EVALSHA entries, 10.0-19.7 ms**; 0 MONITOR clients | `g8/g8-summary.json` (2b3375cf) | **FAIL** |
| G9 readiness | demanded == session-ready; 0 data-path eligibility losses; v1 fallback 0 | 180/180 samples readable, demanded 64 == session-ready in every sample, 0 data-path losses, 24 legitimate quiet TRADE refusals, v1 fallback 0 | `g9g10/g9-g10-summary.json` (3015c2db) | **PASS** |
| G10 TRADE integrity | 0 gaps, 0 dup applied, 0 replay-expired, 0 overflow | all 0; 2 RECONNECTED resumes; 129 log polls, 0 unreadable | same file and trace | **PASS** |
| G11 writer restart recovery | first valid apply <= 30 s; caught up <= 60 s | outage 0.121 s; 64/64 slices caught up, p50 3.919, max 18.923 s; 0 rewinds; cursor never ahead | `g11/g11-summary.json` (c59cccf3) | **PASS** |
| G12 broker/projector/Stream recovery | caught up <= 600 s, then G1-G3 | Isolated, release digests: broker stop/start 22.334 s, projector restart 0.935 s, Stream restart 2.294 s; G1 300 s windows after each inside (max 1970.0 ms OKX after the broker fault). G2/G3 after recovery: NOT_MEASURED (BOOK-only source) | `p184-g12-rc10-20261011-a2/evidence/verdict.json` (05c0652d), `teardown.json` (f17de5ca) | **NOT_MEASURED** (catch-up and G1 inside; G2/G3 after recovery not measured) |
| G13 DR01-DR08 | every cell once on the final digests | 41/41 DL cells ok on the release digests: dr05-upstream 6, dr08 4, dr05-transport 9, views 22 (2 quiet cells NOT_OBSERVED). 4 TS steps deferred | `p184-g13-20261011T043821Z/summary.json` (3a3a34e2), `...T045533Z/summary.json` (309199f1) | DEFERRED |
| G14 resources | CPU/RAM per role at stage 100, with headroom | 18/18 roles, 151 samples, none at or over its memory limit. Mean CPU: kafka 81/98/102 %, query 57/66 %, writer 151 %; DL mean sum 5.37 vCPU. Lowest memory headroom query_kn_1 14.3 % | `stage100/host.json` (06eab7ba) | **PASS** |

Measurement identity. Campaign 4 ran before the rc10 roll, on this mix:
- query_kn_1/2: rc8 `sha256:db5ae6752280` (6d5da7f). `git diff --stat 6d5da7f 413c5b9`, without scripts, tests and
  journal: `qdl/admission/edge.py` +9, `qdl_sdk` (2.2.1 version string and stream transport), the Stream gateway
  Rust source (not run by Query) and a contracts doc. Service code imports only `qdl_sdk.models` and
  `qdl_sdk.reference`. **Not source-identical**: reads that call a provider through `BlockingProviderAdmission`
  (OKX admitted REST and reference, Binance admitted klines) differ when an INFLIGHT_CAPACITY deferral occurs. The
  other Query paths are source-identical.
- stream_kn_1/2: rc9 `sha256:d32e354bd662` (70bef7d). The only diff to 413c5b9 is `qdl/admission/edge.py`, which
  is not in the Rust build. Rust tree, `generated/rust`, Cargo files and config are unchanged: source-identical.
- binance_bar_edge: the rc10 release image itself.
- rust_core x3, projectors and ingestors: rc7d `sha256:64468c72ce5a` (bbfb3be). The Rust diff to 413c5b9 is
  `rust/qdl-stream-gateway` only. No other crate depends on it, and Cargo files, `generated/rust`, config and
  Dockerfile are unchanged: source-equivalent for these binaries (rebuilt, not byte-compared).

G12 and G13 ran on the release digests themselves.

Findings behind the verdicts:
- **G2 FAIL: writer throughput, not the DL canonical path.** Read-only per-second analysis of the campaign-4
  delivery trace (214 parts, the scorecard load window):
  - 1699 of 1801 seconds have max end-to-end < 500 ms; 28 seconds exceed 1500 ms;
  - in 25 of those 28 seconds every record over 1500 ms is TRADE; the other three also hold BOOK_DELTA records over
    1500 ms (G1 max stays below 2000 ms);
  - the largest burst is 4274 TRADE records in one source second on one slice (BINANCE ETHUSDT), all on one writer
    worker; the most a single worker applied in one second is 1138 TRADE records;
  - TRADE BINANCE segment p95: venue -> ingest 39.6, ingest -> SDK return 1430.7, SDK -> Redis reply 118.2 ms;
  - G4 in the same campaign puts venue -> canonical TRADE p95 at 108.2-136.6 ms in all five windows, and G10 has 0
    overflow, so the tail sits behind canonical visibility, in single-slice TRADE bursts queued at one writer
    worker;
  - TRADE volume was about 2.2x the earlier campaigns (TRADE BINANCE n 160328 against 69966 and 74061).
  - The Stream-side dispatch and queue metrics for this campaign were not kept in a file, so they are not cited.
- **G8 FAIL**, same cause. All six SLOWLOG entries are writer APPLY EVALSHA calls in TRADE burst seconds. The
  TRADE volume peak within +-1 s of each entry is at or above the 93rd percentile of per-second TRADE volume (median
  46, p95 428 applied records per second). Command CPU and PING stay inside.
- **Owner decision, 2026-10-11 (AskUserQuestion):** release the DL. Writer throughput (SDK batch decode, Rust
  projection, EVAL chunking) is the first item of TS phase E. The gates are not widened, and the FAILs stay FAILs.
- G4 stays NOT_MEASURED only because of rows without a frozen baseline. No baseline is added after measuring.
- G4 bar latency for OKX is measured from bar open (the OKX source time is the open). The baseline used the same
  metric.

## Earlier Final Campaigns (2026-10-10 .. 2026-10-11)

Paths under `~/.local/state/qdl-v2/`. None of these results is erased by campaign 4.

| Campaign | Setup | Result | What it led to |
| --- | --- | --- | --- |
| `p184-final-campaign-20261010T171129Z` (94f30f85) | writer `sha256:ace42326` depth 1; Query rc8, rest rc7d | protocol met; FAIL G2, G6 | Stream replica byte-budget overflow at 17:22:48Z (84 streams closed BACKPRESSURE with empty own queues) -> dadb9c8. G6 FAIL was a scorer defect -> ba496ff |
| `p184-final-campaign-20261010T174944Z` (1c6a5d30) | same, writer depth 0 | protocol met; FAIL G2, G4, G6, G8 | G4 bar BINANCE 3m/30m at 18:00Z: admission slept the INFLIGHT_CAPACITY lease-expiry hint -> 413c5b9 |
| `p184-final-campaign-20261010T185053Z` (ac0b3f66) | writer bae2509f depth 0; Query rc8, Stream rc9, bar edge rc10, rest rc7d | protocol NOT met (startup:bounded FAIL); FAIL G1, G2, G4 | Compaction stall at 19:09Z -> Kafka cleaner throttle. G6 funding anchor took the oldest settlement row -> 2f489b1 |
| `p184-final-campaign-20261011T033641Z` (c3535323) | writer bae2509f depth 1 | aborted about 3 min into the load (driver SIGINT; the coordinator's watcher restarted the writer once when G11 armed during the shutdown) | stopped to apply the Kafka cleaner throttle (03:40:56Z) |

`startup:bounded` across the five stage-100 runs (stage 100m and the four driver runs above): one FAIL, in
20261010T185053Z, where session s89 exhausted the frozen 10 retries (`retry_max_attempts` 10, retry code
RATE_LIMITED; gates.json `8a0db47a...`). The frozen budget is kept.

## G12 And G13 On The Release Digests

G12, isolated (owner approved), `p184-g12-rc10-20261011-a2`:
- `status COMPLETED`, `pass true`. Image set: core/projector/Stream rust `96a2c912`, python `a0c3649d`, writer
  `bae2509f`.
- Caught up after the fault cleared: broker stop/start 22.334 s (OKX 2.177 s), projector restart 0.935 s, Stream
  restart 2.294 s, all <= 600 s.
- G1 in the 300 s window after each fault: inside; the largest max is 1970.0 ms (OKX, after the broker fault).
- Teardown: 12 containers removed, 0 left by label or name, volumes 217 -> 217.
- Scope: same host, TEST_ONLY BOOK source, single projector/Stream replica; G2/G3 after recovery NOT_MEASURED.

Failed and stopped G12 attempts, kept:
- `p184-g12-rc10-20261011` FAILED on a harness STOP: `RuntimeError: STOP role exited: ...-monitor`. The monitor's
  last record is 3.9 s before the first fault injection (kafka-2 stop), and the STOP fired 27 ms after the
  injection. No traceback or OOM was retained, the same symptom as attempt a3 on 2026-10-10 (verdict `728b460d...`;
  teardown 10 removed, 0 left, volumes 217 -> 217).
- `p184-g12-rc10-20261011-r2` was a duplicate run started alongside a2. The coordinator stopped it with SIGTERM.
  Its teardown removed 5 containers and left 0 by label or name. It read volumes 217 -> 219 while a2 held its own
  volumes; a2's teardown later read 217.
- Open harness item: keep the monitor container's exit code and OOM state in the evidence.

G13, on DL 413c5b9 and writer bae2509f (TS 8b567eb):
- `p184-g13-20261011T043821Z`: dr05-upstream 6/6, dr08-isolated 4/4 (QUALIFICATION), dr05-transport 9/9 ok.
- Its dr04-dr06-views step failed on DR04.EXPLICIT_LAST@BINANCE with `EXECUTION_CONTEXT_STALE`. The live LAST was
  older than the probe limit in a quiet Sunday market, so the refusal is correct and typed. The age sample behind
  this reading was not kept in a file. The rows are kept (`views-binance.rows`, `22e943b9...`).
- `p184-g13-20261011T045533Z`: dr04-dr06-views 22/22 ok. DR05.QUIET_TYPED BINANCE/OKX are ok but NOT_OBSERVED,
  because no book was quiet in the 300 s window.
- Runtime approval: the owner's AskUserQuestion answer of 2026-10-11. The file
  `p184-g13-inputs/runtime-approval-rc10.md` (`55189088...`) was written by an agent from that relayed approval.
- Deferred to the TS release: dr01-deployed, dr01-wrong-account, dr02-dr03-alpha, dr07-reference.

## Items Closed (commit)

| Item | Commit(s) | State |
| --- | --- | --- |
| IDR-039 one-commit images for every application role | rc10 rollout of 413c5b9 | **closed**: 12/12 roles on 413c5b9 |
| Stream replica byte budget: shared decoded record charged once; per-subscription cap total/2 | dadb9c8 | in images |
| Provider admission: INFLIGHT_CAPACITY deferral polls 50-200 ms instead of sleeping the lease expiry | 413c5b9 | in images |
| Funding settlement publication allowance 60 s, backed by the 2026-10-10 16:00Z poll | 2e0de95 | in images |
| SDK 2.2.1: stream transport starts on a random replica per instance | 70bef7d | wheel below |
| G6 scorer: read-own-requirement, D3 by a TS EXECUTION probe; anchor per product and grade | ba496ff, 2f489b1 | tooling |
| Gate-campaign tooling G3/G8/G9/G10/G11/G14 and the G1-G14 scorer | 848d5d2, 65ada37 | tooling |
| G12 harness: image set as a parameter, topic metadata retry, signal teardown, in-container stderr | 627f19d, 7188b8b, 8bbcaae | tooling |
| Kafka log-cleaner I/O throttle 8 MiB/s | runtime config, receipt above | runtime |
| Items closed in the 2026-10-10 record (IDR-062, IDR-040 fixes 1-2, IDR-002/029, D2, IDR-075, IDR-076, IDR-082, IDR-084, Astra R1-R7, Codex review items, OKX fixes, Stream terminal errors, cursor base64, DR05 fence, CI convergence) | see appendix | in images |

The funding allowance revert 530a406 listed in the 2026-10-10 record is superseded by 2e0de95.

## SDK 2.2.1 Wheel

SDK 2.2.0 is superseded. It was never published, but its wheel hash is in the 2026-10-10 record, so the version was
bumped rather than reused.

| Field | Value |
| --- | --- |
| wheel | `qdl_sdk-2.2.1-py3-none-any.whl` |
| wheel sha256 | `27db39715c58988128dfec3364919a3ef3bb9425e9bdb4cb7655829ee3f1076f` |
| reproducible | two builds byte-identical (`cmp`); wheel, SBOM and release json hashes equal |
| sdk_source_digest | `fcbd832c1a3e89c66ad625e488ae2a4f80f7baa895abb7b8904febfcb54aa217` |
| generated_contract_digest | `ae77dc42074fd7cc896a46bde3282837a51b667acee23c4957d37565690856e9` (unchanged from 2.2.0) |
| release json sha256 | `d2ae9ef67e334540e3cef4d0e1e1b17896762262263e4d6d4fadfeb518c894e6` |
| SBOM sha256 | `e80dcec6ce588d715e7e5d6bf3aa5449426c80d999f8d69647081047398d9111` |

The artifacts are under `~/.local/state/qdl-v2/releases/v2.2.3/sdk221-build-{1,2}/`, outside Git. The TS writer runs
SDK 2.1.0.

## CI

Local exact-source CI (`scripts/p184_local_ci.py` replaying `.github/workflows/ci.yml` on an archive of 2f489b1):
- all jobs except unit-tests, summary `p184-local-ci-2f489b1/summary.json` (sha256 `f00a9423...`, receipt status
  `INCOMPLETE`):
  - consolidated-source-preflight, kn-native-integration and sdk-python310 PASS;
  - contract-tests 7 PASS, 3 SKIPPED: #2 and #5 are pull-request-only steps; #10 (Rust dependency, license and
    advisory policy) needs a host download, which the runner does not do;
- unit-tests, summary `p184-local-ci-2f489b1-unit/summary.json` (sha256 `98c6c157...`, status `INCOMPLETE`):
  19 PASS, 3 SKIPPED:
  - #4 needs sudo (reported, never escalated);
  - #19 and #20 (trivy image scan, trivy secret/misconfiguration scan) had no local trivy image;
- the unit-tests re-run with `--trivy-image aquasec/trivy:0.58.1` (`p184-local-ci-2f489b1-unit-r2`, summary sha256
  `f6b6b5e7`): every step PASS, including #19 and #20 (trivy scans); only #4 (sudo) SKIPPED.

Remote GitHub CI did NOT run, because the DL repository is not pushed. Publication requires the required jobs on
the final head.

## Runtime Changes Performed (2026-10-10 .. 2026-10-11)

Each change was owner-approved. Each role was recreated one at a time.

| Change | Rollback reference |
| --- | --- |
| Query rc8 (6d5da7f, funding allowance) on query_kn_1/2, 2026-10-10 17:09Z | rc7d python `sha256:e0252f4c96da` |
| Stream rc9 (70bef7d, charge-once budget) on stream_kn_1/2, 18:44-18:45Z | rc7d rust `sha256:64468c72ce5a` |
| bar edge rc10 python (413c5b9), 18:46Z | rc7d python `sha256:e0252f4c96da` |
| TS writer images for the campaigns (ace42326, then bae2509f 8b567eb) and env depth/pins | `trading-system/p184-rev11-window/campaign-roll/rollback-*.compose.json` |
| Kafka dynamic default `log.cleaner.io.max.bytes.per.second=8388608`, 2026-10-11T03:40:56Z | `--delete-config log.cleaner.io.max.bytes.per.second` |
| TS writer depth 1 -> 0 after campaign 4, 04:24:53Z | `campaign-roll/rollback-20261011T042442Z.compose.json` (e9c22775) |
| **rc10 (413c5b9), all 12 application roles, 04:30-04:35Z 2026-10-11**, writer READY 64/64 after every step | `p184-d3-capacity-20261009/rollout-rc6/<role>-rollback.compose.json`: Query -> rc8 `db5ae6752280`, Stream -> rc9 `d32e354bd662`, projectors/cores/ingestors -> rc7d `64468c72ce5a`, bar edge -> rc10 (unchanged) |

Rollback runs in reverse role order. Kafka data, Redis, realm dirs and offsets need no data migration. Rollback is
for failure only. The 2026-10-08 .. 2026-10-10 changes are listed in the appendix.

## Known Limits

- **G2 TRADE and G8 SLOWLOG FAIL: writer throughput under TRADE bursts.** Owner decision: TS phase E, first item.
  The gates are not widened.
- **DL CPU above the owner's 5 vCPU budget:** G14 passes its own per-role headroom rule, but the DL mean sum at
  stage 100 was 5.37 vCPU in campaign 4 (`stage100/host.json`), above the owner's DL budget of 5 vCPU. Kafka is the
  largest share. Tuning it is open: one variable at a time, measured against G1/G2.
- **Single host:** every run used the same host. No HA and no independent failure domains. No mainnet or funded
  claim.
- **G4 rows without a frozen baseline** (bar BINANCE 2h/4h/5m, bar OKX 15m/4h/5m, OKX_INVERSE book_delta,
  book_snapshot, mark and quote) keep G4 NOT_MEASURED. No baseline is added after measuring.
- **G6 BOOK_DELTA** is not a Query shape in the frozen workload, so it has no G6 row.
- **startup:bounded** is a stochastic tail of the warmup lane: one session exhausted its 10 retries in one of five
  stage-100 runs. The frozen budget is kept.
- **G12 scope:** isolated, TEST_ONLY BOOK, single replica. G2/G3 after recovery are not measured. The harness does
  not yet keep the monitor's exit code and OOM state.
- **G13:** the four TS-stack steps stay OPEN. The two DR05.QUIET_TYPED cells are NOT_OBSERVED, not a positive
  quiet-state proof.
- **Measurement identity:** campaign 4 ran the pre-rc10 mix. Query is not source-identical to 413c5b9 on provider
  admission waits (see above).
- **IDR-040 fix 2 is OFF** (`book_liveness_proof_ms` unset). A quiet Binance book stays stale, never falsely fresh.
- **Catalog revision split (C.19)** is unchanged: image catalog revision 11, runtime-mounted revision 12.
- **CI:** local only; contract-tests #10 was skipped (needs a download), #4 needs sudo, and remote CI has not run.
- Historical failures stay recorded: the four earlier final campaigns above, the failed G12 attempt, the first G13
  views run, and every failure listed in the appendix. None is erased by a later PASS.

## Consumer Hand-Off (TS phase E)

- Adopt SDK **2.2.1** (wheel sha256 `27db3971...`). The writer runs 2.1.0 today, which ignores the new fields.
- Alpha callers mint JWTs for manifest revision **15** (4950 RPM / 300 streams). An older JWT revision gets 401.
- TS Paper manifest/JWT revision 11 (64 products incl. OKX BTC-USD-SWAP) is live.
- Deploy the IDR-085 gateway fix (TS be21d49) with the TS release. That closes the deferred DR01 cells.
- IDR-040 fix 2: activate the in-band quiet-book proof flag through its own packet when TS needs it.
- Use `event_id` (IDR-082) for trade identity.
- Close the four deferred G13 steps: dr01-deployed, dr01-wrong-account, dr02-dr03-alpha, dr07-reference.
- Writer throughput, first item: SDK batch decode, Rust projection, EVAL chunking. G2 and G8 are re-scored after it
  against the same frozen gates.
- The writer keeps `BINANCE=100` for the MARK wake read delay and the OKX worker pins.

## Publication Requires

1. The owner's publication decision. The 2026-10-11 release decision records G2/G8 as known limits; it does not turn
   them into PASS.
2. Push, remote CI (contract-tests, sdk-python310, unit-tests, kn-native-integration) on the final head, and merge
   to dev and main.
3. An annotated tag `v2.2.3` that links the final CI runs and the hashes of scorecard.json and this file.

No push, merge or tag was performed.

## Appendix: Preparation History

### 2026-10-10 record at bbfb3be (verbatim, superseded by the record above)

Status: `PREPARED_NOT_PUBLISHED`. This is not a published release, a Git tag or an owner publication decision.
Publication needs the owner to push and tag. Remote CI must also run on the final head. The published stable
release is still v2.2.2.

Scorecard: [scorecard.json](scorecard.json). Inventory, hashes and the SDK build: [release-preparation.json](release-preparation.json).

#### Scope

This release closes the P18.4 phase D Data Layer work on the Kafka-native stack. It contains:
- one-commit images for every application role (IDR-039);
- execution-view correctness fixes (IDR-062, IDR-040, IDR-084, Astra R1/R3);
- hot-path contention fixes (Astra R2/R3), the OKX session fixes and the ingestor liveness knob;
- the G5 warmup fixes;
- additive public fields: `event_id` (IDR-082) and `proof_observed_at_ns` (IDR-040 fix 2), both in SDK 2.2.0.

Public endpoints keep their versions. Every change is additive, and readers on SDK 2.1.0 ignore the new fields.

This is not a Trading System order, accounting or alpha certification. It is not HA or independent-failure-domain
evidence, because every run used the same host.

#### Source And Images

- Release source: `bbfb3be6b3f1b7f83b44022e77439daeb4999a65` on `fix/p184-dl-closure`. Its code tree equals
  2c84e2f plus the plan journal.
- Later commits on the branch are journal or tooling only:
  - 259aeb9 is the local CI runner;
  - 163191c and e7b2028 are the plan journal;
  - this commit adds the trace analysis script, its test and the release evidence.
  - None of them is in an image.

Images were checked with `docker image inspect`. Each image's revision label equals the release source. The
version label reads `2.2.3-rc7d`, because a tag never changes a digest.

| Image | Digest | Tag | Roles |
| --- | --- | --- | --- |
| Python | `sha256:e0252f4c96da70791b0c0fd031538bff2dd7249cdf6e0fbd4399c90e2e527a2a` | `qdl-v2-python:2.2.3-rc7d-bbfb3be` | query_kn_1/2, binance_bar_edge |
| Rust | `sha256:64468c72ce5ab36518107014bd4a3e311b96d47fad06e531f6627c46f532a1b7` | `qdl-v2-rust:2.2.3-rc7d-bbfb3be` | stream_kn_1/2, market_projector_1/2, rust_core x3, ingestor_okx_swap, ingestor_binance_usdm |

All 12 application roles run these two digests: restart count 0, OOM false (`docker inspect`, 2026-10-10 15:0xZ).
The following were not changed by this release:
- kafka1-3 (`sha256:9516fb7634ba`);
- market_cache and stable_redis (`sha256:dfa18828cbc0`);
- the one-shots stable_state_init and stable_tls_init (`sha256:bd0163fd76b0`).

TS writer `market_data_service_p184w`:
- image `sha256:34202753bbe8b74d50e45afed1cf2e7b400141324c550168462b30db220edb31`, TS `ed02064`, SDK 2.1.0;
- env `DATA_LAYER_V2_MARK_WAKE_READ_DELAY_BY_VENUE=BINANCE=100` is the one tuned variable of G2;
- the `TS_DELIVERY_TRACE_*` env from the G2 windows is still set but expired (until 1791639603470577388). The trace
  refuses to rearm after its expiry.

Ingestor configs come from `~/.local/state/qdl-v2/releases/v2.2.3-rc7-ingestor-liveness/`. They are mounted
read-only at `/runtime/config.json` (checked with `docker inspect`) and read through a `--network none` container:

| Ingestor | Config revision | Liveness ping/dead | sha256 |
| --- | --- | --- | --- |
| OKX | 20 | 1500/4000 ms | `eb982fac...` |
| Binance | 19 | 1500/4000 ms | `5833bc4e...` |

Neither config contains `book_liveness_proof_ms`, so IDR-040 fix 2 is OFF.

#### Gate Scorecard (G1-G14)

Gates come from the frozen TS table `upgrade/p184-phase-d-gates.md` (TS 31cf663, sha256 `c67bf69b...`) and the
frozen budget `config/v2/p184-stage100-budget.json` (sha256 `ff85335b...`).

Scoring rule: a PASS needs the table's measurement rule, which is steady state for at least 30 min at the stage-100
workload plus a burst window. A measurement outside that rule can FAIL a gate but cannot PASS it. "Observed" gives
such a value against the threshold. A NOT_MEASURED is never upgraded.

Summary:
- PASS: G5.
- FAIL: G2.
- DEFERRED: G13.
- NOT_MEASURED: G1, G3, G4, G6, G7, G8, G9, G10, G11, G12, G14.

Paths under `~/.local/state/`; sha256 prefixes, full hashes in scorecard.json.

| Gate | Rule | Measured | Source (sha256) | Verdict |
| --- | --- | --- | --- | --- |
| G1 BOOK e2e to TS cache | p95 <= 500, p99 <= 1000, max < 2000 ms | Window 2: DELTA B/O p95 126.5/130.0, p99 163.0/170.5, max 510.9/501.8 (n 24664/26105); SNAPSHOT B/O p95 152.4/155.5, max 294.0/353.1. Window 1 similar (max 528.1). Observed INSIDE | `qdl-v2/releases/v2.2.3/trace-transit/feed-transit-1791639603470577388.json` (e9e82309), `...-1791638906937534999.json` (d8d0d6c3) | NOT_MEASURED: writer-only 2 x 535 s, no stage-100 load |
| G2 TRADE/QUOTE/MARK new-event transit | TRADE p95 <= 500, max < 1500; QUOTE/MARK p95 <= 500, max < 1000 ms | Window 1, final images: **TRADE BINANCE p95 539.1, max 1078.7 (n 21983)**. Window 2 (deployed config): TRADE B/O p95 269.7/158.9, max 623.9/394.0; QUOTE B/O p95 152.2/154.0, max 516.9/513.4; MARK B/O p95 339.3/317.9, max 708.0/495.2 | same two files; `p184-g2-20261010/g2-mark-*.json` (292256e8, 3da349c4); stage100m receipt.json (9dfce5d9) | **FAIL** |
| G3 BAR e2e from close | p95 <= 5 s, max < 60 s | The trace has no BAR records. Related, alpha SDK streams (not the TS cache): BAR 1m close -> usable max 3095 ms Binance, 1511 ms OKX | stage100m receipt.json (9dfce5d9) | NOT_MEASURED |
| G4 venue -> canonical | candidate p95 <= baseline p95 x 1.2 per row | 16/17 frozen rows PASS in all 4 windows (e.g. trade B/O 105.7/110.0 vs 269.3/257.4; mark OKX 1453.1 vs 1586.6). bar BINANCE 30m: no close in any window. 5 observed rows have no baseline. Inverse is pooled into OKX | `p184-d3-capacity-20261009/g4m-{1..4}-summary.json` (2c98891f, c6f321b9, b349e8c4, 6955061d); budget (ff85335b) | NOT_MEASURED: missing coverage blocks the gate |
| G5 request latency | certified p95/p99 per class and venue; COLD_2500 <= 6265.1, COLD_5000 <= 15267.0 ms | All 10 class/venue rows PASS (worst p95 89.4 OKX BAR_LATEST, p99 257.6 OKX L2). Cold B/O 2500 3124/3727 ms, 5000 5299/6178 ms (n=1 each). 1800 s stage 100m, 32/32 driver gates | `p184-d3-capacity-20261009/stage100m/gates.json` (c65db308), receipt.json (9dfce5d9) | **PASS** |
| G6 durable-view event age + quality | per-binding D3 age thresholds; zero unsafe eligibility | Not recorded per binding. Related: 182709 requests with 0 failures or refusals; 180 streams with 0 errors | stage100m gates.json (c65db308) | NOT_MEASURED |
| G7 TS consumer path | SDK -> writer p95 <= 100; writer -> Redis p95 <= 50 ms per feed | Window 2, SDK -> writer p95: BOOK_DELTA 29.5, BOOK_SNAPSHOT 37.8, QUOTE 28.6, TRADE 41.5, MARK 0.6. Writer -> Redis p95: 8.9, 13.3, 4.0, 26.2, 6.8. Window 1 TRADE writer -> Redis p95 36.9. Observed INSIDE | trace-transit files (e9e82309, d8d0d6c3) | NOT_MEASURED: writer-only windows |
| G8 TS execution Redis | commandstats <= 60 % core per 10 min; SLOWLOG 0; PING p99 <= 5 ms | One read-only 600 s window, 15:06-15:16Z, writer-only traffic: commandstats 0.065 core (EVALSHA 23.5 s, EVAL 9.7 s); SLOWLOG new entries 0 (threshold 10 ms); PING p50/p95/p99/max 0.318/0.528/0.942/5.279 ms (n 5971); 0 MONITOR clients. Observed INSIDE | `qdl-v2/releases/v2.2.3/g8-redis/g8-window-600s.json` (fbc269fd) | NOT_MEASURED: not under full mixed traffic |
| G9 readiness | demanded == session-ready; 0 data-path eligibility losses; v1 fallback 0 | ts:ready 154/154 samples, 0 unreadable, READY 64/64; v1_fallback_calls 0; writer MARK errors 0 in 32/32 min. Eligibility losses by cause: not counted | stage100m gates.json (c65db308), `guard-s100m.log` (cef88fa8), `mark-watch-s100m.log` (f905c281) | NOT_MEASURED |
| G10 TRADE integrity | 0 gaps, 0 dup applied, 0 replay-expired, 0 overflow | No production counters read on the final digests. DR08 isolated bounded queue closes at 1000 events with typed replay | g13 dr08 evidence.jsonl (a7bf8d01) | NOT_MEASURED |
| G11 writer restart recovery | first valid apply <= 30 s; caught up <= 60 s | Four timestamps not recorded for the writer. DR08 DL consumer restart -> first event 5.07-5.14 s, n=5 (not the TS writer) | g13 dr08 evidence.jsonl (a7bf8d01) | NOT_MEASURED |
| G12 broker/projector/Stream recovery | caught up <= 600 s, then G1-G3 | The isolated PASS (broker 0.202 s, projector 0.256 s, Stream 2.355 s; BOOK only) ran the predecessor core/projector/Stream/Query/writer images. Not inheritable, see Known Limits | `p184-g12-20261010/evidence/verdict.json` (7e98572c) | NOT_MEASURED on final digests |
| G13 DR01-DR08 | every cell once on the final digests | 41/41 DL-side cells harness-ok (2 of them NOT_OBSERVED quiet cells). 4 steps deferred to TS | `p184-g13-20261010T134219Z/summary.json` (b64f35e1), `...T142735Z/summary.json` (24d659c5), `...T142914Z/summary.json` (eb6ae4e2) | DEFERRED |
| G14 resources | CPU/RAM per role at stage 100, with headroom | 16 roles from 152 samples. Mean CPU: kafka 77/95/98 %, query 68/48 %, writer 124 %; DL total about 5.0 vCPU. Max RSS inside every limit. binance_bar_edge and stable_redis not sampled | `stage100m/g14-resources.txt` (6b1fdcab) | NOT_MEASURED: role coverage incomplete |

Findings behind the verdicts:
- **G2 FAIL.** Both windows ran the final DL and writer images. The only difference between them is the Binance MARK
  read delay (250 ms in window 1), which does not act on TRADE.
  - The TRADE BINANCE excess is in the ingest -> SDK return segment: p95 434.5 ms, max 1018 ms.
  - The other two segments are small: venue -> ingest p95 36.9 ms; writer, SDK -> Redis reply, p95 85.0 ms.
  - G4 puts venue -> canonical visibility p95 at about 105 ms, so the tail sits between canonical visibility and
    the SDK return. That is Stream dispatch/queue or SDK receive; not yet attributed.
  - Under stage-100 load the alpha SDK streams show the same shape: TRADE BINANCE per-stream p95 up to 872.5 ms, max
    1402 ms.
  - Not a threshold question: the gate is frozen.
- **G1/G7** are inside both windows by a wide margin, but only writer-only traffic was traced. The trace caps one
  activation at 600 s, so a 30-min stage-100 measurement needs several activations during the run.
- G4 bar latency for OKX is measured from bar open (the OKX source time is the open). The same metric was used for
  the baseline.

#### Items Closed (commit)

| Item | Commit(s) | State |
| --- | --- | --- |
| IDR-039 one-commit images for every application role | rc7d rollout of bbfb3be | runtime: 12/12 roles on bbfb3be |
| IDR-062 BOOK_SNAPSHOT on top-of-book change | 43bbcf0, 63238bf | in images |
| IDR-040 fix 1 REST refresh ahead of stream (`REST_AHEAD_OF_STREAM`) | 4eb08dc | in images |
| IDR-040 fix 2 in-band quiet-book proof (`proof_observed_at_ns`) | 1a2daf1 | in images, ingestor flag OFF |
| IDR-002 binding V3 sealed request policy; IDR-029 quota log | 4eb08dc | in images |
| D2 tolerant SDK response models (SDK 2.1.0) | eb7c649 | SDK |
| IDR-075 OKX inverse BTC-USD-SWAP Paper entitlement | 704552d, 76db472; D1 rev11 applied on readers (plan 8c90ddc) | runtime: TS manifest rev 11 |
| IDR-076 coherence acceptance on the TS product path | 1cd24e3 (decision) | TS-owned |
| IDR-082 canonical `event_id` on the public view | 616342c | in images, SDK 2.2.0 |
| IDR-084 execution MARK takes the newer committed hot record | 922a6ec; bd74faa; rc4 regression fix ec89a31; real-ordering tests 4e19966 | in images |
| Astra R1 hard hot refusals not downgraded; R3 one MARK hot budget; R6 bounded refusal lines | 57881a2 | in images |
| Astra R2 Stream hot lookup per key partition | fdd379e | in images |
| Astra R4 absolute inbound deadline (silent socket) | d4f5ec9; liveness knob 0f30e17 | in images, config 1500/4000 |
| Astra R5 G5 same-replica queue | 5c81c25 (SDK random start), 65e0757/76950c8 (cold read), 7f974c3 (attribution), 47bec04 (replica redirect) | in images, G5 PASS |
| Astra R7 release discipline | this record | see Known Limits |
| Codex review: TS readiness sampled on the configured writer; run-scoped supervisor | 713aa90 | tooling |
| Codex review: rc5 ordering test | 4e19966 | tests |
| Codex review: no funding settlement allowance before measurement | 530a406 (reverts the allowance of a2184f4; keeps item diagnostics and the cache re-read) | in images |
| OKX make-before-break on 64008, session lineage | f861663 | in images |
| OKX WebSocket port 8443 -> 443; plan load accepts old URL | 6674d0b, f69353b | in images, config on 443 |
| Stream terminal errors across stalled handoffs; per-subscription counters | 0b374bc, b6a0119 | in images |
| Cursor keeps standard-base64 Kafka topic UUID | 47d906e | in images |
| DR05 invalid-book fence | d68fb2f | in images |
| CI convergence (cargo fmt, DL-only test skips, vnstock opt-in) | d6fb34c, 2c84e2f, 54460ac | in images |
| SDK 2.2.0 release (start_index, event_id, proof_observed_at_ns, item diagnostics) | 359c9ee, a2184f4 | wheel below |

#### SDK 2.2.0 Wheel

Built by `scripts/build_qdl_sdk_release.py`. The builder image was `sha256:e0252f4c...`, run with
`--network none --cpus 1` as a non-root user. The source was a `git archive` of bbfb3be, extracted separately for
each build.

| Field | Value |
| --- | --- |
| wheel | `qdl_sdk-2.2.0-py3-none-any.whl` |
| wheel sha256 | `d8226f509bce4ae408cacfa30e12f6160726e4040751d2e4f7379d3dc71c3997` |
| reproducible | two builds byte-identical (`cmp`) |
| sdk_source_digest | `63e230312cec966b51bfa20aedd3e73c51c838c1a52b778c24574f61b7399488` |
| generated_contract_digest | `ae77dc42074fd7cc896a46bde3282837a51b667acee23c4957d37565690856e9` |

All 21 wheel files are byte-identical to the deployed Python image (`/app/qdl_sdk`, `/app/generated/python/qdl`).
The artifacts are under `~/.local/state/qdl-v2/releases/v2.2.3/sdk-build-{1,2}/`, outside Git.

#### CI

Local exact-source CI (`scripts/p184_local_ci.py`, archive of bbfb3be), first run:
- summary sha256 `23a49251...`;
- contract-tests, sdk-python310, kn-native-integration and consolidated-source-preflight PASS;
- unit-tests FAIL on one step only, the trivy image scan, because of a runner flag bug fixed in 259aeb9 (boolean
  `ignore-unfixed`, repo `.trivyignore` mount). The secret scan and SBOM steps after it were skipped.

Rerun of the unit-tests job with the fixed runner:
- summary sha256 `07df25eb...`;
- 21 steps PASS, including the unit tests, the trivy image scan, the secret/misconfiguration scan and the SBOM
  rehearsal;
- 1 SKIPPED: "Prepare non-root runtime bind mounts" needs sudo; host preparation is reported, never escalated;
- receipt status `INCOMPLETE` only because of that skip.

Remote GitHub CI did NOT run, because the DL repository is not pushed. Publication requires the required jobs on
the final head.

#### Runtime Changes Performed (2026-10-08 .. 2026-10-10)

Each change was owner-approved and recorded in `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`. Each role was
recreated one at a time.

| Change | Rollback reference |
| --- | --- |
| Bar edge to `514506122111` (2026-10-08, see appendix) | prior Codex evidence dir |
| D1 rev11 on readers: TS Paper manifest rev 11, bundle f04ba6dc, 64 products incl. inverse | plan D1 entries |
| D3 window B: alpha rev15 quotas (4950 RPM / 300 streams); Stream `QDL_KN_MAX_SUBSCRIPTIONS` 384 -> 1024 | `p184-d3-capacity-20261009/readers/*-rollback.compose.json` |
| Query/Stream release candidates rc3-rc7 (rc4 rolled back to rc3 after a regression) | plan entries per candidate |
| OKX ingestor config to port 443 (config_revision 20) | backup `ingestor_okx_swap.json.pre-okx-port443-1791619322166921472` |
| rc7c all roles + ingestor liveness configs | as below, one step back |
| **rc7d (bbfb3be), all 12 application roles, 12:02-12:08Z 2026-10-10**, writer READY 64/64 after every step | `p184-d3-capacity-20261009/rollout-rc6/<role>-rollback.compose.json` -> rc7c python `sha256:55e493786748...` / rust `sha256:206ae872a5fe...` (54460ac); log `rollout-rc7d.log` |
| TS writer image 15524c0b (9b86ed3) -> 34202753 (ed02064) | previous image `sha256:15524c0bea8c...`, same env/mounts |
| TS writer `DATA_LAYER_V2_MARK_WAKE_READ_DELAY_BY_VENUE=BINANCE=100` (13:30Z) | `trading-system/p184-rev11-window/g2-tune/rollback-20261010T133013Z.compose.json` (a7299021) |

Rollback runs in reverse role order. Kafka, Redis, realm dirs and offsets need no data migration. Rollback is for
failure only.

#### Known Limits

- **G12 is isolated and not inheritable.** The run was same-host, single projector/Stream replica, TEST_ONLY BOOK
  at 4 Hz, with G2/G3 NOT_MEASURED. It ran predecessor images:
  - core 658a9570 (0e00c81);
  - projector a696cfba and Stream 1f13408f (c8015ca);
  - Query 672503b7 (76950c8);
  - writer 15524c0b.

  Code changed since on every recovery path it exercised (`git diff --shortstat <rev> bbfb3be`):
  - core +959/-30 (IDR-062, IDR-040 fix 2, the invalid-book fence);
  - Stream +1020/-147 (Astra R2, terminal errors across stalled handoffs);
  - projector +39 (invalid-book fence);
  - Query.

  G12 on the final digests needs a rerun with the image set changed. In `scripts/p184_g12_isolated_fault.py` it is
  a module constant.
- **IDR-040 fix 2 is OFF.** The ingestor flag `book_liveness_proof_ms` is unset and the renderer does not emit it.
  A quiet Binance book stays stale, never falsely fresh. Ping cadence and marker volume are not measured.
- **Catalog revision split (C.19).** The repo and image catalog is revision 11, which the bar edge checkpoint reads
  from the image. Query reads the runtime-mounted revision 12 (`releases/v2.2.0-02cd827/runtime/stable-source-bindings.yaml`,
  sha256 `fc3a1e1e...`, includes BTC-USD-SWAP). This split is deliberate. Unifying it changes the acquisition plan
  revision and needs its own packet.
- **Funding settlement allowance: PENDING evidence.**
  - The stage-100g PARTIAL_RESULT (OKX SOL funding 7.1 s after an 8 h settlement) has a probable root cause: age
    measured from the newest fundingTime before OKX publishes the due row.
  - The 60 s allowance was reverted (530a406); the release ships without it.
  - A read-only publication-latency poll (15:58-16:04Z, `~/.local/state/qdl-v2/funding-publication-20261010/`) is
    PENDING. The coordinator appends its result.
  - Until then a funding read right after settlement can be refused DATA_STALE. That refusal is typed, never a
    stale success.
- **Cells deferred to the TS release:** dr01-deployed, dr01-wrong-account (IDR-085 fix in TS be21d49),
  dr02-dr03-alpha and dr07-reference. Gateway, alpha and risk are not running. These cells stay OPEN.
- **G4 rows not measured:**
  - bar BINANCE 30m (no close in the windows);
  - BAR BINANCE 5m/1h and OKX 5m/15m/1h have no frozen baseline;
  - OKX inverse has no separate cells.
- **G13 quiet cells:** DR05.QUIET_TYPED BINANCE/OKX are ok by the cell rule but NOT_OBSERVED, because no book was
  quiet in the 300 s window. They are not a positive quiet-state proof.
- **G2 trace coverage:** the writer trace dropped 158 and 87 records at export, and records without a venue clock
  are counted and excluded:
  - TRADE/QUOTE from Query/warmup reads;
  - the first records after a resubscribe.
- Historical failures stay recorded in the plan: stage 100/100b/100c/100e/100f/100g/100j, the rc4 regression, the
  rc7 Query crash and the first G13 attempts. None is erased by a later PASS.
- Same host throughout: no HA, no independent failure domain, no mainnet or funded claim.

#### Consumer Hand-Off

- **Alpha callers** mint JWTs for manifest revision **15** (alpha.binance/okx.paper.stable rev 15, 4950 RPM / 300
  streams). The readers run rev 15, so an older JWT revision gets 401.
- **Trading System**:
  - Paper manifest/JWT revision 11 (64 products incl. OKX BTC-USD-SWAP) is live.
  - Adopt SDK **2.2.0** (wheel sha256 `d8226f50...`). The writer runs 2.1.0 today, which ignores the new fields.
  - Phase E uses `event_id` (IDR-082) for trade identity.
  - The IDR-085 gateway fix (TS be21d49, scope lookup after the API key) ships with the TS release, which closes
    the deferred DR01 cells.
  - The writer keeps `BINANCE=100` for the MARK wake read delay.
- The G2 TRADE tail between canonical visibility and SDK return is open. It must be attributed before G2 can PASS.

#### Publication Requires

1. The owner resolves the G2 FAIL and the NOT_MEASURED gates, or explicitly accepts them as release limits. This
   record does not accept them.
2. The funding publication-latency result is appended.
3. Push, remote CI (contract-tests, sdk-python310, unit-tests, kn-native-integration) on the final head, and merge
   to dev and main.
4. An annotated tag `v2.2.3` that links the final CI runs and the hashes of scorecard.json and this file.

No push, merge or tag was performed.

### 2026-10-08 HOLD draft (verbatim, superseded by the records above)

Status: `PREPARED_RELEASE_HOLD_NOT_PUBLISHED`. This record is not a certificate,
Git tag, deploy authorization or proof that the feature HEAD runs on production.
The published stable release remains v2.2.2.

#### Inherited Runtime Change

Owner-approved bar-edge-only recreate at 2026-10-08 02:17:33 UTC:
`f7351c3b` -> `514506122111df8992a5dfac9dc9a3db7ccae202a398424f4dd5a033d2ddf9a9`
(`qdl-v2-python:2.2.2-d6d2637`). This removes the old endless Binance3d history
retry storm using the already-built implementation. No additional role changed
in this continuation. No claim that Binance3d history gaps have been repaired.
Exact rollback compose remains in the prior Claude private evidence directory.

The three cores remain `658a9570`. Owner moved coherence acceptance to TS's
bounded product read path (IDR-076). Prior paired core trial remains historical
FAIL/rollback; core candidate `0d8da150` is explicitly excluded from this release
proposal. No new core is needed merely to pass the revised coherence gate.

#### Busy-Window Evidence

2026-10-08 03:21:03-03:26:03 UTC, 17 DL roles, 31 samples/30 intervals over
299.992s: mean **4.882532**, p95 **5.564052**, max **5.870669 vCPU**. The existing
300s mean<=5 gate passes this observed window; p95/max do not stay below5.
No peak-capacity or every-interval<=5 claim. TS CPU is measured separately.

Simultaneous log-end rates: canonical952.48/raw826.28 offsets/s over302.110s;
both exceed all three prior baselines (canonical +47.7% to70.1%, raw +56.5% to97.2%).
Canonical offsets include transaction markers, not payload-only events. Bar edge
mean0.016960vCPU, no restart/OOM or image/config changes during observation.

**Retained finding:** active Stream overflow+2, replay+15901, subscriptions+3.
Two queue-full closures are TS Paper BTC TRADE, one Binance and one OKX, each
queued1000. A subsequent Binance replay closes OUT_OF_RANGE after10000 replayed
records. This does not establish data loss or an endless loop, but it precludes
claiming fully qualified burst recovery. One TS Binance BTC MARK timeout also
appears in that window. Passive Stream counters remain unchanged.

#### IDR-075 Preparation

Pure preparation reuses deployed catalog identities and preserves all60 existing
V2 products, all prior manifest requirements, quotas and fail-closed policies.
Adds only OKX BTC-USD-SWAP QUOTE, MARK_INDEX_PRICE, BOOK_SNAPSHOT and BOOK_DELTA,
with USD quote and BTC settlement identity. No TRADE/BAR acquisition or grants.
BTC-USDT-SWAP MARK_INDEX_PRICE already exists; its INDEX component supplies the
typed FX path, not a new standalone INDEX feed or a USD=USDT assumption.

Target: Paper manifest11/JWT11, bindingV3 release23,64 products. The matching
binding hash and environment projection are in `release-preparation.json`.
Runtime remains manifest10/bindingV2r22/JWT10. Prepared files are outside runtime;
no token, mount, reader, writer or account was changed. No live/sandbox grants.

#### Publication Gates

DR01-DR08 and broker delta receipts remain mandatory, independently of Paper
feature completion. Retain the new overflow/replay finding for affected recovery
qualification. IDR-075 needs one approved paired reader/caller packet and actual
Paper-identity no-order reads before activation is claimed. Final release must
reconcile source, immutable images, schemas/configs, CI and owner-authorized
publication; never relabel this prepared inventory as a completed release.

Evidence: `~/.local/state/qdl-v2/p184-codex-release-20261008/{cpu,idr075}/`.
The main-plan entry and [Phase C packet](../../p184-dl-phase-c-packet.md) govern
the next action. No other role may change without its owner-approved packet.
