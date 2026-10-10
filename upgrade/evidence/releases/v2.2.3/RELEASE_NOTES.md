# Quant Data Layer v2.2.3

Status: `PREPARED_NOT_PUBLISHED`. This is not a published release, a Git tag or an owner publication decision.
Publication needs the owner to push and tag. Remote CI must also run on the final head. The published stable
release is still v2.2.2.

Scorecard: [scorecard.json](scorecard.json). Inventory, hashes and the SDK build: [release-preparation.json](release-preparation.json).

## Scope

This release closes the P18.4 phase D Data Layer work on the Kafka-native stack. It contains:
- one-commit images for every application role (IDR-039);
- execution-view correctness fixes (IDR-062, IDR-040, IDR-084, Astra R1/R3);
- hot-path contention fixes (Astra R2/R3), the OKX session fixes and the ingestor liveness knob;
- the G5 warmup fixes;
- additive public fields: `event_id` (IDR-082) and `proof_observed_at_ns` (IDR-040 fix 2), both in SDK 2.2.0.

Public endpoints keep their versions. Every change is additive, and readers on SDK 2.1.0 ignore the new fields.

This is not a Trading System order, accounting or alpha certification. It is not HA or independent-failure-domain
evidence, because every run used the same host.

## Source And Images

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

## Gate Scorecard (G1-G14)

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

## Items Closed (commit)

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

## SDK 2.2.0 Wheel

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

## CI

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

## Runtime Changes Performed (2026-10-08 .. 2026-10-10)

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

## Known Limits

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

## Consumer Hand-Off

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

## Publication Requires

1. The owner resolves the G2 FAIL and the NOT_MEASURED gates, or explicitly accepts them as release limits. This
   record does not accept them.
2. The funding publication-latency result is appended.
3. Push, remote CI (contract-tests, sdk-python310, unit-tests, kn-native-integration) on the final head, and merge
   to dev and main.
4. An annotated tag `v2.2.3` that links the final CI runs and the hashes of scorecard.json and this file.

No push, merge or tag was performed.

## Appendix: Preparation History

### 2026-10-08 HOLD draft (verbatim, superseded by the record above)

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
