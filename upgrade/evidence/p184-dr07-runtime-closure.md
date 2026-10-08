# DR07 Runtime Closure Checkpoint - 2026-10-07

Status: TRIAL_FAILED_ROLLED_BACK / RELEASE_HOLD. This is not a release certificate.
Main plan: DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md, DR07 runtime closure continuation.
Guide: DATA_LAYER_V2_KAFKA_NATIVE_ARCHITECTURE_REVIEW.md sections18.13-18.14.
Evidence root: /home/bobby/.local/state/qdl-v2/dr07-runtime-20261007/.
The pre-trial checkpoint below had no production mutation. The later owner-approved
core-only image trial and reverse rollback are recorded at the end; no other
role/config/credential/manifest/cap or order changes.
No TS Paper feature dependency; DR01-DR08 and broker delta remain release inputs.

## Actual Current Runtime Baseline

`audit/baseline-20261007T163940Z/summary.json`, sha256
`6902bd0c96e2ec1e58a08b944bc13fe412e1bea0c60d769ae79d434a4c8ca192`.
Read-only cgroup observations, 16:39:40.979-16:44:41.089 UTC, 31 samples,
30 intervals /299.996 seconds. Build and replay started AFTER this window.

| Measurement | Result | Verdict / boundary |
| --- | --- | --- |
| Full DL, 12 application +5 infrastructure roles | mean5.779832, p955.978048, max6.012644 vCPU; n30 | FAIL existing5vCPU gate; old runtime, not candidate |
| Five non-overlapping60s averages |5.801806 /5.722398 /5.794909 /5.817657 /5.762390 vCPU | all above5 |
| TS writer CPU, separately |mean1.461520 vCPU; n30 | excluded from DL total, not hidden |
| Three Kafka brokers combined |2.885854 vCPU | measured, not attributed solely to L2 |
| Three cores combined |1.181349 vCPU | measured old cores |
| Two projectors combined |0.280874 vCPU | measured |
| Stream overflow increments |0/0, approximately290s, n30 metric samples each | observed clean window only; historical replay loop NOT closed |
| Active Stream delivered |119727 events /50 subscriptions | not a stress capacity certificate |
| OOM/restart delta |0, observed roles | startup/process health alone is not execution acceptance |
| TS replay/reset deltas |NOT_MEASURED | Docker logs empty; actual mounted log has no timestamped entries in window |
| Kafka lag trend |NOT_MEASURED | one partition snapshot after window, not continuous convergence proof |
| Candidate CPU / canonical-to-production-Redis ACK |NOT_MEASURED | candidate not deployed; no substitute from isolated tests |

All canonical roles stayed on their previous images. Initial raw Mounts array hashes
were order-sensitive; preserve originals and see mount-order-correction.json.
Eight repeated inspections had identical normalized mounts, IDs/config/start times.
No mount/config drift established. Do not rewrite the original audit as a PASS.

Kafka memory.events.max increased2294/1651/1770; no OOM. Follow-up memory PSI
avg10/60/300 was0.00. A memory-limit event is not proof of a GC or pressure root
cause; no resource/durability/timeout changes have been inferred from this.
Latest completed quota minutes943/1003 of1500 before the probe; headroom is not
reserved capacity and must be re-read immediately before any same-identity calls.

## New Candidate And Paired Real Capture

Frozen application source `c33c90f993049f06742a7d29a9dfcb017c588c9b`, containing
core optimization63238bf. Built once with tracked Dockerfile, pinned bases and
CARGO_BUILD_JOBS=1: `qdl-v2-rust:2.2.3-rc3-c33c90f`, immutable digest
`sha256:0d8da150820ed684bda084e92f714537cb40bd81e2e3a6bc93fa9a4b33538411`.
All12 packaged executables found; no missing dynamic libraries. This is NOT rc2.
Python application paths have no diff43bbcf0..c33c90f; existing Python rc2 reused
only for capture/test tooling. It does not pretend to bear the new source label.
Future release attestation must explicitly bind unchanged Python bytes to source.

Command: `python3 scripts/measure_l2_top_materialization.py run --work
<evidence>/paired-rc3 --seconds60 --candidate-image sha256:0d8da150...38411`
(use full digest above and `--seconds 60` when executing).
One pair, same real provider bytes, same config, baseline core658a9570 versus
candidate0d8da150; each bounded1CPU/256MiB. No shared Kafka writes.
Receipt sha256 `360c9bc5c8ae8164c8ed26d697648ba6759d09b4a458bf25c69aadf8f13e47ae`.

| Metric | Baseline | Candidate |
| --- | --- | --- |
| Core observed CPU seconds over60s capture |3.59 |3.01 (-16.16%) |
| Core CPU per capture second |0.059833 vCPU |0.050167 vCPU |
| Core sampled RSS high water |18420 KiB |18424 KiB |
| Resource samples |101 |101 |
| Binance ETH snapshot/s; protobuf bytes/s |0.984580;7802.45 |1.468527;11630.93 |
| OKX ETH snapshot/s; protobuf bytes/s |1.013289;8320.25 |1.511628;12403.24 |
| Binance next verified snapshot source-time delay p50/p95/max, n49 |410/920/924ms |0/0/0ms |
| OKX equivalent, n53 |400/900/900ms |0/0/0ms |

Source-time delay is NOT wall-clock consumer latency. Candidate emits on the
same source event that changes top; real Kafka/Query/consumer transit remains.
586 Binance +602 OKX deltas have identical payload chain hashes in both runs.
Capture frame counts592/603; windows59.924/60.2s.49/53 top changes, no uncovered
tail on either candidate binding. This is not the previously measured busy window
(135/156 changes/minute); do not relabel it peak-load acceptance.
Snapshot count multiplier approximately1.492; all BOOK message multiplier1.045.
Those are downstream workload estimates, not projector/Query/Stream CPU measures.

Core savings cannot certify whole-stack5vCPU. Old non-core remainder is4.598483vCPU;
ignoring every downstream/workload change, cores would need approximately66%
less CPU to reach5. The paired replay is16.16% lower. This arithmetic is a
scenario, not a runtime forecast and not grounds to waive the gate or raise caps.

## Observer Safety And Verification

Fixed kn_production_watch default: alert only, no automatic production stops based
on historical KN role names. Optional stop requires distinct full container IDs
matching current eligible roles; no implied rollout authority.
Measurement harness accepts an explicit immutable candidate digest and checks
it before side effects. These are tools, not application contract changes.
Tests in existing Python rc2 image:16 PASS,0fail/skip. Host attempt first ran6
watch tests but could not import capture test (host lacks httpx); retained as an
environment error, not disguised as16 host passes. No dependency installation.

## Remaining Closure Gates

1. Runtime admission: Phase C still requires old stack<=5 before rollout. It
   fails. Proposed decision is a bounded candidate trial with named rollback,
   NOT waiver of final5vCPU or silent change of the rollout packet. Needs explicit
   resolution before action. Existing core-first/coherence117/120 per venue and
   continuous lag safety checks remain unchanged.
2. Source/caller convergence: TS58287fc +19d77e2 remain on codex/dr07-reset-anchor.
   No recorded Claude integration acceptance found; do not overwrite its Paper
   worktree or deploy an unreviewed union. Current shared writer is9150385e2bb3,
   actual SDK2.0.7, bindingV2r22/JWT10; SDK2.1.0/V3 remain candidate.
3. Runtime recovery: observe exact matched reset/anchor/ACK/cursor sequence on
   converged actual writer; zero new overflow in an idle window alone cannot
   close the historical overflow/replay defect.
4. Latency: retain NOT_MEASURED canonical commit and actual Redis ACK until those
   clocks/identity are joined on deployed writer. A bounded real Query-to-isolated
   Redis probe is useful affected-path evidence but cannot close this runtime gate.
5. Release inputs: no newer accepted case-indexed DR01-DR08/broker-delta packet
   was found. Static surface93/93 is not a substitute. Owner removed dependence
   on building Paper features, not on these data/broker checks.
6. Final immutable source/image/config/receipt convergence and scoped cleanup.
   No push/merge/release, no broad catalogue/C2 run to hunt these defects.

## Cleanup And Runtime Identity

Paired capture/test Kafka/core/client/network and private test TLS removed;
capture hashcbbddcf85b016d771435c0d2000ea221cacd023d888e9219af127719bd36f499
retained, raw capture deleted. Runtime inventory unchanged. Pair disk available
89,861,836,800 ->89,632,673,792 bytes (whole-host concurrent activity, not claimed
reclaim). Build context24,557,465 logical bytes removed after provenance:
89,559,764,992 ->89,587,806,208 available bytes. No volume/shared-state cleanup.
Inventory93 images28.31GB; BuildKit248 records18.11GB. No broad prune: shared
builds and active/named rollback/candidate artifacts retained pending closure.
DL canonical /home/bobby/data_layer main5bf5cd5 (v2.2.2 ancestry), feature
/home/bobby/.worktrees/data-layer-p184-closure fix/p184-dl-closure.
TS canonical /home/bobby/trading_system fix/dev-consumer-recovery-integration668e103;
Codex TS worktree remains separate. Old DL core658a9570, projector a696cfba,
Stream1f13408f, Query51450612, ingestors0f6876e1, BARf7351c3b unchanged.
Full immutable rollback roster remains in the Phase C packet and inventory JSON.


## Actual Owner-Approved Core Trial And Rollback

Owner explicitly approved gate-order change after this report's first checkpoint.
Plans committed4da52a8/60a3cf7 BEFORE lifecycle calls. Current-load transition
boundary6.5758528vCPU (two30s windows) and final5vCPU are distinct; no resource cap
was raised. Three cores rolled serially17:46-17:50UTC to0d8da150; private inspected
compose preserved effectiveconfig/mounts/TLS/aliases/caps, receipt checks passed.
Quorum leader2,epoch149,followerlag0; under-replicated partition list empty before
action. One subsequent admin read showed raw lag14-31records across6partitions,
canonical projector19-106. Continuous five-increasing-interval lag was NOT
collected: operational sampling was incomplete, do not claim that gatePASS.

Frozen post-core gate via deployed TS MarketCacheReader, ETH each venue,1Hz:

| Result (n=120 each venue) | Binance | OKX |
| --- | --- | --- |
| Exact BOOK top equals QUOTE, both eligible at read |103/120 |105/120 |
| Gate>=117/120 |FAIL |FAIL |
| Stored BOOK observed_at age p50/p95/max |654.197/1104.025/1218.918ms |652.626/1119.492/1276.319ms |
| Stored QUOTE observed_at age p50/p95/max |181.010/426.622/767.293ms |192.039/362.477/880.194ms |
| Cache reader call p50/p95/max |16.412/150.317/249.192ms |15.700/155.352/246.358ms |

These are TS Redis read and stored-view-age measurements, NOT SDKrequest latency,
canonical commit latency, Redis ACK or alpha latency. Gate failed, so no second
window was used to replace it. Do not diagnose the residual stage from age alone.
BOOK and QUOTE remain independently updated streams. Core-only improvement did
not prove their end-to-end alignment; matched event/offset trace is still needed
at canonical, Stream, SDK and TSwriter before deciding the next source change.

Core-only candidate CPU:17DLroles,19complete same-ID pre-decision intervals,
189.994570s, mean5.158006vCPU,p95/max5.405644. NOT a completed300s acceptance.
Measured cores combined0.325581vCPU; Kafka3.117959; projectors0.290687;
Query0.296130; Stream0.209910; BAR0.551557. Workload differed from earlierbaseline,
so before/after CPU is not a controlled causal performance ratio.
Full300s recorder continued across rollback and its original aggregate failed
on a missing cgroup usage key during recreate. Raw31samples retained. Segmented
report excludes11mixed/post-decision intervals and interpolates nothing:
core-trial/candidate-cpu-segmented.json. This does not change failed gate status.

At17:56-17:57UTC rolled back3->2->1 to658a9570. Every rollback config checkPASS.
Final12DLapp+TSwriter13/13running, noOOM; unchanged9otherDLroles andshared TS.
Rollback consumer read5samples/venue: allQUOTE/BOOKfresh,4/5coherent each. This is
return-to-baseline evidence only, NOT coherence acceptance or fullTSqualification.

Observer v1 falsely flagged6stalls when generation reset processed counters;
corrected identity=(containerID,generation),5controlledchecksPASS. Segments
actually overlapped32.938s, maxsamplegap10.141s; no rawcollectiongap. Preserve
original alerts, do not call them application crashes. Initial coherence
observer incorrectly required quiet-session receipt fields on strict fresh
BOOK/QUOTE; corrected feed-specific predicate,10TEST_ONLYchecksPASS; old5sample
outputs retained and not used tocertify. No new markettimestamp or freshnesspolicy.

Stream1 overflow+4 beforefirstrollout, then0additional observed during trial/
rollback; replayed+23202 acrosswholewindow. Stream2overflow+0. Counters have not
proved successful reset/cursor/ACK closure; unchangedTSwriter lacks the pending
DR07 integration. Wholewindow approximately17:41-17:59, not a steady-state proof.
Canonical-to-actual RedisACK andalpha cache remainNOT_MEASURED. The isolated
QueryACKattempt made0providerrequests (config/harness/quota-preflight stops);
no substituted latency is reported. Three remaining original gates are open,
plus residualBOOK/QUOTEcoherence andTS DR/broker case-indexed releaseinputs.

Next permitted source work is attribution and coordinatedTSreset/ACK integration
in its own branch; no repeat imagebuild or release trial without an affected
fix. No Paperfeaturedependency, nounrelatedDLendpointchanges, nofreshnesswaiver.
Release and remaining9role packetHOLD. Allobserver processes/testresources stopped;
activeoldimages+explicitrc3candidate andnamedrollback kept for investigatedscope.
No push/merge/tag, noextraAPI/manifest change. Private rollbackpacket retained600
outsideGit. Reportedbuild/paircleanup above remainsvalid; no broadDockerprune.


## Actual caller rollout and exact-event receipt - 2026-10-07

Status: CALLER_DEPLOYED_OBSERVED / RELEASE_HOLD. This supersedes only the earlier
NOT_MEASURED statement for event-to-actual-TS-Redis-ACK, not the failed core trial.
No DL runtime role changed in this continuation. No push, merge or release.

### Source, packaging and runtime

TS branch codex/dr07-reset-anchor: 83131d4 corrects BOOK session-budget selection;
452955f adds bounded opt-in tracing to actual fixed/single workers. Bridge/reset
144 PASS, source trace suite 529 PASS, packaged image 491 PASS, all zero skip;
these overlapping suites must not be summed. Tests used disposable Redis; no
shared financial rows, credentials or order submission. Bridge counterfactual:
3/7 regressions failed before correction. Actual deployed market and BOOK budgets
are both 45000ms, so this selector is a latent mismatch fix, NOT attribution of
the observed runtime delay.

Only market_data_service_p184w was replaced at 19:15:49 UTC with
sha256:5849d7167561273af08bda2103c38cec5f4480f7a8809ea592c57abff7716ea1,
source 452955f7b9f32a2b757caf7d9bcfac73e3cf188f, six app files over current base.
SDK2.0.7, binding V2 r22, JWT10 and state/TLS/namespace/policies/caps retained.
No whole Paper upgrade. Explicit rollback container
market_data_service_p184w-dr07-rollback retains
sha256:9150385e2bb3596793b6ee209dbc99696c3ed07439d6f4199356f417a97b6105.
Old writer stopped after 30s ceiling with exit137, no OOM: do NOT certify graceful
shutdown. New writer restart0/OOMfalse. No concurrent active writer or cache reset.
Trace is bounded 4096 records/process with absolute expiry; four exports,
16384 records, one process omitted three in-flight records at cap. Expired trace
settings are inert; no extra recreate solely to remove them.

### Actual measurements (all latency in ms)

Exact canonical record joins require raw protobuf event-id SHA256, offset, feed,
source hash, instrument, generation, native sequence and all envelope clocks.
2556 exact matched ETH BOOK events, zero validation mismatches, all Redis EVAL1.
Canonical observer is read_committed, explicit assignment, no group join/commit.
Its observation clock is NOT the broker commit clock. Negative observer-to-SDK
intervals reflect independently scheduled readers; never label them negative
processing latency. Core normalized/received clocks were identical in the 701
baseline records, not independent stages. Stream dispatch clock NOT_MEASURED.

| Feed / venue | n | SDK return -> Redis ACK p50 / p95 / max | Event -> Redis ACK p50 / p95 / max |
| --- | ---: | --- | --- |
| BOOK_SNAPSHOT Binance | 169 | 57.893 / 136.682 / 360.574 | 168.772 / 275.901 / 1050.128 |
| BOOK_SNAPSHOT OKX | 173 | 58.736 / 191.736 / 557.328 | 171.002 / 319.197 / 1083.462 |
| BOOK_DELTA Binance | 1127 | 39.487 / 230.395 / 1335.608 | 155.282 / 474.117 / 1708.935 |
| BOOK_DELTA OKX | 1087 | 40.281 / 243.220 / 1470.926 | 159.590 / 440.066 / 1639.998 |

BOOK_DELTA SDK->writer p99: Binance650.2/OKX543.0ms. Writer duration p95
66.2/61.3ms, Redis apply wait p95 32.0/29.4ms. Tail is predominantly before
writer entry; batching/projection/preceding work are included. Without batch IDs
and SDK ACK clocks this does NOT establish a specific queue/ACK root cause.
Largest backlog events precede the resource sample window. 1192 unmatched trace
keys are bounded-capture/sampling coverage, not proof of loss; 348 rows lack a
stream-event join key (query/warmup). No extrapolation to alpha-cache ACK.

Preserved negatives: 85 ordered older-state no-write decisions, seven duplicates,
one MARK UNKNOWN/TIMEOUT. The OKX SOL generation104 timeout was followed by EVAL1
with the same identity/source/generation and advancing component clocks 1121ms
later. Subsequent recovery is observed; original ACK remains UNKNOWN.
Four MARK startup timeouts reconnected. In the subsequent 300s no-order window,
31/31 worker samples READY, session60/60; minimum execution-eligible47/60.
Session-ready is NOT all-prices-eligible. Cursor ACK23021->90457,
commits17772->69078, persistence errors0. This proves actual restart/progression,
not full deliberate reset-fault, zero-downtime or 5000event/s certification.

### Resource and continuous lag gates

DL unchanged stack: 17 containers, 30 intervals/299.992s,
mean5.303766/p95 5.502266/max5.545844 vCPU: FAIL <=5 gate. TS writer measured
separately mean1.323835vCPU, not included in DL sum. All sampled identities stable,
no new OOM. Kafka alone mean2.970528vCPU; no resource-cap changes.
Active Stream overflow3389->3389 and replay1891705->1891705; passive109->109 and
1138433->1138433 over the observed window. No loop observed, not a burst guarantee.

Lag union19:14:50.963-19:24:36.602UTC: five groups, 36 group/topic/partition
series, 60 observations each, 2160/2160 valid, unknown0. 473/2124 comparisons
increased, longest consecutive streak4; zero five-increase alarms, including
across file boundary. Maximum sample gap10.046s, process overlap4.318s. These
are read-uncommitted log-end minus committed offsets, not LSO/processing latency.
All bounded observer processes finished. No duplicate consumer/quota consumption.

### Evidence and remaining release boundary

Evidence root: /home/bobby/.local/state/qdl-v2/dr07-runtime-20261007/continuation/.
Commands and raw bounded outputs: writer_packet.py + writer-packet/roll-*.json;
writer-image-test/receipt.json; caller_resource_window.py + caller-window-300s/;
readiness_observer.py + writer-packet/readiness-300s.jsonl;
readonly_kafka_observer.py + writer-window-canonical-420s/;
writer-window-lag-union-final-1791401299658010600.json;
writer-same-event-preliminary/{summary,tail-attribution,outcome-denominators}.json.
Core candidate0d8da150 remains undeployed after failed trial; active three cores
remain658a9570. Do not substitute this old-DL-runtime CPU window for candidate
qualification or erase prior coherence103/120 and105/120 failures.

DR label warning: this runtime-hardening DR07 is NOT R2's DR07 reference-metrics
case family. TS case-matrix has65 DR cases (9/8/6/10/12/4/12/4), broker matrix11.
Read-only source inspection found no complete indexed DR01-DR08/broker receipt;
this is missing acceptance indexing/evidence, not a claim every case failed.
Broker-delta packet is still DRAFT; its Paper-completion prerequisite contradicts
owner's latest release boundary. Reconcile it with Claude's active ownership;
do not deploy its twelve-role packet blindly, wait for Paper features, or mark
these receipts PASS from the BOOK trace.

Next permitted work stays narrow: attribute BOOK/QUOTE alignment on exact frames
before retrying the failed core candidate; profile pre-writer batching if tails
violate the applicable feed contract, prove affected reset/fault path; qualify
candidate CPU <=5 and the existing coherence gate; collect/index actual DR and
broker delta evidence independently of Paper feature construction. No interface,
freshness, durability, quota or benchmark threshold waiver is implied.

### Cleanup and provenance

Disposable tests/Redis/networks removed; observer processes completed. Build
context (seven files) removed after source/image hashes saved. Free disk
93753360384 ->93753577472 bytes; concurrent host activity prevents exclusive
attribution. Docker inventory94 images/28.31GB, BuildKit444/18.29GB. No broad prune:
concurrent Claude cache ownership is not isolated; active image5849, named9150
rollback and existing explicit DL candidate/rollback retained. No shared volume,
financial data, cursor or raw provider state deleted. Bounded trace evidence
retained outside Git; private packet/inspect remain mode0600 for rollback.
Canonical DL /home/bobby/data_layer main5bf5cd5, published v2.2.2 unchanged;
feature fix/p184-dl-closure. Canonical TS /home/bobby/trading_system
fix/dev-consumer-recovery-integration@668e103 unchanged; active patch source lives
only in codex/dr07-reset-anchor. Claude's upgrade worktree was not modified.


## Claude continuation - CPU source, bar edge storm and product-path coherence - 2026-10-08

Status: CPU_GATE_OBSERVED_PASS_TWO_WINDOWS / COHERENCE_PRODUCT_GATE_PASS_CURRENT_CORE / RELEASE_HOLD (DR/broker
case receipts still open). Owner authorized this continuation; the single runtime change was approved explicitly.
Evidence root (outside Git): /home/bobby/.local/state/qdl-v2/p184-claude-dl-closure-20261008/ (cpu_window.py, roll_bar_edge.py,
coherence_attempts.py, cpu-*.json, rates-*.json, coherence-attempts-current-core.json, private/ mode 0600).

CPU attribution (read-only): Kafka per-thread CPU (/proc task stat) put most broker CPU in data-plane network/request
threads; per-peer socket segment rates (nsenter ss -ti, 20 s) showed inter-broker replication largest and
binance_bar_edge at ~850 segments/s on EVERY broker with ~50 KiB/s. canonical carries 511 records/s plus 193
transaction markers/s (core batch_wait_ms 25); raw has no markers. Bar edge log: Binance 3d history refused
OVERLAPPING_PROVIDER_WINDOWS attempt ~19,500, retry ~2.4 s, image 2.2.0-rc4-4fe7e92 without fba8863 (the v2.2.2
release record kept f7351c3b, recreated:false).

Runtime change (owner-approved, only role binance_bar_edge, 02:17:33 UTC): f7351c3b -> qdl-v2-python:2.2.2-d6d2637
sha256:514506122111df8992a5dfac9dc9a3db7ccae202a398424f4dd5a033d2ddf9a9 (image query already runs). Compose from
inspect, only image differs; config_unchanged true, running, OOM false, restarts 0. stable-acquisition-bindings.yaml,
consumer manifests and catalog sha256 identical in both images; checkpoint mount unchanged. Rollback:
private/rollback.compose.json (f7351c3b). After restart a one-time history coverage recheck ran ~25 min (settled
02:44 UTC, published_rows 0 on rechecked bindings). Overlap now attempt=1 retry 86400 s, 383-row recent suffix.

| 300 s window (17 DL roles, cgroup usage_usec) | mean | p95 | max | bar edge | Kafka canonical/raw msg/s |
| --- | ---: | ---: | ---: | ---: | --- |
| before (01:47, local CI ran on host meanwhile) | 5.1996 | 5.4454 | 5.4784 | 0.5643 | 645/528 (01:30, not simultaneous) |
| after 1 (02:44) | 5.0258 | 6.4545 | 6.5009 | 0.0171 | not measured; series not recorded |
| after 2 | 4.3657 | 4.5680 | 4.6772 | - | 608.0/463.7 (same window) |
| after 3 | 4.2965 | 4.6351 | 4.6714 | - | 560.3/419.0 (same window) |

Load was 10-15% lower in windows 2-3; the direct, load-independent saving is the bar edge 0.55 vCPU. Window 1's
spike is unattributed. This is observed runtime evidence, not a peak-load certificate; the formal gate still needs a
measured busy window.

Product-path coherence (owner 2026-10-08: gate on the TS product path, not single reads). TS fix IDR-076 (TS
2cebb7d): package admission re-reads BOOK+QUOTE after rollback up to 15 times, 100 ms apart, inside its 3 s readset
deadline; exact-equality rule unchanged. Measured on the CURRENT cores 658a9570 through the deployed TS
MarketCacheReader (Redis GET; re-read policy emulated in the probe), 120 attempts/venue at 1 Hz:
Binance 120/120 coherent within deadline (first read 108/120; time p50/p95/max 10.8/352.3/1091.2 ms; reads p95/max
4/11); OKX 120/120 (108/120; 10.6/332.8/416.3 ms; 3/4). The top-change core candidate 0d8da150 is therefore not
needed for this gate; it raises snapshots x1.47 and should not ride the release for coherence.

Open: DR01-DR08 + broker-delta case receipts (TS runners; DR07 runner TS 74852a8), deliberate reset/fault proof on
the writer (Codex reset-anchor now merged into the TS feature branch, dfc70bd), a busy-window CPU gate, and a DL
release record that names the new bar edge image. No push/merge/release.


## DR01/DR08 runner handoff - 2026-10-08

See [runner runbook](p184-dr-runtime-runbook.md) and the main-plan DR08 receipt.
DR01 has7/9 development HTTP cells, account authorization still needs isolated
final-image Gateway; DR08 has4/4 development checks with preserved raw receipt
and parser supplement. Neither is final TS provenance-bound acceptance.
Runtime remains unchanged, v2.2.3 HOLD; retain f7351c3b until approved release.
