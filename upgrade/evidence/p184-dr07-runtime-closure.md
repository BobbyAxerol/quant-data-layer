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
