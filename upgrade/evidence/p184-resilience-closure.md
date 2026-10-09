# Resilience Closure: Three Approved Packages

Status: IN_PROGRESS, source and isolated tests first. Owner2026-10-08.
No architecture rewrite, no broad catalogue or order acceptance rerun.
Architecture: DATA_LAYER_V2_KAFKA_NATIVE_ARCHITECTURE_REVIEW.md.
Prior exact-event evidence: p184-book-ack-trace-20261008.md.

## R1 Consumer Throughput And Recovery

Claude owns TS worktree; Codex owns DL/SDK and shared measurement tooling.
No unsynchronized edits to TS. Existing writer5849d716/SDK2.0.7 is distinct from
Claude candidate. Freeze source/image/config before each affected rehearsal.

Acceptance contract:
- Preserve lossless contract unless explicitly negotiated latest-state feed.
- BOOK resync uses authoritative snapshot + exact boundary and generation.
- No blind deletion of delta cache, offset reset or cursor skip over unapplied data.
- Ordered bounded batch: eliminate measured repeated work, not increase buffers.
- Venue/family ownership addresses shared BTC pressure, not every ETH issue.
- Per-event or batch correlation covers SDK return, prepare, lock/read/CAS,
  Redis reply, SDK ACK admission/completion and durable cursor; report missing.
- Prove before/after against same capture/workload, preserve all failed outcomes.
- Negative tests: partial apply/failure, generation changes, replay, crash between
  Redis and cursor, slow consumer isolation. Recovery must catch up after burst.

R1 remains OPEN. Exact measured post-SDK/pre-writer delay is not fixed by disk
watcher changes or a process restart. Link final TS commit/receipt here, not an
unsupported claim of cross-agent coordination or a quota-only capacity claim.

## R2 Disk Pressure And Safe Recovery

Existing watcher receives bytes/inodes, projected consumption, explicit operator
policy and stable recovery observations. Missing telemetry fails closed for new
load admission. Thresholds must be derived from measured growth, retention,
maintenance lead time and reserve; test thresholds are not production defaults.
No broad prune, deletion of Kafka logs, DB/Redis state or named rollback images.

Actual read-only findings:
- Broker1/2/3 and projector1/2 have Docker unless-stopped restart policy.
- Installed qdl-v2-stable-boot-recovery.service is inactive/dead, points at
  canonical scripts/v2_stable_boot_recovery.py --apply. It is legacy spool logic,
  not KN auto-recovery. NRestarts0 at inspection, not proof no historical fault.
- Legacy recovery can FLUSH and reset offsets. New source fence refuses running,
  stopped or restarting KN roles before destructive methods. Does not deploy
  itself to canonical/systemd. Mixed/unknown topology is not authority to reset.

Recovery state sequence to integrate with KN supervisor:
PRESSURE/UNKNOWN -> HOLD_NONESSENTIAL_WORK -> SPACE_RECOVERED -> WAIT_QUORUM_ISR
-> WAIT_PROJECTOR_PROGRESS -> WAIT_CONSUMER_ELIGIBILITY -> SERVING.
Use hysteresis and one bounded deadline; resumed storage does not equal READY.
Transaction fencing/fatal corruption stays BLOCKED/operator-reviewed. Restart
only exact declared role when eligible, with backoff/retry ceiling and receipt.
Do not automatically restart healthy brokers for stale consumer data.

## R3 Fault And Promotion Gates

Faults are isolated: no filling host disk or production Kafka volumes.
For each case declare cap, timeout and workload BEFORE running:
1. Byte exhaustion and inode exhaustion; monitor unknown; pressure oscillation.
2. Real storage ENOSPC then space return: observe guard and successful writes;
   this alone is not Kafka transaction recovery certification.
3. Broker unavailable/transaction abort/fencing and restart replay from committed
   offsets. Reuse existing isolated Kafka recovery/DR08 harness and captures.
4. Consumer overflow/replay/expired cursor -> snapshot boundary -> contiguous
   usable stream. Slow client must not starve another client.
5. Partial batch failure/crash before and after ACK: no backward state or skips.

Exit: source tests + isolated faults + scoped candidate consumer evidence; no
unexplained loss/order mismatch, no false eligibility, bounded recovery under
predeclared deadlines. Only changed roles may rollout, exact immutable digest,
config drift check and named rollback. Preserve DR/broker acceptance gates.
No release until R1/R2/R3 final receipts are linked; no fabricated Full status.

## Cleanup And Runtime Boundary

No new images unless source requires them. Reuse retained test images, isolated
--rm test containers, network none for filesystem tests, tiny bounded tmpfs.
No host data mount for fault writes. Remove owned test artifacts immediately;
keep hashed bounded evidence and active/named rollback set. Do not touch concurrent
Claude resources. Journal actual disk and runtime checks after each tested slice.
Canonical /home/bobby/data_layer main5bf5cd5, publishedv2.2.2;
feature /home/bobby/.worktrees/data-layer-p184-closure fix/p184-dl-closure.
v2.2.3 stays HOLD until final gates, not waiting on unrelated Paper features.

## Tested Initial R2 Slice

Source changes: kn_production_watch.py/kn_disk_pressure.py expose explicit disk
policy, byte/inode available-to-writer capacity, monotonic consumption rate,
time-to-reserve and capacity/horizon hysteresis. Unknown measurements fail closed.
--ts-container selects the same writer for inspection and log checks, preventing
accidental observation of the stopped legacy market_data_service. No new default
production thresholds or automatic lifecycle authority. Existing exact-ID stop
mechanism unchanged; default remains alert-only. This watcher is NOT yet a native
Kafka recovery coordinator or ongoing production admission controller.

35unit tests PASS0fail0error0skip across boot recovery14, watcher10, disk11.
Counterfactual mixed KN/legacy fixture: previousassess RECOVER, new MANUAL.
Newsource actualread-only assess also MANUAL/KN topology. No cache reads/writes.

Real isolated fault: existingPythonimage77682eea,networknone,read-onlyroot/source,
128MiBRAM/.25CPU,pids64,/fault tmpfs16MiB64inodes,container --rm. Both actual
ENOSPC(bytefull andinodefull) detected. Removing exactlyownedtestfiles restored
capacity and policy cleared, verified write/read after recovery. Duration
0.110404s. No provider data, no hostfaultwrite; Kafka recovery NOT_TESTED.
Temporarycontainer qdl-resilience-storage-fault-20261008 confirmedabsent.
Evidence filesystem-fault.json SHA256 e5c30a8e87ab2e62cdbc3c2de7b7133fca0f6ffef923e8e972954bb53c7dc0e3.
Root ~/.local/state/qdl-v2/resilience-20261008, unit.json/unit.log retained.

R1 remainsOPEN withClaude; R2 nativeprogress/restartcoordination remainsOPEN;
R3 Kafka/consumerfaults+rollout+release remainOPEN. No runtime/installedsystemd
change. Do not claim self-healing fullstack or releasecertificate.

No build/images created. Existingactive/rollback retained. Docker86images69containers;
concurrentBuildKit growth196->219entries/50active refs,1.126GB labeledreclaimable.
Not owned by thisslice; no broadprune during concurrentClaude builds. No volumes
removed; tmpfs test ephemeral. Diskavailable95856402432bytes beforetestinventory;
postmeasurement recorded in mainjournal.

## R2 Coordinator Slice - 2026-10-08

Status: IMPLEMENTED_TESTED_LOCALLY; overall R1/R2/R3 IN_PROGRESS.
`kn_recovery_state.py` supplies the pure state machine. Unknown/stale/conflicting
observations fail closed. Disk, Kafka quorum/ISR, projector progress and consumer
eligibility are independent ordered gates. Stable distinct samples are required;
heartbeat alone cannot produce SERVING. One absolute recovery deadline, bounded
backoff/attempts and sticky BLOCKED prevent infinite retry. After SERVING the
state machine can start a new bounded episode when health deteriorates.

`kn_recovery_runner.py` is a bounded invocation (not an installed daemon). It
exits on SERVING/BLOCKED. Default is observe/propose only. Explicit approval IDs
must belong to the packet; each start reinspects exact ID/image/name/exited state
and refuses OOM. Inspect and start share one timeout. It never stops/recreates,
flushes cache or resets offsets. Probe subprocess output is capped at 256 KiB;
packet <=256 KiB, <=64 roles, run <=3600s, polling >=0.1s. Evidence output is new,
exclusive mode0600; existing receipts cannot be overwritten.

Required production integration STILL MISSING: supply a native probe which
reads actual quorum/ISR, committed progress/cache catch-up and the production TS
validator result, with a fresh monotonic timestamp. Arbitrary JSON booleans or
READY60 are not acceptable certification evidence. No production packet or
systemd installation has been made. The existing boot service has NOT received
the legacy fence patch. Do not claim automatic full-stack recovery is deployed.

Verification: `python3 -B -m unittest tests.test_kn_recovery_state
 tests.test_kn_recovery_runner tests.test_kn_disk_pressure
 tests.test_kn_production_watch tests.test_v2_stable_boot_recovery`: 68 PASS,
0 failures/errors/skips, 0.371s. Coordinator24 + runner9 + prior35. Runner tests
use mocked Docker commands; subprocess output overflow and timeout are real local
process tests, not Kafka faults. Direct script --help passed. No image build,
container creation or runtime mutation in this slice.

## R1 Exact TS Handoff For Claude - 2026-10-08

Read-only review at TS274fb5f; no edits to Claude's worktree. IDR-081 remains
open. Reset anchors exist (58287fc/dfc70bd), but cache_projector still applies
BOOK entries serially through execution_state.store_advancing. Per item this
includes marker/value read, JSON comparison, CAS carrying old/new JSON, retries
and some rejection readback. This identifies avoidable work, not a measured
allocation of all observed latency. Existing writer5849d716 still SDK2.0.7.

Proposed write ownership: Claude edits execution_state.py/cache_projector.py.
Implement bounded runs (at most64) of same-key, same-generation BOOK_DELTA.
Read/verify initial marker and value once, prepare all input before writing and
simulate EVERY transition with existing ordering semantics. A bounded Lua CAS
must check initial marker AND value; validate every argument before mutation,
then preserve accepted writes and publish order/TTL. Do not retain only the last
book delta. Initial CAS conflict performs no writes; recompute inside the SAME
deadline. Lost/unknown Redis reply does not ACK; replay from confirmed cursor.
Adoption, reset anchor and generation transitions remain scalar initially.
Do not pipeline dependent same-key CAS or reuse TRADE semantics for BOOK.

Required real Redis comparisons against scalar: both venues; sizes1/64/65;
older/duplicate/conflicting sequence; corrupt marker/value; generation/reset;
publication order, TTL and exact bytes; lost reply, partial failure, crash before
and after ACK; no backward state or skipped delta. Coordinate tests in
integration/test_edc_book_delta_redis.py and test_edc_execution_state_redis.py.
Then reuse the SAME event-joined trace through SDK/apply/ACK/cursor. No claim
of runtime latency improvement until that test and narrow rollout are complete.

R3 remains open: actual Kafka/consumer fault integration, compatible narrow
rollout and real consumer usability acceptance. Existing filesystem ENOSPC
receipt is inherited, not substituted for Kafka durability/recovery.

## R1 Handoff Correction - 2026-10-09

Claude TS phase-D slices3-6 (sourcea4507abb) provide newer profiling: Redis atomic
APPLY contention blocks BOOK/MARK/readers. Previous same-key BOOK CAS batch
proposal above is NOT an instruction to implement speculatively. Real BOOK
batches are often size1; prioritise APPLY encoding/EVALSHA/source-contract dedup
already under Claude ownership. Full mixed workload must include production-size
BOOK_SNAPSHOT and MARK, absent in the earlier idle-Redis benchmark. Per-callCPU
improvement alone is not end-to-end closure; keep four workers per owner.
See TS upgrade/p184-phase-d-gates.md, Codex DL Review2026-10-09: G4-G6 receipts,
semantic objections and freeze blockers. G4 all-feed canonical baseline and G5
warmup/batch numeric limits are not available as certified stage50 values. No
runtime changes, load, release or new phase authorised by this documentation.
