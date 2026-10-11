# P18.4 Data Layer: Astra Architecture And Closure Advisory

Date: 2026-10-10 UTC. Reviewer: Astra/Codex. Audience: Bobby and Claude.
Status: REVIEW_HANDOFF; recommendations, not implementation instructions,
new gates, runtime approval, or a release certificate.
Reviewed source: `4e19966d1b51a494555dd355375a0df00e76c9cf`, branch
`fix/p184-dl-closure`, `/home/bobby/.worktrees/data-layer-p184-closure`.
Main journal: `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`, entry
"Astra advisory architecture review handoff - 2026-10-10".

## 1. Decision Summary

My recommendation is to KEEP the approved Kafka-native architecture and close
specific implementation/qualification risks. The material reviewed does not
justify a new service topology, another broad migration, or another full-catalogue
investigation. It also does not establish that the entire release is stable yet.

The highest-value sequence appears to be:

1. Decide hard-refusal handling in the opportunistic MARK path (R1).
2. Test silent-socket recovery before enabling quiet-book proof (R4).
3. Attribute hot-reader contention and cold-warmup time with focused tests (R2/R3/R5).
4. Complete affected proof and one converged acceptance, preserving old failures.

Claude can choose alternatives, combine slices, or disprove a finding with a
counterexample grounded in the actual contract. A recommendation below is not a
new "must-have" feature. The existing owner-approved invariants and gates remain
the authority; choosing a different implementation does not require adopting my
preferred design. Claims should match what the selected evidence actually proves.

## 2. Scope, Evidence, And Architecture

### What was examined

- Approved architecture guide, sections 18.3-18.5 and Execution Handoff Hot Backup:
  [guide](../DATA_LAYER_V2_KAFKA_NATIVE_ARCHITECTURE_REVIEW.md).
- Rust Hub hot reader, native Binance ingest/liveness, core L2 proof generation,
  projector transactional pipe and fenced Redis apply.
- Python KN cache reader, Query selection/memo, hot client, cold-work scheduling.
- Current MARK tests, stage-100 evaluator/driver, latest plan and capacity receipts.
- Prior supervisor/readiness findings and Claude's subsequent fixes.

This is a targeted architecture/code review, NOT an exhaustive security audit,
all-provider verification, or a fresh production load test. No source application
changes, shared runtime actions, provider calls, credentials or orders were made
by this review. One disposable network-none Python container reproduced R1.

### Evidence vocabulary

- REPRODUCED_LOCAL: source executed with explicitly described test dependencies.
- SOURCE_CONFIRMED: exact control flow/data structure observed; not automatically
  a demonstrated production incident.
- HYPOTHESIS: plausible causal explanation needing attribution.
- ACCEPTANCE_GAP: evidence cannot yet support a particular breadth of claim.
- REPORTED: journal/receipt result inherited from Claude, not rerun here.

### What is structurally sound

The serving model remains:

    provider -> native ingestor -> raw Kafka -> Rust core
                                                   |
                                          committed canonical
                                           |              |
                               Rust Stream + replay    projector A
                                           |              |
                                           |         Kafka state topics
                                           |              |
                                           |         projector B
                                           |              |
                                           |        Redis market cache
                                           |              |
                                           |         Python Query
                                           |              |
                                           +-> SDK <- - - +
                                                |
                                            TS / alpha

Query can also request the existing Stream hot view. Stage A writes state and
input offsets in
one Kafka transaction; stage B applies with Redis owner/generation fencing.
Redis market data is rebuildable, not the financial or durable source of truth.
There is no evidence in the reviewed path of returning to six SQLite spool writers.
Class names such as StableSpoolQueryBackend alone do not prove spool I/O: the KN
backend overrides the read path and reuses domain derivation/quality logic.

Positive anchors:
- `rust/qdl-projector/src/kafka_pipe.rs:196-240`: shared transaction deadline,
  bounded QueueFull handling, transactional offset commit.
- `rust/qdl-projector/src/apply.lua:82-99`: ownership/pointer checks before apply;
  exact decimal coordinate handling, checkpoint in the atomic script.
- `qdl/runtime/kn_market_cache.py:71-83`: atomic latest payload/pointer/watermark read;
  BAR read checks generation and detects changes across page reads.
- Query uses canonical bytes, source coordinate and lineage checks for the hot view.
- The backup is projector-path redundancy on existing Stream roles, not independent
  provider/host HA. Shared Kafka, quota Redis and host remain dependencies.

## 3. Finding Index

| ID | Review priority | Confidence / status | Suggested focus |
| --- | --- | --- | --- |
| R1 | P1 correctness | REPRODUCED_LOCAL | Do not downgrade a hard hot-view refusal to an old MARK OK |
| R2 | P2 availability | SOURCE_CONFIRMED; incident attribution incomplete | Isolate hot lookup from unrelated partitions and long ring scans |
| R3 | P2 bounded latency | SOURCE_CONFIRMED | Include memo lock wait in the same operation deadline |
| R4 | P1 when proof flag enabled | SOURCE_CONFIRMED timer interaction; needs async fault reproduction | Quiet-book timer can defer socket timeout indefinitely |
| R5 | P2 performance/qualification | Measured failure; causal split still open | Attribute cold queue/GIL/client validation before further tuning |
| R6 | P2 diagnostics | REPORTED failures plus source error taxonomy | Preserve bounded typed refusal reason at the failing layer |
| R7 | P2 release evidence | ACCEPTANCE_GAP | Match final code, SDK, flags, schema and recovery scope |

Priority is review triage, not a demand to implement every option before release.
R4 concerns the new opt-in path; flag-off serving is a different claim. R2/R3
may be closed by bounded measurements without structural changes if they meet the
existing gates. No finding here asserts that live orders used corrupt prices.

## 4. R1: Hard Hot-View Refusal Can Become Old Primary OK

### Evidence and impact

`qdl/runtime/kn_query_backend.py:436-495`, particularly the opportunistic MARK
branch at 469-479. `_hot_candidate` maps HotViewGap to OPEN_SEQUENCE_GAP and
rejects topic/partition mismatch as DATA_NOT_READY. The outer branch preserves
only INTERNAL_ERROR as INTEGRITY; other QueryBackendError values become
`candidate=None`. It then parses/validates the primary and returns `stored, OK`.

Local reproduction used the existing RealOrderingChecksTests fixture at
`tests/test_kn_mark_hot_memo.py`, REAL golden canonical MARK bytes and the actual
backend parser, lineage checks and monotonic checks. Only cache/hot transports
were stand-ins. Results on 4e19966:

| Hot result | Returned primary | State |
| --- | --- | --- |
| HotViewGap(HOT_RECORD_GAPPED) | C10 | OK |
| Hot view with different topic identity | C10 | OK |
| HotViewUnavailable(HOT_READER_BUSY) | C10 | OK |

The first two are hard source-validity questions; the third can be a legitimate
availability fallback. This proves backend refusal suppression, NOT that a full
public request or TS intent was incorrectly approved in production. Session and
component checks later in the caller may independently reject the old record.
Whether MARK currently emits every GAP variant is separate from the confirmed
branch behavior; topic mismatch does not depend on a MARK GAP event existing.

Related taxonomy: `qdl/runtime/kn_hot_view.py:100-150` collapses protocol/auth and
invalid reply identity/payload into HotViewUnavailable. These deserve explicit
review before treating every HotViewUnavailable as harmless transient loss.

### Options

A. Preferred narrow option: classify hard versus transient outcomes explicitly.
Propagate fence/generation/gap/integrity to a typed refusal; retain an independently
valid primary only for transient transport/busy/capacity failures. This can be a
small selection-path change with a focused reason table rather than a new cache.

B. Carry a typed hot-read outcome (available, transient unavailable, hard refusal)
through both normal and prefer_hot branches. Slightly more code, less chance of
future exception subclasses accidentally changing the fallback policy.

C. Keep the current fallback ONLY if Claude establishes a contract proof that a
particular refusal cannot invalidate this primary. Document that narrow case;
"the primary is fresh" alone does not prove generation/authority consistency.

Suggested test matrix: valid primary + transient busy; hard gap; wrong topic;
wrong partition; same offset/different payload; expired session; older hot record;
newer primary; recovery after fence. Exercise both latest_stored_event and the
MARK component-quality/TS at-use path. Existing ordinary E03/E04 tests are unrelated.

Suggested closure evidence: counterfactual regression for hard refusals, positive
primary fallback retained for transient errors, then affected packaged read tests.
No need to manufacture a production fault just to confirm this selection logic.

## 5. R2: Hot Reader Couples Unrelated Products

### Evidence and limits

`rust/qdl-stream-gateway/src/hub.rs:333-447` loops over all partitions, acquires
each partition via try_lock, and walks ring entries backward. A busy unrelated
partition can return HOT_READER_BUSY even if the requested product has a valid
committed record elsewhere. Looking up a quiet product may scan much more than
one recent active product. Per-record deadline checks bound scanning time but
do not remove contention. The scan holds the partition lock used by dispatch.

This is SOURCE_CONFIRMED. The stage-100 DATA_NOT_READY detail was attributed to
HOT_READER_BUSY in Claude's journal, but the proposition that MARK alone caused
those collisions remains a hypothesis. Memo per MARK binding reduces request
volume; it does not make the underlying operation product-local.

### Options

A. Smallest experiment: use the already-authoritative partition mapping where it
is provable, then examine only that partition; retain conflict detection through
mapping validation. Do not assume Python's partition_key is itself a numeric
partition or invent a hash inconsistent with Kafka routing.

B. Preferred if measurements still show contention: keep a bounded latest index
inside the EXISTING Hub, maintained atomically with dispatch. Store Arc references
and both latest product and newest book-sibling/fence scope. Reads can take a
short lock to copy references, then decode/check outside it. Cost: extra bounded
metadata and eviction logic; benefit: predictable lookup instead of ring length.

C. Retain ring scanning, shorten lock-held work and retry under one absolute
budget. This is less structural but can still fail under sustained contention.
Replacing try_lock with an unbounded blocking lock is not the same optimization.

Index safety considerations: BOOK_DELTA and BOOK_SNAPSHOT share physical scope;
a newer reset/gap/unverified sibling defeats an old clean snapshot. Preserve
original record offset AND partition watermark; retain broker-confirmed progress,
read_committed, retention and generation semantics. Memory should remain bounded
by declared products/retention, not every historical event ID.

Focused test: hold an unrelated partition lock; concurrent MARK and BOOK reads;
quiet product behind many busy-product events; newest unsafe sibling; broker
confirmation expiry; ring eviction; same offset conflict. Measure lookup/lock/
decode time, refusal counts and dispatch delay in one isolated workload.

A release choice can be "current path is adequate under the frozen workload"
with evidence; it need not automatically be an index implementation.

## 6. R3: Memo Wait Is Outside The Hot RPC Budget

`qdl/runtime/kn_query_backend.py:332-347` uses `with lock` without an acquisition
timeout. `CanonicalHotClient.latest` starts its timeout AFTER obtaining that
lock. Each request therefore does not have one bound for wait + lookup. If the
leader exceeds the 50ms memo window, queued callers may each start a fresh lookup
because the memo timestamp is from lookup start. Cancellation of an async caller
does not itself interrupt a Python thread waiting on this mutex.

This is SOURCE_CONFIRMED budget composition, not evidence of an infinite loop:
RPC timeout and bounded admission may impose a larger outer bound. No latency
threshold is invented here.

Options:
- A: pass a remaining absolute budget through lock acquisition and hot call;
  return a transient typed timeout when exhausted. Keep the worker's admission
  permit until it actually finishes, as the current cancellation wrapper intends.
- B: use a per-binding in-flight future; each waiter has its own remaining budget.
  More implementation complexity, better explicit cancellation semantics.
- C: retain the mutex if a measured worst-case wait is within the frozen budget;
  make the composite bound explicit in telemetry and a deterministic test.

Tests: delayed leader, multiple waiters, repeated unavailable outcomes, caller
cancellation, recovery after failure, primary advances during the wait. Broker
proof age and quality are evaluated at use time even when reusing a result.
Do not start a fresh full timeout at every stage or lengthen freshness to hide it.

## 7. R4: Quiet-Proof Tick Can Prevent Silent-Socket Recovery

### New finding, activation-dependent

`rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs:1745-1750,1853-1895,2023-2068`.
The Binance loop constructs a NEW timeout(heartbeat_seconds, socket.next()) each
iteration. The new book_proof_tick branch runs every 250..5000 ms and then
`continue`s. If a socket stays open but sends neither data nor pong and proof
interval < heartbeat timeout, the tick can repeatedly win and discard the read
timeout before it expires. Sending ping can still succeed on a half-open socket;
that is not proof of a received pong. The timeout-specific disconnect path may
therefore never run until another transport error/OS timeout occurs.

This follows from source scheduling; no Rust silent-peer fault run was performed
in this review. It is not attributed to the observed OKX disconnects, which use
a different path. The flag is default-off, so distinguish new-feature activation
risk from already-active production behavior.

The proof generator correctly stops emitting fresh evidence once the last
received transport message is too old. Thus the immediate concern is bounded
RECOVERY/availability, not proof that it fabricates an executable price.

### Options

A. Preferred narrow option: track an absolute receive/pong deadline independent
of periodic work. Renew it only on actual accepted inbound progress; proof tick,
publish completion and sending ping do not reset it. Check deadline in the loop
and provide a dedicated select timer which remains expired across iterations.

B. Separate a small persistent liveness state machine from the read timeout.
This may be clearer if several periodic branches already affect the same loop;
no extra service is needed.

C. Ship parsing/schema support with proof flag OFF until this fault case is
checked, if owner prefers release staging. Describe quiet-book availability as
not yet improved; do not call proof activation certified while leaving it off.

Suggested deterministic test: local fake WebSocket accepts ping but never sends
pong/data; fake clock or paused Tokio time, proof tick faster than heartbeat.
Expect bounded disconnect/new generation, no fresh proof after deadline. Add
normal pong recovery, data traffic, publish backpressure, shutdown and flag-off
controls. Test the real loop/state machine, not a Python simulation of it.

## 8. R5: Cold Warmup Needs Attribution, Not More Lucky Runs

Stage100e on rc5 (REPORTED in plan) still failed OKX COLD_2500 at 7531.5 ms;
Binance 4288.6 ms, frozen limit 6265.1 ms, n=1 each. Earlier first-read microbenchmarks
improved 25-31%, but production cold reads did not improve correspondingly.
A second request finishing roughly one service-time later is consistent with
same-replica queuing, but also client-side synchronous validation in one worker.
It does not by itself prove GIL wait is the dominant cause.

Claude commit `7f974c3` now records serving replica and transport/client timing.
This is the right direction; inspect those results before choosing the next fix.
`qdl/query/cold_work.py` implements cooperative CPU duty cycling, not separate CPU
isolation. Increasing COLD_SLICE_MS trades hot responsiveness for cold progress;
one pause slice is not a strict end-to-end hot latency bound over many stages.

Options ordered by scope:
1. Use the new timings to fix the actual unfair admission, redundant per-row
   work or client validation scheduling. Preserve bounded large-read lanes.
2. Choose a measured cold slice with hot feed tail/refusal controls on the SAME
   workload; tuning is useful but is not proof that compute is isolated.
3. If unavoidable Python CPU contention still misses the gate, evaluate a bounded
   process/native render segment within existing roles. This is a larger option,
   not an immediate recommendation to rewrite Query or introduce a new service.

A focused matrix can be short: deliberately same replica and split replicas,
2500/5000 rows, cold and warm cache, cancellation/queue saturation, concurrent
hot MARK/BOOK. Record request-to-validated-consumer, queue wait, server work,
response decode/validation and bytes. Do not rely on random placement eventually
producing a PASS, or force distinct replicas only for the certification run.
Unchanged tests/evidence can be inherited; final mixed load follows attribution.

## 9. R6: Refusal Diagnostics Still Lose The Layer Of Failure

The stage100c/rc3 reports repeatedly say Query logs carry no refusal line and
causality is inferred from a detail hash. Hashes protect payloads and preserve
identity, but without an indexed typed reason they make triage unnecessarily slow.
The OKX incident in stage100e correlates with an ingestor disconnect; correlation
alone does not say whether the provider, client heartbeat, network, resubscribe
or materialization accounts for the full outage.

Options:
- A: bounded structured failure-only counters/logs at existing boundaries:
  problem code, origin (admission/identity quota/hot/cache/session), binding ID,
  replica, request/run ID, generation and queue/RPC duration. No tokens or raw data.
- B: retain payload-free sanitized reason fields directly in harness receipts;
  look up exact requests rather than turning on broad service DEBUG.
- C: short sampled trace for only the affected binding, with expiry/size cap.

Suggested reconnect timeline: last valid receive -> disconnect -> reconnect ->
subscription ACK -> first canonical -> Query/Stream usable -> TS apply ACK.
Split a correct refusal during actual loss from internally delayed recovery.
Do not treat the observed 37 OKX disconnects as proof of a venue defect or a reason
to disable fail-closed behavior. This advisory does not propose new fallback venues
or replacement market prices.

## 10. R7: Release Convergence And Scope Of Proof

Several distinct questions remain even if the next load receipt turns green:

- SDK now targets 2.2.0 after replica-start and additive view fields. A test client
  mounting checkout SDK over an older image is development evidence, not proof
  of the final published wheel/image. Check the final artifact explicitly.
- IDR082 adds event identity; TS adopting it is separate from Query emitting it.
- IDR040 proof flag, generated ingest config, core protobuf, Query contract digest,
  bundle and tolerant readers need a coherent packet. A field being optional does
  not establish that all pinned clients tolerate it or use its semantics.
- G12 PASS is isolated synthetic BOOK recovery with a single Stream/projector and
  TS clones. It is useful, but it does not prove TRADE losslessness, MARK component
  eligibility, BAR finality or independent-host HA during those faults.
- G4 rows absent from observation are NOT_MEASURED. OKX BAR open-time is not
  close-to-canonical latency; MARK composite older-component age is not pure
  transit of a new MARK event. Keep the frozen raw metric for comparison and
  report the semantically correct companion metric; do not relabel silently.
- Historical full-suite failures/skips are not automatically waived. Categorize
  required coverage, reproduced base failures, environment skips and remedies.

Options for a compact release:
A. One final immutable source/image/SDK packet with affected regressions, scoped
real-provider reads and the agreed final load/fault receipts. Inherit unchanged
proof with explicit source/dependency mapping.
B. Stage optional producer flags off, certify the supported behavior accurately,
then activate in a narrow reviewed window. This changes the advertised feature
scope, not the meaning of PASS, and is an owner/Claude release decision.

I would avoid repeated full-catalogue tests, unrelated TS accounting changes,
more revision bumps merely for a retry, and hidden permissive fallback policy.

## 11. Previously Reported Items: Do Not Duplicate Work

| Earlier finding | Latest source/report at review | Remaining check |
| --- | --- | --- |
| TS periodic observer wrong container / dropped unreadables | 713aa90 fixes source and adds regression | Actual run using fixed harness; earlier 2-sample PASS stays limited |
| Generic PID/container cleanup and unreadable logs treated as zero | Claude journal says run-scoped versions replaced old scripts | Inspect actual running script/PID receipt, not just old scratch path |
| rc5 tests mocked ordering | 4e19966 adds RealOrderingChecksTests | No longer report this as missing; use those fixtures for R1 hard-refusal tests |
| rc4 memo on backup path | ec89a31 separates paths | Keep rc4 failed evidence; R1/R2/R3 are distinct residual questions |

Do not rerun E03/E04 or other TS order qualification to settle these DL findings.
No new TRADE latency campaign is suggested for IDR082's already-attributed quiet
snapshot effect. The new OKX disconnect observation is a separate bounded case.

## 12. Suggested Handoff And Closure Order

This is an execution OPTION for Claude, not a replacement phase plan:

1. A short correctness slice: R1 decision table and regression; R4 focused fault
   reproduction before enabling proof. Decide explicit flag scope for release.
2. A focused performance slice: use 7f974c3 timing; R2/R3 contention/deadline tests.
   Pick only the bottleneck proven relevant to the frozen workload.
3. Integrate selected changes once, test affected interfaces/artifacts, then final
   acceptance on the agreed SHA/digests. Score G4/typed refusals/readiness coverage
   together with the load gates; keep old failures and incomplete attempts.
4. Cleanup exact owned runs after evidence; retain active plus named rollback.
   Remote dev/main release sequence follows existing owner approval, not this file.

Suggested response from Claude per item:

| ID | Accept / disprove / not applicable / staged | Selected option or alternative | Test/receipt | Runtime included? |
| --- | --- | --- | --- | --- |
| R1-R7 | To be filled by Claude | Explain trade-off briefly | Hash/path and scope | Exact digest or not deployed |

Do not label an unimplemented chosen fix as external debt. Equally, do not turn
all alternative designs into mandatory scope: a reasoned rejection with evidence
is a valid review response. Existing safety invariants are the common contract.

## 13. Review Reproduction And Hygiene

R1 executed once in image
`sha256:77682eea5054ae9ddbd858323b083286669a55eeb4bc7bc065b04bae8854e32f`,
network none, source bind read-only, python -B, 0.5 CPU, 768 MiB, 128 PIDs, 32 MiB tmpfs.
Container `qdl-astra-review-20261010-fence` used --rm and was confirmed absent.
No image build, provider access, Redis/DB service, persistent test volume or new
credential. No full suite was rerun. The three local outcomes above are NOT three
acceptance PASSes; they expose current behavior for review.

Reproduction recipe (inside the same existing test image, at reviewed source):

```python
from dataclasses import replace
from tests.test_kn_mark_hot_memo import RealOrderingChecksTests
from qdl.runtime.kn_hot_view import HotViewGap, HotViewUnavailable

RealOrderingChecksTests.setUpClass()
for scenario in ("gap", "generation_mismatch", "temporary_busy"):
    case = RealOrderingChecksTests("test_a_primary_advances_past_the_shared_memo")
    case.setUp()
    backend = case.backend()
    if scenario == "gap":
        case.reply = HotViewGap("HOT_RECORD_GAPPED")
    elif scenario == "generation_mismatch":
        hot = case.hot("H1")
        case.reply = replace(hot, boundary=replace(hot.boundary, topic_id="different-topic"))
    else:
        case.reply = HotViewUnavailable("HOT_READER_BUSY")
    stored, state = backend.latest_stored_event(case.binding)
    print(scenario, case.name_of(stored.event.payload) if stored else None, state)
```

No retained runtime, rollback image, build cache or shared state was removed.
Disk pre-test sample was not taken; no disk-reclamation claim is made. General
image pruning was intentionally avoided while Claude was concurrently working.
Canonical checkout remains `/home/bobby/data_layer`, released line `main@5bf5cd5` /
v2.2.2 at the last check; v2.2.3 remains a candidate. This document does not attest
current container health or supersede a newer journal/owner decision.
