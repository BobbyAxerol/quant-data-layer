# DR05 WriterClone Follow-up

Status: BLOCKED_BEFORE_TS_FAULT_VALIDATION / NOT_QUALIFIED, 2026-10-08.

Owner authorized independent isolated DR05 investigation without waiting for TS
Paper. Parent journal: `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`, section
"Owner amendment: 100 alpha plus TS capacity and independent DL closure".
Governing handoffs: `p184-dr05-fence-channel-handoff.md`,
`p184-dr05-dl-fault.md`. Parent owns journal integration; this report is the only
repository file originally owned by this investigation. No TS or DL harness
source changed during the original attempts; the assigned DL-only cleanup fix
and its verification are appended below.

## Result And Exact Blockers

No GAP_REFUSAL or GENERATION_CHANGE_FENCED cell passed. No fault was published:
the corrected bounded OKX run stopped at its first positive anchor failure.
One real TEST_ONLY raw publication was acknowledged at partition0/offset0 and
observed as canonical BOOK_SNAPSHOT generation7/sequence g7:100, with committed
raw next-offset1 and read_committed EOF for canonical/quarantine. TS observation
was null because its writer process had exited. This is not typed refusal proof.

1. Actual runtime blocker: retained-image `services/market_data/main.py:360`
   exited in `asyncpg.create_pool`; connections to both localhost IPv6 and IPv4
   port5433 were refused. TS `scripts/p184_dr05_isolated.py` starts Postgres,
   sleeps five seconds, then starts the writer without database readiness.
   The receipt has the writer traceback, not Postgres logs; whether Postgres was
   still initializing or had failed is not established. Small TS-owned proposal:
   bounded pg_isready plus Redis ping before writer startup, bounded failure-log
   capture, and continued writer liveness checking. Keep the same overall budget.
2. Deadline violation observed: unchanged `TsDr05Adapter.fresh_case(timeout=30)`
   returned after32.194860803s. Its deadline is not enforced after successful
   probes or during WriterClone startup (`p184_dr05_upstream.py:131-144`).
   Proposal: propagate remaining deadline to clone operations and check it before
   returning; do not increase the budget to conceal startup cost.
3. Deterministic evaluator counterexample: unchanged upstream `evaluate()` at
   lines208-213 accepts a gap observation with previous quantity2 (fault quantity3)
   and `ExecutionMarketContextError:EXECUTION_CONTEXT_STALE`: actual `(True, [])`,
   required false. This was a runtime-free test, not market evidence. Proposal:
   require the actual gap/generation refusal and retained-book age at fault time;
   reject generic STALE, missing-book or unrelated EXECUTION_CONTEXT errors.
4. Static Binance mismatch: `APPROVE_PHASES` includes positive_anchor and
   return_anchor, requiring their exact quantities to be approved before the next
   delta. The DL handoff explicitly requires Binance bootstrap continuity first;
   generation recovery can replay a buffered delta instead of exposing anchor
   quantity5. Coordinate venue-aware expectations with existing DL schedule;
   do not change domain logic or fabricate visibility. Binance was not run.
5. Both cleanup attempts encountered `cleanup incomplete:
   helper:CalledProcessError` after killing the JSONL Docker client. Exact-name
   rechecks found no containers; private erasure then completed. Small DL-owned
   proposal, only after parent assigns ownership of
   `scripts/p184_dr05_dl_fault.py` and its test: when stop fails, accept absence
   only after rechecking the exact previously verified disposable identity.

No source fix was attempted across another owner's files. Next permitted step:
coordinate TS startup/deadline/evaluator fixes and narrow DL cleanup ownership,
then one fresh bounded positive-control run before attempting fault cells.
Final TS qualification, release-image packaging and v2.2.3 remain HOLD.

## Method And Provenance

Unchanged TS candidate modules were imported read-only from
`/home/bobby/.worktrees/trading-system-next-upgrade`, inspected HEAD
`cc76c05ab3f3a18babe14b3ab20dd51a790e5599`, adapter commit
`01eb0d79161687eba578ce185c93d314fdc30255`. Existing DL init/up, host reset watcher,
JSONL channel, TS WriterClone/TsDr05Adapter and production BOOK_PROBE were used.
A host inline orchestrator used the real DL schedule, stopped at the first failed
step, and retained acceptance=false rather than emitting final CaseEvidence.
No monkeypatch to domain functions, timeout/freshness relaxation or image build.

| Artifact | SHA256 / revision |
| --- | --- |
| TS candidate p184_dr05_upstream.py | 57186fc65e134cc87ba66399fabeeabf53486de183a7c1a002ef8c6b44b2803b |
| TS candidate p184_dr05_isolated.py | e29860f6cfee54bf498d258a8fc0b413e20ff13c626f4449bacc998fbd920c88 |
| Retained TS image | a6548ee36aeb0242c1734c071678c525c1506d05c10a70b533eb7110e7eaf16a |
| TS image revision label | 2abbe394ea0fb8f7f5ebcbb745ff70a8c83c9769 |
| TS in-image market_data/main.py | 4f69633bbc83076f3bf68aff587008467a2aad4882efe9c9ffcf3b8640e82bf7 |
| TS in-image execution_market_context.py | 54169bd15e254223891d257d0538ce7d2d825febfe87821b20feefef4750aeb3 |
| Retained core executable | baafcb489bb17fabdd73bef383dcf3448581cc8bb63578e9df83e2a57107b6f2 |

The TS image does NOT contain p184_dr05_upstream.py. Its older isolated.py hashes
71648d9a489c66ab29dc73738d1b686470cecf6b89dc2473de0e621a8c340dfd,
not the imported candidate bytes. Do not claim that this image contains adapter
01eb0d7 or that candidate TS application code ran in it. Network-none file/hash
preflight inspected the retained image; it did not validate application behavior.
Core reused the explicit handoff binary at
`~/.local/state/qdl-v2/dr05-fence-20261008/bin/qdl-realtime-core-baafcb48`.
DL source at start: `0b374bc918af02476cdfb1502d5df132426f901b`.
Query used existing image77682eea with DL source mounted read-only, not a final
release image. Full reused image digests are in the corrected-run receipt.

Synthetic catalog-derived MaterialTests.exported core bindings only; fresh
TEST_ONLY TLS/JWT generated offline. Broker network none, no published ports;
all other disposable roles joined its namespace. Clone Redis6391/PG5433 exist
only there. A supplied WriterClone inspect argument contained an explicit clean
environment, no shared writer inspection/secrets/mounts. UID/GID matched the
owned work directory for private-file access. V2_PRIMARY and EPHEMERAL_ONLY were
explicit; fallback URL was loopback port1. This is a configured disposable clone,
not a byte-identical production environment or shared-runtime acceptance.

## Attempts, Receipts And Cleanup

All receipt paths below are under `~/.local/state/qdl-v2/` and outside Git.

| Attempt | Result | Receipt SHA256 |
| --- | --- | --- |
| dr05-writerclone-followup-20261008T154437Z-4e5659/cleanup-recovery.json | First anchor aborted; recovery-only receipt | 1e1cf02a1850a51ca6ef7a60b74aae42818ea0bc2fdecac8008e082667a4f9a4 |
| dr05-writerclone-followup-20261008T154751Z-fbf95f/receipt.json | Corrected attempt94.413106s; canonical anchor observed, TS unavailable | 587c1244bbf474deea8e17b8f2d1a6e0d0ef7c8f37e24ca639c4606dd514eeb1 |

First attempt supplied invalid temporary mode PROJECTED_PRIMARY; corrected to
the declared EPHEMERAL_ONLY before the second attempt. First-attempt in-memory
observations were lost when cleanup raised; its receipt is expressly cleanup
recovery only, not reconstructed validation. Its writer-exit root cause was not
retained. No third/blind repeat occurred. Both outer runs had a240s bound, fresh
namespaces and stop-on-first-failure. These are attempts, not passing test cases.

Namespaces `p184-dr05-dl-01fcf029155e4389` and
`p184-dr05-dl-cc0a5f9ba9854619`: owned DL five-role stacks, channels, reset
watchers and TS writer/Redis/Postgres removed; privatefiles0 and work directories
removed. Final `docker ps -a --filter name=p184-dr05` returned no containers.
No image was built/deleted and no shared volume/network/cache was pruned.
First recovery cleanup free bytes13006569472 ->12999852032; corrected full run
12793827328 ->11175157760. Concurrent host builds prevent attribution to this
small test; no claim of disk consumption or reclamation from these deltas.
Read-only `docker system df`:95images28.96GB, BuildKit467entries20.52GB.
Shared cache cleanup intentionally deferred during concurrent parent/TS work.
Retain active images, explicitly protected BAR rollbackf7351c3b and handoff binary.

## Runtime And Ownership Handoff

Canonical DL `/home/bobby/data_layer`, main
`5bf5cd540498b82959342c7cfb2ebeaac3007426`, publishedv2.2.2 unchanged.
Active DL feature worktree `/home/bobby/.worktrees/data-layer-p184-closure`,
fix/p184-dl-closure at0b374bc when inspected; only this active feature worktree
was listed. TS checkout/branch feat/v2-rust-first-okx-demo is Claude-owned and
had unrelated dirty files; none touched. Parent concurrently staged DL changes;
this investigation did not stage, commit, merge or push anything.

Read-only shared runtime snapshot:17roles, restart0/OOMfalse. Services:
rust_core/2/3 (658a9570), ingestor_binance_usdm and ingestor_okx_swap (0f6876e1),
query_kn_1/2 and binance_bar_edge (51450612), market_projector_1/2 (a696cfba),
stream_kn_1/2 (1f13408f), kafka1/2/3 (9516fb76), market_cache and stable_redis
(dfa18828). Full image identities match the prior fence handoff snapshot.
Compose config hashes were inspected read-only; no configuration/release
revision, service, provider, authority, shared credentials or shared data changed.
This snapshot is not new certification or a claim of final-candidate deployment.

Parent must integrate this BLOCKED result and receipt hashes into the main
journal. The journal and final acceptance remain open; this report does not
close the phase. In particular actual TS typed gap/generation, recovery, ACK and
control/cache invalidation evidence is still missing.

## Assigned DL Cleanup Fix And Claude Handoff

Cleanup status: IMPLEMENTED_TESTED_LOCALLY, not upstream fault qualification.
Owner assigned only scripts/p184_dr05_dl_fault.py and its tests after reviewing
this report. Parent recorded the pre-edit approval in the main journal. No plan
edit/staging/commit by this agent; parent integrates these results. Rollback is
source-only withdrawal of this narrow uncommitted diff, not a runtime operation.

Cleanup now pins each preflight-inspected, correctly named/labelled AutoRemove
container. It accepts a failed stop only for exit1 on the exact stop command,
with Docker's exact "No such container: <pinned ID>" stderr, followed by successful
empty full-ID and exact-name listings. Other stop errors, failed listings, failed
inspection, retained containers and either authorized or unauthorized same-name
replacements fail closed. No replacement is stopped. Successful stop also needs
the pinned ID to be absent, not just the name. Cleanup disables owned()'s old
inspection-disappearance tolerance; unrelated owned() callers keep their existing
behavior. No protobuf, market semantics, freshness, TS source or release changes.

Verification, with overlapping suites NOT summed:
- Old-source bounded lifecycle counterfactual:12tests,3failures+1error,0skip,
  0.011s. Detects missing stop-race handling, both inspection-error paths and
  waiting on a replacement. The first counterfactual used a mocked sleep that
  produced excessive failure output; it was bounded before the recorded rerun.
- Initial portable focused suite:29PASS,0fail/error/skip,0.025s.
- Initial dependency-image four-module suite:81PASS,0fail/error/skip,89.028s.
- Final portable focused suite:30PASS,0fail/error/skip,0.047s.
- Final dependency-image four-module suite:82PASS,0fail/error/skip,87.401s;
  outer elapsed90.286593s, exit0. This is the final-source total.
- Agent expanded host attempt:78tests,70pass,0fail,8import errors,0skip,
  0.614s, due to absent host google.protobuf/redis. These are dependency
  limitations, not source failures; the corresponding image suite passed.
- Parent-reported separate host attempt:29tests with1Redis import error.
  Preserve this failed attempt as host-dependency evidence, not source failure.
  Parent's image rerun p184-dr05-cleanup-parent-test is parent-owned; its result
  is not invented or included in the agent totals.
- git diff --check passed. New cases cover exact absence, still-present ID,
  same-name authorized/foreign replacement, wrong-ID/unrelated/multi-error stop
  responses, failed ID/name listings, inspection errors before/after stop,
  stopped replacements and name disappearance while the pinned ID remains.

Final image command (inside existing image77682eea):
python -B -m unittest tests.test_p184_dr05_dl_fault tests.test_p184_dr05_case
tests.test_p184_dr05_channel tests.test_p184_dr05_reset -q
Final portable command:
python3 -B -m unittest tests.test_p184_dr05_dl_fault.LifecycleTests
tests.test_p184_dr05_dl_fault.PreparationTests
tests.test_p184_dr05_dl_fault.TimeoutTests tests.test_p184_dr05_reset -q

Both image checks used fresh --rm network-none/read-only helpers,0.5CPU/512MiB,
128PIDs,64MiB /tmp tmpfs, read-only DL source, no Docker socket/secrets, no image
build and a180s host deadline. Fake Docker lifecycle tests plus offline material
generation only; no WriterClone, upstream fault retry or shared runtime mutation.
Helpers p184-dr05-cleanup-unit-57e89c7bcba3 and
p184-dr05-cleanup-unit-55928093b694 were removed, each verified remaining0.
Temporary material lived in deleted test directories/tmpfs; no new private files
or test state retained. Parent helper was not stopped or otherwise touched.
Shared images/cache and named BAR rollbackf7351c3b remain protected; no prune.
Pre-final-helper-cleanup disk observation:1461370880available bytes; concurrent
host builds mean this is not attributed test consumption. No persistent host
artifact cleanup was performed by this fix.

Current inspected parent HEAD fa10d1b5627cbec0a44eb9f70760e96d672a76c2,
fix/p184-dl-closure, canonical main5bf5cd5/v2.2.2 and previously recorded runtime
image/config/service identities unchanged by this work. Parent capacity changes
are preserved. Only the two assigned source/test files and this report were edited.
No commit, push, merge or deployment. Parent main-journal closure remains owned
by parent; the broader DR05 phase is still BLOCKED_BEFORE_TS_FAULT_VALIDATION.

Claude handoff for parent delivery: the DL cleanup race is locally fixed; the
original TS findings1-4 and both runtime receipts above remain unchanged.
Address bounded Postgres/Redis readiness, overall startup deadline enforcement,
typed gap/generation (not generic STALE) verification, and venue-aware Binance
anchor expectations in the TS-owned helper. Parent independently confirmed the
generic-STALE counterexample. No new WriterClone attempt is authorized by this
cleanup-only assignment. Candidate helper byte provenance is not image packaging,
and no actual TS gap/generation/ACK/control-cache qualification was obtained.
No direct Claude messaging transport is available in this session; this report
is the handoff artifact for the parent to relay.
