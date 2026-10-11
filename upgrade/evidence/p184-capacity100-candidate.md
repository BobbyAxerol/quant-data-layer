# 100 Alpha Plus TS: Candidate Capacity Contract

Status: IMPLEMENTED_PLANNER / LOAD_NOT_MEASURED / NOT_CERTIFIED, 2026-10-08.
Authority: Unified Plan, "Owner amendment: 100 alpha plus TS capacity and
independent DL closure". Existing Kafka-native architecture and endpoint contracts
remain unchanged. No shared quota, resource, identity or runtime was changed.

## Workload, Not Merely Quota

`scripts/p184_capacity100.py` reuses the existing Phase-3 typed workload. Inputs
must be actual versioned manifests and explicit instrument/product selections.
100 means logical alpha sessions, NOT 100 authenticated identities. This first
candidate multiplexes 50 sessions through each venue's alpha identity and keeps
TS on a separate identity. It does not certify arbitrary strategy demand,
unlimited universe scans, additional symbols, or 100 different credentials.

Two frozen profiles, both with real data processing rather than idle sockets:

| Profile | Alpha classes | Alpha streams | Hot read calls/s |
| --- | --- | --- | --- |
| realistic | 40 candle / 30 realtime / 20 grid / 10 multi | 180 | 100 |
| stress5 | BAR1m, TRADE, QUOTE, BOOK_DELTA, BOOK_SNAPSHOT per alpha | 500 | 100 |

Each session uses 2-5 distinct products INCLUDING startup anchors. All stream
products explicitly require DURABLE delivery. A duplicated stream, missing feed,
wildcard or mixed identity is rejected. The ordinary universe warmup requirement
is not expanded by this profile. Shared instrument selection must be recorded;
500 streams do not mean 500 independently acquired provider subscriptions.

The unit-fixture TS reservation is 60 routes, 50 streams, 1000 requests/minute.
These are INPUT ASSUMPTIONS, not observed TS usage. Before load, replace/confirm
with writer's actual reconnect/snapshot demand; include probes and recovery calls
under the same identity in the request budget. TS heartbeat from production is
only a protection guard, never proof that TS consumed the candidate workload.

## Candidate Quotas And Memory

The following is arithmetic on the typed TEST_ONLY fixtures, not an entitlement
change or a capacity measurement. The planner also reports exact deficits versus
its input manifests. Startup plus one reconnect minute is counted separately
from steady traffic. Snapshot/warmup plus Subscribe are both counted. Driver
probe/cold-read overhead still needs addition before any admission; these values
are not a complete runnable quota packet. Steady RPM headroom is 1.5x, stream headroom is 1.2x.

| Identity | Realistic RPM / slots | Stress5 RPM / slots |
| --- | --- | --- |
| Binance alpha | 4506 / 108 | 4950 / 300 |
| OKX alpha | 4509 / 108 | 4950 / 300 |
| TS, assumed workload above | 1500 / 60 | 1500 / 60 |

Stress5 needs 550 active streams and 660 reserved slots. The CURRENT observed
384-slot Stream replica cannot carry all of these after its peer is lost.
Do not multiply capacity by two and claim failover. The existing 1024-slot
MemoryPlan is the candidate to measure, with replay RPCs accounted separately.

Existing Rust MemoryPlan arithmetic for 1024 Subscribe + 32 Replay slots:
48 MiB ring + 32 MiB live pool + 32 MiB replay + 32 MiB Kafka + 64 MiB process
reserve + 99 MiB transport = 307 MiB. Historical container cap is 512 MiB.
This is a modeled allocation budget, NOT measured RSS or an OOM guarantee. In particular,
32 MiB shared live queue may contend with fanout. Size aggregate pools from
measured event sizes and concurrent draining after correctness; keep per-client
backpressure bounded. Do not enlarge a slow client's backlog to hide throughput.

Host observation: 16 vCPU, Stream 0.75 vCPU per replica. These values were read
previously, not a fresh resource-availability claim. The old <=5-vCPU ceiling is
superseded by owner's adequate-resource direction. There is deliberately no
invented CPU requirement for 100 alpha: measure cost under the actual profile,
then assign CPU/RAM with headroom while protecting TS and co-hosted services.
100 alpha container compute itself is outside DL serving-capacity measurement.

## Correctness And Measurement Order

1. Finish DR05 typed GAP/generation and DR08 terminal/replay on exact candidate.
   Paper/OR feature completion is not a dependency. Keep failed receipts.
2. Freeze image/config/manifest, workload, offered-rate schedule, test identities,
   TS reservation, client resource budget and allowed stop conditions BEFORE load.
   A quota candidate is not permission to load the shared identity without this.
3. Reuse affected fast matrix and current driver: ramp 20/50 then 100 only when
   previous stage has no internal errors, queue growth or unsafe eligibility.
   Passing earlier unrelated catalogue/economic tests remains inherited.
4. Include sustained and burst workload, one slow alpha versus healthy peers,
   disconnect/replay, worker crash after apply before cursor commit, and one
   Stream replica unavailable. Do not reset a cursor or skip records to catch up.
5. Report server offered/dequeued, SDK receive, apply, Redis ACK and cursor commit
   for the SAME product/generation/offset. External Redis polls are not ACK time.
   Report backlog high-water/events/bytes and bounded recovery independently.
6. Measure request-to-usable, event age, canonical delivery and consumer apply
   in ms, p50/p95/max and n; p99 only with n>=100. Refusals remain in denominator,
   separating valid market quietness from internal pipeline failures. Active
   sessions must process data; health, quota slots and idle sockets do not pass.
7. Measure CPU/throttling, RSS/OOM, I/O and Kafka lag over matching intervals for
   all DL roles AND the test clients separately. Choose operational headroom from
   peaks/bursts, not a post-hoc threshold chosen to make a failing result pass.

Existing freshness, schema, source identity and execution gates are unchanged.
The acceptance cannot pass while a GAP leaves a fresh old book execution-eligible,
while queue/replay loops never catch up, or while a consumer loses ordering.
Only then prepare the exact role-by-role resource/config/rollback packet.

## Runtime And Cleanup Boundary

No production load, quota activation, image build, rollout, push or merge in this
planner slice. Canonical /home/bobby/data_layer main5bf5cd5, published v2.2.2;
feature fix/p184-dl-closure. v2.2.3 remains HOLD. Preserve active digests and named
BAR rollback f7351c3b. Unit Docker runs use existing images, network none, read-only
source and --rm; no persistent volumes. Load resources must use owned namespaces
and be removed after each attempt, preserving bounded receipts/hashes and faults.

## Executed Verification And Remaining Implementation

Existing image77682, network none, read-only source, bounded64MiB/tmp and --rm:
`python -B -m unittest tests.test_p184_capacity100
 tests.test_phase3_target_workload tests.test_phase3_consumer_load
 tests.test_phase3_target_driver tests.test_phase3_consumer_load_driver
 tests.test_kn_capacity_budget`:103PASS0fail0skip,4.535s (agent transcript).
Parent independently ran capacity+target_workload:32PASS0fail0skip,0.092s;
this is a SUBSET, not32additional tests. No persisted performance receipt was
produced by these unit tests. Containers removed; no images/volumes created.
Host-only driver import initially lacked protobuf/pydantic, and first read-only
container lacked writable/tmp; neither failed attempt is called a product test
failure or hidden. Final image test used the declared dependencies and tmpfs.

The existing host driver still intentionally rejects stage100. It was not
silently widened to reuse the stage50 acceptance budget. Required implementation:
explicit candidate-only stage100 configuration, complete probe/cold/startup quota
preflight, module/worker wiring, validated per-stream activity/ACK evidence and
real TS-clone consumption. Load execution and CPU/RAM sizing remain unfinished,
not a Paper dependency or an external approval excuse. This report closes only
the tested planner/design slice, NOT the100-alpha acceptance.

### Real Manifest Compile And Disk Stop

Parent repeated offline compile at sourcefa10d1b with actual source catalogue,
acquisition and both alpha manifests plus TS. Both profiles compile; active/
reserved counts remain230/276 and550/660. Alpha manifests currently declare
2400RPM/60streams each; both fail the proposed workload quota check. Assumed TS
requires60reserved streams versus source manifest50. This is NOT runtime quota
readback and NOT traffic. Docker--rm/networknone/readonly removed after success.

Before further load/build: root filesystem observed100%,229122048bytes free.
Restore measured disk headroom with an ownership-safe cleanup packet; no broad
prune while Claude may be building. No test or runtime data deleted to mask this.

## TS Ten MARK Streams Amendment - 2026-10-09

Status QUOTA_DESIGN_CONFIRMED / RUNTIME_NOT_ACTIVATED / CAPACITY_NOT_CERTIFIED.
This supersedes the50-stream assumption above for the proposed linear TS60
stream workload; it does not alter historical measurements.

Read-only runtime evidence: both deployed Stream replicas use image
`1f13408fe94698a45f18e59e61c6554e979308dcebfe21514a5d0534609a51e9`,
`QDL_KN_MAX_SUBSCRIPTIONS=384`, `QDL_KN_MAX_REPLAY_RPCS=32`, restart0/OOMfalse.
Read `/consumer-realms/bundle.json` inside active Stream: TS paper revision10,
max_streams50, RPM1500, max_buffer_events2000, max_batch_items50, max_warmup_rows2000.
Source confirms `consumers/stable/trading-system-paper.yaml:23`/`:26`.
Therefore **ten additional concurrent MARK streams are NOT admitted by the
current TS quota**. Existing route entitlement is not an extra stream slot.

| Quantity | Realistic100 | Stress5 x100 |
| --- | ---: | ---: |
| Alpha active streams | 180 | 500 |
| TS active streams, including ten MARK | 60 | 60 |
| Total active | 240 | 560 |
| Alpha reserved,1.2x | 216 | 600 |
| TS reserved,ceil(60x1.2) | 72 | 72 |
| Total reserved | 288 | 672 |

The existing planner with TSReservation(ts,60,60,1000) confirms both profiles;
new regression `test_ten_mark_streams_need_seventy_two_ts_slots` checks total,
reserved, quota shortfall22 and unchanged1500RPM candidate. The22 deficit is
72reserved minus50configured; immediate active deficit is10.
RPM1500 remains conditional on1000steadyrequests/minute including recovery and
probes. Do NOT subtract ten polling lanes without measuring actual caller changes.
The planner accounts startup/reconnect calls; quota is not latency acceptance.

Candidate request: TS max_streams72, no per-client buffer increase. For full
stress100, reserve672slots on **one** replica so peer loss fits; current384fails
by288reserved slots (176active). Existing modeled1024-slot plan fits arithmetically
with352reserved slots left, plus separately bounded32Replay. Its307MiB modeled
RAM is not a measurement. No CPU/RAM or quota activation was performed.
Alpha current2400RPM/60slots each still falls short of the stage100 candidates
4506/4509RPM+108slots(realistic),4950RPM+300slots(stress).

Pending IDR075 inverse products are not silently included in the linear60.
If all four additional products become simultaneous streams, TS64 needs77
reserved slots; stress total becomes564active/677reserved. Freeze the actual
caller subscription roster before rendering the coordinated manifest/bundle
revision and JWT/consumer config. Do not spend recovery headroom on uncounted
feeds or open a second probe on the shared TS identity.

Decision boundary: owner/Claude can use72 as the TS60 quota requirement in the
next reviewed packet. Current deployment cannot accept the extra10 unchanged.
Stage100 remains unmeasured, not blocked on Paper implementation. Approval to
plan100alphas is not evidence that the existing384slot runtime can serve them.

## Confirmed TS Demand And Joint RPM Decision - 2026-10-09

Status: D1_ROSTER_CONFIRMED_RPM_NOT_FROZEN. Supersedes the conditional72-slot
request above, not its historical evidence. Authority: Claude TS commits9ca122a
and e292195, upgrade/p184-phase-d-mark-quota-handoff.md, appended answers to
Codex86dd0ae. No deployment or fresh runtime measurement in this amendment.

- Only market_data_service_p184w uses trading-system.paper.stable, per Claude's
  runtime inventory. Linear50 + MARKwake10 + inverse4 =64 active streams.
- Proposed rev11 max_streams=77 (ceil(64*1.2)); r23 inverse is paired with
  MARKET_DATA_OKX_SYMBOLS activation by Claude. Existing50-slot grant is not enough.
- Realistic100:244 active/293 reserved; stress100:564 active/677 reserved.
  These are demand arithmetic, not measured capacity. One384-slot replica cannot
  carry stress100 during peer loss; no buffer increase is implied.
- Measured pre-wake identity traffic:900/898 quota units per complete minute,
  n=2, writer12be5f9. This is a baseline, not a busy-market upper bound.
- Fast BOOK retry now0.5/s/process:4*0.5*2*60=240 units/min. Two units cover
  snapshot and successful Subscribe; failed snapshot costs less. This excludes
  SDK in-session reconnects, initial subscription/resolve, and MARK/inverse delta.
  qdl_sdk/client.py reconnect default5 is per session, not a global minute cap.

Freeze requests_per_minute and max_streams together in the same rev11 packet.
Use quota units counted by the real shared identity limiter, not event counts:

    projected_peak = 900 + mark_delta + inverse_delta + 240
                     + sdk_reconnect_reserve + startup_resolve_reserve
                     + observer_reserve
    proposed_rpm = ceil(projected_peak * (1 + approved_headroom_fraction))

All terms are quota units/min. Subtract replaced polling only with paired
evidence; avoid counting it twice. Deduplicate overlapping reconnect/startup
paths when measured, or label their sum a conservative bound. Headroom fraction
is a packet decision, not a new acceptance gate. Do not silently retain1500RPM.

MARK delta is NOT_MEASURED. Observe canonical MARK wake rate per product and
evaluate actual TS reference batching/coalescing over complete minute windows.
Account for Query plus Subscribe across both replicas and fixed-minute reset
bursts. MARK event count alone does not establish charged requests or compute
capacity. Include inverse reference polls/resolve/wake, not just its four slots.
Use existing read-only observations or an approved bounded observer, never a
second Paper-identity stream. Record measured vs modeled terms separately and
verify the selected quota with the actual caller before calling D1 accepted.

Exit: numerical RPM with explicit inputs, shared-limiter namespace verified,
old/new bundle hashes, r23/JWT11 paired reader/writer packet, rollback to rev10
and r22/JWT10. Neither final RPM nor runtime activation is certified here.

## D1 Sizing Frozen For Rev11 Packet - 2026-10-09

D1_PACKET_COMPLETE_NOT_APPLIED. Supersedes RPM_NOT_FROZEN above. TS f457e7a
bounds MARK wake with DATA_LAYER_V2_MARK_WAKE_MIN_INTERVAL_MS=500, constructor
range100..1000ms. Pin500 in the paired TS packet; parser accepting100 does not
make a100ms configuration fit this budget. Wake and poll share the read cadence.
No provider event-rate measurement or900-request decomposition is needed to
select the conservative3000RPM option requested by owner.

| Component | Quota units/min | Evidence class |
| --- | ---: | --- |
| Existing traffic counted conservatively as non-MARK | 900 | Measured total900/898,n2; not a hard future bound |
| Linear MARK | 1200 | 10symbols x2reads/s x60; modeled unbatched ceiling |
| Inverse MARK | 120 | 1symbol x2reads/s x60; modeled unbatched ceiling |
| Fast BOOK retry | 240 | 4processes x0.5/s x2units x60 |
| Incremental probes/reconnects | 20 | Planning allowance, not a storm bound |
| Total planning load | 2480 | Conservative steady/retry scenario, not measured capacity |
| Selected requests_per_minute | 3000 | Packet decision |
| Remaining reserve | 520 | 20.97% above planning load;17.33% of quota |

max_streams=77 for64 active. The64/77 and stage100244/293 or564/677 arithmetic
is unchanged.3000 replaces1500 in the proposed Paper rev11 only; no other
consumer, buffer, CPU/RAM limit or freshness gate changes. Do not choose2400
without measured non-MARK<=300 decomposition and a new reviewed budget.

Review caveat: reference_batch costs one authenticated HTTP call; batching can
reduce charges. SDK resubscriptions, cold resolve, fixed-minute edge and startup
can consume the520 reserve; repeated failures are not bounded by the20 estimate.
Stagger startup and measure real shared Query+Stream minute counters,429 and
reconnect counts in the approved window. Do not disable limits or open a second
Paper probe to force admission. Quota admission is not stage100 capacity proof.
Paired execution/rollback checklist: p184-dl-phase-c-packet.md, D1 rev11 section.
