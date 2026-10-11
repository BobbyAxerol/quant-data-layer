# DR08 Continuity Attribution And Recovery Hardening

Status IMPLEMENTED_TESTED_ISOLATED / TRACE_PARTIAL / NOT_RUNTIME_CLOSED, 2026-10-08. Owner scope is three steps:
freeze source/runtime, bounded passive attribution, isolated DR08 correction.
Main journal: DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md, DR08 continuity entry.
No TS edits, shared rollout, new TS subscription, orders, push/merge or release.

## Runtime And Ownership

Canonical DL main5bf5cd5/v2.2.2; feature fix/p184-dl-closure base d68fb2f.
Claude TS source8b41608 owns OR plus IDR078/081; do not amend his worktree.
Writer sha256:5849d7167561273af08bda2103c38cec5f4480f7a8809ea592c57abff7716ea1,
SDK2.0.7, started2026-10-07T19:15:49Z. Stream replicas image
sha256:1f13408fe94698a45f18e59e61c6554e979308dcebfe21514a5d0534609a51e9.
Both Stream and writer restart0/OOMfalse. Core658a9570, Query/BAR51450612,
projectora696cfba unchanged. Pending worker logging/offered/dequeued source is
not this deployed pair. TS trace absolute expiry1791401054410448004 is past;
read-only inspection cannot reactivate SDK/apply/ACK capture. No silent restart.

## Incident And Passive Trace

Previous read-only finding retained: about15min overflow+59/replay+84263/expired+4;
TRADE queue1000/replay10000/OUT_OF_RANGE; BOOK_DELTA ~7.9MB exhausted. TS cache
600/600present, delta source-age max8288.56ms. Not equivalent to usable data.
New bounded passive trace uses existing readonly_kafka_observer.py --mode canonical
--seconds90 --max-records2000 --sample-ms100 --launch, assigned read_committed,
no group join/commit. Cache MGET four ETH BOOK keys every500ms for180rounds,
no subscriptions, payloads, writes or key changes. Source identities/offsets only.
Evidence root ~/.local/state/qdl-v2/dr08-continuity-20261008/.
1465canonical observations,720cache reads,380unique matched product/source/
authority/timestamp/offset events,0identity mismatch. Generation independently
matches all596joined poll rows (repeated offsets included). Native sequence uses
different encoded representations; no assertion of scalar sequence equality.

| Feed/venue | Unique n | Event -> external cache observation p50/p95/max ms |
| --- | ---: | --- |
| BOOK_SNAPSHOT Binance ETH |83|704.880 /1028.558 /1228.310|
| BOOK_SNAPSHOT OKX ETH |85|609.930 /1116.752 /1419.858|
| BOOK_DELTA Binance ETH |101|5033.440 /9615.223 /9971.622|
| BOOK_DELTA OKX ETH |111|4555.829 /7690.110 /8554.624|

This is first external visibility sampled within the window, NOT Redis ACK,
canonical commit or execution-eligibility proof. Initial sampling and polling
add delay. Canonical observer -> cache poll delta p50 is4830.661/4329.095ms
for Binance/OKX delta, respectively. It narrows the path after durable visibility
but does not separate transport, SDK and TS processing. It also shows why BTC
worker ownership alone cannot explain all ETH BOOK observations.

Stream aggregate interval13:31:31->13:37:31UTC:258943delivered,0new overflow,
0replay/expired. Queue-wait buckets all<=50ms, handoff buckets all<=20ms;
producer CreateTime->dispatch all<=200ms. These are aggregate bucket bounds,
NOT exact subscription/ACK trace or a retroactive explanation for prior errors.
Unmeasured on current writer: Stream per-event enqueue/dequeue, SDK return,
RedisACK, cursor persist. No production root-cause closure from these aggregates.

Evidence hashes:
- runtime-observation-01.json:2feb47115d7f0f17ba00ea5f10d5cf78d2e58c36098590ae688e546b779cb15b
- runtime-observation-02.json:db7b5c272c21baaad50603d2dea0b9a05467f820039477cc8684cb8a0e5ae537
- cache-eth-paired-02.json:6beaa8be443900bfbc6ddcc5685612e09f8a4b7dc75c8b5fa5d322fbe6945295
- paired-join-02.json:8ffdce5206d9ca5c4f2fda56b2c50c649990757c3a1d7365efd31d28b034cc0d

## Isolated DR08 Reproduction

Fixed a harness false-positive: stopping at10000 replay events did not prove
LIVE. Now require LIVE with exactly10000 events before publishing marker10001,
then prove its ordered delivery; backlog10001 separately returns OUT_OF_RANGE.
Unit43PASS0fail0skip (python3 -B -m unittest discover -s tests -p
test_p184_dr08_isolated.py -v). Runtime --orchestrate replay-boundary-01,
--development --restart-samples5 with dummy TS provenance (never final evidence):
all4development checks PASS on existing Stream image, candidate Queryrc2 explicitly
named. Queue observed1000/4719110bytes, sampled aggregate peak4955925bytes;
independent fast consumer10001events ordered,64progress observations while slow
stopped. Exact10000->LIVE->marker PASS. OUT_OF_RANGE->snapshot->firstevent
7347.908ms(n1); five independent consumer restarts p50 5516.627/max5545.290ms.
These are gRPC receive boundaries, NOT TS apply/RedisACK. Input SYNTHETIC_TEST_ONLY;
not provider, production, BOOK or lossless recovery after expired backlog proof.

New Rust finding: a full outbound handoff can time out sending both control and
terminal status; old task can exit with clean EOF. Narrow candidate preserves
one terminal status outside the bounded data channel and drains prior data in
order. Both new prolonged TRADE/BOOK_DELTA stall regressions failed before the change
(10.31s, missing terminal), then passed. Final Rust suites39library+37native
PASS0fail0ignored,0.17s/12.60s. Direct-service tests, not prolonged-stall wire
proof. Test log ../dr08-terminal-20261008/final-tests.log SHA256
0b32adeb22227569702487378fb6b7e1482a1f49812d252b3e775ad997ba99f6.
Final harness46PASS0fail0skip; extra negatives forbid relabelling binary overlay
as final qualification and detect changed executable bytes.

Candidate executableb5e1c2c48d4009e1a3861bcb7e80554afb89c3d24e38ebe9fced3860dfb01325
was copied into a second isolated stack (candidate-wire-02), existing immutable
base image unchanged. Actual Kafka/core/gRPC DR08 all4development checks PASS:
queue1000/4719110bytes, sampled aggregate peak4941708bytes, fast10001events ordered
and69progress observations while slow stopped, replay10000->LIVE->new marker,
10001 backlog->OUT_OF_RANGE. Five restarts p50 5400.475/max5542.696ms;
range->snapshot->firstevent7063.953ms(n1). No TS RedisACK measurement.
Wire receipt41da3328df76b7d8ffa15cd533f0b310842fd80fa01df85f545a63a777188085.
This tests TRADE gRPC; BOOK_DELTA prolonged stall remains direct-service proof.
The two last harness-only negative checks do not change the wire workload.
Do not claim this source defect caused all observed runtime stalls.

## Cleanup And Remaining Work

DR08 cleanup shared_runtime_unchanged=true, failures0, all8owned container names
retired, private material removed by orchestrator. No images built/removed.
Disk free53510434816->52509847552bytes; concurrent host activity, not attributable
to this run. Both passive observers finished. Active/rollbackBARf7351c3b retained.
Candidate stack cleanup unchanged=true/errors0; all8owned names retired,
private files and staging executable removed. Cleanup receipt
 d235cadc1161b4e88ac462e27db706de5b73b6f18e07c9be6e536d90a2a03ff6.
Candidate stack free45993418752->45420568576bytes (concurrent host activity).
Agent removed exact private Cargo target1480911808bytes; free45976342528 ->
47115079680bytes. Retained one121770368byte candidate executable for review,
not a production image. Docker inventory95images28.96GB/cache467entries20.52GB;
no test images built, no shared prune during Claude's builds. No dr08 containers
remain. Agent closed; observer/driver processes completed.

Claude handoff: retain IDR081, but trace ETH BOOK_DELTA as well as BTC TRADE on
the intended writer. New worker logs and bounded SDK->ACK trace must run together
with canonical/Stream counters; current expired trace cannot supply those spans.
No ownership repartition, timestamp relaxation, larger buffers or DATA_STALE
relabeling is implemented here. DR08 final and v2.2.3 remain HOLD.


## Three-Step Exit Accounting

1. Finding/runtime freeze DONE; incident linked to TS078/081 without touching TS.
2. Passive trace PARTIAL:380exact canonical/cache joins, but current writer lacks
active SDK/apply/ACK/cursor capture. No complete seven-stage causal attribution.
3. Narrow recovery defect IMPLEMENTED_TESTED_ISOLATED; production backlog root
cause and full TS crash-between-apply/ACK evidence remain OPEN. Existing direct
service replay tests do not replace writer/Redis crash recovery acceptance.
Next permitted coordination: Claude's intended writer logging/trace (bounded
activation in approved writer packet), then exact same-event join and affected
runtime acceptance; no C2/full catalogue/strategy retest. No new freshness gate.
