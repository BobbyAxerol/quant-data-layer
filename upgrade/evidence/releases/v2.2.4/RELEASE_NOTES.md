# Quant Data Layer v2.2.4 (DRAFT)

Status: **DRAFT - not certified, not published.** Updated 2026-10-17 ~11:00Z (file edits only, host lock held by
another agent's G13 v5.4 window). The release is certified only when [CERTIFICATE.md](CERTIFICATE.md) can be turned
into `certificate.json` with status PASS. Every `<<PENDING:...>>` marker below names the receipt that fills it; the
published file must contain none (`grep -rn '<<PENDING' upgrade/evidence/releases/v2.2.4 --exclude-dir=drafts` prints
nothing). Machine inventory: [release-preparation.json](release-preparation.json). Ordered steps:
[FINAL_STEPS.md](FINAL_STEPS.md). Gate table: [SCORECARD.md](SCORECARD.md). Per-role equivalence:
[equivalence.json](equivalence.json).

- Previous stable release: v2.2.3 (source `481fd67`, main merge `28e605e`, SDK 2.3.0).
- Publication path (unchanged): required remote CI on the final head, `fix/p184-dl-closure` -> `dev` PR, release PR
  `dev` -> `main`, annotated tag `v2.2.4` on the main merge commit. `.github/workflows/release.yml` checks main ancestry,
  the certificate status, the endpoint-report hash and the SDK wheel hash.
- Scope: the Data Layer release only. Not a Trading System release. Not HA (single host).

## Scope

134 commits since `481fd67` at branch head `5dfabe6` (`git rev-list --count 481fd67..HEAD`; recompute at tag). Runtime
paths changed: 60 files, +12422/-924 (`git diff --stat 481fd67 HEAD -- rust qdl qdl_sdk contracts generated config
consumers Cargo.toml Cargo.lock 'Dockerfile*' app`). Grouped by runtime area:

- **Core rebalance, rewind and Kafka client** (rust_core, shared `qdl-kafka`): pipelined transaction commit
  (IDR-094, a962a5c), K1/K3 stop intent and epoch abort (bcb0283), capacity step 1 (98b421f: producer queue, ingestor
  single producer and DOM-free routing, A1 lazy canonical JSON, B1 TCP_NODELAY, raw_age split), batch constant 1024
  (abfcc9f), consumer session timeout 10 s (f8e5442), same-consumer rewind on a rebalance abort with heartbeat 3 s
  (e19a9f6), typed rewind failure and epoch fence (RB1, 32fe603), native Kafka error classification (DL-R1, a4e7b3b).
- **Query**: G5 BAR warmup cold path renders validated rows directly and authorizes once per source (4d3476c,
  byte-identical output), typed `CANONICAL_LINEAGE_MISMATCH` (IDR-099, 00056e5), `retry_after_ms` 2000 on a full
  large-warmup queue (b7f80eb), Binance current OI entitlement and UTC-grid / settlement-grid coverage
  (IDR-090/091/092, 2524732).
- **Stream gateway** (N1-OPT and isolation): bounded replay batching and wake-ups (ed1b85b, aa1f98f), bounded
  near-frontier registration and catch-up (f0242dc, 3defbbb, 2743938), stalled-peer isolation and preemption
  (b3fdd43, c140e7e), copy-on-write subscriber lists (8ddba09), IDR-096 replay-budget status opt-in (f5cd787),
  LIVE carries the REPLAYING high watermark again (ffa382f), live-budget reserve for subscriptions at their floor
  (e3b5552), exact outstanding-record floor and attributable handoff refusals (Codex SR1/SR4, c934c1e), gateway
  dispatch/dequeue/send clocks (a388537).
- **Projector**: IDR-107 bounded retry of transient poll errors (58fb7cd) with an allowlist and commit-only budget
  reset (Codex CF3/CF4, ff5683c).
- **Kafka A' contract**: `md.latest.v2` carries no tombstones (bb15890: budget `state_topic_tombstones` v1, golden
  invariant, Rust guard test); operator tools for the cleaner I/O throttle (2eb77f8), the marker retention (c767b91),
  the internal-topic compaction plan C (2542cf0, 68556d4, c7c0355, cd3fd3a, c5aabbd), the preferred-leader election
  (32d2cf6) and the decision-B roll (e955324, 8064025). All dry-run by default.
- **Apparatus and oracles** (scripts/tests only, no product path): G4 observer v2-v5, G9/G10/G11 native-writer
  oracles (Codex SR2/SR3/SR5), G9/TS sampler heartbeat dedupe (1e09f87), G12 oracle v3 and WriterClone lifecycle
  (d54264e, 945b99f, 968d5f2, 965f080, 3bb6bee), core-group harness (simultaneous replace), DR08 isolation campaign and
  deterministic starvation cell (2169c26), DR05 harness (f7d6afa, d03cd67, 8ae4266), G13 runner fixes, the isolated
  DR05 quiet-session producer for DR05.QUIET_TYPED (6506d01, c5d3610), host-measurement lock wrapper, release
  image recipe with section-level binary verdicts (46f4628, 696fa3d).

## Defects fixed in this release (register: Trading System `INTEGRATION_DEFECT_REGISTER.md`)

| ID | Defect | Fix | Where it ships |
| --- | --- | --- | --- |
| IDR-090 | Binance current OPEN_INTEREST missing from the alpha entitlement | demand rev 4, reference-l2 rev 7, alpha-binance rev 16, routing rev 28 (2524732) | config baked into the python image; consumer-side JWT rev 16 / realm r16 is a deployment item |
| IDR-091 | Binance sampled-statistics history failed coverage on rolling windows not aligned to UTC midnight | cadence and edges on the UTC epoch grid (2524732) | python image |
| IDR-092 | Binance funding-rate history returned PARTIAL_RESULT for a rolling 3-day window | settlement-grid coverage (2524732) | python image |
| IDR-094 | rust_core held each batch until its Kafka transaction committed (stop-and-wait) | pipelined commit (a962a5c), K1/K3 stop intent and epoch abort (bcb0283) | rust_core (kept on rc22, binary equal) |
| IDR-095 | possible gateway-side delay after a re-read | mitigation only: TCP_NODELAY (98b421f). The delay itself stays unmeasured | rust image (gateway) |
| IDR-096 | replay work-budget status changed to RESOURCE_EXHAUSTED for existing clients | opt-in `SubscribeRequest.replay_budget_status_v2` (f5cd787); SDK sends it, progress-aware reconnect budget (bb75d06) | rust image + SDK 2.4.1 |
| IDR-099 | untyped canonical-lineage refusal on the Query warmup path | typed `CANONICAL_LINEAGE_MISMATCH` (00056e5) | python image |
| IDR-107 | a transient consumer poll error killed the projector (`exit 2`) | bounded retry with an allowlist, commit-only budget reset (58fb7cd, ff5683c) | rust image (projector) |

Already in v2.2.3 and unchanged here: IDR-086, IDR-087, IDR-088. IDR-089 (reader tolerance for additive fields): see
Public Interface for the minimum reader version.

Trading System items measured with this release, owned by the TS release (not shipped here): IDR-100/101/102/105/106,
IDR-110 and IDR-110b (native writer probe vs the D27 view boundary; both fixes in the TS writer, not in the Data
Layer), IDR-109 and IDR-111 (apparatus).

## Public Interface

Every change is additive (owner rule 2026-10-07). Endpoint versions are unchanged:
- `StreamRecord.gateway_dispatch_wall_ns = 3`, `gateway_dequeue_wall_ns = 4`, `gateway_send_wall_ns = 5`
  (diagnostics only; 0 = not measured).
- `SubscribeRequest.replay_budget_status_v2 = 8` (bool). Absent keeps the pre-2.4.1 `OUT_OF_RANGE CURSOR_EXPIRED`
  mapping; when set, replay work budgets end with retryable `RESOURCE_EXHAUSTED RATE_LIMITED:REPLAY_*_LIMIT`.
- No field number is reused or removed.
- Minimum response reader: `qdl-sdk >= 2.1.0` (readers must ignore unknown response fields; `MarketDataView.event_id`
  has been rendered since v2.2.3 and caused IDR-089 in an older strict reader). Check deployed reader SDK versions before
  shipping an additive field.

SDK **2.4.1**, wheel sha256 `9f79e032604bf79db5ca9070c7f8a99b242e7cf22e1659b395b307dc798c28e7` (two `--network none`
builds at REV `c7ce7d3`, byte-identical; `generated_contract_digest`
`2ee96b1edab01d3185e4fb973f7e3a0715956f8648938039108eb6156187c116`), superseding 2.4.0 (vendored by TS, never published
by a DL release): gateway clocks on `StreamEvent` (optional, not part of equality), the field-8 opt-in, and reconnect
budgets renewed only on new progress. A caller that uses `GrpcStreamTransport` directly now sees `RESOURCE_EXHAUSTED`
where 2.4.0 saw `OUT_OF_RANGE` for those budgets. The TS writer in this release vendors 2.4.0; re-vendoring 2.4.1 is a
separate reviewed TS delta.

## Source And Images

- Release source: `c7ce7d3087cb8a2daf82ba941982bd296d488f45` on `fix/p184-dl-closure`. Later commits only touch
  scripts, tests and docs (`git diff --stat c7ce7d3 5dfabe6 -- rust qdl qdl_sdk contracts generated config consumers
  Cargo.toml Cargo.lock 'Dockerfile*' app` is empty). Release head carrying the certificate:
  `<<PENDING:DL_RELEASE_HEAD_SHA>>`.
- Build: `python3 scripts/build_qdl_release_images.py --build --lock-owner claude-release-build` under lock
  15:42:10-15:48:45Z on 2026-10-16. Receipt `~/.local/state/qdl-v2/build-v2.2.4-c7ce7d3/receipt.json` sha256
  `40d727b28a3a581eb9c5911e150a39ae0dac68d8d0a9afbb5cceee47b4cb4f7f`.
  **The receipt status is FAIL and is kept.** Two whole-file expectations failed (projector vs rc22, gateway vs
  baa72c96). A coordinator, owner-delegated decision (2026-10-16) replaced them with a section-level verdict and a
  build-path classification backed by a behavioural proof, without a rebuild. Addendum
  `postcheck-addendum.json` sha256 `dad31fe8dc7f768de72e7577951af61d726689d8b32fcf5153046fd9bb190260`.
- Rust `qdl-v2-rust:2.2.4-c7ce7d3` `sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e`:
  12 binaries compiled in builder `551428d9` and copied onto rc16 `f41ecb66` (the owner-approved thin method). Revision
  label = REV (verified with `docker image inspect`).
- Python `qdl-v2-python:2.2.4-c7ce7d3`
  `sha256:c21e164ac4db14af498b73104e344f003fa7117fdae2e465f50df8668cdca252`: recipe build of `Dockerfile` at REV. Revision
  label = REV (verified).
- Production roll 2026-10-16 17:14-17:53Z, one container at a time, saved rollback composes, compose diff `['image']`,
  RestartCount 0, writer READY 64/64 gates (summary `eefb041c5ece53c17d28148eb3fa1c2e6be64f84b79ab20e6442f1bd44c0b271`).

| Role | Measured before the release (stage-100 #3 / pre-release) | Release | Equivalence (post-check) |
| --- | --- | --- | --- |
| rust_core x3 | rc22 `7c848aad` (a4e7b3b) | **kept on rc22** (owner option B5a) | `qdl-realtime-core` `2d82167336a4268a9407d565325f656f2315632b26bdd18bd4527e73dd15e475` EQUAL to rc22 |
| stream_kn_1/2 | `baa72c96` (TEST overlay, `qdl.test.only`, gateway c934c1e) | rust `60f83a51` | gateway DIFFERENT_BUILD_PATH (986 registry path strings, same .text size 8,202,831 B), source tree empty-diff since c934c1e, DR08 live-budget isolation A/B PASS (prereg `d22561fe...`, verdict `137ab779...`) |
| market_projector_1/2 | rc14 `ce629061` | rust `60f83a51` | differs from rc14 by design (IDR-107, CF3/CF4); every loaded ELF section equals rc22 `efeb4728`; `.symtab`, `.strtab`, build-id differ |
| ingestor_okx_swap / binance_usdm | rc18 `7fba2577` | rust `60f83a51` | differs from rc18 by design (qdl-kafka DL-R1/RB1); `qdl-native-raw-ingestor` equals rc22 `77ceeee1c2484db3f40f237040441878969dc3558709de282dfc4fe2ecb1ddbb` |
| query_kn_1/2 | rc19 `b9ca6c36` | python `c21e164a` | 438 vs 438 files; only the 9 git-derived files differ (SDK files, query_pb2 field 8, budget config, `phase3_consumer_load.py` apparatus); pip freeze equal |
| binance_bar_edge | rc14 `08f356a8` | unchanged (HELD) | not on the release revision |

The final stage-100 measures the release digests directly (`<<PENDING:S100_DIR>>`), so the "measured before" column
documents the equivalence evidence, not the gate numbers.

## Runtime Config

- Kafka log-cleaner I/O throttle: cluster dynamic default **4 MiB/s** (`log.cleaner.io.max.bytes.per.second=4194304`)
  since 2026-10-14 12:21:52Z. **Correction:** the v2.2.3 notes say 8 MiB/s. That was true when v2.2.3 was published
  (applied 2026-10-11T03:40:56Z) and has been stale since 2026-10-14; the published v2.2.3 notes are not rewritten.
  Receipt for the 4 MiB/s value: `credit-snapshot.json` (cleaner `default` 4194304 on a read at 2026-10-17T09:16:16Z),
  sha256 `6bb2021ab3e51065bf54697a7f424991fc4125bd29fb4ac0536597be9f4ff2fe`.
- `md.latest.v2` `delete.retention.ms` 604800000 -> 21600000 (A', 2026-10-16T02:05:37Z, contract bb15890; rollback
  `--rollback-to 604800000 --apply`). Its effect arrives gradually (up to 7 d for markers already stamped).
- Kafka internal topics (decision **C**, owner "B + C 10 min", 2026-10-16): `__consumer_offsets`
  `delete.retention.ms=600000` + `segment.ms=60000`, `__transaction_state` `segment.ms=600000`, applied dynamically
  ~09:2xZ with `scripts/kafka_internal_topic_compaction.py --apply`. Convergence is estimated at ~49 h. Rollback:
  `--rollback --apply --cert-dir <stable cert dir>`.
- Temporary `min.cleanable.dirty.ratio=0.05` on `__consumer_offsets` only (owner-approved): applied 12:54:49Z, removed
  14:12Z on 2026-10-16. `__consumer_offsets-43` shrank from ~1.65 GB / 18.4M records to ~230-260 MB / ~2.95-3.2M
  records on all three replicas. Group offsets before and after: 5/5 groups, none missing or decreased.
- Kafka `auto.leader.rebalance.enable=false` (decision **B**): applied 2026-10-16 by sequential recreate (kafka1 14:14Z,
  kafka2 14:21Z, kafka3 14:25Z). Each roll passed a manual HOLD gate; core commit stall max per roll 2.5 / 3.3 / 5.2 s.
  Receipts `~/.local/state/qdl-v2/p184-kafka-b-20261016/`. Rollback, one broker at a time:
  `scripts/kafka_auto_leader_rebalance_roll.py --rollback <packet> --broker kafkaN --apply`.
- Effective production describe at the G12 credit: sha256
  `d3d77176c3dfca46e7f44cba465d271afe2709a1fc9d97f75e808808fa959094` (B x3, C, A', cleaner 4 MiB/s, no temporary key).
  The describe after B (2026-10-16 14:28:49Z, `effective.sha256` `f4836dfc...`) uses schema v1, which has no
  cleaner field; the credit describe uses schema v2, which records the cleaner default, so the two hashes differ.
- Kafka CPU: kafka1 1.25 -> 1.75 vCPU (2026-10-14 13:47:40Z, docker update); kafka2/3 already 1.75.
- Core config `releases/v2.2.3-rc18-core-batch1024` (batch 1024), ingestor_binance_usdm 0.75 vCPU (2026-10-15).

## Findings During Preparation

- **Coordinator load (G12 broker-restart mechanism).** Production `__consumer_offsets-43` held 1.65 GB (14.31M markers,
  4.07M offset-commit records). The first real coordinator load after the cleaner pass was measured at **4,783 ms**
  (3,201,999 records / 267 MB, 1,593 ms of it queued behind `__consumer_offsets-24`) on 2026-10-16 14:25Z, replacing a
  ~37.5 s projection that was for the 18.4M-record partition. Source:
  `roll-kafka3-20261016T142516Z/coordinator-loads.json` sha256
  `a7c0a8661482a4c30ebc303c8d9d4992c62e60a9cf317ddc20c3ed78dd26f473`. A coordinator move costs ~5 s today and falls as C
  converges.
- **IDR-108 (OPEN).** `stream_kn` accepts on 8210 before its hot view serves. In the TEST_ONLY measurement all 10
  bindings served at port-ready +356.6 ms, and one QUOTE `DATA_NOT_READY` came at +39.7 ms. The v2.2.4 source does not
  change this. It is a stated limit, not a gate.
- **Harness parity.** The isolated TEST harness capped its Redis at 0.5 CPU while production `redis_marketdata` is
  unlimited; isolated capacity cells ran with the cap removed (1.0 CPU). Frozen rules unchanged.
- **Stage-A key coalescing.** Measured, report-only, not designed: 29-48 % of records in a stage-A transaction repeat a
  key (TRADE 79-88 %).

## TEST_ONLY Burst Results (not release gates)

Internal capacity on production image pins in isolated namespaces (raw publish to Redis ACK and cursor; not
provider, not venue latency, fresh brokers):
- Ladder on writer B16: 7/8 CLEAN; `b16-9k-2` NOT_CLEAN on rule S (one overflow).
- Amendment-1 (writer Subscribe depth 2000): 4/5 CLEAN; `a1-corepause9k` NOT_CLEAN on rule S.
- Drain-rate diagnosis: the binding stage was the harness Redis CPU cap (~11.3k/s at 0.5 CPU, ~13.3k/s at 1.0 CPU).
- Parity rerun with the cap removed: 4/5 CLEAN; `a1-streamrs9k` NOT_CLEAN `DATA_NOT_READY_AFTER_UPSTREAM_READY`
  (IDR-108); stream-serving addendum `a1s-streamrs9k` CLEAN.
- Closure on the Redis-cap-removed profile with writer B16: 9k no-fault x2, writer pause, core pause 700 ms and stream
  restart are CLEAN. Every earlier NOT_CLEAN receipt is kept. These results are not production capacity claims.

## Gate Scorecard (G1-G14)

Full table, evidence and PENDING markers: [SCORECARD.md](SCORECARD.md). Frozen gate table sha256
`c67bf69bb921c992964eb8ff8b2e1bb17085a943fdf756d55526b6269641d5c4`; no gate was widened; a NOT_MEASURED is never
upgraded.

- G12: **PASS, credited** (final, writer `a44349a1`, verdict `30648759e5766f5e085e1ea3874efe2ab69ceca0475fcca2ab23e3016b67d1be`).
- G13: isolated 12/12 PASS; runtime window v5.4 `<<PENDING:G13_V54_W05_CELLS_CLOSED>>` of 76.
- G1-G11, G14: `<<PENDING:S100_OUTCOME>>` (final stage-100 on the release digests).
- Summary: PASS `<<PENDING:SUMMARY_PASS>>`; FAIL `<<PENDING:SUMMARY_FAIL>>`; NOT_MEASURED
  `<<PENDING:SUMMARY_NOT_MEASURED>>`; DEFERRED `<<PENDING:SUMMARY_DEFERRED>>`.

## Known Residuals And Deferred Items

| Item | State | Owner / next step |
| --- | --- | --- |
| H1 (G12 WriterClone lifecycle owner) | ACCEPTED-DEFERRED. The G12 credit used the `77a6d671` WriterClone apparatus pin because the later WriterClone refuses `keep_exited=False`. Unifying the lifecycle owner and a mocked WriterClone integration test follow after the release. | TS follow-up; no gate change |
| IDR-110 R1 (no record identity after a native restart) | fixed in IDR-110b (`a44349a1`): identity seeded from the verified Redis marker; tested locally and in the isolated quiet-key proof run2 (`e4074aa0...`) | monitored in the final stage-100 |
| IDR-110 R2 (non-eligible Query QUOTE ended the read loop) | fixed in IDR-110b: DATA_STALE / DATA_NOT_READY on a probe marks only that route SESSION_UNVERIFIED; `probes_failed` must stay 0 in the final stage-100 | monitored; fall back to SkippedOffset if it recurs |
| All-quiet `DATA_NOT_READY` | diagnostic only. In the isolated DR05 stack the QUOTE route turned SESSION_UNVERIFIED about 14 s after the last frame (Query detail "required data is not available"). Not reproduced on the release path with valid session evidence, so no DL defect is recorded and DL is unchanged. | revisit only if it reproduces |
| IDR-108 | OPEN. Candidate fix: a readiness signal that turns SERVING only after the hot reader progressed and confirmed the broker head. | next DL release |
| Rust Shadow Core CI (TS) | red before this work (`fmt` / `clippy` and wheel-matrix steps), not a required check. clippy and fmt were not run (the offline image has no component). Slice deferred. | TS CI follow-up |
| Python debt ratchet | about 276 legacy ruff findings in the TS tree (coordinator figure, **not re-measured here**: ruff is not installed on the host); `scripts/p184_n1_isolated.py:605` is reported as F821 (line read: `before = await read()`), **not verified** | TS cleanup |
| Stage-A key coalescing | measured, report-only, not designed | DL backlog |
| Build receipt FAIL | kept; resolved by the postcheck addendum (see Source And Images) | none |
| TS writer vendors SDK 2.4.0 | release this writer on 2.4.0; re-vendor 2.4.1 later | TS delta |
| Kafka C | converging (~49 h estimate from 2026-10-16) | watch coordinator load |

## Limits

- Single host for every run: no HA, no independent failure domains, no mainnet or funded claim.
- `binance_bar_edge` is not on the release revision. rust_core x3 stay on rc22 and are certified by binary equality.
- The stream gateway and projector are certified by classification plus behavioural proof, not byte equality (see the
  equivalence table).
- Kafka C converging at measurement time; B applied 2026-10-16 14:25Z.
- IDR-108: after a restart, Stream readiness (port accept) comes before hot-view serving.
- G12 harness shape is not production shape (core members, Kafka CPU, topic segment settings, synthetic publisher).
- TEST_ONLY burst results above are not production capacity claims.
- `<<PENDING:LIMIT_G4_G9_FINAL>>` (final stage-100 outcome for G4 and G9).
- `<<PENDING:LIMIT_G13_OPEN_CELLS>>` (open G13 cells, if any, carried by an owner decision).

## Rollback

Per role, failure only, one container at a time with the saved composes in
`~/.local/state/qdl-v2/releases/v2.2.4/rollout/` (query receipts in `.../p184-d3-capacity-20261009/rollout-rc15/`):
query `b9ca6c36`, stream `baa72c96`, projector `ce629061`, ingestor `7fba2577`. Cores were not changed. Kafka config:
cleaner `--add-config log.cleaner.io.max.bytes.per.second=8388608`; marker retention
`--rollback-to 604800000 --apply`; internal topics `scripts/kafka_internal_topic_compaction.py --rollback --apply
--cert-dir <stable cert dir>`. Not touched: kafka1-3 images, rust_core x3, binance_bar_edge, market_cache, stable_redis,
catalog, consumer manifests, JWT revisions.
