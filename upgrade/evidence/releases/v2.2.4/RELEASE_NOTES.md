# Quant Data Layer v2.2.4

Status: **CERTIFICATE PASS UNDER A POST-HOC OWNER AMENDMENT (A4); NOT TAGGED, NOT PUBLISHED.** First assembled
2026-10-18 ~13:40Z with certificate status FAIL. Re-issued ~15:30Z.

- The final stage-100 met its protocol and passed G1 G3 G5-G11 G14. G12 (credited) and G13 (76/76) passed on the final
  image set.
- Under the frozen rule (G4-A1 + G4-A2 + A3, all frozen before the run), **G2 and G4 FAIL** on the rows named in
  [SCORECARD.md](SCORECARD.md). These are internal latency tails under the stage-100 load.
- On 2026-10-18 ~14:45Z the owner adopted **A4**, labelled `POST_HOC_OWNER_DECISION`. It was decided after the run was
  scored:
  - G4 OKX_INVERSE rows are gated by the pooled OKX p95 limits, with the max report-only and every outlier retained.
  - G2 MARK BINANCE max is <= 1200 ms (0.6 x the TS 2000 ms budget).
- Under A1-A4 every gate PASSES, so [certificate.json](certificate.json) has status **PASS** with a required
  `amendments` list in which A4 is `post_hoc: true`.
- The FAIL verdicts without A4 are kept next to the A4 verdicts everywhere.
- The latency itself is unchanged. The optimisation package (IDR-116..IDR-119) stays open, one variable at a time with
  A/B in a later release.

`.github/workflows/release.yml` accepts status PASS. It was not changed. No tag was created; a tag needs a merge into
`main`, green required CI and an explicit owner instruction.

Machine files: [certificate.json](certificate.json), [scorecard.json](scorecard.json),
[endpoint-report.json](endpoint-report.json), [equivalence.json](equivalence.json),
[release-preparation.json](release-preparation.json) (preparation packet of 2026-10-16, kept as written).

- Previous stable release: v2.2.3 (source `481fd67`, main merge `28e605e`, SDK 2.3.0).
- Scope: the Data Layer release only. Not a Trading System release. Not HA (single host).

## Scope

Commits since `481fd67`: 177 at `0c55957` plus the packaging commit (`git rev-list --count 481fd67..HEAD`). Runtime paths
changed: 68 files, +15644/-950 (`git diff --shortstat 481fd67 0c55957 -- rust qdl qdl_sdk contracts generated config
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
- **Ingestor** (OKX lanes): IDR-112 venue-timestamp staleness recycle (6d61710) and review F1-F3 (83bef59).
- **Query stall**: IDR-114 per-request cycle removal and `qdl.stall` telemetry (31a5155).
- **Projector**: IDR-107 bounded retry of transient poll errors (58fb7cd) with an allowlist and commit-only budget
  reset (Codex CF3/CF4, ff5683c).
- **Kafka A' contract**: `md.latest.v2` carries no tombstones (bb15890: budget `state_topic_tombstones` v1, golden
  invariant, Rust guard test); operator tools for the cleaner I/O throttle (2eb77f8), the marker retention (c767b91),
  the internal-topic compaction plan C (2542cf0, 68556d4, c7c0355, cd3fd3a, c5aabbd), the preferred-leader election
  (32d2cf6) and the decision-B roll (e955324, 8064025). All dry-run by default.
- **Apparatus and oracles** (scripts/tests only, no product path): G4 observer v2-v7, G4-A1/A2/A3 amendments and the books/Binance probes, the A1 reference-bound parity and exact-client smoke, G9/G10/G11 native-writer
  oracles (Codex SR2/SR3/SR5), G9/TS sampler heartbeat dedupe (1e09f87), G12 oracle v3 and WriterClone lifecycle
  (d54264e, 945b99f, 968d5f2, 965f080, 3bb6bee), core-group harness (simultaneous replace), DR08 isolation campaign and
  deterministic starvation cell (2169c26), DR05 harness (f7d6afa, d03cd67, 8ae4266), G13 runner fixes, the isolated
  DR05 quiet-session producer for DR05.QUIET_TYPED (6506d01, c5d3610), host-measurement lock wrapper, release
  image recipe with section-level binary verdicts (46f4628, 696fa3d).

## Defects fixed in this release (register: Trading System `INTEGRATION_DEFECT_REGISTER.md`)

| ID | Defect | Fix | Where it ships |
| --- | --- | --- | --- |
| IDR-090 | Binance current OPEN_INTEREST missing from the alpha entitlement | demand rev 4, reference-l2 rev 7, alpha-binance rev 16, routing rev 28 (2524732) | config baked into the python image; consumer-side JWT rev 16 / realm r16 is a deployment item (not verified live here) |
| IDR-091 | Binance sampled-statistics history failed coverage on rolling windows not aligned to UTC midnight | cadence and edges on the UTC epoch grid (2524732) | python image |
| IDR-092 | Binance funding-rate history returned PARTIAL_RESULT for a rolling 3-day window | settlement-grid coverage (2524732) | python image |
| IDR-094 | rust_core held each batch until its Kafka transaction committed (stop-and-wait) | pipelined commit (a962a5c), K1/K3 stop intent and epoch abort (bcb0283) | rust_core (kept on rc22, binary equal) |
| IDR-095 | possible gateway-side delay after a re-read | mitigation only: TCP_NODELAY (98b421f). The delay itself stays unmeasured | rust image (gateway) |
| IDR-096 | replay work-budget status changed to RESOURCE_EXHAUSTED for existing clients | opt-in `SubscribeRequest.replay_budget_status_v2` (f5cd787); SDK sends it, progress-aware reconnect budget (bb75d06) | rust image + SDK 2.4.1 |
| IDR-099 | untyped canonical-lineage refusal on the Query warmup path | typed `CANONICAL_LINEAGE_MISMATCH` (00056e5) | python image |
| IDR-107 | a transient consumer poll error killed the projector (`exit 2`) | bounded retry with an allowlist, commit-only budget reset (58fb7cd, ff5683c) | rust image (projector, 60f83a51) |
| IDR-112 | one OKX public WS session kept delivering while every frame arrived late; nothing detected it | venue-timestamp staleness detector on QUOTE + LATEST_STATE lanes with make-before-break recycle (6d61710); Codex review F1-F3 fixed in 83bef59 (absolute 15 s establish budget, ACK-cutover wording, recycle slot taken after the control permit) | rust image `2.2.4-83bef59` (ingestors x2) |
| IDR-114 | each Query replica stalled 0.2-0.5 s about every 76-104 s (gen0 GC of per-request cycles: in-process httpx MARK/INDEX reads, refusal tracebacks) | `InProcessAsgiClient`, `release_exception_frames`, `qdl.stall` telemetry (31a5155) | python image `2.2.4-31a5155` (query x2) |

Already in v2.2.3 and unchanged here: IDR-086, IDR-087, IDR-088. IDR-089 (reader tolerance for additive fields): see
Public Interface for the minimum reader version.

Trading System items measured with this release, owned by the TS release (not shipped here): IDR-100/101/102/105/106,
IDR-110 and IDR-110b (native writer probe vs the D27 view boundary), IDR-113 (MARK woken-read re-read; writer
2deb90b0), IDR-109 and IDR-111 (apparatus).

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

The deployed release set is not one image revision. Each role runs the newest image whose source covers it, and every
other binary or file in that image is proven equal to the measured one (equivalence receipts below). Runtime source
after the newest image revision: `git diff 83bef59 HEAD -- rust Cargo.toml Cargo.lock 'Dockerfile*'` is empty;
`git diff 31a5155 HEAD -- qdl app generated config consumers qdl_sdk Dockerfile pyproject.toml poetry.lock` changes only
apparatus (`qdl/certification/reference_l2_acceptance.py`, imported only by scripts/ and tests/; and the four frozen
amendment files `config/v2/p184-stage100-{g4-amendment-1,g4-amendment-2,a3-external-attribution,a4-ts-eligibility}.json`,
read only by the gate tooling; `consumers/` is unchanged, see "Consumer manifest packaging"). `git diff c7ce7d3 HEAD -- qdl_sdk generated contracts scripts/build_qdl_sdk_release.py` is empty, so the
SDK wheel built at c7ce7d3 is the wheel `release.yml` would rebuild at the release head.

| Role | Image (tag, digest) | Revision | Build receipt (sha256) | Equivalence |
| --- | --- | --- | --- | --- |
| ingestor_okx_swap, ingestor_binance_usdm | `qdl-v2-rust:2.2.4-83bef59` `sha256:a9679defdd970aaa961736d414cf8c443c2ead2f26ebf2e68eb481dda6725784` | `83bef59` | `~/.local/state/qdl-v2/build-v2.2.4-83bef59/receipt.json` `7e630925462e5cac3de7423c36b964834eeacba9f8c3e94cb9bfbd49d40e5671` PASS | `qdl-native-raw-ingestor` DIFFERENT (expected, IDR-112 + F1-F3); the other 11 binaries whole-file EQUAL to `60f83a51` and to `5e261809`; `qdl-realtime-core` EQUAL rc22 |
| stream_kn_1/2, market_projector_1/2 | `qdl-v2-rust:2.2.4-c7ce7d3` `sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e` | `c7ce7d3` | `~/.local/state/qdl-v2/build-v2.2.4-c7ce7d3/receipt.json` `40d727b28a3a581eb9c5911e150a39ae0dac68d8d0a9afbb5cceee47b4cb4f7f` **FAIL, kept**, resolved by addendum `dad31fe8dc7f768de72e7577951af61d726689d8b32fcf5153046fd9bb190260` | `qdl-stream-gateway` / `qdl-projector` byte-identical to the 83bef59 image (so not recreated, owner decision in the roll packet); gateway DIFFERENT_BUILD_PATH vs `baa72c96` + DR08 A/B; projector EQUAL_LOADED_SECTIONS vs rc22 |
| query_kn_1, query_kn_2 | `qdl-v2-python:2.2.4-31a5155` `sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff` | `31a5155` | `~/.local/state/qdl-v2/build-v2.2.4-31a5155/receipt.json` `e1a9bcc57847a83ea1425dfc66ed3598322a46e46948375bb47f97bbe1c93e38` PASS | vs `c21e164a`: exactly the 6 IDR-114 files differ (`equivalence-vs-c21e164a.json` `2ec87f343e30152d117b341abecd68ef27382a5e2a947f37fdf7252230e20424`), pip freeze equal |
| rust_core x3 | `qdl-v2-rust:2.2.3-rc22-a4e7b3b` `sha256:7c848aadcb0dd28178bc6dc2b58c42df6c2c235de6d4bfd6602ef7c19fa64bfb` | `a4e7b3b` | (rc22) | **kept**: release `qdl-realtime-core` `2d82167336a4268a9407d565325f656f2315632b26bdd18bd4527e73dd15e475` EQUAL rc22 |
| binance_bar_edge | `qdl-v2-python:2.2.3-rc14-a388537` `sha256:08f356a84151c6e865fbdc4a2efe6924df9336d15353254c2b2da684261794f5` | `a388537` | - | **held, scoped out** (not on a v2.2.4 revision) |

Why the tags differ from the first release build `c7ce7d3` (2026-10-16):
- **IDR-112** (final stage-100 135719Z: G2 QUOTE OKX 1350 ms from one late OKX session) needed an ingestor change:
  rebuilt rust at `6d61710` (`5e261809`, rolled to both ingestors 2026-10-17 16:46-17:06Z), then at `83bef59` for the
  Codex review F1-F3 (`a9679def`, rolled 2026-10-18 03:35-03:46Z). Only `qdl-native-raw-ingestor` changed, so stream
  and projectors stay on `60f83a51`.
- **IDR-114** (Query GC stalls, same run) changed six Python files: python rebuilt at `31a5155` (`3e9e4d3a`, rolled
  query_kn_1 03:47Z, query_kn_2 03:58Z on 2026-10-18). The c7ce7d3 python image `c21e164a` is the Query rollback.
- The first roll of the c7ce7d3 images (2026-10-16 17:14-17:53Z, summary
  `eefb041c5ece53c17d28148eb3fa1c2e6be64f84b79ab20e6442f1bd44c0b271`) stays the provenance of stream/projector.

Final roll (packet `drafts/FINAL_ROLL_PACKET_IDR112b_IDR114.md`, lock `claude-dl-final-roll`): driver
`~/.local/state/qdl-v2/releases/v2.2.4/rollout-idr112b-idr114/driver.log` ALL_DONE 04:09:25Z (sha256
`6b6173e563812703b2a54ad3bdb76ea69bedb3538790f1baf7789696381d22f2`; directory `SHA256SUMS`
`8b034e7cee2e4879bd01069f302955e6d3aa08d847da85d8dc182867866327a4`); each role diff `['image']`, HOLD per role, writer
READY 64 64.

TS writer measured with this release: `market_data_service_p184w`
`sha256:2deb90b0cc52eae7610fa44692f90df077e9db100ca259e38fd652a2dc1c5dc5` (TS `30f50a91`, IDR-113), build receipt
`~/.local/state/trading-system/p184-n1-opt-20261013/release/30f50a91/receipt.json`
`7040ba9fc68f17641ca69ffb9e52fa4240c9e9e4c956f06c2a3568ea97f64a1c` PASS; rolled 2026-10-17 16:59Z
(`rc15/native-trace-roll/receipt-20261017T165933Z.json` `135f26f74361f49d4b16ef42593d488028810488da469ad1c89264fda34b5a62`).
It vendors SDK 2.4.0; re-vendoring 2.4.1 is a separate TS delta.

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

## Findings During Preparation (2026-10-16/17, unchanged)

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

Full table, RCA and evidence: [SCORECARD.md](SCORECARD.md). Frozen gate table sha256
`c67bf69bb921c992964eb8ff8b2e1bb17085a943fdf756d55526b6269641d5c4`.

Amendments:

| Id | File | sha256 | When |
| --- | --- | --- | --- |
| G4-A1 | `config/v2/p184-stage100-g4-amendment-1.json` | `1c8a276f6cc6c03cdf768cdc9d31b65902bd849a1f3b7df56cda07ea47e6efa6` | frozen before the counted run |
| G4-A2 | `config/v2/p184-stage100-g4-amendment-2.json` | `99961c41e08993944d158ae812ca61514271862a2d03e011f5d01ce71a0f9997` | frozen before the counted run |
| A3 | `config/v2/p184-stage100-a3-external-attribution.json` | `cc5fc7dcf07e30b1b52cbc51b78c4a5485a63be84d5b814dcf89452007e57326` | frozen before the counted run |
| **A4** | `config/v2/p184-stage100-a4-ts-eligibility.json` | `14958a681848192c8b9e93feaf0ee3137a1851c38fe6d6763f3bcf6ce0c81312` | **POST_HOC_OWNER_DECISION**: adopted after the counted run was scored; applied to 123222Z only ([a4-post-hoc-ts-eligibility.md](a4-post-hoc-ts-eligibility.md)) |

A4 widens one number after measuring: G2 MARK BINANCE max goes from < 1000 to <= 1200 ms. It also replaces one
statistic after measuring: the G4 OKX_INVERSE max becomes the pooled p95. Both are recorded owner decisions. No other
limit changed.

- Final stage-100 `p184-final-campaign-20261018T123222Z` (load 12:34:45-13:04:45Z, protocol_met true, guard CLEAN).
  - **Without A4** (scorecard `b9e76a83795f1af8f64b802b22ff1f410087a8f87ee4ef5d23b26a1d39cb4964`, untouched): PASS G1 G3
    G5 G6 G7 G8 G9 G10 G11 G14.
    - **FAIL G2**: MARK_INDEX_PRICE BINANCE max 1082.9 ms > 1000; p95 418.6.
    - **FAIL G4**: OKX_INVERSE book_delta, book_snapshot and quote maxima over the addendum limits. The excess is in
      internal segments; A3 rescued none.
  - **With A4** (post-hoc rescore `scorecard.a4-post-hoc.json`
    `7a821d2d60c45ae290023e9434af7b320c06b906a4185dc299eee467a71fab7e`):
    - **PASS G2**: max 1082.9 <= 1200, p95 418.6 <= 500.
    - **PASS G4**: OKX_INVERSE p95 per window at most 198 / 193 / 231 ms against 289.1 / 270.1 / 283.3. The maxima are
      report-only, and all 8 outliers are retained.
    - Every other gate is byte-identical to the scorecard without A4.
- G12: **PASS, credited** on the final image set (`p184-g12-v224-final-2deb90b0-20261018T042415Z`, verdict
  `170af83797df3309d4c9778042ee5b4e8f4c7ec5fca89fede22d9d1d0eeadc27`).
- G13: **76/76** on writer 2deb90b0 (runtime v5.5 `p184-g13-20261018T051237Z` + dr08 `045558Z` + dr05-quiet `071750Z` +
  dr05-upstream `085316Z`; W05 `5490c21e66bf33bae326f2ea2051677f70cf95372b13b823d4c2912835da9895`).
- Summary without A4: PASS 12; FAIL 2 (G2, G4). Summary with A4: PASS 14; FAIL 0; NOT_MEASURED 0; DEFERRED 0.

## Known Residuals And Deferred Items

Register rows are in the Trading System `INTEGRATION_DEFECT_REGISTER.md` (the DL plan uses that register).

| Item | State | Next step |
| --- | --- | --- |
| G2 MARK BINANCE tail (PASS only under post-hoc A4) | OPEN: IDR-126 (TS trace wake/poll markers to split DL vs TS read scheduling) first, then IDR-119 (Query MARK read path) | attribution, then one variable at a time, A/B on the stage-100 harness |
| G4 OKX_INVERSE internal tails (PASS only under post-hoc A4; maxima report-only) | OPEN, IDR-116 (Kafka cleaner I/O, C1), IDR-117 (broker CPU quota), IDR-118 (core commit batching) | one variable at a time with A/B; never two at once |
| Repository consumer manifests / catalog vs deployed (Paper r11, alpha-okx r15, catalog r12) | OPEN, IDR-127 | one slice: catalog r12 + routing + manifests + tests together; no runtime change |
| OKX BOOK lane session degradation | OPEN, IDR-115 (BOOK recycle with re-snapshot semantics; not an IDR-112 scope extension) | design + fake-venue BOOK handover + core fence tests |
| IDR-108 | OPEN: Stream accepts on 8210 before its hot view serves | readiness signal (next DL release) |
| OKX FUNDING_RATE bound = 8 h cadence | OPEN, IDR-125 (contract option of the A1 RCA: max_freshness 28,860,000 or 32,400,000 ms, manifest + JWT revision batch) | release-bundle work |
| Stage-A key coalescing | OPEN, IDR-124 (measured, report-only, not designed) | DL backlog |
| H1 WriterClone lifecycle owner | OPEN, IDR-120 (G12 used the `77a6d671` apparatus pin) | TS follow-up |
| Rust Shadow Core CI (TS) | OPEN, IDR-121 (red before this work, not a required check) | TS CI slice |
| TS ruff debt | OPEN, IDR-122: `ruff 0.15.13 check .` on TS `e4992fe5` = **1286** findings (402 auto-fixable); the earlier "~276" figure was not reproduced (it matches the I001 count, 275). CI checks changed files only | TS cleanup ratchet |
| F821 in `scripts/p184_n1_isolated.py:605` | OPEN, IDR-123: verified with ruff, the undefined name is `asyncio` (line 605 `await asyncio.sleep(.1)`), not `read` | TS apparatus fix |
| Build receipt FAIL (c7ce7d3) | kept; resolved by the postcheck addendum | none |
| TS writer vendors SDK 2.4.0 | release this writer on 2.4.0; re-vendor 2.4.1 later | TS delta |
| Kafka C | converging (estimate ~49 h from 2026-10-16) | watch coordinator load |

## Limits

- **Post-hoc PASS.** G2 and G4 PASS only under A4, adopted after the counted run was scored. Without A4 they FAIL.
- **G2**: MARK_INDEX_PRICE BINANCE max 1082.9 ms. That is above the frozen max < 1000 ms and inside A4's <= 1200 ms;
  p95 418.6 ms is inside both. Headroom under the TS 2000 ms budget is 1.67x instead of the frozen 2x.
  - The cause of the sample is not determined: there is no per-sample DL receipt clock on the Query-read MARK path.
  - It is unknown how much of the tail belongs to DL Query and how much to the TS writer's wake / min-interval / 1 s
    refresh read scheduling. IDR-126 adds wake/poll markers to the TS trace.
- **G4**: OKX_INVERSE BTC-USD-SWAP maxima in some windows: book_delta 387.1 (vs the replaced 282.5), book_snapshot 406.7
  (vs 300.7), quote 531.9 (vs 322.6).
  - Venue -> DL is about 30 ms; the excess is internal, and the internal stage is not separated.
  - Under A4 these maxima are report-only, and no max limit applies to these three rows.
- **TS acceptance evidence.** G6 D3 has 0 refusals and 0 unsafe; G9 has 0 data-path losses; the TS Python worker logs
  are clean. No order admission was observed, and the native Rust writer and Risk logs were not read.
- Single host for every run: no HA, no independent failure domains, no mainnet or funded claim.
- `binance_bar_edge` is not on a v2.2.4 revision. rust_core x3 stay on rc22 and are certified by binary equality.
- Stream gateway and projector are on the c7ce7d3 image; their binaries equal the 83bef59 build; the gateway is
  classified DIFFERENT_BUILD_PATH vs the measured `baa72c96` overlay (behavioural DR08 A/B), projector equal in loaded
  sections to rc22.
- G14 DL mean CPU sum 5.63 vCPU at stage 100, above the owner's 5 vCPU DL budget (G14 records resources only).
- Kafka C converging at measurement time; B applied 2026-10-16 14:25Z.
- IDR-108: after a restart, Stream readiness (port accept) comes before hot-view serving.
- G12 harness shape is not production shape (core members, Kafka CPU, topic segment settings, synthetic publisher);
  its integrity layer is report-only.
- G13 cells are shadow-certified (TEST_ONLY isolated harness or TS stack on one host); the W05 output file carries no
  run identity (see SCORECARD.md).
- TEST_ONLY burst results above are not production capacity claims.

## Consumer manifest packaging (finding 2026-10-18)

- The deployed Paper manifest is `trading-system.paper.stable` **revision 11** (`efd482fa...`, query_kn_1 and
  query_kn_2, `/consumer-realms/`).
- The repository file `consumers/stable/trading-system-paper.yaml` is still revision 10 (`1e489237...`, no
  BTC-USD-SWAP).
- Source of r11, established and reproduced byte for byte: `scripts/prepare_paper_read_extension.py` (76db472) with
  quotas 77/3000, from the deployed catalog revision 12 (`fc3a1e1e...`), the r10 manifest and the r22 binding (2026-10-09
  D1 rev11).
- The exact deployed bytes are packaged as release evidence:
  - [trading-system.paper.stable-r11.yaml](trading-system.paper.stable-r11.yaml), sha256 = deployed;
  - [consumer-manifest-r11-provenance.json](consumer-manifest-r11-provenance.json).
- The loadable repository path stays r10. With r11 there, 13 manifest/catalog test modules fail (1 failure, 38
  errors), because the repository catalog revision 11 does not declare BTC-USD-SWAP. Changing that path also changes
  what a future python image bakes. Alignment is IDR-127.
- Runtime unchanged. Also seen, not investigated: the repository `alpha-okx-paper.yaml` is r14 and the deployed realm is
  r15.

## Rollback

Failure only, one container at a time, reverse role order, with the saved composes (sha256 in
[equivalence.json](equivalence.json)):

| Role | Roll back to | Saved compose |
| --- | --- | --- |
| query_kn_1, query_kn_2 | `c21e164a` (`qdl-v2-python:2.2.4-c7ce7d3`) | `~/.local/state/qdl-v2/p184-d3-capacity-20261009/rollout-rc15/query_kn_{1,2}-rollback-20261018T03*.compose.json` |
| ingestor_okx_swap, ingestor_binance_usdm | `5e261809` (`qdl-v2-rust:2.2.4-6d61710`), then `60f83a51` | `~/.local/state/qdl-v2/releases/v2.2.4/rollout-idr112b-idr114/<role>-rollback-20261018T03*.compose.json` |
| stream_kn_1/2, market_projector_1/2 | not changed by the final roll; v2.2.3-era targets `baa72c96` (stream, TEST overlay) / `ce629061` (projector rc14) | `~/.local/state/qdl-v2/releases/v2.2.4/rollout/<role>-rollback-20261016T17*.compose.json` |
| market_data_service_p184w (TS) | `a44349a1` (then `734b08f1`, `7c71e3af`, B17 `cb5216b8`) | `rc15/native-trace-roll/rollback-20261017T165933Z.compose.json` |

Kafka config: cleaner `--add-config log.cleaner.io.max.bytes.per.second=8388608`; marker retention
`--rollback-to 604800000 --apply`; internal topics `scripts/kafka_internal_topic_compaction.py --rollback --apply
--cert-dir <stable cert dir>`; broker rebalance `scripts/kafka_auto_leader_rebalance_roll.py --rollback <packet> --broker
kafkaN --apply`. Not touched: kafka1-3 images, rust_core x3, binance_bar_edge, market_cache, stable_redis, catalog,
consumer manifests, JWT revisions, realm dirs, offsets, Redis data.
