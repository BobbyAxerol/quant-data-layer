# Quant Data Layer v2.2.4

Status: **CERTIFICATE PASS ON THE FINAL COUNTED RUN 040311Z, UNDER A POST-HOC OWNER AMENDMENT FOR G11 (A5); NOT
TAGGED, NOT PUBLISHED.** Re-issued 2026-10-19. This supersedes the 2026-10-18 PASS under the post-hoc A4 on 123222Z,
which measured the pre-rustls image set.

- Final counted run `p184-final-campaign-20261019T040311Z`, on the deployed set: rustls 0.23.46 images on the ingestors
  and Stream, plus the OPT resource caps. Protocol met.
  - G1-G10 and G14 PASS under the pre-registered rules (G4-A1, G4-A2, A3 refreshed `384e8512`).
  - A4 v2 was bound before load and changes no verdict. G2 and G4 PASS without it.
  - G12 (credited) and G13 (76/76) passed on the same set.
- **G11 FAILs under the frozen rule on one slice.** OKX BNB-USDT-SWAP TRADE took 36.415 s against the 30 s limit,
  because the venue printed no trade on that instrument for 41.9 s. The canonical topic and the OKX public history
  both show adjacent trade ids. The writer applied the next trade 0.413 s after DL published it.
- Under the owner's external rule this was adopted as **A5** (`POST_HOC_OWNER_DECISION` for this run, pre-registered
  for later runs). A G11 TRADE slice is timed from its first post-fault canonical event. With A5 every gate PASSES, so
  [certificate.json](certificate.json) has status **PASS** with a required `amendments` list in which A5 is
  `post_hoc: true`.
- The G11 verdict without A5 is kept next to the A5 verdict everywhere.
- **Operational note: measured with the portal stack paused.** 15 portal containers were paused and the Codex tree was
  stopped for the counted run. The same set with the portal stack running (021044Z) failed G4 and G8. **The production
  host must not run unrelated heavy workloads beside DL.** This is a capacity limit; IDR-128 is open.

`.github/workflows/release.yml` accepts status PASS. It was not changed. No tag was created and nothing was merged; a
tag needs a merge into `main`, green required CI and an explicit owner instruction.

Machine files: [certificate.json](certificate.json), [scorecard.json](scorecard.json),
[endpoint-report.json](endpoint-report.json), [equivalence.json](equivalence.json),
[release-preparation.json](release-preparation.json) (preparation packet of 2026-10-16, kept as written).

- Previous stable release: v2.2.3 (source `481fd67`, main merge `28e605e`, SDK 2.3.0).
- Scope: the Data Layer release only. Not a Trading System release. Not HA (single host).

## Scope

Commits since `481fd67`: 196 at `288a580` plus the packaging commit (`git rev-list --count 481fd67..HEAD`). Paths
changed: 70 files, +16615/-1018 at `288a580` (`git diff --shortstat 481fd67 288a580 -- rust qdl qdl_sdk contracts
generated config consumers Cargo.toml Cargo.lock 'Dockerfile*' app`; this includes the frozen amendment files under
`config/v2/`). Grouped by runtime area:

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
- **TLS dependency**: rustls 0.23.45 -> 0.23.46 for RUSTSEC-2026-0346..0349 (9dab224; `Cargo.toml` pin `=0.23.46`,
  `Cargo.lock` version + checksum only, features and the ring provider unchanged).
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
  image recipe with section-level binary verdicts (46f4628, 696fa3d), the per-function Rust symbol diff (97c9ecb),
  the pre-registered A4 v2 binding (f2094a2), and the A5 G11 TRADE-clock rescore (this packaging commit).

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
| IDR-112 | one OKX public WS session kept delivering while every frame arrived late; nothing detected it | venue-timestamp staleness detector on QUOTE + LATEST_STATE lanes with make-before-break recycle (6d61710); Codex review F1-F3 fixed in 83bef59 (absolute 15 s establish budget, ACK-cutover wording, recycle slot taken after the control permit) | rust image `2.2.4-9dab224` (ingestors x2; first shipped in `2.2.4-83bef59`) |
| IDR-114 | each Query replica stalled 0.2-0.5 s about every 76-104 s (gen0 GC of per-request cycles: in-process httpx MARK/INDEX reads, refusal tracebacks) | `InProcessAsgiClient`, `release_exception_frames`, `qdl.stall` telemetry (31a5155) | python image `2.2.4-31a5155` (query x2) |
| RUSTSEC-2026-0346..0349 | rustls 0.23.45 advisories (zero-exception policy) | rustls 0.23.46 (9dab224; `Cargo.lock` version + checksum only) | rust image `2.2.4-9dab224` (ingestors x2, stream x2); `qdl-realtime-core` (rust_core x3) contains no rustls code |

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
after the newest image revisions:
- `git diff 9dab224 HEAD -- rust Cargo.toml Cargo.lock 'Dockerfile*'` changes only `rust/qdl-kafka/src/bin/
  qdl-native-raw-ingestor.rs` (539d28c). That change is a `#[cfg(test)]` slot-observation hook and its test assertion;
  no product code changed.
- `git diff 31a5155 HEAD -- qdl app generated config consumers qdl_sdk Dockerfile pyproject.toml poetry.lock` changes
  only apparatus:
  - `qdl/certification/reference_l2_acceptance.py`, imported only by scripts/ and tests/;
  - the frozen amendment files `config/v2/p184-stage100-{g4-amendment-1,g4-amendment-2,a3-external-attribution,
    a4-ts-eligibility,a4-v2-preregistered,a5-g11-trade-clock}.json`, read only by the gate tooling.
  - `consumers/` is unchanged (see "Consumer manifest packaging").
- `git diff c7ce7d3 HEAD -- qdl_sdk generated contracts scripts/build_qdl_sdk_release.py` is empty, so the SDK wheel
  built at c7ce7d3 is the wheel `release.yml` would rebuild at the release head.
- `docker-compose.v2-stable.yml` records the live OPT caps (b58e40d).

| Role | Image (tag, digest) | Revision | Build receipt (sha256) | Equivalence |
| --- | --- | --- | --- | --- |
| ingestor_okx_swap, ingestor_binance_usdm, stream_kn_1, stream_kn_2 | `qdl-v2-rust:2.2.4-9dab224` `sha256:27c79b53400eea5582713a3477034c8561f533755e09a94223e254bffadd5c92` | `9dab224` (rustls 0.23.46) | `~/.local/state/qdl-v2/build-v2.2.4-9dab224/receipt.json` `e38fd27548f7482621d5ed1e7d2e6bc2225fd14aa39d365696d2eef0ab86674c` PASS | vs `a9679def` (`scripts/p184_rust_symbol_diff.py`, summary `49f2164e...`): projector and venue-core-certify whole-file EQUAL; ingestor and gateway change rustls functions plus same-source codegen (NOT_CONFINED); rolled M1-M4 2026-10-18 18:46-19:06Z, diff `['image']`, HOLD PASS per role (`SHA256SUMS.roll-20261018T1846Z` `c518f9b3...`) |
| market_projector_1/2 | `qdl-v2-rust:2.2.4-c7ce7d3` `sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e` | `c7ce7d3` | `~/.local/state/qdl-v2/build-v2.2.4-c7ce7d3/receipt.json` `40d727b28a3a581eb9c5911e150a39ae0dac68d8d0a9afbb5cceee47b4cb4f7f` **FAIL, kept**, resolved by addendum `dad31fe8dc7f768de72e7577951af61d726689d8b32fcf5153046fd9bb190260` | `qdl-projector` whole-file equal to the 83bef59 and 9dab224 builds; EQUAL_LOADED_SECTIONS vs rc22 |
| query_kn_1, query_kn_2 | `qdl-v2-python:2.2.4-31a5155` `sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff` | `31a5155` | `~/.local/state/qdl-v2/build-v2.2.4-31a5155/receipt.json` `e1a9bcc57847a83ea1425dfc66ed3598322a46e46948375bb47f97bbe1c93e38` PASS | vs `c21e164a`: exactly the 6 IDR-114 files differ (`equivalence-vs-c21e164a.json` `2ec87f343e30152d117b341abecd68ef27382a5e2a947f37fdf7252230e20424`), pip freeze equal |
| rust_core x3 | `qdl-v2-rust:2.2.3-rc22-a4e7b3b` `sha256:7c848aadcb0dd28178bc6dc2b58c42df6c2c235de6d4bfd6602ef7c19fa64bfb` | `a4e7b3b` | (rc22) | **kept**: `qdl-realtime-core` of the 83bef59 build `2d82167336a4268a9407d565325f656f2315632b26bdd18bd4527e73dd15e475` EQUAL rc22; it contains no rustls code in either build (symbol diff), so the rustls roll does not apply (M5 not rolled) |
| binance_bar_edge | `qdl-v2-python:2.2.3-rc14-a388537` `sha256:08f356a84151c6e865fbdc4a2efe6924df9336d15353254c2b2da684261794f5` | `a388537` | - | **held, scoped out** (not on a v2.2.4 revision) |

Image history (each step kept as provenance in [equivalence.json](equivalence.json)):
- First release build `c7ce7d3`: rolled to 8 roles on 2026-10-16 17:14-17:53Z (summary
  `eefb041c5ece53c17d28148eb3fa1c2e6be64f84b79ab20e6442f1bd44c0b271`).
- **IDR-112** (late OKX session): the ingestor was rebuilt at `6d61710` (`5e261809`, 2026-10-17), then at `83bef59`
  for the Codex review F1-F3 (`a9679def`, rolled 2026-10-18 03:35-03:46Z).
- **IDR-114** (Query GC stalls): python rebuilt at `31a5155` (`3e9e4d3a`, rolled 2026-10-18 03:47Z / 03:58Z). Final
  roll driver `6b6173e563812703b2a54ad3bdb76ea69bedb3538790f1baf7789696381d22f2`.
- **rustls 0.23.46** (owner ~15:50Z "Nang rustls ngay"): rust rebuilt at `9dab224` (`27c79b53`) and rolled to the
  ingestors x2 and Stream x2 on 2026-10-18 18:46-19:06Z (lock `claude-dl-rustls-roll`). The cores were not rolled:
  their binary has no rustls code. The projectors were not rolled: their binary is whole-file equal.
- **OPT resource caps K/Q/I** (2026-10-19 00:20-00:44Z), each with its own HOLD. See "Runtime Config".

TS writer measured with this release: `market_data_service_p184w`
`sha256:2deb90b0cc52eae7610fa44692f90df077e9db100ca259e38fd652a2dc1c5dc5` (TS `30f50a91`, IDR-113), build receipt
`~/.local/state/trading-system/p184-n1-opt-20261013/release/30f50a91/receipt.json`
`7040ba9fc68f17641ca69ffb9e52fa4240c9e9e4c956f06c2a3568ea97f64a1c` PASS; rolled 2026-10-17 16:59Z
(`rc15/native-trace-roll/receipt-20261017T165933Z.json` `135f26f74361f49d4b16ef42593d488028810488da469ad1c89264fda34b5a62`).
It vendors SDK 2.4.0; re-vendoring 2.4.1 is a separate TS delta.

## Runtime Config

- **Resource caps** (OPT package, owner ~16:00Z 10-18; adjacent-baseline HOLD method approved ~20:05Z; evidence
  `~/.local/state/qdl-v2/releases/v2.2.4/opt-resource-20261018/SHA256SUMS` `ba5e465e...`; recorded in
  `docker-compose.v2-stable.yml`, b58e40d):

  | Step | Role | Change | Applied | HOLD |
  | --- | --- | --- | --- | --- |
  | K | kafka1-3 | 1.75 -> **2.25** vCPU | 2026-10-19 00:20:57Z | PASS (`eval2-K2.json`) |
  | Q | query_kn_1/2 | -> **2.0** vCPU, **2 GiB**, swap 4 GiB | 00:32:44Z | PASS (`eval2-Q.json`) |
  | I | ingestor_okx_swap | 0.5 -> **1.0** vCPU | 00:44:10Z | PASS (`eval2-I.json`) |

  ingestor_binance_usdm stays at 0.75 vCPU. A first K attempt on 2026-10-18 19:24Z failed its single-baseline HOLD;
  it was rolled back at 19:37:12Z and is kept (`eval-K.json`). With the caps, DL throttling is ~0 % of wall (G14).
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
- Kafka CPU history: kafka1 1.25 -> 1.75 vCPU (2026-10-14 13:47:40Z); all three -> 2.25 (K above).
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

Amendments (all required to read the certificate):

| Id | File | sha256 | When |
| --- | --- | --- | --- |
| G4-A1 (A1) | `config/v2/p184-stage100-g4-amendment-1.json` | `1c8a276f6cc6c03cdf768cdc9d31b65902bd849a1f3b7df56cda07ea47e6efa6` | frozen before the counted run |
| G4-A2 (A2) | `config/v2/p184-stage100-g4-amendment-2.json` | `99961c41e08993944d158ae812ca61514271862a2d03e011f5d01ce71a0f9997` | frozen before the counted run |
| A3 | `config/v2/p184-stage100-a3-external-attribution.json` | `384e85121c7c4c2fec06de12dd031a78550ac199fe9b6da9771dec118df5cec9` | frozen before the counted run (runtime-image pin refresh of `cc5fc7dc...` for the rustls images; rules unchanged) |
| A4 v2 | `config/v2/p184-stage100-a4-v2-preregistered.json` | `63f98668150bfda776317f97114b73855800515d0cfc9b7aa90ccb050f500324` | **PRE_REGISTERED**, bound before load (binding `f08b594a...`); applied, not needed |
| **A5** | `config/v2/p184-stage100-a5-g11-trade-clock.json` | `c5b46c5faefe20e31061a3bd647b9e60537b982eee43b484715ec9cdcc5aae78` | **POST_HOC_OWNER_DECISION** for 040311Z (EXTERNAL_QUIET); pre-registered for later runs ([a5-g11-trade-clock.md](a5-g11-trade-clock.md)) |

A5 does not widen a number: the G11 limits stay 30 s and 60 s. It moves the start of the G11 first-valid-apply clock for
TRADE slices to the slice's first post-fault canonical event, after measuring, by a recorded owner rule.
A4 v1 (`14958a68...`) stays the post-hoc decision for 123222Z only and is not an input of this certificate.

- Final stage-100 `p184-final-campaign-20261019T040311Z` (load 04:05:17-04:35:17Z, protocol_met true, guard CLEAN).
  - **Without A5** (scorecard `ebe11b8c10145bb92b7a850c4134d81101aaf7000a210f198b175316b20acbbf`, untouched): PASS
    G1-G10 and G14 (the same with A4 v2, receipt `7fffb04e...`).
    - **FAIL G11**: first valid apply max 36.415 s > 30 s, on OKX BNB-USDT-SWAP TRADE only. Every other slice is
      <= 4.928 s. Caught up <= 60 s, 0 rewinds, cursor never ahead.
  - **With A5** (rescore `scorecard.a5.json` `b1c2e7b450363f094c25adb34988f9356da327ef3d0b6a863a001dab2692d08d`):
    - **PASS G11**: that slice is 0.413 s from its first post-fault event; the max over 64 slices is 4.928 s.
    - Every other gate is byte-identical to the scorecard without A5.
- G12: **PASS, credited** on the rolled set (`p184-g12-v224-final-27c79b53-20261019T005952Z`, verdict
  `87386ad0f34423136132b4abb83077decd3573a382694b145b515cec3055449e`).
- G13: **76/76** on the rolled set with writer 2deb90b0 (runtime v5.6 `p184-g13-20261019T015047Z` + dr08 `012840Z` +
  dr05-upstream `014124Z` + dr05-quiet `013746Z`; W05 `5490c21e66bf33bae326f2ea2051677f70cf95372b13b823d4c2912835da9895`).
- Summary without A5: PASS 13; FAIL 1 (G11). Summary with A5: PASS 14; FAIL 0; NOT_MEASURED 0; DEFERRED 0.

Earlier attempts and their status: [SCORECARD.md](SCORECARD.md) "Stage-100 attempts". In short:
- `123222Z` (the 2026-10-18 counted run, PASS under post-hoc A4) is SUPERSEDED.
- `021044Z` (FAIL G4 G8 with the portal stack running) is HISTORY.
- `030130Z` is TAINTED (aborted).
- `040311Z` is COUNTED.

## Known Residuals And Deferred Items

Register rows are in the Trading System `INTEGRATION_DEFECT_REGISTER.md` (the DL plan uses that register).

| Item | State | Next step |
| --- | --- | --- |
| Host workload isolation (counted run measured with the portal stack paused) | **OPEN, IDR-128** (021044Z with the portal stack running: FAIL G4 and G8; 040311Z with it paused: PASS) | production runbook: no unrelated heavy workloads on the DL host; options cpuset / cpu.weight / separate host, one variable at a time with a stage-100 A/B; host layout is an owner decision |
| G11 frozen clock times venue silence on quiet TRADE slices | handled by A5 (pre-registered for later runs); evidence directory `<campaign>.g11-canonical/` | produce the canonical read for any TRADE slice over 30 s right after G11 |
| G2 MARK BINANCE tail | not binding on 040311Z (max 515.5 ms); IDR-126 then IDR-119 stay open | attribution, then one variable at a time, A/B on the stage-100 harness |
| G4 internal tails under contention | not binding on 040311Z; IDR-116 (Kafka cleaner I/O, C1), IDR-117 (broker CPU quota, partly addressed by OPT K), IDR-118 (core commit batching) | one variable at a time with A/B; never two at once |
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

- **Post-hoc PASS for G11.** G11 PASSES only under A5, adopted after 040311Z was scored. Without A5 it FAILs on one
  TRADE slice.
  - Venue silence is proven from md.canonical.v2 and the OKX public trade history (adjacent trade ids).
  - The evidence is a canonical read produced after the run; the tool proves its coverage from the records.
- **Measured with the portal stack paused (capacity limit, IDR-128).** Host load1 in the load phase was p50 16.2 /
  p95 20.6 / max 25.1 on 16 vCPU. With the portal stack running (021044Z: 22.0 / 26.6 / 29.4) the same set failed G4
  (bar BINANCE 1m p95 up to 362.9 vs 257.6; book_snapshot BINANCE window 5 p95 292 vs 260) and G8 (one writer EVAL
  12.25 ms). **The production host must not run unrelated heavy workloads beside DL.**
- **rustls 0.23.46 equivalence** is per function, not behavioural. The ingestor and gateway change in rustls
  functions and in same-source codegen elsewhere. Behaviour rests on the HOLDs, G12, G13 and the stage-100.
- **TS acceptance evidence.** G6 D3 has 0 refusals and 0 unsafe; G9 has 0 data-path losses. No order admission was
  observed, and the native Rust writer and Risk logs were not read.
- Single host for every run: no HA, no independent failure domains, no mainnet or funded claim.
- `binance_bar_edge` is not on a v2.2.4 revision. rust_core x3 stay on rc22 and are certified by binary equality; their
  binary contains no rustls code.
- Projectors are on the c7ce7d3 image. Their binary equals the 83bef59 and 9dab224 builds, and its loaded sections
  equal rc22.
- G14: DL mean CPU sum 5.75 vCPU at stage 100, above the owner's 5 vCPU DL budget (G14 records resources only).
  query_kn_1 peaked at 1,822.7 MiB of 2,048 MiB.
- Kafka C was still converging at measurement time; B was applied 2026-10-16 14:25Z.
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
| ingestor_okx_swap, ingestor_binance_usdm | `a9679def` (`qdl-v2-rust:2.2.4-83bef59`), then `5e261809`, `60f83a51` | `~/.local/state/qdl-v2/releases/v2.2.4/rollout-rustls-0.23.46/<role>-rollback-20261018T18*.compose.json` (it carries the pre-OPT cap of ingestor_okx_swap, 0.5 vCPU: re-apply `docker update --cpus 1.0` if only the image is rolled back) |
| stream_kn_1, stream_kn_2 | `60f83a51` (`qdl-v2-rust:2.2.4-c7ce7d3`), then `baa72c96` (TEST overlay) | `rollout-rustls-0.23.46/<role>-rollback-20261018T19*.compose.json` |
| query_kn_1, query_kn_2 | `c21e164a` (`qdl-v2-python:2.2.4-c7ce7d3`) | `~/.local/state/qdl-v2/p184-d3-capacity-20261009/rollout-rc15/query_kn_{1,2}-rollback-20261018T03*.compose.json` (pre-OPT caps: re-apply Q if only the image is rolled back) |
| market_projector_1/2 | not changed since 2026-10-16; v2.2.3-era target `ce629061` (projector rc14) | `~/.local/state/qdl-v2/releases/v2.2.4/rollout/<role>-rollback-20261016T17*.compose.json` |
| market_data_service_p184w (TS) | `a44349a1` (then `734b08f1`, `7c71e3af`, B17 `cb5216b8`) | `rc15/native-trace-roll/rollback-20261017T165933Z.compose.json` |
| Resource caps (K/Q/I) | `docker update`: kafka1-3 `--cpus 1.75`; query_kn_1/2 `--cpus 1.5 --memory 1536m --memory-swap 3g` (memory only while usage <= 1.3 GiB); ingestor_okx_swap `--cpus 0.5`; then revert the compose record (b58e40d) | OPT packet section 12 |

Kafka config: cleaner `--add-config log.cleaner.io.max.bytes.per.second=8388608`; marker retention
`--rollback-to 604800000 --apply`; internal topics `scripts/kafka_internal_topic_compaction.py --rollback --apply
--cert-dir <stable cert dir>`; broker rebalance `scripts/kafka_auto_leader_rebalance_roll.py --rollback <packet> --broker
kafkaN --apply`. Not touched: kafka1-3 images, rust_core x3, binance_bar_edge, market_cache, stable_redis, catalog,
consumer manifests, JWT revisions, realm dirs, offsets, Redis data.
