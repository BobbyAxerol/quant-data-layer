# v2.2.4 - ordered remaining release steps (DL v2.2.4 + TS final writer)

Status: PLAN, nothing in this file has been run. Written 2026-10-16 ~15:40Z while another agent held the host lock
`claude-g12-final` (G12 final, until 16:26:44Z). The only actions were file edits and git reads.

Basis:
- [release-preparation.json](release-preparation.json): the `proof_map` and blockers B1-B20.
- [RELEASE_NOTES.md](RELEASE_NOTES.md).
- [g12-g13-final-rerun.md](g12-g13-final-rerun.md): commands and the 12-point pre-flight.
- [g12-g13-inheritance.md](g12-g13-inheritance.md).
- TS `upgrade/evidence/P184_WRITER_RELEASE_PACKET_20261016.md` section 8 (TS `8b497acf`).
- Codex reviews of 2026-10-16: parallel-recovery F2, Redis-parity R4/E, final-path preflight, writerclone
  follow-up.

Rules for every step:
- Read `~/.local/state/host-measurement-window.lock` before every production action. Never run inside another
  owner's window.
- Every build, measurement or roll takes its own lock with
  `python3 -B scripts/host_measurement_lock.py acquire --owner <o> --purpose <p> --minutes <= 180`, and releases it
  afterwards.
- `df` must show at least 40 GB free before a build. The scratchpad shares the runtime disk.
- A failed step stops the sequence. Never retry under a changed rule.

Paths used below:
```
DL=/home/bobby/.worktrees/data-layer-p184-closure
TS=/home/bobby/.worktrees/trading-system-next-upgrade
```

## Order at a glance

| Step | What | Host time | Lock | Approval needed |
| --- | --- | --- | --- | --- |
| a | G12 final credit (Kafka B/C config) | 31-44 min per run (measured rc12/rc18/rc10) + ~1 min C apply | own, 75 min | inside the approved Kafka/G12 plan |
| b | DL release images + SDK wheel | Rust thin build ~3 min (rc22: 10:09:19-10:12:27Z, warm cache); Python recipe build and the 2 wheel builds not measured here (network) | own, 60-90 min | build only, no runtime change |
| c | TS governance reword + push + CI, then canonical writer build + proofs | CI not measured; build not measured (timeout 90 min); proofs ~25 min (B17: cargo 10 + proofs 12.5 min) | own, <= 170 min | push (owner decision recorded 2026-10-16) |
| d | production roll per role | ~40 min estimate (inferred from earlier rolls, see d) | own | **new runtime packet: AskUserQuestion** (roles, digests, rollback, not touched) |
| e | G13 isolated + runtime | ~10 min isolated (lock 70) + 30-40 min runtime window (lock 150) | own, two windows | packet v5 approval; TS CI run id |
| f | final stage-100 on release digests + G4 observer | ~80 min lock windows (#2 16:23:52Z with lock to 17:42:27Z; #3 02:45:30Z with lock to 04:04:12Z) | own | inside the closeout plan |
| g | certificate, scorecard, endpoint report | host python + `docker inspect`, not measured | none (read-only) | none |
| h | PRs, merges, tag `v2.2.4` | CI time, not measured | none | envelope 4 after f + e PASS; push/merge per owner rule G6 |
| i | cleanup | short; measure `docker system df` before and after | own (image removal) | cleanup packet with inventory |

Open owner questions:
- **Q1**: which G12 run is credited (step a).
- **Q2**: B4/B5 roll scope (step d).
- **Q3**: SDK 2.4.1 re-vendor order (step c).

## a. G12 final credit

1. The current owner is lock `claude-g12-final`. Two attempts have stopped before the faults, with a clean teardown
   (12 removed, 0 left):
   - `p184-g12-v224-final-20261016T143156Z`: "TS clones never applied". WriterClone lifecycle fix `968d5f2`.
   - `...T151149Z`: "bootstrap writer BINANCE left no native checkpoint".

   Wait for that agent's receipt. Do not start a competing run.
2. Before any credit, record a disposition in the DL Unified Plan for the Codex writerclone follow-up review (W1
   strict-resume attribution, W2 erase after confirmed stop, T1/T2, B1 per-launch lock check).
3. Credit command, against a fresh post-B snapshot:
   ```
   python3 -B scripts/p184_g12_isolated_fault.py --parity-snapshot $BASE/credit-snapshot.json
   python3 -B scripts/p184_g12_isolated_fault.py --credit $BASE --production-parity $BASE/credit-snapshot.json
   ```
   It exits 0 only for: v3 PASS, three faults, `production_parity` MATCH, and a run profile equal to production
   (rebalance false). Link the effective describe sha256 (`f4836dfc...` or newer) before and after the run (Codex
   K3).
4. **Q1, owner.** The run in a.1 uses the *pre-roll* image set: projector rc14 `ce629061`, stream `baa72c96`,
   writer B17 `cb5216b8`. The release projector (IDR-107, which sits on the broker-stop path) and the canonical writer
   (1a648ea8, which sits on the Stream-restart replay path) are therefore not in it.
   - **Recommended: option A.** Treat a.1 as the Kafka-config qualification. Run G12 once more as step **a'** after
     d, on the release image set, with the canonical writer as `ts`. `--run` refuses a `ts` that is not the
     deployed writer, so this run must come after the writer roll. Cost: one ~45 min window.
   - Option B: credit a.1 for G12, and cover the projector and writer deltas by post-roll checks and the isolated
     proofs. This needs an explicit owner inheritance record (CLAUDE.md P18.4 rule).

## b. DL release images + SDK wheel

1. Pre-checks:
   - Lock absent.
   - `git -C $DL diff --stat aaa81a8 HEAD -- rust qdl qdl_sdk contracts generated config consumers Cargo.toml Cargo.lock 'Dockerfile*' app`
     is empty (true at `968d5f2`).
   - `git fetch` (B8).
   - `df -h /`.
2. Release source REV = the DL commit that carries this packet. Later evidence-only commits are allowed under
   `release_source_rule`.
3. `python3 -B scripts/build_qdl_release_images.py` (dry run: prints the plan and the expected-different Python set).
4. Take the lock, then run `python3 -B scripts/build_qdl_release_images.py --build --lock-owner claude-dl-release`.
   This builds Rust first, then Python, and writes the receipt `~/.local/state/qdl-v2/build-v2.2.4-<sha7>/receipt.json`.
   - Rust: thin build in builder `551428d9` with `--network none`, on rc16 `f41ecb66`.
   - Python: recipe build. It needs network (B10: `python:3.12-slim@sha256:2c941e86...` is not local).
5. The receipt must show:
   - `qdl-realtime-core` EQUAL rc22 `2d821673...`.
   - `qdl-stream-gateway` EQUAL the `baa72c96` gateway.
   - Ingestor == rc22 `77ceeee1`, projector == rc22 `efeb4728` (both DIFFERENT from production, as expected).
   - Python files equal to rc19 except the git-derived set; pip freeze equal to rc19.
   - ldd 0 not found; uid 10001; no `qdl.test.only` label.
   - Feature-only TEST clock markers absent (B15).

   Any other difference: stop.
6. SDK 2.4.1: two `--network none` builds at REV with `scripts/build_qdl_sdk_release.py`. They must be
   byte-identical. `generated_contract_digest` must be `2ee96b1e...`.
7. Same lock, read-only receipts for B9: `kafka-configs.sh --describe --entity-type brokers --entity-default`
   (cleaner 4 MiB/s), and a fresh `kafka_internal_topic_compaction.py --observe`. Store both in
   `~/.local/state/qdl-v2/releases/v2.2.4/`.
8. Write `equivalence.json` (per role: measured digest, release digest, binary/file sha256 pairs, verdict) into the
   same directory and cite its sha256.

## c. TS: governance reword, push, CI, then the canonical writer build + proofs

The order matters. The owner decision of 2026-10-16 is to reword the 7 unpushed governance-failing subjects
(message only, tree unchanged) immediately before the push, at a time when no agent commits to the worktree. The
set read at TS `8b497acf` is:
- `e9eecd13`, `3a09fe19`, `4598e392`, `68f2572f`, `471ff37c`, `ebc12525`, `f1d526bd`.
- Scope `p18.4` fails `COMMIT_RE`.

Rewording rewrites every descendant SHA. That includes `68f2572f` (the B17 label revision) and `1a648ea8`.
Therefore:

1. **Reword before the build.** The canonical image's revision label, the CI run (`release-image.yml`,
   `quality-contract.yml`) and the G13 `dr01-deployed --ci-confirmed` must all name the *pushed* SHA.
2. In the same transaction as the reword:
   - Prove tree equality for each old -> new pair (`git rev-parse <old>^{tree}` == `<new>^{tree}`), at least for
     68f2572f, 1a648ea8 and HEAD.
   - Update `scripts/p184_writer_release_build.py` `B17_REV`. Keep the B17 image label `68f2572f` as a separate
     constant, checked by tree equality.
   - Update `upgrade/evidence/P184_WRITER_REVIEWED_DELTA_B17_TO_FINAL.json` (new commit SHA; the blobs do not
     change).
   - Run the tests.

   Until this is done, the build guard refuses, because the commit list check fails closed.
3. Push `feat/v2-rust-first-okx-demo` (no force-push of pushed history). Open the grandfather PR into `dev` for the
   10 already-pushed failing subjects: an exemption list by full SHA + reviewed subject, with tests. Wait for green
   CI on the pushed head and record the run id.
4. **Q3, owner.** The SDK 2.4.1 re-vendor (`PIN.json`, `contract_pin`, wheel) is a new closure delta.
   - Recommended: release this writer on 2.4.0.
   - Re-vendor after DL v2.2.4 publishes 2.4.1, as a separate reviewed delta and build.
5. Build the canonical writer and run its proofs:
   ```
   python3 -B $TS/scripts/p184_writer_release_build.py --reviewed-delta $TS/upgrade/evidence/P184_WRITER_REVIEWED_DELTA_B17_TO_FINAL.json
   # take the lock: owner claude-writer-release
   python3 -B $TS/scripts/p184_writer_release_build.py --build --reviewed-delta <same> --lock-owner claude-writer-release
   python3 -B $TS/scripts/p184_writer_release_build.py --prove --lock-owner claude-writer-release
   ```
   - The dry run must exit 0.
   - `--build` needs network (maturin image, crates, uv).
   - `--prove` re-derives the B17 handoff13/expiry3/drill harness for the canonical image.

   Then, in the same lock, the affected proofs of TS packet 8.3:
   - process test and health-chain probe in the canonical image;
   - in-image Python units;
   - `tests/flat_v2.rs` run with the image's Python.

   Copy the 1a648ea8 cargo log from the session scratchpad into the release receipt dir first.
6. Regenerate for the canonical writer digest:
   - `g12-image-set-v224.json` (`ts`);
   - the G13 `--writer-image` / `--ts-source-sha`;
   - packet v5;
   - the stage-100 writer pin.

   The old maps name B17 (B18).

## d. Production roll (new runtime packet: AskUserQuestion before any recreate)

Tools:
- DL roles: `rollout_rc17_v2.py` (`~/.local/state/trading-system/p184-20261005/runtime/p1/rc17/`), one container
  at a time, saved rollback compose, diff must be `['image']`.
- Writer: `roll_writer_campaign_native.py` (`rc15/`). Image + trace window only. Auto-rollback when READY 64/64
  is not reached in 180 s.

Gate after each role:
- writer READY 64/64;
- 0 new error lines;
- RestartCount 0;
- `rust_core` x3: 0 retry/rebalance lines.

Python env check before every TS recreate: `python3 -B scripts/preflight_env.py` (CLAUDE.md coupling 9).

| Order | Role | To | Post-roll check | Rollback (failure only) |
| --- | --- | --- | --- | --- |
| 1 | query_kn_1, then query_kn_2 | python `2.2.4-<sha7>` | warmup/batch smoke, refusal counters | `b9ca6c36` |
| 2 | stream_kn_1, then stream_kn_2 | rust `2.2.4-<sha7>` | gateway refused/overflow/handoff counters 0; writer READY 64/64 | `baa72c96` |
| 3 | market_projector_1, then _2 | rust `2.2.4-<sha7>` | catch-up, cache identity, no `ProjectionCacheMismatch` | `ce629061` |
| 4 | ingestor_okx_swap, then ingestor_binance_usdm | rust `2.2.4-<sha7>` | 5 min watch each: raw_age, producer errors | `7fba2577` |
| 5 | market_data_service_p184w (TS writer) | canonical writer digest | READY 64/64, trace window, native typed counters | B17 `cb5216b8` compose (then B16 `d27db0cf`) |
| - | rust_core x3 | **kept on rc22** if the core post-check is EQUAL (B5 option a, recommended) | n/a | n/a |

- Duration estimate (inferred, not a measured total): about 3 min per role for query/stream/projector (01:45-01:50Z
  on 2026-10-16 covered one writer + two stream rolls), 2 x 5 min ingestor watches, about 5 min for the writer:
  ~40 min.
- Not touched: kafka1-3 (config, CPU, topics), stable_redis, market_cache, binance_bar_edge, core/ingestor configs,
  catalog, consumer manifests, JWT revisions.
- **Q2, owner.**
  - B4: roll the projectors and ingestors (recommended) or scope them out.
  - B5: core option (a) or (b).
- **a'** (if Q1 = option A): G12 on the release image set + canonical writer, ~45 min, own lock. Then the credit
  command of a.3.

## e. G13 isolated + runtime (on the release digests)

1. Regenerate `g13-roles-v224.json` from `docker inspect` after d, then run
   `deployed_mismatches(...) == []` (pre-flight 4).
2. Isolated phase:
   - `dr05-upstream` (6 cells) + `dr08-isolated` (4 cells), ~8-10 min, lock 70 min.
   - Command from g12-g13-final-rerun.md section 2, with the new `--role-images`,
     `--python-image-ref qdl-v2-python:2.2.4-<sha7>`, `--writer-image <canonical>`, `--ts-source-sha <pushed SHA>`.
3. Runtime phase:
   - 66 cells, ~20 min of steps, 30-40 min window, lock 150 min.
   - Needs owner approval of packet v5 regenerated for the release digests.
   - Needs the CI run id from c.3: `make_g13v5.py --ci-run <id> --out-suffix=-ci`. Without it the 7
     `dr01-deployed` cells stay open.
4. Verify: `p184_w05_verifier.py --expect-source-sha <pushed SHA> --expect-image-digest <canonical writer>`.
   Target: 76/76 with no provenance or drift issue.
   - The 21 INHERIT cells (DR01/DR07) run inside the runtime window anyway, so no inheritance amendment is needed.

## f. Final stage-100 on the release digests, with the G4 observer

- Protocol:
  - frozen rules;
  - apparatus `aee0945` or later;
  - G4 observer v5;
  - G9/TS sampler heartbeat dedupe `1e09f87`;
  - G11 v2 oracle;
  - native typed G10 counters.
- The campaign dry run comes first. Lock: ~80 min. Clean host: no other job.
- It measures the release roles, the canonical writer, Kafka B + C on the aged state, and the Codex R4 items:
  - quiet QUOTE renewal at bound/3;
  - delayed and skipped-offset probe counters;
  - weighted Query quota;
  - G9;
  - Redis apply.

  G14 records resources. It is not a headroom gate.
- `protocol_met` must be true. A NOT_MEASURED gate is never upgraded. Stage-100 #3 stays diagnostic only.

## g. Certificate, scorecard, endpoint report

1. `scorecard.json` from f; G12 and G13 from a/a' and e.
2. `endpoint-report.json`, schema `qdl.release-endpoint-report.v2`, with `before_rollout.measurement_identity` per
   role from `equivalence.json`.
3. Certificate, schema `qdl.scoped-release-certificate.v2`, with the fields listed in
   `certificate_fields_after_stage100`. Limits to state:
   - single host;
   - bar_edge scoped out;
   - Kafka C converging;
   - IDR-108;
   - TEST_ONLY burst.
4. CERTIFICATION_LEDGER rows: preparation, release images, Kafka runtime config, final stage-100. Integration
   defect register: IDR-094/095/096/099/107 status, IDR-108 open.
5. Replace every `<...>` in RELEASE_NOTES.md with a receipt value.
6. Record the Unified Plan entries (DL and TS) in the same commit as the evidence (AGENTS 5-8).

## h. PR flow and tag

1. DL:
   - Run `scripts/p184_local_ci.py --sha <head>` first.
   - Push `fix/p184-dl-closure`, open the PR into `dev`. Required CI: consolidated-source-preflight,
     contract-tests, sdk-python310, unit-tests, kn-native-integration.
   - Release PR `dev` -> `main`.
   - Tag `v2.2.4` on the main merge commit. `release.yml` checks the certificate PASS, the `endpoint_report_sha256`
     and the wheel sha256.
2. TS:
   - The grandfather PR (c.3) merges first.
   - Then `feat/v2-rust-first-okx-demo` -> `dev`, then `dev` -> `main`. Each needs one approving review and
     CODEOWNERS.
   - `release-image.yml` publishes `ghcr.io/bobbyaxerol/executorbroker:sha-<main sha>`.
   - If the owner later wants the GHCR digest in production, that is its own roll packet. Its binary is not
     byte-equal to the local build.
3. Push and merge only on the owner's explicit request (CLAUDE.md G6). Never push to `main` directly. Never
   force-push pushed history.

## i. Cleanup (after the release receipts are sealed)

1. Inventory first: `docker ps -a` references, `docker system df` before.
2. Remove by full digest once nothing references them:
   - `baa72c96` (test overlay);
   - superseded rc images not used for rollback;
   - TS B16 `d27db0cf`, after the canonical writer has soaked (keep B17 until then).
3. Keep:
   - the release images;
   - the rollback set of step d;
   - builder `551428d9`, until the release is published.
4. Remove the run worktrees without `--force`: `data-layer-p184-final-run-*`, `trading-system-final-run-*`.
5. Remove the build dirs' `src/` copies, after their hashes are recorded.
6. Record `docker system df` after, the exact removed IDs, and any errors. No broad prune, and no shared volumes
   (Codex standing reminder; AGENTS 12, 15, 33-34a, 37-40).
