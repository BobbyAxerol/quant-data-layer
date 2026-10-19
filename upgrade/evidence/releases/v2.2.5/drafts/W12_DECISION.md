# DL v2.2.5 window: W12 decision record (TS writer slot)

Status: proposal for the window owner, 2026-10-19 (Claude). **Verdict: W12-A**: keep writer image 2deb90b0 and switch
only its TLS identity mount, as in `MAINTENANCE_WINDOW_PACKET.md` section 5. W12-B (roll the writer to the TS
v2.0.0 release image) needs every proof item below to PASS. Item 2 passes. Items 1 and 3 are incomplete and item 4
has only a component-level measurement, so W12-B is not taken in this window.

Rule (coordinator + Codex review `p184-v225-bounded-window-review-20261019.md` section C): W12-B only when the release
image is final and has its closure proof, its T1 runtime-path proof, the T1 correctness tests and an isolated
interval 0 vs 50 ms A/B; otherwise W12-A. Machine-readable evidence: `W12_DECISION_EVIDENCE.json` (same folder).

| Item | Result | Evidence |
|---|---|---|
| 1 Binary + image closure | **NOT COMPLETE** (binary: function-level PASS; GHCR image: not verified) | below, 1a-1c |
| 2 T1 runtime route proof | **PASS** | below, 2 |
| 3 T1 tests (Codex C list) | **PARTIAL**: tests pass locally; PR not opened, CI not run | below, 3 |
| 4 Performance A/B 0 vs 50 ms | **PARTIAL**: component-level isolated A/B done; full-writer A/B not done | below, 4 |

## Inputs

- Production writer: `market_data_service_p184w`, image `sha256:2deb90b0cc52…` (label revision TS 30f50a91), binary
  sha256 `aefe6a47…b433`. Never touched; all production reads were `docker inspect`, `docker exec` reads,
  `docker cp` of trace files and host log reads.
- Release candidate: `ghcr.io/bobbyaxerol/executorbroker:sha-3b4466521df2ba78e2a68a6ce1f6da819346fe1b`, digest
  `sha256:9a251870cae21e0588f51d9db4f6fe47e293983e593496efd05fefffe9fc0ff9` (TS v2.0.0, main 3b446652).
- Host measurement lock `claude-w12-writer-proof` held 13:19:58-13:50:05Z (released on completion, not at the
  deadline). It covered the binary rebuilds and the isolated A/B. Containers: `tsa-eq-*` (builder) and
  `ts-p184-t1ab-*` / `ts-p184-w12-*`, all `--rm` with CPU caps.

## 1. Binary and image closure

1a. **Writer binary (Rust).** Recipe: the Dockerfile builder `ghcr.io/pyo3/maturin@sha256:e557fd4c…` (maturin 1.8.2,
rustc 1.84.1) and the Dockerfile's third cargo command, run on `git archive` of the revision.
- 30f50a91 -> 2deb90b0's binary: bit-identical rebuild (earlier proof).
- 30f50a91 -> 54c4dbaa (rustfmt only): only panic `Location` line/column words (180) and the build-id differ
  (earlier proof).
- 54c4dbaa -> main 3b446652: the only writer source change is `latency.rs` `LatencyAggregates::observe` (the slice
  cell is updated in place instead of through `Option::map`, then moved at its last use) plus a doc line in
  `session.rs`. `Cargo.lock` is unchanged since 54c4dbaa, and the writer has no dependency on `trading-core`.
- Local rebuild of main: sha256 `448ad37e…7ec6`. Its size equals the deployed binary's (6 547 864 bytes).
- Function-level comparison. Both revisions were built unstripped and disassembled with objdump; addresses and
  rip-relative displacements were normalised and ThinLTO `.llvm.N` suffixes removed. Of 4216 functions, 4214 are
  identical and none exists on only one side. The two that differ:
  - `market_data_writer::latency::LatencyAggregates::observe`: the reviewed change, 1813 -> 1817 instructions with a
    different stack-slot assignment;
  - `ring_core_0_17_14__p256_point_mul_base_vartime`: trailing alignment `nop` padding only.
- Stripped pair (deployed `aefe6a47` vs main `448ad37e`): the differing sections are `.text`, `.rodata`,
  `.eh_frame(_hdr)`, `.gcc_except_table`, `.rela.dyn`, `.got`, `.data.rel.ro`, `.data`, `.init_array` and the
  build-id.
  - Classification: a code-address shift after the grown `observe()` (jump tables, FDEs, relocations, GOT and vtable
    entries) plus the panic-location lines. This follows from the function-level result.
  - It was **not verified word by word**, because the unstripped build changes symbol hashes and so is not
    byte-equal to the stripped one.

1b. **Python closure (git and deployed image).**
- The deployed image's 43-file writer closure (`scripts/p184_writer_release_build.py` `WRITER_FILE_ROOTS`) equals git
  30f50a91. So does the 56-file import closure of `services/market_data/main.py`, computed by an AST walk that
  includes function-level imports.
- 30f50a91 -> 3b446652 import closure (57 files): one file added, eight changed. Every change was read.

| File | Change |
|---|---|
| `adapters/market_data/cursor_group_commit.py` (added) | T1 store |
| `adapters/market_data/data_layer_v2.py` | T1 wiring |
| `shared/config.py` | adds `DATA_LAYER_V2_CURSOR_MIN_COMMIT_INTERVAL_MS` = 50 and `MARKET_DATA_V1_COMPAT_ENABLED` |
| `services/market_data/main.py` | V1 feed-lease renewal moves behind the V1 switch |
| `services/market_data/data_layer_bridge.py` | V1 pub/sub, V1 REST recovery and VN quotes move behind the V1 switch |
| `services/market_data/native_plan.py` | lint only (`zip(..., strict=False)` is the default behaviour) |
| `services/market_data/native_worker.py` | lint only |
| `services/market_data/trailing_observation.py` | lint only |
| `alpha_sdk/order_batch.py` | lint only (import order) |

- In the 43-file set but outside the import closure: `native_handoff.py` and `scripts/p184_n1_cursor_handoff.py`.
  Both are lint only.
- Runtime dependencies: `uv.lock`, `pyproject.toml` and `vendor/` are unchanged. The deployed image's interpreter is
  CPython 3.10.22 with qdl-sdk 2.4.0.
- Behaviour delta of the V1 switch in production: the live writer logs `market active scope refresh failed
  reason=ConnectError` every 30 s, because V1 lease renewal targets the stopped V1 Data Layer. Under v2.0.0 that
  renewal is off. The scope update itself is unchanged.

1c. **GHCR image: NOT VERIFIED.**
- `docker manifest inspect` of the digest returns `unauthorized`, and this session holds no credential for the TS
  package. So nothing here compares the image's actual `/app` files, its `/usr/local/bin/market-data-writer` or its
  site-packages against the local rebuild.
- `python:3.10-slim` in the Dockerfile is not digest-pinned, so the release image's base layer and interpreter may
  differ from 2deb90b0's.
- To complete: pull by digest with a read credential, run `image_probe` (sha256 of `/app`, site-packages and
  `/usr/local`) on both images, then check that the image binary equals `448ad37e…` and that the `/app` diff equals
  1b.

## 2. T1 runtime route proof (production, read-only) - PASS

- Cursor root `/app/state/data-layer-v2/cursors/fixed-workers-v1/`. Two key snapshots were taken 14 s apart
  (12:53:56Z and 12:54:10Z).
  - Python workers `count-4-worker-0..3`: BAR/1m, BOOK_DELTA and BOOK_SNAPSHOT keys advance (10 / 11 / 11 routes).
    Their TRADE/QUOTE keys do not move; they are leftovers from before the native cutover.
  - `native-4` (the Rust `CursorCommitter`, `native_plan.py:203`): TRADE and QUOTE advance (11 QUOTE, 10 TRADE).
  - Process list: 4 Python workers plus one `market-data-writer` (native). Env `MARKET_DATA_NATIVE_FEEDS=TRADE,QUOTE`.
- Live store counters (`qdl_cursor_persistence`, 12:53:25-13:08:26Z, 901 s):

| Worker | commits/s | ACKs per commit | store I/O per commit | pending peak |
|---|---|---|---|---|
| worker-0 | 27.9 | 1.15 | 14.4 ms | 7 |
| worker-1 | 24.5 | 1.11 | 14.2 ms | 7 |
| worker-2 | 20.8 | 1.03 | 13.6 ms | 4 |
| worker-3 | 28.2 | 1.12 | 14.4 ms | 7 |

  All four workers: errors 0. Total: 101 commits/s, 113 ACKs/s.
- ACK path: these feeds ACK inline (`data_layer_v2.py`, non-overlap branch `await acknowledge_checkpoint` before the
  next batch). Overlapped ACK applies only to TRADE/QUOTE, which are native here.
- Consequence: under W12-B the 50 ms default would govern the BOOK and BAR routes. Each ACK holds that route's read
  loop.
- The T1 setting lives in the shared builder, so every service built from v2.0.0 that streams through
  `build_versioned_data_layer_client` gets it. This is outside W12 and noted for TS.

## 3. T1 tests (Codex C) - PARTIAL

- On main already (commit 0d1273f9): same-route overlapping ACKs; replace/new generation with older queued work;
  file-fsync ENOSPC (single request); aclose while spacing; queue saturation with an unaffected route; replay after
  restart never regresses; SIGKILL readback.
- Added on TS branch `test/writer-t1-proof` (from dev 883fc193, commit
  `0c5148e1ec86960dcc8c02de14ef7c237c3e243a`, worktree `/home/bobby/.worktrees/ts-writer-t1-proof`):
  - failure at each durability step of a coalesced batch with same-route overlap: file fsync ENOSPC, rename EIO,
    directory fsync EIO. No request of the failed commit is ACKed, no temp file is left, and the last durable
    cursor is kept. In the directory-fsync case the file may already show the applied but never-ACKed offsets,
    which is never ahead of apply. Recovery by re-ACK;
  - a caller cancelled while its ACK waits for the spacing is released only after its write is durable, and it does
    not stall the rest of the batch;
  - the TEST-only harness `scripts/p184_t1_cursor_ab.py`;
  - plan journal entry E-A17.
- Local preflight: test image `local/trading-system-test:p184-r59` (python 3.10.21) with the vendored qdl-sdk 2.4.0
  wheel, `--network none`. Result: 18 passed, 3 times; the 4 new cases passed.
- No product code change was needed, so there is no v2.0.1.
- Open:
  - the branch is pushed, but the PR into `dev` is not opened: the available API token cannot see the TS repository
    (404). It can be opened at `https://github.com/BobbyAxerol/ExecutorBroker/pull/new/test/writer-t1-proof`;
  - CI has not run;
  - the SIGKILL test still does not model power loss at each fsync step. Fault injection now covers each step's
    error path, not data loss in the page cache.

## 4. Interval 0 vs 50 ms A/B (isolated) - PARTIAL

**Apparatus.** `scripts/p184_t1_cursor_ab.py`, component level. It runs the real SDK 2.4.0 `FileCursorStore`
(interval 0, the builder's choice) against `GroupCommitFileCursorStore` (50 ms); the TS dev code equals main 3b446652
for these files. Each route runs the writer's inline-ACK loop: a batch of at most 64 events, at most 20 ms wait, the
recorded apply, then an awaited ACK. One store and cursor file per worker, seeded to the production document size.
- Workload: the production writer's own delivery trace (4 workers, 22 BOOK routes, DL gateway-send arrival times,
  recorded Redis apply durations), the same 300 s window for every arm (33 985 events), and a 20 s stall at 200 s
  for catch-up.
- Container: `--network none`, 2 CPUs, order A1 B1 B2 A2, under the host lock.
- Calibration: arm A reproduces production. It gives 90 commits/s (production 101, which also includes BAR),
  1.17 ACKs per commit (production 1.03-1.15), 13.8-14.0 ms store I/O per commit (production 13.6-14.4) and
  BOOK_DELTA arrival -> apply p50 of 24.9 ms (production gateway-send -> Redis reply p50 25.3-27.0 ms).

| Metric (4 workers) | A1 (0) | A2 (0) | B1 (50) | B2 (50) |
|---|---|---|---|---|
| commits/s | 90.4 | 90.2 | 73.4 | 73.4 |
| ACKs/s | 105.8 | 105.8 | 104.2 | 104.2 |
| ACKs per commit | 1.17 | 1.17 | 1.42 | 1.42 |
| write syscalls (300 s) | 27 112 | 27 061 | 22 015 | 22 012 |
| container write IOs (cgroup) | 29 676 | 29 194 | 24 189 | 23 925 |
| container CPU s | 61.6 | 61.7 | 56.1 | 55.7 |
| host iowait share | 11.2 % | 11.3 % | 10.7 % | 10.8 % |
| pending p99 / peak | 3 / 6 | 3 / 6 | 5 / 6 | 5 / 6 |
| apply -> durable ACK p50 (worst worker) | 15.6 ms | 15.7 ms | 74.6 ms | 74.6 ms |
| apply -> durable ACK p99 (worst worker) | 32.1 ms | 32.8 ms | 99.0 ms | 98.5 ms |
| apply -> durable ACK max | 46.8 ms | 44.8 ms | 107.2 ms | 114.6 ms |
| BOOK_DELTA arrival -> apply done p50 (worst worker) | 24.9 ms | 24.9 ms | 37.6 ms | 37.3 ms |
| catch-up after 20 s stall (worst route) | 0.13 s | 0.14 s | 0.37 s | 0.32 s |

**Reading.**
- The repeated arms agree (A1/A2 and B1/B2 within 1-2 %).
- At this workload T1 at 50 ms removes about **19 % of cursor commits and write I/O** (90 -> 73 commits/s, 29.4k ->
  24.1k write IOs) and about 9 % of the component's CPU. Host iowait barely moves.
- In exchange, the durable ACK becomes about **+59 ms at p50 and +67 ms at p99**, the inline BOOK_DELTA apply
  latency about **+12.5 ms at p50** on the busiest workers, and catch-up after a stall rises from about 0.13 s to
  0.3-0.4 s.
- The reduction is small because most commits carry a single route's ACK (1.17 ACKs per commit): the inline-ACK
  loops do not produce enough concurrent ACKs to coalesce.
- As Codex noted, 50 ms is not an upper bound on ACK latency (max 107-115 ms).

**Not measured.**
- Steady-state inline p95/p99: in this run the injected stall dominates the tails.
- A full writer instance. Stream transport, projection and Redis apply are replaced by the recorded schedule. The
  isolated N1/G12 apparatus has no BOOK publisher, and BOOK feeds are the only Python-store routes.
- Power-loss behaviour.

**Implication for W12-B.** Even with complete proof, the measured trade-off (about -19 % write I/O for about
+12 ms BOOK apply p50 and about +60 ms cursor ACK) argues for
`DATA_LAYER_V2_CURSOR_MIN_COMMIT_INTERVAL_MS=0` on the writer (the builder then installs the plain SDK store, the
current production behaviour) or a T1 redesign before using 50 ms. This is a TS decision, recorded here because it
changes the W12-B env.

## Decision and what W12-B would still need

- **W12-A** for this window: the packet's W12 row and rollback are unchanged.
- W12-B later needs:
  - GHCR image probe and `/app` diff (1c);
  - the TS PR opened and its CI green (3);
  - a decision on the interval (4), and, if 50 ms is kept, a full-writer or longer A/B with steady-state
    inline-latency tails;
  - the W12-B rollback (2deb90b0 + old identity) and its own evidence, as the packet requires.
- Side finding (DL tooling, not fixed here): `scripts/host_measurement_lock.py acquire` treats the agent's own
  sibling shell as a foreign job when the `--purpose` text contains a `FOREIGN_PROCESSES` keyword such as "pytest".
  The `pgrep -af` match covers the wrapper shell's command line, so such an acquire waits on itself.
