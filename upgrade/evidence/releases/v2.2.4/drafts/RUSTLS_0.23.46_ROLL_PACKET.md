# v2.2.4 rustls 0.23.46 roll packet (draft, NOT executed)

Status: prepared 2026-10-18 by Claude. Nothing in this packet has been run against production. Executing it needs the
owner's approval of exactly this blast radius and rollback. The packet is modular: each role is one self-contained
module (preconditions, command, HOLD, rollback), so it can be merged into the combined window with the resource
package (ingestor CPU, Kafka broker quota, cleaner throttle, Query CPU). Merge rule in section 8.

## 1. What changes

- Source: DL `9dab224` (`fix(deps): bump rustls 0.23.45 -> 0.23.46 ...`), owner decision 2026-10-18 ~15:50Z
  ("Nang rustls ngay"): RUSTSEC-2026-0346..0349, zero-exception policy. `Cargo.toml` pin `=0.23.46`; `Cargo.lock`
  changes rustls version + checksum only (no companion crate); resolved features `log, logging, ring, std, tls12`
  before and after; ring provider unchanged.
- Image: `qdl-v2-rust:2.2.4-9dab224` = `sha256:27c79b53400eea5582713a3477034c8561f533755e09a94223e254bffadd5c92`,
  label revision `9dab22429ea44566587f31d9f56f8b059933c795`, build receipt
  `~/.local/state/qdl-v2/build-v2.2.4-9dab224/receipt.json` (`e38fd275...`), status PASS (labels, image = context,
  ldd 0 missing, uid 10001, no TEST clock env, no feature marker, `unexpected []`, `missing []`).
- Python image: unchanged, `qdl-v2-python:2.2.4-31a5155` `3e9e4d3a` (9dab224 changes no python runtime file).

## 2. Per-binary equivalence vs the deployed binaries

Reference `qdl-v2-rust:2.2.4-83bef59` `a9679def` (live on both ingestors). Its gateway is whole-file equal to the
live stream image `60f83a51`, its realtime core to the live core image rc22 `7c848aad`, its projector to `60f83a51`
(build receipt 83bef59). Tool `scripts/p184_rust_symbol_diff.py` (normalized disassembly per function, GOT slots
resolved, data references masked; limits in its summary). Evidence
`~/.local/state/qdl-v2/build-v2.2.4-9dab224/symbol-diff-vs-a9679def/` (`summary.json`, one report per binary).

Whole-file (build receipt): every one of the 10 rustls-linked binaries DIFFERENT from every reference (expected,
new crate metadata hashes); `qdl-projector` and `qdl-venue-core-certify` whole-file EQUAL to `a9679def` (and the
projector to `60f83a51`).

Function level (`summary.json` `49f2164e...`; "rustls fn" = functions whose demangled name has a rustls path,
release / reference; "rustls paths" = `rustls-0.23.x/src/` panic-location strings in the file; OTHER = changed
functions without a rustls name, by class):

| Binary (role) | Verdict | rustls fn | rustls paths | changed rustls fn | OTHER: crate-hash consts / calls rustls / CODEGEN | added / removed OTHER |
| --- | --- | --- | --- | --- | --- | --- |
| `qdl-native-raw-ingestor` (ingestors) | NOT_CONFINED | 1186 / 1189 | 33 / 32 | 101 (+6 / -10) | 5 / 0 / 16 | 2 / 1 |
| `qdl-stream-gateway` (stream_kn) | NOT_CONFINED | 1416 / 1413 | 46 / 45 | 81 (+8 / -7) | 4 / 5 / 68 | 3 / 5 |
| `qdl-realtime-core` (rust_core x3) | NOT_CONFINED | **0 / 0** | **0 / 0** | 0 | 4 / 0 / 5 | 0 / 0 |
| `qdl-production-core` | NOT_CONFINED | 0 / 0 | 0 / 0 | 0 | 4 / 0 / 9 | 1 / 2 |
| `qdl-binance-shadow` | NOT_CONFINED | 792 / 792 | 30 / 28 | 58 | 1 / 1 / 8 | 3 / 2 |
| `qdl-kafka-smoke` | CONFINED_TO_RUSTLS | 0 / 0 | 0 / 0 | 0 | 1 / 0 / 0 | 0 / 0 |
| `qdl-phase92-primary-rehearsal` | CONFINED_TO_RUSTLS | 0 / 0 | 0 / 0 | 0 | 1 / 0 / 0 | 0 / 0 |
| `qdl-authority-rehearsal` | NOT_CONFINED | 0 / 0 | 0 / 0 | 0 | 1 / 0 / 1 | 1 / 0 |
| `qdl-phase91-canary-rehearsal` | NOT_CONFINED | 0 / 0 | 0 / 0 | 0 | 1 / 0 / 1 | 1 / 1 |
| `qdl-parity-replay` | NOT_CONFINED | 0 / 0 | 0 / 0 | 0 | 0 / 0 / 2 | 0 / 1 |
| `qdl-projector` (projectors) | EQUAL (whole file) | - | 0 / 0 | - | - | - |
| `qdl-venue-core-certify` | EQUAL (whole file) | - | 0 / 0 | - | - | - |

Reading:
- The change is NOT confined to rustls functions at machine-code level in any binary that contains code from a crate
  depending on rustls. The non-rustls differences are of two kinds: CRATE_HASH_CONSTANTS (64-bit `TypeId` constants
  that derive from the crate metadata hash, e.g. the downcast in `qdl_realtime_core::generation_failure`) and
  CODEGEN: same source, different machine code. Inspected examples: in the core,
  `DecimalValue::encode_raw` inlines `Coefficient::encode` in the reference and calls it out of line in the release
  (prost encoders of `RawProviderEnvelope` / `FeedStateEvent`, register allocation); in the gateway 68 functions of
  h2 / hyper / tonic / prost / tokio monomorphizations. Mechanism (inferred, not proven): the codegen-unit
  partitioning and inlining decisions of the crates whose metadata hash changed (qdl-core, qdl-kafka,
  qdl-stream-gateway; default 16 release codegen units) differ. Rust source delta `83bef59..9dab224`: Cargo.toml /
  Cargo.lock (rustls) plus, for the gateway only, the whitespace-only `cargo fmt` commit `f6ce816` (proved on
  2026-10-18 to change only panic-location line/column words, `fmt-equivalence-addendum.md`); no other Rust or
  generated source changed. Legacy symbol mangling does not show generic
  arguments, so a gateway/ingestor generic instantiated over a rustls type (for example h2 over a `TlsStream`)
  is counted as OTHER.
- `qdl-realtime-core` contains no rustls code in either build (0 functions, 0 rustls source paths): the four
  advisories cannot apply to the deployed core binary. Its new build still differs in 5 prost-encoder functions of
  the Kafka envelope hot path and 4 TypeId constants.
- What the tool does not prove: behaviour equivalence of CODEGEN changes (same source, so expected; not measured).

## 3. Roles

| Module | Role | From -> To | rustls code in the binary | Recommendation |
| --- | --- | --- | --- | --- |
| M1 | `ingestor_okx_swap` (0.5 cpu) | `a9679def` -> `27c79b53` | yes (venue WSS/HTTPS client) | roll |
| M2 | `ingestor_binance_usdm` (0.75 cpu) | `a9679def` -> `27c79b53` | yes | roll |
| M3 | `stream_kn_1` (0.75 cpu) | `60f83a51` -> `27c79b53` | yes (mTLS server, Query view client) | roll |
| M4 | `stream_kn_2` (0.75 cpu) | `60f83a51` -> `27c79b53` | yes | roll |
| M5 | `rust_core`, `rust_core_2`, `rust_core_3` | `7c848aad` -> `27c79b53` | **none** in either build | **do not roll** (section 6) |

Not touched by this packet: `market_projector_1/2` (`qdl-projector` whole-file EQUAL in `27c79b53`, `a9679def` and
`60f83a51`), `query_kn_1/2` (python), `binance_bar_edge`, kafka1-3, `stable_redis`, `market_cache`, the TS writer
`market_data_service_p184w`, alphas; every config, catalog, consumer manifest, realm dir, JWT revision, env, mount and
cpu cap; no Kafka offset, Redis key or spool row.

## 4. Preconditions (every module; stop if any fails)

1. Owner approval of the modules to run (and of section 8 if merged with the resource package).
2. Host lock (AGENTS 34a), from `/home/bobby/.worktrees/data-layer-p184-closure`:
   `python3 -B scripts/host_measurement_lock.py check` shows no foreign job, then
   `python3 -B scripts/host_measurement_lock.py acquire --owner claude-dl-rustls-roll --purpose "v2.2.4 rustls 0.23.46 roll M1-M4" --minutes 90`.
   Release it after the last module's HOLD or after a rollback.
3. Live containers equal the saved inspects in `$PK/inspect/` (captured 2026-10-18T16:23:55Z): same container id,
   image, RestartCount 0. Otherwise recapture and re-run the dry runs (section 9).

   | Role | Container | Image | RestartCount |
   | --- | --- | --- | --- |
   | `ingestor_okx_swap` | `34c93a82aa7c` | `a9679def` | 0 |
   | `ingestor_binance_usdm` | `d9c4b76c03e9` | `a9679def` | 0 |
   | `stream_kn_1` | `0f46d52cdbb2` | `60f83a51` | 0 |
   | `stream_kn_2` | `2cbf41ac53bd` | `60f83a51` | 0 |
   | `rust_core` / `_2` / `_3` | `2a60f79227ac` / `3a9ca9b6da02` / `21b9dffde9c3` | `7c848aad` | 0 |
4. `docker image inspect qdl-v2-rust:2.2.4-9dab224 --format '{{.Id}}'` prints exactly `$RUST`.
5. No `ROLL_*` variable in the shell (`env | grep ^ROLL_` empty) except those a command sets itself.
6. Baseline HOLD: `S0=$(date -u +%s); python3 -B $P/hold_check.py $S0 $PK/hold-pre.json` shows writer
   `READY 64 64` and `violations []`.

```
cd /home/bobby/.worktrees/data-layer-p184-closure
T17=~/.local/state/trading-system/p184-20261005/runtime/p1/rc17/rollout_rc17_v2.py        # sha256 8de00c46...
P=~/.local/state/qdl-v2/releases/v2.2.4/rollout/postchecks                                  # read-only tools
PK=~/.local/state/qdl-v2/releases/v2.2.4/rollout-rustls-0.23.46
RUST=sha256:27c79b53400eea5582713a3477034c8561f533755e09a94223e254bffadd5c92
OLD_ING=sha256:a9679defdd970aaa961736d414cf8c443c2ead2f26ebf2e68eb481dda6725784
OLD_STREAM=sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e
BUNDLE=63efd9ffb9a4cb9eca7a98bdd3189444fd1629674e3e0a883da557f939218c2c                   # live stream bundle (both)
```

Postcheck tools (read-only, sha256): `hold_check.py` `ae8768f3...`, `verify_service.py` `f6605d23...`,
`ingestor_watch.py` `48c5aa08...`, `ingestor_lane_report.py` `d46e971d...` (as in the 10-16/10-17/10-18 rolls).

## 5. Modules M1-M4 (one role at a time, next module only after the previous HOLD passes)

### M1 / M2: ingestors (`R=ingestor_okx_swap`, then `R=ingestor_binance_usdm`)

```
S=$(date -u +%s); echo $S > $PK/$R.start
ROLL_ROOT=$PK python3 -B $T17 $RUST $R 2>&1 | tee $PK/roll-$R.out    # 0 accepted | 2 rolled back (verified) | 5 operator
python3 -B $P/verify_service.py qdl_v2_stable_candidate-$R-1 $PK/inspect/$R.json $RUST | tee $PK/verify-$R.json
python3 -B $P/ingestor_watch.py $R 300 > $PK/watch-$R.jsonl
python3 -B $P/ingestor_lane_report.py $R $S > $PK/lanes-$R.json
python3 -B $P/hold_check.py $S $PK/hold-after-$R.json $PK/hold-pre.json
```

HOLD (both): rc17v2 exit 0 (compose 0, image id = `RUST`, effective service = candidate, diff `['image']`, writer READY
64 64); `verify_service.py` `image_ok` true, `mismatch []`, `restart_count` 0, healthy; `hold_check.py`
`violations []`; `ingestor_watch.py` 5 samples with every lane of the venue present, `core_raw_age_ws_max_ms` in the
pre-roll range, no `qdl_native_runtime_retry`. TLS proof: every lane reached `qdl_native_session_ready` on the new
container (rustls handshake to the venue). OKX additionally: `lanes-$R.json` `armed` = one lane `okx-public-003`, 6
bindings, default staleness config, `f1_fields_present_on_every_lane` true. STOP and roll back: a lane without
session ready after 120 s, any TLS / certificate error line, more than 2 staleness recycles in 5 min.

Rollback -> `a9679def`: automatic (rc17v2 rolls back once to the captured pre-roll service, exit 2). Manual (exit 5 or
a HOLD failure after acceptance): `docker compose -f $PK/$R-rollback-<stamp>.compose.json up -d --no-deps
--force-recreate $R`, then `verify_service.py qdl_v2_stable_candidate-$R-1 $PK/inspect/$R.json $OLD_ING`. Reference
compose if the stamp file is missing: `$PK/reference-rollback/$R.compose.json` (built from the live inspect
16:23:55Z; equal to the compose that put `a9679def` live on 2026-10-18 03:35/03:41Z, `service_mismatch []`).

### M3 / M4: stream gateways (`R=stream_kn_1`, then `R=stream_kn_2`)

```
S=$(date -u +%s); echo $S > $PK/$R.start
ROLL_EXPECT_BUNDLE_SHA=$BUNDLE ROLL_ROOT=$PK python3 -B $T17 $RUST $R 2>&1 | tee $PK/roll-$R.out
python3 -B $P/verify_service.py qdl_v2_stable_candidate-$R-1 $PK/inspect/$R.json $RUST | tee $PK/verify-$R.json
python3 -B $P/hold_check.py $S $PK/hold-after-$R.json $PK/hold-pre.json        # at t+0 and again at t+5 min
```

HOLD: rc17v2 exit 0 (diff `['image']`, new gateway's progress line carries `bundle_sha256` = `BUNDLE`, writer READY 64
64); `verify_service.py` as above; `hold_check.py` `violations []` (gateway refused/overflow/queue-isolation counter
deltas, see the 10-16 disposition: the writer's TRADE BINANCE BTCUSDT overflow under host load is pre-existing and
recorded, not a gateway regression). TLS proof: the writer's subscriptions fail over to the other gateway and come
back over mTLS (READY 64 64 at t+5 min), 0 TLS / handshake error lines in the new gateway's log. Serves meanwhile: the
other gateway (subscriptions fail over, ~seconds). STOP and roll back: writer READY < 64 after 5 min, any handshake /
certificate error, `hub_live_budget_refused` > 0.

Rollback -> `60f83a51`: automatic as M1; manual `docker compose -f $PK/$R-rollback-<stamp>.compose.json up -d
--no-deps --force-recreate $R`, verify with `$OLD_STREAM`. Reference compose `$PK/reference-rollback/$R.compose.json`
(equal to the 10-16 composes that put `60f83a51` live, `service_mismatch []`).

A rollback of one module does not roll back earlier accepted modules (no interface change between ingestors and
gateways). After any rollback the run stops; release the lock and report.

## 6. M5 rust_core x3: recommendation NOT to roll

Facts: `qdl-realtime-core` in rc22 `7c848aad` has 0 rustls functions and 0 rustls source paths, and so does the new
build: RUSTSEC-2026-0346..0349 are not present in the deployed core binary, so rolling it brings no security change.
The new core binary differs from the measured rc22 core in 5 CODEGEN functions (prost encoders on the Kafka envelope
path) and 4 TypeId constants, i.e. the cores would run machine code that G12 / stage-100 have not measured. A core
roll needs the simultaneous replace (the rolling path caused the rc19 / RUST18 group storms on 2026-10-15, memory
"consumer-group change needs multi-member test"): all three cores pause ~12 s.

Recommendation: do not roll the cores in this window; the release record keeps rc22 `7c848aad` for `rust_core` x3
with this statement. If the owner still wants one image digest everywhere, module M5 is ready (not recommended now):

```
# M5 (optional, owner decision): simultaneous replace, same tool and shape as rc22 on 2026-10-15 13:32Z
TC=~/.local/state/trading-system/p184-20261005/runtime/p1/rc17/rollout_core_simultaneous.py   # sha256 b5c9ca2b...
C=~/.local/state/qdl-v2/releases/v2.2.3-rc18-core-batch1024                                   # live core configs
ROLL_CORE_CONFIG_DIR=$C \
ROLL_CORE_CONFIG_SHA256S='{"rust_core.json":"e11550dc6eac8d9311469afca7a42f567596ebd7b4625925c85a7f773521a482","rust_core_2.json":"9d8ed362ed8ddb4881bb3679900f798ee87be71423bb099610cbd1ad01e8f229","rust_core_3.json":"f79a6b99860560b23f0e0901b7ea839a037eacba1b2ab91aade322ff6b538d42"}' \
ROLL_EXPECT_BATCH_SIZE=1024 ROLL_LOCK_OWNER=claude-dl-rustls-roll ROLL_ROOT=$PK \
python3 -B $TC sha256:7c848aadcb0dd28178bc6dc2b58c42df6c2c235de6d4bfd6602ef7c19fa64bfb $RUST 2>&1 | tee $PK/roll-core.out
# exit 0 accepted | 2 rolled back simultaneously (verified) | 4 nothing stopped | 5 operator
python3 -B $P/hold_check.py $S $PK/hold-after-core.json $PK/hold-pre.json
```

M5 HOLD (inside the tool): P0 group 3x2 stable on every sample, 0 core retries in the last 300 s, writer READY 64 64,
lock held with >= 1200 s left; M3 group re-formed 3x2 <= 30 s after compose start, 120 s window with the group 3x2 on
every sample, per new member retries 0, rewinds <= 2, rewind_failed 0, started <= 1, drain failures 0, RestartCount 0,
writer READY 64 64. Rollback: the tool's one simultaneous rollback onto the saved rollback composes (verified: old
image, exact old service, group 3x2 <= ROLLBACK_REFORM_S, 60 s clean); manual (exit 5): stop all three, then
`docker compose -f $PK/reference-rollback/<role>.compose.json up -d --no-deps --force-recreate <role>` for the three
roles in parallel (references equal the 2026-10-15 rc22 simultaneous composes, `service_mismatch []`). Dry run:
rc17v2 `candidate()` on the saved inspects with the live core configs: `diff ["image"]`, configs e11550dc / 9d8ed362 /
f79a6b99, `volume_changed []` for all three (the simultaneous tool has no dry-run mode; it imports this candidate()).

## 7. After the roll

- Journal the receipts in the DL plan (one entry); release record v2.2.4: rust `27c79b53` on ingestors + stream
  gateways, `60f83a51` projectors, rc22 `7c848aad` core (if M5 is not run), python `3e9e4d3a` Query.
- Certification: the rolled binaries are not byte-identical to the measured ones (section 2), so G12 / stage-100
  evidence on `a9679def` / `60f83a51` is not inherited automatically; whether a rerun is required is an owner /
  coordinator decision (not part of this packet).

## 8. Merging with the resource package (one window)

The resource package is `OPT_RESOURCE_PACKAGE.md` (round 2, e6559e4): live `docker update` of cpu/memory caps (no
recreate) for kafka1-3, query_kn_1/2 and ingestor_okx_swap, one variable per 10-min HOLD, applied after the rust-image
roll (its section 11).

- Order: M1-M4 first (each a recreate, `['image']` only), then the OPT `docker update` steps. Never both in one HOLD.
- `ingestor_okx_swap` is in both: the OPT cap raise is a `docker update` on the container M1 created. A recreate
  after it (an M1 rollback, or any rc17v2 run) re-applies the compose cap 0.5, so: if M1 is rolled back, the OPT
  raise for that role must be re-applied or dropped explicitly; M1's rollback compose keeps cap 0.5 (pre-roll state).
  rc17v2 refuses the raise itself (`evidence_does_not_justify_raise`, OPT packet), so do not pass `ROLL_CPUS`.
- kafka / Query steps are independent of M1-M4 (no recreate of a rust role).
- Never run any module inside a measurement window (G12 / G13 / stage-100) of any agent.

## 9. Dry runs (no docker mutation; outputs under `$PK`, sha256 in `$PK/SHA256SUMS`)

Captured 2026-10-18T16:23:55Z (`$PK/inspect/`, 9 roles incl. the untouched projectors for reference).

- `ROLL_DRY_RUN=1 ROLL_INSPECT_DIR=$PK/inspect python3 -B $T17 $RUST <role>` (saved inspects, the tool's own
  candidate and diff assertions, no docker call), exit 0 each:
  - `dry-run-ingestor_okx_swap.json` (`61205575...`): `diff ["image"]`, cpus 0.5 -> 0.5, `a9679def` -> `27c79b53`,
    `volume_changed []`.
  - `dry-run-ingestor_binance_usdm.json` (`f89072cd...`): `diff ["image"]`, cpus 0.75 -> 0.75, same images.
  - `dry-run-stream_kn_1.json` (`95ea16b5...`) / `dry-run-stream_kn_2.json` (`4574de50...`), with
    `ROLL_EXPECT_BUNDLE_SHA=$BUNDLE`: `diff ["image"]`, cpus 0.75, `60f83a51` -> `27c79b53`, `volume_changed []`.
    `BUNDLE` = the `bundle_sha256` of both live gateways (60 progress lines each in the last 10 min, 16:2xZ).
  - M5 only: `dry-run-rust_core{,_2,_3}.json` (`81dfa636...`, `ebdb4511...`, `90dc113e...`) with the live core
    configs and `ROLL_EXPECT_BATCH_SIZE=1024`: `diff ["image"]`, cpus 1.0 / 1.0 / 0.5, `7c848aad` -> `27c79b53`.
- Reference rollback composes `$PK/reference-rollback/` (`index.json` `7ad5b8e5...`): built from the live inspects
  with rc17v2 `service_from_inspect` + `compose_doc`; `service_mismatch` against the composes that put the current
  images live is `[]` for all seven (ingestors 2026-10-18 03:35/03:41Z, stream 2026-10-16 17:19/17:22Z, cores
  2026-10-15 13:32Z rc22 simultaneous).
- `$PK/SHA256SUMS` lists every file above plus the build receipt and the symbol-diff reports.
