# Quant Data Layer v2.4.4 release packet (DRAFT, 2026-11-27)

Status: `implemented`, `tested locally`. Not pushed, not built, not deployed, no PR. Every runtime step below waits
for the owner's GO on its exact blast radius. Companion: `RELEASE_NOTES_DRAFT.md` (bar_edge slices 1-2),
`observation/` (the load-observation gate of 2026-11-25).

## 1. Source

| Item | Value |
|---|---|
| Branch / worktree | `release/v2.4.4`, `/home/bobby/.worktrees/dl-v244-release`, from `release/v2.4.3` `ad2112c` (PR #63, superseded by the bundle PR) |
| Merges | `6dc3322` DEF-J (`4b6e88a`, brings `fix/v244-restore-incomplete-resume` `25aba8b`), `b71b78e` DEF-L (`2af353e`), `6e6c831` DEF-M (`207fdad`) |
| Slices on the branch | `8022b8e` Query typed `HOT_QUIET_BEYOND_MAX`; `f09e288` DL-BL-2026-11-26-K lifecycle API + SDK 2.11.0; this packet commit |
| Version | tags stop at `v2.4.2`; 2.4.3 and 2.4.3.1 (bar_edge) run untagged; this bundle is **v2.4.4**. SDK **2.11.0** |
| Not in the bundle | R1 retention (`feat/dl-bar-retention-demand-tiered`): coordinator item, scope not checked here |

Content (register ids): DL-DEF-2026-11-25-A (v2.4.3 bar_edge, running), DL-BL-2026-11-25-B/C (bar_edge resume and
venue-hole span), DL-BL-2026-11-25-D (per-consumer Query counts), DL-DEF-2026-11-25-E (SDK 2.10.1 lineage),
DL-DEF-2026-11-25-F (shared consumer subject in the compiler), DL-DEF-2026-11-26-G (typed coverage tokens),
DL-DEF-2026-11-26-I (derived bars thread; running as 2.4.3.1), DL-DEF-2026-11-26-J (compiler selector grants),
DL-BL-2026-11-26-K (lifecycle API), DL-DEF-2026-11-26-L (successor probation + OKX QUOTE lane spread),
DL-DEF-2026-11-26-M (stream gateway last value + max-quiet) and its Query typed-reason follow-up.

## 2. Roles: what is rebuilt, recreated, and what is not touched

Production today (read-only `docker inspect`, 2026-11-27 ~05:00Z):

| Role | Running image | v2.4.4 action | Why |
|---|---|---|---|
| `stream_kn_1`, `stream_kn_2` | rust `abafab2c` (2.4.0-c17472c) | **recreate**, rolling, new rust image | DEF-M (`qdl-stream-gateway`: hub last value, max-quiet, memory term, metrics) |
| `ingestor_okx_swap` | rust `e628fbe0` (2.4.2-48137eb) | **recreate**, new rust image + r11 ingestor lane config | DEF-L probation (ingestor binary) and the 3-session QUOTE lane spread (declaration r11: lanes 003/006/007 = 33/32/32, 64 moves, `config_revision` 23 unchanged) |
| `ingestor_binance_usdm` | rust `e628fbe0` | **optional** recreate on the same new image, config unchanged | Same binary. The probation applies to every all-quote lane with an IDR-112 detector. Skipping it keeps Binance on the v2.4.2 binary (a supported mix). Recommendation: recreate it last, after OKX holds for 30 min |
| `query_kn_1`, `query_kn_2` | python `5ea7a7a4` (2.4.2-48137eb) | **recreate**, rolling, new python image | Typed `HOT_QUIET_BEYOND_MAX`, lifecycle endpoint, DEF-G typed tokens, BL-D counts. Lifecycle stays `NOT_CONFIGURED` until `QDL_INSTRUMENT_LIFECYCLE_LEDGER` is set (a separate owner step, section 6) |
| `binance_bar_edge` | python `396a4654` (2.4.3.1-b8935d1) | **recreate**, new python image | BL-B/C resume and venue-hole span (python `stable_bar_edge.py`, OKX/Binance bar_edge adapters, `derived_bars.py`) |
| compiler (DEF-J) | not a role | none at runtime | `scripts/compile_alpha_deployment_bindings.py` ships in the python image and the repo. It is used at the next Crossec render/reseal (r11), TS-side, after the release |
| `rust_core` x3, `market_projector_1/2` | rust `58bdd282` (2.3.0-rc2) | **not touched** | No change in `qdl-core`/`qdl-realtime-core`/`qdl-projector` since their image (diff `48137eb..HEAD -- rust/` touches only the ingestor and the stream gateway) |
| `kafka1-3`, `market_cache`, `stable_redis` | unchanged | **not touched** | |
| TS, writer, hosted alphas | | **not touched** | Additive interfaces only; SDK 2.11.0 is optional for consumers |

Rollout order (the v2.4.2 runbook lesson "stream replica before Query on a catalogue-revision change" does not bind
here: no catalogue revision change; r11 touches only the ingestor lane file):
1. `stream_kn_2`, then `stream_kn_1` (Query falls over between replicas). Gate per replica: last_value metrics present,
   `hot_on_change` hits on a quiet OKX key, no `HOT_AUTHORITY_*` growth, writer refusals not above baseline.
2. `query_kn_2`, then `query_kn_1`. Gate: readiness, the v2.4.2 W2/W3 probe set, `reason_tokens` show
   `HOT_QUIET_BEYOND_MAX` instead of `HOT_READER_UNAVAILABLE` for a key quiet > 900 s (if any), lifecycle endpoint
   answers `NOT_CONFIGURED`.
3. `binance_bar_edge` (with the `.pre-v244` checkpoint copy). Gate: the RELEASE_NOTES_DRAFT bound
   (`ceil(N/16)` turns), `venue_holes_refused` counts open spans only, all OKX/Binance BAR current.
4. `ingestor_okx_swap` with the r11 lane file. Gate: 3 quote sessions 33/32/32, `subscriptions_rejected 0`, IDR-112
   recycles/suppressions and probation outcomes logged, Query execution QUOTE refusals (the DEF-L RCA classes) not
   above baseline for 30 min.
5. Optional: `ingestor_binance_usdm`.

Precondition for every step (host rules): MemAvailable >= 2 GiB, no new OOM kill, no build running, OOM monitor armed;
`python3 -B scripts/preflight_env.py` equivalent for the DL compose; containers started with `docker start`/compose of
the `releases/v2.4.4-*` host-prep directory, never `compose up` from a removed worktree.

## 3. Image build commands (not run)

The recipe bump is the release commit: `RELEASE = "2.4.4"` in `scripts/build_qdl_release_images.py`, with
`PYTHON_REFERENCES` = the python images running then (Query `qdl-v2-python:2.4.2-48137eb` `5ea7a7a4…`, bar_edge
`qdl-v2-python:2.4.3.1-b8935d1` `396a4654…`) and `PRODUCTION_RUST_REFERENCE`/`BINARY_EXPECTATIONS` updated so that
only `qdl-stream-gateway` and `qdl-native-raw-ingestor` are expected DIFFERENT against their production references
(streams `abafab2c`, ingestors `e628fbe0`) and every core/projector binary EQUAL (or EQUAL_LOADED_SECTIONS) against
`58bdd282`. `SDK_VERSION` is already `2.11.0` on the branch.

```sh
cd /home/bobby/.worktrees/dl-v244-release
# 1. dry run: prints the plan, runs no docker command
python3 -B scripts/build_qdl_release_images.py --rev <release-commit> --component all
# 2. build (one cargo build host-wide; heavy slot; correctness class activity record)
scratchpad/hostslots/run_in_slot.sh dl-v244-build 3600 -- \
  python3 -B scripts/build_qdl_release_images.py --rev <release-commit> --component all --build \
  --lock-owner dl-v244-build --max-load 30
# 3. SDK wheel (reproducible; CI sdk-python310 job builds the same)
python3 -B scripts/build_qdl_sdk_release.py --output-dir <scratch>/qdl-sdk
```

Outputs: `qdl-v2-rust:2.4.4-<sha7>` and `qdl-v2-python:2.4.4-<sha7>`, receipts in
`~/.local/state/qdl-v2/build-v2.4.4-<sha7>/`. Then R0 on each image once: pip-audit (python) and trivy, 0 fixable
CRITICAL/HIGH. Record both digests here before any window. Builder: `qdl-rust-builder:1.82.0-v2.3.2` (`2cb66ad6`).

## 4. Rollback

Per role, the image it runs today, on its current env/config; every v2.4.4 change is additive or behind a default:

| Role | Rollback | Notes |
|---|---|---|
| `stream_kn_1/2` | rust `sha256:abafab2c98cbdd9d2fe86f2b26fd6fcf64800ccdd114a3dd5b92803de16a3e98`, same env | Soft rollback without a recreate is not possible; an env-only fallback on the new image is `QDL_KN_LAST_VALUE_MAX_KEYS=0` (ring-only, pre-DEF-M lookups) |
| `query_kn_1/2` | python `sha256:5ea7a7a40ef2d1df920d0f7cf94f22431d53087f8002929e7dffdb4208f7dcc4` | Lifecycle env unset; nothing persistent written by Query |
| `binance_bar_edge` | python `sha256:396a4654014758f70362b3097559e03dcb412d36904d94d3cb7c973f6ccb1637` (2.4.3.1) + `.pre-v244` checkpoint copy restored as uid 10001; sidecar format unchanged | v2.4.3 `50124df8…` remains the older rollback |
| `ingestor_okx_swap` | rust `sha256:e628fbe07afe94e8f716692633e77217a16223ea03abc7ea34edb959a48c72a7` **and** the r10 ingestor lane file | Lanes 006/007 disappear; QUOTE is latest-state, no checkpoint is stranded (DEF-L render note) |
| `ingestor_binance_usdm` | rust `e628fbe0…`, config unchanged | only if it was recreated |

Order: reverse of the rollout. Readers before writers is satisfied: Query understands both gateway versions; the SDK
2.11.0 reader accepts servers without the lifecycle path (404).

## 5. Evidence required (CERTIFICATION_LEDGER rule: re-run only what the pinned commit/image/config changed)

| Item | Pinned at | Status |
|---|---|---|
| Rust job: fmt, clippy -D warnings, `cargo test --workspace` | merged tree `6e6c831` (Rust unchanged since: later commits are python/docs) | **local PASS**: 569 passed / 0 failed / 72 ignored |
| Python unit suite, 4 shards | `f09e288` | see the plan journal entry of 2026-11-27 (local run result, classified failures) |
| Impacted modules for the new slices | `f09e288` | local PASS: 85 (typed reason, hot view, mark memo), 114 (lifecycle + contracts/API/SDK/release counts) |
| `--ignored` Kafka/Redis integration (`committed_reads`, projector) | release tree | CI `kn-native-integration` only (DEF-M touches `committed_reads`' crate) |
| cargo-deny, buf, pip-audit | release tree | CI (`rust-workspace`, `contract-tests`, `release-gates`) |
| Full CI receipt (CI6) | release tree on dev, then main (identical tree inherits) | pending |
| Build receipts (binary expectations, python file hashes) | release commit | pending |
| R0 trivy/pip-audit per image | new digests | pending |
| Runtime windows (section 2 gates), one receipt per step | new digests | pending |
| DEF-M decode CPU | microprofile done (+0.38 % of a core per replica, `207fdad`) | live CPU per stream_kn replica in the window = capacity evidence still to record |
| DEF-L | lane render r11 offline gate PASS | live 3-session lag distribution vs the RCA table after 24 h |
| BL-K | endpoint `NOT_CONFIGURED` in the window; ledger seeding is its own owner step | pending |

The certificate (`upgrade/evidence/releases/v2.4.4/CERTIFICATE.md`) is written after the windows, in the v2.4.2 format
(identity, scope, CI, runtime acceptance, deviations, not changed).

## 6. Separate owner decisions (not in the image windows)

1. **Lifecycle ledger seeding.** `scripts/instrument_lifecycle_record.py init` once, then one `record` per venue listing
   of the next declaration render (OKX SWAP first, then Binance USD-M), into a host path under
   `~/.local/state/qdl-v2/` mounted read-only into `query_kn_1/2` with `QDL_INSTRUMENT_LIFECYCLE_LEDGER`. That is a
   Query recreate with an env/mount delta; rollback = unset the env.
2. **r11 declaration** for the ingestor lane spread (DEF-L) and, separately, Crossec rev7 (DEF-J, parked by the owner).

## 7. CI plan (CLAUDE.md §7b CI1-CI11)

Classifier dry run on the real diff (local, `python3 -B scripts/ci_change_scope.py --base <origin/dev sha> --head
<HEAD sha> --event pull_request --base-ref dev`, 2026-11-27): `full=false`, `python=true`, `rust=true`,
`contracts=true` (because `contracts/v2/openapi.snapshot.json` changed), `base_check=true`, 62 changed files, no
CI-defining file changed (no workflow, Dockerfile, compose, lockfile or classifier change), so CI5 does not force full.

| Job | Run / inherit | Why | Runner, expected wall time (DL repo is public: GitHub-hosted minutes are not billed) |
|---|---|---|---|
| changes, consolidated-source-preflight, repository-scan | run | always | ubuntu-latest, a few minutes |
| contract-tests | run | `contracts=true`; only the OpenAPI snapshot changed, no `.proto`, so `buf breaking` is expected green; PR-base check always runs | ubuntu-latest |
| rust-workspace | run | stream gateway + ingestor changed. No green run of this tree exists to inherit | ubuntu-latest, cache keyed on `Cargo.lock` (unchanged, so warm) |
| kn-native-integration | run | `rust=true`; DEF-M is in `qdl-stream-gateway` whose `committed_reads` test it runs | ubuntu-latest, timeout 30 |
| sdk-python310 | run | SDK 2.11.0 | ubuntu-latest |
| unit-tests x4 | run | python | ubuntu-latest, ~1583 s of suite split in 4 (run 39201550345) |
| release-gates | run | python + rust | ubuntu-latest |
| ci-result | run | verdict | |

- One PR `release/v2.4.4` → `dev`, one push after the local preflight is green (no WIP pushes; this packet and the
  journal ride in the same push). PR #63 is closed as superseded, not merged.
- No `ci:full` label, no `workflow_dispatch`: the scoped PR run already selects every suite this diff affects.
- CI6: the push of the merge to `dev` runs full (non-PR event); `main` merges the identical tree and inherits it
  (CI1, `reuse_run` = that dev run). Merge locally with G2 identity (G7), tag after the owner's GO.
- Flakes: rerun only the failed job (CI7); two red runs with the same cause stop the line.

## 8. DEF-M follow-up 1: REST BBO reconciliation for keys quiet past the maximum (design; not implemented)

**Why not implemented in this bundle.** The change is in the Rust OKX ingestor and adds venue REST traffic and a new
record provenance into the QUOTE stream. Three parts cannot be verified offline: the REST/WS equality semantics on a
real quiet book, the OKX REST budget shared on the host IP, and the freshness-basis contract for a REST-confirmed
quote. Building it without those would be a guess shipped as a fix. Until it lands, `HOT_QUIET_BEYOND_MAX` fails
closed after 900 s and is now typed end to end (gateway → Query → `reason_tokens`).

**Venue facts (OKX docs-v5, read from the 2026-11-27 copy of https://www.okx.com/docs-v5/en/):**
- WS `bbo-tbt` (https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel): "1 depth level
  snapshot will be pushed in the initial push. Snapshot data will be pushed every 10 ms when there are changes in the
  1 depth level snapshot." So a quiet top of book sends nothing, by design; silence alone cannot distinguish quiet
  from a dead subscription. For bbo-tbt, `ts` "is the timestamp when the book is generated by matching engine".
- REST `GET /api/v5/market/books?instId=<id>&sz=1`
  (https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-order-book): rate limit **40 requests per 2
  seconds, rule IP**; `asks`/`bids` entries `[price, size, "0", order count]`, `ts` "Order book generation time",
  `seqId`; "updated once every 50 milliseconds", and "returns the latest data once the server-side cache has been
  updated".
- REST `GET /api/v5/market/ticker?instId=<id>`
  (https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-ticker): **20 requests per 2 seconds, rule IP**;
  `bidPx/bidSz/askPx/askSz`, `ts` "Ticker data generation time".
- WS limits (https://www.okx.com/docs-v5/en/#overview-websocket-connect): "The total number of
  'subscribe'/'unsubscribe'/'login' requests per connection is limited to 480 times per hour."

**Design (OKX first; Binance after, with its own docs).**
1. **Tracker** in `rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs`, one per all-quote lane, modelled on
   `BookLivenessProof` (same file, per-channel `last_frame_at_ns`): per QUOTE binding the last frame's receive time,
   venue `ts` and top of book (bid px/sz, ask px/sz) as decoded strings.
2. **Due rule.** A binding with no frame for `QDL_OKX_QUOTE_RECONCILE_AFTER_S` (default 300, must be <
   `QDL_KN_MAX_QUIET_SECONDS` 900 minus one probe period) and not probed within that period, on a session that is
   itself live (other frames or pong within the IDR-112 window); never during a recycle or probation.
3. **Budget.** A token bucket per process: default 8 requests / 2 s on `books` (20 % of the 40/2 s IP limit), with
   the host's other OKX REST users (bar_edge history, instruments captures) named in the change record. 97 quotes
   all quiet at once drain in about 25 s.
4. **Fetch** `books?instId=<id>&sz=1` with the existing `reqwest` client pattern (`fetch_binance_book_snapshot`:
   3 s connect, 5 s total). Code ≠ "0", HTTP error, timeout or a 50011 rate-limit answer is `UNAVAILABLE`: count it,
   emit nothing, the key stays quiet and Query keeps refusing typed.
5. **Compare** REST top (asks[0][0..1], bids[0][0..1]) with the tracker's last WS top, as exact decimal strings.
   - **MATCH**: the WS stream was correct and quiet. Emit a confirmation (step 6).
   - **MISMATCH**: the subscription missed changes. Emit a gap/resync marker for that key (the existing
     out-of-order/resync flags make the gateway refuse `HOT_RECORD_GAPPED` until the next record), then
     unsubscribe/subscribe that `instId` on its session (counts against 480/h; bounded to N per hour per session), so
     the initial push re-seeds the top. Log `qdl_okx_quote_reconcile{outcome=MISMATCH}`.
6. **Confirmation record: contract decision needed (owner).** Two options:
   - (a) A QUOTE record with the REST top and a per-record freshness basis `PROVIDER_CONFIRMATION` and the confirmation
     time as basis time. Needs an additive envelope field (proto minor) read by the core, the gateway last value and
     Query's freshness oracle (`qdl/query/service.py` already special-cases `PROVIDER_CONFIRMATION` for reference
     products, `stable_catalog.py` validates it per binding today).
   - (b) A session-liveness-style marker (like the books liveness marker) that renews the key's quiet age in the
     gateway without a new price record. Smaller contract, but the gateway's `max_quiet` rule would then accept a
     price older than 900 s on venue-confirmed equality.
   Recommendation: (a), because the consumer sees an explicit basis instead of an older price that looks fresh.
7. **Tests.** Fake-venue e2e (the DEF-L `cdd31043` harness): quiet key → probe → MATCH → gateway last value renewed
   with the confirmation basis; MISMATCH → resync + resubscribe; UNAVAILABLE → nothing; budget exhaustion order;
   never during recycle. Then a bounded live read-only probe on OKX Paper.

**Exact remaining work:** owner choice of 6a/6b; proto change (6a) with buf compatibility; the tracker, scheduler,
fetch and compare in the ingestor with unit tests; the core/gateway/Query handling of the basis; the fake-venue e2e;
an REST budget note for the host IP; a live Paper probe; then a DL release (rust image: ingestors, core if 6a, stream
gateway if 6a; python Query if 6a).
