# P18.4 G13 - DR01-DR08 inherit / rerun matrix (final DL + writer digests)

Status: PLAN / NOT RUN (2026-10-10, Claude coordinator, read-only planning). No DR, build or runtime action was
taken to produce this. Runner: `scripts/p184_g13_final.py` (tested locally, 22 unit tests; never executed against
Docker). Gate: G13 in TS `upgrade/p184-phase-d-gates.md` - "DR01-DR08 (incl. DR05 typed gap/generation fence,
inverse feeds): every cell closed once on the final DL + writer digests", cells in TS
`contracts/baseline/rust-first-v2/p18/p184-r2-case-matrix.v2.json` (TS `3538b48`; v1 kept unchanged and readable):
76 DR cells = the 65 of v1 (DR01 9, DR02 8, DR03 6, DR04 10, DR05 12, DR06 4, DR07 12, DR08 4) + 11 inverse cells
scoped `OKX_INVERSE` (DR04 4, DR05 5, DR06 2). Counted with a Python read of the file; the runner refuses
(`CASE_MATRIX_MISMATCH`) if its `DR_CELLS` differ from the matrix.

Update 2026-10-10 (later): blockers 1, 4 and 5 below are resolved in code (harness image parameters; TS
WRONG_ACCOUNT Gateway runner `6c44bf2` wired as step `dr01-wrong-account`; matrix v2 inverse cells; DR05
harnesses scoped for OKX_INVERSE, TS `acce72d`). Every one of the 76 DR cells now has a producer step.
New run inputs for the inverse DR05 faults: `dr05_catalog` and an export that carries the BTC-USD-SWAP BOOK
binding (blocker 5).

## Result in one line

**Inherit 0 of 76 cells.** No DR cell has a receipt on final-qualifying provenance, and every DR exercises at least
one path changed after its newest receipt. What can be inherited are inputs, not cells (see "Inherited inputs").
The minimum is therefore eight harness steps, ~57 min of estimated wall time run serially (hard timeouts sum to
150 min). `dr01-wrong-account` is Gateway-dependent and closes with the TS release.

## Receipts that exist (all found with `grep -rl` over `~/.local/state` for `DR0n.` cell ids)

| DR | Newest receipt | Provenance | Why it cannot close a G13 cell |
|---|---|---|---|
| DR01 | `~/.local/state/qdl-v2/p184-dr01-20261008-agent/dr01.rehearsal.jsonl` (10-08 05:06Z) | `p184_dr01_runtime`, `development: true`, source/image all zeroes | Runbook `p184-dr-runtime-runbook.md`: CONTROLLED_SIM must NEVER close a DR01 runtime cell; the deployed probe (`p184_dr01_deployed.py`) never ran |
| DR02, DR03, DR04, DR06 | none | - | No rows exist (runners `p184_dr_alpha_reads.py`, `p184_dr_views.py` only dry-run, TS plan "DR runners DR02-DR04/DR06", "not evidence") |
| DR05 GAP/GENERATION | `~/.local/state/trading-system/p184-dr05-joint-20261009/r16/dr05-rows.jsonl` (10-09 12:27Z), 4/4 ok at ISOLATED_FAULT | writer `411a6a8` / `sha256:b5274bb9...`; DL python `77682eea` (rc2), core image `658a9570` + binary overlay `baafcb48` (built from `d68fb2f`), stream `1f13408f` (`r16/work/handoff.json`) | Rehearsal on non-final DL and writer; never relabelled (runbook) |
| DR05 STALE/DISCONNECT/FALLBACK/QUIET | none | - | `p184_dr05_isolated.py` / `p184_dr_views.py` quiet watch never produced rows |
| DR07 | none for R2 DR07 | - | `~/.local/state/qdl-v2/dr07-*-20261007` is the runtime-hardening "DR07", a different label (TS plan, "DR label warning") |
| DR08 | `~/.local/state/qdl-v2/dr08-continuity-20261008/candidate-wire-02/evidence.jsonl` (10-08 13:46Z), dev attempts 1-12 | `mode: DEVELOPMENT`, TS zeroes, `stream_binary_override` `b5e1c2c4`, n=1 restart | Development rows deliberately emit `ok=false` (`p184_dr08_isolated.py` `evidence_rows`); final mode needs >= 5 restarts |

## DL changes after the receipts (DL `fix/p184-dl-closure`, HEAD `6674d0b`)

Commands: `git log --oneline 43bbcf0..HEAD -- qdl rust qdl_sdk consumers config contracts Dockerfile*`
(rc2 source `43bbcf0` = IDR-062 top-change materialization, already inside every rc2-based receipt),
`git log --oneline d68fb2f..HEAD -- rust/qdl-core rust/qdl-realtime-core rust/qdl-kafka/src/bin/qdl-production-core.rs`,
`git log --oneline 0b374bc..HEAD -- rust/qdl-stream-gateway`.

| Item | Commit(s) | Changed paths (role) |
|---|---|---|
| IDR-040 fix 1 | `4eb08dc` (prerequisite, before rc2) | in all receipts |
| IDR-040 fix 2 (proof flag OFF) | `1a2daf1` | core `rust/qdl-core/src/{canonical,l2_adapter}.rs`, `rust/qdl-realtime-core/src/lib.rs`, ingestor, query `qdl/query/{results,service}.py`, `qdl/runtime/stable_source.py`, SDK models/projection |
| IDR-062 core snapshot-on-top-change | `43bbcf0`, `63238bf` (l2_book copies) | core; `63238bf` is after rc2 |
| IDR-082 event_id view field | `616342c` | query router/results/stable_source, SDK |
| IDR-084 hot MARK | `922a6ec`, `bd74faa`, `ec89a31` | query `qdl/runtime/kn_query_backend.py` |
| Astra R1/R3/R6 | `57881a2` | query `kn_hot_view`, `kn_query_backend`, `kn_read_view`, `execution_mark_index`, router |
| Astra R2 | `fdd379e` | stream `rust/qdl-stream-gateway/src/hub.rs` |
| Astra R4 | `d4f5ec9` | ingestor `qdl-native-raw-ingestor.rs` |
| OKX make-before-break + session lineage | `f861663` | ingestor, `qdl/runtime/session_liveness.py` |
| G5 replica redirect | `47bec04` | query lanes/service/router, SDK transport |
| OKX port 443 | `6674d0b` | `config/v2/stable-acquisition-bindings.yaml` (URLs only, plan revision 19 kept), OKX client, production catalog |
| G5 warmup/BAR perf | `65e0757`, `76950c8`, `5c81c25` | query row cache / cold BAR, SDK transport |
| DR08 terminal status | `0b374bc` | stream `service.rs` |
| SDK 2.2.0 | `359c9ee` | SDK package metadata |
| heartbeat liveness | not in HEAD `6674d0b` (in progress) | if it lands before the build it changes ingestor/session paths; covered by the RUNTIME steps, no extra step, but it must be inside the final digests |

Unchanged since rc2 (checked, empty `git log`): `qdl/security`, `qdl/consumer`, `consumers/`,
`qdl/reference`, `qdl/adapters/okx/reference.py`, `qdl/canonical/reference.py`, `qdl/adapters/binance`.
They do not rescue any DR, because each DR also crosses a changed role (router, query backend, core or stream)
and has no final-provenance receipt to inherit from.

## Matrix

| DR | Cells | Exercised paths (roles) | Receipt | Decision | Reason | Step / est. min |
|---|---|---|---|---|---|---|
| DR01 | 9 (7 DL + 2 WRONG_ACCOUNT via TS Gateway runner) | alpha SDK credentials -> Query HTTP `qdl/api_v2/router.py`, `qdl/security/data_plane.py` (JWT/realm/revision/entitlement), binding compiler `qdl/consumer/universal_release.py`, `consumers/stable/alpha-*-paper.yaml` | rehearsal only | RERUN | no deployed-probe receipt; router changed (`616342c`, `65e0757`, `76950c8`, `57881a2`, `47bec04`) | `dr01-deployed` / 3, `dr01-wrong-account` / 2 (Gateway-dependent) |
| DR02 | 8 | alpha `AlphaDataLayerV2` -> SDK transport -> Query warmup/batch, row cache, cold BAR (`qdl/query/*`, `kn_query_backend`, `stable_source`) | none | RERUN | warmup/batch path and SDK transport changed | `dr02-dr03-alpha` / 10 |
| DR03 | 6 | same as DR02 plus 1m append (Query BAR, projector-fed spool) | none | RERUN | same | (same step) |
| DR04 | 10 + 4 inverse | Stream/Query -> TS writer -> TS Redis -> `MarketCacheReader` + Risk validator; MARK/INDEX hot path, L2 view | none | RERUN | IDR-084/Astra MARK, IDR-082, IDR-040 fix 2 changed; writer digest new | `dr04-dr06-views` / 14 (binance, okx, okx_inverse) |
| DR05 | 12 + 5 inverse | GAP/GEN: raw -> core fence (`canonical.rs`, `realtime-core/lib.rs`, `qdl-production-core.rs`) -> stream/query `kn_hot_view` -> writer clone -> production `_validate_book`; STALE/DISCONNECT/FALLBACK: writer clone transport; QUIET: TS views | r16 rehearsal (4 cells) | RERUN | core `1a2daf1`, query `57881a2`/`922a6ec`, stream `0b374bc`/`fdd379e`, writer all changed after r16; 8 cells never produced | `dr05-upstream` / 6 (3 scopes), `dr05-transport` / 8 (3 scopes), QUIET inside `dr04-dr06-views` |
| DR06 | 4 + 2 inverse | production `services.risk_engine.execution_market_context` on a copy of the live context (writer views) | none | RERUN | reads changed MARK/L2 views; writer digest new | (in `dr04-dr06-views`) |
| DR07 | 12 | alpha `alpha_sdk.reference` -> Query reference routes (`router.py`) -> `qdl/reference/*`; FX proof from TS DB | none | RERUN | no R2 DR07 rows; router changed | `dr07-reference` / 2 |
| DR08 | 4 | isolated kafka -> core -> stream `service.rs`/`subscription.rs`/`hub.rs` -> query GetSnapshot | development only | RERUN | stream `fdd379e` after the overlay; final mode never ran | `dr08-isolated` / 12 |

Estimated minutes: from measured runs where they exist (DR05 joint r16: 236 s init-to-down from
`r16/orchestrator.jsonl`; DR08 dev ~3 min for n=1, driver cap 660 s for n=5) and otherwise from harness defaults
(DR02/03 `--append-seconds 150` + `--restart-seconds 90` per venue; views `--quiet-seconds 300` per venue; DR05
transport `--ready-seconds 180` + 2 x (20 s cut + <= 120 s recover)). These are estimates, not measurements.

## Inherited inputs (not cells)

- DR05 sanitized core export `~/.local/state/qdl-v2/dr05-observer-20261008/public-export.json` (SHA256 `9cf857a2...`,
  BINANCE ETHUSDT and OKX ETH-USDT-SWAP BOOK bindings, no transport). `6674d0b` changed only OKX transport URLs and
  `consumers/` is unchanged, so the export stays valid for the two linear scopes; TLS/JWT material is regenerated per
  run by the harness. It does NOT cover the OKX_INVERSE scope, so G13 needs a fresh export with the inverse BOOK
  binding as well (blocker 5).
- DR05/DR08 harnesses are reused as they are at the release commit (latest DR05 harness changes: `522cf3e` D6
  reset stderr, `75673bc` G12 helper); no new harness is written for G13.
- Unit tests of the harnesses are not rerun for G13; they gate the build CI (`scripts/p184_local_ci.py`).

## Blockers before the final run (none is fixed here)

1. **Harness image pins - RESOLVED (code, tested locally).** `scripts/p184_dr08_isolated.py` `IMAGES` /
   `QUERY_IMAGE_REF` are now defaults only: `--orchestrate` takes `--image ROLE=sha256:ID` (repeatable) and
   `--query-image-ref`, passes the resolved set to every container as `P184_DR08_IMAGES` / `P184_DR08_QUERY_IMAGE_REF`,
   and `validate_query_provenance` / `validate_packet` compare against those attested values (pinned defaults when
   absent, so behaviour without flags is unchanged). `p184_dr05_dl_fault.py --init/--prepare` takes the same
   `--image` flag and seals the set in `handoff.json` `images`; `--up/--ready/--down`, reset, channel, readiness and
   the in-container `--material` read it via `image_set(work)`. The runner passes the final python (query, python)
   and rust (core, stream) images; kafka/redis keep the harness defaults. `HARNESS_IMAGE_PIN` refusal removed.
2. **DR08 reads the deployed core.** `orchestrate` copies `/runtime/core.json` from `qdl_v2_stable_candidate-rust_core-1`
   and refuses a core image other than its pin; with final pins DR08 runs only after the final rust image is
   deployed. The runner therefore checks the deployed inventory before DR08 too.
3. **Python code is mounted from the checkout** (`-v <DL worktree>:/app:ro`, `PYTHONPATH=/app`) in DR05/DR08: the
   checkout must sit at the release commit. The runner refuses if HEAD differs from `--dl-source-sha` or tracked
   files under `qdl rust scripts generated consumers config contracts` are modified.
4. **DR01.WRONG_ACCOUNT_DENIED@BINANCE/OKX - producer exists, Gateway-dependent.** TS `6c44bf2`
   `scripts/p184_dr01_wrong_account.py` (probe inside the deployed Gateway, record on the host). Runner step
   `dr01-wrong-account` (runtime; inputs `gateway_container`, `gateway_image_digest`): probe via
   `docker exec -i <gateway> python -B - probe`, then `record --rows .. --source-sha .. --image-digest <writer>
   --gateway-image-digest ..`. At run time it is refused as `GATEWAY_NOT_RUNNING` (receipt `deferred`, other steps
   still run, overall exit not failed by it) unless the container is running the declared gateway image; it closes
   with the TS release, when the Gateway runs the final image incl. IDR-085.
5. **Inverse feeds - cells added (TS matrix v2), 5 without producer.** Deployed writer binding (r23, manifest
   revision 11, `market_data_service_p184w` mount) grants OKX BTC-USD-SWAP exactly QUOTE, BOOK_SNAPSHOT, BOOK_DELTA,
   MARK_INDEX_PRICE (TRADE/BAR route BLOCK, no V1 fallback). Added `@OKX_INVERSE`: DR04 QUOTE_BBO,
   L2_SNAPSHOT_DELTA_SEQUENCE_DEPTH, MARK_INDEX_COMPONENTS, RISK_REREADS_OWN_AUTHORITY; DR06 both cells; DR05
   STALE_TYPED, DISCONNECT_RECONNECT, GAP_REFUSAL, GENERATION_CHANGE_FENCED, APPROVED_FALLBACK_AND_RETURN_NO_FORBIDDEN_V1.
   Not added (reasons in the matrix `inverse_excluded`): DR01 (no alpha grant), DR02/DR03 (BAR), DR04.EXPLICIT_LAST
   and DR05.QUIET_TYPED (TRADE), DR07 (no reference grant), DR08 (@ANY). `p184_dr_views.py` reads the inverse scope
   (`--venue okx_inverse`), so DR04/DR06 inverse run inside `dr04-dr06-views`. Inverse DR05 producers (code,
   tested locally): DL `p184_dr05_dl_fault.py` scopes (`SCOPES`; `--init --scope BINANCE --scope OKX --scope
   OKX_INVERSE --catalog <dr05_catalog>`): `isolated_config` takes the exported BTC-USD-SWAP BOOK binding, the
   material catalog filter keeps exactly each scope's BOOK_SNAPSHOT/BOOK_DELTA (no QUOTE/MARK/TRADE), consumer
   `<ns>.okx_inverse`, OKX `books` frames for `BTC-USD-SWAP`; publisher, observer, channel and reset are scope-aware.
   TS `acce72d`: `p184_dr05_upstream.py --venues ... OKX_INVERSE` and `p184_dr05_isolated.py --venues` (default all
   three). The runner passes all three scopes to both DR05 steps.
   **Run inputs this adds (not checked here, read-only session):** `dr05_catalog` = the deployed source catalog that
   defines BTC-USD-SWAP (the repo `config/v2/stable-source-bindings.yaml` is revision 11 without it; the rev-12 paper
   catalog copy is `~/.local/state/qdl-v2/p184-d1-rev11-20261009/base-catalog.yaml`; the deployed
   `/runtime/stable-source-bindings.yaml` is root-only and was not read), and a fresh `dr05_public_export` that
   includes the deployed core's BTC-USD-SWAP BOOK binding (the 10-08 export `9cf857a2...` has the two ETH bindings
   only; `--init` refuses with `exactly one complete exported BOOK binding required: OKX_INVERSE`).
6. **DR01 record needs CI confirmation** (`--ci-confirmed` only when the inputs file states `dr01.ci_confirmed: true`
   for the TS source SHA).
7. RUNTIME steps read the shared stack and open extra Paper sessions: they need the owner-approved runtime packet
   (`--runtime-approval`) after the D5 rollout and the final writer roll.

## Runner

```text
# plan only (no Docker, nothing created)
python3 -B scripts/p184_g13_final.py --python-image sha256:<py> --rust-image sha256:<rust> \
  --python-image-ref qdl-v2-python:2.2.3-<sha7> --writer-image sha256:<writer> \
  --ts-source-sha <40hex> --dl-source-sha <40hex> --phase all --inputs <inputs.json> --dry-run
# isolated phase (DR08 after the final rust image is deployed; --ts-root must hold the TS matrix v2)
... --phase isolated
# runtime phase (after rollout + writer roll, approved packet)
... --phase runtime --inputs <inputs.json> --runtime-approval <packet>
# one failed step again, fresh receipt dir
... --only dr05-transport
```

Inputs JSON keys: `dr05_public_export` (must include the BTC-USD-SWAP BOOK binding), `dr05_catalog`, `redis_image`, `postgres_image`, `dr01.{inventory, deployments.BINANCE,
deployments.OKX, compose, alpha_service, compose_cwd?, catalog?, ci_confirmed?}`, `alpha.{containers.*, symbols.*,
universe.*?}`, `ts_container`, `gateway_container`, `gateway_image_digest`, `dr07.{connections, fx_account,
dsn_env?}`, `dr08_restart_samples?` (default 5).

Receipts: `~/.local/state/qdl-v2/p184-g13-<YYYYMMDDTHHMMSSZ>/` (exclusive create): `plan.json`,
`<step>/receipt.json` (argv, cwd, UTC start/end, exit codes, timeout/error, sha256 of every output,
removed leftovers), harness rows and logs, `summary.json`. Cleanup: only containers with the step's own prefix
(`p184-dr05-`, `p184-dr08-`) that did not exist before the step, removed by exact ID; harnesses keep their own
`cleanup.json`/`--down`. `harness_ok` is the harness exit status; cells close only through the TS matrix check.
