# P18.4 G13 - DR01-DR08 inherit / rerun matrix (final DL + writer digests)

Status: PLAN / NOT RUN (2026-10-10, Claude coordinator, read-only planning). No DR, build or runtime action was
taken to produce this. Runner: `scripts/p184_g13_final.py` (tested locally, 16 unit tests; never executed against
Docker). Gate: G13 in TS `upgrade/p184-phase-d-gates.md` - "DR01-DR08 (incl. DR05 typed gap/generation fence,
inverse feeds): every cell closed once on the final DL + writer digests", frozen cells in TS
`contracts/baseline/rust-first-v2/p18/p184-r2-case-matrix.v1.json` (65 DR cells: DR01 9, DR02 8, DR03 6, DR04 10,
DR05 12, DR06 4, DR07 12, DR08 4; counted with a Python read of that file).

## Result in one line

**Inherit 0 of 65 cells.** No DR cell has a receipt on final-qualifying provenance, and every DR exercises at least
one path changed after its newest receipt. What can be inherited are inputs, not cells (see "Inherited inputs").
The minimum is therefore seven harness steps, ~52 min of estimated wall time run serially (hard timeouts sum to
140 min), plus the two DR01 WRONG_ACCOUNT_DENIED cells that have no producer anywhere.

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
| DR01 | 9 (7 DL + 2 WRONG_ACCOUNT) | alpha SDK credentials -> Query HTTP `qdl/api_v2/router.py`, `qdl/security/data_plane.py` (JWT/realm/revision/entitlement), binding compiler `qdl/consumer/universal_release.py`, `consumers/stable/alpha-*-paper.yaml` | rehearsal only | RERUN | no deployed-probe receipt; router changed (`616342c`, `65e0757`, `76950c8`, `57881a2`, `47bec04`) | `dr01-deployed` / 3 |
| DR02 | 8 | alpha `AlphaDataLayerV2` -> SDK transport -> Query warmup/batch, row cache, cold BAR (`qdl/query/*`, `kn_query_backend`, `stable_source`) | none | RERUN | warmup/batch path and SDK transport changed | `dr02-dr03-alpha` / 10 |
| DR03 | 6 | same as DR02 plus 1m append (Query BAR, projector-fed spool) | none | RERUN | same | (same step) |
| DR04 | 10 | Stream/Query -> TS writer -> TS Redis -> `MarketCacheReader` + Risk validator; MARK/INDEX hot path, L2 view | none | RERUN | IDR-084/Astra MARK, IDR-082, IDR-040 fix 2 changed; writer digest new | `dr04-dr06-views` / 12 |
| DR05 | 12 | GAP/GEN: raw -> core fence (`canonical.rs`, `realtime-core/lib.rs`, `qdl-production-core.rs`) -> stream/query `kn_hot_view` -> writer clone -> production `_validate_book`; STALE/DISCONNECT/FALLBACK: writer clone transport; QUIET: TS views | r16 rehearsal (4 cells) | RERUN | core `1a2daf1`, query `57881a2`/`922a6ec`, stream `0b374bc`/`fdd379e`, writer all changed after r16; 8 cells never produced | `dr05-upstream` / 5, `dr05-transport` / 8, QUIET inside `dr04-dr06-views` |
| DR06 | 4 | production `services.risk_engine.execution_market_context` on a copy of the live context (writer views) | none | RERUN | reads changed MARK/L2 views; writer digest new | (in `dr04-dr06-views`) |
| DR07 | 12 | alpha `alpha_sdk.reference` -> Query reference routes (`router.py`) -> `qdl/reference/*`; FX proof from TS DB | none | RERUN | no R2 DR07 rows; router changed | `dr07-reference` / 2 |
| DR08 | 4 | isolated kafka -> core -> stream `service.rs`/`subscription.rs`/`hub.rs` -> query GetSnapshot | development only | RERUN | stream `fdd379e` after the overlay; final mode never ran | `dr08-isolated` / 12 |

Estimated minutes: from measured runs where they exist (DR05 joint r16: 236 s init-to-down from
`r16/orchestrator.jsonl`; DR08 dev ~3 min for n=1, driver cap 660 s for n=5) and otherwise from harness defaults
(DR02/03 `--append-seconds 150` + `--restart-seconds 90` per venue; views `--quiet-seconds 300` per venue; DR05
transport `--ready-seconds 180` + 2 x (20 s cut + <= 120 s recover)). These are estimates, not measurements.

## Inherited inputs (not cells)

- DR05 sanitized core export `~/.local/state/qdl-v2/dr05-observer-20261008/public-export.json` (SHA256 `9cf857a2...`,
  BINANCE ETHUSDT and OKX ETH-USDT-SWAP BOOK bindings, no transport). `6674d0b` changed only OKX transport URLs and
  `consumers/` is unchanged, so the export stays valid; TLS/JWT material is regenerated per run by the harness.
- DR05/DR08 harnesses are reused as they are at the release commit (latest DR05 harness changes: `522cf3e` D6
  reset stderr, `75673bc` G12 helper); no new harness is written for G13.
- Unit tests of the harnesses are not rerun for G13; they gate the build CI (`scripts/p184_local_ci.py`).

## Blockers before the final run (none is fixed here)

1. **Harness image pins.** `scripts/p184_dr08_isolated.py` pins `IMAGES` (rc2 python `77682eea`, core `658a9570`,
   stream `1f13408f`) and `QUERY_IMAGE_REF/DIGEST` (2.2.3-rc2-43bbcf0); `p184_dr05_dl_fault.py` imports the same
   `IMAGES`. The containers re-validate against those constants (`validate_query_provenance`, readiness image
   check), so the runner cannot override them from the host. Needs an additive, reviewed change in the harness
   (final image IDs as arguments, constants as defaults) before `dr05-upstream`/`dr08-isolated` can run. The
   runner refuses with `HARNESS_IMAGE_PIN` until then (its dry run shows this today).
2. **DR08 reads the deployed core.** `orchestrate` copies `/runtime/core.json` from `qdl_v2_stable_candidate-rust_core-1`
   and refuses a core image other than its pin; with final pins DR08 runs only after the final rust image is
   deployed. The runner therefore checks the deployed inventory before DR08 too.
3. **Python code is mounted from the checkout** (`-v <DL worktree>:/app:ro`, `PYTHONPATH=/app`) in DR05/DR08: the
   checkout must sit at the release commit. The runner refuses if HEAD differs from `--dl-source-sha` or tracked
   files under `qdl rust scripts generated consumers config contracts` are modified.
4. **DR01.WRONG_ACCOUNT_DENIED@BINANCE/OKX have no qualifying producer.** `grep -rln WRONG_ACCOUNT` finds only
   the CONTROLLED_SIM `scripts/p184_dr01_runtime.py` (DL), which the runbook forbids for closing DR01, and no TS
   script outside the case matrix. The runbook assigns them to a TS Gateway runner that does not exist yet.
5. **Inverse feeds.** G13 names inverse feeds, but the frozen matrix v1 has no inverse-scoped DR cell and
   `p184_dr_views.py` `IDENTITY` is linear only (ETHUSDT / ETH-USDT-SWAP). Either a matrix revision adds inverse DR
   cells (gates file line 119: "Inverse gets its own cells; linear is not inverse evidence") or G13's wording is
   narrowed by the owner. Owner decision; not silently dropped.
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
# isolated phase (after blocker 1; DR08 also after the rust image is deployed)
... --phase isolated
# runtime phase (after rollout + writer roll, approved packet)
... --phase runtime --inputs <inputs.json> --runtime-approval <packet>
# one failed step again, fresh receipt dir
... --only dr05-transport
```

Inputs JSON keys: `dr05_public_export`, `redis_image`, `postgres_image`, `dr01.{inventory, deployments.BINANCE,
deployments.OKX, compose, alpha_service, compose_cwd?, catalog?, ci_confirmed?}`, `alpha.{containers.*, symbols.*,
universe.*?}`, `ts_container`, `dr07.{connections, fx_account, dsn_env?}`, `dr08_restart_samples?` (default 5).

Receipts: `~/.local/state/qdl-v2/p184-g13-<YYYYMMDDTHHMMSSZ>/` (exclusive create): `plan.json`,
`<step>/receipt.json` (argv, cwd, UTC start/end, exit codes, timeout/error, sha256 of every output,
removed leftovers), harness rows and logs, `summary.json`. Cleanup: only containers with the step's own prefix
(`p184-dr05-`, `p184-dr08-`) that did not exist before the step, removed by exact ID; harnesses keep their own
`cleanup.json`/`--down`. `harness_ok` is the harness exit status; cells close only through the TS matrix check.
