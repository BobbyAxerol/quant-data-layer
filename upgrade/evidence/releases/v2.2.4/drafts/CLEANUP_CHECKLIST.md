# v2.2.4 post-release cleanup checklist (DRAFT, nothing here has been executed)

Status: inventory read on 2026-10-17 ~11:00Z (`docker system df`, `docker images`, `docker ps -a --filter ancestor=`,
`git worktree list`, `du`, `df`; all read-only). Execute only after the release receipts are sealed (FINAL_STEPS i),
inside its own host-measurement lock, with a cleanup packet and the owner's approval (CLAUDE.md 3b, memory
`kn-rollback-cleanup-and-resource-approval`). No broad prune. No shared volumes. A refusal from any command (for
example `git worktree remove` on a dirty tree) stops the checklist and is reported.

Rules: remove images by full digest (never by tag); remove build cache by record id; remove only objects this release
work created; record the removed ids and `docker system df` before and after.

## 0. Baseline (read again right before executing)

At drafting: images 104 total / 40 active, 31.8 GB, 8.306 GB reclaimable; containers 71 / 42; volumes 41 / 14,
98.46 GB, 218.3 MB reclaimable; build cache 392 records / 47 active, 29.54 GB, 455.5 MB reclaimable; `/` 484 GB with
220 GB free (55 % used).

- [ ] `python3 -B scripts/host_measurement_lock.py check` is clean; own lock acquired (<= 60 min).
- [ ] `docker system df` and `df -h /` saved before.
- [ ] No other agent's G13 v5.4 window, final stage-100 or roll is running (the lock file at drafting belonged to
      `claude P18.4 G13 v5.4 window`).

## 1. Images by digest

Reference counts are `docker ps -a -q --filter ancestor=<id> | wc -l` at drafting. "Keep" means the item is a rollback
target or a base of the release; it is removed only by a later, separate packet.

| Digest (12) | Tag(s) | Refs | Action |
| --- | --- | --- | --- |
| `60f83a5106b0` | `qdl-v2-rust:2.2.4-c7ce7d3` | 6 | KEEP (release) |
| `c21e164ac4db` | `qdl-v2-python:2.2.4-c7ce7d3` | 3 | KEEP (release) |
| `a44349a10e52` | `local/trading-system:p184-writer-release-6af62a7e` | 1 | KEEP (production writer) |
| `734b08f1fefa` | `...p184-writer-release-e21f6edd` | 0 | KEEP as writer rollback until a44349a1 has soaked; then remove |
| `7c71e3af5628` | `...p184-writer-release-77a6d671` | 0 | remove after 734b08f1 is removed (older rollback); keep until the owner confirms the writer soak |
| `cb5216b877f1` | `trading-system-market-data:p184-writer-cand-b17-68f2572f` | 0 | candidate for removal after the soak (B17 rollback chain: 734b08f1 -> 7c71e3af -> B17) |
| `d27db0cffe15` | `...p184-writer-cand-b16-5636e443` | 0 | candidate for removal after B17 is removed (FINAL_STEPS i.2) |
| `7cf232ace561` | `...p184-writer-cand-32593f35` | 0 | candidate (older writer candidate, not in the rollback chain) |
| `2beedc0448c3` | `local/trading-system:p184-writer-f1b52f6` | 0 | candidate (older writer used by earlier G13 receipts) |
| `7c848aadcb0d` | `qdl-v2-rust:2.2.3-rc22-a4e7b3b` | 3 | KEEP (cores run it) |
| `b9ca6c36fb0a` | `qdl-v2-python:2.2.3-rc19-4d3476c` | 0 | KEEP (query rollback, harness python image ref) |
| `ce629061a8c1` | `qdl-v2-rust:2.2.3-rc14-a388537` | 0 | KEEP (projector rollback) |
| `7fba25779f72` | `qdl-v2-rust:2.2.3-rc18-cdbf28b` | 0 | KEEP (ingestor rollback) |
| `baa72c96d525` | `local/qdl-stream:isolation-sr1-c934c1e` (`qdl.test.only`) | 0 | KEEP (stream rollback) until the release soaks; FINAL_STEPS i.2 then removes it (test overlay) |
| `b095b18e2094` | `local/qdl-stream:isolation-e3b5552` | 0 | candidate (superseded test overlay) |
| `b012aa18ef1d` | `local/qdl-stream:n1-opt05-ffa382f` | 0 | candidate (superseded test overlay) |
| `381d6bf9c069` | `local/qdl-projector:p184-test-idr107-58fb7cd` | 0 | candidate (TEST projector) |
| `0df95f8888e1` | `local/qdl-core:n1-test-clock-f1-d221fb8` | 0 | candidate (TEST core) |
| `f41ecb66ecb0` | `qdl-v2-rust:2.2.3-rc16-bcb0283` | 0 | KEEP (base of the thin Rust overlay; the recipe pins it) |
| `7d43121e41c5` | `qdl-v2-python:2.2.3-rc16-bcb0283` | 0 | candidate (superseded python, G13 roles v4) |
| `551428d98db6` | `qdl-rust-builder:r134-test` (3.29 GB) | 0 | KEEP until the release is published (build recipe input) |
| `08f356a84151` | `qdl-v2-python:2.2.3-rc14-a388537` | 1 | KEEP (bar edge) |

Procedure per candidate:
- [ ] Re-run `docker ps -a -q --filter ancestor=<full id>` and `docker image inspect -f '{{.RepoTags}}' <id>`; refs must
      be 0.
- [ ] `docker image rm <full sha256 id>` (digest, not tag). If a second tag points at the same id, remove the tags
      first only when the owner agrees; do not use `-f`.
- [ ] Record the id and size removed.

The many `local/trading-system-test:*`, `e09-*`, `owner-*`, `compat-*` and portal images belong to other work streams;
they are out of scope for this release checklist and are not touched here.

## 2. Run worktrees (remove without `--force`)

DL (`git -C /home/bobby/.worktrees/data-layer-p184-closure worktree remove <path>`):
- [ ] `data-layer-p184-final-run-6506d011` (G13 dr05-quiet amendment run)
- [ ] `data-layer-p184-final-run-860b79ba`
- [ ] `data-layer-p184-final-run-b6d21c9e` (734b08f1 signal runs)
- [ ] `data-layer-p184-final-run-d03cd679` (stage-100 attempt, v5.2)
- [ ] `data-layer-p184-final-run-f6db0b82` (G12 final credit and G13 isolated final): remove only after the release
      packet cites the receipts that name it and after the v5.4 window and the final stage-100 are done
- [ ] `data-layer-p184-s100-7285868` (apparatus `aee0945`; the final stage-100 used the d03cd679 worktree because the
      `aee0945` tree lacks `1e09f87`)

TS (`git -C /home/bobby/.worktrees/trading-system-next-upgrade worktree remove <path>`):
- [ ] `trading-system-final-run-68f2572f` (stale pre-reword pin), `-77a6d671`, `-ba096818`, `-1401ccb1`, `-f60e7fe5`,
      `-c385aaf6`
- [ ] `trading-system-final-run-c9ebd619` and `-ca435a18`: likely pins of the v5.4 window and the final stage-100; NOT
      verified. Do not remove until those runs are sealed.

Not release run worktrees (leave alone): `data-layer-mark-index-live-view`, `ts-codex-p184-review` (Codex support
worktree: move the index pointer to the integrated index first, per CLAUDE.md P18.4 rule), the portal and EDS worktrees,
`trading-system-next-upgrade-claude`.

Each removal: `git status --porcelain --ignored` empty before; a refusal means the tree became dirty: stop and report.

## 3. Build cache by record id

- [ ] `docker builder du --verbose` (read-only) saved; identify records created by the release builds only (DL build
      2026-10-16 created 16 records, 15 were already removed by id, 1 shared layer kept:
      `sk2ugtdskwdcui6qbcazla7zt`, backs the release Rust image: KEEP; TS writer builds kept 12 records that back the
      candidate image `a44349a1`: KEEP while the image is in use).
- [ ] Remove by id only: The removal syntax by record id (buildx / builder client) is NOT verified on this host: confirm it with `--help`
      on a dry listing first, and skip the step if there is no by-id form. Never run an unfiltered prune.
- [ ] The CI-scale build cache that took Kafka down on 2026-10-08 (94 GB) is the reason this section stays by id.
- [ ] Record cache size before and after (drafting: 29.54 GB, 455.5 MB reclaimable).

## 4. Build directories and scratchpad (shared disk)

- [ ] `~/.local/state/qdl-v2/build-v2.2.4-c7ce7d3/src` (33 MB) and `ctx` (70 MB): remove only after the hashes are
      recorded in `equivalence.json` and the release notes (receipt, addendum, SDK builds are already hashed; `ctx`
      holds the ctx sha256 listed in the receipt). KEEP `receipt.json`, `postcheck-addendum.*`, `rust.iid`, `python.iid`,
      `sdk-build-1`, `sdk-build-2`.
- [ ] TS release dirs `~/.local/state/trading-system/p184-n1-opt-20261013/release/{e21f6edd,77a6d671}/src`: remove the
      `src` copies only; keep receipts. `6af62a7e` stays until the writer is released (it holds the PASS receipt).
- [ ] Scratchpad dirs under `/tmp/claude-1001/-home-bobby/<session>/scratchpad` (shares the runtime disk): list with
      `du -sh`; remove only scratch files whose names and owners you know.
      **DO NOT REMOVE** `798e80a2-9f96-4c92-b157-75b07ed3460c/scratchpad/b9/dl-4d3476c` and
      `.../rehearsal/dl-4d3476c-san`: they are harness inputs of the writer `--prove` run (memory
      `harness-inputs-not-in-scratchpad-cleanup`, 2026-10-16). Grep the pending proof scripts for these paths before
      touching any scratchpad.
- [ ] Remove run-state evidence for the superseded attempts only after the final receipts cite what they need; keep every
      NOT_SCORED / INTERRUPTED / failed receipt (`T092050Z`, `T040244Z`, `T094028Z`, the 03:20Z stage-100 attempt).

## 5. Containers, networks, volumes

- [ ] `docker ps -a --format '{{.Names}}' | grep -E '^(p184-g12-dl-|p184-dr05-|p184-dr08-|p184-g13-|ts-p184)'` is empty
      after the v5.4 window teardown (at drafting, `ts-p184-*` containers were running for that window: not mine).
- [ ] `docker network ls | grep -E 'ts-p184|p184-dr05'` empty.
- [ ] Volumes: only volumes named by the run's own label and created by it; never a shared volume (AGENTS 12, 15).

## 6. Close-out record

- [ ] `docker system df` and `df -h /` after; exact removed ids, sizes and errors written to the DL Unified Plan and the
      CERTIFICATION_LEDGER cleanup row in the same commit.
- [ ] Release the lock; `ls ~/.local/state/host-measurement-window.lock` reports absent.
- [ ] State "removed N images / M worktrees / K cache records; freed X GB" with the measured before/after.
