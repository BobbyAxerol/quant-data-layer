# Quant Data Layer v2.4.3 release notes (DRAFT, 2026-11-25)

Status: **deployed on the KN stack (bar_edge only) and accepted at runtime**. Not yet on `dev`/`main`, no tag, no CI6
receipt. These notes are a draft for the release PR.

## Scope

One role changes: `binance_bar_edge`. It runs `qdl-v2-python:2.4.3-eb9371d`
(`sha256:50124df8a7914f4494f898926bffa9487df90841205d051ce915564538716651`), built from `eb9371d` by
`scripts/build_qdl_release_images.py --component python`.

Every other role keeps its v2.4.2 image:
- Query x2: python `5ea7a7a4…`;
- Stream, core, projectors and ingestors: rust `2.4.2-48137eb`, and `abafab2c`/`58bdd282` where v2.4.2 kept them;
- market_cache and Kafka are untouched.

There is no catalogue, acquisition, checkpoint-schema, consumer-manifest or contract change.

## Fix: DL-DEF-2026-11-25-A (plan journal, DL v2.4.2 slice 3)

Trigger: a market-cache loss (host OOM 2026-11-25 05:54:48Z) made the bar edge rebase all 1,319 BAR bindings and
re-bootstrap them one per turn with full 10,000-row windows. Live derived 1h/12h bars stopped for up to about 4 h.
OKX ASTER-USDT-SWAP never resumed, because its window holds a venue hole.

1. **Resume from the previous watermark.**
   - After a product-generation or cache-epoch rebase, the edge proves from the cache alone that the demand window
     up to the old watermark survived. It then reads only the suffix, normally one provider page.
   - Rebased bindings go first, up to 16 per loop turn.
   - Resume points survive a restart in `<checkpoint>.resume.json`. The checkpoint field set is unchanged, so
     rollback to v2.4.2 is safe.
2. **Typed venue history holes** (OKX and Binance), decided from durable coverage:
   - `VENUE_HOLE_DURABLY_COVERED`: the cache holds the bars the venue lacks. The binding resumes and the hole is
     recorded.
   - `VENUE_HOLE_NOT_COVERED`: fail-closed, with a 300 s recheck. Other bindings are not blocked.
   - An OKX hole wider than 2 bars is no longer an untyped "history is short" RuntimeError that fails the cycle.
3. **Observability.**
   - Heartbeat: `rebootstrap_waiting`, `rebootstrap_wait_s`, `resume_pending`, `venue_holes_recorded`,
     `venue_holes_refused`.
   - Events: `qdl_bar_edge_rebootstrap_required`, `qdl_bar_edge_resume_refused`, `qdl_bar_edge_venue_hole`.

## Runtime acceptance, 2026-11-25 (owner-approved window)

| Time (UTC) | Event |
|---|---|
| 10:03 | Backup `.pre-v243`: checkpoint `30b74165…`, history-short `24f5dc5e…` (equal to live at backup) |
| 10:04:14 | T0. `compose up -d --no-deps --pull never binance_bar_edge` with the v2.4.2 layering plus the `QDL_IMAGE_BAR_EDGE` override (config-hash `e7a5486f…`) |
| 10:04:32 | Container started (RestartCount 0 since) |
| 10:12:37 | `stable BAR checkpoint restored bindings=1222 complete=False`. The silent restore took 8 min 05 s, within the runbook's 5-9 min. 97 bindings were without a watermark: 94 from the v2.4.2 process's interrupted rebase sweep, plus 3 TAO gaps |
| 10:13:43 | ASTER-USDT-SWAP 1m `VENUE_HOLE_DURABLY_COVERED`. The OKX hole is 2026-11-19 15:38→15:40Z, 1 venue-missing bar, durable_missing 0 |
| 10:14:11 / 10:15:11 | TAO-USDT-SWAP 1m and 5m: the same OKX hole, durably covered |
| 10:30:41 | XPL-USDT-SWAP 1m: the same OKX hole, durably covered |
| 10:40:09 | **All BAR series current**: OKX 1m/1h/12h 94/94, Binance 1m/1h/12h 96/96 |
| 10:41:08 | `rebootstrap_waiting=0`, `published=1319/1319`, `venue_holes_recorded=4`, `venue_holes_refused=0` |

Checks since T0:
- Error lines: only the 5 known Binance 3d `OVERLAPPING_PROVIDER_WINDOWS` refusals (daily recheck; unchanged class).
- Traceback lines: 0. `qdl_bar_edge_resume_refused` events: 0.
- bar_edge: RestartCount 0, healthy.
- Projectors: no errors, not rebuilding, conflicts unchanged (33/25).
- Query x2: healthy, RestartCount 0.
- Query refusals: the `DATA_NOT_READY HOT_QUIET_PRIMARY_UNVERIFIED` rate is at its 08:00-10:00Z baseline, and no BAR
  `DATA_STALE EVENT_AGE` since T0.
- The one `LIVE_VIEW_UNAVAILABLE SESSION_STATE` burst, at 10:27:28Z, is an OKX public WS reset on `okx-public-005`
  (ingestor log). That class occurs a few times a day (2026-11-24 08/09/15Z) and is unrelated to bar_edge.
- TAO after its resume: 1m, 5m, 15m, 30m, 1h and 2h all at their latest closes.

### Recovery time of this deployment, and the bound

This start had no resume points: the v2.4.2 process never writes the sidecar, and a plain restart is not a rebase.
So the 97 bindings missing from the checkpoint took the deep path, one per turn. That was 28.5 min after the restore
(about 3.4 bindings/min). This is the expected one-time cost of the first v2.4.3 start.

The documented bound applies to a rebase inside a v2.4.3 process, and after it across restarts through the sidecar:
- live within `ceil(N/16)` turns;
- one page per binding for outages up to 300 bars.

A restart now would restore all 1,319 bindings from the checkpoint (every binding has a watermark), and the cache
check passes, including the four covered venue holes. Only the 5-9 min silent restore remains.

Follow-up (DL-BL-2026-11-25-B). Bindings that the restore check marks incomplete (here 3 TAO 1h/2h/30m) still take
the deep path. Seeding their checkpoint watermark as a resume candidate would bound them too.

## Rollback

- Stop the edge.
- Restore `.pre-v243` as uid 10001 (`alpine:3.20 --rm`) and remove `crypto-bar-checkpoint.json.resume.json`.
- Run `compose up` with the v2.4.2 layering, image `5ea7a7a4…`, config-hash `2f753d62…`.
- Script: `window.sh rollback` (coordinator scratchpad `dl-v243-window/`). Not needed.

## Prevention of the cache loss (owner decision; plan journal)

Options:
- `oom_score_adj` for critical DL roles;
- memory protection for a DL slice;
- an RDB snapshot or a parallel stage-B rebuild for a faster cache restore.

## Release follow-ups

- PR to `dev` (python-only scope; CI2 / CI11 inheritance).
- CI6 full receipt on the release tree.
- Merge locally (G7), then tag `v2.4.3` on `main` after owner GO.
- TS INTEGRATION_DEFECT_REGISTER row (plan journal).
