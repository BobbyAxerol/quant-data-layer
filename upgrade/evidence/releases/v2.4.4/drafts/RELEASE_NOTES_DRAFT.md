# Quant Data Layer v2.4.4 release notes (DRAFT, 2026-11-25)

Status: `implemented`, `tested locally`. Not built, not deployed, no PR. The release waits for the owner's GO.

## Scope

`binance_bar_edge` only (python). It builds on v2.4.3 (`4fd6d65`). The recipe bump to RELEASE 2.4.4 comes with the
release commit.

## DL-BL-2026-11-25-B: every binding whose durable window survived resumes within the bound, after any restart

Observed on 2026-11-25 in the first v2.4.3 start. The restore check marks some bindings incomplete (3 TAO 1h/2h/30m).
It removes their watermark, and they take the deep full-window bootstrap at one per turn. So do all bindings after a
restart across a cache-epoch change ("cache generation changed; bounded bootstrap required").

Fix (`qdl/runtime/stable_bar_edge.py`):
- **Seeding.**
  - `_restore_state` seeds the checkpoint watermark of every restore-incomplete binding as a resume candidate.
  - On a cache-epoch change it seeds every restored watermark.
  - The candidates go to the rollback-safe sidecar as well. If the sidecar holds a candidate too, the larger
    watermark wins.
- **Resume plan** (`_resume_plan`, cache only):
  - It reads back the part of today's demand window that lies at or before the watermark.
  - The provider read starts at the oldest missing open (or at the watermark when nothing is missing) and runs to
    the latest close, within the demand.
  - When the repair would need the whole demand window, the durable window did not survive. The edge logs typed
    `RESUME_DURABLE_WINDOW_INCOMPLETE` and takes the deep path.
- **Proof scope changed.** The proof now covers only today's demand window, not the whole window ending at the old
  watermark. Rows older than the current demand no longer force a deep bootstrap.

Bound: unchanged, and now for every surviving binding after any restart or cache loss: live within `ceil(N/16)`
turns, with one page per binding for outages up to 300 bars.

## DL-BL-2026-11-25-C: a venue hole is fail-closed for its span only

v2.4.3 published only the bars after the newest hole. Every older open of the window had to be durable already;
otherwise the binding was refused with `VENUE_HOLE_NOT_COVERED` and had no live bars. That happened even when the
venue had returned the bars below the hole, for example a cache loss below an old venue hole.

Fix:
- **Adapter.** `BarHistoryProviderHoleError.window_envelopes` carries every provider-confirmed bar the OKX fetch
  returned, on both sides of every hole, each run continuity-checked. An overlap anywhere stays a plain error. The
  Binance pager stops at the hole and passes the suffix only.
- **Edge** (`_venue_hole_decision`).
  - Every venue bar of today's demand window is published, missing opens only; the canonical core validates each.
  - Only the opens the venue lacks inside the returned window are decided from the cache.
  - All durable: `VENUE_HOLE_DURABLY_COVERED`.
  - Otherwise `VENUE_HOLE_NOT_COVERED`, fail-closed for exactly that span (`uncovered_from_ms`..`uncovered_to_ms`):
    - no bar is invented;
    - a derived bucket over the span stays a gap, and a warmup across it is refused as a gap;
    - live bars resume (`binding_resumed: true`).
- **Recheck** (`_recheck_venue_holes`, one span per turn).
  - Cache first, then a bounded venue read from the span to the latest close.
  - The delay doubles from 300 s to 6 h.
  - `qdl_bar_edge_venue_hole_healed` when the span is filled.
- **Heartbeat.** `venue_holes_refused` now counts open uncovered spans; a binding with one still has live bars.

## Operator section: window-script lesson (2026-11-25)

Under `set -o pipefail`, `docker logs … | grep -q PATTERN` reports "no match" exactly when the line is present:
- grep exits at the first match;
- `docker logs` is killed by SIGPIPE (exit 141);
- pipefail makes the whole pipeline non-zero.

The v2.4.3 window script's precheck (sweep ACK) and verify (checkpoint restored) checks had this defect; it was
written by agent F. Use one of:
- capture the logs first: `out=$(docker logs --since "$T0" "$CTR" 2>&1)`, then `grep -q … <<<"$out"`;
- `grep -c`, which reads the whole stream;
- no pipefail for that check (`grep -m1 >/dev/null` is then fine).

## Rollback

`qdl-v2-python@sha256:50124df8…` (v2.4.3), with the `.pre-v244` checkpoint copy. The sidecar format is unchanged.
