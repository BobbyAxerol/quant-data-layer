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

Known limitation (DL-BL-2026-11-25-C, fail-closed): when a venue hole is in the read and the cache does not hold
every older open of the window, the binding stays `VENUE_HOLE_NOT_COVERED` and is rechecked every 300 s. It stays
that way even when the venue itself returned the bars below the hole. A later slice can publish the venue-confirmed
bars on both sides of the hole and require durability only for the venue-missing opens.

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
