# v2.2.3 Preparation: Not Published

Status: `PREPARED_RELEASE_HOLD_NOT_PUBLISHED`. This record is not a certificate,
Git tag, deploy authorization or proof that the feature HEAD runs on production.
The published stable release remains v2.2.2.

## Inherited Runtime Change

Owner-approved bar-edge-only recreate at 2026-10-08 02:17:33 UTC:
`f7351c3b` -> `514506122111df8992a5dfac9dc9a3db7ccae202a398424f4dd5a033d2ddf9a9`
(`qdl-v2-python:2.2.2-d6d2637`). This removes the old endless Binance3d history
retry storm using the already-built implementation. No additional role changed
in this continuation. No claim that Binance3d history gaps have been repaired.
Exact rollback compose remains in the prior Claude private evidence directory.

The three cores remain `658a9570`. Owner moved coherence acceptance to TS's
bounded product read path (IDR-076). Prior paired core trial remains historical
FAIL/rollback; core candidate `0d8da150` is explicitly excluded from this release
proposal. No new core is needed merely to pass the revised coherence gate.

## Busy-Window Evidence

2026-10-08 03:21:03-03:26:03 UTC, 17 DL roles, 31 samples/30 intervals over
299.992s: mean **4.882532**, p95 **5.564052**, max **5.870669 vCPU**. The existing
300s mean<=5 gate passes this observed window; p95/max do not stay below5.
No peak-capacity or every-interval<=5 claim. TS CPU is measured separately.

Simultaneous log-end rates: canonical952.48/raw826.28 offsets/s over302.110s;
both exceed all three prior baselines (canonical +47.7% to70.1%, raw +56.5% to97.2%).
Canonical offsets include transaction markers, not payload-only events. Bar edge
mean0.016960vCPU, no restart/OOM or image/config changes during observation.

**Retained finding:** active Stream overflow+2, replay+15901, subscriptions+3.
Two queue-full closures are TS Paper BTC TRADE, one Binance and one OKX, each
queued1000. A subsequent Binance replay closes OUT_OF_RANGE after10000 replayed
records. This does not establish data loss or an endless loop, but it precludes
claiming fully qualified burst recovery. One TS Binance BTC MARK timeout also
appears in that window. Passive Stream counters remain unchanged.

## IDR-075 Preparation

Pure preparation reuses deployed catalog identities and preserves all60 existing
V2 products, all prior manifest requirements, quotas and fail-closed policies.
Adds only OKX BTC-USD-SWAP QUOTE, MARK_INDEX_PRICE, BOOK_SNAPSHOT and BOOK_DELTA,
with USD quote and BTC settlement identity. No TRADE/BAR acquisition or grants.
BTC-USDT-SWAP MARK_INDEX_PRICE already exists; its INDEX component supplies the
typed FX path, not a new standalone INDEX feed or a USD=USDT assumption.

Target: Paper manifest11/JWT11, bindingV3 release23,64 products. The matching
binding hash and environment projection are in `release-preparation.json`.
Runtime remains manifest10/bindingV2r22/JWT10. Prepared files are outside runtime;
no token, mount, reader, writer or account was changed. No live/sandbox grants.

## Publication Gates

DR01-DR08 and broker delta receipts remain mandatory, independently of Paper
feature completion. Retain the new overflow/replay finding for affected recovery
qualification. IDR-075 needs one approved paired reader/caller packet and actual
Paper-identity no-order reads before activation is claimed. Final release must
reconcile source, immutable images, schemas/configs, CI and owner-authorized
publication; never relabel this prepared inventory as a completed release.

Evidence: `~/.local/state/qdl-v2/p184-codex-release-20261008/{cpu,idr075}/`.
The main-plan entry and [Phase C packet](../../p184-dl-phase-c-packet.md) govern
the next action. No other role may change without its owner-approved packet.
