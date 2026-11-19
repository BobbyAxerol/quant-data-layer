# DL v2.4.2: retiring delisted instruments through the declaration path (plan)

**Status:** plan, not rendered. It needs a declaration tool slice (below) and a coordinator window.

## 1. What the gate finds today

Command:
```
scripts/catalog_venue_listing_gate.py --catalog <live r16 runtime catalogue>
    --listing OKX:SWAP:<capture 2026-11-19T08:40:58Z> --listing BINANCE:USDM:<exchangeInfo 2026-11-19T06:35:43Z>
```
Result: **FAIL**. 519 instruments checked; 3 are missing from the venue lists.

| Instrument | Status | Bindings |
|---|---|---|
| `OKX.SWAP.PERPETUAL.RVN-USDT` | delisted by OKX between about 07:30Z and 08:40Z | 1 (`okx-swap-rvn-usdt-swap-bar-1d`) |
| `BINANCE.USDM.FUTURE.BTC-USDT-260925` | expired quarterly | none |
| `BINANCE.USDM.FUTURE.ETH-USDT-260925` | expired quarterly | none |

- Not covered by any captured list: OKX FUTURES 4, OKX SPOT 1, BINANCE SPOT 1, and the VN instruments 2. A capture
  for OKX FUTURES and SPOT and for Binance spot can be added to the next declaration.
- Binance `RVNUSDT` is still TRADING, so its binding `binance-usdm-rvnusdt-bar-1d` stays.

## 2. Why the tool needs a retirement slice

`scripts/dl_declaration.py` is additive-only by design:
- the gate reports removed rows as problems (`additive` checks around `:2114-2154`);
- a universe that removes members is refused with "a removal needs its own ...".

So retiring an instrument needs an explicit, narrow path. Proposed:
- **Declaration field** `retire_instruments: [{instrument_id, reason}]`. It is accepted only when the declaration's
  `venue_listings` show that instrument missing or not live. Nothing else may be removed.
- **Render** removes the instrument, its bindings, and its demand, scope, acquisition and routing rows. It bumps
  catalogue, acquisition and demand by one revision.
- **Render also writes** the bar_edge checkpoint migration record:
  - `scripts/dl_declaration.py checkpoint-migration --previous-catalog ... --catalog ... --reason "delisted RVN-USDT-SWAP" --approved-by <owner>`;
  - `removed_binding_ids = [okx-swap-rvn-usdt-swap-bar-1d]`.
- **Gate** passes only if every removal is one of the listed retirements, and the listing check
  (`catalog_instruments_listed_on_venue`) is then green.

## 3. bar_edge checkpoint across the catalogue bump (C.19 / C.32, no strand)

`qdl/runtime/bar_edge_checkpoint.py` `plan_crossing`:
- a binding present before and after with the same series digest keeps its watermark (`retained`);
- a removed binding is refused unless a migration record names exactly that set. With the record, its watermark is
  dropped (`dropped`).

So, with the record:
- crossing 16→17 / 24→25 retains 1319 bindings and drops 1 (RVN);
- `migration_record=True`;
- no strand.

Without the record the edge refuses to start (the C.19/C.32 strand), so the render must ship the record, and the window
must set `QDL_STABLE_BAR_CHECKPOINT_MIGRATION` on bar_edge.

The v2.4.1 short-proof sidecar keys proofs by binding id plus binding digest, so retained bindings keep their proofs.

## 4. Coupling with the catalogue revision (window 1 lesson)

A catalogue bump moves the hot-read authority. The order:
1. Query x2 on the new runtime, with the v2.4.0 fallback.
2. Streams with the new bundle: stream_kn_2 first, then stream_kn_1.
3. The cores, projectors and ingestor need no change: RVN 1d is a bar_edge REST binding, not a live binding.
4. bar_edge last, with the migration record.

**Consumer manifests.**
- TS r15 does not reference RVN, and crossec's 96 instruments do not include RVN.
- But the OKX RVN binding `okx-swap-rvn-usdt-swap-bar-1d` (BAR 1d, ALPHA) is required by three manifests:
  `alpha.okx.paper.stable`, `alpha.okx.live.stable` and `alpha.okx.sandbox.stable`. The requirement comes from the
  top-300 1d universe (bundle `v2.4.0-realm` readback). So the retirement render must also drop that row from those
  three manifests, through the universe membership revision.
- That is a manifest revision bump. Under the v2.4.0 S2 advisory revision, an alpha JWT at the old revision is still
  accepted, so no same-window JWT move is needed.
- The Binance `RVNUSDT` rows stay, because that instrument is still TRADING.
