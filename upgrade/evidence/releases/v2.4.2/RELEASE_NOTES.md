## Quant Data Layer v2.4.2

CI6: full run RUN_ID on tree TREE.

Runtime windows (r10, 2026-11-19): W1 stream_kn_2, W2 query_kn_2, W3 query_kn_1, W4 stream_kn_1, W5 market_projector_1/2 and
W6 binance_bar_edge all GATES PASS. W6 had a 468 s silent restore; its crossing was 16->17 / 24->25, retained 1319, dropped 1.
Bootstrap end: 1319/1319 at 17:11:07Z, pending 0, D6b 0 outstanding. W7 ingestors (rust `e628fbe0`): PASS, subscriptions_rejected 0 on both. Certificate: `CERTIFICATE.md`.

### Images
- query_kn x2 and binance_bar_edge: `qdl-v2-python@sha256:5ea7a7a40ef2d1df920d0f7cf94f22431d53087f8002929e7dffdb4208f7dcc4`
- ingestors (W7): `qdl-v2-rust@sha256:e628fbe07afe94e8f716692633e77217a16223ea03abc7ea34edb959a48c72a7`
- stream_kn, projectors and cores: image unchanged; streams and projectors take the r10 bundles.
- trivy: 0 fixable CRITICAL/HIGH on both images.

### Highlights
- **Delisted instruments are a per-binding typed failure.**
  - Only documented codes are quarantined:
    - OKX 51001, 51021, 51022 and 51027 (cited from the OKX V5 docs). The transient 51028, 51029 and 51030 keep the
      retry path.
    - Binance -1121, -1122 and -4141.
  - The bar edge quarantines the binding for 6 h, logs `qdl_bar_edge_instrument_unavailable`, adds the heartbeat
    counter `instrument_unavailable`, and continues the cycle.
  - The failing-cycle backoff is now `max()`, 2 to 30 s.
- **Live path.** An OKX `60018` refusal of one subscribe argument (a public-endpoint capture is kept as a fixture)
  drops only that argument. The ingestor logs `qdl_native_subscription_rejected`, and the progress line gains
  `subscriptions_rejected`. Everything else still fails closed.
- **Declaration path.**
  - A venue-listing gate: a declaration from r10 must carry `venue_listings`.
  - `retire_instruments`, the one narrow non-additive path. The render and the gate share one retired base, so
    additivity proves nothing else left.
  - Deployed realm manifests are retired too, with the advisory floor 1.
- **Query.** A manifest-allowed requirement with no catalogue binding is a typed per-item refusal:
  `INSTRUMENT_NOT_FOUND`, or `UNSUPPORTED_FEED` when the instrument is present but the product is not. It used to be
  an untyped 500, and a snapshot batch holding it failed as a whole.
- **Declaration r10.**
  - Retires OKX `RVN-USDT-SWAP` (delisted 2026-11-19) and the expired Binance `BTCUSDT_260925` and `ETHUSDT_260925`.
  - Catalogue 16 to 17, acquisition 24 to 25.
  - bar_edge checkpoint migration: retained 1319, dropped 1.
  - Manifests: alpha.okx.paper.stable r18, alpha.okx.live.stable r2, alpha.okx.sandbox.stable r2.
  - Offline gate: PASS 21/21.

### Consumer-visible
- The OKX RVN BAR 1d row leaves alpha.okx paper, live and sandbox. Existing JWTs stay valid.
- RVN requests get a typed refusal. Binance `RVNUSDT` is unchanged.
- These `execution_alpha` symbol lists name RVN; none of them is running: scalping_PSAR, scalping_sl_tp, taalpha,
  grid_long_only, vwma00315, fib_sl_tp_strength, regressionportfolioA001, rsiboundportfolioA001, signal_combine,
  deep_momentum.

### Interfaces
Additive only:
- refusal codes, both existing documented members of `CanonicalErrorCode`;
- the heartbeat counter `instrument_unavailable`;
- the ingestor progress fields `subscriptions_rejected` and `subscriptions_rejected_args`;
- the declaration fields `venue_listings` and `retire_instruments`.

### Owner note
The execution_alpha symbol lists above get a typed refusal for OKX RVN once r10 is live. Each strategy's owner should
drop OKX RVN-USDT-SWAP or map it to Binance RVNUSDT before re-hosting it.

### Notes
- The cores keep one inert RVN row each until their next restart. The rendered r10 core files remove it.
