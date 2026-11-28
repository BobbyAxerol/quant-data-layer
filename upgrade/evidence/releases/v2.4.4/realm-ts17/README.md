# TS consumer manifest r16 (deployed) and r17 (DL v2.4.4)

- `trading-system.paper.stable-r16.yaml`: byte copy of the deployed file
  `~/.local/state/qdl-v2/releases/v2.4.2-realm-ts16/trading-system.paper.stable-r16.yaml` (Query realm mount).
- `trading-system.paper.stable-r17.yaml`: r16 + OPEN_INTEREST x96 (ALPHA, crypto_liquid_v2, OKX SWAP), from the TS patch
  `V223_DL_TS_MANIFEST_R17_OKX_TRADE_OPEN_INTEREST.patch` with its 91 TRADE additions removed (split to r18).
- `SHA256SUMS`: both hashes. The repository's `consumers/stable/trading-system-paper.yaml` stays at r13: it is part of
  the repository's own consistent config set (catalogue r13, demand), which the test suite checks together.
