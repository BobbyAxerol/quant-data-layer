# DR05 Public Query Router Handoff

Status: IMPLEMENTED / ISOLATED_HTTP_TESTED, not DR05 certified.
Scope: owner request 2026-10-09, public router in the disposable Query harness.
No shared role, credential, quota, manifest or application source changed.

## Change

`scripts/p184_dr05_observer.py::public_query_app` uses production
`create_v2_app` with packet JWT keys pinned to subject/realm, both isolated
manifests and RedisMinuteQuota. HTTP and Rust Stream share the exact namespace
quota key prefix and Redis DB0. Query remains mTLS-only, loopback8200, with
bounded16 concurrent requests. Internal read-view remains installed, unchanged.
`scripts/p184_dr08_isolated.py::serve_query` accepts an optional factory; DR08
without it retains its original app. No duplicated endpoint or permissive auth.

## Tests And Actual HTTP

Existing image77682eea (rc2-43bbcf0), read-only source, no build:
`python -B -m unittest tests.test_p184_dr05_public_query
 tests.test_p184_capacity100 tests.test_p184_dr05_dl_fault
 tests.test_p184_dr05_observer`: **62 PASS, 0 fail/error/skip**,80.643s.
Includes the quota arithmetic regression committed in the next slice.
Public tests check route presence, valid signed instrument reads, missing token,
wrong realm/revision/consumer, ungranted feed, quota dependency failure and
non-test packet rejection. Earlier fixture failures (wrong InstrumentRecord
attribute; URL placeholder identity vs instrument_uid) were fixed, not hidden.

Fresh namespace `p184-dr05-dl-a8d47c9c951a4e11`: existing Kafka/core/Stream/Query/
Redis images; core658a9570, Query77682eea plus source-mounted harness.
`--init`, `--up`, then SDK RestQueryTransport over realHTTPS+mTLS:
- instrument/snapshot/warmup/feed_status x Binance/OKX: **8/8 success**;
- wrong realm/revision x both venues: **4/4 HTTP401**.
Synthetic provider frames carry TEST_ONLY; core emits canonical, Stream hot view
serves it to Query. This is NOT a real-provider or TS writer/validator receipt.
First probe incorrectly repeated Binance snapshots with discontinuous deltas:
core rejected gaps as designed (3 read failures). Corrected the probe to one
anchor and contiguous deltas in a fresh generation; kept failed receipt/script.
No reader correctness or freshness gate was relaxed.

External evidence: `~/.local/state/qdl-v2/dr05-public-router-20261009/`:
- `work/public-http-receipt.json`: SHA256
  `84cbb14608b471c559126eb0383dd65f9f5b14acf545d1636df3f88ab9f76126`.
- `probe.py`: `129444eec51327f13c0e56d1decc424e916639b5afc1b8ad307aa9401f0d82fd`.
- `receipt-attempt1.json`: `8c1ec9f197ca1f4e3cb7b5302a0012ed97ae929649c87c2021d9670f35af1990`.
- `probe-attempt1.py`: `f9226c8b2105afb9b19e4eb3a2bb13c13a64ded9bedd073a8b1512ccd93c0e15`.

## Claude Next Step

Use unchanged DR05 init/up/reset/channel protocol. Each newly started Query now
exposes SDK public routes. Keep the previously required GAP/generation candidate
core binary for final fault cases: this HTTP check used the base core and does
NOT replace that patch. TS writer-clone/validator final rows remain Claude-owned;
`drive()` still cannot issue acceptance. Use fresh material because private test
keys of this run have been erased. Ports6391/5433 remain reserved for TS clones.

## Cleanup And Runtime

`--down` removed exact query/stream/core/quota/kafka; private keys erased; exact
label inventory empty. Unit container --rm removed. No image built, no volume
created. Existing production/rollback images retained; unrelated BuildKit cache
not pruned while Claude works. Inventory89images/30.82GB, cache23.3GB; disk after
111828414464bytes free (no measured pre/post reclaimed-byte claim).
Canonical `/home/bobby/data_layer` main5bf5cd5, stablev2.2.2; active feature
`fix/p184-dl-closure`, v2.2.3HOLD. Shared runtime untouched. No push/merge.
