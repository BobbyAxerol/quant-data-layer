# DR05 Canonical / Reader / TS Validator Results

Status: IMPLEMENTED_TESTED_ISOLATED; DR05 FAULT_PROPAGATION_FAIL; release HOLD.
Date: 2026-10-08. This is TEST_ONLY fault injection, not provider/live evidence.
Guide: [DR05](p184-dr05-dl-fault.md). Coordination: [Claude handoff](p184-claude-stage3-alignment.md).

## Exact Result

| Venue | Fault | Old snapshot age at status read (ms), n=1 | Reader / TS BOOK validator | Cell |
|---|---|---:|---|---|
| Binance | GAP | 473 | LIVE, eligible / accepted BOOK | FAIL |
| OKX | GAP | 544 | LIVE, eligible / accepted BOOK | FAIL |
| Binance | Unanchored generation | 465 | LIVE, eligible / accepted BOOK | FAIL |
| OKX | Unanchored generation | 542 | LIVE, eligible / accepted BOOK | FAIL |

These are event ages, not request latencies or outage durations. No percentile
or capacity claim. In all four cases core quarantines the exact fault capture,
but the prior healthy BOOK_SNAPSHOT remains readable while fresh. Redis reports
NO_WRITE for its repeated sequence; that is not a data-quality refusal. The
helper now reports book_validated separately from cache-write/equality outcome.

12/12 positive/return/post-fence controls returned snapshot bytes with SHA256
matching the exact capture-correlated canonical BOOK_SNAPSHOT. All reached the
real TS image BOOK validator, production Redis apply and re-read. Four old-
generation raw records were committed/filtered without a canonical publication;
post-fence controls stayed generation8. No synthetic quality is substituted.

Core source `rust/qdl-realtime-core/src/lib.rs` sequence-gap branch requests
resync and returns quarantine. The tested reader hot path does not receive an
invalidation of the previously healthy canonical book. This is the next narrow
repair target; not justification to loosen freshness or classify it as quiet.

## Evidence

Root: `/home/bobby/.local/state/qdl-v2/dr05-observer-20261008/`.
Each row names an immutable `receipt.json` directory and SHA256.

| Directory | SHA256 |
|---|---|
| binance-gap-v2 | 8e4eae0dc523256305a70e4066ba6397d3e18d6a86ddb1e0387db1a836b4b25e |
| okx-gap-v2 | 3703f46edf9bd308620724982a86d09b54e7b74b3a7b50ef0127278f744e53ac |
| binance-generation-v3 | 590291f159d34c89919ff9a864a43987800f99e70025d11426c2c087df294bdb |
| okx-generation-v2 | f89356e55d6e688555c1fe943d97619a0fb4ca3567460e3a74f5b818658fd9c2 |

Earlier attempts are retained. Binance generation-v2 exposed a fixture error:
anchor200 replays buffered delta201, so the next delta must reference201. Fixed
only that venue/fault schedule, added regression, reran only that affected cell.
The older OKX preliminary subcheck incorrectly equated NO_WRITE with refusal;
final verdict requires explicit typed refusal. No earlier receipt was edited.

## Verification And Boundaries

- Parent final selected DL suite: 52 PASS, 0 FAIL, 0 SKIP, 32.861s.
- Agent TS-image suite with disposable Redis: 22 PASS, 0 FAIL, 0 SKIP, 5.992s.
- Agent final marker/cache semantics tests: 3 PASS, 0 FAIL, 0 SKIP, 3.638s.
- Parent independent network-none TS suite: 19 PASS, 3 explicit Redis skips.
- These suites overlap; do not sum them as unique coverage.
- Cursor parser fix47d906e: old source3tests/2errors; corrected source3PASS.

Application images: core658a9570, Stream1f13408f, Query base77682eea
(2.2.3-rc2-43bbcf0 with feature Python source mounted), Kafka9516fb76,
Redisdfa18828. TS application imagea6548ee3/source2abbe39, helper-only mount.
Receipts retain full image digests. No app-source overlay into TS. No new image
build, shared manifest/ACL/config/credential change, provider call or order.

This covers real unary Stream -> Query/hot view and TS BOOK validator/Redis
application, NOT full WriterClone Subscribe/ACK, projector-primary materialization,
or full Risk admission requiring MARK/QUOTE. Claude's newer TS01eb0d7 adapter
still needs protocol alignment described in the linked handoff. Do not declare
all TS DR05 integration complete or emit a PASS certificate from these receipts.

## Cleanup And Runtime

All four selected cases removed their six isolated roles plus the --rm helper;
private files remaining0 in each receipt. No shared data flush/delete or image
prune. Per-case free bytes before/after:

- Binance GAP: 74,227,871,744 -> 74,229,985,280.
- OKX GAP: 74,228,985,856 -> 73,978,429,440.
- Binance generation: 74,422,525,952 -> 74,396,819,456.
- OKX generation: 74,523,852,800 -> 74,509,160,448.

Host activity is concurrent; disk deltas are not attributed solely to tests.
Inventory: 95images/28.96GB, BuildKit20.52GB. No images/cache were built by this
slice; broad pruning deferred to the approved release cleanup, preserving active
and BAR rollbackf7351c3b plus artifacts owned by Claude's active work.

Canonical `/home/bobby/data_layer` main5bf5cd5/v2.2.2 unchanged. Only active DL
feature worktree is data-layer-p184-closure/fix/p184-dl-closure. Read-only final
snapshot of 12 shared application roles plus five infrastructure roles shows
restart0/OOMfalse. This is not a fresh runtime acceptance. Shared core658a9570,
Query/BAR51450612, Stream1f13408f, projectora696cfba, ingestor0f6876e1 unchanged.
No push/merge/release. v2.2.3 remains HOLD.
