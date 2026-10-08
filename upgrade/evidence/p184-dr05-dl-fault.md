# DR05 Isolated DL Stack Handoff

Status: IMPLEMENTED_TESTED_ISOLATED / TS_FINAL_ACCEPTANCE_PENDING, 2026-10-08.
Current packet/API and fence results: [DR05 fence/channel handoff](p184-dr05-fence-channel-handoff.md).
Earlier receipts and implementation checkpoints below remain historical.
Governing scope: Unified Plan, "Pre-stage3 consolidated build, SDK and DR05";
[Phase C packet](p184-dl-phase-c-packet.md). No shared runtime changes.

## Implemented

`scripts/p184_dr05_dl_fault.py` prepares a new external directory, sanitizes an
explicitly supplied core binding export and creates fresh short-lived TLS/JWT,
catalog, manifests and Stream bundle. It never discovers shared credentials.
Only ETHUSDT Binance USDM and ETH-USDT-SWAP OKX BOOK snapshot/delta are admitted.
Public binding identity is retained; Kafka topics, groups, Redis and credentials
are isolated. Synthetic provider-shaped fault frames carry TEST_ONLY provenance.

Five bounded roles use existing immutable images from `p184_dr08_isolated.IMAGES`:
Kafka, Redis quota, Rust core, Rust Stream and the real KN Query read-view service.
Query is **2.2.3-rc2-43bbcf0**, digest
`sha256:77682eea5054ae9ddbd858323b083286669a55eeb4bc7bc065b04bae8854e32f`,
not production v2.2.2. Core/Stream are the existing images recorded by that
inventory, not a consolidated v2.2.3 build. Query source is mounted read-only
from this checkout; this tests a harness stack, not release-image packaging.

Kafka uses network none; the other roles share only its network namespace.
There are no published ports, shared networks, shared state or broker credentials.
All containers use --rm, immutable image IDs, bounded CPU/RAM/PIDs/logs and a
read-only root filesystem. Readiness verifies identity/isolation separately from
Kafka mTLS, Query TLS and gRPC connectivity. Neither check certifies market data.

Cleanup prechecks exact names, ownership labels and AutoRemove for all roles,
including bootstrap helpers, then stops them in reverse order. It waits boundedly
for asynchronous Docker removal and handles disappearance between list/inspect.
No broad prune, image removal, shared volume or network deletion occurs.

## Commands For The Scheduled Window

Use a NEW directory outside the repository and an explicitly sanitized exported
core JSON with both BOOK bindings. Do not copy secrets or use shared bootstrap.

```sh
python3 -B scripts/p184_dr05_dl_fault.py --init "$NEW_WORK" --core-config "$PUBLIC_EXPORT"
python3 -B scripts/p184_dr05_dl_fault.py --up "$NEW_WORK"
python3 -B scripts/p184_dr05_dl_fault.py --ready "$NEW_WORK"
# Run the separately integrated real fault publisher / TS observer here.
# Always execute down in the outer harness finally/trap, including on failure:
python3 -B scripts/p184_dr05_dl_fault.py --down "$NEW_WORK"
```

The directory must not already exist. TLS lifetime is two hours; regenerate a
fresh packet for a later window. Down removes private files and invalidates the
material marker. Retain bounded receipts, not keys, before removing test files.

## Remaining Runtime Integration

The four prepared cells are GAP_REFUSAL and GENERATION_CHANGE_FENCED for each
venue. Schedules cover positive anchor/control, missing sequence or unanchored
generation, new anchor/recovery, late old-generation snapshot and post-fence
control. Timestamps are stamped at publication, never reused from preparation.

`drive()` defines the adapter interface. The bounded Kafka publisher is now
implemented in `scripts/p184_dr05_raw_publisher.py` and has a 16/16 exact-byte/
offset readback on an isolated broker. See [alignment handoff](p184-claude-stage3-alignment.md).
The canonical/Query/Stream observer and pinned-image TS BOOK validator are now
implemented in `p184_dr05_observer.py`, `p184_dr05_observe_stack.py` and
`p184_dr05_ts_validate.py`. This is a diagnostic isolated path, NOT Claude's full
WriterClone subscription/ACK acceptance. One fresh disposable stack per cell;
no phase-derived quality or rewritten clocks. Actual results below keep the
upstream gap/generation refusal gate open.

Pending rows have ok=false and no verifier provenance header. Even an adapter
returning all-success cannot make drive emit PASS. Claude must supply reviewed
observations and the final verifier-format evidence under the confirmed TS
SHA/image. Stack startup does not close DR05. DR01 deployed and DR08 final remain
scheduled separately for stage3; no such probes ran in this slice.

## Tests And Recorded Smoke

Parent verification in existing image77682eea, --network none, 0.5CPU/512MiB:
`python -B -m unittest tests.test_p184_dr05_dl_fault -q`:
**26 PASS, 0 FAIL, 0 SKIP, 68.539 seconds**. Includes real offline bundle and
manifest compilation, independent keys, private modes, namespace/network/image
checks, fault schedules, false-PASS rejection, timeout cleanup and removal races.

Agent startup/connectivity smoke used the source catalog-derived
`MaterialTests.exported()` fixture, init -> up -> cleanup -> erase_private in
TemporaryDirectory. Successful rerun exit0, namespace
`p184-dr05-dl-fa9ce97d336d41a2`, elapsed55.13s, all5roles process-ready and
services-reachable; functional_ready=false and acceptance=false. Removed
query/stream/core/quota/kafka, zero private files remaining, temporary directory
removed. Initial cleanup race was fixed and regression-tested before this rerun.

These smoke numbers are transcribed from the agent's tool-session output, **not
an archived DR acceptance receipt**. No provider/public market capture or TS
fault validation was run. Parent independently confirmed scoped containers gone.
Free disk before/after smoke: 79,907,880,960 / 79,716,106,240 bytes; concurrent host
activity prevents attributing that difference to this test. No new images built.

## Release And Cleanup Boundary

Canonical /home/bobby/data_layer main5bf5cd5, published v2.2.2 remains unchanged.
Active worktree /home/bobby/.worktrees/data-layer-p184-closure,
fix/p184-dl-closure. Shared service images/config/manifest untouched. Retain active
images and explicitly protected BAR rollback f7351c3b. No push/merge/public release.
v2.2.3 HOLD; rebuild only after TS test completion and exact-source DL CI.


## Observer And In-Image TS Validation (2026-10-08)

Run from the feature checkout with existing images (no build):

```bash
python3 scripts/p184_dr05_observe_stack.py \
  --output "$EVIDENCE/new-case-directory" --exported "$PUBLIC_EXPORT" \
  --venue OKX --fault GENERATION_CHANGE_FENCED
```

`PUBLIC_EXPORT` is sanitized test material, never a credentials export. All raw
frames are explicit TEST_ONLY. Receipt output is immutable per new directory.
The observer follows raw capture ID -> core committed offset -> read_committed
canonical/quarantine; negative absence needs EOF fetched after the core commit.
Real Stream unary GetFeedStatus/GetSnapshot traverse Query and native hot-read
views with temporary mTLS/JWT EXECUTION identities. Query's public serializer
captures real handoff context; the harness never manufactures authority/quality.

A helper-only mount runs in the pinned TS image a6548ee3; application bytes come
from that image. Live mode uses actual wall clock, production SDK/adapter/BOOK
validator and MarketCacheProjector, isolated Redis DB2, then a cache re-read.
New generation uses production adoption, not deletion of the previous key.
`book_validated` is separate from Redis write/cache equality: NO_WRITE is NOT a
refusal. The offline replay is labelled separately and cannot replace live proof.

Limits: no full Risk admission (MARK/QUOTE absent), no persistent Subscribe/ACK
writer loop, no provider/mainnet evidence, no production modification. Query
Python uses feature source mounted over the existing image; core and Stream are
existing immutable binaries. These observations cannot certify a future image.

### Finding Which Keeps DR05 Open

A gap/unanchored generation can be quarantined correctly by core while the old
BOOK_SNAPSHOT remains LIVE/eligible and passes the TS BOOK validator until its
freshness deadline. Recovery with a new anchor works; old-generation input is
filtered. This is a fault propagation problem, not proof that TS should loosen
freshness. The current sequence-gap/resync branch in
`rust/qdl-realtime-core/src/lib.rs` returns quarantine without invalidating the
reader's previously healthy canonical book. Do not call DATA_STALE after waiting
proof of immediate typed GAP/generation refusal.

The candidate also exposed a source-coordinate parser defect for '/' in genuine
Kafka standard-base64 topic UUID. Commit47d906e fixes the intermediate parser,
not the signed cursor schema. No image or shared runtime was changed.

Next narrow implementation must propagate authoritative book invalidation and
recovery through existing canonical quality/read paths, fence by identity and
generation, and close only on a valid replacement anchor. Both primary and hot
backup must see the fence; no timestamp rewrite, Redis flush, timeout relaxation,
provider substitution or extra service. The observer regression is the fixed
counterexample for that repair. Core build/rollout remains under the approved
single-revision release window, not silently included in this observer slice.
