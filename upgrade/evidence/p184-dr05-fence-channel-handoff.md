# DR05 Fence And JSONL Handoff

Status: IMPLEMENTED_TESTED_ISOLATED; TS_FINAL_ACCEPTANCE_PENDING.
2026-10-08. Governing journal: Unified Plan, "DR05 upstream invalidation and
TS packet integration". This supersedes the open plumbing/fence implementation
items in the earlier observer handoff, not its retained failing receipts.

## Domain Fix

Core previously quarantined a broken book but left its last readable canonical
view in place. Readers could therefore approve that old view while still fresh.
The core now emits one non-readable snapshot/delta fence per invalid owner /
generation scope. It uses existing sequence-gap/resync/reset flags, no prices,
no fabricated native sequence, and the actual raw capture identity. Its timestamp
dates invalidation, not a refresh of an old price. A valid anchor restores the
existing venue-specific state machine. Superseded sessions/generations cannot
invalidate the current owner; repeated unanchored frames do not amplify fences.

Projector StageA retains the fence bytes and separate product keys. Query's
primary validator recognizes the non-readable fence after catalog identity and
provenance checks. The hot-read path preserves the existing Rust Stream typed
refusal; it must not retry another replica or return an older healthy primary
to hide that refusal. No protobuf, public endpoint, policy or freshness changes.

## TS Interface

`--init` now emits `ts-consumer.binance.json` and `ts-consumer.okx.json`:
schema `p184.dr05.ts-consumer.v1`, venue, consumer ID/subject, issuer/audience,
environment/manifest revision, JWT key ID, and path-only credential references.
The five required `*_file` paths reside under `/work/`. No secret is inline.
Each binding is V3 rendered from that venue's isolated manifest, with request
policies and a TEST_ONLY manifest digest, not a production release certificate.
Query `https://localhost:8200`; Stream `localhost:8210`. TS ports6391/5433 free.

The JSONL channel runs in existing DL Python image77682eea (Query rc2-43bbcf0),
inside `container:<namespace>-kafka`, with the attestation environment variable.
Requests are exactly `{op,args,timeout}`; replies `{ok:true,result}` or
`{ok:false,error}`. Invalid/oversized input, ambiguous publish or timeout fails
closed. It never echoes credentials or raw input on errors.

- `publish {frame}`: actual RawPublisher ACK and coordinates; one outstanding
  publication until observation, no retry on unknown delivery.
- `canonical_observe {receipt}`: exact receipt issued by this channel, matched
  capture ID, core committed offset plus read_committed EOF barrier. Returns
  actual canonical/quarantine topic, partition, offset, payload hashes and reason.
  Filtered frames have an empty record list with a proven commit/EOF barrier.
- `reset_case {venue,fault}`: bounded host bridge resets only the owned core,
  Stream, Query, quota and three TEST_ONLY topics / two consumer groups. Broker
  and channel network survive. New canonical topic UUID, generation starts at7.
  An attached TS clone or foreign container makes reset refuse before mutation.

No Docker socket is mounted in any container. Start the host watcher before
Claude's adapter starts the channel; it watches only its private work directory:

```bash
python3 -B scripts/p184_dr05_reset.py --serve-reset-host "$WORK" --duration 1800
```

Generate the channel argv through the existing helper to preserve limits/mounts:

```python
argv = dr.helper_command(ns, work, ["--channel", "/work"],
                         network="container:" + ns + "-kafka")
argv.insert(2, "-i")
argv[argv.index("scripts/p184_dr05_dl_fault.py")] = "scripts/p184_dr05_channel.py"
pos = argv.index(dr.IMAGES["python"])
argv[pos:pos] = ["-e", "P184_DR05_ISOLATED_NAMESPACE=" + ns]
# shlex.join(argv) is Claude's --dl-channel argument; stdin/stdout are JSONL.
```

Use `dr.up(work, core_binary=<explicit candidate path>)` for the isolated patched
core. The optional binary is mounted read-only and pinned by SHA256; reset checks
the same pin. This is source/binary-overlay evidence, not image packaging.
Stop TS clones and channel, terminate the watcher, then `--down` in finally.
The host bridge allows at most16 resets, each bounded by the request deadline.

## Verification

All fixtures are TEST_ONLY, not exchange or production evidence.

- Python:120 PASS,0fail,0skip,43.876s across dr05_dl_fault, case, channel,
  observer, raw_publisher, verdict, kn_hot_view and kn_hot_book_invalidation.
- Reset controller:4 PASS,0fail,0skip,0.006s. Foreign/missing stack prechecks,
  exact owned stop set, unchanged broker and invalid deadline rejection.
- Rust realtime-core:53 PASS,0fail,1ignored (final source,0.11s). The ignored pre-existing test
  requires separate Redis provider admission; not represented as passed.
- Rust projector product tests:8 PASS,0fail,0ignored (23 unrelated filtered).
- Four isolated before/after cells: baseline FAIL is retained in the previous
  observer report; patched core gives4/4 diagnostic PASS on Binance/OKX x
  GAP/unanchored generation.12 positive/recovery/post-fence controls match exact
  canonical bytes and actual TS in-image SDK/BOOK validator/Redis application.
  Refusal is immediate typed OPEN_SEQUENCE_GAP, not eventual stale or NO_WRITE.
- JSONL smoke:16 actual publications/committed observations;2 resets preserve
  broker ID, replace topic UUID and restart raw offset0. Reset durations:
  Binance21670.885ms, OKX19807.860ms (n=2; no p99 claim). Both under30s.
  This budget is DL-only; Claude's fresh_case also starts its TS clone and must
  enforce its overall deadline rather than silently extending it.

Evidence root outside Git: `~/.local/state/qdl-v2/dr05-fence-20261008/`.

| Receipt | SHA256 |
| --- | --- |
| binance-gap-candidate-r1/receipt.json | 374495d08fa6d69a079a8f3ec6248929776dce0a3abceb8391d40d5fc900a3c8 |
| binance-generation-candidate-r1/receipt.json | be8f7417528ab7318f8624623778cd44067ff343bdada2ce9ab96637ee1ed41a |
| okx-gap-candidate-r1/receipt.json | 7fc9265e8ff76ff7c240dfe9301a5015ac7b007ab4a4137c11b83c17fadd4629 |
| okx-generation-candidate-r1/receipt.json | 6fc44d71487c937fb347daf868783ef9a69a6f25927c8945ec5d459bba185a4e |
| channel-r1/receipt.json | 2e6539b18805b393e345b8d3712389e1fd29ab93181a54397d2e234ea07f6152 |

Those runs pin core binary0ca2dad272dbf1948a907c45df23e16fcd9a3e4651ff1466d0d9d4740bf60f90.
Final source removes redundant borrows from the extracted Rust inner function;
recompile succeeds (28.60s) as e76d38173fe9d5dc456df0cda8c889275b4dd8faaff4c5438b3e0f8ad7b11c8e.
Do not rewrite prior binary hashes or label prior receipts as final-image tests.

## Still Required From Joint TS Acceptance

Claude owns WriterClone subscribe/control/Redis/production validator and final
TS evidence header. `drive()` continues ok=false. The diagnostic helper is NOT
full WriterClone ACK acceptance. In particular verify old cached books are
invalidated on the live TS control path, not only that a new unary read refuses.
Binance bootstrap snapshot alone is not a ready book until its continuity delta.
Require GAP/generation refusal while previous data remains fresh; generic
DATA_STALE after a wait is insufficient. Keep the TS SHA/image pair honest.

Primary projector coverage here is the StageA product test plus Query primary
validation tests, not a new full projector/cache runtime acceptance. No shared
rollout is approved by these isolated receipts. Final consolidated build/CI,
DR01-DR08/broker delta and approved packet remain required; v2.2.3 HOLD.

## Runtime And Cleanup

Five isolated case stacks (four observers and JSONL smoke) removed; private
files remaining0 per receipt. No images built; reused existing immutable images.
Smoke free disk66834546688 ->66607972352 bytes; concurrent host changes mean
this is an observation, not attributed test disk consumption. No broad prune.
Canonical `/home/bobby/data_layer` main5bf5cd5 / publishedv2.2.2 unchanged.
Feature `/home/bobby/.worktrees/data-layer-p184-closure` fix/p184-dl-closure.
No TS edits, push, merge, shared runtime/config/manifest changes. Retain active
images and explicit BAR rollbackf7351c3b. Final local build-cache cleanup and
retained handoff binary inventory are appended in the journal.


### Pre-Commit Self-Review

A same-generation recovery snapshot can legitimately repeat its prior native
sequence. Regression failed on the first fence patch (no recovery publication).
The fix distinguishes the new real recovery anchor by immutable raw capture ID;
native sequence, generation, values and timestamps remain unchanged. A repeated
copy is still deduplicated. The fence latch only clears after publication.
The alternate production-core runner must preserve mixed fence/quarantine
outputs atomically rather than rejecting or dropping one; its result is recorded
in the final journal before commit. These are narrow source changes, not a
shared rollout or a new certification campaign.


## Final Handoff Freeze

Alternate runner regression:3/3 tests fail on old behavior; final suite7PASS,
0fail/ignored using cargo test --release --offline --locked -p qdl-kafka
--bin qdl-production-core -- --test-threads=1. It accepts only the paired empty
unverified BOOK fence as a mixed outcome and preserves primary quarantine in
the same transaction/watermark. Other ambiguous outputs still fail closed.

Final core binary after recovery fix:
`~/.local/state/qdl-v2/dr05-fence-20261008/bin/qdl-realtime-core-baafcb48`
SHA256 baafcb489bb17fabdd73bef383dcf3448581cc8bb63578e9df83e2a57107b6f2.
Affected OKX generation rerun on this binary: diagnostic PASS, receipt
`okx-generation-final-r2/receipt.json`, SHA256
fcf4b6593732cde3fc6897e750346dea2ed9d3744d80ce29add82aa1a824000a.
Earlier four receipts and channel smoke retain their earlier binary identity;
do not claim every run used this final binary or that TS WriterClone passed.

All six isolated case stacks and helpers removed; privatefiles0, no test
containers remain. Scoped Cargo target removed1225121125 file bytes, retaining
only the named handoff executable. Free disk59893260288 ->60988051456 bytes;
concurrent host activity prevents exact attribution. Cleanup initially met
root-owned copied build files; a network-none helper removed only that target
mount. No image build, image deletion, volume prune or shared cache prune.
Receipt `cleanup-final.json` records scope and retained hash.

Read-only shared snapshot `shared-runtime-readonly.json`:12app+5infra roles,
restart0/OOMfalse. Core658a9570, ingestors0f6876e1, Query/BAR51450612,
projectora696cfba, Stream1f13408f, Kafka9516fb76, Redisdfa18828 (full digests in JSON). These were observed, not modified or recertified.
Source/runtime remain deliberately different until the approved consolidated
release packet. No newly created Docker image needs deletion. Shared inventory
95images28.96GB/BuildKit20.52GB is retained during Claude's concurrent work.
