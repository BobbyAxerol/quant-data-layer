# Claude / Codex Stage3 Alignment

Read-only TS inspection on 2026-10-08; this document is a handoff, not a message
acknowledged by Claude and not a runtime certificate. Unified Plan journal:
"Claude alignment and DR05 raw publisher". DL release remains HOLD.

## Concurrent Ownership

TS worktree `/home/bobby/.worktrees/trading-system-next-upgrade` is owned by
Claude. HEAD at inspection: `2abbe394ea0fb8f7f5ebcbb745ff70a8c83c9769`.
Dirty work includes Gateway order/fill history pagination, cursor module,
database index, tests, compatibility manifests and alpha_sdk2.4.0. No TS file was
changed by Codex. These are source observations, not claims that the new work
passed CI or was deployed. Owner's plan-heading blank-line edit is preserved.

The previous CI/image attestation at2abbe39/a6548ee3 remains valid only for that
revision. New source must not silently inherit that identity. Confirm the final
TS SHA/image before writing final provenance. `alpha_sdk`2.4.0 is the TS trading
SDK, not `qdl_sdk`2.1.0, the Data Layer SDK.

## Work Which Does Not Wait

- Codex: DR05 raw injection plumbing and isolated regressions, using the already
  prepared five-role DL stack. No shared provider, credentials, quota or runtime.
- Claude: actual TS validation/observation integration for upstream fault cells.
  His `scripts/p184_dr05_isolated.py` already owns STALE_TYPED,
  DISCONNECT_RECONNECT and APPROVED_FALLBACK_AND_RETURN_NO_FORBIDDEN_V1. It
  explicitly excludes GAP_REFUSAL and GENERATION_CHANGE_FENCED, which require
  upstream faults; do not duplicate those existing consumer-transport cases.
- Consumer packaging: use the immutable SDK wheel in
  [SDK handoff](qdl-sdk-2.1.0-consumer-handoff.md). Alpha's inspected image has
  PyJWT2.13.0; install against the declared >=2.15.1 requirement. Exact2.15.1 plus
  the SDK was already verified in the isolated alpha image. Do not lower the
  dependency requirement, change strategy logic, or rebuild unrelated services.
- [One-commit recipe/CI](p184-consolidated-build.md) is implemented; no need to
  rebuild it or rerun unrelated acceptance while TS continues.

## Gates Which Still Wait

DR01 deployed and DR08 final need Claude's stage3 signal. DR05 upstream cells
need raw -> canonical -> reader -> actual TS refusal/recovery proof, not only
Kafka ACK or process readiness. Wrong-account DR01 remains Gateway-owned.
IDR075 grants require the paired reader/caller revision window. IDR040fix2
heartbeat/quiet-field contract remains deferred; no silent freshness change.
Consolidated DL rebuild waits for TS acceptance completion and exact-source CI;
public release additionally requires DR01-DR08/broker delta and rollout receipt.

## New Raw Publisher Contract

`scripts/p184_dr05_raw_publisher.py` uses the existing validated raw-envelope
builder. `RawPublisher(packet, config).publish(frame, timeout=5)` returns the
actual broker topic/partition/offset, raw-byte hash and capture ID. It preserves
the frame's clock, generation and explicit TEST_ONLY provenance. Same venue,
symbol, namespace and transport checks run before publishing. Its endpoints and
TLS paths are fixed to the disposable loopback namespace.

The calling disposable helper must set `P184_DR05_ISOLATED_NAMESPACE` to its
packet namespace. This attestation supplements Docker namespace isolation; it
does not replace it. No automatic retries after timeout or delivery errors.
Offset0 is valid. Unknown/sentinel offsets fail the test because this observer
needs an exact offset; this does not change production ingestor ACK semantics.

Broker ACK means publication only. Every receipt has acceptance=false, and the
publisher cannot close a DR cell. Match capture ID and committed offset to
canonical/quarantine observations, then test the actual TS reader/validator.
Do not manufacture status from the injected phase or treat a quiet poll as proof
that an old-generation event was fenced. Final evidence must retain controls,
typed refusal, post-fault recovery and both venue identities.

Canonical DL remains main5bf5cd5/v2.2.2, feature fix/p184-dl-closure. No push,
merge, shared runtime change or image build. Retain active/rollback artifacts,
including BARf7351c3b; cleanup is restricted to owned disposable tests.


## DR05 Observer Handoff After TS01eb0d7

Read-only TS inspection now finds `01eb0d7`, adding
`scripts/p184_dr05_upstream.py` and WriterClone. This source is newer than the
confirmed2abbe39/a6548ee3 pair: do not put01eb0d7 into provenance for that image.
Codex has not changed or run Claude's TS checkout.

Available DL implementation: RawPublisher + CanonicalObserver (bounded raw
commit/EOF proof), real unary Query/Stream observations, and actual in-image TS
SDK/BOOK validation + Redis adoption. See [DR05 guide](p184-dr05-dl-fault.md).
This is NOT a substitute for the full writer clone stream/ACK path.

Integration points to reconcile before claiming the new TS runner is wired:

1. Claude's DlChannel expects JSONL reset_case/publish/canonical_observe. DL's
   standalone runner owns one NEW five-role stack per cell and has no in-place
   reset RPC. Reuse the Python publisher/observer APIs; bind each cell to a fresh
   disposable stack. Do not implement reset by flushing shared state or merely
   restarting sequence7 against an already advanced core.
2. CanonicalObserver returns raw_committed_next, read_committed_eof, records and
   capture-matched rows. `observed=true` must derive from those proofs, never the
   phase label or broker publish ACK alone.
3. Binance bootstrap anchor alone is intentionally not yet canonical LIVE; the
   following continuity delta establishes the book. The TS evaluator currently
   demands positive_anchor APPROVED on both venues. Respect this venue protocol;
   do not force a bootstrap-only frame to become executable.
4. The TS evaluator currently accepts any EXECUTION_CONTEXT_* refusal after
   polling. DATA_STALE after a wait is not evidence of GAP propagation. Preserve
   early snapshots while the prior book is still fresh and require a typed fault
   fence (not INPUT_ERROR or Redis NO_WRITE). All repeated old views need their
   canonical generation/sequence and actual validator outcome recorded.
5. BOOK quality fault propagation is currently FAIL on the tested isolated path;
   fixing the harness alone cannot turn that into PASS. Keep releaseHOLD and the
   four cells open. Positive recovery and old-generation filtering are retained,
   but do not imply immediate invalidation after a gap.

No acknowledgement from Claude or full writer-clone acceptance is claimed here.
The next shared handoff should pin the actual TS image and use these observations
as the counterexample for the narrow upstream invalidation fix, not rerun C2.
