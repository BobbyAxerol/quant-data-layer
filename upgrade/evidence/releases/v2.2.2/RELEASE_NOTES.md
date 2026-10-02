# Quant Data Layer v2.2.2

## Scope

Bounded execution-readiness and recovery patch on the existing Kafka-native
architecture. Kafka client retries and projector handoff are bounded; Query can
use broker-confirmed canonical hot backup without inventing price timestamps.
MARK/INDEX retains component clocks and capture lineage. SDK2.0.7 shares the
execution proof validator. Public API, entitlement, realms and quality thresholds
are unchanged. This is not a Trading System order/accounting certification.

## Runtime Acceptance

Query candidate `qdl-v2-python:2.2.2-d6d2637`:
`sha256:514506122111df8992a5dfac9dc9a3db7ccae202a398424f4dd5a033d2ddf9a9`.
Only two Query roles were recreated in the final convergence;15 other active
Data Layer roles were unchanged. Both Query roles healthy, restart0/OOMfalse.
Existing Rust recovery/Stream/projector component images and evidence are retained.

Actual TS sandbox SDK2.0.7, both Query replicas:
- MARK/INDEX snapshot/reference: **44/44 usable**, including OKX inverse BTC;
 22 reference views preserve all six component clock/capture fields.
- Total **62/64 usable**. Two OKX TRADE prices older than3s were correctly refused;
 no simultaneous provider witness, so quiet-market versus pipeline attribution is
 not claimed. No stale execution price was admitted.
- Worker20/20 sampled READY over190.224s, session22/22, execution18/22, zeroV1fallback.
 This is scoped read acceptance, not another C2 or uninterrupted availability.

SDK call through validation, after metadata resolution, milliseconds:

| Venue | Path | N | Median | Maximum |
| --- | --- | ---: | ---: | ---: |
| Binance | MARK reference |10|14.06|68.79|
| Binance | MARK snapshot |10|19.70|55.97|
| OKX | MARK reference |12|14.09|30.36|
| OKX | MARK snapshot |12|18.02|69.37|

No p99 for these small groups. These are not event-to-Redis-commit measurements.
Unchanged endpoint/catalogue and prior hot-backup benchmarks remain explicitly
inherited in endpoint-report.json, not relabelled as fresh full-system tests.

## Verification And Limits

Candidate64/64 packaging tests,42/42 isolated Redis tests, actual entrypoint and
six-file Git/image hash checks pass. Remote contract/SDK/native fault gates pass
at the reviewed code head; publication requires all checks at the final PR heads.
The full certificate records the exact artifacts, evidence hashes and requirement.

Same-host Kafka backup is not independent HA or zero downtime. Historical refusal
and failed rollout/probe windows remain retained. Binance3d and DNSE/VN V2 remain
excluded. No alpha activation, mainnet authority or funded sizing claim.

## Rollback

Only `query_kn_2` and `query_kn_1`, same config/TLS/state, to
`sha256:3af57ddf17642e2073e09e8d92450e3aca551ebd5d855462c9ce60f148e5e85c`.
No Kafka offset reset, Redis flush, old SQLite reactivation or TS/order mutation.

## Final Publication Attestation

The frozen certificate records `REQUIRES_REMOTE_CI_AND_MAIN_ANCESTRY` because it
was assembled before the final remote checks. Publication resolves that external
gate through the **annotated `v2.2.2` tag**, not by rewriting historical evidence.
The annotation records the exact successful CI heads, run IDs and job conclusions,
feature/dev/main ancestry, and SHA-256 of this frozen certificate and endpoint report.
Inspect it with `git fetch origin tag v2.2.2` followed by
`git show --no-patch v2.2.2`, or resolve the tag object via the
[GitHub tag reference](https://api.github.com/repos/BobbyAxerol/quant-data-layer/git/ref/tags/v2.2.2)
and read its `object.url` annotation. The annotation must resolve all required
checks to PASS before the tag is pushed. The tag-triggered publication workflow
then verifies main ancestry and the certified endpoint-report and SDK-wheel hashes.
This resolution does not widen the scoped runtime acceptance or erase refusals.
