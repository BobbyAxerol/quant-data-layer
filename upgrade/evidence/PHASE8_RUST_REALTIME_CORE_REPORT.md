# Phase 8 Rust Realtime Core Certification Report

Date: 2026-08-15

## Decision

Phase 8 is `COMPLETE`. The immutable Rust realtime-core candidate is certified
for isolated `RUST_SHADOW` operation only. V1 remains authoritative. Phase 8
does not authorize a public, legacy, canary or primary write cutover.

## Delivered Boundary

- A replicated Kafka 4.2.0 RF3/minISR2 mTLS shadow substrate with fail-closed
  ACLs, durable ACK cursors, bounded resources and replayable raw/canonical
  topics.
- A provider-neutral raw envelope and Rust core for exact identity, decimal,
  ordering, session/generation fencing, backpressure and stable sharding.
- Exact-frame Python/Rust conformance for authentic Binance USD-M, OKX SWAP and
  DNSE/VN data. Deribit remains explicitly fixture-only.
- An immutable non-root Rust image with pinned builder/runtime bases, SBOM,
  checksummed release manifest, RSA-3072 signatures, candidate partition plan
  and exact Python V1 rollback manifest.
- Persistent authority state and append-only authority audit responsibilities:
  compacted state restores the latest revision; the audit topic preserves the
  complete transition history.

## Certification Results

### Cross-Language And Provider Parity

- Phase 8.2 observed 1,855 authentic Binance and 510 authentic OKX events over
  189.03 seconds and retained a bounded 128 exact frames per venue.
- DNSE evidence contains 241 authentic `VN30F1M` one-minute rows for the full
  2026-08-14 session.
- 498 cross-venue fixtures replayed 200 times produced 99,600 deterministic
  events with zero semantic, byte, count, quality or process-restart mismatch.
- Release-profile certification replayed 279 fixtures 500 times: 139,500
  events, zero mismatch across three clean Rust processes, Python p99 0.224 ms,
  and minimum Rust release throughput 81,710 events/s.

### Authority And Recovery

- Rehearsed `RUST_SHADOW -> RUST_CANARY -> RUST_SHADOW` on an isolated broker.
- The compacted authority state restored revision 3 after full three-broker
  restart; append-only audit restored revisions `[1, 2, 3]` in order.
- Stale revision, public V2, legacy V1 and canary-after-rollback writes were all
  rejected. Public and legacy write counts remained zero.
- Certification cleanup left zero Phase 8 containers, networks and volumes.
  V1 health was HTTP 200 before and after; its inspected topology was unchanged.
- The unchanged V1 runtime still reports broad Binance Spot missing/stale
  telemetry, while demanded feeds have zero missing/stale and recent queue-drop
  delta is zero. Spot retirement remains a controlled consumer-migration task;
  Phase 8 intentionally did not restart or reconfigure V1.

### Contract And Regression Gates

- Buf format/lint passed.
- Breaking checks passed against frozen Phase 1 and Phase 7 beta baselines.
- Generated bindings remained clean.
- 45 targeted Python tests passed across broker, raw envelope, exact-frame,
  canonical pipeline, Binance, OKX, release and artifact boundaries.
- Full Rust workspace fmt, clippy with warnings denied, unit and doc tests passed.
- Frozen release signatures and artifact checksums verify in a network-disabled
  container; no private signing key is retained.

## Defects Found During Certification

The first restart test incorrectly expected a compacted state topic to serve as
an append-only journal. The design was corrected to use a compacted authority
state keyed by slice and a separate append-only audit record. A second run found
that the evidence consumer group did not match the fail-closed ACL prefix. The
consumer identity and evidence helper were corrected; missing records now fail
with explicit topic/group diagnostics rather than looking like data loss.

## Frozen Candidate

- Image: `qdl-phase8-rust@sha256:46a7c3fa516c0035c3ce41add0ce77e9acb4d4dfd1b0ac74130c894ca7ad5280`
- Image source revision: `053ec76`
- Runtime user: `10001:10001`
- Default authority: `RUST_SHADOW`
- Candidate slice: `BINANCE:USDM:TRADE:BTCUSDT`
- Release: `phase8-rust-realtime-core-v0.1.0-beta`

## Remaining Gates

Phase 9 still requires explicit operator approval and fresh slice-specific
shadow evidence before any canary. OKX SBE, Deribit live, BBO, L2/book and BAR
remain separate capability certifications. Regional failure domains, production
workload identity, external secret rotation and registry admission are not
claimed by this same-host shadow certification.

## Post-Merge Runtime Closure

On 2026-08-16, final runtime inspection found that the Python V1 image copied a
Poetry environment from `/app/.venv` to `/opt/venv`, leaving console-script
shebangs pointed at the builder path. The image now builds the environment at
`/opt/venv` directly, and CI invokes the actual `uvicorn` executable. This is a
V1 packaging correction only: the signed Phase 8 Rust candidate, its source
revision, shadow authority and evidence remain unchanged. Verification of that
historical candidate is explicitly separated from verification of artifacts at
the current repository HEAD. No Phase 9 canary or authority transition occurred.
