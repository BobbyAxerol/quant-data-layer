# Phase 8.0 Replicated Shadow Substrate Report

Date: 2026-08-15  
Decision: PASS for isolated `RUST_SHADOW`; no authority change

## Implemented

- Digest-pinned Apache Kafka 4.2.0, three KRaft broker/controller processes,
  RF3, minISR2, `acks=all`, idempotent producer, no unclean leader election.
- Mutual TLS and `StandardAuthorizer` with distinct admin, producer, consumer
  and unauthorized identities. No host port is published.
- Raw, canonical, quality, authority, quarantine and audit topics with bounded
  retention, record size, ownership and partition-key declarations.
- Async Rust Kafka durable sink/source. Producer cursor is the acknowledged
  Kafka partition/offset; consumer checkpoint is committed explicitly after a
  record is accepted by the downstream caller.
- Separate non-root Rust test image; V1 image and runtime are unchanged.
- OTel collector and alert contracts with bounded metric labels.

## Verification

| Gate | Result |
|---|---|
| Python topology/observability tests | 4 passed |
| Rust fmt/clippy | passed, warnings denied |
| Rust workspace tests | 13 passed |
| Rust mTLS produce/consume/checkpoint | passed |
| Initial durable replay | 64/64 exact records |
| Redis shadow rebuild | equal before/after flush |
| Unauthorized producer | durable offset unchanged |
| One broker stopped | write ACKed, offset +1 |
| Two brokers stopped | write failed closed, offset unchanged |
| Full broker restart | 65/65 ACKed records retained |
| One replica volume deleted/recreated | 65/65 retained, full ISR restored |
| Cleanup | zero test containers, networks and volumes |
| V1 invariant | health 200 before/after; topology unchanged |

Peak observed RSS was about 391 MiB per Kafka process and 4 MiB for the
ephemeral Redis projector. This is certification sizing, not a production
capacity approval.

## Boundary

The three replicas ran in isolated containers on one host. This proves broker
protocol, mTLS/ACL, ACK, minISR, restart and replica-restore semantics but not
independent rack or regional disaster recovery. All records remained in test
topics. V1 stayed authoritative and no public/legacy sink was written.

Machine-readable evidence:

- `phase8-broker-topology.json`
- `phase8-broker-failover.json`
- `phase8-broker-security.json`
