# Data Layer v2.2.2 - Draft Release Preparation

Status: **not published, not yet certified at the final Query artifact**.

This patch groups existing Kafka client recovery, graceful projector handoff,
broker-confirmed canonical hot backup and lossless reference proof changes.
It keeps the KN architecture, source timestamps, entitlements and strict execution
eligibility. SDK 2.0.7 carries the shared proof validator; public API remains 2.0.0.

Rust recovery/Stream/projector changes already run on the recorded immutable
component images. Python commits `12785ab` and `a243096` still need Query-only
packaging and affected runtime readback. No Rust rebuild or catalogue C2 is implied.

The preparation index lists exact source commits, current runtime, inherited
measurements, rollback and remaining gates. It deliberately is not named
`certificate.json`: no PASS release certificate exists until final artifact and
runtime delta agree. Existing 300-second acceptance is inherited only for
unchanged predicates. Same-host Kafka backup is not independent HA or zero downtime.

TRADE eligibility refusals remain visible; session health never authorizes an
expired execution price. No alpha activation, TS accounting or broker/mainnet
execution certification is part of this Data Layer patch.
