# Phase 9.0-B Isolated V2 Beta Runbook

## Scope

This runbook re-certifies the read-only V2 query/stream beta from a bounded V1
source slice. V1 remains authoritative. The procedure must not recreate V1,
write production Redis, expose V2 publicly or promote Rust/source authority.

## Run

From the repository root on the approved feature revision:

```bash
make phase90b-test
make phase90b-certify
```

The certification uses isolated Compose projects, beta-only credentials,
loopback ports, dedicated AOF Redis and bounded durable stores. It validates the
real `BINANCE / USDM / PERPETUAL / BTCUSDT / BAR / 1m` bridge, exact V1/V2
closed-bar parity, active/passive fencing, outage recovery, security abuse,
capacity and cleanup.

## Required Evidence

- `upgrade/evidence/phase90b-isolated-v2-beta.json`
- `upgrade/evidence/phase90b-continuous-bridge.json`
- `upgrade/evidence/phase90b-capacity.json`
- `upgrade/evidence/phase90b-security-adversarial.json`
- `upgrade/evidence/PHASE90B_ISOLATED_V2_BETA_REPORT.md`
- `upgrade/evidence/phase90b-evidence.sha256`

Verify the bundle with:

```bash
sha256sum -c upgrade/evidence/phase90b-evidence.sha256
```

The decision must be `PASS_ISOLATED_NO_AUTHORITY_CUTOVER`. Generated market
events, canonical mismatches, duplicate timestamps, non-final bars,
execution-eligible events, production beta keys and cleanup counters must all
be zero.

## Rollback

The script removes its candidate projects, volumes and image tag automatically.
On interruption, remove only `qdl_phase90b_matrix` and `qdl_phase90b_bridge`
with both `phase7-beta` and `phase7-canary` profiles. Revoke beta credentials,
then verify V1 container identity/OpenAPI and zero `qdl:beta:v2:*` keys in
production Redis. Never restart or repair V1 to make this beta gate pass.

## Promotion Boundary

Passing this runbook permits isolated read-only beta review only. It does not
authorize Phase 9.1, public V2 exposure, critical-consumer sole dependency or
Rust/source authority promotion.
