# Quant Data Layer v2.0.26

## Certified Scope

`v2.0.26` certifies the current declared V2 consumer plane for Binance USD-M
and OKX Swap. The sealed no-order C2 receipt passed all `299` active products:
`234` durable and `65` on-demand. It exercised both Query replicas, public V2
Query/Stream SDK paths, signed cursor/reconnect, final BAR/history, reference
batch, L2 status/snapshot and manifest-governed V1 fallback.

The acceptance observed for `300.089s`, completed every opening and closing
product read, made zero order actions and zero provider-direct client
connections, retained no secrets or raw market payload, and removed its client
cursor directory. Seven allowed `TRADE` routes passed `V2_PRIMARY ->
V1_FALLBACK -> V2_PRIMARY`; no blocked route was downgraded.

## Runtime Provenance

The active Query and Stream roles run
`qdl-v2-python:2.0.26-137633b@sha256:0a69fbf0c883cad27a433ea529555269ab7386706de6e1c635fa5fef2107a545`.
Both Query and both Stream roles were healthy with restart count `0` and no
OOM kill at certification. The Stream image override is stored in external
release state, not `/tmp`; the temporary source path was removed after a
byte-identical Compose resolution and serial Stream roll.

The release is component-attested. The active reader image contains all
serving runtime changes in this closure. Source changes after that image are
only the acceptance harness, its regression tests and release journal. Rust
post-image work is a pure shared golden/parity oracle and is not a new serving
path in `qdl-realtime-core`.

## Consumer-Call Latency

The certificate retains per-binding/per-replica p50/p95/p99/max measurements.
They measure authenticated consumer request start to SDK-usable response under
the concurrent 299-product C2 workload. They must not be read as venue event
age or final-bar close latency. The full bounded figures and semantics are in
`DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md` R1.35-C and the sealed runtime
receipt identified by SHA-256
`6cbbbac45c7f8f9e321677499dc698addd5cce62f74201f261a1dce68a5b53aa`.

## Declared Boundaries

- DNSE/VN remains `V1_PRIMARY` until its separate market-hours certificate.
- Dark or unentitled Spot/catalog rows are not claimed as V2 execution scope.
- This data-plane release does not grant broker-order authority; Trading System
  and alpha paper/live policy remain their own control plane.

The machine-readable [certificate](./certificate.json) and
[scope evidence](./scope-evidence.json) are part of this release.
