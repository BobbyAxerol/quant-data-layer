# Quant Data Layer v2.0.27

## Certified Scope

`v2.0.27` certifies the declared V2 consumer plane for Binance USD-M and OKX
Swap. The sealed no-order C2 receipt passed all `299` active products: `234`
durable and `65` on-demand. It exercised both Query replicas, public V2
Query/Stream SDK paths, signed cursor replay/reconnect, final BAR/history,
reference batch, L2 status/snapshot and manifest-governed V1 fallback.

The final receipt observed for `300.1s`, completed every opening and closing
product read, made zero order actions and zero provider connections, retained
no secrets or raw market payload, and removed its cursor directory. Seven
allowed `TRADE` routes passed `V2_PRIMARY -> V1_FALLBACK -> V2_PRIMARY`; all
`292` blocked routes remained blocked.

## Runtime Provenance

The active Query pair runs
`qdl-v2-python:2.0.26-8232d1b@sha256:f671dcebfdca1b28fc2cb99f3129f8f13e78a9a5853d2be8adfd9ee00c6f31b9`.
The release alias `qdl-v2-python:2.0.27-8232d1b` resolves to that same
immutable digest; no redundant rebuild or service recreation is required.
The active Stream pair remains on the separately attested
`qdl-v2-python:2.0.26-657e86f@sha256:c8d7458e57d62d6fb2d6366f42911d86085918f6f6939d624c61494798dac2ab`.
All four reader roles were healthy with restart count `0` and no OOM kill at
certification. Frozen V1 fallback remains available at
`qdl-v1-fallback:v1.2.4-2b0dcf7@sha256:dbfb57844977513ae7ec0a4782e04da0213028a789753c6b991f26043b615d65`.

This is a component-attested patch release. The final Query executable source
is `8232d1b`; later source changes through the release tree are the journal and
public certificate only, not a serving-code change. No redundant Query rebuild
or runtime recreation was performed merely to change a version label.

## Acceptance Timing

The C2 opening was `993.905s` against a quota-derived `1015s` budget, followed
by `300.1s` observation and `28.171s` closing readback. This is a full
concurrent evidence workload, not a normal alpha read latency measurement.
Per-binding endpoint latency and freshness retain their own semantics in the
sealed receipt and implementation journal.

## Declared Boundaries

- DNSE/VN remains `V1_PRIMARY` until its separate market-hours certificate.
- Dark or unentitled Spot/catalog rows are not claimed as V2 execution scope.
- This data-plane release does not grant broker-order authority; Trading System
  and alpha paper/live policy remain their own control plane.

The machine-readable [certificate](./certificate.json) and
[scope evidence](./scope-evidence.json) are part of this release.
