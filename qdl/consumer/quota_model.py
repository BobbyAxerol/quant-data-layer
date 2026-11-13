"""Request/stream demand model of an EXECUTION consumer manifest, and the quota floor it implies (v2.2.6).

Why: on 2026-11-13 the stage A window granted Trading System Paper r12 `requests_per_minute` 4,500 for 104 products.
The writer's steady Query demand for those products is about 6,000/min, so Query refused it with
`RATE_LIMITED: consumer request quota is exhausted` (qdl/security/data_plane.py `RedisMinuteQuota`, one fixed
window per calendar minute per consumer identity). The quota was a hand-picked number, not a derived one. This module
derives it from the declared requirements.

Per-product steady Query requests per minute (the Trading System market-data writer, image 7332e85a):

* QUOTE 90/min. The native writer probes a route on a cadence of execution-bound/3: 2,000 ms / 3 ≈ 667 ms
  (TS `rust/crates/market-data-writer/src/session.rs` `probe_cadence_ms`). Measured 83/min per route
  (2026-11-13 08:20-08:29Z: native_probe QUOTE 8,265 requests over 9.05 min, 11 routes).
* MARK_INDEX_PRICE 100/min. There is a reference read every `DATA_LAYER_V2_MARK_REFRESH_SECONDS` (1 s), and stream
  wakes are spaced at least `DATA_LAYER_V2_MARK_WAKE_MIN_INTERVAL_MS` (500 ms) apart. Measured ≈ 95/min per slice:
  the identity total, minus the QUOTE, TRADE and BOOK_SNAPSHOT shares, divided by 11 slices.
* TRADE 10/min. The writer probes only quiet routes, at bound/3. Measured 3.8/min per route.
* BOOK_SNAPSHOT 2/min. The REST snapshot refresh runs every 30 s (`DATA_LAYER_V2_BOOK_SNAPSHOT_REFRESH_SECONDS`).
* BOOK_DELTA and BAR 0/min. They are stream only; their reads are opening operations, covered below.

Check against measurement: for r11 (64 products) the model gives 2,212/min, and the shared quota counter read
2,015-2,034/min (stable_redis, 2026-11-13 08:27-08:29Z). The model is about 9 % conservative.

Quota floor (a quota is a ceiling, not a reservation, so it carries real headroom):
- `requests_per_minute >= ceil_to((steady + opening) * 1.5, 500)`. The opening operations are the initial
  snapshot/warmup reads of every product, which land in the first minute after a (re)start. The 1.5 factor exists
  because the shared counter INCRements before it compares (`_REDIS_MINUTE_QUOTA`), so refused requests also burn
  quota. The 2026-11-13 window showed a refusal spiral (ready 82 -> 27) once the minute was exhausted; 1.5x keeps
  retries after transient refusals (e.g. the read lane's finite pending bound) inside the minute.
- `max_streams >= ceil_to(streams * 1.5, 10)`, which also covers stream overlap during a bootstrap -> strict
  writer switch, where streams = one subscription per V2 product: the native slot streams
  TRADE/QUOTE, the Python workers stream MARK (`DATA_LAYER_V2_MARK_STREAM_WAKE=true`), BAR, BOOK_DELTA and
  BOOK_SNAPSHOT (`DATA_LAYER_V2_BOOK_SNAPSHOT_STREAM=true`). Measured: 64 Paper subscriptions for r11's 64 V2 products
  (stream `qdl_kn_subscription_closed`: 43 Python + 21 native at the 2026-11-13 restarts). r11's 3000 / 77 ran
  without refusals at about 1.5x the MEASURED load (2,000/min, 64 streams), which is the same factor applied here
  to the model.

Rows routed to V1 (VN / DNSE, source policy `vn_*`, routing reason VN_REAL_PROVIDER_GATE_UNEXERCISED) open no V2
stream or Query read and are not counted.

Boundary: a pure function of a manifest (requirements + quotas); no runtime access. It applies to EXECUTION-grade
consumers only. Alpha consumers have a different client, which this model does not cover.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Iterable, Mapping

STEADY_REQUESTS_PER_MINUTE = {
    "QUOTE": 90,
    "MARK_INDEX_PRICE": 100,
    "TRADE": 10,
    "BOOK_SNAPSHOT": 2,
    "BOOK_DELTA": 0,
    "BAR": 0,
}
# Initial reads per product on a writer (re)start (snapshot + warmup or reference read), all in the first minute.
OPENING_REQUESTS = {
    "QUOTE": 4,
    "MARK_INDEX_PRICE": 4,
    "TRADE": 4,
    "BOOK_SNAPSHOT": 4,
    "BOOK_DELTA": 4,
    "BAR": 6,
}
STREAM_FEEDS = frozenset(STEADY_REQUESTS_PER_MINUTE)
HEADROOM = 1.5
STREAM_HEADROOM = 1.5
RPM_STEP = 500
STREAM_STEP = 10


@dataclass(frozen=True)
class QuotaModel:
    products_by_feed: dict[str, int]
    steady_requests_per_minute: int
    opening_requests: int
    streams: int
    required_requests_per_minute: int
    required_max_streams: int

    def evidence(self) -> dict[str, Any]:
        return {
            "schema": "qdl.v2.consumer-quota-model.v1",
            "products_by_feed": dict(sorted(self.products_by_feed.items())),
            "steady_requests_per_minute": self.steady_requests_per_minute,
            "opening_requests": self.opening_requests,
            "streams": self.streams,
            "required_requests_per_minute": self.required_requests_per_minute,
            "required_max_streams": self.required_max_streams,
            "rates": dict(sorted(STEADY_REQUESTS_PER_MINUTE.items())),
            "headroom": HEADROOM,
            "stream_headroom": STREAM_HEADROOM,
        }


def _ceil_to(value: float, step: int) -> int:
    return int(math.ceil(value / step) * step)


def _feeds(requirements: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in requirements:
        if str(row.get("source_policy_id", "")).startswith("vn_"):
            continue  # V1-routed VN/DNSE row: no V2 stream or Query read
        feed = str(row.get("feed", "")).upper()
        if feed not in STEADY_REQUESTS_PER_MINUTE:
            raise ValueError(f"quota model has no request rate for feed {feed!r}")
        counts[feed] = counts.get(feed, 0) + 1
    return counts


def model_manifest(manifest: Mapping[str, Any]) -> QuotaModel:
    """Model a DataRequirement manifest mapping (the YAML shape of consumers/stable/*.yaml)."""
    counts = _feeds(manifest["spec"]["requirements"])
    steady = sum(STEADY_REQUESTS_PER_MINUTE[feed] * n for feed, n in counts.items())
    opening = sum(OPENING_REQUESTS[feed] * n for feed, n in counts.items())
    streams = sum(n for feed, n in counts.items() if feed in STREAM_FEEDS)
    return QuotaModel(
        products_by_feed=counts,
        steady_requests_per_minute=steady,
        opening_requests=opening,
        streams=streams,
        required_requests_per_minute=_ceil_to((steady + opening) * HEADROOM, RPM_STEP),
        required_max_streams=_ceil_to(streams * STREAM_HEADROOM, STREAM_STEP),
    )


def quota_shortfall(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Empty when the manifest's quotas cover the model; otherwise the missing amounts (and the model)."""
    model = model_manifest(manifest)
    quotas = manifest["spec"]["quotas"]
    short = {}
    if int(quotas["requests_per_minute"]) < model.required_requests_per_minute:
        short["requests_per_minute"] = {"granted": int(quotas["requests_per_minute"]),
                                        "required": model.required_requests_per_minute}
    if int(quotas["max_streams"]) < model.required_max_streams:
        short["max_streams"] = {"granted": int(quotas["max_streams"]), "required": model.required_max_streams}
    return {"shortfall": short, "model": model.evidence()} if short else {}


def is_execution_manifest(manifest: Mapping[str, Any]) -> bool:
    rows = manifest.get("spec", {}).get("requirements") or []
    return bool(rows) and all(str(row.get("consumer_grade", "")).upper() == "EXECUTION" for row in rows)
