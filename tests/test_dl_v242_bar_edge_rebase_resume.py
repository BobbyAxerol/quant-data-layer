"""DL v2.4.2 slice 3: after a market-cache loss every rebased BAR binding resumes live bars quickly, and a venue
history hole blocks only the binding it concerns, and only when no durable copy covers it.

The incident (2026-11-25). A host OOM killed ``market_cache`` at 05:54:48Z. The projectors rebuilt it, and the bar
edge rebased all 1,319 bindings ("product generation changed"). The edge then re-bootstrapped them one binding per
loop turn, A to Z, each with its full 10,000-row provider window (~1.75 min per OKX symbol). Derived 1h/12h bars stay
stopped until both the binding and its parent have a watermark (``run_derived_cycle``), so 60 of 94 OKX 1h series
had no live bar for up to ~4 h. OKX ASTER-USDT-SWAP never resumed. Its 1m window holds a venue hole: OKX has no
2026-11-19T15:39Z candle, although the cache had it from the live feed. Every attempt was refused with
``MISSING_PROVIDER_WINDOW``, so its 1m, and through the chain its 1h and 12h, stayed without live bars.

Covered here, at unit level, on the real catalogue, the real edge, the real OKX/Binance history fetchers and an
in-process market cache in the projector's layout:

- the incident end to end: cache rebuilt, rebase, resume from the old watermark (one small provider read per
  binding), live derived 1h and 12h bars resume at the next closes;
- venue holes, outside the resume suffix (never read), in the full window and durably covered (recorded
  ``VENUE_HOLE_DURABLY_COVERED``, resumes), at the watermark edge, and inside the suffix and not covered (refused
  ``VENUE_HOLE_NOT_COVERED``, typed event, the binding stays without live bars, the others are not blocked);
- a rebuilt cache that lost old rows falls back to the deep bootstrap (``RESUME_DURABLE_WINDOW_INCOMPLETE``);
- the recovery bound for N bindings: at most ``ceil(N / 16)`` loop turns and one provider page per binding
  (the legacy path needed N turns and full windows);
- the heartbeat counters an operator reads;
- the adapters: a hole is typed with its certified suffix, and a hole of several bars is no longer the untyped
  "history is short" RuntimeError that failed the whole cycle.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import math
from pathlib import Path
import unittest

from qdl.adapters.intervals import (
    BarHistoryGapError, BarHistoryProviderHoleError, canonical_interval_ms, latest_closed_boundary_ms, okx_bar_size,
)
from qdl.adapters.okx.history import HistoryCoverage, OkxCandle, OkxCandleHistory
from qdl.runtime import stable_bar_edge as edge_module
from qdl.runtime.kn_bar_readback import KnBarReadback, binding_product_key
from qdl.runtime.stable_bar_edge import StableBinanceBarEdge
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan, StableDerivedBarAcquisition
from tests.test_kn_bar_readback import FakeRedis, build_partitions, put_rows
from tests.test_v230_derived_bars import _authority, _envelope

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
ENVIRONMENT = "paper"
MINUTE, HOUR = 60_000, 3_600_000
DAY0 = 1_794_441_600_000  # 2026-11-12T00:00:00Z, a UTC day (and 12h) boundary
ROW = ["0", "1", "1", "1", "1", "1", "1", "1"]
ETH = "okx-swap-eth-usdt-swap-bar-"
SOL = "okx-swap-sol-usdt-swap-bar-"


class Venue:
    """OKX history-candles double: every bar of the series' own grid except the ones the venue lacks."""

    def __init__(self) -> None:
        self.missing: dict[tuple[str, str], set[int]] = {}
        self.calls: list[tuple[str, str, int]] = []
        self.native = {okx_bar_size(item): item for item in ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h",
                                                             "6h", "12h", "1d", "2d", "3d", "1w")}

    async def candles(self, *, inst_id, bar, start_ms, end_ms, price_type, max_records, max_pages):
        interval = self.native[bar]
        step = canonical_interval_ms(interval)
        newest = latest_closed_boundary_ms(interval, end_ms + 1, provider="OKX") - step
        lacking = self.missing.get((inst_id, interval), set())
        opens = [value for value in range(newest, start_ms - 1, -step) if value not in lacking][:max_records]
        self.calls.append((inst_id, interval, max_records - 3))  # the fetcher asks limit + 3
        records = tuple(OkxCandle(inst_id=inst_id, bar=bar, price_type=price_type, open_ts_ms=value, open="1",
                                  high="1", low="1", close="1", volume_raw="1", volume_ccy_raw="1",
                                  volume_quote_raw="1", confirmed=True) for value in sorted(opens))
        return OkxCandleHistory(records, HistoryCoverage(
            requested_start_ms=start_ms, requested_end_ms=end_ms,
            observed_min_ts_ms=records[0].open_ts_ms if records else None,
            observed_max_ts_ms=records[-1].open_ts_ms if records else None, complete_left=True,
            complete_right=True, truncated=False, terminal_reason="REACHED_REQUEST_START",
            provider_endpoint="/api/v5/market/history-candles"))


class Projector:
    """Kafka + Rust core + projector double: every ACKed record becomes a durable cache row at once."""

    def __init__(self, redis, sources) -> None:
        self.redis = redis
        self.by_source_id = {item.source_id: item for item in sources}
        self.published: list[tuple[str, int, str]] = []

    def publish_many(self, values):
        values = list(values)
        for value in values:
            source = self.by_source_id[value.subscription_id]
            raw = json.loads(value.raw_frame_bytes)
            kind = "derived" if raw.get("schema") == "qdl.derived-bar.v1" else "history"
            open_ms = int(raw["open_time_ms"]) if kind == "derived" else int(raw["data"][0][0])
            self.published.append((source.binding_id, open_ms, kind))
            pointer = f"kn3:{ENVIRONMENT}:ptr:{binding_product_key(source, ENVIRONMENT).encode()}"
            generation = int(self.redis.hashes[pointer]["ready"])  # the product's READY generation
            put_rows(self.redis, ENVIRONMENT, source, generation, [_envelope(source, open_ms, ROW)])
        return tuple(range(len(values)))

    def opens(self, binding_id: str, kind: str | None = None) -> list[int]:
        return [item[1] for item in self.published if item[0] == binding_id and (kind is None or item[2] == kind)]


class _Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = StableSourceCatalog.load(CATALOG_PATH)
        cls.plan = StableAcquisitionPlan.load(ACQUISITION_PATH, catalog=cls.catalog)
        cls.sources = {item.binding_id: item for item in cls.catalog.bindings}
        cls.acquisitions = {item.binding_id: item for item in cls.plan.bindings}
        cls.authority = _authority()

    def derived(self, binding_id: str, base: str):
        value = replace(self.acquisitions[binding_id], mode="DERIVED_FINAL", websocket_url=None,
                        business_websocket_url=None, derived_from=StableDerivedBarAcquisition(base))
        value.validate(self.sources[binding_id])
        return value

    def make_edge(self, pairs, *, derived=(), parents=None, warmup_rows: int, now_ms: int):
        self.redis = FakeRedis()
        build_partitions(self.redis, ENVIRONMENT)
        self.now = [now_ms / 1000]
        self.projector = Projector(self.redis, [source for source, _a in pairs])
        edge = StableBinanceBarEdge(catalog=self.catalog, acquisition=self.plan, authority=self.authority,
                                    publisher=self.projector, warmup_rows=warmup_rows, clock=lambda: self.now[0],
                                    bar_readback=KnBarReadback(self.redis, ENVIRONMENT))
        # The edge narrowed to the bindings under test (the catalogue is the real one).
        edge.history_bindings = tuple(pair for pair in pairs if pair[1].runtime == "BINANCE")
        edge.history_okx_bindings = tuple(pair for pair in pairs if pair[1].runtime == "OKX")
        edge._acquisition_by_id = {a.binding_id: a for _s, a in pairs}
        edge._binding_digests = {s.binding_id: edge._binding_digests[s.binding_id] for s, _a in pairs}
        edge.derived_bindings = tuple(derived)
        edge._derived_parent = {child: self.sources[parent] for child, parent in (parents or {}).items()}
        edge._derived_active = bool(derived)
        edge.canonical_cache_id = edge._observed_canonical_cache_id()
        edge._issue_connection_generation()
        self.venue = Venue()
        edge._okx_kwargs = lambda _priority: {"history_client": self.venue}
        self.edge = edge
        return edge

    def fill(self, binding_id: str, last_open_ms: int, rows: int, generation: int, *, skip=()):
        source = self.sources[binding_id]
        step = canonical_interval_ms(source.interval)
        payloads = [_envelope(source, last_open_ms - index * step, ROW) for index in range(rows)
                    if last_open_ms - index * step not in skip]
        put_rows(self.redis, ENVIRONMENT, source, generation, payloads)

    def cache_loss(self, watermarks: dict[str, int], rows: dict[str, int], *, lost=()):
        """Generation 1 observed, then the cache is rebuilt as generation 2 from the compacted topic."""

        for binding_id, last in watermarks.items():
            self.fill(binding_id, last, rows[binding_id], 1)
        self.edge._last_open_ms.update(watermarks)
        self.assertEqual(self.edge._rebase_changed_products(), ())  # generations recorded, nothing rebases
        for binding_id, last in watermarks.items():
            self.fill(binding_id, last, rows[binding_id], 2, skip=lost)

    def turn(self) -> None:
        """One production loop turn (``run_forever``): history quantum, then derived bars."""

        self.edge.bootstrap_history(max_bindings=1)
        if self.edge._derived_active:
            self.edge._derived_next_at.clear()
            self.edge.run_derived_cycle()

    def pair(self, binding_id: str, acquisition=None):
        return self.sources[binding_id], acquisition or self.acquisitions[binding_id]


class IncidentEndToEndTests(_Base):
    """1m (native) -> 1h (derived from 1m) -> 12h (derived from 1h) for OKX ETH, the AVAX/ASTER chain shape."""

    OUTAGE_START = DAY0 + 10 * HOUR          # last bars before the loss: 1m 09:59, 1h 09:00, 12h prev 12:00
    NOW = DAY0 + 12 * HOUR + 5 * MINUTE + 1_000
    OLD_HOLE = DAY0 + 2 * HOUR               # OKX lacks this 1m candle; the cache has it from the live feed

    def setUp(self) -> None:
        child_1h = self.derived(ETH + "1h", "1m")
        child_12h = self.derived(ETH + "12h", "1h")
        pairs = (self.pair(ETH + "1m"), self.pair(ETH + "1h", child_1h), self.pair(ETH + "12h", child_12h))
        derived = ((self.sources[ETH + "1h"], child_1h), (self.sources[ETH + "12h"], child_12h))
        self.make_edge(pairs, derived=derived, parents={ETH + "1h": ETH + "1m", ETH + "12h": ETH + "1h"},
                       warmup_rows=900, now_ms=self.NOW)
        self.venue.missing[("ETH-USDT-SWAP", "1m")] = {self.OLD_HOLE}
        self.watermarks = {ETH + "1m": self.OUTAGE_START - MINUTE, ETH + "1h": self.OUTAGE_START - HOUR,
                           ETH + "12h": DAY0 - 12 * HOUR}
        self.cache_loss(self.watermarks, {key: 900 for key in self.watermarks})

    def test_cache_loss_resumes_from_the_watermark_and_live_derived_1h_12h_follow(self):
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="INFO") as logs:
            self.turn()
        # The first turn rebased the three products and resumed all of them (quantum 16 >= 3).
        self.assertEqual(self.edge._history_resume_from, {})
        self.assertEqual(set(self.edge._last_open_ms), set(self.watermarks))
        self.assertTrue(any("qdl_bar_edge_rebootstrap_required" in line for line in logs.output))
        # One small provider read each: 1m from 09:59 to 12:04 (127 rows), 1h 4, 12h 2; the old hole never read.
        self.assertEqual([(interval, rows) for _symbol, interval, rows in self.venue.calls],
                         [("1m", 127), ("1h", 4), ("12h", 3)])
        self.assertFalse(self.edge._history_venue_holes)
        # Only the outage bars were published (the rest is durable).
        self.assertEqual(self.projector.opens(ETH + "1m", "history"),
                         list(range(self.OUTAGE_START, DAY0 + 12 * HOUR + 5 * MINUTE, MINUTE)))
        self.assertEqual(self.projector.opens(ETH + "1h", "history"), [DAY0 + 10 * HOUR, DAY0 + 11 * HOUR])
        self.assertEqual(self.projector.opens(ETH + "12h", "history"), [DAY0])
        # Live: the native 1m keeps arriving (ingestor); the derived 1h and 12h close at their next boundaries.
        source = self.sources[ETH + "1m"]
        live = [_envelope(source, value, ROW) for value in range(DAY0 + 12 * HOUR + 5 * MINUTE, DAY0 + 24 * HOUR,
                                                                   MINUTE)]
        put_rows(self.redis, ENVIRONMENT, source, 2, live)
        self.edge._last_open_ms[ETH + "1m"] = DAY0 + 24 * HOUR - MINUTE
        self.now[0] = (DAY0 + 24 * HOUR + 1_000) / 1000
        for _ in range(3):
            self.turn()
        self.assertEqual(self.projector.opens(ETH + "1h", "derived"),
                         list(range(DAY0 + 12 * HOUR, DAY0 + 24 * HOUR, HOUR)))
        self.assertEqual(self.projector.opens(ETH + "12h", "derived"), [DAY0 + 12 * HOUR])
        status = self.edge.serving_status()
        self.assertEqual((status["rebootstrap_waiting"], status["resume_pending"], status["rebootstrap_wait_s"]),
                         (0, 0, 0))

    def test_without_a_watermark_the_full_window_meets_the_old_hole_and_the_cache_covers_it(self):
        """The restart case (the watermark is in memory only): ASTER after a bar_edge recreate."""

        self.edge._rebase_changed_products()
        self.edge._history_resume_from.clear()
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="WARNING") as logs:
            for _ in range(3):
                self.turn()
        hole = self.edge._history_venue_holes[ETH + "1m"]
        self.assertEqual((hole["code"], hole["position"], hole["previous_ms"], hole["current_ms"],
                          hole["venue_missing_rows"], hole["durable_missing_rows"]),
                         (edge_module.VENUE_HOLE_DURABLY_COVERED, "NO_RESUME_WATERMARK", self.OLD_HOLE - MINUTE,
                          self.OLD_HOLE + MINUTE, 1, 0))
        event = json.loads(next(line.split(":", 2)[2] for line in logs.output if "qdl_bar_edge_venue_hole" in line))
        self.assertEqual((event["binding"], event["refused"]), (ETH + "1m", False))
        self.assertEqual(self.edge._last_open_ms[ETH + "1m"], DAY0 + 12 * HOUR + 4 * MINUTE)
        self.assertNotIn(ETH + "1m", self.edge._history_retry)
        self.assertEqual(self.edge.serving_status()["venue_holes_recorded"], 1)

    def test_a_hole_at_the_watermark_is_covered_and_one_just_after_it_is_refused(self):
        lacking = self.venue.missing[("ETH-USDT-SWAP", "1m")]
        lacking.add(self.watermarks[ETH + "1m"])  # venue lacks 09:59, the cache holds it
        self.turn()
        self.assertEqual(self.edge._history_venue_holes[ETH + "1m"]["position"], "OLDER_THAN_RESUME_WATERMARK")
        self.assertEqual(self.edge._history_venue_holes[ETH + "1m"]["code"], edge_module.VENUE_HOLE_DURABLY_COVERED)
        self.assertIn(ETH + "1m", self.edge._last_open_ms)

    def test_a_hole_inside_the_resume_suffix_is_fail_closed_for_its_span_only(self):
        """DL-BL-2026-11-25-C: the bar nobody holds stays a typed gap; the venue's other bars and live resume."""

        self.venue.missing[("ETH-USDT-SWAP", "1m")].add(self.OUTAGE_START)  # 10:00 exists nowhere
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="ERROR") as logs:
            self.turn()
        hole = self.edge._history_venue_holes[ETH + "1m"]
        self.assertEqual((hole["code"], hole["position"], hole["durable_missing_rows"], hole["uncovered_from_ms"],
                          hole["uncovered_to_ms"]),
                         (edge_module.VENUE_HOLE_NOT_COVERED, "INSIDE_RESUME_SUFFIX", 1, self.OUTAGE_START,
                          self.OUTAGE_START))
        event = json.loads(next(line.split(":", 2)[2] for line in logs.output if "qdl_bar_edge_venue_hole" in line))
        self.assertEqual((event["code"], event["refused"], event["span_fail_closed"], event["binding_resumed"]),
                         (edge_module.VENUE_HOLE_NOT_COVERED, True, True, True))
        # Live resumed; every venue bar after the watermark is published except the span, which is never invented.
        self.assertEqual(self.edge._last_open_ms[ETH + "1m"], DAY0 + 12 * HOUR + 4 * MINUTE)
        published = self.projector.opens(ETH + "1m", "history")
        self.assertEqual(published, list(range(self.OUTAGE_START + MINUTE, DAY0 + 12 * HOUR + 5 * MINUTE, MINUTE)))
        status = self.edge.serving_status()
        self.assertEqual((status["venue_holes_refused"], status["rebootstrap_waiting"], status["resume_pending"]),
                         (1, 0, 0))
        # The span is rechecked after its delay, not every turn; it heals once the venue serves the bar.
        calls = len(self.venue.calls)
        self.turn()
        self.assertEqual(len(self.venue.calls), calls)
        self.now[0] += edge_module._VENUE_HOLE_RETRY_SECONDS
        self.turn()  # still missing at the venue: rescheduled with a doubled delay
        self.assertEqual(len(self.venue.calls), calls + 1)
        self.assertEqual(self.edge._history_venue_holes[ETH + "1m"]["recheck_attempt"], 1)
        self.venue.missing[("ETH-USDT-SWAP", "1m")].discard(self.OUTAGE_START)
        self.now[0] += 2 * edge_module._VENUE_HOLE_RETRY_SECONDS
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="WARNING") as logs:
            self.turn()
        self.assertTrue(any("qdl_bar_edge_venue_hole_healed" in line for line in logs.output))
        self.assertIn(self.OUTAGE_START, self.projector.opens(ETH + "1m", "history"))
        self.assertEqual(self.edge.serving_status()["venue_holes_refused"], 0)

    def test_the_incident_shape_venue_hole_plus_cache_loss_below_it_publishes_the_venue_bars(self):
        """DL-BL-2026-11-25-C: OKX lacks the old bar (the cache has it); the cache lacks an older one (OKX has it)."""

        lost = self.OLD_HOLE - 2 * HOUR  # below the venue hole, inside today's demand window
        self.fill(ETH + "1m", self.watermarks[ETH + "1m"], 900, 3, skip={lost})
        self.edge._rebase_changed_products()
        self.edge._history_resume_from.clear()  # the full-window path, as after a first start
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="WARNING") as logs:
            for _ in range(3):
                self.turn()
        hole = self.edge._history_venue_holes[ETH + "1m"]
        self.assertEqual((hole["code"], hole["venue_missing_rows"], hole["durable_missing_rows"]),
                         (edge_module.VENUE_HOLE_DURABLY_COVERED, 1, 0))
        self.assertIn(lost, self.projector.opens(ETH + "1m", "history"))  # the venue's bar below the hole
        self.assertNotIn(self.OLD_HOLE, self.projector.opens(ETH + "1m"))   # never invented, the cache holds it
        self.assertIn(ETH + "1m", self.edge._last_open_ms)
        self.assertFalse([line for line in logs.output if '"refused":true' in line])

    def test_a_rebuilt_cache_that_lost_an_old_row_repairs_from_it_with_a_bounded_read(self):
        """DL-BL-2026-11-25-B: the read starts at the oldest missing open, never the whole window."""

        lost = DAY0 + 3 * HOUR
        self.fill(ETH + "1m", self.watermarks[ETH + "1m"], 900, 3, skip={lost})  # generation 3 lacks 03:00
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="INFO") as logs:
            self.turn()
        self.assertFalse([line for line in logs.output if "qdl_bar_edge_resume_refused" in line])
        rows = (self.NOW - (lost - MINUTE)) // MINUTE + 1  # 02:59 .. 12:04
        self.assertIn(("ETH-USDT-SWAP", "1m", rows), self.venue.calls)
        self.assertNotIn(("ETH-USDT-SWAP", "1m", 900), self.venue.calls)
        self.assertIn(lost, self.projector.opens(ETH + "1m", "history"))
        self.assertIn(ETH + "1m", self.edge._last_open_ms)
        self.assertFalse(self.edge._history_venue_holes)  # the 02:00 venue hole is older than the read

    def test_a_loss_at_the_window_start_is_a_typed_refusal_then_the_deep_bootstrap(self):
        # The oldest open of the demand window older than the watermark: repairing from it is the whole window.
        lost = self.watermarks[ETH + "1m"] - (900 - 127) * MINUTE
        # Without the old venue hole: a venue hole whose older window the cache does not fully hold stays
        # VENUE_HOLE_NOT_COVERED (fail-closed; DL-BL-2026-11-25-C), which is a separate case.
        self.venue.missing.clear()
        self.fill(ETH + "1m", self.watermarks[ETH + "1m"], 900, 3, skip={lost})
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="WARNING") as logs:
            self.turn()
        refused = [json.loads(line.split(":", 2)[2]) for line in logs.output if "qdl_bar_edge_resume_refused" in line]
        self.assertEqual([(item["binding"], item["code"], item["durable_missing_rows"]) for item in refused],
                         [(ETH + "1m", edge_module.RESUME_DURABLE_WINDOW_INCOMPLETE, 1)])
        self.assertNotIn(ETH + "1m", self.edge._history_resume_from)
        for _ in range(3):
            self.turn()
        # The deep window was re-read (900 rows) and the row refilled.
        self.assertIn(("ETH-USDT-SWAP", "1m", 900), self.venue.calls)
        self.assertIn(lost, self.projector.opens(ETH + "1m", "history"))
        self.assertIn(ETH + "1m", self.edge._last_open_ms)


    def test_resume_points_survive_a_restart_in_a_rollback_safe_sidecar(self):
        import tempfile

        with tempfile.TemporaryDirectory(prefix="qdl-v242-resume-") as directory:
            self.edge.state_path = Path(directory) / "crypto-bar-checkpoint.json"
            self.edge._rebase_changed_products()
            checkpoint = json.loads(self.edge.state_path.read_text())
            sidecar = json.loads((Path(directory) / "crypto-bar-checkpoint.json.resume.json").read_text())
            # The checkpoint keeps exactly its v2.4.2 fields (an older image reads it unchanged).
            self.assertEqual(set(checkpoint), {"schema", "slice_id", "authority_revision", "catalog_revision",
                                               "acquisition_revision", "warmup_rows", "binding_ids",
                                               "binding_digests", "canonical_cache_id", "connection_generation",
                                               "last_open_ms"})
            self.assertEqual({key: item["open_ms"] for key, item in sidecar["watermarks"].items()}, self.watermarks)
            # A restarted process (in-memory map gone) adopts them; a changed series digest is not adopted.
            self.edge._history_resume_from = {}
            self.edge._binding_digests = {**self.edge._binding_digests, ETH + "12h": "changed"}
            self.edge._load_resume_sidecar()
            self.assertEqual(self.edge._history_resume_from,
                             {key: value for key, value in self.watermarks.items() if key != ETH + "12h"})
            # A damaged sidecar is ignored as a whole (the deep bootstrap remains the fallback).
            (Path(directory) / "crypto-bar-checkpoint.json.resume.json").write_text("{")
            self.edge._history_resume_from = {}
            self.edge._load_resume_sidecar()
            self.assertEqual(self.edge._history_resume_from, {})


class RestoreResumeTests(_Base):
    """DL-BL-2026-11-25-B: after ANY restart a binding whose durable window survived resumes within the bound."""

    OUTAGE_START = DAY0 + 10 * HOUR
    NOW = DAY0 + 12 * HOUR + 5 * MINUTE + 1_000

    def setUp(self) -> None:
        import tempfile

        self.tmp = tempfile.TemporaryDirectory(prefix="qdl-v243-restore-")
        pairs = (self.pair(ETH + "1m"), self.pair(ETH + "1h"), self.pair(SOL + "1m"))
        self.make_edge(pairs, warmup_rows=900, now_ms=self.NOW)
        self.watermarks = {ETH + "1m": self.OUTAGE_START - MINUTE, ETH + "1h": self.OUTAGE_START - HOUR,
                           SOL + "1m": self.OUTAGE_START - MINUTE}
        self.lost = DAY0 + 3 * HOUR
        for binding_id, last in self.watermarks.items():
            self.fill(binding_id, last, 900, 1, skip={self.lost} if binding_id == ETH + "1m" else ())
        self.edge.state_path = Path(self.tmp.name) / "crypto-bar-checkpoint.json"
        self.edge._last_open_ms.update(self.watermarks)
        self.edge._persist_state()
        # The restarted process: nothing in memory.
        self.edge._last_open_ms, self.edge._history_resume_from, self.edge._rebootstrap_since = {}, {}, None

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_a_restore_incomplete_binding_resumes_from_its_oldest_missing_open(self):
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="INFO") as logs:
            self.edge._restore_state()
        self.assertTrue(any("bounded repair required" in line for line in logs.output))
        self.assertEqual(self.edge._history_resume_from, {ETH + "1m": self.watermarks[ETH + "1m"]})
        self.assertEqual(set(self.edge._last_open_ms), {ETH + "1h", SOL + "1m"})
        self.assertEqual(self.edge.serving_status()["resume_pending"], 1)
        self.turn()
        rows = (self.NOW - (self.lost - MINUTE)) // MINUTE + 1
        self.assertEqual([item for item in self.venue.calls if item[1] == "1m"], [("ETH-USDT-SWAP", "1m", rows)])
        self.assertIn(self.lost, self.projector.opens(ETH + "1m", "history"))
        self.assertEqual(self.edge.serving_status()["rebootstrap_waiting"], 0)

    def test_a_restart_across_a_cache_epoch_change_resumes_every_binding(self):
        checkpoint = json.loads(self.edge.state_path.read_text())
        checkpoint["canonical_cache_id"] = "0" * 32  # the cache epoch the old process saw
        self.edge.state_path.write_text(json.dumps(checkpoint))
        with self.assertLogs("qdl.runtime.stable_bar_edge", level="WARNING") as logs:
            self.edge._restore_state()
        self.assertTrue(any("cache generation changed; bounded bootstrap required" in line for line in logs.output))
        self.assertEqual(self.edge._history_resume_from, self.watermarks)
        self.turn()  # all three in one turn (resume quantum), each a bounded read
        self.assertEqual(set(self.edge._last_open_ms), set(self.watermarks))
        rows = {(symbol, interval): count for symbol, interval, count in self.venue.calls}
        self.assertEqual(rows[("ETH-USDT-SWAP", "1m")], (self.NOW - (self.lost - MINUTE)) // MINUTE + 1)
        self.assertEqual(rows[("SOL-USDT-SWAP", "1m")], (self.NOW - self.watermarks[SOL + "1m"]) // MINUTE + 1)
        self.assertEqual(rows[("ETH-USDT-SWAP", "1h")], (self.NOW - self.watermarks[ETH + "1h"]) // HOUR + 1)


class RecoveryBoundTests(_Base):
    """N bindings after one cache loss: every binding has live bars within ceil(N/16) loop turns."""

    OUTAGE_START = DAY0 + 10 * HOUR
    NOW = DAY0 + 12 * HOUR + 5 * MINUTE + 1_000

    def setUp(self) -> None:
        pairs = tuple((self.sources[item.binding_id], item) for item in self.plan.bindings
                      if item.enabled and item.runtime == "OKX" and self.sources[item.binding_id].feed.value == "BAR")
        self.make_edge(pairs, warmup_rows=30, now_ms=self.NOW)
        self.watermarks, rows = {}, {}
        for source, _acquisition in pairs:
            step = canonical_interval_ms(source.interval)
            self.watermarks[source.binding_id] = latest_closed_boundary_ms(
                source.interval, self.OUTAGE_START, provider="OKX") - step
            rows[source.binding_id] = self.edge._bootstrap_rows_for(source)
        self.cache_loss(self.watermarks, rows)
        self.count = len(pairs)

    def turns_until_live(self, limit: int) -> int:
        for turn in range(1, limit + 1):
            self.turn()
            if self.edge.serving_status()["rebootstrap_waiting"] == 0:
                return turn
        self.fail(f"bindings still waiting after {limit} turns: {self.edge.serving_status()}")

    def test_every_binding_is_live_within_the_stated_bound(self):
        self.assertGreater(self.count, 300)  # the repository catalogue's OKX BAR bindings
        bound = math.ceil(self.count / edge_module._RESUME_BINDINGS_PER_TURN)
        self.assertLessEqual(self.turns_until_live(bound), bound)
        # One provider read per binding, each at most the outage plus one bar.
        self.assertEqual(len(self.venue.calls), self.count)
        for (_symbol, interval, rows) in self.venue.calls:
            step = canonical_interval_ms(interval)
            self.assertLessEqual(rows, (self.NOW - self.OUTAGE_START) // step + 3)

    def test_the_legacy_path_needed_one_turn_per_binding(self):
        self.edge._rebase_changed_products()
        self.edge._history_resume_from.clear()  # what v2.4.2 did: no resume point
        self.assertEqual(self.turns_until_live(self.count), self.count)

    def test_waiting_bindings_are_visible_on_the_heartbeat(self):
        self.edge._rebase_changed_products()
        status = self.edge.serving_status()
        self.assertEqual((status["rebootstrap_waiting"], status["resume_pending"]), (self.count, self.count))
        self.now[0] += 42
        self.assertEqual(self.edge.serving_status()["rebootstrap_wait_s"], 42)


class AdapterHoleTests(unittest.TestCase):
    def _okx(self, interval="1m"):
        from qdl.adapters.okx.bar_edge import OkxBarRawBinding

        return OkxBarRawBinding(market="SWAP", product_type="PERPETUAL", native_symbol="ASTER-USDT-SWAP",
                                interval=interval, subscription_id="s", source_session_id="x",
                                connection_generation=1, lease_epoch=1, authority_revision=1, partition_plan_epoch=1,
                                adapter_version="t", config_revision=1, instrument_catalog_revision=1)

    def _okx_fetch(self, missing, *, allow_short, limit=100):
        from qdl.adapters.okx.bar_edge import fetch_closed_bar_history_raw_envelopes

        venue = Venue()
        venue.missing[("ASTER-USDT-SWAP", "1m")] = set(missing)
        return asyncio.run(fetch_closed_bar_history_raw_envelopes(
            self._okx(), limit=limit, now_ms=DAY0 + 1_000, history_client=venue, allow_short=allow_short))

    def test_okx_hole_is_typed_with_its_certified_suffix(self):
        hole_at = DAY0 - 10 * MINUTE
        with self.assertRaises(BarHistoryProviderHoleError) as caught:
            self._okx_fetch({hole_at}, allow_short=True)
        error = caught.exception
        self.assertEqual((error.kind, error.previous, error.current, error.requested_rows),
                         ("MISSING_PROVIDER_WINDOW", hole_at - MINUTE, hole_at + MINUTE, 100))
        self.assertEqual(error.missing_opens(), (hole_at,))
        opens = [int(json.loads(item.raw_frame_bytes)["data"][0][0]) for item in error.recent_envelopes]
        self.assertEqual(opens, list(range(hole_at + MINUTE, DAY0, MINUTE)))
        with self.assertRaises(BarHistoryGapError) as legacy:
            self._okx_fetch({hole_at}, allow_short=False)
        self.assertNotIsInstance(legacy.exception, BarHistoryProviderHoleError)

    def test_okx_multi_bar_hole_is_no_longer_an_untyped_short_history(self):
        missing = {DAY0 - offset * MINUTE for offset in range(20, 26)}  # 6 bars, more than the 2-bar slack
        with self.assertRaises(BarHistoryProviderHoleError) as caught:
            self._okx_fetch(missing, allow_short=True)
        self.assertEqual(len(caught.exception.missing_opens()), 6)

    def test_okx_hole_carries_every_venue_bar_on_both_sides_of_every_hole(self):
        holes = {DAY0 - 10 * MINUTE, DAY0 - 40 * MINUTE}
        with self.assertRaises(BarHistoryProviderHoleError) as caught:
            self._okx_fetch(holes, allow_short=True)
        error = caught.exception
        window = [int(json.loads(item.raw_frame_bytes)["data"][0][0]) for item in error.window_envelopes]
        self.assertEqual(window, sorted(window))
        self.assertTrue(holes.isdisjoint(window))
        self.assertIn(DAY0 - 41 * MINUTE, window)  # below the older hole
        self.assertEqual(error.previous, DAY0 - 11 * MINUTE)  # the newest hole names the suffix
        self.assertEqual(len(error.recent_envelopes), 9)

    def test_binance_hole_is_typed_with_its_certified_suffix(self):
        from qdl.adapters.binance.bar_edge import BinanceBarRawBinding, fetch_closed_bar_history_raw_envelopes

        binding = BinanceBarRawBinding(market="USDM", product_type="PERPETUAL", native_symbol="ASTERUSDT",
                                       interval="1m", subscription_id="s", source_session_id="x",
                                       connection_generation=1, lease_epoch=1, authority_revision=1,
                                       partition_plan_epoch=1, adapter_version="t", config_revision=1,
                                       instrument_catalog_revision=1)
        hole_at = DAY0 - 10 * MINUTE

        def fetch(symbol, *, interval, limit, end_time, market):
            top = end_time - end_time % MINUTE
            opens = [value for value in range(top, top - (limit + 2) * MINUTE, -MINUTE) if value != hole_at]
            rows = [[value, "1", "1", "1", "1", "1", value + 59_999, "1", 1, "1", "1", "0"]
                    for value in sorted(opens[:limit])]
            return {"data": rows}

        with self.assertRaises(BarHistoryProviderHoleError) as caught:
            fetch_closed_bar_history_raw_envelopes(binding, limit=100, now_ms=DAY0 + 1_000, attempts=1,
                                                   fetcher=fetch, sleep=lambda _s: None, allow_short=True)
        self.assertEqual((caught.exception.previous, caught.exception.current), (hole_at - MINUTE, hole_at + MINUTE))
        self.assertEqual(len(caught.exception.recent_envelopes), 9)
        with self.assertRaises(BarHistoryGapError):
            fetch_closed_bar_history_raw_envelopes(binding, limit=100, now_ms=DAY0 + 1_000, attempts=1,
                                                   fetcher=fetch, sleep=lambda _s: None)

    def test_the_hole_outcome_requires_a_missing_window_and_a_suffix(self):
        with self.assertRaises(ValueError):
            BarHistoryProviderHoleError("OKX", "X", "1m", 0, 30_000, requested_rows=1, recent_envelopes=(object(),))
        with self.assertRaises(ValueError):
            BarHistoryProviderHoleError("OKX", "X", "1m", 0, 180_000, requested_rows=1, recent_envelopes=())


if __name__ == "__main__":
    unittest.main()
