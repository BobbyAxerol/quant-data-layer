from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

import yaml

from qdl.domain.instrument import InstrumentIdentity, ProductType
from qdl.query.contracts import (
    ConsumerGrade,
    DataRequirement,
    FeedType,
    RecoveryPolicy,
)
from qdl.query.results import MarketDataQueryBackend
from qdl.runtime.provider_history import (
    PASS_THROUGH_QUALITY_FLAG,
    ProviderBarHistorySource,
)
from qdl.runtime.routed_query import RoutedQueryBackend
from qdl.runtime.stable_catalog import StableSourceCatalog

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
BINANCE_BTC = "a953e16e-7138-5562-b5e8-c337a44d0b65"
SCHEMA_DIGEST = "b" * 64


def _binance_row(open_ms: int) -> list:
    return [
        open_ms, "100", "101", "99", "100.5", "10",
        open_ms + 59_999, "1005", 7, "5", "500", "0",
    ]


class _RawEnvelope:
    def __init__(self, frame: dict):
        self.raw_frame_bytes = json.dumps(frame, sort_keys=True).encode()
        self.provider = "BINANCE_DIRECT"
        self.capture_id = b"\x01" * 16
        self.raw_frame_sha256 = b"\x02" * 32


class _SpoolStub:
    """Stands in for the spool backend, recording what it was asked for."""

    def __init__(self, catalog: StableSourceCatalog):
        self.catalog = catalog
        self.schema_digest = SCHEMA_DIGEST
        self.calls: list[tuple[str, str]] = []

    def history(self, requirement):
        self.calls.append(("history", requirement.instrument_uid))
        return "SPOOL_HISTORY"

    def latest(self, requirement):
        self.calls.append(("latest", requirement.instrument_uid))
        return "SPOOL_LATEST"

    def open_gaps(self):
        self.calls.append(("open_gaps", ""))
        return ()


class RoutedQueryBackendTests(unittest.TestCase):
    """A binding always wins; the pass-through only covers what none does."""

    def setUp(self) -> None:
        self.catalog, self.unbound_uid = self._catalog_with_unbound_instrument()
        self.spool = _SpoolStub(self.catalog)
        self.fetch_calls: list[int] = []

        def fetcher(binding, *, limit: int, **rest):
            self.fetch_calls.append(limit)
            return tuple(
                _RawEnvelope({
                    "symbol": binding.native_symbol,
                    "interval": binding.interval,
                    "bar_origin": "BACKFILLED",
                    "row": _binance_row(1_787_385_300_000 + index * 60_000),
                })
                for index in range(limit)
            )

        self.pass_through = ProviderBarHistorySource(
            self.catalog, binance_fetcher=fetcher
        )
        self.backend = RoutedQueryBackend(self.spool, self.pass_through)

    def _catalog_with_unbound_instrument(self):
        payload = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
        spare = copy.deepcopy(payload["instruments"][0])
        identity = InstrumentIdentity.create(
            venue="BINANCE",
            market="USDM",
            product_type=ProductType("PERPETUAL"),
            canonical_symbol="SOL-USDT",
        )
        spare["instrument_uid"] = identity.instrument_uid
        spare["instrument_id"] = identity.instrument_id
        spare["canonical_symbol"] = "SOL-USDT"
        spare["native_symbol"] = "SOLUSDT"
        spare["base_asset"] = "SOL"
        payload["instruments"].append(spare)
        directory = Path(tempfile.mkdtemp())
        path = directory / "catalog.yaml"
        path.write_text(yaml.safe_dump(payload), encoding="utf-8")
        return StableSourceCatalog.load(path), identity.instrument_uid

    def _requirement(self, uid: str, **overrides) -> DataRequirement:
        values = dict(
            instrument_uid=uid,
            feed=FeedType.BAR,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="crypto_primary_v2",
            interval="1m",
            warmup_limit=3,
            recovery=RecoveryPolicy.FRESH_SNAPSHOT,
        )
        values.update(overrides)
        return DataRequirement(**values)

    def test_it_satisfies_the_backend_protocol(self):
        self.assertIsInstance(self.backend, MarketDataQueryBackend)

    def test_a_bound_instrument_always_uses_the_authoritative_spool(self):
        # Declaring FRESH_SNAPSHOT must never downgrade a covered consumer.
        requirement = self._requirement(BINANCE_BTC)
        self.assertFalse(self.backend.routes_to_pass_through(requirement))
        self.assertEqual(self.backend.history(requirement), "SPOOL_HISTORY")
        self.assertEqual(self.backend.latest(requirement), "SPOOL_LATEST")
        self.assertEqual(self.fetch_calls, [])

    def test_an_unbound_instrument_uses_the_pass_through(self):
        requirement = self._requirement(self.unbound_uid)
        self.assertTrue(self.backend.routes_to_pass_through(requirement))
        result = self.backend.history(requirement)
        self.assertEqual(len(result.items), 3)
        self.assertIn(
            PASS_THROUGH_QUALITY_FLAG, result.items[0].quality.flags
        )
        self.assertEqual(self.spool.calls, [])

    def test_an_unbound_instrument_without_fresh_snapshot_is_not_rerouted(self):
        requirement = self._requirement(
            self.unbound_uid, recovery=RecoveryPolicy.SNAPSHOT_AND_REPLAY
        )
        self.assertFalse(self.backend.routes_to_pass_through(requirement))
        self.assertEqual(self.backend.history(requirement), "SPOOL_HISTORY")

    def test_latest_takes_the_newest_row_of_a_bounded_window(self):
        requirement = self._requirement(self.unbound_uid, warmup_limit=2)
        item = self.backend.latest(requirement)
        self.assertEqual(self.fetch_calls, [2])
        self.assertEqual(item.payload["open_time_ns"], 1_787_385_360_000_000_000)
        self.assertFalse(item.quality.execution_eligible)

    def test_a_refused_pass_through_reports_not_ready_rather_than_empty(self):
        requirement = self._requirement(self.unbound_uid, interval="1M")
        self.assertIsNone(self.backend.history(requirement))

    def test_feed_status_follows_the_selected_source(self):
        quality = self.backend.feed_status(self._requirement(self.unbound_uid))
        self.assertIn(PASS_THROUGH_QUALITY_FLAG, quality.flags)
        self.assertFalse(quality.execution_eligible)

    def test_open_gaps_only_ever_come_from_materialised_bindings(self):
        self.assertEqual(self.backend.open_gaps(), ())
        self.assertIn(("open_gaps", ""), self.spool.calls)

    def test_without_a_pass_through_every_request_uses_the_spool(self):
        backend = RoutedQueryBackend(self.spool, None)
        requirement = self._requirement(self.unbound_uid)
        self.assertFalse(backend.routes_to_pass_through(requirement))
        self.assertEqual(backend.history(requirement), "SPOOL_HISTORY")


if __name__ == "__main__":
    unittest.main()
