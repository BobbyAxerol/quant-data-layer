"""DL-DEF-2026-11-25-E: the SDK stream projection and Query deliver byte-identical BAR payloads, lineage included.

Incident (2026-11-25, hosted Crossec g71). OKX AAVE-USDT-SWAP 1h bar 12:00Z, event 7bba9b62..., revision 0, origin
AGGREGATED. The live (stream) copy had ``resample_lineage=None`` and the warmup (Query) copy carried it. SDK 2.10.0
``qdl_sdk/projection.py`` never read ``Bar.aggregation``, while Query renders it (``qdl/runtime/stable_source.py``
``bar_item_fields`` -> ``qdl/api_v2/router.py``). Both read the same canonical envelope.

Covered:
- parity of the Query and SDK stream paths for an AGGREGATED envelope and a VENUE_NATIVE one;
- the hosted rule as the public ``classify_bar_copies``. Positive: identical copies, and lineage-only in both
  directions. Negative: two different present lineages, a lineage on a non-AGGREGATED bar, a price difference, a
  revision difference.
"""
from __future__ import annotations

import hashlib
import json
import types
import unittest

from qdl.api_v2.router import _typed_payload
from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.runtime.stable_source import bar_item_fields
from qdl_sdk import BAR_COPIES_IDENTICAL, BAR_COPIES_LINEAGE_ONLY, __version__, classify_bar_copies
from qdl_sdk.errors import ContinuityError
from qdl_sdk.models import BarPayload
from qdl_sdk.projection import _payload

OPEN_MS = 1_795_579_200_000  # 2026-11-25T12:00:00Z
HOUR_MS = 3_600_000


def decimal(text: str) -> common_pb2.DecimalValue:
    whole, _, fraction = text.partition(".")
    return common_pb2.DecimalValue(mantissa=int(whole + fraction), scale=len(fraction), source_text=text)


def envelope(*, aggregated: bool, close: str = "171.23") -> market_data_pb2.EventEnvelope:
    value = market_data_pb2.EventEnvelope(schema_name="qdl.marketdata.bar", schema_major=2,
                                          event_id=bytes.fromhex("7bba9b625a7def88fffb833f991e2120"))
    bar = value.bar
    bar.interval = "1h"
    bar.open_time_ns = OPEN_MS * 1_000_000
    bar.close_time_ns = (OPEN_MS + HOUR_MS - 1) * 1_000_000
    for name, text in (("open", "170.10"), ("high", "172.00"), ("low", "169.50"), ("close", close),
                       ("volume", "12345.6")):
        getattr(bar, name).CopyFrom(decimal(text))
    bar.base_volume.CopyFrom(decimal("1234.56"))
    bar.quote_volume.CopyFrom(decimal("210000.5"))
    bar.contract_volume.CopyFrom(decimal("12345.6"))
    bar.trade_count = 42
    bar.is_final = True
    bar.revision = 0
    bar.lifecycle = market_data_pb2.BAR_LIFECYCLE_FINAL
    bar.volume_unit = common_pb2.QUANTITY_UNIT_CONTRACT
    if aggregated:
        bar.origin = common_pb2.BAR_ORIGIN_AGGREGATED
        bar.aggregation.base_interval = "30m"
        bar.aggregation.base_source_id = "okx-swap-aave-usdt-swap-bar-30m-stable-001"
        bar.aggregation.constituent_count = 2
        bar.aggregation.first_constituent_event_id = b"\x01" * 16
        bar.aggregation.last_constituent_event_id = b"\x02" * 16
        bar.aggregation.constituent_sha256 = hashlib.sha256(b"constituents").digest()
        bar.aggregation.first_constituent_offset = 1_206_474_152
        bar.aggregation.last_constituent_offset = 1_206_992_005
    else:
        bar.origin = common_pb2.BAR_ORIGIN_VENUE_NATIVE
    return value


def wire(document: dict) -> dict:
    """What a consumer receives: the JSON of the payload, validated by the SDK model."""

    return BarPayload.model_validate(json.loads(json.dumps(document))).model_dump(mode="json")


def query_payload(value: market_data_pb2.EventEnvelope) -> dict:
    item = types.SimpleNamespace(**bar_item_fields(value))
    return wire(_typed_payload(item))


def stream_payload(value: market_data_pb2.EventEnvelope) -> dict:
    _feed, _interval, _revision, payload = _payload(value)
    return wire(payload)


class StreamQueryParityTests(unittest.TestCase):
    def test_sdk_version(self):
        # 2.10.1 introduced the lineage parity; 2.11.0 (DL-BL-2026-11-26-K lifecycle) keeps it.
        self.assertEqual(__version__, "2.11.1")

    def test_aggregated_bar_is_byte_identical_on_both_paths_lineage_included(self):
        value = envelope(aggregated=True)
        stream, query = stream_payload(value), query_payload(value)
        self.assertEqual(stream, query)
        self.assertEqual(stream["resample_lineage"], {
            "base_interval": "30m", "constituent_count": 2, "first_watermark": 1_206_474_152,
            "last_watermark": 1_206_992_005, "constituent_sha256": hashlib.sha256(b"constituents").hexdigest()})

    def test_venue_native_bar_has_no_lineage_on_either_path(self):
        value = envelope(aggregated=False)
        stream, query = stream_payload(value), query_payload(value)
        self.assertEqual(stream, query)
        self.assertIsNone(stream["resample_lineage"])

    def test_every_bar_payload_key_is_emitted_by_both_builders(self):
        value = envelope(aggregated=True)
        _feed, _interval, _revision, raw_stream = _payload(value)
        raw_query = _typed_payload(types.SimpleNamespace(**bar_item_fields(value)))
        self.assertEqual(set(raw_stream), set(raw_query))


class HostedRuleTests(unittest.TestCase):
    """The consumer rule of README "BAR revision identity", as ``classify_bar_copies``."""

    def setUp(self):
        self.with_lineage = stream_payload(envelope(aggregated=True))
        self.without_lineage = {**self.with_lineage, "resample_lineage": None}  # a 2.10.0 live copy

    def test_identical_copies(self):
        self.assertEqual(classify_bar_copies(self.with_lineage, dict(self.with_lineage)), BAR_COPIES_IDENTICAL)

    def test_lineage_only_in_both_directions_is_the_same_bar(self):
        self.assertEqual(classify_bar_copies(self.without_lineage, self.with_lineage), BAR_COPIES_LINEAGE_ONLY)
        self.assertEqual(classify_bar_copies(self.with_lineage, self.without_lineage), BAR_COPIES_LINEAGE_ONLY)
        self.assertEqual(classify_bar_copies(BarPayload.model_validate(self.without_lineage),
                                             BarPayload.model_validate(self.with_lineage)), BAR_COPIES_LINEAGE_ONLY)

    def test_two_different_lineages_conflict(self):
        other = {**self.with_lineage,
                 "resample_lineage": {**self.with_lineage["resample_lineage"], "last_watermark": 1_206_992_006}}
        with self.assertRaises(ContinuityError) as caught:
            classify_bar_copies(self.with_lineage, other)
        self.assertEqual(caught.exception.code, "CONFLICT")

    def test_a_lineage_on_a_non_aggregated_bar_conflicts(self):
        native = stream_payload(envelope(aggregated=False))
        bogus = {**native, "resample_lineage": self.with_lineage["resample_lineage"]}
        with self.assertRaises(ContinuityError):
            classify_bar_copies(native, bogus)

    def test_a_price_difference_is_never_tolerated_even_with_lineage(self):
        revised_price = stream_payload(envelope(aggregated=True, close="171.24"))
        with self.assertRaises(ContinuityError):
            classify_bar_copies(self.without_lineage, revised_price)

    def test_a_revision_difference_is_not_a_lineage_difference(self):
        revised = {**self.with_lineage, "revision": 1, "lifecycle": "REVISED",
                   "supersedes_event_id": "7bba9b625a7def88fffb833f991e2120"}
        with self.assertRaises(ContinuityError):
            classify_bar_copies(self.without_lineage, revised)


if __name__ == "__main__":
    unittest.main()
