from __future__ import annotations

import csv
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

from qdl_sdk import AsyncDataLayerClient, Grade
from qdl_sdk.errors import ContinuityError, DataLayerError
from scripts.build_qdl_sdk_release import build_wheel


class QdlSdkReleaseTests(unittest.TestCase):
    def test_wheel_is_reproducible_self_contained_and_recorded(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            first_result = build_wheel(Path(first))
            second_result = build_wheel(Path(second))
            self.assertEqual(first_result["wheel_sha256"], second_result["wheel_sha256"])
            wheel = Path(first) / str(first_result["wheel"])
            self.assertEqual(hashlib.sha256(wheel.read_bytes()).hexdigest(), first_result["wheel_sha256"])
            with zipfile.ZipFile(wheel) as archive:
                names = set(archive.namelist())
                self.assertIn("qdl_sdk/__init__.py", names)
                self.assertIn("qdl/query/v2/query_pb2.py", names)
                self.assertIn("qdl/marketdata/v2/market_data_pb2.py", names)
                self.assertFalse(any(name.startswith("qdl/api_v2/") for name in names))
                record_name = f"qdl_sdk-{first_result['version']}.dist-info/RECORD"
                rows = list(csv.reader(io.StringIO(archive.read(record_name).decode())))
                self.assertEqual({row[0] for row in rows}, names)
            release = json.loads(Path(first_result["manifest"]).read_text())
            self.assertFalse(release["contains_service_internals"])
            self.assertEqual(release["version"], "2.0.2")
            self.assertEqual(len(release["generated_contract_digest"]), 64)
            sbom = json.loads(Path(first_result["sbom"]).read_text())
            self.assertEqual(sbom["bomFormat"], "CycloneDX")


class _CatalogTransport:
    def __init__(self, pages):
        self.pages = pages

    async def instruments(self, *, consumer_id, consumer_grade, cursor, limit):
        del consumer_id, consumer_grade, limit
        return self.pages[cursor]

    async def close(self):
        return None


class _UnusedStreamTransport:
    async def close(self):
        return None


class QdlSdkInstrumentResolverTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _item(uid, *, market, native_symbol="BTCUSDT"):
        return {
            "instrument_uid": uid,
            "instrument_id": f"BINANCE.{market}.PERPETUAL.BTC-USDT",
            "venue": "BINANCE",
            "market": market,
            "product_type": "PERPETUAL",
            "canonical_symbol": "BTC-USDT",
            "metadata_revision": 7,
            "asset_class": "CRYPTO_DERIVATIVE",
            "native_symbol": native_symbol,
            "status": "ACTIVE",
        }

    def _client(self, pages):
        return AsyncDataLayerClient(
            query_transport=_CatalogTransport(pages),
            stream_transport=_UnusedStreamTransport(),
            consumer_id="consumer-alpha-v2",
        )

    async def test_resolves_exact_active_identity_across_bounded_pages(self):
        pages = {
            None: {
                "schema": "qdl.instruments.page.v2",
                "items": [self._item("uid-spot", market="SPOT", native_symbol="ETHUSDT")],
                "next_cursor": "page-2",
            },
            "page-2": {
                "schema": "qdl.instruments.page.v2",
                "items": [self._item("uid-usdm", market="USDM")],
                "next_cursor": None,
            },
        }
        result = await self._client(pages).resolve_instrument(
            venue="binance",
            product_type="perpetual",
            native_symbol="btcusdt",
            market="usdm",
            consumer_grade=Grade.ALPHA,
        )
        self.assertEqual(result.instrument_uid, "uid-usdm")

    async def test_missing_and_ambiguous_identity_fail_closed(self):
        missing_pages = {
            None: {
                "schema": "qdl.instruments.page.v2",
                "items": [],
                "next_cursor": None,
            }
        }
        with self.assertRaises(DataLayerError) as missing:
            await self._client(missing_pages).resolve_instrument(
                venue="BINANCE",
                product_type="PERPETUAL",
                native_symbol="BTCUSDT",
                consumer_grade=Grade.EXECUTION,
            )
        self.assertEqual(missing.exception.code, "INSTRUMENT_NOT_FOUND")

        ambiguous_pages = {
            None: {
                "schema": "qdl.instruments.page.v2",
                "items": [
                    self._item("uid-usdm", market="USDM"),
                    self._item("uid-coinm", market="COINM"),
                ],
                "next_cursor": None,
            }
        }
        with self.assertRaises(ContinuityError) as ambiguous:
            await self._client(ambiguous_pages).resolve_instrument(
                venue="BINANCE",
                product_type="PERPETUAL",
                native_symbol="BTCUSDT",
                consumer_grade=Grade.EXECUTION,
            )
        self.assertEqual(ambiguous.exception.code, "CONFLICT")

    async def test_catalog_cursor_cycle_fails_closed(self):
        pages = {
            None: {
                "schema": "qdl.instruments.page.v2",
                "items": [],
                "next_cursor": "cycle",
            },
            "cycle": {
                "schema": "qdl.instruments.page.v2",
                "items": [],
                "next_cursor": "cycle",
            },
        }
        with self.assertRaises(ContinuityError) as cycle:
            await self._client(pages).resolve_instrument(
                venue="OKX",
                product_type="PERPETUAL",
                native_symbol="BTC-USDT-SWAP",
                consumer_grade=Grade.ALPHA,
            )
        self.assertEqual(cycle.exception.code, "CONFLICT")


if __name__ == "__main__":
    unittest.main()
