from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl_sdk.errors import DataLayerError
from scripts.phase105_execution_l2_status_matrix import (
    DEFAULT_ACQUISITION,
    DEFAULT_CATALOG,
    DEFAULT_EXECUTION_DEMAND,
    DEFAULT_TRADING_MANIFEST,
    execution_book_products,
    ready_book_row,
    replica_parity,
    _read_one,
)


def _ready_row(*, source_id: str = "source-1", native_symbol: str = "BTCUSDT") -> dict[str, object]:
    return {
        "instrument_uid": "instrument-1",
        "venue": "BINANCE",
        "market": "USDM",
        "native_symbol": native_symbol,
        "feed": "BOOK_SNAPSHOT",
        "source_policy_id": "crypto_primary_v2",
        "source_id": source_id,
        "depth": 100,
        "typed_status": {
            "quality": {
                "state": "LIVE",
                "complete": True,
                "gap_open": False,
                "execution_eligible": True,
            }
        },
        "snapshot": {
            "sequence_verified": True,
            "book_generation": 1,
            "depth": 100,
            "watermark_offset": 12,
            "complete": True,
            "gap_open": False,
            "execution_eligible": True,
        },
    }


class ExecutionL2StatusMatrixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = StableSourceCatalog.load(DEFAULT_CATALOG)
        cls.acquisition = StableAcquisitionPlan.load(DEFAULT_ACQUISITION, catalog=cls.catalog)

    def test_declared_execution_matrix_is_exactly_ten_physical_books(self) -> None:
        products = execution_book_products(
            catalog=self.catalog,
            acquisition=self.acquisition,
            execution_demand=DEFAULT_EXECUTION_DEMAND,
            trading_manifest=DEFAULT_TRADING_MANIFEST,
        )
        self.assertEqual(len(products), 10)
        self.assertEqual(
            {(item.venue, item.native_symbol) for item in products},
            {
                ("BINANCE", symbol)
                for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT")
            }
            | {
                ("OKX", symbol)
                for symbol in (
                    "BTC-USDT-SWAP",
                    "ETH-USDT-SWAP",
                    "SOL-USDT-SWAP",
                    "DOGE-USDT-SWAP",
                    "BNB-USDT-SWAP",
                )
            },
        )

    def test_quality_matrix_fails_closed_for_gap_or_unverified_resync_view(self) -> None:
        row = _ready_row()
        self.assertTrue(ready_book_row(row))
        for field, value in (
            (
                "typed_status",
                {
                    "quality": {
                        "state": "GAPPED",
                        "complete": False,
                        "gap_open": True,
                        "execution_eligible": False,
                    }
                },
            ),
            ("snapshot", {**row["snapshot"], "sequence_verified": False}),
            ("snapshot", {**row["snapshot"], "book_generation": 0}),
        ):
            with self.subTest(field=field):
                changed = dict(row)
                changed[field] = value
                self.assertFalse(ready_book_row(changed))

    def test_replica_parity_rejects_cross_book_and_preserves_duplicate_ready_view(self) -> None:
        primary = _ready_row()
        duplicate_ready = _ready_row()
        self.assertTrue(replica_parity(primary, duplicate_ready))
        cross_book = _ready_row(source_id="source-other", native_symbol="ETHUSDT")
        self.assertFalse(replica_parity(primary, cross_book))


class ExecutionL2StatusMatrixReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_status_transport_error_returns_compact_fail_closed_row(self) -> None:
        class Client:
            async def feed_status(self, _requirement):
                raise DataLayerError("UNAVAILABLE", "query replica unavailable")

            async def close(self) -> None:
                return None

        product = SimpleNamespace(
            instrument_uid="book-uid",
            instrument_id="BINANCE.USDM.PERPETUAL.BTCUSDT",
            venue="BINANCE",
            market="USDM",
            native_symbol="BTCUSDT",
            feed=SimpleNamespace(value="BOOK_SNAPSHOT"),
            source_policy_id="crypto_primary_v2",
        )
        with tempfile.TemporaryDirectory() as raw:
            with patch(
                "scripts.phase105_execution_l2_status_matrix.sdk_requirement",
                return_value=object(),
            ), patch(
                "scripts.phase105_execution_l2_status_matrix._client",
                return_value=Client(),
            ):
                row = await _read_one(
                    product,
                    label="primary",
                    base_url="https://query.example",
                    grpc_target="stream.example:8210",
                    identity=object(),
                    state_dir=Path(raw),
                    timeout_seconds=15.0,
                )
        self.assertEqual(row["status_error"], {
            "code": "UNAVAILABLE",
            "detail": "query replica unavailable",
        })
        self.assertIsNone(row["typed_status"])
        self.assertIsNone(row["snapshot"])
        self.assertFalse(ready_book_row(row))
        self.assertFalse(row["payload_recorded"])


if __name__ == "__main__":
    unittest.main()
