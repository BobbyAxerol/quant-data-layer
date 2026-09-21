from __future__ import annotations

import unittest

from qdl.runtime.core_binding_identity import core_binding_map


def _binding(
    *,
    source_id: str = "okx-mark-index",
    component: str | None = None,
    channel: str = "mark-price",
) -> dict[str, object]:
    value: dict[str, object] = {
        "provider": "OKX",
        "venue": "OKX",
        "market": "SWAP",
        "product_type": "PERPETUAL",
        "native_symbol": "BTC-USDT-SWAP",
        "native_channel": channel,
        "instrument_uid": "OKX.SWAP.PERPETUAL.BTC-USDT",
        "source_id": source_id,
    }
    if component is not None:
        value["mark_index"] = {"component": component}
    return value


class CoreBindingIdentityTests(unittest.TestCase):
    def test_accepts_one_logical_source_with_mark_and_index_components(self) -> None:
        result = core_binding_map(
            [
                _binding(component="MARK", channel="mark-price"),
                _binding(
                    component="INDEX",
                    channel="index-tickers",
                ),
            ],
            field="test",
        )
        self.assertEqual(len(result), 2)

    def test_rejects_a_duplicate_component(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate mark_index component"):
            core_binding_map(
                [
                    _binding(component="MARK"),
                    _binding(component="MARK", channel="mark-price-secondary"),
                ],
                field="test",
            )

    def test_rejects_an_ordinary_mark_index_source_collision(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate ordinary source_id"):
            core_binding_map(
                [
                    _binding(component="MARK", channel="mark-price"),
                    _binding(component="INDEX", channel="index-tickers"),
                    _binding(component=None, channel="trades"),
                ],
                field="test",
            )

    def test_rejects_an_incomplete_component_pair(self) -> None:
        with self.assertRaisesRegex(ValueError, "incomplete mark_index"):
            core_binding_map(
                [_binding(component="INDEX", channel="index-tickers")],
                field="test",
            )


if __name__ == "__main__":
    unittest.main()
