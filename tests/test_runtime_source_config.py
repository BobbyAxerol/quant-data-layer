import unittest

from app.runtime_source_config import DEFAULT_BINANCE_SOURCES, RuntimeSourceConfig


class RuntimeSourceConfigTests(unittest.TestCase):
    def test_defaults_preserve_current_runtime_ownership(self):
        config = RuntimeSourceConfig.from_env({})

        self.assertEqual(config.binance_sources, DEFAULT_BINANCE_SOURCES)
        self.assertTrue(config.spot_enabled)
        self.assertTrue(config.usdm_enabled)
        self.assertTrue(config.dnse_stream_enabled)
        self.assertTrue(config.vnstock_poller_enabled)
        self.assertTrue(config.preload_watchdog_enabled)

    def test_spot_can_be_disabled_without_disabling_usdm(self):
        config = RuntimeSourceConfig.from_env(
            {
                "DATA_LAYER_BINANCE_SOURCES": "binance_futures_trade,binance_futures_kline",
                "DATA_LAYER_DNSE_STREAM_ENABLED": "true",
            }
        )

        self.assertFalse(config.spot_enabled)
        self.assertTrue(config.usdm_enabled)
        self.assertEqual(
            config.public_summary()["binance_sources"],
            ["binance_futures_trade", "binance_futures_kline"],
        )

    def test_empty_source_list_disables_binance_only(self):
        config = RuntimeSourceConfig.from_env({"DATA_LAYER_BINANCE_SOURCES": ""})

        self.assertEqual(config.binance_sources, ())
        self.assertFalse(config.spot_enabled)
        self.assertFalse(config.usdm_enabled)
        self.assertTrue(config.dnse_stream_enabled)

    def test_independent_vn_runtime_flags(self):
        config = RuntimeSourceConfig.from_env(
            {
                "DATA_LAYER_DNSE_STREAM_ENABLED": "false",
                "DATA_LAYER_VNSTOCK_POLLER_ENABLED": "0",
                "DATA_LAYER_PRELOAD_WATCHDOG_ENABLED": "off",
            }
        )

        self.assertFalse(config.dnse_stream_enabled)
        self.assertFalse(config.vnstock_poller_enabled)
        self.assertFalse(config.preload_watchdog_enabled)

    def test_unknown_source_fails_fast(self):
        with self.assertRaisesRegex(ValueError, "Unsupported DATA_LAYER_BINANCE_SOURCES"):
            RuntimeSourceConfig.from_env({"DATA_LAYER_BINANCE_SOURCES": "binance_magic"})

    def test_duplicate_source_fails_fast(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            RuntimeSourceConfig.from_env(
                {"DATA_LAYER_BINANCE_SOURCES": "binance_futures_trade,binance_futures_trade"}
            )

    def test_invalid_boolean_fails_fast(self):
        with self.assertRaisesRegex(ValueError, "DATA_LAYER_DNSE_STREAM_ENABLED"):
            RuntimeSourceConfig.from_env({"DATA_LAYER_DNSE_STREAM_ENABLED": "sometimes"})


if __name__ == "__main__":
    unittest.main()
