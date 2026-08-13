import asyncio
import os
import unittest
from unittest import mock


try:
    import fastapi  # noqa: F401

    HAS_APP_DEPENDENCIES = True
except ModuleNotFoundError:
    HAS_APP_DEPENDENCIES = False

if HAS_APP_DEPENDENCIES:
    from app import main as app_main


@unittest.skipUnless(HAS_APP_DEPENDENCIES, "lifespan ownership test runs in the application image")
class RuntimeSourceLifespanTests(unittest.IsolatedAsyncioTestCase):
    async def test_spot_off_starts_only_usdm_sources_without_starting_vn_workers(self):
        async def idle_stream(*args, **kwargs):
            await asyncio.Event().wait()

        with (
            mock.patch.dict(
                os.environ,
                {
                    "DATA_LAYER_BINANCE_SOURCES": "binance_futures_trade,binance_futures_kline",
                    "DATA_LAYER_DNSE_STREAM_ENABLED": "false",
                    "DATA_LAYER_VNSTOCK_POLLER_ENABLED": "false",
                    "DATA_LAYER_PRELOAD_WATCHDOG_ENABLED": "false",
                },
            ),
            mock.patch.object(app_main.redis_cache, "init_ping", new=mock.AsyncMock()),
            mock.patch.object(app_main, "start_stream", side_effect=idle_stream) as start_stream,
            mock.patch.object(app_main, "load_vn_symbols", return_value=["VN30F1M"]),
        ):
            async with app_main.lifespan(app_main.app):
                await asyncio.sleep(0)
                sources = start_stream.call_args.kwargs["enabled_sources"]
                self.assertEqual(sources, ["binance_futures_trade", "binance_futures_kline"])
                self.assertFalse(any(source.startswith("binance_spot") for source in sources))
                self.assertIsNone(app_main.dnse_stream_manager)


if __name__ == "__main__":
    unittest.main()
