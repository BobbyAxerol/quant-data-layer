from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from app.config import DNSE_API_KEY, DNSE_API_SECRET_KEY, DNSE_WS_BASE
from app.database.dnse_fallback import _fetch_ohlc_raw
from app.stream.dnse_ws import TradingClient
from qdl.adapters.vn import (
    VnRawBinding,
    build_dnse_bar_raw_envelope,
    build_dnse_trade_raw_envelope,
)
from qdl.domain.calendar import trading_calendar_for_id
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl.transport.kafka_raw import KafkaRawPublisher, KafkaRawPublisherConfig


logger = logging.getLogger(__name__)


class StableDnseVendorEdge:
    """DNSE SDK/REST acquisition with durable ACK and zero silent queue drops."""

    def __init__(
        self,
        *,
        catalog: StableSourceCatalog,
        acquisition: StableAcquisitionPlan,
        authority: dict[str, Any],
        publisher: KafkaRawPublisher,
        queue_capacity: int = 5000,
        warmup_rows: int = 500,
        history_lookback_days: int = 30,
        history_attempts: int = 4,
        history_fetcher=_fetch_ohlc_raw,
        clock=time.time,
        sleep=time.sleep,
    ) -> None:
        if not 1 <= queue_capacity <= 100_000:
            raise ValueError("stable DNSE queue capacity is invalid")
        if not 1 <= warmup_rows <= 2000:
            raise ValueError("stable DNSE warmup rows must be between 1 and 2000")
        if not 1 <= history_lookback_days <= 87:
            raise ValueError("stable DNSE history lookback must be between 1 and 87 days")
        if not 1 <= history_attempts <= 8:
            raise ValueError("stable DNSE history attempts must be between 1 and 8")
        if (
            authority.get("mode") != "RUST_SHADOW"
            or authority.get("public_write_allowed") is not False
            or authority.get("legacy_write_allowed") is not False
        ):
            raise ValueError("stable DNSE edge requires shadow authority")
        self.catalog = catalog
        self.acquisition = acquisition
        self.authority = authority
        self.publisher = publisher
        self.warmup_rows = warmup_rows
        self.history_lookback_days = history_lookback_days
        self.history_attempts = history_attempts
        self.history_fetcher = history_fetcher
        self.clock = clock
        self.sleep = sleep
        self.session_id = f"qdl-v2-stable-dnse-{uuid.uuid4()}"
        source_by_id = {item.binding_id: item for item in catalog.bindings}
        selected = tuple(
            source_by_id[item.binding_id]
            for item in acquisition.bindings
            if item.mode == "PYTHON_VENDOR_SDK" and item.runtime == "DNSE"
        )
        self.trade_sources = {
            item.instrument.native_symbol: item
            for item in selected
            if item.feed.value == "TRADE"
        }
        self.bar_sources = {
            item.instrument.native_symbol: item
            for item in selected
            if item.feed.value == "BAR"
        }
        if set(self.trade_sources) != set(self.bar_sources) or not self.trade_sources:
            raise ValueError("stable DNSE trade/BAR symbols are inconsistent")
        self._queue: queue.Queue[tuple[dict[str, Any], int]] = queue.Queue(
            maxsize=queue_capacity
        )
        self._fatal = threading.Event()
        self._stopped = threading.Event()
        self._last_bar_open_ms: dict[str, int] = {}
        self._history_bootstrapped = False
        self._worker: threading.Thread | None = None

    def _binding(self, source) -> VnRawBinding:
        identity = source.instrument.identity
        return VnRawBinding(
            venue=identity.venue,
            market=identity.market,
            product_type=identity.product_type.value,
            native_symbol=source.instrument.native_symbol,
            subscription_id=source.source_id,
            source_session_id=self.session_id,
            connection_generation=1,
            lease_epoch=1,
            authority_revision=int(self.authority["revision"]),
            partition_plan_epoch=1,
            adapter_version=source.adapter_version,
            config_revision=self.acquisition.revision,
            instrument_catalog_revision=self.catalog.catalog_revision,
        )

    @staticmethod
    def _row_identity(row: dict[str, Any]) -> tuple[int, str]:
        try:
            open_time = int(row["t"])
            payload = {key: row[key] for key in ("t", "o", "h", "l", "c", "v")}
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("DNSE historical BAR row is malformed") from error
        return open_time, json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )

    def _closed_history(self, symbol: str) -> tuple[dict[str, Any], ...]:
        now_s = int(self.clock())
        start_s = now_s - self.history_lookback_days * 86_400
        last_error: Exception | None = None
        for attempt in range(self.history_attempts):
            try:
                rows = self.history_fetcher(symbol, "1", start_s, now_s)
                by_open: dict[int, tuple[str, dict[str, Any]]] = {}
                for value in rows:
                    row = dict(value)
                    open_time, digest = self._row_identity(row)
                    if open_time + 60 > now_s:
                        continue
                    previous = by_open.get(open_time)
                    if previous is not None and previous[0] != digest:
                        raise RuntimeError(
                            f"DNSE historical BAR conflict symbol={symbol} open={open_time}"
                        )
                    by_open[open_time] = digest, row
                closed = tuple(by_open[key][1] for key in sorted(by_open))
                if len(closed) < self.warmup_rows:
                    raise RuntimeError(
                        f"DNSE historical BAR coverage incomplete symbol={symbol} "
                        f"expected={self.warmup_rows} actual={len(closed)}"
                    )
                return closed[-self.warmup_rows :]
            except Exception as error:
                last_error = error
                if attempt + 1 < self.history_attempts:
                    self.sleep(min(2 ** attempt, 8))
        raise RuntimeError(
            f"DNSE historical BAR bootstrap exhausted symbol={symbol} "
            f"attempts={self.history_attempts}"
        ) from last_error

    def bootstrap_history(self) -> int:
        if self._history_bootstrapped:
            return 0
        published = 0
        for symbol, source in sorted(self.bar_sources.items()):
            rows = self._closed_history(symbol)
            received_at_ns = int(self.clock() * 1_000_000_000)
            envelopes = tuple(
                build_dnse_bar_raw_envelope(
                    row,
                    self._binding(source),
                    received_at_ns=received_at_ns + index,
                    test_provenance=False,
                )
                for index, row in enumerate(rows)
            )
            acknowledgements = self.publisher.publish_many(envelopes)
            if len(acknowledgements) != len(envelopes):
                raise RuntimeError("stable DNSE BAR bootstrap missed a Kafka ACK")
            self._last_bar_open_ms[symbol] = int(rows[-1]["t"]) * 1000
            published += len(acknowledgements)
            logger.info(
                "stable real-provider DNSE BAR bootstrap ACK symbol=%s rows=%s "
                "first_open_s=%s last_open_s=%s",
                symbol,
                len(rows),
                rows[0]["t"],
                rows[-1]["t"],
            )
        self._history_bootstrapped = True
        return published

    def _market_open(self, source) -> bool:
        calendar = trading_calendar_for_id(
            source.instrument.session_calendar_id
        )
        return calendar.is_open_ns(int(self.clock() * 1_000_000_000))

    def on_trade(self, trade) -> None:
        symbol = str(getattr(trade, "symbol", "") or "").upper()
        if symbol not in self.trade_sources or self._fatal.is_set():
            return
        delivery = {
            "symbol": symbol,
            "price": getattr(trade, "price", None),
            "quantity": getattr(trade, "quantity", None),
            "market_id": getattr(trade, "marketId", ""),
            "board_id": getattr(trade, "boardId", ""),
            "trading_session_id": getattr(trade, "tradingSessionId", ""),
            "total_volume_traded": getattr(trade, "totalVolumeTraded", None),
        }
        try:
            self._queue.put_nowait((delivery, time.time_ns()))
        except queue.Full:
            # Silent loss is forbidden. Fence this source and let supervision
            # restart from a fresh provider session after operator inspection.
            self._fatal.set()
            logger.critical(
                "stable DNSE queue exhausted; source fenced capacity=%s",
                self._queue.maxsize,
            )

    def _publish_worker(self) -> None:
        while not self._fatal.is_set() and (
            not self._stopped.is_set() or not self._queue.empty()
        ):
            try:
                first = self._queue.get(timeout=0.25)
            except queue.Empty:
                continue
            batch = [first]
            while len(batch) < 100:
                try:
                    batch.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            try:
                envelopes = tuple(
                    build_dnse_trade_raw_envelope(
                        delivery,
                        self._binding(self.trade_sources[str(delivery["symbol"]).upper()]),
                        received_at_ns=received_at_ns,
                        test_provenance=False,
                    )
                    for delivery, received_at_ns in batch
                )
                self.publisher.publish_many(envelopes)
            except Exception:
                self._fatal.set()
                logger.exception("stable DNSE durable raw ACK failed; source fenced")
                return

    def _poll_provider_rows(
        self, symbol: str, from_s: int, to_s: int
    ) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        for attempt in range(self.history_attempts):
            try:
                return list(self.history_fetcher(symbol, "1", from_s, to_s))
            except Exception as error:
                last_error = error
                if attempt + 1 < self.history_attempts:
                    self.sleep(min(2 ** attempt, 8))
        raise RuntimeError(
            f"DNSE live BAR poll exhausted symbol={symbol} "
            f"attempts={self.history_attempts}"
        ) from last_error

    async def poll_bars_once(self) -> int:
        now = int(self.clock())
        published = 0
        for symbol, source in self.bar_sources.items():
            if not self._market_open(source):
                continue
            try:
                rows = await asyncio.to_thread(
                    self._poll_provider_rows, symbol, now - 300, now
                )
                closed = [
                    row for row in rows
                    if int(row["t"]) * 1000 + 59_999 < now * 1000
                ]
                if not closed:
                    continue
                row = max(closed, key=lambda item: int(item["t"]))
                open_time_ms = int(row["t"]) * 1000
                if open_time_ms <= self._last_bar_open_ms.get(symbol, -1):
                    continue
                envelope = build_dnse_bar_raw_envelope(
                    row,
                    self._binding(source),
                    received_at_ns=int(self.clock() * 1_000_000_000),
                    test_provenance=False,
                )
                acknowledgements = await self.publisher.publish_many_async((envelope,))
                if len(acknowledgements) != 1:
                    raise RuntimeError("stable DNSE live BAR poll missed Kafka ACK")
                self._last_bar_open_ms[symbol] = open_time_ms
                published += 1
            except Exception:
                logger.exception("stable DNSE BAR poll failed symbol=%s", symbol)
        return published

    async def _poll_bars(self) -> None:
        while not self._stopped.is_set() and not self._fatal.is_set():
            await self.poll_bars_once()
            now = int(self.clock())
            delay = max(1.0, (now // 60 + 1) * 60 + 1 - self.clock())
            await asyncio.sleep(delay)

    async def run(self) -> None:
        if not DNSE_API_KEY or not DNSE_API_SECRET_KEY:
            raise RuntimeError("stable DNSE credentials are unavailable")
        await asyncio.to_thread(self.bootstrap_history)
        self._worker = threading.Thread(
            target=self._publish_worker,
            name="qdl-stable-dnse-kafka",
            daemon=True,
        )
        self._worker.start()
        client = TradingClient(
            api_key=DNSE_API_KEY,
            api_secret=DNSE_API_SECRET_KEY,
            base_url=DNSE_WS_BASE,
            encoding="msgpack",
            auto_reconnect=True,
            max_retries=10,
            heartbeat_interval=25.0,
        )
        await client.connect()
        symbols = sorted(self.trade_sources)
        await client.subscribe_trades(
            symbols=symbols, on_trade=self.on_trade, encoding="msgpack", board_id="G1"
        )
        await client.subscribe_trades(
            symbols=symbols, on_trade=self.on_trade, encoding="msgpack", board_id="G3"
        )
        bar_task = asyncio.create_task(self._poll_bars())
        try:
            while not self._stopped.is_set() and not self._fatal.is_set():
                if not client.is_healthy:
                    raise RuntimeError("stable DNSE SDK session is unhealthy")
                await asyncio.sleep(5)
            if self._fatal.is_set():
                raise RuntimeError("stable DNSE source fenced after loss/ACK failure")
        finally:
            self._stopped.set()
            bar_task.cancel()
            await asyncio.gather(bar_task, return_exceptions=True)
            await client.disconnect()
            if self._worker is not None:
                await asyncio.to_thread(self._worker.join, 2.0)
            self.publisher.close()

    def stop(self) -> None:
        self._stopped.set()


def build_from_environment() -> StableDnseVendorEdge:
    catalog = StableSourceCatalog.load(os.environ["QDL_STABLE_SOURCE_BINDINGS"])
    acquisition = StableAcquisitionPlan.load(
        os.environ["QDL_STABLE_ACQUISITION_BINDINGS"], catalog=catalog
    )
    authority = json.loads(
        (Path(os.environ["QDL_STABLE_RUNTIME_DIR"]) / "authority.json").read_text()
    )
    cert_root = Path(os.environ["QDL_KAFKA_CERT_ROOT"])
    publisher = KafkaRawPublisher(KafkaRawPublisherConfig(
        bootstrap_servers=os.environ["QDL_KAFKA_BOOTSTRAP_SERVERS"],
        client_id=os.environ["QDL_KAFKA_CLIENT_ID"],
        topic=acquisition.raw_topic,
        ca_path=cert_root / "ca.crt",
        certificate_path=cert_root / "client.crt",
        key_path=cert_root / "client.key",
    ))
    return StableDnseVendorEdge(
        catalog=catalog,
        acquisition=acquisition,
        authority=authority,
        publisher=publisher,
        queue_capacity=int(os.environ.get("QDL_STABLE_DNSE_QUEUE_CAPACITY", "5000")),
        warmup_rows=int(os.environ.get("QDL_STABLE_VN_WARMUP_ROWS", "500")),
        history_lookback_days=int(
            os.environ.get("QDL_STABLE_VN_HISTORY_LOOKBACK_DAYS", "30")
        ),
        history_attempts=int(os.environ.get("QDL_STABLE_VN_HISTORY_ATTEMPTS", "4")),
    )


async def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    edge = build_from_environment()
    loop = asyncio.get_running_loop()
    for signal_name in (__import__("signal").SIGTERM, __import__("signal").SIGINT):
        loop.add_signal_handler(signal_name, edge.stop)
    await edge.run()
    return 0
