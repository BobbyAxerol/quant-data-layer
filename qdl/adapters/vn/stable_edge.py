from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import queue
import threading
import time
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from app.config import DNSE_API_KEY, DNSE_API_SECRET_KEY, DNSE_WS_BASE
from app.providers.dnse import fetch_dnse_ohlc_raw
from app.stream.dnse_ws import TradingClient
from qdl.adapters.vn import (
    VnRawBinding,
    build_dnse_bar_raw_envelope,
    build_dnse_trade_raw_envelope,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    validate_shared_authority_record,
)
from qdl.transport.kafka_raw import KafkaRawPublisher, KafkaRawPublisherConfig


logger = logging.getLogger(__name__)


class StableDnseVendorEdge:
    """DNSE vendor acquisition with lossless Kafka ACK and atomic BAR recovery."""

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
        history_attempts: int = 1,
        history_fetcher=fetch_dnse_ohlc_raw,
        state_path: str | Path | None = None,
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
        validate_shared_authority_record(authority)
        self.catalog = catalog
        self.acquisition = acquisition
        self.authority = authority
        self.publisher = publisher
        self.warmup_rows = warmup_rows
        self.history_lookback_days = history_lookback_days
        self.history_attempts = history_attempts
        self.history_fetcher = history_fetcher
        self.state_path = Path(state_path) if state_path is not None else None
        self.clock = clock
        self.sleep = sleep
        self.session_id = (
            f"qdl-v2-stable-dnse-r{int(authority['revision'])}-{uuid.uuid4()}"
        )
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
        if any(source.interval != "1m" for source in self.bar_sources.values()):
            raise ValueError("stable DNSE edge requires native final 1m BAR bindings")

        self._queue: queue.Queue[tuple[str, dict[str, Any], int]] = queue.Queue(
            maxsize=queue_capacity
        )
        self._fatal = threading.Event()
        self._stopped = threading.Event()
        self._state_lock = threading.Lock()
        self._last_bar: dict[str, tuple[int, str]] = {}
        self._observed_bar: dict[str, tuple[int, str]] = {}
        self._history_bootstrapped = False
        self._worker: threading.Thread | None = None
        self._restore_state()

    @property
    def _bar_binding_ids(self) -> tuple[str, ...]:
        return tuple(sorted(source.binding_id for source in self.bar_sources.values()))

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
    def _decimal_identity(value: Any, field: str, *, allow_zero: bool) -> str:
        if value is None or isinstance(value, bool):
            raise ValueError(f"DNSE BAR {field} is missing")
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, ValueError) as error:
            raise ValueError(f"DNSE BAR {field} is invalid") from error
        if not parsed.is_finite() or parsed < 0 or (not allow_zero and parsed == 0):
            raise ValueError(f"DNSE BAR {field} is outside domain")
        return format(parsed.normalize(), "f")

    @classmethod
    def _row_identity(cls, row: dict[str, Any]) -> tuple[int, str]:
        try:
            open_time = int(row["t"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("DNSE BAR timestamp is malformed") from error
        if open_time <= 0 or open_time % 60:
            raise ValueError("DNSE BAR timestamp is not aligned to native 1m")
        prices = {
            key: cls._decimal_identity(row.get(key), key, allow_zero=False)
            for key in ("o", "h", "l", "c")
        }
        decimals = {key: Decimal(value) for key, value in prices.items()}
        if (
            decimals["h"] < max(decimals["o"], decimals["c"], decimals["l"])
            or decimals["l"] > min(decimals["o"], decimals["c"], decimals["h"])
        ):
            raise ValueError("DNSE BAR price invariants failed")
        payload = {
            "t": open_time,
            **prices,
            "v": cls._decimal_identity(row.get("v"), "v", allow_zero=True),
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        return open_time, hashlib.sha256(encoded.encode("ascii")).hexdigest()

    def _state_payload(self) -> dict[str, Any]:
        return {
            "schema": "qdl.stable-dnse-edge-state.v1",
            "slice_id": str(self.authority.get("slice_id", "")),
            "authority_revision": int(self.authority["revision"]),
            "catalog_revision": int(self.catalog.catalog_revision),
            "acquisition_revision": int(self.acquisition.revision),
            "binding_ids": list(self._bar_binding_ids),
            "last_bar": {
                binding_id: {
                    "open_time_ms": value[0],
                    "payload_sha256": value[1],
                }
                for binding_id, value in sorted(self._last_bar.items())
            },
        }

    def _restore_state(self) -> None:
        if self.state_path is None or not self.state_path.exists():
            return
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise RuntimeError("stable DNSE checkpoint is unreadable") from error
        if not isinstance(payload, dict) or set(payload) != {
            "schema",
            "slice_id",
            "authority_revision",
            "catalog_revision",
            "acquisition_revision",
            "binding_ids",
            "last_bar",
        }:
            raise RuntimeError("stable DNSE checkpoint fields are invalid")
        expected = self._state_payload()
        for field in (
            "schema",
            "slice_id",
            "authority_revision",
            "catalog_revision",
            "acquisition_revision",
            "binding_ids",
        ):
            if payload[field] != expected[field]:
                raise RuntimeError(
                    f"stable DNSE checkpoint {field} differs from runtime authority"
                )
        values = payload["last_bar"]
        if not isinstance(values, dict) or set(values) != set(self._bar_binding_ids):
            raise RuntimeError("stable DNSE checkpoint is partial")
        restored: dict[str, tuple[int, str]] = {}
        for binding_id, item in values.items():
            if not isinstance(item, dict) or set(item) != {
                "open_time_ms", "payload_sha256"
            }:
                raise RuntimeError("stable DNSE checkpoint watermark is invalid")
            open_time_ms = item["open_time_ms"]
            digest = item["payload_sha256"]
            if (
                isinstance(open_time_ms, bool)
                or not isinstance(open_time_ms, int)
                or open_time_ms <= 0
                or open_time_ms % 60_000
                or not isinstance(digest, str)
                or len(digest) != 64
                or any(char not in "0123456789abcdef" for char in digest)
            ):
                raise RuntimeError("stable DNSE checkpoint watermark is invalid")
            restored[binding_id] = open_time_ms, digest
        self._last_bar = restored
        self._observed_bar = dict(restored)
        self._history_bootstrapped = True
        logger.info("stable DNSE checkpoint restored bindings=%s", len(restored))

    def _persist_state(self) -> None:
        if self.state_path is None:
            return
        parent = self.state_path.parent
        parent.mkdir(parents=True, exist_ok=True)
        temporary = parent / f".{self.state_path.name}.{os.getpid()}.tmp"
        encoded = (
            json.dumps(
                self._state_payload(),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            )
            + "\n"
        ).encode("ascii")
        descriptor = None
        try:
            descriptor = os.open(
                temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600
            )
            pending = memoryview(encoded)
            while pending:
                written = os.write(descriptor, pending)
                if written <= 0:
                    raise OSError("stable DNSE checkpoint write made no progress")
                pending = pending[written:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            os.replace(temporary, self.state_path)
            directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)

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
        pending: list[tuple[str, dict[str, Any], Any]] = []
        for symbol, source in sorted(self.bar_sources.items()):
            for row in self._closed_history(symbol):
                pending.append((symbol, row, source))
        received_at_ns = int(self.clock() * 1_000_000_000)
        envelopes = tuple(
            build_dnse_bar_raw_envelope(
                row,
                self._binding(source),
                received_at_ns=received_at_ns + index,
                acquisition_origin="REST_HISTORY",
                test_provenance=False,
            )
            for index, (_symbol, row, source) in enumerate(pending)
        )
        acknowledgements = self.publisher.publish_many(envelopes)
        if len(acknowledgements) != len(envelopes):
            raise RuntimeError("stable DNSE BAR bootstrap missed a Kafka ACK")
        last_bar: dict[str, tuple[int, str]] = {}
        for _symbol, row, source in pending:
            open_time, digest = self._row_identity(row)
            last_bar[source.binding_id] = open_time * 1000, digest
        if set(last_bar) != set(self._bar_binding_ids):
            raise RuntimeError("stable DNSE BAR bootstrap did not cover every binding")
        with self._state_lock:
            self._last_bar = last_bar
            self._observed_bar = dict(last_bar)
            self._persist_state()
            self._history_bootstrapped = True
        logger.info(
            "stable real-provider DNSE BAR bootstrap ACK bindings=%s rows=%s",
            len(last_bar),
            len(envelopes),
        )
        return len(acknowledgements)

    def _enqueue(self, kind: str, delivery: dict[str, Any], received_at_ns: int) -> None:
        try:
            self._queue.put_nowait((kind, delivery, received_at_ns))
        except queue.Full:
            self._fatal.set()
            logger.critical(
                "stable DNSE queue exhausted; source fenced capacity=%s",
                self._queue.maxsize,
            )

    def on_trade(self, trade) -> None:
        symbol = str(getattr(trade, "symbol", "") or "").upper()
        if symbol not in self.trade_sources or self._fatal.is_set():
            return
        self._enqueue(
            "TRADE",
            {
                "symbol": symbol,
                "price": getattr(trade, "price", None),
                "quantity": getattr(trade, "quantity", None),
                "market_id": getattr(trade, "marketId", ""),
                "board_id": getattr(trade, "boardId", ""),
                "trading_session_id": getattr(trade, "tradingSessionId", ""),
                "total_volume_traded": getattr(trade, "totalVolumeTraded", None),
            },
            int(self.clock() * 1_000_000_000),
        )

    @staticmethod
    def _provider_seconds(value: Any) -> int:
        if isinstance(value, bool):
            raise ValueError("DNSE closed BAR timestamp is invalid")
        try:
            timestamp = int(value)
        except (TypeError, ValueError) as error:
            raise ValueError("DNSE closed BAR timestamp is invalid") from error
        if timestamp >= 1_000_000_000_000:
            if timestamp % 1000:
                raise ValueError("DNSE closed BAR millisecond timestamp loses precision")
            timestamp //= 1000
        if timestamp <= 0:
            raise ValueError("DNSE closed BAR timestamp is invalid")
        return timestamp

    def on_ohlc_closed(self, ohlc) -> None:
        symbol = str(getattr(ohlc, "symbol", "") or "").upper()
        if symbol not in self.bar_sources or self._fatal.is_set():
            return
        try:
            resolution = str(getattr(ohlc, "resolution", "") or "")
            if resolution != "1":
                raise ValueError("DNSE closed BAR resolution differs from 1m binding")
            row = {
                "t": self._provider_seconds(getattr(ohlc, "time", None)),
                "o": getattr(ohlc, "open", None),
                "h": getattr(ohlc, "high", None),
                "l": getattr(ohlc, "low", None),
                "c": getattr(ohlc, "close", None),
                "v": getattr(ohlc, "volume", None),
            }
            open_time, digest = self._row_identity(row)
            if open_time + 60 > int(self.clock()) + 2:
                raise ValueError("DNSE closed BAR is not closed at receipt time")
            source = self.bar_sources[symbol]
            with self._state_lock:
                previous = self._observed_bar.get(source.binding_id)
                if previous is not None and open_time * 1000 < previous[0]:
                    return
                if previous is not None and open_time * 1000 == previous[0]:
                    if digest != previous[1]:
                        raise RuntimeError(
                            f"DNSE closed BAR conflict symbol={symbol} open={open_time}"
                        )
                    return
                self._observed_bar[source.binding_id] = open_time * 1000, digest
            self._enqueue(
                "BAR",
                {"symbol": symbol, "row": row, "digest": digest},
                int(self.clock() * 1_000_000_000),
            )
        except Exception:
            self._fatal.set()
            logger.exception("stable DNSE closed BAR invalid; source fenced symbol=%s", symbol)

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
                envelopes = []
                bar_updates: dict[str, tuple[int, str]] = {}
                for kind, delivery, received_at_ns in batch:
                    symbol = str(delivery["symbol"]).upper()
                    if kind == "TRADE":
                        envelopes.append(build_dnse_trade_raw_envelope(
                            delivery,
                            self._binding(self.trade_sources[symbol]),
                            received_at_ns=received_at_ns,
                            test_provenance=False,
                        ))
                    elif kind == "BAR":
                        source = self.bar_sources[symbol]
                        row = delivery["row"]
                        envelopes.append(build_dnse_bar_raw_envelope(
                            row,
                            self._binding(source),
                            received_at_ns=received_at_ns,
                            acquisition_origin="WEBSOCKET_CLOSED",
                            test_provenance=False,
                        ))
                        bar_updates[source.binding_id] = (
                            int(row["t"]) * 1000,
                            str(delivery["digest"]),
                        )
                    else:
                        raise RuntimeError("stable DNSE queue event kind is invalid")
                acknowledgements = self.publisher.publish_many(tuple(envelopes))
                if len(acknowledgements) != len(envelopes):
                    raise RuntimeError("stable DNSE raw batch missed a Kafka ACK")
                if bar_updates:
                    with self._state_lock:
                        self._last_bar.update(bar_updates)
                        if set(self._last_bar) == set(self._bar_binding_ids):
                            self._persist_state()
            except Exception:
                self._fatal.set()
                logger.exception("stable DNSE durable raw ACK failed; source fenced")
            finally:
                for _item in batch:
                    self._queue.task_done()

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
            dispatch_queue_capacity=max(100, self._queue.maxsize // 6),
        )
        await client.connect()
        symbols = sorted(self.trade_sources)
        await client.subscribe_trades(
            symbols=symbols,
            on_trade=self.on_trade,
            encoding="msgpack",
            board_id="G1",
        )
        await client.subscribe_trades(
            symbols=symbols,
            on_trade=self.on_trade,
            encoding="msgpack",
            board_id="G3",
        )
        await client.subscribe_ohlc_closed(
            symbols=symbols,
            resolution="1",
            on_ohlc=self.on_ohlc_closed,
            encoding="msgpack",
        )
        try:
            while not self._stopped.is_set() and not self._fatal.is_set():
                if not client.is_healthy:
                    raise RuntimeError("stable DNSE SDK session is unhealthy")
                await asyncio.sleep(5)
            if self._fatal.is_set():
                raise RuntimeError("stable DNSE source fenced after loss/ACK failure")
        finally:
            self._stopped.set()
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
        history_attempts=int(os.environ.get("QDL_STABLE_VN_HISTORY_ATTEMPTS", "1")),
        state_path=os.environ.get(
            "QDL_STABLE_DNSE_STATE_PATH",
            "/var/lib/qdl-stable/runtime/stable-dnse-edge.json",
        ),
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
