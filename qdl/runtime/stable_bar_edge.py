from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import threading
import time
from pathlib import Path

from qdl.adapters.intervals import canonical_interval_ms
from qdl.adapters.binance import (
    BinanceBarRawBinding,
    fetch_closed_bar_history_raw_envelopes as fetch_binance_history,
    fetch_latest_closed_bar_raw_envelope,
)
from qdl.adapters.okx.bar_edge import (
    OkxBarRawBinding,
    fetch_closed_bar_history_raw_envelopes as fetch_okx_history,
    fetch_latest_closed_bar_raw_envelope as fetch_okx_latest,
)
from qdl.runtime.stable_catalog import StableSourceBinding, StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionBinding,
    StableAcquisitionPlan,
    validate_shared_authority_record,
)
from qdl.transport.kafka_raw import KafkaRawPublisher, KafkaRawPublisherConfig


logger = logging.getLogger(__name__)


def _bar_interval_ms(interval: str) -> int:
    if not interval or interval[-1] not in {"s", "m", "h", "d"}:
        raise ValueError("stable BAR interval is unsupported")
    return canonical_interval_ms(interval)


class StableBinanceBarEdge:
    """Bounded real-provider BAR bootstrap plus Binance closed-bar polling.

    The class name is retained for the existing runtime entrypoint. Historical
    Binance and OKX rows always enter raw Kafka and the Rust canonical core;
    this edge never writes query cache or Redis state directly.
    """

    def __init__(
        self,
        *,
        catalog: StableSourceCatalog,
        acquisition: StableAcquisitionPlan,
        authority: dict,
        publisher: KafkaRawPublisher,
        warmup_rows: int = 500,
        max_catchup_rows: int = 1000,
        settlement_delay_seconds: float = 10.0,
        state_path: str | Path | None = None,
        clock=time.time,
    ) -> None:
        if not 1 <= warmup_rows <= 1000:
            raise ValueError("stable BAR warmup rows must be between 1 and 1000")
        if not 1 <= max_catchup_rows <= 1000:
            raise ValueError("stable BAR catch-up rows must be between 1 and 1000")
        if not 1.0 <= settlement_delay_seconds <= 10.0:
            raise ValueError("stable BAR settlement delay must be between 1 and 10 seconds")
        self.catalog = catalog
        self.acquisition = acquisition
        self.authority = authority
        self.publisher = publisher
        self.warmup_rows = warmup_rows
        self.max_catchup_rows = max_catchup_rows
        self.settlement_delay_seconds = settlement_delay_seconds
        self.state_path = Path(state_path) if state_path is not None else None
        self.clock = clock
        authority_revision = int(authority.get("revision", 0))
        self.binance_session_id = (
            f"qdl-v2-stable-binance-rest-r{authority_revision}"
        )
        self.okx_session_id = f"qdl-v2-stable-okx-rest-r{authority_revision}"
        self._last_open_ms: dict[str, int] = {}
        self._history_bootstrapped = False
        self._stopped = threading.Event()

        source_by_id = {item.binding_id: item for item in catalog.bindings}
        pairs = tuple(
            (source_by_id[item.binding_id], item)
            for item in acquisition.bindings
        )
        # Every enabled Binance/OKX final-BAR demand needs a bounded history
        # bootstrap before it can satisfy a durable warmup. Recurring polling
        # is limited to explicit PYTHON_REST bindings; the execution-grade
        # crypto policy assigns every enabled final BAR to that recovery owner.
        # A future native BAR mode must not bypass this edge without separate
        # finality and reconciliation certification.
        self.history_bindings = tuple(
            pair
            for pair in pairs
            if (
                pair[1].enabled
                and pair[1].runtime == "BINANCE"
                and pair[0].feed.value == "BAR"
            )
        )
        self.history_okx_bindings = tuple(
            pair
            for pair in pairs
            if (
                pair[1].enabled
                and pair[1].runtime == "OKX"
                and pair[0].feed.value == "BAR"
            )
        )
        # Only explicitly configured REST bindings are polled after bootstrap.
        # Native websocket BARs stay owned by their Rust acquisition lane.
        self.bindings = tuple(
            pair for pair in self.history_bindings if pair[1].mode == "PYTHON_REST"
        )
        self.okx_bindings = tuple(
            pair for pair in self.history_okx_bindings if pair[1].mode == "PYTHON_REST"
        )
        # Demand decides which venues and markets exist, so this edge must not
        # assert a fixed market-family set. It owns every enabled crypto BAR
        # bootstrap and only the explicit REST subset for recurring polling.
        expected_history = {
            source.binding_id
            for source, acquisition in pairs
            if acquisition.enabled
            and acquisition.runtime in {"BINANCE", "OKX"}
            and source.feed.value == "BAR"
        }
        history_owned = {
            source.binding_id
            for source, _acquisition in self.history_bindings + self.history_okx_bindings
        }
        if history_owned != expected_history:
            raise ValueError(
                "stable crypto BAR edge does not bootstrap every configured BAR "
                "binding: " + ",".join(sorted(expected_history - history_owned))
            )
        validate_shared_authority_record(authority)
        self._history_bootstrap_active = bool(expected_history)
        self._rest_fallback_active = bool(self.bindings or self.okx_bindings)
        if self._history_bootstrap_active:
            self._restore_state()
        else:
            self._history_bootstrapped = True

    @property
    def _binding_ids(self) -> tuple[str, ...]:
        return tuple(sorted(
            source.binding_id
            for source, _acquisition in self.history_bindings + self.history_okx_bindings
        ))

    def _state_payload(self) -> dict:
        return {
            "schema": "qdl.stable-bar-edge-state.v2",
            "slice_id": str(self.authority.get("slice_id", "")),
            "authority_revision": int(self.authority["revision"]),
            "catalog_revision": int(self.catalog.catalog_revision),
            "acquisition_revision": int(self.acquisition.revision),
            "warmup_rows": self.warmup_rows,
            "binding_ids": list(self._binding_ids),
            "last_open_ms": {
                key: self._last_open_ms[key] for key in sorted(self._last_open_ms)
            },
        }

    def _restore_state(self) -> None:
        if self.state_path is None or not self.state_path.exists():
            return
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise RuntimeError("stable BAR checkpoint is unreadable") from error
        if not isinstance(payload, dict) or set(payload) != {
            "schema", "slice_id", "authority_revision", "catalog_revision",
            "acquisition_revision", "warmup_rows", "binding_ids", "last_open_ms",
        }:
            raise RuntimeError("stable BAR checkpoint fields are invalid")
        expected = self._state_payload()
        for field in (
            "schema", "slice_id", "authority_revision", "catalog_revision",
            "acquisition_revision", "warmup_rows", "binding_ids",
        ):
            if payload[field] != expected[field]:
                raise RuntimeError(
                    f"stable BAR checkpoint {field} differs from runtime authority"
                )
        last_open_ms = payload["last_open_ms"]
        if not isinstance(last_open_ms, dict) or not set(last_open_ms).issubset(
            self._binding_ids
        ):
            raise RuntimeError("stable BAR checkpoint binding watermarks are invalid")
        restored: dict[str, int] = {}
        sources = {
            source.binding_id: source
            for source, _acquisition in self.history_bindings + self.history_okx_bindings
        }
        for binding_id, value in last_open_ms.items():
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
                or value % _bar_interval_ms(sources[binding_id].interval or "")
            ):
                raise RuntimeError("stable BAR checkpoint watermark is invalid")
            restored[binding_id] = value
        self._last_open_ms = restored
        self._history_bootstrapped = set(restored) == set(self._binding_ids)
        logger.info(
            "stable BAR checkpoint restored bindings=%s complete=%s",
            len(restored),
            self._history_bootstrapped,
        )

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
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                0o600,
            )
            pending = memoryview(encoded)
            while pending:
                written = os.write(descriptor, pending)
                if written <= 0:
                    raise OSError("stable BAR checkpoint write made no progress")
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

    def _binance_binding(
        self,
        source: StableSourceBinding,
    ) -> BinanceBarRawBinding:
        identity = source.instrument.identity
        return BinanceBarRawBinding(
            market=identity.market,
            product_type=identity.product_type.value,
            native_symbol=source.instrument.native_symbol,
            interval=source.interval or "",
            subscription_id=source.source_id,
            source_session_id=self.binance_session_id,
            connection_generation=1,
            lease_epoch=1,
            authority_revision=int(self.authority["revision"]),
            partition_plan_epoch=1,
            adapter_version=source.adapter_version,
            config_revision=self.acquisition.revision,
            instrument_catalog_revision=self.catalog.catalog_revision,
        )

    def _okx_binding(
        self,
        source: StableSourceBinding,
    ) -> OkxBarRawBinding:
        identity = source.instrument.identity
        return OkxBarRawBinding(
            market=identity.market,
            product_type=identity.product_type.value,
            native_symbol=source.instrument.native_symbol,
            interval=source.interval or "",
            subscription_id=source.source_id,
            source_session_id=self.okx_session_id,
            connection_generation=1,
            lease_epoch=1,
            authority_revision=int(self.authority["revision"]),
            partition_plan_epoch=1,
            adapter_version=source.adapter_version,
            config_revision=self.acquisition.revision,
            instrument_catalog_revision=self.catalog.catalog_revision,
        )

    def _publish_history(
        self,
        source: StableSourceBinding,
        acquisition: StableAcquisitionBinding,
        envelopes,
    ) -> int:
        values = tuple(envelopes)
        if len(values) != self.warmup_rows:
            raise RuntimeError(
                f"stable BAR bootstrap coverage mismatch binding={source.binding_id} "
                f"expected={self.warmup_rows} actual={len(values)}"
            )
        acknowledgements = self.publisher.publish_many(values)
        if len(acknowledgements) != len(values):
            raise RuntimeError("stable BAR bootstrap did not receive every Kafka ACK")
        payloads = [json.loads(item.raw_frame_bytes) for item in values]
        opens = [
            int(
                payload["row"][0]
                if acquisition.runtime == "BINANCE"
                else payload["data"][0][0]
            )
            for payload in payloads
        ]
        self._last_open_ms[source.binding_id] = max(opens)
        self._persist_state()
        logger.info(
            "stable real-provider BAR bootstrap ACK binding=%s venue=%s rows=%s "
            "first_open_ms=%s last_open_ms=%s",
            source.binding_id,
            acquisition.runtime,
            len(values),
            min(opens),
            max(opens),
        )
        return len(acknowledgements)

    def _settled_observed_ms(self) -> int:
        return int(
            self.clock() * 1000 - self.settlement_delay_seconds * 1000
        )

    def bootstrap_history(self) -> int:
        if not self._history_bootstrap_active:
            return 0
        if self._history_bootstrapped:
            return 0
        observed_ms = self._settled_observed_ms()
        published = 0
        for source, acquisition in self.history_bindings:
            if source.binding_id in self._last_open_ms:
                continue
            published += self._publish_history(
                source,
                acquisition,
                fetch_binance_history(
                    self._binance_binding(source),
                    limit=self.warmup_rows,
                    now_ms=observed_ms,
                    attempts=4,
                    test_provenance=False,
                ),
            )
        for source, acquisition in self.history_okx_bindings:
            if source.binding_id in self._last_open_ms:
                continue
            published += self._publish_history(
                source,
                acquisition,
                asyncio.run(fetch_okx_history(
                    self._okx_binding(source),
                    limit=self.warmup_rows,
                    now_ms=observed_ms,
                    test_provenance=False,
                )),
            )
        self._history_bootstrapped = (
            set(self._last_open_ms) == set(self._binding_ids)
        )
        if not self._history_bootstrapped:
            raise RuntimeError("stable BAR bootstrap did not checkpoint every binding")
        logger.info(
            "stable multi-venue BAR bootstrap complete bindings=%s rows=%s",
            len(self.history_bindings) + len(self.history_okx_bindings),
            published,
        )
        return published

    @staticmethod
    def _open_time_ms(
        acquisition: StableAcquisitionBinding,
        envelope,
    ) -> int:
        payload = json.loads(envelope.raw_frame_bytes)
        if acquisition.runtime == "BINANCE":
            return int(payload["row"][0])
        if acquisition.runtime == "OKX":
            return int(payload["data"][0][0])
        raise ValueError("stable crypto BAR runtime is unsupported")

    def _pending_for_binding(
        self,
        source: StableSourceBinding,
        acquisition: StableAcquisitionBinding,
        latest,
        *,
        observed_ms: int,
    ) -> tuple[tuple[object, int], ...]:
        latest_open_ms = self._open_time_ms(acquisition, latest)
        previous_open_ms = self._last_open_ms.get(source.binding_id)
        if previous_open_ms is None:
            return ((latest, latest_open_ms),)
        if latest_open_ms < previous_open_ms:
            raise RuntimeError(
                f"stable BAR provider latest precedes durable watermark "
                f"binding={source.binding_id} previous={previous_open_ms} "
                f"latest={latest_open_ms}"
            )
        if latest_open_ms == previous_open_ms:
            return ()

        interval_ms = _bar_interval_ms(source.interval or "")
        elapsed_ms = latest_open_ms - previous_open_ms
        if elapsed_ms % interval_ms:
            raise RuntimeError(
                f"stable BAR provider boundary mismatch binding={source.binding_id} "
                f"previous={previous_open_ms} latest={latest_open_ms}"
            )
        pending_rows = elapsed_ms // interval_ms
        if pending_rows > self.max_catchup_rows:
            raise RuntimeError(
                f"stable BAR catch-up exceeds bound binding={source.binding_id} "
                f"required={pending_rows} max={self.max_catchup_rows}"
            )
        if pending_rows == 1:
            values = (latest,)
        elif acquisition.runtime == "BINANCE":
            values = fetch_binance_history(
                self._binance_binding(source),
                limit=pending_rows,
                now_ms=observed_ms,
                attempts=4,
                test_provenance=False,
            )
        else:
            values = asyncio.run(fetch_okx_history(
                self._okx_binding(source),
                limit=pending_rows,
                now_ms=observed_ms,
                test_provenance=False,
            ))

        opens = tuple(self._open_time_ms(acquisition, item) for item in values)
        expected = tuple(
            range(previous_open_ms + interval_ms, latest_open_ms + 1, interval_ms)
        )
        if opens != expected:
            raise RuntimeError(
                f"stable BAR catch-up is not contiguous binding={source.binding_id} "
                f"expected_rows={len(expected)} observed_rows={len(opens)}"
            )
        return tuple(zip(values, opens, strict=True))

    def run_cycle(self) -> int:
        if not self._rest_fallback_active:
            return 0
        observed_ms = self._settled_observed_ms()
        latest = []
        for source, acquisition in self.bindings:
            envelope = fetch_latest_closed_bar_raw_envelope(
                self._binance_binding(source),
                now_ms=observed_ms,
                attempts=4,
                test_provenance=False,
            )
            latest.append((source, acquisition, envelope))
        for source, acquisition in self.okx_bindings:
            envelope = asyncio.run(fetch_okx_latest(
                self._okx_binding(source),
                now_ms=observed_ms,
                test_provenance=False,
            ))
            latest.append((source, acquisition, envelope))

        pending = []
        for source, acquisition, envelope in latest:
            pending.extend(
                (source, item, open_time_ms)
                for item, open_time_ms in self._pending_for_binding(
                    source, acquisition, envelope, observed_ms=observed_ms
                )
            )
        if not pending:
            return 0

        acknowledgements = self.publisher.publish_many(
            item for _source, item, _open_time_ms in pending
        )
        if len(acknowledgements) != len(pending):
            raise RuntimeError("stable latest-closed BAR cycle missed a Kafka ACK")

        acknowledged_opens: dict[str, int] = {}
        for source, _item, open_time_ms in pending:
            acknowledged_opens[source.binding_id] = max(
                open_time_ms, acknowledged_opens.get(source.binding_id, -1)
            )
        self._last_open_ms.update(acknowledged_opens)
        self._persist_state()
        logger.info(
            "stable multi-venue closed BAR ACK count=%s bindings=%s",
            len(acknowledgements),
            ",".join(sorted(acknowledged_opens)),
        )
        return len(acknowledgements)

    def run_forever(self) -> None:
        if not self._history_bootstrap_active:
            logger.info("stable crypto BAR edge idle; no enabled crypto BAR demand")
            while not self._stopped.wait(60.0):
                pass
            return
        failures = 0
        while not self._stopped.is_set():
            now = self.clock()
            ready_at = (
                (int(now) // 60) * 60 + self.settlement_delay_seconds
            )
            if not self._history_bootstrapped and now < ready_at:
                self._stopped.wait(max(0.01, ready_at - now))
                continue
            try:
                self.bootstrap_history()
                if self._rest_fallback_active:
                    self.run_cycle()
                failures = 0
            except Exception:
                failures += 1
                logger.exception(
                    "stable crypto BAR cycle failed consecutive_failures=%s", failures
                )
            now = self.clock()
            if self._rest_fallback_active:
                next_boundary = (
                    (int(now) // 60 + 1) * 60 + self.settlement_delay_seconds
                )
                delay = max(0.25, next_boundary - now)
            else:
                delay = 60.0
            if failures:
                delay = min(delay, min(2 ** min(failures, 6), 30))
            self._stopped.wait(delay)

    def stop(self, *_args) -> None:
        self._stopped.set()
        self.publisher.close()


def build_from_environment() -> StableBinanceBarEdge:
    catalog = StableSourceCatalog.load(os.environ["QDL_STABLE_SOURCE_BINDINGS"])
    acquisition = StableAcquisitionPlan.load(
        os.environ["QDL_STABLE_ACQUISITION_BINDINGS"], catalog=catalog
    )
    runtime_dir = Path(os.environ["QDL_STABLE_RUNTIME_DIR"])
    authority = json.loads((runtime_dir / "authority.json").read_text(encoding="utf-8"))
    cert_root = Path(os.environ["QDL_KAFKA_CERT_ROOT"])
    publisher = KafkaRawPublisher(KafkaRawPublisherConfig(
        bootstrap_servers=os.environ["QDL_KAFKA_BOOTSTRAP_SERVERS"],
        client_id=os.environ["QDL_KAFKA_CLIENT_ID"],
        topic=acquisition.raw_topic,
        ca_path=cert_root / "ca.crt",
        certificate_path=cert_root / "client.crt",
        key_path=cert_root / "client.key",
    ))
    return StableBinanceBarEdge(
        catalog=catalog,
        acquisition=acquisition,
        authority=authority,
        publisher=publisher,
        warmup_rows=int(os.environ.get("QDL_STABLE_BAR_WARMUP_ROWS", "500")),
        max_catchup_rows=int(
            os.environ.get("QDL_STABLE_BAR_MAX_CATCHUP_ROWS", "1000")
        ),
        settlement_delay_seconds=float(
            os.environ.get("QDL_STABLE_BAR_SETTLEMENT_DELAY_SECONDS", "10")
        ),
        state_path=os.environ.get(
            "QDL_STABLE_BAR_STATE_PATH",
            str(
                Path(
                    os.environ.get(
                        "QDL_STABLE_STATE_DIR",
                        "/var/lib/qdl-stable/runtime",
                    )
                )
                / "stable-crypto-bar-edge.json"
            ),
        ),
    )


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    edge = build_from_environment()
    signal.signal(signal.SIGTERM, edge.stop)
    signal.signal(signal.SIGINT, edge.stop)
    try:
        edge.run_forever()
    finally:
        if not edge._stopped.is_set():
            edge.stop()
    return 0
