from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import threading
import time
import uuid
from pathlib import Path

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
)
from qdl.transport.kafka_raw import KafkaRawPublisher, KafkaRawPublisherConfig


logger = logging.getLogger(__name__)


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
        clock=time.time,
    ) -> None:
        if not 1 <= warmup_rows <= 1000:
            raise ValueError("stable BAR warmup rows must be between 1 and 1000")
        self.catalog = catalog
        self.acquisition = acquisition
        self.authority = authority
        self.publisher = publisher
        self.warmup_rows = warmup_rows
        self.clock = clock
        run_id = uuid.uuid4()
        self.binance_session_id = f"qdl-v2-stable-binance-rest-{run_id}"
        self.okx_session_id = f"qdl-v2-stable-okx-rest-{run_id}"
        self._last_open_ms: dict[str, int] = {}
        self._history_bootstrapped = False
        self._stopped = threading.Event()

        source_by_id = {item.binding_id: item for item in catalog.bindings}
        pairs = tuple(
            (source_by_id[item.binding_id], item)
            for item in acquisition.bindings
        )
        self.bindings = tuple(
            pair
            for pair in pairs
            if pair[1].mode == "PYTHON_REST" and pair[1].runtime == "BINANCE"
        )
        self.okx_bindings = tuple(
            pair
            for pair in pairs
            if pair[1].runtime == "OKX" and pair[0].feed.value == "BAR"
        )
        if len(self.bindings) != 2 or len(self.okx_bindings) != 2:
            raise ValueError(
                "stable crypto BAR edge requires Binance and OKX Spot/derivative bindings"
            )
        if (
            authority.get("mode") != "RUST_SHADOW"
            or authority.get("public_write_allowed") is not False
            or authority.get("legacy_write_allowed") is not False
        ):
            raise ValueError("stable crypto BAR edge requires shadow authority")

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

    def bootstrap_history(self) -> int:
        if self._history_bootstrapped:
            return 0
        observed_ms = int(self.clock() * 1000)
        published = 0
        for source, acquisition in self.bindings:
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
        for source, acquisition in self.okx_bindings:
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
        self._history_bootstrapped = True
        logger.info(
            "stable multi-venue BAR bootstrap complete bindings=%s rows=%s",
            len(self.bindings) + len(self.okx_bindings),
            published,
        )
        return published

    def run_cycle(self) -> int:
        pending = []
        for source, _acquisition in self.bindings:
            envelope = fetch_latest_closed_bar_raw_envelope(
                self._binance_binding(source),
                attempts=4,
                test_provenance=False,
            )
            payload = json.loads(envelope.raw_frame_bytes)
            pending.append((source, envelope, int(payload["row"][0])))
        observed_ms = int(self.clock() * 1000)
        for source, _acquisition in self.okx_bindings:
            envelope = asyncio.run(fetch_okx_latest(
                self._okx_binding(source),
                now_ms=observed_ms,
                test_provenance=False,
            ))
            payload = json.loads(envelope.raw_frame_bytes)
            pending.append((source, envelope, int(payload["data"][0][0])))

        envelopes = []
        binding_ids = []
        for source, envelope, open_time_ms in pending:
            if open_time_ms <= self._last_open_ms.get(source.binding_id, -1):
                continue
            self._last_open_ms[source.binding_id] = open_time_ms
            envelopes.append(envelope)
            binding_ids.append(source.binding_id)
        if not envelopes:
            return 0
        acknowledgements = self.publisher.publish_many(envelopes)
        if len(acknowledgements) != len(envelopes):
            raise RuntimeError("stable latest-closed BAR cycle missed a Kafka ACK")
        logger.info(
            "stable multi-venue latest-closed BAR ACK count=%s bindings=%s",
            len(acknowledgements),
            ",".join(binding_ids),
        )
        return len(acknowledgements)

    def run_forever(self) -> None:
        failures = 0
        while not self._stopped.is_set():
            try:
                self.bootstrap_history()
                self.run_cycle()
                failures = 0
            except Exception:
                failures += 1
                logger.exception(
                    "stable crypto BAR cycle failed consecutive_failures=%s", failures
                )
            now = self.clock()
            next_boundary = (int(now) // 60 + 1) * 60 + 1
            delay = max(0.25, next_boundary - now)
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
