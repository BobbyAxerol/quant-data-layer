from __future__ import annotations

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
    fetch_latest_closed_bar_raw_envelope,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl.transport.kafka_raw import KafkaRawPublisher, KafkaRawPublisherConfig


logger = logging.getLogger(__name__)


class StableBinanceBarEdge:
    """Low-rate latest-closed BAR acquisition; Rust remains canonical semantics."""

    def __init__(
        self,
        *,
        catalog: StableSourceCatalog,
        acquisition: StableAcquisitionPlan,
        authority: dict,
        publisher: KafkaRawPublisher,
        clock=time.time,
    ) -> None:
        self.catalog = catalog
        self.acquisition = acquisition
        self.authority = authority
        self.publisher = publisher
        self.clock = clock
        self.session_id = f"qdl-v2-stable-binance-rest-{uuid.uuid4()}"
        self._last_open_ms: dict[str, int] = {}
        self._stopped = threading.Event()
        source_by_id = {item.binding_id: item for item in catalog.bindings}
        self.bindings = tuple(
            (
                source_by_id[item.binding_id],
                item,
            )
            for item in acquisition.bindings
            if item.mode == "PYTHON_REST" and item.runtime == "BINANCE"
        )
        if len(self.bindings) != 2:
            raise ValueError("stable Binance BAR edge requires USDM and Spot bindings")
        if (
            authority.get("mode") != "RUST_SHADOW"
            or authority.get("public_write_allowed") is not False
            or authority.get("legacy_write_allowed") is not False
        ):
            raise ValueError("stable Binance BAR edge requires shadow authority")

    def run_cycle(self) -> int:
        envelopes = []
        for source, _acquisition in self.bindings:
            identity = source.instrument.identity
            envelope = fetch_latest_closed_bar_raw_envelope(
                BinanceBarRawBinding(
                    market=identity.market,
                    product_type=identity.product_type.value,
                    native_symbol=source.instrument.native_symbol,
                    interval=source.interval or "",
                    subscription_id=source.source_id,
                    source_session_id=self.session_id,
                    connection_generation=1,
                    lease_epoch=1,
                    authority_revision=int(self.authority["revision"]),
                    partition_plan_epoch=1,
                    adapter_version=source.adapter_version,
                    config_revision=self.acquisition.revision,
                    instrument_catalog_revision=self.catalog.catalog_revision,
                ),
                attempts=4,
                test_provenance=False,
            )
            payload = json.loads(envelope.raw_frame_bytes)
            open_time_ms = int(payload["row"][0])
            if open_time_ms <= self._last_open_ms.get(source.binding_id, -1):
                continue
            self._last_open_ms[source.binding_id] = open_time_ms
            envelopes.append(envelope)
        if not envelopes:
            return 0
        acknowledgements = self.publisher.publish_many(envelopes)
        logger.info(
            "stable Binance latest-closed BAR ACK count=%s bindings=%s",
            len(acknowledgements),
            ",".join(source.binding_id for source, _ in self.bindings),
        )
        return len(acknowledgements)

    def run_forever(self) -> None:
        failures = 0
        while not self._stopped.is_set():
            try:
                self.run_cycle()
                failures = 0
            except Exception:
                failures += 1
                logger.exception(
                    "stable Binance BAR cycle failed consecutive_failures=%s", failures
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
