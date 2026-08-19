from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass

from qdl.adapters.okx.client import OkxRestClient
from qdl.adapters.okx.history import OkxHistoricalClient
from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.capture import capture_exact_frame


@dataclass(frozen=True)
class OkxBarRawBinding:
    market: str
    product_type: str
    native_symbol: str
    interval: str
    subscription_id: str
    source_session_id: str
    connection_generation: int
    lease_epoch: int
    authority_revision: int
    partition_plan_epoch: int
    adapter_version: str
    config_revision: int
    instrument_catalog_revision: int

    def __post_init__(self) -> None:
        if self.market not in {"SWAP", "SPOT"}:
            raise ValueError("OKX bar market must be SWAP or SPOT")
        if self.product_type not in {"PERPETUAL", "SPOT"}:
            raise ValueError("OKX bar product type is invalid")
        if self.market == "SWAP" and self.product_type != "PERPETUAL":
            raise ValueError("OKX SWAP bar requires PERPETUAL product")
        if self.market == "SPOT" and self.product_type != "SPOT":
            raise ValueError("OKX Spot bar requires SPOT product")
        if self.interval != "1m":
            raise ValueError("stable OKX history edge currently certifies 1m BAR only")
        if any(
            not value.strip()
            for value in (
                self.native_symbol,
                self.subscription_id,
                self.source_session_id,
                self.adapter_version,
            )
        ):
            raise ValueError("OKX bar binding identity is incomplete")
        if min(
            self.connection_generation,
            self.lease_epoch,
            self.authority_revision,
            self.partition_plan_epoch,
            self.config_revision,
            self.instrument_catalog_revision,
        ) <= 0:
            raise ValueError("OKX bar binding revisions/epochs must be positive")


async def fetch_closed_bar_history_raw_envelopes(
    binding: OkxBarRawBinding,
    *,
    limit: int,
    now_ms: int | None = None,
    history_client: OkxHistoricalClient | None = None,
    test_provenance: bool = False,
) -> tuple[raw_provider_pb2.RawProviderEnvelope, ...]:
    if limit < 1 or limit > 10_000:
        raise ValueError("OKX history limit must be between 1 and 10000")
    observed_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    closed_boundary_ms = observed_ms // 60_000 * 60_000
    start_ms = closed_boundary_ms - limit * 60_000
    end_ms = closed_boundary_ms - 1
    client = history_client or OkxHistoricalClient(OkxRestClient())
    history = await client.candles(
        inst_id=binding.native_symbol,
        bar=binding.interval,
        start_ms=start_ms,
        end_ms=end_ms,
        price_type="TRADE",
        max_records=limit,
        max_pages=max(2, (limit + 299) // 300 + 1),
    )
    records = history.records
    if (
        history.coverage.status != "FULL"
        or len(records) != limit
        or history.coverage.observed_min_ts_ms != start_ms
    ):
        raise RuntimeError(
            "OKX closed-bar history is incomplete "
            f"requested={limit} observed={len(records)} coverage={history.coverage.status}"
        )
    opens = [item.open_ts_ms for item in records]
    if any(current - previous != 60_000 for previous, current in zip(opens, opens[1:])):
        raise RuntimeError("OKX closed-bar history contains a time gap")
    if any(not item.confirmed for item in records):
        raise RuntimeError("OKX history returned a provisional candle in the closed window")

    received_at_ns = time.time_ns()
    envelopes = []
    for index, item in enumerate(records):
        row = [
            str(item.open_ts_ms),
            item.open,
            item.high,
            item.low,
            item.close,
            item.volume_raw,
            item.volume_ccy_raw,
            item.volume_quote_raw,
            "1",
        ]
        if any(value is None for value in row):
            raise RuntimeError("OKX trade candle is missing a native volume field")
        raw = {
            "arg": {
                "channel": "candle1m",
                "instId": binding.native_symbol,
            },
            "data": [row],
        }
        raw_bytes = json.dumps(
            raw, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
        envelopes.append(capture_exact_frame(
            provider="OKX_DIRECT",
            venue="OKX",
            market=binding.market,
            product_type=binding.product_type,
            native_symbol=binding.native_symbol,
            native_channel="candle1m",
            subscription_id=binding.subscription_id,
            source_session_id=binding.source_session_id,
            connection_generation=binding.connection_generation,
            lease_epoch=binding.lease_epoch,
            authority_revision=binding.authority_revision,
            partition_plan_epoch=binding.partition_plan_epoch,
            received_at_ns=received_at_ns + index,
            raw_frame_bytes=raw_bytes,
            adapter_version=binding.adapter_version,
            config_revision=binding.config_revision,
            instrument_catalog_revision=binding.instrument_catalog_revision,
            correlation_id=(
                f"okx:{binding.market}:{binding.native_symbol}:"
                f"{binding.interval}:{item.open_ts_ms}"
            ),
            transport_protocol=raw_provider_pb2.TRANSPORT_PROTOCOL_HTTP,
            capture_boundary=raw_provider_pb2.CAPTURE_BOUNDARY_POST_DECOMPRESSION,
            test_provenance=test_provenance,
        ))
    return tuple(envelopes)

async def fetch_latest_closed_bar_raw_envelope(
    binding: OkxBarRawBinding,
    *,
    now_ms: int | None = None,
    history_client: OkxHistoricalClient | None = None,
    attempts: int = 4,
    sleep=asyncio.sleep,
    test_provenance: bool = False,
) -> raw_provider_pb2.RawProviderEnvelope:
    if not 1 <= attempts <= 8:
        raise ValueError("OKX latest-closed BAR attempts must be between 1 and 8")
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            values = await fetch_closed_bar_history_raw_envelopes(
                binding,
                limit=1,
                now_ms=now_ms,
                history_client=history_client,
                test_provenance=test_provenance,
            )
            if len(values) != 1:
                raise RuntimeError(
                    "OKX latest-closed BAR lookup returned invalid cardinality"
                )
            return values[0]
        except Exception as error:
            last_error = error
            if attempt + 1 < attempts:
                await sleep(min(0.5 * (2 ** attempt), 2.0))
    raise RuntimeError(
        f"OKX latest-closed BAR lookup exhausted attempts={attempts}"
    ) from last_error
