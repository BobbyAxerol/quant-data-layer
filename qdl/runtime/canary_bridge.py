from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import time
import uuid
import ipaddress
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, Header, HTTPException, Request

from qdl.canonical.market import canonicalize_binance_usdm_rest_bar
from qdl.canonical.trade import TradeContext, canonical_event
from qdl.marketdata.v2 import market_data_pb2
from qdl.runtime.canary_source import CanarySourceBinding, CanarySourceCatalog
from qdl.runtime.lease import GatewayFenced
from qdl.stream import DurableStreamGateway


@dataclass(frozen=True, slots=True)
class V1ReadOnlyBridgeConfig:
    source_catalog_path: str
    v1_base_url: str
    ingest_urls: tuple[str, ...]
    ingest_secret: bytes
    run_once: bool = False

    def __post_init__(self) -> None:
        if not self.v1_base_url.startswith(("http://", "https://")):
            raise ValueError("V1 bridge base URL must be HTTP(S)")
        host = urlsplit(self.v1_base_url).hostname or ""
        try:
            loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            loopback = host in {"localhost", "data_layer"} or host.endswith(".internal")
        if not loopback:
            raise ValueError("V1 bridge may address only the internal/loopback V1 service")
        if not self.ingest_urls or any(
            not value.startswith("http://") for value in self.ingest_urls
        ):
            raise ValueError("beta internal ingest URLs are required")
        if len(self.ingest_secret) < 32:
            raise ValueError("beta internal ingest secret must contain at least 256 bits")

    @classmethod
    def from_environment(
        cls, values: Mapping[str, str] | None = None
    ) -> "V1ReadOnlyBridgeConfig":
        env = os.environ if values is None else values
        urls = json.loads(env["QDL_BETA_STREAM_INGEST_URLS_JSON"])
        if not isinstance(urls, list):
            raise ValueError("QDL_BETA_STREAM_INGEST_URLS_JSON must be an array")
        return cls(
            source_catalog_path=env["QDL_BETA_SOURCE_BINDINGS"],
            v1_base_url=env["QDL_BETA_V1_BASE_URL"].rstrip("/"),
            ingest_urls=tuple(str(value).rstrip("/") for value in urls),
            ingest_secret=env["QDL_BETA_INTERNAL_INGEST_SECRET"].encode(),
            run_once=env.get("QDL_BETA_BRIDGE_RUN_ONCE", "false").lower()
            in {"1", "true", "yes"},
        )


class V1ReadOnlyBarSource:
    """Bounded V1 API reader. It cannot resolve or call a venue URL directly."""

    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=self.base_url,
            follow_redirects=False,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )

    async def fetch(
        self,
        binding: CanarySourceBinding,
        *,
        warmup: bool,
    ) -> tuple[list, ...]:
        limit = binding.read.warmup_rows if warmup else binding.read.poll_rows
        params = dict(binding.read.params) | {"limit": str(limit)}
        last_error: BaseException | None = None
        for attempt in range(1, binding.read.max_attempts + 1):
            try:
                response = await self._client.get(
                    binding.read.path,
                    params=params,
                    timeout=binding.read.timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                return self._validate_payload(binding, payload)
            except (httpx.HTTPError, ValueError, TypeError, json.JSONDecodeError) as error:
                last_error = error
                if attempt >= binding.read.max_attempts:
                    break
                await asyncio.sleep(min(
                    0.25 * 2 ** (attempt - 1), binding.read.max_backoff_seconds
                ))
        raise RuntimeError(
            f"V1 read-only source failed after {binding.read.max_attempts} attempts"
        ) from last_error

    @staticmethod
    def _validate_payload(
        binding: CanarySourceBinding, payload
    ) -> tuple[list, ...]:
        if not isinstance(payload, dict):
            raise ValueError("V1 source response must be an object")
        if (
            payload.get("provider") != "binance"
            or str(payload.get("market", "")).lower() != "usdm"
            or str(payload.get("symbol", "")).upper()
            != binding.instrument.native_symbol
            or payload.get("requested_interval") != binding.interval
        ):
            raise ValueError("V1 source response identity does not match the binding")
        rows = payload.get("data")
        if not isinstance(rows, list):
            raise ValueError("V1 source response has no data rows")
        validated = []
        previous_open = -1
        for row in rows:
            if not isinstance(row, list) or len(row) < 11:
                raise ValueError("V1 Binance bar row violates the native kline contract")
            open_time = int(row[0])
            close_time = int(row[6])
            if open_time <= previous_open or close_time <= open_time:
                raise ValueError("V1 Binance bars are not strictly time ordered")
            previous_open = open_time
            validated.append(row)
        return tuple(validated)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


class CanonicalV1Bridge:
    def __init__(
        self,
        *,
        config: V1ReadOnlyBridgeConfig,
        catalog: CanarySourceCatalog,
        source: V1ReadOnlyBarSource,
        client: httpx.AsyncClient | None = None,
        clock_ns=time.time_ns,
    ) -> None:
        self.config = config
        self.catalog = catalog
        self.source = source
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(10.0),
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=4),
        )
        self._clock_ns = clock_ns

    async def run_once(self, *, warmup: bool) -> dict[str, int]:
        totals = {"fetched": 0, "closed": 0, "accepted": 0, "duplicates": 0}
        for binding in self.catalog.bindings:
            rows, envelopes = await self.prepare(binding, warmup=warmup)
            totals["fetched"] += len(rows)
            totals["closed"] += len(envelopes)
            if not envelopes:
                continue
            result = await self.submit(envelopes)
            totals["accepted"] += int(result["accepted"])
            totals["duplicates"] += int(result["duplicates"])
        return totals

    async def prepare(
        self,
        binding: CanarySourceBinding,
        *,
        warmup: bool,
    ) -> tuple[tuple[list, ...], tuple[market_data_pb2.EventEnvelope, ...]]:
        rows = await self.source.fetch(binding, warmup=warmup)
        return rows, self.canonical_closed_bars(binding, rows)

    def canonical_closed_bars(
        self,
        binding: CanarySourceBinding,
        rows: tuple[list, ...],
    ) -> tuple[market_data_pb2.EventEnvelope, ...]:
        now_ns = self._clock_ns()
        result = []
        for row in rows:
            if binding.require_closed_bar and int(row[6]) * 1_000_000 >= now_ns:
                continue
            timestamp = self._clock_ns()
            raw = {
                "provider": "binance",
                "market": "usdm",
                "symbol": binding.instrument.native_symbol,
                "interval": binding.interval,
                "row": row,
            }
            context = TradeContext(
                instrument_uid=binding.instrument.instrument_uid,
                instrument_id=binding.instrument.instrument_id,
                instrument_revision=binding.instrument.metadata_revision,
                venue=binding.instrument.identity.venue,
                market=binding.instrument.identity.market,
                product_type=binding.instrument.identity.product_type.value,
                native_symbol=binding.instrument.native_symbol,
                provider=binding.provider,
                source_id=binding.source_id,
                lease_epoch=0,
                received_at_ns=timestamp,
                normalized_at_ns=timestamp,
                published_at_ns=timestamp,
                partition_sequence=int(row[0]),
                normalizer_version=binding.normalizer_version,
                adapter_version=binding.adapter_version,
                config_revision=1,
                correlation_id=f"phase72-{binding.binding_id}-{row[0]}",
            )
            result.append(canonicalize_binance_usdm_rest_bar(raw, context))
        return tuple(result)

    async def submit(
        self, envelopes: tuple[market_data_pb2.EventEnvelope, ...]
    ) -> dict[str, int]:
        body = json.dumps(
            {
                "schema": "qdl.phase7.2.canonical-ingest.v1",
                "batch_id": str(uuid.uuid4()),
                "events": [
                    base64.b64encode(
                        item.SerializeToString(deterministic=True)
                    ).decode("ascii")
                    for item in envelopes
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        signature = hmac.new(
            self.config.ingest_secret, body, hashlib.sha256
        ).hexdigest()
        last_error: BaseException | None = None
        for endpoint in self.config.ingest_urls:
            try:
                response = await self._client.post(
                    f"{endpoint}/internal/canonical/events",
                    content=body,
                    headers={
                        "Content-Type": "application/json",
                        "X-QDL-Bridge-Signature": f"sha256={signature}",
                    },
                )
                if response.status_code in {409, 503}:
                    continue
                response.raise_for_status()
                payload = response.json()
                if payload.get("schema") != "qdl.phase7.2.ingest-result.v1":
                    raise ValueError("beta ingest response schema is invalid")
                return {
                    "accepted": int(payload["accepted"]),
                    "duplicates": int(payload["duplicates"]),
                }
            except (httpx.HTTPError, ValueError, TypeError) as error:
                last_error = error
        raise RuntimeError("no active beta stream gateway accepted canonical data") from last_error

    async def close(self) -> None:
        await self.source.close()
        if self._owns_client:
            await self._client.aclose()


def install_internal_canonical_ingest(
    app: FastAPI,
    *,
    gateway: DurableStreamGateway,
    catalog: CanarySourceCatalog,
    secret: bytes,
) -> None:
    if len(secret) < 32:
        raise ValueError("beta internal ingest secret must contain at least 256 bits")

    @app.post("/internal/canonical/events", include_in_schema=False)
    async def ingest(
        request: Request,
        signature: str | None = Header(None, alias="X-QDL-Bridge-Signature"),
    ):
        body = await request.body()
        expected = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
        if not signature or not hmac.compare_digest(signature, expected):
            raise HTTPException(status_code=401, detail="invalid internal bridge signature")
        try:
            payload = json.loads(body)
            if set(payload) != {"schema", "batch_id", "events"}:
                raise ValueError("canonical ingest fields are incomplete or unknown")
            if payload["schema"] != "qdl.phase7.2.canonical-ingest.v1":
                raise ValueError("canonical ingest schema is invalid")
            uuid.UUID(str(payload["batch_id"]))
            values = payload["events"]
            if not isinstance(values, list) or not 1 <= len(values) <= 1000:
                raise ValueError("canonical ingest batch must contain 1..1000 events")
        except (ValueError, TypeError, json.JSONDecodeError) as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        try:
            lease_epoch = gateway.assert_active()
        except GatewayFenced as error:
            raise HTTPException(status_code=409, detail="gateway is not active") from error
        accepted = 0
        duplicates = 0
        high_watermarks: dict[str, int] = {}
        for value in values:
            try:
                raw = base64.b64decode(value, validate=True)
                envelope = market_data_pb2.EventEnvelope.FromString(raw)
                binding = catalog.binding_for_envelope(envelope)
                if (
                    envelope.bar.lifecycle
                    != market_data_pb2.BAR_LIFECYCLE_FINAL
                    or not envelope.bar.is_final
                ):
                    raise ValueError("beta bridge accepts final bars only")
                envelope.lease_epoch = int(lease_epoch or 1)
                stored = await gateway.publish(canonical_event(
                    envelope,
                    accepted_at_ns=time.time_ns(),
                ))
            except (ValueError, TypeError) as error:
                raise HTTPException(status_code=422, detail=str(error)) from error
            if stored is None:
                duplicates += 1
                continue
            accepted += 1
            high_watermarks[binding.partition_key] = stored.cursor.offset
        return {
            "schema": "qdl.phase7.2.ingest-result.v1",
            "accepted": accepted,
            "duplicates": duplicates,
            "lease_epoch": lease_epoch,
            "high_watermarks": high_watermarks,
        }


async def serve_v1_readonly_bridge() -> None:
    config = V1ReadOnlyBridgeConfig.from_environment()
    catalog = CanarySourceCatalog.load(config.source_catalog_path)
    source = V1ReadOnlyBarSource(config.v1_base_url)
    bridge = CanonicalV1Bridge(config=config, catalog=catalog, source=source)
    warmup = True
    try:
        while True:
            await bridge.run_once(warmup=warmup)
            if config.run_once:
                return
            warmup = False
            await asyncio.sleep(min(item.read.poll_seconds for item in catalog.bindings))
    finally:
        await bridge.close()
