"""Alpha MARK/INDEX reads served inside the Query process from its own spool.

Alpha reads of the current MARK/INDEX first moved from venue REST to the stream
gateway's live view (OKX REST exceeded the venue bucket at stage 50). At stage
35 (2026-09-23) that added HTTP work to the single stream process that also
writes and fans out, and a market burst then left the writer 46 s behind. This
reader keeps the same verified view and the same private endpoint code, but
runs them in-process on the Query replica: before each read the instrument's
latest canonical record is offered from the local spool (exactly as
``hydrate_from_spool`` does on a lease change), then the unchanged endpoint and
``HttpExecutionMarkIndexReader`` conversion apply every identity, freshness and
gap gate. No network hop, no stream-process load, no venue call.

Execution (TS) reads keep the stream gateway's live view: it is fed before the
spool append and carries the quiet-session evidence execution requires.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
import os

import httpx
from fastapi import FastAPI
from google.protobuf.message import DecodeError

from qdl.marketdata.v2 import market_data_pb2
from qdl.reference.execution_live import HttpExecutionMarkIndexReader
from qdl.runtime.execution_mark_index import (
    ExecutionMarkIndexLiveView,
    install_execution_mark_index_read,
)

_LOCAL_EPOCH = 1
# Upper bound of the request freshness the private endpoint accepts.
_ALPHA_STALE_AFTER_MS = 300_000
_LOCAL_URL = "http://localhost"


class _LocalGateway:
    """The Query replica is never a writer; its local view has one epoch."""

    @staticmethod
    def assert_active(epoch: int | None = None) -> int:
        return _LOCAL_EPOCH


class SpoolRefreshingMarkIndexView(ExecutionMarkIndexLiveView):
    """The execution view, offered this replica's latest spooled record per read."""

    def attach(self, *, spool, canonical_stream: str) -> "SpoolRefreshingMarkIndexView":
        self._spool = spool
        self._canonical_stream = canonical_stream
        # Alpha reads are judged by the alpha's own declared freshness (the
        # view bounds a read by min(request freshness, binding stale_after)).
        # The binding's 2 s bound is the execution horizon; applied to alpha it
        # pushed about 30 % of Binance reads (1 s mark cadence plus spool
        # latency) back to venue REST. Execution reads never use this view.
        self._bindings = {
            uid: replace(binding, stale_after_ms=max(binding.stale_after_ms, _ALPHA_STALE_AFTER_MS))
            for uid, binding in self._bindings.items()
        }
        return self

    async def read(self, *, instrument_uid: str, **kwargs):
        binding = self._bindings.get(instrument_uid)
        if binding is not None:
            rows = await asyncio.to_thread(
                self._spool.read_tail,
                stream=self._canonical_stream,
                partition_key=binding.partition_key,
                limit=1,
            )
            if rows:
                stored = rows[-1]
                try:
                    envelope = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
                except DecodeError:
                    envelope = None
                if envelope is not None and envelope.WhichOneof("payload") == "mark_index_price":
                    await self.remember(
                        binding=binding, envelope=envelope, stored=stored,
                        gateway_epoch=_LOCAL_EPOCH,
                    )
        return await super().read(instrument_uid=instrument_uid, **kwargs)


def build_local_alpha_mark_index_reader(*, catalog, spool) -> HttpExecutionMarkIndexReader:
    view = SpoolRefreshingMarkIndexView.from_catalog(catalog).attach(
        spool=spool, canonical_stream=catalog.canonical_stream,
    )
    return reader_for_view(view)


def reader_for_view(view: SpoolRefreshingMarkIndexView) -> HttpExecutionMarkIndexReader:
    secret = os.urandom(32)
    app = FastAPI()
    install_execution_mark_index_read(app, gateway=_LocalGateway(), view=view, secret=secret)
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=_LOCAL_URL)
    return HttpExecutionMarkIndexReader(urls=(_LOCAL_URL,), secret=secret, client=client)
