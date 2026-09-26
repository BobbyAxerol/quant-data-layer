#!/usr/bin/env python3
"""Declared-universe SDK benchmark, not a KN-5 load certificate.

Outside-container invocation (this helper does not orchestrate Docker):
  python -B scripts/benchmark_kn_universe.py --manifest /approved/universe.json
Default: inventory only. --schema prints the input schema. --run-approved opts
into separately approved reads using reachable paired targets and mTLS/JWT files.

run_target(..., on_ready=async_callback) awaits callback(Product, scratch_deque).
Copy any state needed beyond the callback: scratch rows and script-owned
WarmupResponse.data lists are cleared after use. Do not share these responses.
as_of_ns is a client validation barrier, not a server historical-as-of request.
Callback completion is not actual TS cache-write proof. Full KN load, hot coexistence,
retained-universe RAM, reconnect and C2 remain KN5. Durations are always ms.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import deque
from contextlib import aclosing
import json
from pathlib import Path
import re
import sys
import time
from typing import Literal
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from qdl_sdk.client import AsyncDataLayerClient, _fixed_interval_ns, _validate_query_payload
from qdl_sdk.credentials import RotatingJwtCredentialProvider
from qdl_sdk.errors import ContinuityError
from qdl_sdk.models import ControlEvent, DataRequirement, Feed, Grade
from qdl_sdk.projection import market_data_view_from_stream
from qdl_sdk.tls import WorkloadTlsConfig
from qdl_sdk.transport import GrpcStreamTransport, RestQueryTransport


class Closed(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TlsFiles(Closed):
    ca_file: str
    cert_file: str
    key_file: str

    @field_validator("*")
    @classmethod
    def absolute(cls, value):
        if not Path(value).is_absolute():
            raise ValueError("credential paths must be absolute file references")
        return value


class JwtFiles(Closed):
    private_key_file: str
    key_id: str
    issuer: str
    audience: str
    roles: list[str] = Field(min_length=1)

    @field_validator("private_key_file")
    @classmethod
    def absolute(cls, value):
        return TlsFiles.absolute(value)


class Identity(Closed):
    id: str = Field(min_length=1)
    subject: str = Field(min_length=1)
    environment: str = Field(min_length=1)
    manifest_revision: int = Field(ge=1, strict=True)
    max_batch_items: int = Field(ge=1, le=100, strict=True)
    max_streams: int = Field(ge=0, strict=True)
    tls: TlsFiles
    jwt: JwtFiles


class Target(Closed):
    replica: str = Field(min_length=1)
    query: str
    stream: str
    route_revision: str = Field(min_length=1)

    @model_validator(mode="after")
    def origins(self):
        url = urlsplit(self.query)
        if (url.scheme != "https" or not url.hostname or url.username or url.password
                or url.query or url.fragment or url.path not in ("", "/")):
            raise ValueError("query must be a credential-free HTTPS origin")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+:[0-9]+", self.stream):
            raise ValueError("explicit paired stream host:port required")
        return self


class Product(Closed):
    consumer_id: str
    venue: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    requirement: DataRequirement
    stream_approved: bool = False

    def evidence(self):
        return {**self.model_dump(exclude={"requirement", "stream_approved"}),
                "instrument_uid": self.requirement.instrument_uid,
                "feed": self.requirement.feed.value, "interval": self.requirement.interval,
                "requested_rows": self.requirement.warmup_specification.rows,
                "source_policy_id": self.requirement.source_policy_id}


class Profile(Closed):
    provenance: Literal["TEST_ONLY", "REAL_PROVIDER", "PROVIDER_CAPTURE"]
    identities: list[Identity] = Field(min_length=1, max_length=50)
    targets: list[Target] = Field(min_length=1, max_length=4)
    products: list[Product] = Field(min_length=1, max_length=10000)
    interval: str
    limit: int = Field(ge=1, le=10000, strict=True)
    as_of_ns: int = Field(gt=0, strict=True)
    bar_anchor_ns: int = Field(ge=0, strict=True)
    maxlen: int = Field(ge=1, le=10000, strict=True)
    batch_size: int = Field(default=50, ge=1, le=100, strict=True)
    max_batch_rows: int = Field(default=10000, ge=1, le=100000, strict=True)
    timeout_ms: int = Field(default=120000, ge=1, le=3600000, strict=True)
    stream_ms: int = Field(default=0, ge=0, le=60000, strict=True)

    @model_validator(mode="after")
    def declared_scope(self):
        _fixed_interval_ns(self.interval)
        ids = {x.id for x in self.identities}
        if len(ids) != len(self.identities) or ids != {p.consumer_id for p in self.products}:
            raise ValueError("identities must exactly cover declared consumer IDs")
        if len({x.replica for x in self.targets}) != len(self.targets):
            raise ValueError("duplicate replica")
        keys, grades = set(), {}
        for product in self.products:
            req, spec = product.requirement, product.requirement.warmup_specification
            if (req.feed is not Feed.BAR or req.interval != self.interval or spec is None
                    or spec.rows != self.limit or not req.require_final_bars):
                raise ValueError("universe requires final BAR, same interval and row limit")
            if req.consumer_grade is Grade.EXECUTION:
                raise ValueError("universe partial diagnostics require ALPHA/RESEARCH grade")
            if grades.setdefault(product.consumer_id, req.consumer_grade) != req.consumer_grade:
                raise ValueError("one consumer batch cannot mix grades")
            key = (product.consumer_id, req.instrument_uid)
            if key in keys:
                raise ValueError("duplicate declared requirement")
            keys.add(key)
        for identity in self.identities:
            if self.batch_size > identity.max_batch_items:
                raise ValueError("batch_size exceeds declared consumer quota; no automatic entitlement change")
            selected = sum(p.stream_approved for p in self.products if p.consumer_id == identity.id)
            if self.stream_ms and selected > identity.max_streams:
                raise ValueError("explicit stream selection exceeds declared consumer quota")
        if self.maxlen > self.limit:
            raise ValueError("maxlen cannot exceed requested rows")
        if self.stream_ms and not any(p.stream_approved for p in self.products):
            raise ValueError("stream probe requires explicit approved product selection")
        return self


class MeasuredQuery(RestQueryTransport):
    """Byte/decode counters only; HTTP body bytes are not network wire bytes."""
    body_bytes = 0
    decode_ms = 0.0

    def _decode(self, response):
        self.body_bytes += len(response.content)
        started = time.perf_counter()
        try:
            return super()._decode(response)
        finally:
            self.decode_ms += (time.perf_counter() - started) * 1000


def client_for(identity, target, timeout_ms):
    tls = WorkloadTlsConfig(identity.tls.ca_file, identity.tls.cert_file, identity.tls.key_file)
    credential = RotatingJwtCredentialProvider(
        **identity.jwt.model_dump(), algorithm="RS256", subject=identity.subject,
        environment=identity.environment, consumer_manifest_revision=identity.manifest_revision)
    return AsyncDataLayerClient(
        query_transport=MeasuredQuery(target.query, tls=tls, credential_provider=credential,
                                      timeout_seconds=timeout_ms / 1000),
        stream_transport=GrpcStreamTransport(target.stream, tls=tls, credential_provider=credential),
        consumer_id=identity.id, max_buffer_events=100, max_reconnect_attempts=0)


def error_code(error):
    code = str(getattr(error, "code", type(error).__name__))
    return code if re.fullmatch(r"[A-Za-z0-9_]{1,80}", code) else type(error).__name__


def apply_window(profile, product, warmup, window):
    """SDK validates wire/policy; check declared native identity and horizon."""
    duration, previous = _fixed_interval_ns(profile.interval), None
    for view in warmup.data:
        bar = view.payload
        if view.instrument_id != product.instrument_id or view.source.venue != product.venue:
            raise ContinuityError("CONFLICT", "declared instrument/venue differs")
        if ((bar.open_time_ns - profile.bar_anchor_ns) % duration
                or bar.close_time_ns not in (bar.open_time_ns + duration - 1, bar.open_time_ns + duration)):
            raise ContinuityError("BAR_ANCHOR_MISMATCH", "bar duration or anchor differs")
        if bar.open_time_ns + duration > profile.as_of_ns:
            raise ContinuityError("AS_OF_EXCEEDED", "bar closes after declared cutoff")
        if previous is not None and bar.open_time_ns != previous + duration:
            raise ContinuityError("OPEN_SEQUENCE_GAP", "window contains an interior gap")
        previous = bar.open_time_ns
        window.append(view)


async def stream_once(client, product, warmup, window, timeout_ms, profile):
    """One approved invocation using SDK handoff, not continuity certification."""
    started = time.perf_counter()

    controls = {}

    async def consume():
        async with client.warmup_then_stream(product.requirement, initial_warmup=warmup) as session:
            async for event in session:
                if isinstance(event, ControlEvent):
                    if event.code == "SNAPSHOT_REPLACED":
                        replacement = _validate_query_payload(product.requirement, event.snapshot, warmup=True)
                        window.clear()
                        apply_window(profile, product, replacement, window)
                    elif event.code not in {"REPLAYING", "LIVE", "RECONNECTED"}:
                        # Raw SNAPSHOT_REQUIRED does not reset the SDK cursor generation.
                        raise ContinuityError("STREAM_CONTROL", "unhandled control requires a new invocation")
                    controls[event.code] = controls.get(event.code, 0) + 1
                    continue
                view = market_data_view_from_stream(event, template=window[-1], requirement=product.requirement)
                if view.payload.lifecycle.value not in {"FINAL", "REVISED"}:
                    continue
                if view.payload.open_time_ns == window[-1].payload.open_time_ns:
                    window[-1] = view
                elif view.payload.open_time_ns == window[-1].payload.open_time_ns + _fixed_interval_ns(view.interval):
                    window.append(view)
                else:
                    raise ContinuityError("OPEN_SEQUENCE_GAP", "stream is not the next window update")
                session.acknowledge(event)
                return {"status": "APPLIED", "usable_ms": (time.perf_counter() - started) * 1000,
                        "protobuf_bytes": event.event.ByteSize(), "rows_applied": 1, "controls": controls}
            raise ContinuityError("STREAM_ENDED", "stream ended without usable data")

    try:
        return await asyncio.wait_for(consume(), timeout_ms / 1000)
    except Exception as error:
        return {"status": "FAIL", "error_code": error_code(error), "usable_ms": None,
                "elapsed_ms": (time.perf_counter() - started) * 1000, "controls": controls}


async def apply_item(profile, product, item, row, client, elapsed, on_ready):
    if item.problem is not None:
        row.update(status="FAIL", error_code=item.problem.code, failure_scope="ITEM",
                   retryable=item.problem.retryable, http_status=item.problem.status,
                   diagnostics=item.problem.diagnostics.model_dump() if item.problem.diagnostics else None)
        row["quality_flags"] = item.problem.diagnostics.reason_codes if item.problem.diagnostics else None
        return
    warmup, window = item.data, deque(maxlen=profile.maxlen)
    try:
        row["quality_flags"] = sorted({flag for view in warmup.data for flag in view.quality.flags})
        row["coverage"] = warmup.coverage
        apply_window(profile, product, warmup, window)
        row.update(usable_ms=elapsed(), window_rows=len(window), short_history=warmup.count < profile.limit,
                   first_open_ns=window[0].payload.open_time_ns, last_open_ns=window[-1].payload.open_time_ns)
        tail, now = window[-1], time.time_ns()
        row["source_ages_ms"] = {
            "observed": (now - tail.observed_at_ns) / 1e6,
            "received": (now - tail.received_at_ns) / 1e6 if tail.received_at_ns else None,
            "data_as_of": (now - warmup.data_as_of_ns) / 1e6,
            "server_freshness": tail.quality.freshness_ms}
        if on_ready is not None:
            callback_started = time.perf_counter()
            await on_ready(product, window)
            row.update(callback_complete_ms=elapsed(), callback_ms=(time.perf_counter() - callback_started) * 1000)
        if profile.stream_ms and product.stream_approved:
            row["stream"] = await stream_once(client, product, warmup, window, profile.stream_ms, profile)
            row["stream"]["caller_usable_ms"] = elapsed() if row["stream"]["status"] == "APPLIED" else None
        row["status"] = "USABLE"
    except Exception as error:
        row.update(status="FAIL", error_code=error_code(error), failure_scope="ITEM", usable_ms=None)
    finally:
        window.clear()
        warmup.data.clear()


async def run_target(profile, identity, target, client, *, on_ready=None, semaphore=None,
                     setup_error=None, caller_started=None):
    started = time.perf_counter() if caller_started is None else caller_started  # Before queue/SDK.
    elapsed = lambda: (time.perf_counter() - started) * 1000
    products = [p for p in profile.products if p.consumer_id == identity.id]
    rows = [{**p.evidence(), "replica": target.replica, "status": "NOT_ATTEMPTED",
             "usable_ms": None, "callback_complete_ms": None, "returned_rows": None,
             "window_rows": 0, "maxlen": profile.maxlen, "quality_flags": None,
             "stream": {"status": "NOT_SELECTED"}} for p in products]
    report = {"consumer_id": identity.id, "manifest_revision": identity.manifest_revision,
              "target": target.model_dump(), "items": rows, "chunks": [], "queue_ms": None,
              "declared_quotas": {"max_batch_items": identity.max_batch_items, "max_streams": identity.max_streams}}
    query = getattr(client, "query_transport", None)
    initial_bytes, initial_decode = getattr(query, "body_bytes", None), getattr(query, "decode_ms", None)

    async def work():
        queue_started = time.perf_counter()
        async with semaphore or asyncio.Semaphore(1):
            report["queue_ms"] = (time.perf_counter() - queue_started) * 1000
            if setup_error is not None:
                raise setup_error
            iterator = client.iter_warmup_batches(
                [p.requirement for p in products], require_all=False,
                batch_size=profile.batch_size, max_batch_rows=profile.max_batch_rows)
            width = min(profile.batch_size, max(1, profile.max_batch_rows // profile.limit))
            async with aclosing(iterator):
                for offset in range(0, len(products), width):
                    active = range(offset, min(offset + width, len(products)))
                    for index in active:
                        rows[index]["status"] = "IN_FLIGHT"
                    chunk_started = time.perf_counter()
                    body_before = getattr(query, "body_bytes", None)
                    decode_before = getattr(query, "decode_ms", None)
                    batch = await iterator.__anext__()
                    chunk = {"items": len(batch.results), "sdk_complete_ms": elapsed(),
                             "sdk_call_ms": (time.perf_counter() - chunk_started) * 1000,
                             "response_body_bytes": query.body_bytes - body_before if body_before is not None else None,
                             "decode_ms": query.decode_ms - decode_before if decode_before is not None else None}
                    report["chunks"].append(chunk)
                    if len(batch.results) != len(active):
                        raise ContinuityError("PARTIAL_RESULT", "iterator chunk size differs from declared bounds")
                    for index, item in zip(active, batch.results, strict=True):
                        row = rows[index]
                        row["returned_rows"] = item.data.count if item.data else 0
                        await apply_item(profile, products[index], item, row, client, elapsed, on_ready)
                        row["elapsed_ms"] = elapsed()
                    chunk["complete_ms"] = elapsed()
                    chunk["all_usable_ms"] = (max(rows[i]["usable_ms"] for i in active)
                                              if all(rows[i]["usable_ms"] is not None for i in active) else None)
                    del batch, item

    try:
        await asyncio.wait_for(work(), profile.timeout_ms / 1000)
    except Exception as error:
        report["error_code"] = error_code(error)
        for row in rows:
            if row["status"] == "IN_FLIGHT":
                row.update(status="TIMEOUT" if isinstance(error, asyncio.TimeoutError) else "FAIL",
                           error_code=error_code(error), failure_scope="CHUNK", elapsed_ms=elapsed(), usable_ms=None)
    for row in rows:
        if row["status"] == "NOT_ATTEMPTED":
            row["error_code"] = "ABORTED_BEFORE_DISPATCH"
    usable = [r["usable_ms"] for r in rows if r["status"] == "USABLE"]
    report.update(
        offered=len(rows), attempted=sum(r["status"] != "NOT_ATTEMPTED" for r in rows),
        usable=len(usable), failed=sum(r["status"] == "FAIL" for r in rows),
        timed_out=sum(r["status"] == "TIMEOUT" for r in rows),
        not_attempted=sum(r["status"] == "NOT_ATTEMPTED" for r in rows),
        first_item_usable_ms=min(usable) if usable else None,
        all_items_usable_ms=max(usable) if len(usable) == len(rows) else None,
        first_chunk_complete_ms=report["chunks"][0].get("complete_ms") if report["chunks"] else None,
        all_callbacks_complete_ms=(max(r["callback_complete_ms"] for r in rows)
                                   if all(r["callback_complete_ms"] is not None for r in rows) else None),
        elapsed_ms=elapsed(), returned_rows=sum(r["returned_rows"] or 0 for r in rows),
        returned_rows_unknown_items=sum(r["returned_rows"] is None for r in rows),
        completed=sum(r["status"] == "USABLE" for r in rows),
        response_body_bytes=getattr(query, "body_bytes", 0) - initial_bytes if initial_bytes is not None else None,
        decode_ms=getattr(query, "decode_ms", 0) - initial_decode if initial_decode is not None else None,
        status="PASS" if all(r["status"] == "USABLE" and r["stream"]["status"] != "FAIL" for r in rows) else "INCOMPLETE")
    return report


async def run(profile, *, execute=False, factory=client_for, on_ready=None):
    report = {"schema": "qdl.kn-universe-benchmark.v1", "provenance": profile.provenance,
              "provenance_verified": False, "status": "INVENTORY_ONLY", "runs": [],
              "declared_products": len(profile.products), "declared_symbols": [p.evidence() for p in profile.products],
              "interval": profile.interval, "limit": profile.limit, "as_of_ns": profile.as_of_ns,
              "bar_anchor_ns": profile.bar_anchor_ns, "maxlen": profile.maxlen,
              "batch_size": profile.batch_size, "max_batch_rows": profile.max_batch_rows,
              "targets": [t.model_dump() for t in profile.targets],
              "timing_contract": "ms before queue through SDK decode/validation and scratch window apply; callbacks separate",
              "byte_contract": "response_body_bytes is measured decoded HTTP body, not wire; no per-item apportionment/re-encoding",
              "as_of_contract": "client validation barrier only, not sent to server; no historical-as-of guarantee",
              "limits": ["serial target/consumer runs", "no atomic cross-chunk snapshot or replica parity assertion",
                         "scratch rows discarded, not retained universe RAM", "callback is not TS cache-write proof",
                         "full KN load, hot coexistence and C2 remain KN5"]}
    if not execute:
        return report
    for target in profile.targets:
        for identity in profile.identities:
            caller_started = time.perf_counter()
            try:
                client = factory(identity, target, profile.timeout_ms)
            except Exception as error:
                report["runs"].append(await run_target(profile, identity, target, None, setup_error=error, caller_started=caller_started))
                continue
            try:
                report["runs"].append(await run_target(profile, identity, target, client, on_ready=on_ready, caller_started=caller_started))
            finally:
                await client.close()
    report["status"] = "PASS" if all(r["status"] == "PASS" for r in report["runs"]) else "INCOMPLETE"
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--schema", action="store_true")
    parser.add_argument("--run-approved", action="store_true")
    args = parser.parse_args(argv)
    if args.schema:
        print(json.dumps(Profile.model_json_schema(), indent=2))
        return 0
    if args.manifest is None:
        parser.error("--manifest is required")
    profile = Profile.model_validate_json(args.manifest.read_text())
    if args.run_approved and profile.provenance == "TEST_ONLY":
        parser.error("TEST_ONLY uses injected local transports, not live CLI requests")
    report = asyncio.run(run(profile, execute=args.run_approved))
    print(json.dumps(report, indent=2))
    return 0 if report["status"] in {"PASS", "INVENTORY_ONLY"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
