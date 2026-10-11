"""TS-image BOOK validation: offline replay or isolated live Redis server.

stdin JSON: {schema_version: 1, instrument: InstrumentView JSON,
requirements: {BOOK_SNAPSHOT|BOOK_DELTA: DataRequirement JSON},
handoffs: {feed: actual Query MarketDataView JSON}, side: BUY|SELL,
observations: [{id: str, views: <p184_dr05_observer.read_views result>}]}.
Each successful GetSnapshot must also retain its actual stream_cursor.
GetFeedStatus quality is required, never inferred from raw envelope flags.
All application imports must resolve from the existing TS image. Mount this
file only, outside /app, with PYTHONPATH=/app. Offline uses network none;
--serve --namespace NS requires P184_DR05_ISOLATED_NAMESPACE=NS and the
disposable Kafka network namespace. POST /validate needs namespace in JSON
and X-P184-Namespace; Redis is fixed to 127.0.0.1:6379/2. GET /health.
Live mode uses wall clock, never captured-time freshness for application writes.
book_validated records only successful BOOK validation. In live mode accepted
means the complete serialized eligible cache view matches the incoming view,
including volatile quality fields. A mismatch is not a validation refusal.
cache_apply reports cache acceptance and writes separately. A no-write or fence
is not an endpoint/SDK/BOOK-validator rejection or a fault-specific DR verdict.
Synthetic fixtures belong only in tests. Caller owns evidence provenance.
"""

import base64
import hashlib
import json
import sys
import time

MAX_INPUT = 4 * 1024 * 1024
IMAGE = "sha256:a6548ee36aeb0242c1734c071678c525c1506d05c10a70b533eb7110e7eaf16a"
ENDPOINT = "/qdl.query.v2.MarketDataStreamService/"
# QualityView keys the server omits when unset (qdl_sdk/CHANGELOG.md, IDR-040 fix 2).
EMITTED_ONLY_WHEN_SET = frozenset({"proof_observed_at_ns"})


def requirement_from_json(raw):
    from qdl_sdk import models

    values = dict(raw)
    for name, kind in {
        "feed": models.Feed, "consumer_grade": models.Grade,
        "stale_policy": models.StalePolicy, "gap_policy": models.GapPolicy,
        "event_recency_policy": models.StalePolicy,
        "recovery": models.RecoveryPolicy, "bar_revision_policy": models.BarRevisionPolicy,
    }.items():
        if name in values and values[name] is not None:
            values[name] = kind(values[name])
    result = models.DataRequirement(**values)
    if result.feed.value not in {"BOOK_SNAPSHOT", "BOOK_DELTA"}:
        raise ValueError("BOOK feeds only")
    if result.consumer_grade is not models.Grade.EXECUTION:
        raise ValueError("EXECUTION requirement required")
    if result.max_freshness_ms is None or "max_session_liveness_ms" not in raw:
        raise ValueError("explicit freshness and session policy (possibly null) required")
    return result


def positive_clock(value):
    if type(value) is not int or value <= 0:
        raise ValueError("observation clocks must be positive integer nanoseconds")
    return value


def validate(packet, *, live=None):
    from dataclasses import replace
    from qdl.marketdata.v2 import market_data_pb2
    from qdl_sdk.client import _validate_query_payload
    from qdl_sdk.models import InstrumentView, MarketDataView, QualityView, StreamEvent
    from qdl_sdk.projection import market_data_view_from_stream
    from qdl_sdk.errors import DataLayerError
    from adapters.market_data.data_layer_client import DataLayerClientError
    from adapters.market_data.data_layer_v2 import execution_feed_from_v2
    from services.market_data.execution_ordering import ordering, compare, ExecutionOrderingError
    from services.market_data.execution_state import execution_payload
    from services.market_data.cache_reader import MarketCacheReader
    from services.risk_engine.execution_market_context import _validate_book, ExecutionMarketContextError

    if packet.get("schema_version") != 1 or packet.get("side") not in {"BUY", "SELL"}:
        raise ValueError("schema_version=1 and explicit BUY/SELL side required")
    instrument = InstrumentView.model_validate(packet["instrument"])
    requirements = {k: requirement_from_json(v) for k, v in packet["requirements"].items()}
    handoffs = {k: MarketDataView.model_validate(v) for k, v in packet["handoffs"].items()}
    observations = packet["observations"]
    if not isinstance(observations, list) or not 1 <= len(observations) <= 64:
        raise ValueError("1..64 observations required")
    previous, results, ids = {}, [], set()
    app_errors = (DataLayerError, DataLayerClientError, ExecutionOrderingError, ExecutionMarketContextError)
    for observation in observations:
        identifier = observation["id"]
        if not isinstance(identifier, str) or not identifier or identifier in ids:
            raise ValueError("observation ids must be unique nonempty strings")
        ids.add(identifier)
        if not observation["views"] or set(observation["views"]) - {"BOOK_SNAPSHOT", "BOOK_DELTA"}:
            raise ValueError("BOOK views required")
        for feed, row in observation["views"].items():
            result = {"id": identifier, "feed": feed, "accepted": False,
                      "stage": "input", "application_called": False, "current_generation": None,
                      "book_validated": False, "cache_apply": None}
            results.append(result)
            try:
                started = positive_clock(row["requested_at_ns"])
                completed = positive_clock(row["completed_at_ns"])
                if completed < started or row["instrument_uid"] != instrument.instrument_uid:
                    raise ValueError("clock regression or instrument mismatch")
                if live is not None and completed > time.time_ns():
                    raise ValueError("future observation clock")
                status, snapshot = row["GetFeedStatus"], row["GetSnapshot"]
                result["endpoint_codes"] = {"GetFeedStatus": status["code"], "GetSnapshot": snapshot["code"]}
                if status["code"] != "OK" or snapshot["code"] != "OK":
                    result.update(outcome="ENDPOINT_REFUSED", stage="endpoint")
                    continue
                requirement, template = requirements[feed], handoffs[feed]
                if requirement.feed.value != feed or template.feed.value != feed:
                    raise ValueError("feed mismatch")
                if requirement.instrument_uid != instrument.instrument_uid:
                    raise ValueError("requirement identity mismatch")
                # Require every captured status field: model defaults cannot create proof.
                # Exception: SDK fields the server emits only when set (IDR-040
                # proof_observed_at_ns is absent without a quiet-book proof).
                fields = set(QualityView.model_fields)
                keys = set(status["status"])
                if not fields - EMITTED_ONLY_WHEN_SET <= keys <= fields:
                    raise ValueError("complete actual GetFeedStatus quality required")
                quality = QualityView.model_validate(status["status"])
                if snapshot["cursor_valid"] is not True:
                    raise ValueError("verified snapshot cursor required")
                offset = snapshot["watermark_offset"]
                if type(offset) is not int or offset < 0:
                    raise ValueError("nonnegative actual watermark required")
                positive_clock(snapshot["data_as_of_ns"])
                events = snapshot["events_b64"]
                if not isinstance(events, list) or len(events) != 1:
                    raise ValueError("exactly one snapshot event required; no silent tail selection")
                raw = base64.b64decode(events[0], validate=True)
                result["event_sha256"] = hashlib.sha256(raw).hexdigest()
                envelope = market_data_pb2.EventEnvelope.FromString(raw)
                if envelope.instrument_id != instrument.instrument_id:
                    raise ValueError("envelope instrument_id mismatch")
                if not 0 < envelope.source_event_time_ns <= completed or not 0 < envelope.received_at_ns <= completed:
                    raise ValueError("missing/future envelope clocks")
                result.update(stage="sdk_status", application_called=True)
                # The actual status must pass SDK policy before raw projection runs.
                typed = template.model_dump(mode="json")
                typed["quality"] = quality.model_dump(mode="json")
                status_template = MarketDataView.model_validate(typed)
                _validate_query_payload(requirement, {"request_id": identifier, "data": status_template}, warmup=False)
                result["stage"] = "sdk_projection"
                view = market_data_view_from_stream(
                    StreamEvent(offset, snapshot["stream_cursor"], envelope), template=status_template,
                    requirement=requirement, now_ns=time.time_ns() if live is not None else completed,
                )
                # SDK computes envelope recency independently. Keep actual status quality
                # for TS admission; do not replace a refusal with computed healthy flags.
                typed = view.model_dump(mode="json")
                typed["quality"] = quality.model_dump(mode="json")
                view = MarketDataView.model_validate(typed)
                result["stage"] = "ts_adapter"
                execution = execution_feed_from_v2(view, instrument, session_max_ms=requirement.max_session_liveness_ms)
                execution = replace(execution, receipt_ns=started, state_refresh=True)
                result["stage"] = "ts_ordering"
                incoming = ordering(execution)
                if live is None and feed in previous and compare(previous[feed], incoming) < 0:
                    result.update(outcome="FENCED_OLDER", generation=incoming.generation)
                    continue
                if feed == "BOOK_SNAPSHOT":
                    result["stage"] = "ts_book_validator"
                    book = MarketCacheReader(None)._typed_execution_feed(
                        json.loads(execution_payload(execution)), expected_feed=feed)
                    if book is None:
                        raise ValueError("TS cache reader rejected BOOK shape")
                    result["book_proof"] = _validate_book(
                        book, now_ms=(time.time_ns() if live is not None else completed) // 1_000_000,
                        max_age_ms=requirement.max_freshness_ms,
                        side=packet["side"], require_levels=True,
                    )
                    result["book_validated"] = True
                if live is not None:
                    result["stage"] = "ts_redis_apply"
                    result.update(live.apply(execution, requirement.max_freshness_ms))
                    continue
                previous[feed] = incoming
                result.update(outcome="ACCEPTED", accepted=True, generation=incoming.generation,
                              sequence=incoming.sequence, quality=view.quality.model_dump(mode="json"))
            except app_errors as error:
                result.update(outcome="APPLICATION_REFUSED", error_type=type(error).__name__,
                              error=str(error)[:600], code=str(getattr(error, "code", "")))
            except Exception as error:
                result.update(outcome="INPUT_ERROR", error_type=type(error).__name__, error=str(error)[:600])
    return {"schema_version": 1, "live": live is not None, "controlled_replay": live is None, "expected_image": IMAGE, "results": results,
            "endpoints": [ENDPOINT + x for x in ("GetSnapshot", "GetFeedStatus")],
            "application_modules": {"sdk_projection": sys.modules[market_data_view_from_stream.__module__].__file__,
                                    "ts_adapter": sys.modules[execution_feed_from_v2.__module__].__file__,
                                    "ts_book_validator": sys.modules[_validate_book.__module__].__file__},
            "acceptance": False, "missing_coverage": ["full Risk admission (MARK/QUOTE absent)",
                "Redis cache application and generation adoption", "live Subscribe transport and ACK",
                "independent verification of caller capture provenance"]}


class LiveStore:
    """One isolated DB2 writer. Production CAS owns ordering and adoption."""

    def __init__(self):
        self.owned = set()
        self.previous = {}
        self.failed = False

    def apply(self, execution, max_age_ms):
        import asyncio
        if self.failed:
            raise RuntimeError("live writer fenced after ambiguous failure")
        try:
            return asyncio.run(self._apply(execution, max_age_ms))
        except Exception:
            # A transport failure may follow a successful CAS: never blindly retry.
            self.failed = True
            raise

    async def _apply(self, execution, max_age_ms):
        import asyncio
        from redis.asyncio import Redis
        from services.market_data.cache_projector import MarketCacheProjector
        from services.market_data.execution_state import state_keys, execution_payload
        from services.market_data.execution_ordering import ordering, compare
        redis = Redis.from_url("redis://127.0.0.1:6379/2", decode_responses=True,
                               socket_connect_timeout=1, socket_timeout=1)
        try:
            async with _deadline():
                keys = state_keys(execution)
                before = await redis.mget(*keys)
                if keys not in self.owned:
                    if any(value is not None for value in before):
                        raise ValueError("isolated TS keys already exist; refusing ownership")
                    self.owned.add(keys)
                projector = MarketCacheProjector(redis)
                deadline = time.monotonic() + 2
                applied = await projector.project_execution_snapshot(
                    execution, deadline=deadline, max_age_ms=max_age_ms,
                    previous=self.previous.get(keys), wall_clock_ns=time.time_ns,
                )
                after = await redis.mget(*keys)
                current = await projector.current_execution_snapshot(
                    execution.canonical_identity, execution.feed, deadline=deadline,
                    max_age_ms=max_age_ms, wall_clock_ns=time.time_ns,
                )
                if current is None:
                    raise ValueError("no current eligible cache after apply")
                incoming, retained = ordering(execution), ordering(current)
                relation = compare(retained, incoming)
                matches = execution_payload(current) == execution_payload(execution)
                self.previous[keys] = current
                return {"outcome": "APPLIED" if applied else "UNCHANGED" if matches else "FENCED_OLDER" if relation < 0 else "NO_WRITE",
                        "accepted": bool(matches), "applied": bool(applied),
                        "cache_apply": {"accepted": bool(matches), "written": bool(applied)},
                        "generation": incoming.generation, "sequence": incoming.sequence,
                        "current_generation": retained.generation, "retained_generation": retained.generation, "retained_sequence": retained.sequence,
                        "cache_unchanged": before == after,
                        "redis_apply_proof": {"method": "MarketCacheProjector.project_execution_snapshot",
                            "applied": bool(applied), "re_read": True, "db": 2},
                        "cache_sha256": hashlib.sha256(after[1].encode()).hexdigest(),
                        "validated_at_ns": time.time_ns()}
        finally:
            await redis.aclose()

    def cleanup(self):
        import asyncio
        from redis.asyncio import Redis
        async def remove():
            redis = Redis.from_url("redis://127.0.0.1:6379/2", socket_connect_timeout=1, socket_timeout=1)
            try:
                for keys in self.owned:
                    await redis.delete(*keys)
                    if any(v is not None for v in await redis.mget(*keys)):
                        raise RuntimeError("owned TS cleanup failed")
            finally:
                await redis.aclose()
        asyncio.run(remove())


class _deadline:
    """Python 3.10-compatible bounded async operation."""
    async def __aenter__(self):
        import asyncio
        self.task = asyncio.current_task()
        self.handle = asyncio.get_running_loop().call_later(3, self.task.cancel)

    async def __aexit__(self, kind, value, traceback):
        import asyncio
        self.handle.cancel()
        if kind is asyncio.CancelledError:
            raise TimeoutError("live Redis deadline") from value


def live_packet(packet, namespace):
    if packet.get("namespace") != namespace:
        raise ValueError("namespace mismatch")
    rows = packet.get("observations")
    if not isinstance(rows, list) or len(rows) != 1 or set(rows[0].get("views", {})) != {"BOOK_SNAPSHOT"}:
        raise ValueError("one BOOK_SNAPSHOT observation per live request required")
    if set(packet.get("requirements", {})) != {"BOOK_SNAPSHOT"}:
        raise ValueError("live BOOK_SNAPSHOT requirement only")
    if any(key in packet for key in ("wall_clock_ns", "now_ns", "redis_url", "clock")):
        raise ValueError("clock and Redis overrides forbidden")


def serve(namespace):
    import os
    import re
    import signal
    from http.server import BaseHTTPRequestHandler, HTTPServer
    if not re.fullmatch(r"p184-dr05-dl-[0-9a-f]{16}", namespace or ""):
        raise ValueError("invalid isolated namespace")
    if os.environ.get("P184_DR05_ISOLATED_NAMESPACE") != namespace:
        raise ValueError("namespace environment confirmation required")
    # Warm application imports before health becomes available.
    from services.market_data.cache_projector import MarketCacheProjector
    from services.risk_engine.execution_market_context import _validate_book
    store = LiveStore()

    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(3)

        def log_message(self, *args):
            pass

        def reply(self, status, payload):
            body = json.dumps(payload, sort_keys=True).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path != "/health":
                self.reply(404, {"acceptance": False})
                return
            self.reply(200, {"namespace": namespace, "ready": not store.failed, "acceptance": False})

        def do_POST(self):
            if self.path != "/validate" or self.headers.get("X-P184-Namespace") != namespace:
                self.reply(403, {"acceptance": False, "error": "path or namespace"})
                return
            try:
                if self.headers.get("Transfer-Encoding") or len(self.headers.get_all("Content-Length", [])) != 1:
                    raise ValueError("one Content-Length required; no transfer encoding")
                size = int(self.headers["Content-Length"])
                if not 0 < size <= MAX_INPUT:
                    raise ValueError("invalid body size")
                raw = self.rfile.read(size)
                if len(raw) != size:
                    raise ValueError("truncated body")
                packet = json.loads(raw)
                live_packet(packet, namespace)
                result = validate(packet, live=store)
                result["mode"] = "LIVE_REDIS_WALL_CLOCK"
                result["missing_coverage"] = ["full Risk admission (MARK/QUOTE absent)",
                    "live Subscribe transport and ACK", "fault-specific verdict owned by parent"]
                self.reply(200, result)
            except Exception as error:
                self.reply(400, {"acceptance": False, "outcome": "INPUT_ERROR", "error": str(error)[:300]})

    server = HTTPServer(("127.0.0.1", 8260), Handler)
    server.timeout = 0.5
    stopping = False
    def stop(*args):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    expires = time.monotonic() + 300
    try:
        while not stopping and time.monotonic() < expires:
            server.handle_request()
    finally:
        server.server_close()
        store.cleanup()


def main():
    if len(sys.argv) > 1:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--serve", action="store_true", required=True)
        parser.add_argument("--namespace", default=__import__("os").environ.get("P184_DR05_ISOLATED_NAMESPACE"))
        args = parser.parse_args()
        serve(args.namespace)
        return 0
    try:
        raw = sys.stdin.buffer.read(MAX_INPUT + 1)
        if len(raw) > MAX_INPUT:
            raise ValueError("input exceeds 4 MiB")
        result = validate(json.loads(raw))
    except Exception as error:
        print(json.dumps({"acceptance": False, "outcome": "INPUT_ERROR",
                          "error_type": type(error).__name__, "error": str(error)[:600]}))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 2 if any(r["outcome"] == "INPUT_ERROR" for r in result["results"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
